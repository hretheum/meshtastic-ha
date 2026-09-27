# SPDX-FileCopyrightText: 2024-2025 Pascal Brogle @broglep
#
# SPDX-License-Identifier: MIT

import asyncio
import random
from asyncio import StreamReader, StreamWriter
from types import TracebackType
from typing import Self

from google.protobuf import message

from custom_components.meshtastic.aiomeshtastic import MeshInterface
from custom_components.meshtastic.aiomeshtastic.connection import ClientApiConnection
from custom_components.meshtastic.aiomeshtastic.connection.streaming import StreamingClientTransport
from custom_components.meshtastic.aiomeshtastic.protobuf import mesh_pb2
from custom_components.meshtastic.const import LOGGER

_LOGGER = LOGGER.getChild(__name__)

# How long to wait for the gateway to finish a config dump before giving up on it.
_CONFIG_SNAPSHOT_TIMEOUT = 60
# How old a snapshot may get before it is refreshed in the background.
_CONFIG_SNAPSHOT_MAX_AGE = 300
# Special nonce that asks the gateway for its config without the node database. The
# firmware answers it quickly, where a full dump keeps the link busy for many seconds.
_CONFIG_ID_MINIMAL = 69420
# Shortest gap between two gateway session refreshes.
_SESSION_REARM_MIN_INTERVAL = 60
# FromRadio variants that make up a config download. Live traffic (mesh packets, queue
# status, log records) arrives on the same stream and must not end up in the snapshot.
_CONFIG_SNAPSHOT_VARIANTS = frozenset(
    {
        "my_info",
        "node_info",
        "config",
        "moduleConfig",
        "channel",
        "metadata",
        "fileInfo",
        "deviceuiConfig",
    }
)


class ClientProxyTransport(StreamingClientTransport):
    def __init__(self, reader: StreamReader, writer: StreamWriter) -> None:
        super().__init__()
        self._reader = reader
        self._writer = writer

    async def read_to_radio_packet(self) -> tuple[bytes, mesh_pb2.ToRadio] | None:
        packet = await self._read_packet_bytes()
        if packet is None:
            return None

        to_radio = mesh_pb2.ToRadio()
        try:
            to_radio.ParseFromString(packet)
        except message.DecodeError:
            self._logger.debug("Error while parsing ToRadio bytes %s", packet, exc_info=True)
        else:
            return packet, to_radio

    async def write_from_radio_packet(self, from_radio: mesh_pb2.FromRadio) -> None:
        binary = from_radio.SerializeToString()
        await self._write_bytes(StreamingClientTransport.build_frame(binary))

    async def _read_bytes(self, n: int = -1, *, exactly: int | None = None) -> bytes | None:
        if exactly is not None:
            return await self._reader.readexactly(n=exactly)
        return await self._reader.read(n=n)

    async def _write_bytes(self, data: bytes) -> bool:
        self._writer.write(data)
        return True

    def _can_read(self) -> bool:
        return not self._reader.at_eof()

    async def connect(self) -> None:
        pass

    async def disconnect(self) -> None:
        await self._disconnect()

    async def _connect(self) -> None:
        pass

    async def _disconnect(self) -> None:
        self._writer.close()
        await self._writer.wait_closed()

    @property
    def is_connected(self) -> bool:
        return True


class MeshtasticTcpProxy:
    def __init__(self, interface: MeshInterface, host: str | None = None, port: int | None = None) -> None:
        self._client_queues: set[asyncio.Queue] = set()
        self._clients = {}
        self._forward_task = None
        self._interface = interface
        self._should_stop = False
        self._host = host
        self._port = port or 4403
        self._server: asyncio.Server | None = None
        self._config_refresh_task: asyncio.Task | None = None

        self._packet_queue = asyncio.Queue()

        self._config_snapshot: list[mesh_pb2.FromRadio] = []
        self._config_snapshot_at: float | None = None
        self._session_rearm_at: float | None = None
        self._config_snapshot_ready = asyncio.Event()
        self._config_refresh_lock = asyncio.Lock()
        self._config_capture: list[mesh_pb2.FromRadio] | None = None
        self._config_capture_id: int | None = None
        self._config_capture_done: asyncio.Event | None = None

    async def __aenter__(self) -> Self:
        await self.start()
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: TracebackType | None
    ) -> None:
        await self.stop()

    async def start(self) -> None:
        if not self._interface.is_running:
            self._should_stop = True
            await self._interface.start()

        self._server = await asyncio.start_server(self._handle_client, self._host, self._port)
        self._forward_task = asyncio.create_task(self._forward_from_radio(), name="tcp_proxy_read_from_gateway")
        self._config_refresh_task = asyncio.create_task(
            self._refresh_config_snapshot(), name="tcp_proxy_config_snapshot"
        )

    async def stop(self) -> None:
        self._forward_task.cancel()
        if self._config_refresh_task is not None:
            self._config_refresh_task.cancel()

        if self._server:
            self._server.close()

        if self._should_stop:
            await self._interface.stop()
            self._should_stop = False

    async def _handle_client(self, reader: StreamReader, writer: StreamWriter) -> None:
        queue = asyncio.Queue()
        self._client_queues.add(queue)
        client_connection = ClientProxyTransport(reader, writer)
        disconnect_event = asyncio.Event()
        try:
            peer = writer.transport.get_extra_info("peername", (None, None, None, None))
            peer_name = "{}:{}".format(*peer[0:2])

            async def forward_to_radio() -> None:
                while True:
                    packet = await client_connection.read_to_radio_packet()
                    if packet is None:
                        continue
                    packet_bytes, to_radio = packet

                    # skip disconnect request
                    if to_radio.disconnect:
                        disconnect_event.set()
                        await client_connection.disconnect()
                        continue

                    _LOGGER.debug(
                        "Forwarding from %s to gateway: %s",
                        peer_name,
                        ClientApiConnection._protobuf_log(packet[1]),  # noqa: SLF001
                    )

                    if to_radio.HasField("want_config_id"):
                        await self._serve_config(queue, to_radio.want_config_id, peer_name)
                        continue

                    await self._interface._connection._send_packet(packet[0])  # noqa: SLF001

            async def forward_from_radio() -> None:
                while client_connection.is_connected:
                    from_radio = await queue.get()
                    _LOGGER.debug(
                        "Forwarding from gateway to %s: %s",
                        peer_name,
                        ClientApiConnection._protobuf_log(from_radio),  # noqa: SLF001
                    )
                    await client_connection.write_from_radio_packet(from_radio)

            task_read = asyncio.create_task(forward_to_radio(), name=f"tcp_proxy_read_{peer_name}")
            task_write = asyncio.create_task(forward_from_radio(), name=f"tcp_proxy_write_{peer_name}")
            task_wait_disconnect = asyncio.create_task(
                disconnect_event.wait(), name=f"tcp_proxy_disconnect_wait_{peer_name}"
            )

            try:
                await asyncio.wait([task_wait_disconnect, task_read, task_write], return_when=asyncio.FIRST_COMPLETED)
            finally:
                task_read.cancel()
                task_write.cancel()
                task_wait_disconnect.cancel()
            _LOGGER.debug("Closed proxy connection %s", peer_name)
        except:  # noqa: E722
            _LOGGER.warning("Failed handling proxy connection")
        finally:
            self._client_queues.remove(queue)
            await client_connection.disconnect()

    async def _serve_config(self, queue: asyncio.Queue, config_id: int, peer_name: str) -> None:
        """
        Answer a client's config request from the cached snapshot.

        Asking the gateway for a fresh config dump on every client connection is both
        slow and mutually exclusive: the gateway serves one download at a time and
        restarts the dump whenever a new request arrives, so a second client silently
        breaks the first one's handshake. Because every packet the gateway emits is
        already fanned out to all clients, the proxy can keep the last complete dump
        and replay it, stamped with the id this client is waiting for. Clients get
        their config immediately and independently of each other, while live updates
        keep flowing through the normal broadcast path.
        """
        if not self._config_snapshot_ready.is_set():
            await self._refresh_config_snapshot()

        if not self._config_snapshot_ready.is_set():
            _LOGGER.warning("No config snapshot available for %s, asking the gateway directly", peer_name)
            to_radio = mesh_pb2.ToRadio()
            to_radio.want_config_id = config_id
            await self._interface._connection._send_packet(to_radio.SerializeToString())  # noqa: SLF001
            return

        for packet in self._config_snapshot:
            await queue.put(packet)

        config_complete = mesh_pb2.FromRadio()
        config_complete.config_complete_id = config_id
        await queue.put(config_complete)
        _LOGGER.debug("Served config snapshot (%s packets) to %s", len(self._config_snapshot), peer_name)

        # Only when this client is the only one connected. Any config request keeps the
        # gateway from sending anything else for ten seconds or more, and a client hangs up
        # when the gateway goes quiet while it waits on a heartbeat, so a client joining an
        # existing session must never trigger one. It does not need to either: whoever
        # connected first already re-armed the session.
        if len(self._client_queues) <= 1:
            self._config_refresh_task = asyncio.create_task(self._refresh_gateway(), name="tcp_proxy_gateway_refresh")

    async def _refresh_gateway(self) -> None:
        if self._is_config_snapshot_stale():
            await self._refresh_config_snapshot()
        else:
            await self._rearm_gateway_session()

    def _is_config_snapshot_stale(self) -> bool:
        if self._config_snapshot_at is None:
            return True
        return (asyncio.get_running_loop().time() - self._config_snapshot_at) > _CONFIG_SNAPSHOT_MAX_AGE

    async def _rearm_gateway_session(self) -> None:
        """
        Keep the phone session the firmware maintains for the connected client alive.

        The firmware stops answering admin requests once that session goes stale, and the
        apps send admin requests right after connecting, so a client whose requests go
        unanswered hangs up. Only a config request re-arms it, and the client's own request
        is answered from the snapshot and never reaches the gateway, so ask on its behalf.

        The minimal variant is used on purpose. A full dump keeps the gateway from sending
        anything else for ten seconds or more, and clients drop the connection once the
        gateway has been quiet for about twenty, so re-arming with a full dump would cost
        exactly the connections it is meant to keep.
        """
        async with self._config_refresh_lock:
            now = asyncio.get_running_loop().time()
            if self._session_rearm_at is not None and (now - self._session_rearm_at) < _SESSION_REARM_MIN_INTERVAL:
                return
            self._session_rearm_at = now
            await self._gateway_config_dump(_CONFIG_ID_MINIMAL, keep=False)

    async def _refresh_config_snapshot(self) -> None:
        async with self._config_refresh_lock:
            if self._config_snapshot_ready.is_set() and not self._is_config_snapshot_stale():
                return

            # 0 is the default value of config_complete_id, so it can not be used as an id.
            config_id = random.randint(1, 0xFFFFFFFF)  # noqa: S311
            if config_id == _CONFIG_ID_MINIMAL:
                config_id += 1
            packets = await self._gateway_config_dump(config_id, keep=True)
            if packets is None:
                return
            self._config_snapshot = packets
            self._config_snapshot_at = asyncio.get_running_loop().time()
            self._session_rearm_at = self._config_snapshot_at
            self._config_snapshot_ready.set()
            _LOGGER.debug("Refreshed config snapshot, %s packets", len(self._config_snapshot))

    async def _gateway_config_dump(self, config_id: int, *, keep: bool) -> list[mesh_pb2.FromRadio] | None:
        to_radio = mesh_pb2.ToRadio()
        to_radio.want_config_id = config_id

        done = asyncio.Event()
        self._config_capture = []
        self._config_capture_id = config_id
        self._config_capture_done = done
        try:
            await self._interface._connection._send_packet(to_radio.SerializeToString())  # noqa: SLF001
            await asyncio.wait_for(done.wait(), timeout=_CONFIG_SNAPSHOT_TIMEOUT)
        except TimeoutError:
            _LOGGER.warning(
                "Gateway did not finish config dump %s within %s seconds",
                config_id,
                _CONFIG_SNAPSHOT_TIMEOUT,
            )
            return None
        else:
            return self._config_capture if keep else []
        finally:
            self._config_capture = None
            self._config_capture_id = None
            self._config_capture_done = None

    def _capture_config_packet(self, packet: mesh_pb2.FromRadio) -> bool:
        """
        Take a packet out of the stream while a config dump of our own is running.

        Returns True when the packet belongs to that dump, in which case it is kept for
        the snapshot and not passed on. Clients already hold this config, and a second
        completion for an id they finished with would look like the gateway restarting
        their handshake. Live traffic keeps flowing untouched.
        """
        if self._config_capture is None:
            return False
        variant = packet.WhichOneof("payload_variant")
        if variant == "config_complete_id":
            if packet.config_complete_id != self._config_capture_id:
                return False
            if self._config_capture_done is not None:
                self._config_capture_done.set()
            return True
        if variant in _CONFIG_SNAPSHOT_VARIANTS:
            self._config_capture.append(packet)
            return True
        return False

    async def _forward_from_radio(self) -> None:
        try:
            while True:
                async for packet in self._interface.from_radio_stream():
                    if self._capture_config_packet(packet):
                        continue
                    for queue in self._client_queues:
                        await queue.put(packet)
        except asyncio.CancelledError:
            pass
        except:  # noqa: E722
            _LOGGER.warning("Failuring during forwarding from gateway", exc_info=True)
