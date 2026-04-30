"""
TCP listener ingestion method.

Receives speed sensor data pushed over TCP from legacy Wavetronics
devices (port 10088). Devices connect, send a complete data payload,
and close the connection. Decoded events are persisted through the
ingestion target (controller_event_log for signals,
roadside_event for sensors).

Source IP routing: the orchestrator passes a ``devices`` list of
``(device_id, per_device_config)`` pairs at start.  The listener builds
an in-memory IP→device_id map from each device's first-class
``host`` field (mirrored from the table's ``ip_address`` column).
Connections from unmapped IPs are logged and discarded.

This is a ListenerIngestionMethod — the ListenerService manages
start/stop lifecycle. The server runs as a long-lived asyncio TCP
server accepting concurrent connections.
"""

import asyncio
import logging
from typing import Any, Iterable, Optional

from pydantic import BaseModel

from ..registry import IngestionMethodRegistry, ListenerIngestionMethod
from ..sdk import resolve_decoder_by_name
from ..targets import ControllerTarget, IngestionTarget

logger = logging.getLogger(__name__)

_DEFAULT_DECODER = "auto"


class TCPServerConfig(BaseModel):
    """Layer-2 server config for the TCP listener."""

    bind_address: str = "0.0.0.0"
    port: int = 10088
    decoder: Optional[str] = None
    max_connections: int = 100
    read_timeout_seconds: int = 30
    buffer_size: int = 65536


@IngestionMethodRegistry.register("tcp_server")
class TCPServerMethod(ListenerIngestionMethod):
    """
    TCP listener for push-mode roadway sensors and modern controllers.

    Source IPs are resolved to a device_id by looking up the inbound
    peer IP against the device map built from the orchestrator's
    ``devices`` list.  Connections from unmapped IPs are dropped.
    """

    name = "tcp_server"

    def __init__(self) -> None:
        self._server: Optional[asyncio.Server] = None
        self._session_factory = None
        self._config: Optional[TCPServerConfig] = None
        self._target: IngestionTarget = ControllerTarget()
        # IP -> (device_id, per_device_config) map built from devices=
        self._device_map: dict[str, tuple[str, dict[str, Any]]] = {}
        self._active_connections: int = 0
        self._connection_semaphore: Optional[asyncio.Semaphore] = None

    @staticmethod
    def _build_config(raw: dict[str, Any]) -> TCPServerConfig:
        return TCPServerConfig(
            bind_address=raw.get("bind_address", "0.0.0.0"),
            port=raw.get("port", 10088),
            decoder=raw.get("decoder"),
            max_connections=raw.get("max_connections", 100),
            read_timeout_seconds=raw.get("read_timeout_seconds", 30),
            buffer_size=raw.get("buffer_size", 65536),
        )

    @staticmethod
    def _build_device_map(
        devices: Iterable[tuple[str, dict[str, Any]]],
    ) -> dict[str, tuple[str, dict[str, Any]]]:
        """Map peer IP -> (device_id, per-device config).

        The device's first-class ``host`` field (mirrored from the
        ``ip_address`` column by the DeviceSource) is the key.  Devices
        without a host field are skipped — TCP routing has no fallback
        when the device hasn't declared its IP.
        """
        out: dict[str, tuple[str, dict[str, Any]]] = {}
        for device_id, config in devices:
            host = config.get("host")
            if not host:
                logger.warning(
                    "TCP: device %s has no ip_address — cannot route, skipping",
                    device_id,
                )
                continue
            out[host] = (device_id, config)
        return out

    def _resolve_decoder(
        self, per_device: dict[str, Any], server: TCPServerConfig,
    ):
        """Per-device decoder takes precedence over the server default."""
        name = (
            per_device.get("decoder")
            or server.decoder
            or _DEFAULT_DECODER
        )
        return resolve_decoder_by_name(name)

    async def health_check(self) -> bool:
        return self._server is not None and self._server.is_serving()

    async def start(
        self,
        config: dict[str, Any],
        session_factory,
        *,
        target: Any = None,
        devices: Any = None,
    ) -> None:
        """Bind the TCP listener and build the IP→device routing map."""
        self._config = self._build_config(config)
        self._session_factory = session_factory
        self._target = target if target is not None else ControllerTarget()
        self._device_map = self._build_device_map(devices or [])
        self._connection_semaphore = asyncio.Semaphore(
            self._config.max_connections
        )

        if not self._device_map:
            logger.warning(
                "TCP server has no resolvable devices — all connections "
                "will be rejected. Configure %s devices with method=tcp_server "
                "and an ip_address before starting.",
                self._target.device_type,
            )

        self._server = await asyncio.start_server(
            self._handle_connection,
            host=self._config.bind_address,
            port=self._config.port,
        )

        logger.info(
            "TCP server (%s) listening on %s:%d — %d device(s) routable",
            self._target.device_type,
            self._config.bind_address,
            self._config.port,
            len(self._device_map),
        )

    async def stop(self) -> None:
        if self._server is None:
            return

        self._server.close()

        try:
            await asyncio.wait_for(self._server.wait_closed(), timeout=10.0)
        except asyncio.TimeoutError:
            logger.warning(
                "TCP server shutdown timed out with %d active connections",
                self._active_connections,
            )

        logger.info(
            "TCP server stopped on %s:%d",
            self._config.bind_address if self._config else "0.0.0.0",
            self._config.port if self._config else 10088,
        )

        self._server = None

    async def _handle_connection(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        peer = writer.get_extra_info("peername")
        peer_ip = peer[0] if peer else "unknown"

        if not self._connection_semaphore.acquire_nowait():
            logger.warning(
                "TCP connection rejected from %s: max connections reached",
                peer_ip,
            )
            writer.close()
            await writer.wait_closed()
            return

        self._active_connections += 1

        try:
            await self._process_connection(reader, peer_ip)
        except Exception:
            logger.exception(
                "Unhandled error on TCP connection from %s", peer_ip
            )
        finally:
            self._active_connections -= 1
            self._connection_semaphore.release()
            writer.close()
            await writer.wait_closed()

    async def _process_connection(
        self,
        reader: asyncio.StreamReader,
        peer_ip: str,
    ) -> None:
        resolved = self._device_map.get(peer_ip)
        if resolved is None:
            logger.warning(
                "TCP connection from unmapped IP %s — skipping", peer_ip
            )
            return

        device_id, per_device = resolved
        logger.debug(
            "TCP connection accepted from %s (%s %s)",
            peer_ip, self._target.device_type, device_id,
        )

        try:
            data = await asyncio.wait_for(
                self._read_payload(reader),
                timeout=self._config.read_timeout_seconds,
            )
        except asyncio.TimeoutError:
            logger.warning(
                "TCP read timeout from %s for %s %s",
                peer_ip, self._target.device_type, device_id,
            )
            return

        if not data:
            logger.debug(
                "TCP connection from %s closed with no data (%s %s)",
                peer_ip, self._target.device_type, device_id,
            )
            return

        try:
            decoder = self._resolve_decoder(per_device, self._config)
            events = decoder.decode_bytes(data)
        except Exception:
            logger.exception(
                "Failed to decode TCP payload from %s for %s %s",
                peer_ip, self._target.device_type, device_id,
            )
            return

        await self._target.persist(events, device_id, self._session_factory)

        if events:
            logger.info(
                "Collected %d events from %s for %s %s",
                len(events), peer_ip,
                self._target.device_type, device_id,
            )

    async def _read_payload(self, reader: asyncio.StreamReader) -> bytes:
        """Wavetronics devices send one blob then close — read until EOF."""
        return await reader.read(self._config.buffer_size)
