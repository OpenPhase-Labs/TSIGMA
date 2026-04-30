"""
UDP listener ingestion method.

Receives speed sensor data pushed over UDP from Wavetronics devices
(port 10088).  Each datagram is a self-contained payload.  Decoded
events are persisted through the ingestion target
(controller_event_log for signals, roadside_event for sensors).

Source IP routing: the orchestrator passes a ``devices`` list of
``(device_id, per_device_config)`` pairs at start.  The listener builds
an in-memory IP→device_id map from each device's first-class ``host``
field (mirrored from the table's ``ip_address`` column).  Datagrams
from unmapped IPs are logged and discarded.

This is a ListenerIngestionMethod — the ListenerService manages
start/stop lifecycle. The server runs as a long-lived asyncio
datagram endpoint receiving concurrent datagrams.
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


class UDPServerConfig(BaseModel):
    """Layer-2 server config for the UDP listener."""

    bind_address: str = "0.0.0.0"
    port: int = 10088
    decoder: Optional[str] = None
    max_packet_size: int = 65536


@IngestionMethodRegistry.register("udp_server")
class UDPServerMethod(ListenerIngestionMethod):
    """UDP listener for push-mode roadway sensors and modern controllers."""

    name = "udp_server"

    def __init__(self) -> None:
        self._transport: Optional[asyncio.DatagramTransport] = None
        self._protocol: Optional["_UDPProtocol"] = None
        self._session_factory = None
        self._config: Optional[UDPServerConfig] = None
        self._target: IngestionTarget = ControllerTarget()
        self._device_map: dict[str, tuple[str, dict[str, Any]]] = {}

    @staticmethod
    def _build_config(raw: dict[str, Any]) -> UDPServerConfig:
        return UDPServerConfig(
            bind_address=raw.get("bind_address", "0.0.0.0"),
            port=raw.get("port", 10088),
            decoder=raw.get("decoder"),
            max_packet_size=raw.get("max_packet_size", 65536),
        )

    @staticmethod
    def _build_device_map(
        devices: Iterable[tuple[str, dict[str, Any]]],
    ) -> dict[str, tuple[str, dict[str, Any]]]:
        """Map peer IP -> (device_id, per-device config)."""
        out: dict[str, tuple[str, dict[str, Any]]] = {}
        for device_id, config in devices:
            host = config.get("host")
            if not host:
                logger.warning(
                    "UDP: device %s has no ip_address — cannot route, skipping",
                    device_id,
                )
                continue
            out[host] = (device_id, config)
        return out

    def _resolve_decoder(
        self, per_device: dict[str, Any], server: UDPServerConfig,
    ):
        name = (
            per_device.get("decoder")
            or server.decoder
            or _DEFAULT_DECODER
        )
        return resolve_decoder_by_name(name)

    async def health_check(self) -> bool:
        return self._transport is not None and not self._transport.is_closing()

    async def start(
        self,
        config: dict[str, Any],
        session_factory,
        *,
        target: Any = None,
        devices: Any = None,
    ) -> None:
        self._config = self._build_config(config)
        self._session_factory = session_factory
        self._target = target if target is not None else ControllerTarget()
        self._device_map = self._build_device_map(devices or [])

        if not self._device_map:
            logger.warning(
                "UDP server has no resolvable devices — all datagrams will "
                "be rejected. Configure %s devices with method=udp_server "
                "and an ip_address before starting.",
                self._target.device_type,
            )

        loop = asyncio.get_running_loop()
        self._transport, self._protocol = await loop.create_datagram_endpoint(
            lambda: _UDPProtocol(self),
            local_addr=(self._config.bind_address, self._config.port),
        )

        logger.info(
            "UDP server (%s) listening on %s:%d — %d device(s) routable",
            self._target.device_type,
            self._config.bind_address,
            self._config.port,
            len(self._device_map),
        )

    async def stop(self) -> None:
        if self._transport is None:
            return

        self._transport.close()

        logger.info(
            "UDP server stopped on %s:%d",
            self._config.bind_address if self._config else "0.0.0.0",
            self._config.port if self._config else 10088,
        )

        self._transport = None
        self._protocol = None

    async def _process_datagram(self, data: bytes, addr: tuple) -> None:
        peer_ip = addr[0]

        resolved = self._device_map.get(peer_ip)
        if resolved is None:
            logger.warning(
                "UDP datagram from unmapped IP %s — skipping", peer_ip
            )
            return

        device_id, per_device = resolved

        if not data:
            logger.debug(
                "UDP datagram from %s empty (%s %s)",
                peer_ip, self._target.device_type, device_id,
            )
            return

        try:
            decoder = self._resolve_decoder(per_device, self._config)
            events = decoder.decode_bytes(data)
        except Exception:
            logger.exception(
                "Failed to decode UDP payload from %s for %s %s",
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


class _UDPProtocol(asyncio.DatagramProtocol):
    """Bridge synchronous DatagramProtocol callbacks to async handling."""

    def __init__(self, server: UDPServerMethod) -> None:
        self._server = server

    def datagram_received(self, data: bytes, addr: tuple) -> None:
        asyncio.ensure_future(self._server._process_datagram(data, addr))

    def error_received(self, exc: Exception) -> None:
        logger.error("UDP transport error: %s", exc)

    def connection_lost(self, exc: Optional[Exception]) -> None:
        if exc is not None:
            logger.error("UDP transport closed with error: %s", exc)
