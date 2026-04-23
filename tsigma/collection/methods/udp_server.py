"""
UDP listener ingestion method.

Receives speed sensor data pushed over UDP from Wavetronics
devices (port 10088). Each datagram is a self-contained payload.
Decoded events are persisted to the database.

Uses a device_map to resolve source IP addresses to signal IDs.
Datagrams from unmapped IPs are logged and discarded.

This is a ListenerIngestionMethod — the CollectorService manages
start/stop lifecycle. The server runs as a long-lived asyncio
datagram endpoint receiving concurrent datagrams.
"""

import asyncio
import logging
from typing import Any, Optional

from pydantic import BaseModel, Field

from ..registry import IngestionMethodRegistry, ListenerIngestionMethod
from ..sdk import persist_events, resolve_decoder_by_name

logger = logging.getLogger(__name__)

_DEFAULT_DECODER = "auto"


class UDPServerConfig(BaseModel):
    """
    Configuration for the UDP server ingestion method.

    Args:
        bind_address: Network interface to bind to.
        port: UDP port to listen on.
        decoder: Explicit decoder name, or None for auto-detect.
        max_packet_size: Maximum UDP datagram size in bytes.
        device_map: Mapping of source IP to signal ID.
    """

    bind_address: str = "0.0.0.0"
    port: int = 10088
    decoder: Optional[str] = None
    max_packet_size: int = 65536
    device_map: dict[str, str] = Field(default_factory=dict)


@IngestionMethodRegistry.register("udp_server")
class UDPServerMethod(ListenerIngestionMethod):
    """
    UDP listener ingestion method for Wavetronics speed sensors.

    A listener plugin: the CollectorService calls start() once and
    stop() on shutdown. Receives UDP datagrams from external devices,
    decodes pushed data, and persists events to the database.

    Source IPs are resolved to signal IDs via the device_map config.
    Datagrams from unmapped IPs are logged as warnings and discarded.
    """

    name = "udp_server"

    def __init__(self) -> None:
        self._transport: Optional[asyncio.DatagramTransport] = None
        self._protocol: Optional["_UDPProtocol"] = None
        self._session_factory = None
        self._config: Optional[UDPServerConfig] = None

    @staticmethod
    def _build_config(raw: dict[str, Any]) -> UDPServerConfig:
        """
        Build UDPServerConfig from a raw config dict.

        Args:
            raw: Listener config dict.

        Returns:
            UDPServerConfig instance.
        """
        return UDPServerConfig(
            bind_address=raw.get("bind_address", "0.0.0.0"),
            port=raw.get("port", 10088),
            decoder=raw.get("decoder"),
            max_packet_size=raw.get("max_packet_size", 65536),
            device_map=raw.get("device_map", {}),
        )

    def _resolve_decoder(self, config: UDPServerConfig):
        """
        Get a decoder instance for the configured decoder.

        Args:
            config: UDP server configuration.

        Returns:
            Decoder instance.
        """
        return resolve_decoder_by_name(config.decoder or _DEFAULT_DECODER)

    async def health_check(self) -> bool:
        """
        Check if the UDP server is running and receiving datagrams.

        Returns:
            True if the transport is active, False otherwise.
        """
        return self._transport is not None and not self._transport.is_closing()

    async def start(self, config: dict[str, Any], session_factory) -> None:
        """
        Start the UDP listener server.

        Creates an asyncio datagram endpoint bound to the configured
        address and port. Incoming datagrams are handled by the
        internal protocol class.

        Args:
            config: Listener config (port, bind address, device_map, etc.).
            session_factory: Async session factory for DB writes.
        """
        self._config = self._build_config(config)
        self._session_factory = session_factory

        if not self._config.device_map:
            logger.warning(
                "UDP server has empty device_map — "
                "all datagrams will be rejected. "
                "IP-based authentication is the only option "
                "for legacy Wavetronics devices.",
            )

        loop = asyncio.get_running_loop()
        self._transport, self._protocol = await loop.create_datagram_endpoint(
            lambda: _UDPProtocol(self),
            local_addr=(self._config.bind_address, self._config.port),
        )

        logger.info(
            "UDP server listening on %s:%d",
            self._config.bind_address,
            self._config.port,
        )

    async def stop(self) -> None:
        """
        Stop the UDP listener server.

        Closes the datagram transport. No graceful drain is needed
        for UDP — there are no persistent connections to wait on.
        """
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
        """
        Decode and persist a single UDP datagram.

        Resolves the source IP to a signal ID via device_map,
        decodes the payload, and persists events to the database.

        Args:
            data: Raw datagram bytes.
            addr: Source address tuple (ip, port).
        """
        peer_ip = addr[0]

        # Resolve source IP to signal ID
        signal_id = self._config.device_map.get(peer_ip)
        if signal_id is None:
            logger.warning(
                "UDP datagram from unmapped IP %s — skipping", peer_ip
            )
            return

        if not data:
            logger.debug(
                "UDP datagram from %s empty (signal %s)",
                peer_ip,
                signal_id,
            )
            return

        # Decode payload
        try:
            decoder = self._resolve_decoder(self._config)
            events = decoder.decode_bytes(data)
        except Exception:
            logger.exception(
                "Failed to decode UDP payload from %s for signal %s",
                peer_ip,
                signal_id,
            )
            return

        # Persist events
        await persist_events(events, signal_id, self._session_factory)

        if events:
            logger.info(
                "Collected %d events from %s for signal %s",
                len(events),
                peer_ip,
                signal_id,
            )


class _UDPProtocol(asyncio.DatagramProtocol):
    """
    Asyncio datagram protocol for the UDP ingestion server.

    Bridges the synchronous DatagramProtocol callbacks to the
    async _process_datagram method on UDPServerMethod.
    """

    def __init__(self, server: UDPServerMethod) -> None:
        self._server = server

    def datagram_received(self, data: bytes, addr: tuple) -> None:
        """
        Handle an incoming UDP datagram.

        Since this callback is synchronous, the async processing
        is scheduled as a task on the running event loop.

        Args:
            data: Raw datagram bytes.
            addr: Source address tuple (ip, port).
        """
        asyncio.ensure_future(self._server._process_datagram(data, addr))

    def error_received(self, exc: Exception) -> None:
        """
        Handle a transport-level error.

        Args:
            exc: The OS-level exception received.
        """
        logger.error("UDP transport error: %s", exc)

    def connection_lost(self, exc: Optional[Exception]) -> None:
        """
        Handle transport closure.

        Args:
            exc: Exception if the transport was closed due to error,
                 None for normal shutdown.
        """
        if exc is not None:
            logger.error("UDP transport closed with error: %s", exc)
