"""
TCP listener ingestion method.

Receives speed sensor data pushed over TCP from legacy Wavetronics
devices (port 10088). Devices connect, send a complete data payload,
and close the connection. Decoded events are persisted to the database.

Uses a device_map to resolve source IP addresses to signal IDs.
Connections from unmapped IPs are logged and discarded.

This is a ListenerIngestionMethod — the CollectorService manages
start/stop lifecycle. The server runs as a long-lived asyncio TCP
server accepting concurrent connections.
"""

import asyncio
import logging
from typing import Any, Optional

from pydantic import BaseModel, Field

from ..registry import IngestionMethodRegistry, ListenerIngestionMethod
from ..sdk import persist_events, resolve_decoder_by_name

logger = logging.getLogger(__name__)

_DEFAULT_DECODER = "auto"


class TCPServerConfig(BaseModel):
    """
    Configuration for the TCP server ingestion method.

    Args:
        bind_address: Network interface to bind to.
        port: TCP port to listen on.
        decoder: Explicit decoder name, or None for auto-detect.
        max_connections: Maximum simultaneous connections.
        read_timeout_seconds: Per-connection read timeout.
        buffer_size: Read buffer size in bytes.
        device_map: Mapping of source IP to signal ID.
    """

    bind_address: str = "0.0.0.0"
    port: int = 10088
    decoder: Optional[str] = None
    max_connections: int = 100
    read_timeout_seconds: int = 30
    buffer_size: int = 65536
    device_map: dict[str, str] = Field(default_factory=dict)


@IngestionMethodRegistry.register("tcp_server")
class TCPServerMethod(ListenerIngestionMethod):
    """
    TCP listener ingestion method for Wavetronics speed sensors.

    A listener plugin: the CollectorService calls start() once and
    stop() on shutdown. Accepts TCP connections from external devices,
    decodes pushed data, and persists events to the database.

    Source IPs are resolved to signal IDs via the device_map config.
    Connections from unmapped IPs are logged as warnings and dropped.
    """

    name = "tcp_server"

    def __init__(self) -> None:
        self._server: Optional[asyncio.Server] = None
        self._session_factory = None
        self._config: Optional[TCPServerConfig] = None
        self._active_connections: int = 0
        self._connection_semaphore: Optional[asyncio.Semaphore] = None

    @staticmethod
    def _build_config(raw: dict[str, Any]) -> TCPServerConfig:
        """
        Build TCPServerConfig from a raw config dict.

        Args:
            raw: Listener config dict.

        Returns:
            TCPServerConfig instance.
        """
        return TCPServerConfig(
            bind_address=raw.get("bind_address", "0.0.0.0"),
            port=raw.get("port", 10088),
            decoder=raw.get("decoder"),
            max_connections=raw.get("max_connections", 100),
            read_timeout_seconds=raw.get("read_timeout_seconds", 30),
            buffer_size=raw.get("buffer_size", 65536),
            device_map=raw.get("device_map", {}),
        )

    def _resolve_decoder(self, config: TCPServerConfig):
        """
        Get a decoder instance for the configured decoder.

        Args:
            config: TCP server configuration.

        Returns:
            Decoder instance.
        """
        return resolve_decoder_by_name(config.decoder or _DEFAULT_DECODER)

    async def health_check(self) -> bool:
        """
        Check if the TCP server is running and accepting connections.

        Returns:
            True if the server is serving, False otherwise.
        """
        return self._server is not None and self._server.is_serving()

    async def start(self, config: dict[str, Any], session_factory) -> None:
        """
        Start the TCP listener server.

        Creates an asyncio TCP server bound to the configured address
        and port. Each incoming connection is handled in a separate
        coroutine with concurrency limited by max_connections.

        Args:
            config: Listener config (port, bind address, device_map, etc.).
            session_factory: Async session factory for DB writes.
        """
        self._config = self._build_config(config)
        self._session_factory = session_factory
        self._connection_semaphore = asyncio.Semaphore(
            self._config.max_connections
        )

        if not self._config.device_map:
            logger.warning(
                "TCP server has empty device_map — "
                "all connections will be rejected. "
                "IP-based authentication is the only option "
                "for legacy Wavetronics devices.",
            )

        self._server = await asyncio.start_server(
            self._handle_connection,
            host=self._config.bind_address,
            port=self._config.port,
        )

        logger.info(
            "TCP server listening on %s:%d",
            self._config.bind_address,
            self._config.port,
        )

    async def stop(self) -> None:
        """
        Stop the TCP listener server.

        Stops accepting new connections, waits up to 10 seconds for
        in-flight connections to finish, then closes the server.
        """
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
        """
        Handle a single inbound TCP connection.

        Reads the full payload from the device, resolves the source IP
        to a signal ID via device_map, decodes the data, and persists
        events. Errors on individual connections do not affect the server.

        Args:
            reader: Asyncio stream reader for the connection.
            writer: Asyncio stream writer for the connection.
        """
        peer = writer.get_extra_info("peername")
        peer_ip = peer[0] if peer else "unknown"

        # Enforce max connections
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
            await self._process_connection(reader, writer, peer_ip)
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
        writer: asyncio.StreamWriter,
        peer_ip: str,
    ) -> None:
        """
        Read, decode, and persist data from a single TCP connection.

        Args:
            reader: Asyncio stream reader for the connection.
            writer: Asyncio stream writer for the connection.
            peer_ip: Remote IP address of the device.
        """
        # Resolve source IP to signal ID
        signal_id = self._config.device_map.get(peer_ip)
        if signal_id is None:
            logger.warning(
                "TCP connection from unmapped IP %s — skipping", peer_ip
            )
            return

        logger.debug("TCP connection accepted from %s (signal %s)", peer_ip, signal_id)

        # Read full payload with timeout
        try:
            data = await asyncio.wait_for(
                self._read_payload(reader),
                timeout=self._config.read_timeout_seconds,
            )
        except asyncio.TimeoutError:
            logger.warning(
                "TCP read timeout from %s for signal %s", peer_ip, signal_id
            )
            return

        if not data:
            logger.debug(
                "TCP connection from %s closed with no data (signal %s)",
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
                "Failed to decode TCP payload from %s for signal %s",
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

    async def _read_payload(self, reader: asyncio.StreamReader) -> bytes:
        """
        Read a complete payload from the connection.

        Wavetronics devices send a complete data blob and then close
        the connection. Reads until EOF up to the configured buffer size.

        Args:
            reader: Asyncio stream reader for the connection.

        Returns:
            Raw bytes received from the device.
        """
        data = await reader.read(self._config.buffer_size)
        return data
