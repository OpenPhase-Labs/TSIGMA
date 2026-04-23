"""
Unit tests for TCP server ingestion method plugin.

Tests registration, health checks, config building,
and connection processing with mocked asyncio and decoder dependencies.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tsigma.collection.methods.tcp_server import (
    TCPServerConfig,
    TCPServerMethod,
)
from tsigma.collection.registry import IngestionMethodRegistry


class TestTCPServerRegistration:
    """Tests for registry integration."""

    def test_registered(self):
        """IngestionMethodRegistry.get('tcp_server') returns TCPServerMethod."""
        assert IngestionMethodRegistry.get("tcp_server") is TCPServerMethod


class TestTCPServerHealthCheck:
    """Tests for health_check behaviour."""

    @pytest.mark.asyncio
    async def test_health_check_not_running(self):
        """health_check returns False when server not started."""
        method = TCPServerMethod()
        assert await method.health_check() is False


class TestTCPServerBuildConfig:
    """Tests for _build_config."""

    def test_build_config_defaults(self):
        """_build_config with empty dict returns correct defaults."""
        config = TCPServerMethod._build_config({})

        assert config.bind_address == "0.0.0.0"
        assert config.port == 10088
        assert config.decoder is None
        assert config.max_connections == 100
        assert config.read_timeout_seconds == 30
        assert config.buffer_size == 65536
        assert config.device_map == {}

    def test_build_config_custom(self):
        """_build_config with all fields populated."""
        raw = {
            "bind_address": "192.168.1.10",
            "port": 9999,
            "decoder": "asc3",
            "max_connections": 50,
            "read_timeout_seconds": 10,
            "buffer_size": 4096,
            "device_map": {"10.0.0.1": "SIG-001"},
        }
        config = TCPServerMethod._build_config(raw)

        assert config.bind_address == "192.168.1.10"
        assert config.port == 9999
        assert config.decoder == "asc3"
        assert config.max_connections == 50
        assert config.read_timeout_seconds == 10
        assert config.buffer_size == 4096
        assert config.device_map == {"10.0.0.1": "SIG-001"}


class TestTCPServerProcessConnection:
    """Tests for _process_connection."""

    @pytest.mark.asyncio
    async def test_process_connection_unmapped_ip(self):
        """Connection from IP not in device_map is skipped (no persist)."""
        method = TCPServerMethod()
        method._config = TCPServerConfig(
            device_map={"10.0.0.1": "SIG-001"},
        )
        method._session_factory = AsyncMock()

        reader = AsyncMock()
        writer = MagicMock()

        with patch(
            "tsigma.collection.methods.tcp_server.persist_events"
        ) as mock_persist:
            await method._process_connection(reader, writer, "192.168.99.99")
            mock_persist.assert_not_called()

    @pytest.mark.asyncio
    async def test_process_connection_decodes_and_persists(self):
        """Mapped IP: reader data is decoded and events are persisted."""
        method = TCPServerMethod()
        method._config = TCPServerConfig(
            device_map={"10.0.0.1": "SIG-001"},
            read_timeout_seconds=5,
            buffer_size=4096,
        )
        method._session_factory = AsyncMock()

        payload = b"\x01\x02\x03\x04"
        reader = AsyncMock()
        reader.read = AsyncMock(return_value=payload)
        writer = MagicMock()

        mock_decoder = MagicMock()
        fake_events = [MagicMock(), MagicMock()]
        mock_decoder.decode_bytes.return_value = fake_events

        with (
            patch(
                "tsigma.collection.methods.tcp_server.resolve_decoder_by_name",
                return_value=mock_decoder,
            ) as mock_resolve,
            patch(
                "tsigma.collection.methods.tcp_server.persist_events",
                new_callable=AsyncMock,
            ) as mock_persist,
        ):
            await method._process_connection(reader, writer, "10.0.0.1")

            mock_resolve.assert_called_once()
            mock_decoder.decode_bytes.assert_called_once_with(payload)
            mock_persist.assert_awaited_once_with(
                fake_events, "SIG-001", method._session_factory
            )


# ---------------------------------------------------------------------------
# start / stop lifecycle (lines 129-181)
# ---------------------------------------------------------------------------

class TestTCPServerStartStop:
    """Tests for start() and stop() lifecycle."""

    @pytest.mark.asyncio
    async def test_start_creates_server(self):
        """start() calls asyncio.start_server and stores the server."""
        method = TCPServerMethod()
        mock_server = AsyncMock()
        mock_server.is_serving.return_value = True

        config = {"port": 12345, "device_map": {"10.0.0.1": "SIG-001"}}
        session_factory = AsyncMock()

        with patch("asyncio.start_server", new_callable=AsyncMock) as mock_start:
            mock_start.return_value = mock_server
            await method.start(config, session_factory)

            mock_start.assert_awaited_once()
            assert method._server is mock_server
            assert method._config.port == 12345
            assert method._session_factory is session_factory
            assert method._connection_semaphore is not None

    @pytest.mark.asyncio
    async def test_start_warns_empty_device_map(self):
        """start() with empty device_map logs a warning."""
        method = TCPServerMethod()
        mock_server = AsyncMock()

        with (
            patch("asyncio.start_server", new_callable=AsyncMock, return_value=mock_server),
            patch("tsigma.collection.methods.tcp_server.logger") as mock_logger,
        ):
            await method.start({}, AsyncMock())
            mock_logger.warning.assert_called_once()
            assert "empty device_map" in mock_logger.warning.call_args[0][0]

    @pytest.mark.asyncio
    async def test_stop_closes_server(self):
        """stop() closes the server and sets it to None."""
        method = TCPServerMethod()
        method._config = TCPServerConfig(port=10088)
        mock_server = AsyncMock()
        mock_server.wait_closed = AsyncMock()
        method._server = mock_server

        await method.stop()

        mock_server.close.assert_called_once()
        mock_server.wait_closed.assert_awaited_once()
        assert method._server is None

    @pytest.mark.asyncio
    async def test_stop_noop_when_not_started(self):
        """stop() does nothing if server was never started."""
        method = TCPServerMethod()
        await method.stop()  # should not raise

    @pytest.mark.asyncio
    async def test_stop_handles_timeout(self):
        """stop() handles wait_closed timeout gracefully."""
        method = TCPServerMethod()
        method._config = TCPServerConfig(port=10088)
        method._active_connections = 5
        mock_server = MagicMock()
        mock_server.close = MagicMock()
        mock_server.wait_closed = AsyncMock(side_effect=asyncio.TimeoutError)
        method._server = mock_server

        with patch("asyncio.wait_for", new_callable=AsyncMock, side_effect=asyncio.TimeoutError):
            await method.stop()

        assert method._server is None

    @pytest.mark.asyncio
    async def test_health_check_true_when_serving(self):
        """health_check returns True when server is serving."""
        method = TCPServerMethod()
        mock_server = MagicMock()
        mock_server.is_serving.return_value = True
        method._server = mock_server
        assert await method.health_check() is True


# ---------------------------------------------------------------------------
# _handle_connection (lines 199-224)
# ---------------------------------------------------------------------------

class TestTCPServerHandleConnection:
    """Tests for _handle_connection semaphore and error handling."""

    @staticmethod
    def _make_semaphore(acquire_nowait_returns):
        """Build a mock semaphore with acquire_nowait behavior."""
        sem = MagicMock()
        sem.acquire_nowait.return_value = acquire_nowait_returns
        return sem

    @pytest.mark.asyncio
    async def test_handle_connection_rejects_max(self):
        """Connection rejected when semaphore is full."""
        method = TCPServerMethod()
        method._connection_semaphore = self._make_semaphore(False)

        writer = MagicMock()
        writer.get_extra_info = MagicMock(return_value=("192.168.1.1", 5000))
        writer.close = MagicMock()
        writer.wait_closed = AsyncMock()
        reader = AsyncMock()

        await method._handle_connection(reader, writer)

        writer.close.assert_called_once()
        writer.wait_closed.assert_awaited_once()
        assert method._active_connections == 0
        # Semaphore should NOT have release called (was never acquired)
        method._connection_semaphore.release.assert_not_called()

    @pytest.mark.asyncio
    async def test_handle_connection_processes_and_releases(self):
        """Successful connection increments/decrements counter and releases semaphore."""
        method = TCPServerMethod()
        method._connection_semaphore = self._make_semaphore(True)
        method._config = TCPServerConfig(device_map={})

        writer = MagicMock()
        writer.get_extra_info = MagicMock(return_value=("10.0.0.1", 5000))
        writer.close = MagicMock()
        writer.wait_closed = AsyncMock()
        reader = AsyncMock()

        with patch.object(method, "_process_connection", new_callable=AsyncMock):
            await method._handle_connection(reader, writer)

        assert method._active_connections == 0
        writer.close.assert_called_once()
        writer.wait_closed.assert_awaited_once()
        method._connection_semaphore.release.assert_called_once()

    @pytest.mark.asyncio
    async def test_handle_connection_catches_exception(self):
        """Unhandled error in _process_connection is caught; connection is cleaned up."""
        method = TCPServerMethod()
        method._connection_semaphore = self._make_semaphore(True)

        writer = MagicMock()
        writer.get_extra_info = MagicMock(return_value=("10.0.0.1", 5000))
        writer.close = MagicMock()
        writer.wait_closed = AsyncMock()
        reader = AsyncMock()

        with patch.object(
            method, "_process_connection",
            new_callable=AsyncMock,
            side_effect=RuntimeError("boom"),
        ):
            await method._handle_connection(reader, writer)

        # Connection should still be cleaned up
        assert method._active_connections == 0
        writer.close.assert_called_once()
        method._connection_semaphore.release.assert_called_once()

    @pytest.mark.asyncio
    async def test_handle_connection_unknown_peer(self):
        """Connection with no peername uses 'unknown'."""
        method = TCPServerMethod()
        method._connection_semaphore = self._make_semaphore(True)
        method._config = TCPServerConfig(device_map={})

        writer = MagicMock()
        writer.get_extra_info = MagicMock(return_value=None)
        writer.close = MagicMock()
        writer.wait_closed = AsyncMock()
        reader = AsyncMock()

        with patch.object(method, "_process_connection", new_callable=AsyncMock) as mock_proc:
            await method._handle_connection(reader, writer)
            mock_proc.assert_awaited_once_with(reader, writer, "unknown")
