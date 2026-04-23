"""
Unit tests for UDP server ingestion method plugin.

Tests registration, health checks, config building,
and datagram processing with mocked asyncio and decoder dependencies.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tsigma.collection.methods.udp_server import (
    UDPServerConfig,
    UDPServerMethod,
    _UDPProtocol,
)
from tsigma.collection.registry import IngestionMethodRegistry


class TestUDPServerRegistration:
    """Tests for registry integration."""

    def test_registered(self):
        """IngestionMethodRegistry.get('udp_server') returns UDPServerMethod."""
        assert IngestionMethodRegistry.get("udp_server") is UDPServerMethod


class TestUDPServerHealthCheck:
    """Tests for health_check behaviour."""

    @pytest.mark.asyncio
    async def test_health_check_not_running(self):
        """health_check returns False when transport not started."""
        method = UDPServerMethod()
        assert await method.health_check() is False


class TestUDPServerBuildConfig:
    """Tests for _build_config."""

    def test_build_config_defaults(self):
        """_build_config with empty dict returns correct defaults."""
        config = UDPServerMethod._build_config({})

        assert config.bind_address == "0.0.0.0"
        assert config.port == 10088
        assert config.decoder is None
        assert config.max_packet_size == 65536
        assert config.device_map == {}

    def test_build_config_custom(self):
        """_build_config with all fields populated."""
        raw = {
            "bind_address": "192.168.1.10",
            "port": 9999,
            "decoder": "asc3",
            "max_packet_size": 2048,
            "device_map": {"10.0.0.1": "SIG-001"},
        }
        config = UDPServerMethod._build_config(raw)

        assert config.bind_address == "192.168.1.10"
        assert config.port == 9999
        assert config.decoder == "asc3"
        assert config.max_packet_size == 2048
        assert config.device_map == {"10.0.0.1": "SIG-001"}


class TestUDPServerProcessDatagram:
    """Tests for _process_datagram."""

    @pytest.mark.asyncio
    async def test_process_datagram_unmapped_ip(self):
        """Datagram from IP not in device_map is skipped (no persist)."""
        method = UDPServerMethod()
        method._config = UDPServerConfig(
            device_map={"10.0.0.1": "SIG-001"},
        )
        method._session_factory = AsyncMock()

        with patch(
            "tsigma.collection.methods.udp_server.persist_events"
        ) as mock_persist:
            await method._process_datagram(b"\x01\x02", ("192.168.99.99", 5000))
            mock_persist.assert_not_called()

    @pytest.mark.asyncio
    async def test_process_datagram_decodes_and_persists(self):
        """Mapped IP: datagram is decoded and events are persisted."""
        method = UDPServerMethod()
        method._config = UDPServerConfig(
            device_map={"10.0.0.1": "SIG-001"},
        )
        method._session_factory = AsyncMock()

        payload = b"\x01\x02\x03\x04"

        mock_decoder = MagicMock()
        fake_events = [MagicMock(), MagicMock()]
        mock_decoder.decode_bytes.return_value = fake_events

        with (
            patch(
                "tsigma.collection.methods.udp_server.resolve_decoder_by_name",
                return_value=mock_decoder,
            ) as mock_resolve,
            patch(
                "tsigma.collection.methods.udp_server.persist_events",
                new_callable=AsyncMock,
            ) as mock_persist,
        ):
            await method._process_datagram(payload, ("10.0.0.1", 5000))

            mock_resolve.assert_called_once()
            mock_decoder.decode_bytes.assert_called_once_with(payload)
            mock_persist.assert_awaited_once_with(
                fake_events, "SIG-001", method._session_factory
            )


# ---------------------------------------------------------------------------
# start / stop lifecycle
# ---------------------------------------------------------------------------

class TestUDPServerStartStop:
    """Tests for start() and stop() lifecycle."""

    @pytest.mark.asyncio
    async def test_start_creates_endpoint(self):
        """start() creates a datagram endpoint and stores transport/protocol."""
        method = UDPServerMethod()
        mock_transport = MagicMock()
        mock_protocol = MagicMock()

        config = {"port": 12345, "device_map": {"10.0.0.1": "SIG-001"}}
        session_factory = AsyncMock()

        mock_loop = AsyncMock()
        mock_loop.create_datagram_endpoint = AsyncMock(
            return_value=(mock_transport, mock_protocol)
        )

        with patch("asyncio.get_running_loop", return_value=mock_loop):
            await method.start(config, session_factory)

        assert method._transport is mock_transport
        assert method._protocol is mock_protocol
        assert method._config.port == 12345
        assert method._session_factory is session_factory

    @pytest.mark.asyncio
    async def test_start_warns_empty_device_map(self):
        """start() with empty device_map logs a warning."""
        method = UDPServerMethod()
        mock_transport = MagicMock()
        mock_protocol = MagicMock()

        mock_loop = AsyncMock()
        mock_loop.create_datagram_endpoint = AsyncMock(
            return_value=(mock_transport, mock_protocol)
        )

        with (
            patch("asyncio.get_running_loop", return_value=mock_loop),
            patch("tsigma.collection.methods.udp_server.logger") as mock_logger,
        ):
            await method.start({}, AsyncMock())
            mock_logger.warning.assert_called_once()
            assert "empty device_map" in mock_logger.warning.call_args[0][0]

    @pytest.mark.asyncio
    async def test_stop_closes_transport(self):
        """stop() closes transport and sets it to None."""
        method = UDPServerMethod()
        method._config = UDPServerConfig(port=10088)
        mock_transport = MagicMock()
        mock_protocol = MagicMock()
        method._transport = mock_transport
        method._protocol = mock_protocol

        await method.stop()

        mock_transport.close.assert_called_once()
        assert method._transport is None
        assert method._protocol is None

    @pytest.mark.asyncio
    async def test_stop_noop_when_not_started(self):
        """stop() does nothing if transport was never started."""
        method = UDPServerMethod()
        await method.stop()  # should not raise

    @pytest.mark.asyncio
    async def test_health_check_true_when_transport_active(self):
        """health_check returns True when transport is not closing."""
        method = UDPServerMethod()
        mock_transport = MagicMock()
        mock_transport.is_closing.return_value = False
        method._transport = mock_transport
        assert await method.health_check() is True

    @pytest.mark.asyncio
    async def test_health_check_false_when_transport_closing(self):
        """health_check returns False when transport is closing."""
        method = UDPServerMethod()
        mock_transport = MagicMock()
        mock_transport.is_closing.return_value = True
        method._transport = mock_transport
        assert await method.health_check() is False


# ---------------------------------------------------------------------------
# _UDPProtocol
# ---------------------------------------------------------------------------

class TestUDPProtocol:
    """Tests for the internal _UDPProtocol class."""

    def test_datagram_received_schedules_task(self):
        """datagram_received schedules _process_datagram via ensure_future."""
        server = MagicMock()
        protocol = _UDPProtocol(server)

        mock_future = MagicMock()
        with patch("asyncio.ensure_future", return_value=mock_future) as mock_ef:
            protocol.datagram_received(b"\x01\x02", ("10.0.0.1", 5000))
            mock_ef.assert_called_once()
            # The argument should be the coroutine from _process_datagram
            server._process_datagram.assert_called_once_with(
                b"\x01\x02", ("10.0.0.1", 5000)
            )

    def test_error_received_logs(self):
        """error_received logs the transport error."""
        server = MagicMock()
        protocol = _UDPProtocol(server)

        with patch("tsigma.collection.methods.udp_server.logger") as mock_logger:
            protocol.error_received(OSError("bind failed"))
            mock_logger.error.assert_called_once()
            assert "UDP transport error" in mock_logger.error.call_args[0][0]

    def test_connection_lost_logs_on_error(self):
        """connection_lost logs when closed with an error."""
        server = MagicMock()
        protocol = _UDPProtocol(server)

        with patch("tsigma.collection.methods.udp_server.logger") as mock_logger:
            protocol.connection_lost(OSError("reset"))
            mock_logger.error.assert_called_once()
            assert "closed with error" in mock_logger.error.call_args[0][0]

    def test_connection_lost_silent_on_normal_close(self):
        """connection_lost does not log on normal shutdown (exc=None)."""
        server = MagicMock()
        protocol = _UDPProtocol(server)

        with patch("tsigma.collection.methods.udp_server.logger") as mock_logger:
            protocol.connection_lost(None)
            mock_logger.error.assert_not_called()
