"""
Unit tests for UDP server ingestion method plugin.

Covers the new contract: Layer-2 server config in ``config`` dict,
per-device routing built from the orchestrator's ``devices`` argument,
event persistence through the ``IngestionTarget``.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tsigma.collection.methods.udp_server import (
    UDPServerConfig,
    UDPServerMethod,
    _UDPProtocol,
)
from tsigma.collection.registry import IngestionMethodRegistry
from tsigma.collection.targets import ControllerTarget, RoadsideTarget


def _devices(*pairs):
    return [
        (device_id, {"host": host, "decoder": "auto"})
        for device_id, host in pairs
    ]


class TestUDPServerRegistration:
    def test_registered(self):
        assert IngestionMethodRegistry.get("udp_server") is UDPServerMethod


class TestUDPServerHealthCheck:
    @pytest.mark.asyncio
    async def test_health_check_not_running(self):
        method = UDPServerMethod()
        assert await method.health_check() is False

    @pytest.mark.asyncio
    async def test_health_check_true_when_transport_active(self):
        method = UDPServerMethod()
        mock_transport = MagicMock()
        mock_transport.is_closing.return_value = False
        method._transport = mock_transport
        assert await method.health_check() is True

    @pytest.mark.asyncio
    async def test_health_check_false_when_transport_closing(self):
        method = UDPServerMethod()
        mock_transport = MagicMock()
        mock_transport.is_closing.return_value = True
        method._transport = mock_transport
        assert await method.health_check() is False


class TestUDPServerBuildConfig:
    def test_build_config_defaults(self):
        config = UDPServerMethod._build_config({})
        assert config.bind_address == "0.0.0.0"
        assert config.port == 10088
        assert config.decoder is None
        assert config.max_packet_size == 65536

    def test_build_config_custom(self):
        config = UDPServerMethod._build_config({
            "bind_address": "192.168.1.10",
            "port": 9999,
            "decoder": "asc3",
            "max_packet_size": 2048,
        })
        assert config.bind_address == "192.168.1.10"
        assert config.port == 9999
        assert config.decoder == "asc3"
        assert config.max_packet_size == 2048


class TestUDPServerBuildDeviceMap:
    def test_device_map_keyed_by_host(self):
        m = UDPServerMethod._build_device_map(
            _devices(("SIG-001", "10.0.0.1"), ("SIG-002", "10.0.0.2")),
        )
        assert m["10.0.0.1"][0] == "SIG-001"
        assert m["10.0.0.2"][0] == "SIG-002"

    def test_device_map_skips_devices_without_host(self):
        m = UDPServerMethod._build_device_map([
            ("SIG-OK", {"host": "10.0.0.1"}),
            ("SIG-NO-HOST", {"host": ""}),
        ])
        assert list(m.keys()) == ["10.0.0.1"]


class TestUDPServerProcessDatagram:
    @pytest.mark.asyncio
    async def test_unmapped_ip_skipped(self):
        method = UDPServerMethod()
        method._config = UDPServerConfig()
        method._device_map = {"10.0.0.1": ("SIG-001", {"decoder": "auto"})}
        method._target = ControllerTarget()
        method._target.persist = AsyncMock()
        method._session_factory = AsyncMock()

        await method._process_datagram(b"\x01\x02", ("192.168.99.99", 5000))
        method._target.persist.assert_not_called()

    @pytest.mark.asyncio
    async def test_mapped_ip_decodes_and_persists_via_target(self):
        method = UDPServerMethod()
        method._config = UDPServerConfig()
        method._device_map = {
            "10.0.0.1": ("SIG-001", {"decoder": "asc3"}),
        }
        method._target = ControllerTarget()
        method._target.persist = AsyncMock()
        method._session_factory = AsyncMock()

        payload = b"\x01\x02\x03\x04"
        mock_decoder = MagicMock()
        fake_events = [MagicMock(), MagicMock()]
        mock_decoder.decode_bytes.return_value = fake_events

        with patch(
            "tsigma.collection.methods.udp_server.resolve_decoder_by_name",
            return_value=mock_decoder,
        ) as mock_resolve:
            await method._process_datagram(payload, ("10.0.0.1", 5000))

        mock_resolve.assert_called_once_with("asc3")
        mock_decoder.decode_bytes.assert_called_once_with(payload)
        method._target.persist.assert_awaited_once_with(
            fake_events, "SIG-001", method._session_factory,
        )

    @pytest.mark.asyncio
    async def test_empty_datagram_short_circuits(self):
        method = UDPServerMethod()
        method._config = UDPServerConfig()
        method._device_map = {"10.0.0.1": ("SIG-001", {})}
        method._target = ControllerTarget()
        method._target.persist = AsyncMock()
        method._session_factory = AsyncMock()

        await method._process_datagram(b"", ("10.0.0.1", 5000))
        method._target.persist.assert_not_called()


class TestUDPServerStartStop:
    @pytest.mark.asyncio
    async def test_start_creates_endpoint_and_routes_devices(self):
        method = UDPServerMethod()
        mock_transport = MagicMock()
        mock_protocol = MagicMock()

        mock_loop = MagicMock()
        mock_loop.create_datagram_endpoint = AsyncMock(
            return_value=(mock_transport, mock_protocol),
        )

        with patch("asyncio.get_running_loop", return_value=mock_loop):
            await method.start(
                {"port": 12345},
                AsyncMock(),
                target=ControllerTarget(),
                devices=_devices(("SIG-001", "10.0.0.1")),
            )

        assert method._transport is mock_transport
        assert method._protocol is mock_protocol
        assert method._config.port == 12345
        assert method._device_map["10.0.0.1"][0] == "SIG-001"

    @pytest.mark.asyncio
    async def test_start_warns_when_no_routable_devices(self):
        method = UDPServerMethod()
        mock_transport = MagicMock()
        mock_protocol = MagicMock()
        mock_loop = MagicMock()
        mock_loop.create_datagram_endpoint = AsyncMock(
            return_value=(mock_transport, mock_protocol),
        )

        with (
            patch("asyncio.get_running_loop", return_value=mock_loop),
            patch("tsigma.collection.methods.udp_server.logger") as mock_logger,
        ):
            await method.start({}, AsyncMock(), target=ControllerTarget(), devices=[])
            mock_logger.warning.assert_called_once()
            assert "no resolvable devices" in mock_logger.warning.call_args[0][0]

    @pytest.mark.asyncio
    async def test_start_with_roadside_target(self):
        method = UDPServerMethod()
        target = RoadsideTarget()
        mock_loop = MagicMock()
        mock_loop.create_datagram_endpoint = AsyncMock(
            return_value=(MagicMock(), MagicMock()),
        )
        with patch("asyncio.get_running_loop", return_value=mock_loop):
            await method.start(
                {}, AsyncMock(),
                target=target,
                devices=_devices(("SENSOR-1", "10.0.0.50")),
            )
        assert method._target is target

    @pytest.mark.asyncio
    async def test_stop_closes_transport(self):
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
        method = UDPServerMethod()
        await method.stop()  # should not raise


class TestUDPProtocol:
    def test_datagram_received_schedules_task(self):
        server = MagicMock()
        protocol = _UDPProtocol(server)

        with patch("asyncio.ensure_future") as mock_ef:
            protocol.datagram_received(b"\x01\x02", ("10.0.0.1", 5000))
            mock_ef.assert_called_once()
            server._process_datagram.assert_called_once_with(
                b"\x01\x02", ("10.0.0.1", 5000),
            )

    def test_error_received_logs(self):
        server = MagicMock()
        protocol = _UDPProtocol(server)
        with patch("tsigma.collection.methods.udp_server.logger") as mock_logger:
            protocol.error_received(OSError("bind failed"))
            mock_logger.error.assert_called_once()
            assert "UDP transport error" in mock_logger.error.call_args[0][0]

    def test_connection_lost_logs_on_error(self):
        server = MagicMock()
        protocol = _UDPProtocol(server)
        with patch("tsigma.collection.methods.udp_server.logger") as mock_logger:
            protocol.connection_lost(OSError("reset"))
            mock_logger.error.assert_called_once()
            assert "closed with error" in mock_logger.error.call_args[0][0]

    def test_connection_lost_silent_on_normal_close(self):
        server = MagicMock()
        protocol = _UDPProtocol(server)
        with patch("tsigma.collection.methods.udp_server.logger") as mock_logger:
            protocol.connection_lost(None)
            mock_logger.error.assert_not_called()
