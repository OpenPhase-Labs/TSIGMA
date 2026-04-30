"""
Unit tests for TCP server ingestion method plugin.

Covers the new contract: Layer-2 server config in ``config`` dict,
per-device routing built from the orchestrator's ``devices`` argument,
event persistence through the ``IngestionTarget``.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tsigma.collection.methods.tcp_server import (
    TCPServerConfig,
    TCPServerMethod,
)
from tsigma.collection.registry import IngestionMethodRegistry
from tsigma.collection.targets import ControllerTarget, RoadsideTarget


def _devices(*pairs):
    """Helper: turn (device_id, host) tuples into the orchestrator shape."""
    return [
        (device_id, {"host": host, "decoder": "auto"})
        for device_id, host in pairs
    ]


class TestTCPServerRegistration:
    def test_registered(self):
        assert IngestionMethodRegistry.get("tcp_server") is TCPServerMethod


class TestTCPServerHealthCheck:
    @pytest.mark.asyncio
    async def test_health_check_not_running(self):
        method = TCPServerMethod()
        assert await method.health_check() is False

    @pytest.mark.asyncio
    async def test_health_check_true_when_serving(self):
        method = TCPServerMethod()
        mock_server = MagicMock()
        mock_server.is_serving.return_value = True
        method._server = mock_server
        assert await method.health_check() is True


class TestTCPServerBuildConfig:
    def test_build_config_defaults(self):
        config = TCPServerMethod._build_config({})
        assert config.bind_address == "0.0.0.0"
        assert config.port == 10088
        assert config.decoder is None
        assert config.max_connections == 100
        assert config.read_timeout_seconds == 30
        assert config.buffer_size == 65536

    def test_build_config_custom(self):
        config = TCPServerMethod._build_config({
            "bind_address": "192.168.1.10",
            "port": 9999,
            "decoder": "asc3",
            "max_connections": 50,
            "read_timeout_seconds": 10,
            "buffer_size": 4096,
        })
        assert config.bind_address == "192.168.1.10"
        assert config.port == 9999
        assert config.decoder == "asc3"
        assert config.max_connections == 50
        assert config.read_timeout_seconds == 10
        assert config.buffer_size == 4096


class TestTCPServerBuildDeviceMap:
    """The IP→device_id map is built from the devices argument at start."""

    def test_device_map_keyed_by_host(self):
        m = TCPServerMethod._build_device_map(
            _devices(("SIG-001", "10.0.0.1"), ("SIG-002", "10.0.0.2")),
        )
        assert "10.0.0.1" in m
        assert m["10.0.0.1"][0] == "SIG-001"
        assert "10.0.0.2" in m
        assert m["10.0.0.2"][0] == "SIG-002"

    def test_device_map_skips_devices_without_host(self):
        m = TCPServerMethod._build_device_map([
            ("SIG-OK", {"host": "10.0.0.1"}),
            ("SIG-NO-HOST", {"host": ""}),
            ("SIG-NULL", {}),
        ])
        assert list(m.keys()) == ["10.0.0.1"]


class TestTCPServerProcessConnection:
    @pytest.mark.asyncio
    async def test_unmapped_ip_skipped(self):
        method = TCPServerMethod()
        method._config = TCPServerConfig()
        method._device_map = {"10.0.0.1": ("SIG-001", {"decoder": "auto"})}
        method._target = ControllerTarget()
        method._session_factory = AsyncMock()

        method._target.persist = AsyncMock()
        await method._process_connection(AsyncMock(), "192.168.99.99")
        method._target.persist.assert_not_called()

    @pytest.mark.asyncio
    async def test_mapped_ip_decodes_and_persists_via_target(self):
        method = TCPServerMethod()
        method._config = TCPServerConfig(read_timeout_seconds=5, buffer_size=4096)
        method._device_map = {
            "10.0.0.1": ("SIG-001", {"decoder": "asc3"}),
        }
        method._target = ControllerTarget()
        method._target.persist = AsyncMock()
        method._session_factory = AsyncMock()

        payload = b"\x01\x02\x03\x04"
        reader = AsyncMock()
        reader.read = AsyncMock(return_value=payload)

        mock_decoder = MagicMock()
        fake_events = [MagicMock(), MagicMock()]
        mock_decoder.decode_bytes.return_value = fake_events

        with patch(
            "tsigma.collection.methods.tcp_server.resolve_decoder_by_name",
            return_value=mock_decoder,
        ) as mock_resolve:
            await method._process_connection(reader, "10.0.0.1")

        mock_resolve.assert_called_once_with("asc3")
        mock_decoder.decode_bytes.assert_called_once_with(payload)
        method._target.persist.assert_awaited_once_with(
            fake_events, "SIG-001", method._session_factory,
        )

    @pytest.mark.asyncio
    async def test_per_device_decoder_overrides_server_default(self):
        method = TCPServerMethod()
        method._config = TCPServerConfig(decoder="server-default")
        method._device_map = {
            "10.0.0.1": ("SIG-001", {"decoder": "device-specific"}),
        }
        method._target = ControllerTarget()
        method._target.persist = AsyncMock()
        method._session_factory = AsyncMock()

        reader = AsyncMock()
        reader.read = AsyncMock(return_value=b"\x01")

        with patch(
            "tsigma.collection.methods.tcp_server.resolve_decoder_by_name",
            return_value=MagicMock(decode_bytes=MagicMock(return_value=[])),
        ) as mock_resolve:
            await method._process_connection(reader, "10.0.0.1")
        mock_resolve.assert_called_once_with("device-specific")


class TestTCPServerStartStop:
    @pytest.mark.asyncio
    async def test_start_creates_server_and_routes_devices(self):
        method = TCPServerMethod()
        mock_server = AsyncMock()
        mock_server.is_serving.return_value = True

        with patch(
            "asyncio.start_server", new_callable=AsyncMock,
            return_value=mock_server,
        ) as mock_start:
            await method.start(
                {"port": 12345},
                AsyncMock(),
                target=ControllerTarget(),
                devices=_devices(("SIG-001", "10.0.0.1")),
            )

        mock_start.assert_awaited_once()
        assert method._server is mock_server
        assert method._config.port == 12345
        assert "10.0.0.1" in method._device_map
        assert method._device_map["10.0.0.1"][0] == "SIG-001"

    @pytest.mark.asyncio
    async def test_start_warns_when_no_routable_devices(self):
        method = TCPServerMethod()
        mock_server = AsyncMock()

        with (
            patch(
                "asyncio.start_server", new_callable=AsyncMock,
                return_value=mock_server,
            ),
            patch("tsigma.collection.methods.tcp_server.logger") as mock_logger,
        ):
            await method.start({}, AsyncMock(), target=ControllerTarget(), devices=[])
            mock_logger.warning.assert_called_once()
            assert "no resolvable devices" in mock_logger.warning.call_args[0][0]

    @pytest.mark.asyncio
    async def test_start_defaults_target_to_controller(self):
        method = TCPServerMethod()
        with patch(
            "asyncio.start_server", new_callable=AsyncMock, return_value=AsyncMock(),
        ):
            await method.start({}, AsyncMock())
        assert isinstance(method._target, ControllerTarget)

    @pytest.mark.asyncio
    async def test_start_with_roadside_target(self):
        method = TCPServerMethod()
        target = RoadsideTarget()
        with patch(
            "asyncio.start_server", new_callable=AsyncMock, return_value=AsyncMock(),
        ):
            await method.start(
                {}, AsyncMock(),
                target=target,
                devices=_devices(("SENSOR-1", "10.0.0.50")),
            )
        assert method._target is target
        assert method._target.device_type == "sensor"

    @pytest.mark.asyncio
    async def test_stop_closes_server(self):
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
        method = TCPServerMethod()
        await method.stop()  # should not raise

    @pytest.mark.asyncio
    async def test_stop_handles_timeout(self):
        method = TCPServerMethod()
        method._config = TCPServerConfig(port=10088)
        method._active_connections = 5
        mock_server = MagicMock()
        mock_server.close = MagicMock()
        mock_server.wait_closed = AsyncMock(side_effect=asyncio.TimeoutError)
        method._server = mock_server

        with patch(
            "asyncio.wait_for", new_callable=AsyncMock,
            side_effect=asyncio.TimeoutError,
        ):
            await method.stop()
        assert method._server is None


class TestTCPServerHandleConnection:
    @staticmethod
    def _make_semaphore(acquire_returns):
        sem = MagicMock()
        sem.acquire_nowait.return_value = acquire_returns
        return sem

    @pytest.mark.asyncio
    async def test_handle_connection_rejects_max(self):
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
        method._connection_semaphore.release.assert_not_called()

    @pytest.mark.asyncio
    async def test_handle_connection_processes_and_releases(self):
        method = TCPServerMethod()
        method._connection_semaphore = self._make_semaphore(True)
        method._config = TCPServerConfig()

        writer = MagicMock()
        writer.get_extra_info = MagicMock(return_value=("10.0.0.1", 5000))
        writer.close = MagicMock()
        writer.wait_closed = AsyncMock()
        reader = AsyncMock()

        with patch.object(method, "_process_connection", new_callable=AsyncMock):
            await method._handle_connection(reader, writer)

        assert method._active_connections == 0
        writer.close.assert_called_once()
        method._connection_semaphore.release.assert_called_once()

    @pytest.mark.asyncio
    async def test_handle_connection_catches_exception(self):
        method = TCPServerMethod()
        method._connection_semaphore = self._make_semaphore(True)

        writer = MagicMock()
        writer.get_extra_info = MagicMock(return_value=("10.0.0.1", 5000))
        writer.close = MagicMock()
        writer.wait_closed = AsyncMock()
        reader = AsyncMock()

        with patch.object(
            method, "_process_connection",
            new_callable=AsyncMock, side_effect=RuntimeError("boom"),
        ):
            await method._handle_connection(reader, writer)

        assert method._active_connections == 0
        writer.close.assert_called_once()
        method._connection_semaphore.release.assert_called_once()

    @pytest.mark.asyncio
    async def test_handle_connection_unknown_peer(self):
        method = TCPServerMethod()
        method._connection_semaphore = self._make_semaphore(True)
        method._config = TCPServerConfig()

        writer = MagicMock()
        writer.get_extra_info = MagicMock(return_value=None)
        writer.close = MagicMock()
        writer.wait_closed = AsyncMock()
        reader = AsyncMock()

        with patch.object(
            method, "_process_connection", new_callable=AsyncMock,
        ) as mock_proc:
            await method._handle_connection(reader, writer)
            mock_proc.assert_awaited_once_with(reader, "unknown")
