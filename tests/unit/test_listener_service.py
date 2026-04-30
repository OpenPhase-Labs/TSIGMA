"""
Unit tests for ``tsigma.collection.listener_service.ListenerService``.

Covers: enable-flag gating (umbrella + per-method), Layer-2 config
construction, instance discriminator filtering for MQTT/NATS,
per-source dispatch, error-tolerant start/stop, health aggregation.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from tsigma.collection.listener_service import ListenerService
from tsigma.collection.registry import (
    IngestionMethodRegistry,
    ListenerIngestionMethod,
)
from tsigma.collection.targets import ControllerTarget


def _settings(**overrides):
    """Build a minimal settings namespace covering every flag the orchestrator reads."""
    base = dict(
        enable_listeners=False,
        enable_tcp_listener=False,
        enable_udp_listener=False,
        enable_grpc_listener=False,
        enable_mqtt_listener=False,
        enable_nats_listener=False,
        enable_directory_watch=False,
        # TCP
        tcp_bind_host="0.0.0.0", tcp_bind_port=10088,
        tcp_max_connections=2000, tcp_idle_timeout=300,
        tcp_read_buffer_size=65536, tcp_decoder="",
        # UDP
        udp_bind_host="0.0.0.0", udp_bind_port=10088,
        udp_max_packet_size=4096, udp_decoder="",
        # gRPC
        grpc_bind_host="0.0.0.0", grpc_bind_port=50051,
        grpc_tls_cert_file="", grpc_tls_key_file="",
        grpc_max_message_size=4194304,
        # MQTT
        mqtt_broker_url="", mqtt_client_id="tsigma-test",
        mqtt_username="", mqtt_username_file="",
        mqtt_password="", mqtt_password_file="",
        mqtt_keepalive=60, mqtt_tls=False,
        mqtt_instance="default",
        # NATS
        nats_url="", nats_credentials_file="",
        nats_tls=False, nats_max_reconnects=-1,
        nats_instance="default",
        # Directory watch
        directory_watch_paths="", directory_watch_patterns="*",
        directory_watch_decoder="auto",
        # Polling cadence (only used by SignalDeviceSource defaults)
        collector_poll_interval=900, sensor_poll_interval=900,
    )
    base.update(overrides)
    return SimpleNamespace(**base)


def _fake_source(device_type="controller", devices=None):
    """Build a DeviceSource-shaped MagicMock returning a fixed device list."""
    src = MagicMock()
    src.device_type = device_type
    src.target = ControllerTarget()
    src.list_devices_for_method = AsyncMock(return_value=devices or [])
    return src


class TestIsEnabled:
    def test_umbrella_enables_all(self):
        svc = ListenerService(MagicMock(), _settings(enable_listeners=True))
        for name in (
            "tcp_server", "udp_server", "grpc_server",
            "mqtt_listener", "nats_listener", "directory_watch",
        ):
            assert svc._is_enabled(name) is True

    def test_per_method_enables_only_that_one(self):
        svc = ListenerService(
            MagicMock(),
            _settings(enable_mqtt_listener=True),
        )
        assert svc._is_enabled("mqtt_listener") is True
        assert svc._is_enabled("nats_listener") is False
        assert svc._is_enabled("tcp_server") is False

    def test_neither_flag_disables_everything(self):
        svc = ListenerService(MagicMock(), _settings())
        assert svc._is_enabled("mqtt_listener") is False


class TestInstanceFilter:
    def test_only_mqtt_and_nats_are_instance_aware(self):
        svc = ListenerService(MagicMock(), _settings(mqtt_instance="cloud"))
        assert svc._instance_for("mqtt_listener") == "cloud"
        assert svc._instance_for("nats_listener") == "default"
        assert svc._instance_for("tcp_server") is None
        assert svc._instance_for("directory_watch") is None


class TestBuildLayer2Config:
    def test_tcp_layer2_pulls_from_settings(self):
        svc = ListenerService(
            MagicMock(),
            _settings(tcp_bind_host="127.0.0.1", tcp_bind_port=11111),
        )
        cfg = svc._build_layer2_config("tcp_server")
        assert cfg["bind_address"] == "127.0.0.1"
        assert cfg["port"] == 11111
        assert cfg["max_connections"] == 2000
        assert cfg["read_timeout_seconds"] == 300
        assert cfg["decoder"] is None  # empty string normalized to None

    def test_udp_layer2(self):
        svc = ListenerService(
            MagicMock(),
            _settings(udp_bind_port=12222, udp_decoder="wavetronix_advance"),
        )
        cfg = svc._build_layer2_config("udp_server")
        assert cfg["port"] == 12222
        assert cfg["decoder"] == "wavetronix_advance"

    def test_mqtt_layer2(self):
        svc = ListenerService(
            MagicMock(),
            _settings(
                mqtt_broker_url="mqtts://broker:8883",
                mqtt_username="u",
                mqtt_password="p",
                mqtt_instance="cloud",
            ),
        )
        cfg = svc._build_layer2_config("mqtt_listener")
        assert cfg["broker_url"] == "mqtts://broker:8883"
        assert cfg["username"] == "u"
        assert cfg["password"] == "p"
        assert cfg["instance"] == "cloud"

    def test_nats_layer2(self):
        svc = ListenerService(
            MagicMock(),
            _settings(
                nats_url="nats://server:4222",
                nats_credentials_file="/run/secrets/nats.creds",
                nats_instance="internal",
            ),
        )
        cfg = svc._build_layer2_config("nats_listener")
        assert cfg["url"] == "nats://server:4222"
        assert cfg["credentials_file"] == "/run/secrets/nats.creds"
        assert cfg["instance"] == "internal"

    def test_directory_watch_layer2_splits_csv_lists(self):
        svc = ListenerService(
            MagicMock(),
            _settings(
                directory_watch_paths="/var/lib/a, /var/lib/b",
                directory_watch_patterns="*.dat,*.csv",
            ),
        )
        cfg = svc._build_layer2_config("directory_watch")
        assert cfg["paths"] == ["/var/lib/a", "/var/lib/b"]
        assert cfg["patterns"] == ["*.dat", "*.csv"]


class TestMatchedDevices:
    @pytest.mark.asyncio
    async def test_no_filter_when_method_not_instance_aware(self):
        session_factory = MagicMock()
        # Async-context-manager wrapper around a MagicMock session.
        session_factory.return_value.__aenter__ = AsyncMock(
            return_value=MagicMock(),
        )
        session_factory.return_value.__aexit__ = AsyncMock(return_value=False)

        svc = ListenerService(session_factory, _settings())
        source = _fake_source(devices=[
            ("DEV-A", {"instance": "cloud"}),
            ("DEV-B", {}),
        ])
        result = await svc._matched_devices(source, "tcp_server", None)
        # No instance filter applied — all devices returned.
        assert len(result) == 2

    @pytest.mark.asyncio
    async def test_filters_by_instance_when_aware(self):
        session_factory = MagicMock()
        session_factory.return_value.__aenter__ = AsyncMock(
            return_value=MagicMock(),
        )
        session_factory.return_value.__aexit__ = AsyncMock(return_value=False)

        svc = ListenerService(session_factory, _settings())
        source = _fake_source(devices=[
            ("DEV-CLOUD", {"instance": "cloud"}),
            ("DEV-INTERNAL", {"instance": "internal"}),
            ("DEV-DEFAULT", {}),  # missing instance → counts as "default"
        ])

        cloud = await svc._matched_devices(source, "mqtt_listener", "cloud")
        assert [d[0] for d in cloud] == ["DEV-CLOUD"]

        default = await svc._matched_devices(source, "mqtt_listener", "default")
        assert [d[0] for d in default] == ["DEV-DEFAULT"]


class TestStartStop:
    """End-to-end orchestrator behavior with synthetic plugins."""

    @pytest.fixture(autouse=True)
    def _isolate_registry(self):
        """Snapshot the registry so synthetic plugins don't leak."""
        original = dict(IngestionMethodRegistry._methods)
        yield
        IngestionMethodRegistry._methods = original

    def _make_listener_class(self, name, started_log):
        # Concrete subclass with real (non-abstract) implementations that
        # delegate to per-instance AsyncMocks the test can introspect.
        class _Synth(ListenerIngestionMethod):
            pass

        _Synth.name = name
        _Synth.__name__ = f"Synth_{name}"

        async def _start(self, config, session_factory, *, target=None, devices=None):
            return await self._mock_start(
                config, session_factory, target=target, devices=devices,
            )

        async def _stop(self):
            return await self._mock_stop()

        async def _health_check(self):
            return await self._mock_health_check()

        def _init(self):
            self._mock_start = AsyncMock()
            self._mock_stop = AsyncMock()
            self._mock_health_check = AsyncMock(return_value=True)
            started_log.append(self)

        _Synth.start = _start
        _Synth.stop = _stop
        _Synth.health_check = _health_check
        _Synth.__init__ = _init
        # Methods are now concrete — clear ABC's abstract-method set so
        # the class is instantiable.  __abstractmethods__ is computed at
        # class-creation time and isn't refreshed by later attribute
        # assignment.
        _Synth.__abstractmethods__ = frozenset()
        IngestionMethodRegistry._methods[name] = _Synth
        return _Synth

    @pytest.mark.asyncio
    async def test_start_skips_disabled_methods(self):
        # No flag set → service starts nothing.
        instances: list = []
        self._make_listener_class("tcp_server", instances)

        sf = MagicMock()
        sf.return_value.__aenter__ = AsyncMock(return_value=MagicMock())
        sf.return_value.__aexit__ = AsyncMock(return_value=False)

        svc = ListenerService(
            sf, _settings(),
            sources=[_fake_source(devices=[("DEV-1", {"host": "10.0.0.1"})])],
        )
        await svc.start()
        assert not svc._started

    @pytest.mark.asyncio
    async def test_start_skips_methods_with_no_matching_devices(self):
        instances: list = []
        self._make_listener_class("tcp_server", instances)

        sf = MagicMock()
        sf.return_value.__aenter__ = AsyncMock(return_value=MagicMock())
        sf.return_value.__aexit__ = AsyncMock(return_value=False)

        svc = ListenerService(
            sf, _settings(enable_tcp_listener=True),
            sources=[_fake_source(devices=[])],
        )
        await svc.start()
        # No instance constructed — zero devices means we skip.
        assert not svc._started
        assert instances == []

    @pytest.mark.asyncio
    async def test_start_per_source_dispatch(self):
        instances: list = []
        self._make_listener_class("tcp_server", instances)

        sf = MagicMock()
        sf.return_value.__aenter__ = AsyncMock(return_value=MagicMock())
        sf.return_value.__aexit__ = AsyncMock(return_value=False)

        signal_source = _fake_source(
            "controller", [("SIG-1", {"host": "10.0.0.1"})],
        )
        sensor_source = _fake_source(
            "sensor", [("SENSOR-1", {"host": "10.0.0.50"})],
        )
        svc = ListenerService(
            sf, _settings(enable_tcp_listener=True),
            sources=[signal_source, sensor_source],
        )
        await svc.start()
        # One method instance per source.
        assert len(svc._started) == 2

    @pytest.mark.asyncio
    async def test_start_swallows_per_method_start_failures(self):
        instances: list = []
        synth = self._make_listener_class("tcp_server", instances)

        # Override the class-level start to raise — every instance will fail.
        async def _boom(self, config, session_factory, *, target=None, devices=None):
            raise RuntimeError("boom")

        synth.start = _boom

        sf = MagicMock()
        sf.return_value.__aenter__ = AsyncMock(return_value=MagicMock())
        sf.return_value.__aexit__ = AsyncMock(return_value=False)

        svc = ListenerService(
            sf, _settings(enable_tcp_listener=True),
            sources=[_fake_source(devices=[("DEV-1", {"host": "10.0.0.1"})])],
        )
        # Should not raise; failed instance not added to _started.
        await svc.start()
        assert not svc._started

    @pytest.mark.asyncio
    async def test_stop_calls_each_instance_stop(self):
        instances: list = []
        self._make_listener_class("tcp_server", instances)

        sf = MagicMock()
        sf.return_value.__aenter__ = AsyncMock(return_value=MagicMock())
        sf.return_value.__aexit__ = AsyncMock(return_value=False)

        svc = ListenerService(
            sf, _settings(enable_tcp_listener=True),
            sources=[_fake_source(devices=[("DEV-1", {"host": "10.0.0.1"})])],
        )
        await svc.start()
        assert len(svc._started) == 1
        started_instance = svc._started[0]

        await svc.stop()
        # Real ``.stop`` delegates to the per-instance mock.
        started_instance._mock_stop.assert_awaited_once()
        assert not svc._started

    @pytest.mark.asyncio
    async def test_stop_swallows_individual_stop_failures(self):
        instances: list = []
        synth = self._make_listener_class("tcp_server", instances)

        sf = MagicMock()
        sf.return_value.__aenter__ = AsyncMock(return_value=MagicMock())
        sf.return_value.__aexit__ = AsyncMock(return_value=False)

        svc = ListenerService(
            sf, _settings(enable_tcp_listener=True),
            sources=[_fake_source(devices=[("DEV-1", {"host": "10.0.0.1"})])],
        )
        await svc.start()
        # Make this instance's stop blow up — orchestrator must still finish.
        svc._started[0].stop = AsyncMock(side_effect=RuntimeError("stop boom"))

        await svc.stop()  # should not raise
        assert not svc._started


class TestHealthCheck:
    @pytest.fixture(autouse=True)
    def _isolate_registry(self):
        original = dict(IngestionMethodRegistry._methods)
        yield
        IngestionMethodRegistry._methods = original

    @pytest.mark.asyncio
    async def test_health_aggregates_per_started_instance(self):
        # Distinct types so each instance lands on its own dict key.
        class _OK:
            async def health_check(self): return True
        class _Bad:
            async def health_check(self): return False
        class _Boom:
            async def health_check(self): raise RuntimeError("nope")

        svc = ListenerService(MagicMock(), _settings())
        svc._started = [_OK(), _Bad(), _Boom()]

        result = await svc.health_check()
        assert result["_OK"] is True
        assert result["_Bad"] is False
        assert result["_Boom"] is False  # raised → coerced to False
