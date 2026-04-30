"""
Unit tests for MQTT listener ingestion method plugin.

Covers the new contract: Layer-2 server config (broker URL, credentials,
TLS, instance) sourced from process env via ``ListenerService``;
per-device subscriptions (topic, qos, decoder) come from the orchestrator
``devices`` argument; events persisted through the ``IngestionTarget``.
"""

import asyncio
import logging
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tsigma.collection.methods.mqtt_listener import (
    MQTTListenerMethod,
    MQTTServerConfig,
    MQTTSubscription,
)
from tsigma.collection.registry import IngestionMethodRegistry
from tsigma.collection.targets import ControllerTarget


def _server_config(**overrides):
    base = {
        "broker_url": "mqtt://localhost:1883",
        "client_id": "tsigma-test",
        "username": None,
        "password": None,
        "keepalive": 60,
        "tls": False,
        "instance": "default",
    }
    base.update(overrides)
    return base


class TestMQTTListenerRegistration:
    def test_registered(self):
        assert IngestionMethodRegistry.get("mqtt_listener") is MQTTListenerMethod


class TestMQTTListenerHealthCheck:
    @pytest.mark.asyncio
    async def test_health_check_no_tasks(self):
        method = MQTTListenerMethod()
        assert await method.health_check() is False

    @pytest.mark.asyncio
    async def test_health_check_with_done_tasks(self):
        method = MQTTListenerMethod()
        done_task = MagicMock()
        done_task.done.return_value = True
        method._tasks["SIG-DONE"] = done_task
        assert await method.health_check() is False


class TestMQTTBuildServerConfig:
    def test_defaults_when_empty(self):
        cfg = MQTTListenerMethod._build_server_config({"broker_url": "mqtt://h:1883"})
        assert cfg.broker_url == "mqtt://h:1883"
        assert cfg.client_id == "tsigma-listener"
        assert cfg.username is None
        assert cfg.password is None
        assert cfg.keepalive == 60
        assert cfg.tls is False
        assert cfg.instance == "default"

    def test_inline_credentials(self):
        cfg = MQTTListenerMethod._build_server_config({
            "broker_url": "mqtts://h:8883",
            "username": "user",
            "password": "pw",
            "tls": True,
            "instance": "cloud",
        })
        assert cfg.username == "user"
        assert cfg.password == "pw"
        assert cfg.tls is True
        assert cfg.instance == "cloud"

    def test_secret_file_takes_precedence(self, tmp_path):
        user_file = tmp_path / "u"
        pass_file = tmp_path / "p"
        user_file.write_text("file-user\n")
        pass_file.write_text("file-pw\n")
        cfg = MQTTListenerMethod._build_server_config({
            "broker_url": "mqtt://h",
            "username": "inline-user",
            "password": "inline-pw",
            "username_file": str(user_file),
            "password_file": str(pass_file),
        })
        assert cfg.username == "file-user"
        assert cfg.password == "file-pw"


class TestMQTTBuildSubscriptions:
    def test_skips_devices_without_topic(self):
        subs = MQTTListenerMethod._build_subscriptions([
            ("SIG-OK", {"topic": "atspm/ok", "decoder": "openphase"}),
            ("SIG-NO-TOPIC", {"decoder": "openphase"}),
            ("SIG-EMPTY-TOPIC", {"topic": ""}),
        ])
        assert list(subs.keys()) == ["SIG-OK"]

    def test_qos_default_and_override(self):
        subs = MQTTListenerMethod._build_subscriptions([
            ("SIG-A", {"topic": "atspm/a"}),
            ("SIG-B", {"topic": "atspm/b", "qos": 2}),
        ])
        assert subs["SIG-A"].qos == 1
        assert subs["SIG-B"].qos == 2


class TestMQTTHandleMessage:
    @pytest.mark.asyncio
    async def test_decodes_and_persists_via_target(self):
        method = MQTTListenerMethod()
        method._session_factory = AsyncMock()
        method._target = ControllerTarget()
        method._target.persist_with_drift_check = AsyncMock()

        sub = MQTTSubscription(
            device_id="SIG-100",
            topic="atspm/100",
            decoder="asc3",
        )

        fake_events = [MagicMock(), MagicMock()]
        mock_decoder = MagicMock()
        mock_decoder.decode_bytes.return_value = fake_events

        with patch(
            "tsigma.collection.methods.mqtt_listener.resolve_decoder_by_name",
            return_value=mock_decoder,
        ) as mock_resolve:
            await method._handle_message(sub, b"\x01\x02\x03")

        mock_resolve.assert_called_once_with("asc3")
        mock_decoder.decode_bytes.assert_called_once_with(b"\x01\x02\x03")
        method._target.persist_with_drift_check.assert_awaited_once()
        # device_id arg
        assert (
            method._target.persist_with_drift_check.call_args[0][1] == "SIG-100"
        )

    @pytest.mark.asyncio
    async def test_decode_error_is_swallowed(self):
        method = MQTTListenerMethod()
        method._session_factory = AsyncMock()
        method._target = ControllerTarget()
        method._target.persist_with_drift_check = AsyncMock()

        sub = MQTTSubscription(
            device_id="SIG-200", topic="atspm/200", decoder="broken",
        )

        with patch(
            "tsigma.collection.methods.mqtt_listener.resolve_decoder_by_name",
            side_effect=RuntimeError("bad decoder"),
        ):
            await method._handle_message(sub, b"\xff")

        method._target.persist_with_drift_check.assert_not_called()


class TestMQTTSubscriberLoop:
    @pytest.mark.asyncio
    async def test_subscriber_loop_subscribes_and_handles_message(self):
        method = MQTTListenerMethod()
        method._session_factory = AsyncMock()
        method._target = ControllerTarget()
        method._server_config = MQTTServerConfig(**_server_config())

        sub = MQTTSubscription(
            device_id="SIG-CONN", topic="atspm/conn",
            decoder="auto", qos=1,
        )

        mock_client = AsyncMock()
        mock_client.subscribe = AsyncMock()

        mock_message = MagicMock()
        mock_message.payload = b"\x01\x02"

        async def _fake_messages():
            yield mock_message
            method._stop_event.set()

        mock_client.messages = _fake_messages()

        mock_ctx = AsyncMock()
        mock_ctx.__aenter__ = AsyncMock(return_value=mock_client)
        mock_ctx.__aexit__ = AsyncMock(return_value=False)
        mock_client_cls = MagicMock(return_value=mock_ctx)

        with patch(
            "tsigma.collection.methods.mqtt_listener.aiomqtt"
        ) as mock_aiomqtt:
            mock_aiomqtt.Client = mock_client_cls
            mock_aiomqtt.MqttError = Exception
            mock_aiomqtt.TLSParameters = MagicMock()
            with patch.object(method, "_handle_message", new_callable=AsyncMock):
                await method._subscriber_loop(sub)

        mock_client.subscribe.assert_awaited_once_with("atspm/conn", qos=1)

    @pytest.mark.asyncio
    async def test_subscriber_loop_reconnects_on_mqtt_error(self):
        method = MQTTListenerMethod()
        method._session_factory = AsyncMock()
        method._target = ControllerTarget()
        method._server_config = MQTTServerConfig(**_server_config())

        sub = MQTTSubscription(
            device_id="SIG-RECONN", topic="atspm/reconn", decoder="auto",
        )

        call_count = 0

        class FakeMqttError(Exception):
            pass

        async def _fake_aenter(_ctx):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise FakeMqttError("connection lost")
            method._stop_event.set()
            raise FakeMqttError("stop now")

        mock_ctx = AsyncMock()
        mock_ctx.__aenter__ = _fake_aenter
        mock_ctx.__aexit__ = AsyncMock(return_value=False)
        mock_client_cls = MagicMock(return_value=mock_ctx)

        with patch(
            "tsigma.collection.methods.mqtt_listener.aiomqtt"
        ) as mock_aiomqtt:
            mock_aiomqtt.Client = mock_client_cls
            mock_aiomqtt.MqttError = FakeMqttError
            mock_aiomqtt.TLSParameters = MagicMock()
            with patch("asyncio.sleep", new_callable=AsyncMock):
                await method._subscriber_loop(sub)

        assert call_count >= 2

    @pytest.mark.asyncio
    async def test_subscriber_loop_handles_unexpected_error(self, caplog):
        method = MQTTListenerMethod()
        method._session_factory = AsyncMock()
        method._target = ControllerTarget()
        method._server_config = MQTTServerConfig(**_server_config())

        sub = MQTTSubscription(
            device_id="SIG-GENERIC", topic="atspm/generic", decoder="auto",
        )

        call_count = 0

        async def _fake_aenter(_ctx):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("unexpected boom")
            method._stop_event.set()
            raise RuntimeError("stop now")

        mock_ctx = AsyncMock()
        mock_ctx.__aenter__ = _fake_aenter
        mock_ctx.__aexit__ = AsyncMock(return_value=False)
        mock_client_cls = MagicMock(return_value=mock_ctx)

        class FakeMqttError(Exception):
            pass

        with patch(
            "tsigma.collection.methods.mqtt_listener.aiomqtt"
        ) as mock_aiomqtt:
            mock_aiomqtt.Client = mock_client_cls
            mock_aiomqtt.MqttError = FakeMqttError
            mock_aiomqtt.TLSParameters = MagicMock()
            with (
                caplog.at_level(
                    logging.ERROR,
                    logger="tsigma.collection.methods.mqtt_listener",
                ),
                patch("asyncio.sleep", new_callable=AsyncMock),
            ):
                await method._subscriber_loop(sub)

        assert call_count >= 2
        assert "unexpected error" in caplog.text.lower()


class TestMQTTStartStop:
    @pytest.mark.asyncio
    async def test_start_no_broker_url_refuses(self, caplog):
        method = MQTTListenerMethod()
        with caplog.at_level(
            logging.ERROR,
            logger="tsigma.collection.methods.mqtt_listener",
        ):
            await method.start(
                {"broker_url": ""},
                AsyncMock(),
                target=ControllerTarget(),
                devices=[("SIG-1", {"topic": "x"})],
            )
        assert not method._tasks
        assert "missing broker_url" in caplog.text.lower()

    @pytest.mark.asyncio
    async def test_start_no_devices_warns(self, caplog):
        method = MQTTListenerMethod()
        with caplog.at_level(
            logging.WARNING,
            logger="tsigma.collection.methods.mqtt_listener",
        ):
            await method.start(
                _server_config(),
                AsyncMock(),
                target=ControllerTarget(),
                devices=[],
            )
        assert not method._tasks
        assert "no matching" in caplog.text.lower()

    @pytest.mark.asyncio
    async def test_start_skips_devices_without_topic(self):
        method = MQTTListenerMethod()
        # Patch the subscriber loop so we don't actually try to connect.
        with patch.object(method, "_subscriber_loop", new_callable=AsyncMock):
            await method.start(
                _server_config(),
                AsyncMock(),
                target=ControllerTarget(),
                devices=[
                    ("SIG-OK", {"topic": "atspm/ok"}),
                    ("SIG-NO-TOPIC", {}),
                ],
            )
        # Only SIG-OK should have a task
        assert "SIG-OK" in method._tasks
        assert "SIG-NO-TOPIC" not in method._tasks
        # Cancel the spawned tasks before exiting
        await method.stop()

    @pytest.mark.asyncio
    async def test_stop_clears_state(self):
        method = MQTTListenerMethod()
        method._subscriptions["SIG-001"] = MQTTSubscription(
            device_id="SIG-001", topic="atspm/1",
        )

        async def _noop():
            await asyncio.sleep(3600)

        task = asyncio.create_task(_noop())
        method._tasks["SIG-001"] = task

        await method.stop()

        assert not method._tasks
        assert not method._subscriptions
