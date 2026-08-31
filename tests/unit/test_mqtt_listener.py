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
