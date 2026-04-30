"""
MQTT listener ingestion method.

Subscribes to MQTT topics for real-time event streaming from traffic
signal controllers and roadside sensors.  Each message is decoded and
persisted as it arrives — no polling, no batching delay.

Layer-2 server config (broker URL, credentials, TLS, keepalive, instance
discriminator) comes from process env vars via ``ListenerService``.
Per-device routing fields (topic, qos, decoder) come from each device's
``metadata.collection`` JSONB and are passed in via the ``devices``
argument from the orchestrator.

Multiple devices share one broker connection; the listener manages one
subscription per device's topic.  In multi-broker DOTs, run multiple
listener containers each with its own ``TSIGMA_MQTT_INSTANCE`` matching
the ``instance`` field on per-device JSONB.

This is a ListenerIngestionMethod — the ListenerService manages
start/stop lifecycle.
"""

import asyncio
import logging
from contextlib import suppress
from typing import Any, Iterable, Optional
from urllib.parse import urlparse

import aiomqtt
from pydantic import BaseModel

from ..registry import IngestionMethodRegistry, ListenerIngestionMethod
from ..sdk import resolve_decoder_by_name
from ..targets import ControllerTarget, IngestionTarget

logger = logging.getLogger(__name__)

_DEFAULT_DECODER = "auto"


def _read_secret_file(path: Optional[str]) -> Optional[str]:
    """Read and strip the contents of a secret file path, or None."""
    if not path:
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return f.read().strip()
    except OSError:
        logger.exception("Failed to read MQTT secret file %s", path)
        return None


class MQTTServerConfig(BaseModel):
    """Layer-2 server config for the MQTT listener."""

    broker_url: str
    client_id: str = "tsigma-listener"
    username: Optional[str] = None
    password: Optional[str] = None
    keepalive: int = 60
    tls: bool = False
    instance: str = "default"


class MQTTSubscription(BaseModel):
    """Per-device subscription routing."""

    device_id: str
    topic: str
    decoder: Optional[str] = None
    qos: int = 1


@IngestionMethodRegistry.register("mqtt_listener")
class MQTTListenerMethod(ListenerIngestionMethod):
    """One MQTT broker connection, N per-device topic subscriptions."""

    name = "mqtt_listener"

    def __init__(self) -> None:
        self._tasks: dict[str, asyncio.Task] = {}
        self._session_factory = None
        self._server_config: Optional[MQTTServerConfig] = None
        self._target: IngestionTarget = ControllerTarget()
        self._subscriptions: dict[str, MQTTSubscription] = {}
        self._stop_event = asyncio.Event()

    @staticmethod
    def _build_server_config(raw: dict[str, Any]) -> MQTTServerConfig:
        # Prefer file-mounted secrets over inline values.
        username = (
            _read_secret_file(raw.get("username_file"))
            or raw.get("username")
            or None
        )
        password = (
            _read_secret_file(raw.get("password_file"))
            or raw.get("password")
            or None
        )
        return MQTTServerConfig(
            broker_url=raw.get("broker_url", ""),
            client_id=raw.get("client_id", "tsigma-listener"),
            username=username,
            password=password,
            keepalive=raw.get("keepalive", 60),
            tls=bool(raw.get("tls", False)),
            instance=raw.get("instance", "default"),
        )

    @staticmethod
    def _build_subscriptions(
        devices: Iterable[tuple[str, dict[str, Any]]],
    ) -> dict[str, MQTTSubscription]:
        out: dict[str, MQTTSubscription] = {}
        for device_id, config in devices:
            topic = config.get("topic")
            if not topic:
                logger.warning(
                    "MQTT: device %s has no collection.topic — skipping",
                    device_id,
                )
                continue
            out[device_id] = MQTTSubscription(
                device_id=device_id,
                topic=topic,
                decoder=config.get("decoder"),
                qos=int(config.get("qos", 1)),
            )
        return out

    async def health_check(self) -> bool:
        if not self._tasks:
            return False
        return any(not t.done() for t in self._tasks.values())

    async def start(
        self,
        config: dict[str, Any],
        session_factory,
        *,
        target: Any = None,
        devices: Any = None,
    ) -> None:
        self._session_factory = session_factory
        self._target = target if target is not None else ControllerTarget()
        self._stop_event.clear()
        self._server_config = self._build_server_config(config)
        self._subscriptions = self._build_subscriptions(devices or [])

        if not self._server_config.broker_url:
            logger.error(
                "MQTT listener missing broker_url (TSIGMA_MQTT_BROKER_URL); "
                "refusing to start.",
            )
            return

        if not self._subscriptions:
            logger.warning(
                "MQTT listener (instance=%s) has no matching %s devices — "
                "not connecting to broker.",
                self._server_config.instance,
                self._target.device_type,
            )
            return

        for device_id, sub in self._subscriptions.items():
            task = asyncio.create_task(
                self._subscriber_loop(sub),
                name=f"mqtt_{device_id}",
            )
            self._tasks[device_id] = task

        logger.info(
            "MQTT listener (%s, instance=%s, broker=%s) started — "
            "%d subscription(s)",
            self._target.device_type,
            self._server_config.instance,
            self._server_config.broker_url,
            len(self._tasks),
        )

    async def _subscriber_loop(self, sub: MQTTSubscription) -> None:
        """Long-lived subscriber loop for a single device."""
        cfg = self._server_config
        parsed = urlparse(cfg.broker_url)
        hostname = parsed.hostname or "localhost"
        port = parsed.port or (8883 if cfg.tls else 1883)

        while not self._stop_event.is_set():
            try:
                async with aiomqtt.Client(
                    hostname=hostname,
                    port=port,
                    username=cfg.username,
                    password=cfg.password,
                    identifier=cfg.client_id,
                    keepalive=cfg.keepalive,
                    tls_params=aiomqtt.TLSParameters() if cfg.tls else None,
                ) as client:
                    await client.subscribe(sub.topic, qos=sub.qos)
                    logger.info(
                        "MQTT subscribed: %s=%s topic=%s broker=%s:%d",
                        self._target.device_type,
                        sub.device_id, sub.topic, hostname, port,
                    )

                    async for message in client.messages:
                        if self._stop_event.is_set():
                            break
                        await self._handle_message(sub, message.payload)

            except aiomqtt.MqttError as exc:
                if self._stop_event.is_set():
                    break
                logger.warning(
                    "MQTT connection lost for %s %s: %s — reconnecting in 5s",
                    self._target.device_type, sub.device_id, exc,
                )
                await asyncio.sleep(5)
            except asyncio.CancelledError:
                break
            except Exception:
                if self._stop_event.is_set():
                    break
                logger.exception(
                    "MQTT unexpected error for %s %s — reconnecting in 10s",
                    self._target.device_type, sub.device_id,
                )
                await asyncio.sleep(10)

    async def _handle_message(
        self, sub: MQTTSubscription, payload: bytes,
    ) -> None:
        try:
            decoder = resolve_decoder_by_name(sub.decoder or _DEFAULT_DECODER)
            events = decoder.decode_bytes(payload)
        except Exception:
            logger.exception(
                "Failed to decode MQTT message for %s %s on %s",
                self._target.device_type, sub.device_id, sub.topic,
            )
            return

        if events:
            await self._target.persist_with_drift_check(
                events, sub.device_id, self._session_factory,
                source_label=self._target.device_type,
            )
            logger.debug(
                "MQTT: %d events from %s %s on %s",
                len(events),
                self._target.device_type, sub.device_id, sub.topic,
            )

    async def stop(self) -> None:
        self._stop_event.set()

        for task in self._tasks.values():
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task

        self._tasks.clear()
        self._subscriptions.clear()

        logger.info("MQTT listener stopped")
