"""
MQTT listener ingestion method.

Subscribes to MQTT topics for real-time event streaming from
traffic signal controllers. Each message is decoded and persisted
as it arrives — no polling, no batching delay.

Per-signal configuration via signal_metadata JSONB:

    {
        "collection": {
            "method": "mqtt_listener",
            "broker": "mqtt://localhost:1883",
            "topic": "signals/1001/events",
            "decoder": "asc3"
        }
    }

Multiple signals can share the same MQTT broker but subscribe to
different topics. The listener manages one subscription per signal.

This is a ListenerIngestionMethod — the CollectorService manages
start/stop lifecycle.
"""

import asyncio
import logging
from contextlib import suppress
from typing import Any, Optional
from urllib.parse import urlparse

import aiomqtt
from pydantic import BaseModel

from ..registry import IngestionMethodRegistry, ListenerIngestionMethod
from ..sdk import persist_events_with_drift_check, resolve_decoder_by_name

logger = logging.getLogger(__name__)

_DEFAULT_DECODER = "auto"


class MQTTListenerConfig(BaseModel):
    """
    Configuration for a single MQTT subscription.

    Args:
        signal_id: Traffic signal this subscription belongs to.
        broker: MQTT broker URL (e.g., "mqtt://localhost:1883").
        topic: MQTT topic to subscribe to.
        decoder: Decoder name for incoming messages.
        username: Optional MQTT username.
        password: Optional MQTT password.
        client_id: Optional MQTT client ID.
        qos: Quality of Service level (0, 1, or 2).
        use_tls: Use TLS for the broker connection.
    """

    signal_id: str
    broker: str
    topic: str
    decoder: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    client_id: Optional[str] = None
    qos: int = 1
    use_tls: bool = False


@IngestionMethodRegistry.register("mqtt_listener")
class MQTTListenerMethod(ListenerIngestionMethod):
    """
    MQTT listener ingestion method for real-time event streaming.

    A listener plugin: the CollectorService calls start() once with
    a config dict containing all signal subscriptions, and stop()
    on shutdown.

    Each signal gets its own MQTT subscription. Messages are decoded
    and persisted as they arrive using the collection SDK.
    """

    name = "mqtt_listener"

    def __init__(self) -> None:
        self._tasks: dict[str, asyncio.Task] = {}
        self._session_factory = None
        self._configs: dict[str, MQTTListenerConfig] = {}
        self._stop_event = asyncio.Event()

    async def health_check(self) -> bool:
        """
        Check if subscriber tasks are running.

        Returns:
            True if at least one subscriber task is active, False if none.
        """
        if not self._tasks:
            return False
        return any(not t.done() for t in self._tasks.values())

    async def start(self, config: dict[str, Any], session_factory) -> None:
        """
        Start MQTT subscriptions for all configured signals.

        Args:
            config: Listener config with signal subscription details.
            session_factory: Async session factory for DB writes.
        """
        self._session_factory = session_factory
        self._stop_event.clear()

        signals = config.get("signals", [])
        if not signals:
            logger.warning("MQTT listener started with no signal subscriptions")
            return

        for signal_config in signals:
            mqtt_config = MQTTListenerConfig(
                signal_id=signal_config["signal_id"],
                broker=signal_config.get("broker", "mqtt://localhost:1883"),
                topic=signal_config.get("topic", ""),
                decoder=signal_config.get("decoder"),
                username=signal_config.get("username"),
                password=signal_config.get("password"),
                client_id=signal_config.get("client_id"),
                qos=signal_config.get("qos", 1),
                use_tls=signal_config.get("use_tls", False),
            )

            if not mqtt_config.topic:
                logger.error(
                    "MQTT config for signal %s has no topic — skipping",
                    mqtt_config.signal_id,
                )
                continue

            self._configs[mqtt_config.signal_id] = mqtt_config
            task = asyncio.create_task(
                self._subscriber_loop(mqtt_config),
                name=f"mqtt_{mqtt_config.signal_id}",
            )
            self._tasks[mqtt_config.signal_id] = task

        logger.info(
            "MQTT listener started — %d subscriptions active",
            len(self._tasks),
        )

    async def _subscriber_loop(self, config: MQTTListenerConfig) -> None:
        """
        Long-lived subscriber loop for a single signal.

        Connects to the MQTT broker, subscribes to the topic, and
        processes messages until stop() is called. Reconnects
        automatically on connection loss.

        Args:
            config: Per-signal MQTT configuration.
        """
        parsed = urlparse(config.broker)
        hostname = parsed.hostname or "localhost"
        port = parsed.port or (8883 if config.use_tls else 1883)

        while not self._stop_event.is_set():
            try:
                async with aiomqtt.Client(
                    hostname=hostname,
                    port=port,
                    username=config.username,
                    password=config.password,
                    identifier=config.client_id,
                    tls_params=aiomqtt.TLSParameters() if config.use_tls else None,
                ) as client:
                    await client.subscribe(config.topic, qos=config.qos)
                    logger.info(
                        "MQTT subscribed: signal=%s topic=%s broker=%s:%d",
                        config.signal_id,
                        config.topic,
                        hostname,
                        port,
                    )

                    async for message in client.messages:
                        if self._stop_event.is_set():
                            break
                        await self._handle_message(
                            config.signal_id, config, message.payload
                        )

            except aiomqtt.MqttError as exc:
                if self._stop_event.is_set():
                    break
                logger.warning(
                    "MQTT connection lost for signal %s: %s — reconnecting in 5s",
                    config.signal_id,
                    exc,
                )
                await asyncio.sleep(5)
            except asyncio.CancelledError:
                break
            except Exception:
                if self._stop_event.is_set():
                    break
                logger.exception(
                    "MQTT unexpected error for signal %s — reconnecting in 10s",
                    config.signal_id,
                )
                await asyncio.sleep(10)

    async def _handle_message(
        self,
        signal_id: str,
        config: MQTTListenerConfig,
        payload: bytes,
    ) -> None:
        """
        Handle a single incoming MQTT message.

        Decodes the message payload and persists events to the database.

        Args:
            signal_id: Traffic signal identifier.
            config: Per-signal MQTT configuration.
            payload: Raw message bytes.
        """
        try:
            decoder = resolve_decoder_by_name(config.decoder or _DEFAULT_DECODER)
            events = decoder.decode_bytes(payload)
        except Exception:
            logger.exception(
                "Failed to decode MQTT message for signal %s on %s",
                signal_id,
                config.topic,
            )
            return

        if events:
            await persist_events_with_drift_check(
                events, signal_id, self._session_factory
            )
            logger.debug(
                "MQTT: %d events from signal %s on %s",
                len(events),
                signal_id,
                config.topic,
            )

    async def stop(self) -> None:
        """
        Signal all subscriber loops to stop and wait for them to finish.
        """
        self._stop_event.set()

        for signal_id, task in self._tasks.items():
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task

        self._tasks.clear()
        self._configs.clear()

        logger.info("MQTT listener stopped")
