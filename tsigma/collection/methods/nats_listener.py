"""
NATS listener ingestion method.

Subscribes to NATS subjects for real-time event streaming from
traffic signal controllers. Each message is decoded and persisted
as it arrives — no polling, no batching delay.

Per-signal configuration via signal_metadata JSONB:

    {
        "collection": {
            "method": "nats_listener",
            "url": "nats://localhost:4222",
            "subject": "signals.1001.events",
            "decoder": "asc3"
        }
    }

Multiple signals can share the same NATS server but subscribe to
different subjects. The listener manages one subscription per signal.

This is a ListenerIngestionMethod — the CollectorService manages
start/stop lifecycle.
"""

import logging
from typing import Any, Optional

import nats
from nats.aio.client import Client as NATSClient
from nats.aio.subscription import Subscription
from pydantic import BaseModel

from ..registry import IngestionMethodRegistry, ListenerIngestionMethod
from ..sdk import persist_events_with_drift_check, resolve_decoder_by_name

logger = logging.getLogger(__name__)

_DEFAULT_DECODER = "auto"


class NATSListenerConfig(BaseModel):
    """
    Configuration for a single NATS subscription.

    Args:
        signal_id: Traffic signal this subscription belongs to.
        url: NATS server URL (e.g., "nats://localhost:4222").
        subject: NATS subject to subscribe to.
        decoder: Decoder name for incoming messages.
        queue_group: Optional queue group for load balancing
                     across multiple TSIGMA instances.
        token: Optional NATS auth token.
        credentials_file: Optional NATS credentials file path.
    """

    signal_id: str
    url: str
    subject: str
    decoder: Optional[str] = None
    queue_group: Optional[str] = None
    token: Optional[str] = None
    credentials_file: Optional[str] = None


@IngestionMethodRegistry.register("nats_listener")
class NATSListenerMethod(ListenerIngestionMethod):
    """
    NATS listener ingestion method for real-time event streaming.

    A listener plugin: the CollectorService calls start() once with
    a config dict containing all signal subscriptions, and stop()
    on shutdown.

    Each signal gets its own NATS subscription. Messages are decoded
    and persisted as they arrive using the collection SDK.
    """

    name = "nats_listener"

    def __init__(self) -> None:
        self._clients: dict[str, NATSClient] = {}
        self._subscriptions: dict[str, Subscription] = {}
        self._session_factory = None
        self._configs: dict[str, NATSListenerConfig] = {}

    async def health_check(self) -> bool:
        """
        Check if all NATS connections are active.

        Returns:
            True if at least one connection is active, False if none.
        """
        if not self._clients:
            return False
        return any(not c.is_closed for c in self._clients.values())

    async def start(self, config: dict[str, Any], session_factory) -> None:
        """
        Start NATS subscriptions for all configured signals.

        The config dict contains a "signals" key with a list of
        per-signal subscription configs, or the CollectorService
        passes individual signal configs.

        Args:
            config: Listener config with signal subscription details.
            session_factory: Async session factory for DB writes.
        """
        self._session_factory = session_factory

        signals = config.get("signals", [])
        if not signals:
            logger.warning("NATS listener started with no signal subscriptions")
            return

        for signal_config in signals:
            nats_config = NATSListenerConfig(
                signal_id=signal_config["signal_id"],
                url=signal_config.get("url", "nats://localhost:4222"),
                subject=signal_config.get("subject", ""),
                decoder=signal_config.get("decoder"),
                queue_group=signal_config.get("queue_group"),
                token=signal_config.get("token"),
                credentials_file=signal_config.get("credentials_file"),
            )

            if not nats_config.subject:
                logger.error(
                    "NATS config for signal %s has no subject — skipping",
                    nats_config.signal_id,
                )
                continue

            self._configs[nats_config.signal_id] = nats_config
            await self._subscribe(nats_config)

        logger.info(
            "NATS listener started — %d subscriptions active",
            len(self._subscriptions),
        )

    async def _subscribe(self, config: NATSListenerConfig) -> None:
        """
        Connect to NATS and subscribe to the configured subject.

        Args:
            config: Per-signal NATS configuration.
        """
        signal_id = config.signal_id

        try:
            connect_opts: dict[str, Any] = {"servers": [config.url]}
            if config.token:
                connect_opts["token"] = config.token
            if config.credentials_file:
                connect_opts["user_credentials"] = config.credentials_file

            client = await nats.connect(**connect_opts)
            self._clients[signal_id] = client

            async def _message_handler(msg):
                await self._handle_message(signal_id, config, msg)

            if config.queue_group:
                sub = await client.subscribe(
                    config.subject,
                    queue=config.queue_group,
                    cb=_message_handler,
                )
            else:
                sub = await client.subscribe(
                    config.subject,
                    cb=_message_handler,
                )

            self._subscriptions[signal_id] = sub

            logger.info(
                "NATS subscribed: signal=%s subject=%s url=%s",
                signal_id,
                config.subject,
                config.url,
            )
        except Exception:
            logger.exception(
                "Failed to subscribe to NATS for signal %s at %s",
                signal_id,
                config.url,
            )

    async def _handle_message(
        self,
        signal_id: str,
        config: NATSListenerConfig,
        msg,
    ) -> None:
        """
        Handle a single incoming NATS message.

        Decodes the message data and persists events to the database.

        Args:
            signal_id: Traffic signal identifier.
            config: Per-signal NATS configuration.
            msg: NATS message object.
        """
        try:
            decoder = resolve_decoder_by_name(config.decoder or _DEFAULT_DECODER)
            events = decoder.decode_bytes(msg.data)
        except Exception:
            logger.exception(
                "Failed to decode NATS message for signal %s on %s",
                signal_id,
                config.subject,
            )
            return

        if events:
            await persist_events_with_drift_check(
                events, signal_id, self._session_factory
            )
            logger.debug(
                "NATS: %d events from signal %s on %s",
                len(events),
                signal_id,
                config.subject,
            )

    async def stop(self) -> None:
        """
        Unsubscribe from all subjects and close all NATS connections.
        """
        for signal_id, sub in self._subscriptions.items():
            try:
                await sub.unsubscribe()
            except Exception:
                logger.exception(
                    "Error unsubscribing NATS for signal %s", signal_id
                )

        for signal_id, client in self._clients.items():
            try:
                await client.close()
            except Exception:
                logger.exception(
                    "Error closing NATS connection for signal %s", signal_id
                )

        self._subscriptions.clear()
        self._clients.clear()
        self._configs.clear()

        logger.info("NATS listener stopped")
