"""
NATS listener ingestion method.

Subscribes to NATS subjects for real-time event streaming from traffic
signal controllers and roadside sensors.  Each message is decoded and
persisted as it arrives — no polling, no batching delay.

Layer-2 server config (server URL, credentials, TLS, max reconnects,
instance discriminator) comes from process env vars via
``ListenerService``.  Per-device routing fields (subject, decoder,
queue_group) come from each device's ``metadata.collection`` JSONB and
are passed in via the ``devices`` argument from the orchestrator.

One NATS connection per listener container; the listener manages one
subscription per device's subject.  In multi-broker DOTs, run multiple
listener containers each with its own ``TSIGMA_NATS_INSTANCE`` matching
the ``instance`` field on per-device JSONB.

This is a ListenerIngestionMethod — the ListenerService manages
start/stop lifecycle.
"""

import logging
from typing import Any, Iterable, Optional

import nats
from nats.aio.client import Client as NATSClient
from nats.aio.subscription import Subscription
from pydantic import BaseModel

from ..registry import IngestionMethodRegistry, ListenerIngestionMethod
from ..sdk import resolve_decoder_by_name
from ..targets import ControllerTarget, IngestionTarget

logger = logging.getLogger(__name__)

_DEFAULT_DECODER = "auto"


class NATSServerConfig(BaseModel):
    """Layer-2 server config for the NATS listener."""

    url: str
    credentials_file: Optional[str] = None
    tls: bool = False
    max_reconnects: int = -1
    instance: str = "default"


class NATSSubscription(BaseModel):
    """Per-device subscription routing."""

    device_id: str
    subject: str
    decoder: Optional[str] = None
    queue_group: Optional[str] = None


@IngestionMethodRegistry.register("nats_listener")
class NATSListenerMethod(ListenerIngestionMethod):
    """One NATS connection, N per-device subject subscriptions."""

    name = "nats_listener"

    def __init__(self) -> None:
        self._client: Optional[NATSClient] = None
        self._subscriptions: dict[str, Subscription] = {}
        self._session_factory = None
        self._server_config: Optional[NATSServerConfig] = None
        self._target: IngestionTarget = ControllerTarget()
        self._configs: dict[str, NATSSubscription] = {}

    @staticmethod
    def _build_server_config(raw: dict[str, Any]) -> NATSServerConfig:
        return NATSServerConfig(
            url=raw.get("url", ""),
            credentials_file=raw.get("credentials_file"),
            tls=bool(raw.get("tls", False)),
            max_reconnects=int(raw.get("max_reconnects", -1)),
            instance=raw.get("instance", "default"),
        )

    @staticmethod
    def _build_subscriptions(
        devices: Iterable[tuple[str, dict[str, Any]]],
    ) -> dict[str, NATSSubscription]:
        out: dict[str, NATSSubscription] = {}
        for device_id, config in devices:
            subject = config.get("subject")
            if not subject:
                logger.warning(
                    "NATS: device %s has no collection.subject — skipping",
                    device_id,
                )
                continue
            out[device_id] = NATSSubscription(
                device_id=device_id,
                subject=subject,
                decoder=config.get("decoder"),
                queue_group=config.get("queue_group"),
            )
        return out

    async def health_check(self) -> bool:
        return self._client is not None and not self._client.is_closed

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
        self._server_config = self._build_server_config(config)
        self._configs = self._build_subscriptions(devices or [])

        if not self._server_config.url:
            logger.error(
                "NATS listener missing url (TSIGMA_NATS_URL); refusing to start.",
            )
            return

        if not self._configs:
            logger.warning(
                "NATS listener (instance=%s) has no matching %s devices — "
                "not connecting to server.",
                self._server_config.instance,
                self._target.device_type,
            )
            return

        # One connection per listener container.
        connect_opts: dict[str, Any] = {
            "servers": [self._server_config.url],
            "max_reconnect_attempts": self._server_config.max_reconnects,
        }
        if self._server_config.credentials_file:
            connect_opts["user_credentials"] = (
                self._server_config.credentials_file
            )
        # TLS in nats-py is configured via context; for v1 we rely on the
        # url scheme (tls://...) when self._server_config.tls is true and
        # the operator supplies a TLS-scheme URL.

        try:
            self._client = await nats.connect(**connect_opts)
        except Exception:
            logger.exception(
                "NATS connect failed for url=%s — listener not started",
                self._server_config.url,
            )
            return

        for device_id, sub_cfg in self._configs.items():
            await self._subscribe(device_id, sub_cfg)

        logger.info(
            "NATS listener (%s, instance=%s, server=%s) started — "
            "%d subscription(s)",
            self._target.device_type,
            self._server_config.instance,
            self._server_config.url,
            len(self._subscriptions),
        )

    async def _subscribe(
        self, device_id: str, sub_cfg: NATSSubscription,
    ) -> None:
        """Subscribe to one device's subject on the shared connection."""
        if self._client is None:
            return

        async def _message_handler(msg):
            await self._handle_message(sub_cfg, msg)

        try:
            if sub_cfg.queue_group:
                sub = await self._client.subscribe(
                    sub_cfg.subject,
                    queue=sub_cfg.queue_group,
                    cb=_message_handler,
                )
            else:
                sub = await self._client.subscribe(
                    sub_cfg.subject,
                    cb=_message_handler,
                )
            self._subscriptions[device_id] = sub
            logger.info(
                "NATS subscribed: %s=%s subject=%s",
                self._target.device_type, device_id, sub_cfg.subject,
            )
        except Exception:
            logger.exception(
                "Failed to subscribe to NATS for %s %s subject=%s",
                self._target.device_type, device_id, sub_cfg.subject,
            )

    async def _handle_message(
        self, sub_cfg: NATSSubscription, msg,
    ) -> None:
        try:
            decoder = resolve_decoder_by_name(
                sub_cfg.decoder or _DEFAULT_DECODER,
            )
            events = decoder.decode_bytes(msg.data)
        except Exception:
            logger.exception(
                "Failed to decode NATS message for %s %s on %s",
                self._target.device_type, sub_cfg.device_id, sub_cfg.subject,
            )
            return

        if events:
            await self._target.persist_with_drift_check(
                events, sub_cfg.device_id, self._session_factory,
                source_label=self._target.device_type,
            )
            logger.debug(
                "NATS: %d events from %s %s on %s",
                len(events),
                self._target.device_type,
                sub_cfg.device_id, sub_cfg.subject,
            )

    async def stop(self) -> None:
        for device_id, sub in self._subscriptions.items():
            try:
                await sub.unsubscribe()
            except Exception:
                logger.exception(
                    "Error unsubscribing NATS for %s", device_id,
                )

        if self._client is not None:
            try:
                await self._client.close()
            except Exception:
                logger.exception("Error closing NATS connection")
            self._client = None

        self._subscriptions.clear()
        self._configs.clear()

        logger.info("NATS listener stopped")
