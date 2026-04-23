"""
HTTP pull ingestion method.

Polls Econolite MaxTime/HD controllers for event logs via their
HTTP XML API. Supports incremental collection using a `since`
query parameter to fetch only new events.

Uses persistent polling_checkpoint table to track last event
timestamp per signal — non-destructive, restartable.

Checkpoint resilience: the checkpoint timestamp is capped at
server_time + configurable tolerance so that future-dated events
from a misconfigured controller clock cannot advance the watermark
past real time (which would cause all subsequent polls to return
zero new data). Future-dated events are still ingested but flagged
via the notification system.

This is a PollingIngestionMethod — the CollectorService calls
poll_once() on a schedule with per-signal config from the database.
"""

import logging
from datetime import datetime
from typing import Any, Optional

import aiohttp
from pydantic import BaseModel

from ..registry import IngestionMethodRegistry, PollingIngestionMethod
from ..sdk import (
    load_checkpoint,
    persist_events_with_drift_check,
    record_error,
    resolve_decoder_by_name,
    save_checkpoint,
)

logger = logging.getLogger(__name__)

_DEFAULT_PATH = "/v1/asclog/xml/full"
_DEFAULT_DECODER = "maxtime"


class HTTPPullConfig(BaseModel):
    """
    Configuration for the HTTP pull ingestion method.

    Args:
        host: Controller hostname or IP address.
        signal_id: Traffic signal ID these events belong to.
        port: HTTP port. Default 80.
        use_tls: Use HTTPS instead of HTTP.
        path: URL path for the event log endpoint.
        timeout_seconds: HTTP request timeout.
        decoder: Explicit decoder name, or None for default (maxtime).
    """

    host: str
    signal_id: str
    port: int = 80
    use_tls: bool = False
    path: str = _DEFAULT_PATH
    timeout_seconds: int = 30
    decoder: Optional[str] = None


@IngestionMethodRegistry.register("http_pull")
class HTTPPullMethod(PollingIngestionMethod):
    """
    HTTP pull ingestion method for MaxTime/Econolite controllers.

    A polling plugin: the CollectorService calls poll_once() on a
    schedule with per-signal config from signal_metadata JSONB.

    Uses persistent polling_checkpoint table to track the last event
    timestamp per signal for incremental collection via the `since`
    query parameter.
    """

    name = "http_pull"

    @staticmethod
    def _build_config(signal_id: str, raw: dict[str, Any]) -> HTTPPullConfig:
        """
        Build HTTPPullConfig from a signal_metadata collection dict.

        Args:
            signal_id: Traffic signal identifier.
            raw: Collection config dict from signal_metadata JSONB.

        Returns:
            HTTPPullConfig instance.
        """
        return HTTPPullConfig(
            host=raw.get("host", ""),
            signal_id=signal_id,
            port=raw.get("port", 80),
            use_tls=raw.get("use_tls", False),
            path=raw.get("path", _DEFAULT_PATH),
            timeout_seconds=raw.get("timeout_seconds", 30),
            decoder=raw.get("decoder"),
        )

    @staticmethod
    def _build_url(config: HTTPPullConfig, since: Optional[datetime]) -> str:
        """
        Build the full request URL with optional since parameter.

        Args:
            config: HTTP pull configuration.
            since: Timestamp for incremental collection, or None.

        Returns:
            Full URL string.
        """
        scheme = "https" if config.use_tls else "http"
        if not config.use_tls:
            logger.warning(
                "Plain HTTP (unencrypted) connection to %s "
                "— data sent in cleartext. "
                "Set use_tls=true when the controller supports it.",
                config.host,
            )
        url = f"{scheme}://{config.host}:{config.port}{config.path}"
        if since:
            tenths = since.microsecond // 100000
            since_str = since.strftime("%m-%d-%Y %H:%M:%S.") + str(tenths)
            url += f"?since={since_str}"
        return url

    async def health_check(self) -> bool:
        """
        Polling methods are always considered healthy.

        Returns:
            True always.
        """
        return True

    async def poll_once(
        self, signal_id: str, config: dict[str, Any], session_factory
    ) -> None:
        """
        Execute one poll cycle for a single signal.

        Connects to the controller HTTP API, fetches event XML,
        decodes it, and persists events to the database. Uses
        the persistent checkpoint for incremental collection.

        Args:
            signal_id: Traffic signal identifier.
            config: Collection config dict from signal_metadata JSONB.
            session_factory: Async session factory for DB writes.
        """
        http_config = self._build_config(signal_id, config)

        # Load checkpoint for incremental query
        checkpoint = await load_checkpoint(self.name, signal_id, session_factory)
        since = checkpoint.last_event_timestamp if checkpoint else None
        url = self._build_url(http_config, since)

        timeout = aiohttp.ClientTimeout(total=http_config.timeout_seconds)

        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url) as response:
                    if response.status != 200:
                        error_msg = f"HTTP {response.status} from {http_config.host}"
                        logger.error("%s for signal %s", error_msg, signal_id)
                        await record_error(
                            self.name, signal_id, session_factory, error_msg
                        )
                        return
                    data = await response.read()
        except Exception as exc:
            logger.error(
                "Connection failed to http://%s:%d for signal %s",
                http_config.host,
                http_config.port,
                signal_id,
            )
            await record_error(self.name, signal_id, session_factory, str(exc))
            return

        try:
            decoder = resolve_decoder_by_name(http_config.decoder or _DEFAULT_DECODER)
            events = decoder.decode_bytes(data)
        except Exception as exc:
            logger.exception(
                "Failed to decode response from %s for signal %s",
                http_config.host,
                signal_id,
            )
            await record_error(self.name, signal_id, session_factory, str(exc))
            return

        await persist_events_with_drift_check(
            events, signal_id, session_factory
        )

        if events:
            latest = max(e.timestamp for e in events)
            await save_checkpoint(
                self.name,
                signal_id,
                session_factory,
                last_event_timestamp=latest,
                events_ingested=len(events),
            )
            logger.info(
                "Collected %d events from %s for signal %s",
                len(events),
                http_config.host,
                signal_id,
            )
