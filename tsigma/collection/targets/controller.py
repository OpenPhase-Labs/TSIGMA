"""
ControllerTarget — the ingestion target for cabinet controller devices.

Wraps the existing collection SDK so the controller path is unchanged
at behaviour level; callers now invoke the methods through a target
object instead of calling free functions.  That indirection is what
lets a future ``RoadsideTarget`` feed the same transports
(``ftp_pull``, ``http_pull``, ``tcp_server``, ...) without duplicating
them per device class.

All checkpoint operations are scoped to ``device_type='controller'``;
``device_id`` is the ``Signal.signal_id`` text identifier.  Events are
persisted to ``controller_event_log`` via
``sdk.persist_events_with_drift_check``.
"""

from typing import Any, Optional

from ...models.checkpoint import PollingCheckpoint
from .. import sdk


class ControllerTarget:
    """Ingestion target for cabinet controller devices."""

    device_type: str = "controller"

    def resolve_decoder(
        self,
        *,
        decoder_name: Optional[str] = None,
        filename: Optional[str] = None,
    ) -> Any:
        """Resolve a decoder by explicit name or by filename extension.

        Exactly one of ``decoder_name`` / ``filename`` must be given.
        """
        if decoder_name:
            return sdk.resolve_decoder_by_name(decoder_name)
        if filename:
            return sdk.resolve_decoder_by_extension(filename)
        raise ValueError(
            "resolve_decoder requires decoder_name or filename",
        )

    async def persist(
        self, events, device_id: str, session_factory,
    ) -> None:
        await sdk.persist_events(events, device_id, session_factory)

    async def persist_with_drift_check(
        self,
        events,
        device_id: str,
        session_factory,
        *,
        source_label: str = "device",
    ) -> None:
        await sdk.persist_events_with_drift_check(
            events,
            device_id,
            session_factory,
            source_label=source_label,
        )

    async def load_checkpoint(
        self, method_name: str, device_id: str, session_factory,
    ) -> Optional[PollingCheckpoint]:
        return await sdk.load_checkpoint(
            method_name, self.device_type, device_id, session_factory,
        )

    async def save_checkpoint(
        self,
        method_name: str,
        device_id: str,
        session_factory,
        **kwargs,
    ) -> None:
        await sdk.save_checkpoint(
            method_name,
            self.device_type,
            device_id,
            session_factory,
            **kwargs,
        )

    async def record_error(
        self,
        method_name: str,
        device_id: str,
        session_factory,
        error_msg: str,
    ) -> None:
        await sdk.record_error(
            method_name,
            self.device_type,
            device_id,
            session_factory,
            error_msg,
        )
