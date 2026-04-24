"""
IngestionTarget protocol — the contract between a transport method and
the destination for its decoded events.

A target owns three responsibilities that vary between device classes:

  1. **Decoder resolution** — controllers decode into ``DecodedEvent``
     (event_code / event_param); sensors decode into a different shape
     (mph / lane / class / etc.).
  2. **Event persistence** — controller events land in
     ``controller_event_log``; sensor events land in ``roadside_event``.
  3. **Checkpoint I/O** — controller checkpoints use
     ``device_type='controller'``; sensor checkpoints use
     ``device_type='sensor'``.

Transport methods (``ftp_pull``, ``http_pull``, ``tcp_server``, ...)
accept an ``IngestionTarget`` parameter and call through it.  That is
what lets one transport feed either event stream without duplication.
"""

from typing import Any, Optional, Protocol, runtime_checkable

from ...models.checkpoint import PollingCheckpoint


@runtime_checkable
class IngestionTarget(Protocol):
    """What a transport needs from its destination.

    ``device_type`` is the stable discriminator written to
    ``polling_checkpoint.device_type`` and used by silent-signal
    detection to look up the right rows.
    """

    device_type: str

    def resolve_decoder(
        self,
        *,
        decoder_name: Optional[str] = None,
        filename: Optional[str] = None,
    ) -> Any:
        """Return a decoder instance for the given hint.

        Exactly one of ``decoder_name`` (explicit lookup) or ``filename``
        (extension-based lookup) must be provided.  The returned
        decoder's ``decode_bytes`` output shape is target-specific.
        """
        ...

    async def persist(
        self, events, device_id: str, session_factory,
    ) -> None:
        """Bulk-insert events into the target's event table (idempotent)."""
        ...

    async def persist_with_drift_check(
        self,
        events,
        device_id: str,
        session_factory,
        *,
        source_label: str = "device",
    ) -> None:
        """Persist events, warning on future-dated event timestamps."""
        ...

    async def load_checkpoint(
        self, method_name: str, device_id: str, session_factory,
    ) -> Optional[PollingCheckpoint]:
        """Load the checkpoint for this device + method, or ``None``."""
        ...

    async def save_checkpoint(
        self,
        method_name: str,
        device_id: str,
        session_factory,
        **kwargs,
    ) -> None:
        """Create/update checkpoint after a successful ingest cycle."""
        ...

    async def record_error(
        self,
        method_name: str,
        device_id: str,
        session_factory,
        error_msg: str,
    ) -> None:
        """Record a poll error without advancing the checkpoint."""
        ...
