"""
Ingestion method plugin SDK.

Shared helpers for TSIGMA ingestion method plugins.  Ingestion methods
are plugins, and plugins should be able to stand on a well-defined
toolbox instead of copy-pasting checkpoint management, event persistence,
clock-drift detection, and decoder resolution into every file.

``PollingIngestionMethod``, ``ListenerIngestionMethod``, etc. intentionally
live in ``tsigma.collection.registry`` — they define the *contract*
between core and plugins.  This package provides the *toolbox* plugins
use to implement that contract.
"""

import logging
from datetime import datetime, timedelta, timezone
from pathlib import PurePosixPath
from typing import Optional

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from ...config import settings
from ...models.checkpoint import PollingCheckpoint
from ...models.event import ControllerEventLog
from ...notifications.registry import WARNING, notify
from ..decoders.base import DecoderRegistry

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Checkpoint management (polling plugins)
# ---------------------------------------------------------------------------


async def load_checkpoint(
    method_name: str,
    signal_id: str,
    session_factory,
) -> Optional[PollingCheckpoint]:
    """Load the checkpoint for a signal and method.

    Args:
        method_name: Ingestion method name (e.g. ``"http_pull"``).
        signal_id: Traffic signal identifier.
        session_factory: Async session factory.

    Returns:
        ``PollingCheckpoint`` if one exists, ``None`` otherwise.
    """
    async with session_factory() as session:
        stmt = select(PollingCheckpoint).where(
            PollingCheckpoint.signal_id == signal_id,
            PollingCheckpoint.method == method_name,
        )
        result = await session.execute(stmt)
        row = result.scalar_one_or_none()
        if row:
            session.expunge(row)
        return row


async def record_error(
    method_name: str,
    signal_id: str,
    session_factory,
    error_msg: str,
) -> None:
    """Record a poll error without advancing the checkpoint.

    Args:
        method_name: Ingestion method name.
        signal_id: Traffic signal identifier.
        session_factory: Async session factory.
        error_msg: Error description (truncated to 1000 chars).
    """
    now = datetime.now(timezone.utc)
    async with session_factory() as session:
        stmt = select(PollingCheckpoint).where(
            PollingCheckpoint.signal_id == signal_id,
            PollingCheckpoint.method == method_name,
        )
        result = await session.execute(stmt)
        checkpoint = result.scalar_one_or_none()

        if checkpoint is None:
            checkpoint = PollingCheckpoint(
                signal_id=signal_id,
                method=method_name,
            )
            session.add(checkpoint)

        checkpoint.consecutive_errors += 1
        checkpoint.last_error = error_msg[:1000]
        checkpoint.last_error_time = now
        checkpoint.updated_at = now

        await session.flush()


# ---------------------------------------------------------------------------
# Event persistence
# ---------------------------------------------------------------------------


async def persist_events(
    events,
    signal_id: str,
    session_factory,
) -> None:
    """Write decoded events to the database (idempotent).

    Uses ``INSERT ... ON CONFLICT DO NOTHING`` so re-ingesting the
    same events is a safe no-op.  This enables non-destructive
    collection — controllers never need to stop logging or delete
    files, and overlapping polls cannot create duplicates.

    Bare persistence — no clock-drift detection.  Use this for push/
    event-driven plugins (TCP, UDP, directory watch) that have no
    checkpoint watermark to protect.

    Args:
        events: List of ``DecodedEvent`` objects.
        signal_id: Traffic signal identifier.
        session_factory: Async session factory for DB writes.
    """
    if not events:
        return

    await _upsert_events(events, signal_id, session_factory)


async def persist_events_with_drift_check(
    events,
    signal_id: str,
    session_factory,
    *,
    source_label: str = "signal",
) -> None:
    """Write decoded events to the database with clock-drift detection.

    Detects future-dated events (controller clock drift), logs a warning,
    and sends a notification via the notification system.  Events are
    always ingested — data is never discarded.

    Use this for polling plugins (HTTP, FTP) that manage checkpoint
    watermarks.

    Args:
        events: List of ``DecodedEvent`` objects.
        signal_id: Traffic signal identifier.
        session_factory: Async session factory for DB writes.
        source_label: Label used in notification messages (e.g.
            ``"signal"`` for HTTP, ``"signal"`` for FTP).
    """
    if not events:
        return

    # Detect future-dated events
    now = datetime.now(timezone.utc)
    tolerance = timedelta(
        seconds=settings.checkpoint_future_tolerance_seconds,
    )
    future_cutoff = now + tolerance
    future_events = [e for e in events if e.timestamp > future_cutoff]
    if future_events:
        max_future = max(e.timestamp for e in future_events)
        drift = max_future - now
        logger.warning(
            "Signal %s: %d/%d events have future timestamps "
            "(max drift: %s, latest: %s)",
            signal_id,
            len(future_events),
            len(events),
            drift,
            max_future.isoformat(),
        )
        await notify(
            subject=f"Clock drift detected: {source_label} {signal_id}",
            message=(
                f"Signal {signal_id} produced {len(future_events)} of "
                f"{len(events)} events with future timestamps.\n"
                f"Max drift: {drift} "
                f"(latest event: {max_future.isoformat()}).\n"
                f"Server time: {now.isoformat()}\n\n"
                f"Events have been ingested but the checkpoint has been "
                f"capped at server_time + "
                f"{settings.checkpoint_future_tolerance_seconds}s."
            ),
            severity=WARNING,
            metadata={
                "signal_id": signal_id,
                "future_event_count": len(future_events),
                "total_event_count": len(events),
                "max_drift_seconds": drift.total_seconds(),
                "latest_event_time": max_future.isoformat(),
                "alert_type": "clock_drift",
            },
        )

    await _upsert_events(events, signal_id, session_factory)


# ---------------------------------------------------------------------------
# Idempotent insert helper
# ---------------------------------------------------------------------------


async def _upsert_events(
    events,
    signal_id: str,
    session_factory,
) -> None:
    """Bulk-insert events with ON CONFLICT DO NOTHING.

    The composite PK ``(signal_id, event_time, event_code, event_param)``
    on ``controller_event_log`` makes each event naturally unique.
    Duplicates from overlapping polls or re-ingested files are silently
    skipped — zero data loss, zero duplicates.

    Args:
        events: List of ``DecodedEvent`` objects.
        signal_id: Traffic signal identifier.
        session_factory: Async session factory for DB writes.
    """
    rows = [
        {
            "signal_id": signal_id,
            "event_time": e.timestamp,
            "event_code": e.event_code,
            "event_param": e.event_param,
        }
        for e in events
    ]

    stmt = pg_insert(ControllerEventLog).values(rows).on_conflict_do_nothing(
        index_elements=["signal_id", "event_time", "event_code", "event_param"],
    )

    async with session_factory() as session:
        await session.execute(stmt)
        await session.flush()


# ---------------------------------------------------------------------------
# Decoder resolution
# ---------------------------------------------------------------------------


def resolve_decoder_by_name(decoder_name: str):
    """Get a decoder instance by explicit name.

    Args:
        decoder_name: Registered decoder name (e.g. ``"maxtime"``).

    Returns:
        Decoder instance.

    Raises:
        ValueError: If the decoder name is not registered.
    """
    cls = DecoderRegistry.get(decoder_name)
    return cls()


def resolve_decoder_by_extension(
    filename: str,
    *,
    explicit_decoder: Optional[str] = None,
) -> object:
    """Get a decoder instance for a filename, with optional override.

    If ``explicit_decoder`` is given, uses that.  Otherwise, looks up
    decoders by file extension via the ``DecoderRegistry``.

    Args:
        filename: Filename (used for extension lookup).
        explicit_decoder: Explicit decoder name override, or ``None``.

    Returns:
        Decoder instance.

    Raises:
        ValueError: If no decoder can be found.
    """
    if explicit_decoder:
        cls = DecoderRegistry.get(explicit_decoder)
        return cls()

    ext = PurePosixPath(filename).suffix.lower()
    candidates = DecoderRegistry.get_for_extension(ext)
    if not candidates:
        raise ValueError(f"No decoder found for extension '{ext}'")
    return candidates[0]()


# ---------------------------------------------------------------------------
# Checkpoint save (polling plugins)
# ---------------------------------------------------------------------------


async def save_checkpoint(
    method_name: str,
    signal_id: str,
    session_factory,
    **kwargs,
) -> None:
    """Create or update the checkpoint after successful ingest.

    Loads (or creates) the checkpoint row, updates standard fields
    (last_successful_poll, consecutive_errors reset, updated_at), and
    sets any plugin-specific fields passed as keyword arguments.

    Supported kwargs (all optional):
        last_event_timestamp: Capped at server_time + tolerance.
        last_filename: Most recently ingested filename.
        last_file_mtime: Modification time of newest ingested file.
        files_hash: SHA-256 of sorted filenames from directory listing.
        events_ingested: Number of NEW events ingested this cycle (added).
        files_ingested: Number of NEW files ingested this cycle (added).

    Args:
        method_name: Ingestion method name (e.g. ``"http_pull"``).
        signal_id: Traffic signal identifier.
        session_factory: Async session factory.
        **kwargs: Plugin-specific checkpoint fields.
    """
    now = datetime.now(timezone.utc)
    async with session_factory() as session:
        stmt = select(PollingCheckpoint).where(
            PollingCheckpoint.signal_id == signal_id,
            PollingCheckpoint.method == method_name,
        )
        result = await session.execute(stmt)
        checkpoint = result.scalar_one_or_none()

        if checkpoint is None:
            checkpoint = PollingCheckpoint(
                signal_id=signal_id,
                method=method_name,
            )
            session.add(checkpoint)

        # Handle last_event_timestamp with future-capping
        if "last_event_timestamp" in kwargs:
            tolerance = timedelta(
                seconds=settings.checkpoint_future_tolerance_seconds,
            )
            future_cutoff = now + tolerance
            raw_ts = kwargs.pop("last_event_timestamp")
            capped = min(raw_ts, future_cutoff)
            if capped < raw_ts:
                logger.warning(
                    "Signal %s: capping checkpoint from %s to %s "
                    "(future-dated events detected)",
                    signal_id,
                    raw_ts.isoformat(),
                    capped.isoformat(),
                )
            checkpoint.last_event_timestamp = capped

        # Set file-based fields directly
        if "last_filename" in kwargs:
            checkpoint.last_filename = kwargs.pop("last_filename")
        if "last_file_mtime" in kwargs:
            checkpoint.last_file_mtime = kwargs.pop("last_file_mtime")
        if "files_hash" in kwargs:
            checkpoint.files_hash = kwargs.pop("files_hash")

        # Additive counters
        if "events_ingested" in kwargs:
            checkpoint.events_ingested = (
                (checkpoint.events_ingested or 0) + kwargs.pop("events_ingested")
            )
        if "files_ingested" in kwargs:
            checkpoint.files_ingested = (
                (checkpoint.files_ingested or 0) + kwargs.pop("files_ingested")
            )

        # Standard reset fields
        checkpoint.last_successful_poll = now
        checkpoint.consecutive_errors = 0
        checkpoint.last_error = None
        checkpoint.consecutive_silent_cycles = 0
        checkpoint.updated_at = now

        await session.flush()


# ---------------------------------------------------------------------------
# Decode-and-persist helper (listener plugins)
# ---------------------------------------------------------------------------


async def decode_and_persist_message(
    raw: bytes,
    signal_id: str,
    session_factory,
    *,
    decoder_name: str,
    source_label: str = "signal",
) -> int:
    """Resolve decoder, decode bytes, persist with drift check.

    Convenience for listener plugins (NATS, MQTT) that receive raw bytes,
    decode them, and persist events in a single step.

    Args:
        raw: Raw message bytes.
        signal_id: Traffic signal identifier.
        session_factory: Async session factory.
        decoder_name: Decoder name to resolve.
        source_label: Label for drift-check notifications.

    Returns:
        Number of events persisted.
    """
    decoder = resolve_decoder_by_name(decoder_name)
    events = decoder.decode_bytes(raw)
    if events:
        await persist_events_with_drift_check(
            events, signal_id, session_factory, source_label=source_label
        )
    return len(events) if events else 0


__all__ = [
    # checkpoint
    "load_checkpoint",
    "save_checkpoint",
    "record_error",
    # persistence
    "persist_events",
    "persist_events_with_drift_check",
    # decode + persist
    "decode_and_persist_message",
    # decoder resolution
    "resolve_decoder_by_name",
    "resolve_decoder_by_extension",
]
