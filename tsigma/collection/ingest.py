"""Host-owned ingest orchestrator.

`ingest_raw` runs decode -> normalize -> persist and returns an explicit 3-state
outcome. Methods hand it raw bytes and never decode, validate, or persist
themselves - that is the host's integrity spine (ADR-0034), and it is what lets an
untrusted out-of-process method plugin exist at all.

Time normalization lives here, not in decoders. A decoder emits NAIVE datetimes
when its source is controller-local and tz-aware UTC when the source is absolute
(an epoch, a protobuf Timestamp, a server arrival time). This module converts the
naive ones using the signal's resolved zone. Carrying the fact in the timestamp
rather than a per-class flag is what lets one decoder emit both - `maxtime.py`
returns local from its XML path and epoch-UTC from its binary path.
"""

import enum
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from .sdk import (
    persist_events_with_drift_check,
    resolve_decoder_by_name,
    resolve_source_timezone,
)

logger = logging.getLogger(__name__)


class IngestOutcome(str, enum.Enum):
    """Terminal verdict on one ingest. Mirrors the contract's IngestOutcome."""

    SUCCESS = "success"   # decoded and persisted; advance the checkpoint
    PARTIAL = "partial"   # some rows persisted, remainder flagged; advance to max_event_time
    FAILURE = "failure"   # nothing persisted; do NOT advance, record_error, alert


@dataclass
class IngestResult:
    """What `ingest_raw` returns; the input to the checkpoint-advancement policy."""

    outcome: IngestOutcome
    events_inserted: int
    max_event_time: datetime | None = None
    error: str = ""

    @property
    def advanced(self) -> bool:
        return self.outcome is not IngestOutcome.FAILURE


class UnresolvedTimezoneError(RuntimeError):
    """A decoder emitted naive timestamps and no source zone could be resolved."""


def normalize_event_times(events: list, zone: str) -> list:
    """Convert naive (controller-local) event timestamps to UTC, in place.

    Aware timestamps are already absolute and pass through untouched, so a
    decoder that emits both kinds is handled correctly.

    Fold: hi-res logs are sequential, so a local timestamp moving BACKWARDS marks
    the DST fall-back crossing; everything after it resolves as ``fold=1``. The
    whole ordered run is seen here, so a crossing on a file boundary is caught.

    Spring-forward gap times are ingested best-effort and never withheld
    (spec 2026-06-06 sec.2.4; ADR-0046 flag-never-block).
    """
    tz = ZoneInfo(zone)
    utc = timezone.utc
    fold = 0
    previous: datetime | None = None

    for event in events:
        ts = event.timestamp
        if ts.tzinfo is not None:
            continue  # already absolute
        if previous is not None and ts < previous:
            fold = 1
        previous = ts
        event.timestamp = ts.replace(tzinfo=tz, fold=fold).astimezone(utc)
    return events


def has_naive_timestamps(events: list) -> bool:
    """True when any event carries a naive (controller-local) timestamp."""
    return any(e.timestamp.tzinfo is None for e in events)


async def ingest_raw(
    raw: bytes,
    *,
    device_id: str,
    decoder_name: str,
    session_factory,
    source_label: str = "signal",
) -> IngestResult:
    """Decode, normalize, and persist one raw payload.

    Returns an explicit outcome rather than a bare count so the caller's
    advancement policy can distinguish "nothing to do" from "do not advance".
    """
    try:
        decoder = resolve_decoder_by_name(decoder_name)
    except (ValueError, KeyError) as exc:
        return IngestResult(IngestOutcome.FAILURE, 0, error=f"decoder {decoder_name!r}: {exc}")

    try:
        result = decoder.decode(raw)
    except Exception as exc:  # a bad file must not take the poller down
        logger.exception("%s: decode failed for %s", decoder_name, device_id)
        return IngestResult(IngestOutcome.FAILURE, 0, error=f"decode failed: {exc}")

    events = list(result.events or [])
    if not events:
        # Nothing decoded is not a failure - an empty poll is normal.
        return IngestResult(IngestOutcome.SUCCESS, 0)

    if has_naive_timestamps(events):
        async with session_factory() as session:
            zone = await resolve_source_timezone(device_id, session)
        if zone is None:
            # Never guess a zone: storing an unconverted local time as UTC is the
            # bug this exists to prevent.
            return IngestResult(
                IngestOutcome.FAILURE,
                0,
                error=(
                    f"{device_id}: decoder emitted local timestamps and no source "
                    "timezone resolved (Signal.source_timezone / "
                    "collection.default_timezone)"
                ),
            )
        normalize_event_times(events, zone)

    inserted = await persist_events_with_drift_check(
        events, device_id, session_factory, source_label=source_label
    )
    return IngestResult(
        IngestOutcome.SUCCESS,
        inserted or 0,
        max_event_time=max(e.timestamp for e in events),
    )
