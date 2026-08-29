"""Host-owned ingest orchestrator.

`ingest_raw` runs decode -> normalize -> persist and returns an explicit 3-state
outcome. Methods hand it raw bytes and never decode, validate, or persist
themselves: ADR-0034 makes `fetch -> decode -> validate -> persist` a host-owned
spine in which "the decoder is a pure transform (bytes -> events); the host
attaches signal_id / device_id / validation_metadata". That is also what lets an
untrusted out-of-process method plugin exist at all.

ADR-0034 governs failure handling here: "Any integrity/poison failure -> ingest +
flag + needs-review + correct-later. Never withhold, drop, or hold data. This
overrides programming-correctness objections." So a payload is never refused
because something about it looks wrong - it is ingested and flagged.

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
from uuid import uuid4
from zoneinfo import ZoneInfo

from sqlalchemy import select

from ..config import settings
from ..models.approach import Approach
from ..models.file_provenance import FileIngestProvenance
from ..models.ingest_review import IngestReview
from ..models.signal import Signal
from ..notifications.registry import notify
from ..notifications.suppression import is_suppressed
from .sdk import (
    check_configured_phases,
    check_controller_replacement,
    check_temporal_integrity,
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


# Last-resort zone when even the deployment default is missing. The spec makes
# collection.default_timezone a REQUIRED fallback so resolution always succeeds
# (no quarantine). UTC is chosen because it is the only zone that does not MOVE
# the timestamp: a misconfigured deployment gets today's behaviour plus a flag,
# never a new and different wrongness.
LAST_RESORT_ZONE = "UTC"

# Review reason for the missing-zone finding. A deployment-wide misconfiguration
# recurs on every poll, so the row is deduplicated on (signal_id, reason) while
# it is still open - one worklist item per signal, not one per file.
UNRESOLVED_TIMEZONE_REASON = "unresolved_source_timezone"


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
            # Ingest and flag, never withhold (ADR-0034 never-lose-data; spec
            # sec.3 rules out an "unresolvable zone" quarantine). Reaching here
            # means collection.default_timezone is missing, which is a
            # misconfiguration to surface - not a reason to drop the file.
            zone = LAST_RESORT_ZONE
            logger.warning(
                "%s: no source timezone resolved; ingesting local timestamps as %s. "
                "Set Signal.source_timezone or collection.default_timezone.",
                device_id, LAST_RESORT_ZONE,
            )
            await _flag_unresolved_timezone(session_factory, device_id)
        normalize_event_times(events, zone)

    # After normalization on purpose: check_temporal_integrity compares event
    # times to server UTC, so running it on unconverted local timestamps
    # false-trips it - one of the symptoms the timezone bug produces today.
    await validate_and_record_provenance(
        session_factory, device_id, result.metadata, decoder_name, events
    )

    inserted = await persist_events_with_drift_check(
        events, device_id, session_factory, source_label=source_label
    )
    return IngestResult(
        IngestOutcome.SUCCESS,
        inserted or 0,
        max_event_time=max(e.timestamp for e in events),
    )


async def validate_and_record_provenance(
    session_factory,
    device_id: str,
    metadata,
    decoder_name,
    events=None,
) -> None:
    """Validate header identity/config and record file provenance.

    Best-effort and NON-BLOCKING: any failure is logged and swallowed so
    ingest always proceeds. Detects controller replacement (MAC change at a
    reused IP) and config-phase drift, flags them (suppressible, best-effort
    notify), seeds an absent registered MAC on first sight, and writes one
    provenance row per ingested file.
    """
    if metadata is None:
        return
    provenance_id = uuid4()
    try:
        async with session_factory() as session:
            signal = await session.get(Signal, device_id)
            findings = []
            if signal is not None:
                registered_mac = (signal.signal_metadata or {}).get(
                    "controller_mac",
                )
                if not registered_mac and metadata.device_mac:
                    # First sight: silently seed the registered MAC
                    # (reassign dict — JSONB is not MutableDict, in-place
                    # edits aren't tracked).
                    signal.signal_metadata = {
                        **(signal.signal_metadata or {}),
                        "controller_mac": metadata.device_mac,
                    }
                else:
                    replacement = check_controller_replacement(
                        metadata, registered_mac,
                    )
                    if replacement is not None:
                        findings.append(replacement)
                rows = (
                    await session.execute(
                        select(
                            Approach.protected_phase_number,
                            Approach.permissive_phase_number,
                            Approach.ped_phase_number,
                        ).where(Approach.signal_id == device_id)
                    )
                ).all()
                configured = {p for row in rows for p in row if p is not None}
                drift = check_configured_phases(metadata, configured)
                if drift is not None:
                    findings.append(drift)
            if events:
                server_now = datetime.now(timezone.utc)
                temporal = check_temporal_integrity(
                    events, server_now,
                    settings.checkpoint_future_tolerance_seconds,
                )
                if temporal is not None:
                    findings.append(temporal)
                    session.add(
                        IngestReview(
                            provenance_id=provenance_id,
                            signal_id=device_id,
                            reason=temporal.check_name,
                            source_filename=metadata.source_filename,
                            severity=temporal.severity,
                            summary=temporal.summary,
                            detail=temporal.detail,
                            status="open",
                        )
                    )
            for finding in findings:
                if await is_suppressed(session, device_id, finding.check_name):
                    continue
                logger.warning(
                    "Integrity finding %s for %s: %s",
                    finding.check_name, device_id, finding.summary,
                )
                try:
                    await notify(
                        subject=f"{finding.summary}: {device_id}",
                        message=(
                            f"Device {device_id} file "
                            f"{metadata.source_filename}: {finding.summary}."
                        ),
                        severity=finding.severity,
                        metadata={
                            "signal_id": device_id,
                            "alert_type": finding.check_name,
                            **finding.detail,
                        },
                    )
                except Exception:
                    logger.exception(
                        "Failed to notify integrity finding %s for %s — "
                        "events still ingested",
                        finding.check_name, device_id,
                    )
            session.add(
                FileIngestProvenance(
                    provenance_id=provenance_id,
                    signal_id=device_id,
                    source_filename=metadata.source_filename,
                    device_ip=metadata.device_ip,
                    device_mac=metadata.device_mac,
                    log_version=metadata.log_version,
                    phases_in_use=metadata.phases_in_use,
                    log_begin=metadata.log_begin,
                    header_anchor=metadata.header_anchor,
                    decoder_name=decoder_name,
                )
            )
            await session.commit()
    except Exception:
        logger.exception(
            "Identity/provenance validation failed for %s (%s) — "
            "events still ingested",
            getattr(metadata, "source_filename", None), device_id,
        )


async def _flag_unresolved_timezone(session_factory, device_id: str) -> None:
    """Queue an operator-actionable review for a signal with no resolvable zone.

    ADR-0034 requires ingest + flag + needs-review; a missing
    `collection.default_timezone` is operator-actionable, so it belongs in the
    worklist rather than only the log. Best-effort: a failure here must never
    block ingest, which is the whole point of never-lose-data.
    """
    try:
        async with session_factory() as session:
            existing = (
                await session.execute(
                    select(IngestReview.review_id).where(
                        IngestReview.signal_id == device_id,
                        IngestReview.reason == UNRESOLVED_TIMEZONE_REASON,
                        IngestReview.status == "open",
                    )
                )
            ).first()
            if existing is not None:
                return
            session.add(
                IngestReview(
                    signal_id=device_id,
                    reason=UNRESOLVED_TIMEZONE_REASON,
                    severity="warning",
                    summary="No source timezone resolved; local timestamps ingested as UTC",
                    detail={
                        "assumed_zone": LAST_RESORT_ZONE,
                        "fix": (
                            "Set Signal.source_timezone for this signal, or the "
                            "collection.default_timezone deployment setting."
                        ),
                    },
                    status="open",
                )
            )
            await session.commit()
    except Exception:
        logger.exception(
            "Could not queue the unresolved-timezone review for %s - events still ingested",
            device_id,
        )
