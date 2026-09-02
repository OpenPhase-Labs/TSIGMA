"""Host-owned ingest orchestrator.

`ingest_raw` runs decode -> normalize -> integrity spine -> persist and returns
an explicit 3-state outcome. Methods hand it raw bytes and never decode,
validate, or persist themselves: ADR-0034 makes `fetch -> decode -> validate ->
persist` a host-owned spine in which "the decoder is a pure transform (bytes ->
events); the host attaches signal_id / device_id / validation_metadata". That is
also what lets an untrusted out-of-process method plugin exist at all.

ADR-0034 governs failure handling here: "Any integrity/poison failure -> ingest +
flag + needs-review + correct-later. Never withhold, drop, or hold data. This
overrides programming-correctness objections." So a payload is never refused
because something about it looks wrong - it is ingested and flagged. As amended,
that binds the host and not the plugin author: once bytes or rows reach this
module they are persisted, and a plugin's declared outcome is evidence to record
and flag, never an instruction to discard data already in hand.

Nothing raises out of `ingest_raw`. Every stage failure becomes an outcome that
names its stage, so a caller always has a verdict to act on.

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
from ..notifications.registry import WARNING, notify
from ..notifications.suppression import is_suppressed
from .decoders.base import DecoderRegistry
from .sdk import (
    ZONE_FROM_SIGNAL,
    check_configured_phases,
    check_controller_replacement,
    check_temporal_integrity,
    is_backward_poisoned,
    persist_events_with_drift_check,
    resolve_decoder_by_extension,
    resolve_decoder_by_name,
    resolve_source_timezone_with_origin,
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
    # Rows the decoder produced, before ON CONFLICT DO NOTHING. Callers need it
    # for checkpoint bookkeeping: duplicates_absorbed = decoded - inserted.
    events_decoded: int = 0
    error: str = ""
    # Which step failed - "decode", "persist", or "" when nothing did. Methods
    # used to distinguish these by having separate try/except blocks; the
    # boundary took that away, so the result carries it instead.
    failed_stage: str = ""

    @property
    def duplicates_absorbed(self) -> int:
        return max(self.events_decoded - self.events_inserted, 0)

    @property
    def advanced(self) -> bool:
        return self.outcome is not IngestOutcome.FAILURE


# Last-resort zone when even the deployment default is missing. UTC is chosen
# because it is the only zone that does not MOVE the timestamp: a misconfigured
# deployment gets today's behaviour plus a flag, never a new and different
# wrongness. Falling back is never silent - see UNRESOLVED_TIMEZONE_REASON.
LAST_RESORT_ZONE = "UTC"

# Review reasons. A deployment-wide misconfiguration recurs on every poll, so
# the timezone row is deduplicated on (signal_id, reason) while it is still
# open - one worklist item per signal, not one per file.
UNRESOLVED_TIMEZONE_REASON = "unresolved_source_timezone"
SPRING_FORWARD_REASON = "spring_forward_gap_time"
DECODE_REVIEW_REASON = "decode_incomplete"

# Stage names carried on IngestResult.failed_stage, and the stem of the review
# reason a terminal failure at that stage writes ("<stage>_failure").
STAGE_DECODE = "decode"
STAGE_TIMEZONE = "timezone"
STAGE_NORMALIZE = "normalize"
STAGE_PERSIST = "persist"
STAGE_INGEST = "ingest"

# How one local wall-clock time sits against the zone's DST transitions.
ORDINARY = "ordinary"
AMBIGUOUS = "ambiguous"    # fall-back: the wall clock reads this hour twice
IMAGINARY = "imaginary"    # spring-forward gap: the wall clock never reads it


def classify_local_time(naive: datetime, tz: ZoneInfo) -> str:
    """Classify one naive local time against `tz`'s DST transitions (PEP 495).

    ``fold=0`` carries the offset in force BEFORE a transition and ``fold=1``
    the one after, so the two disagree only across a transition - and the SIGN
    of the change says which transition: a fall-back shrinks the offset (the
    hour repeats), a spring-forward grows it (the hour never happens).
    """
    before = naive.replace(tzinfo=tz, fold=0).utcoffset()
    after = naive.replace(tzinfo=tz, fold=1).utcoffset()
    if before == after:
        return ORDINARY
    return AMBIGUOUS if before > after else IMAGINARY


@dataclass
class NormalizationReport:
    """What normalization did, so the caller can flag what it could not settle."""

    events: list
    # Local times in a spring-forward gap. Ingested onto the valid side and
    # flagged; never withheld (ADR-0046 flag-never-block).
    gap_times: list
    # Local times in a fall-back hour, where the wall clock alone is not enough
    # to name the instant.
    ambiguous_times: list


def normalize_event_times(events: list, zone: str) -> NormalizationReport:
    """Convert naive (controller-local) event timestamps to UTC, in place.

    Aware timestamps are already absolute and pass through untouched, so a
    decoder that emits both kinds is handled correctly.

    Fold is resolved PER OCCURRENCE. Only a time inside a fall-back hour is
    ambiguous at all, and only a backward step BETWEEN two such times means the
    sequential run has crossed into the second occurrence; the flag clears the
    moment the run leaves that hour. This replaces a latching flag, under which
    one out-of-order pair anywhere - a jittery clock, a merged file, a
    reordered batch - re-read every later ambiguous timestamp an hour earlier.

    Spring-forward gap times are ingested best-effort and never withheld
    (spec 2026-06-06 sec.2.4); they are returned for the caller to flag.
    """
    tz = ZoneInfo(zone)
    utc = timezone.utc
    fold = 0
    previous_ambiguous: datetime | None = None
    gap_times: list = []
    ambiguous_times: list = []

    for event in events:
        ts = event.timestamp
        if ts.tzinfo is not None:
            continue  # already absolute
        kind = classify_local_time(ts, tz)
        if kind == AMBIGUOUS:
            if previous_ambiguous is not None and ts < previous_ambiguous:
                fold = 1
            previous_ambiguous = ts
            ambiguous_times.append(ts)
        else:
            fold = 0
            previous_ambiguous = None
            if kind == IMAGINARY:
                gap_times.append(ts)
        event.timestamp = ts.replace(tzinfo=tz, fold=fold).astimezone(utc)
    return NormalizationReport(events, gap_times, ambiguous_times)


def has_naive_timestamps(events: list) -> bool:
    """True when any event carries a naive (controller-local) timestamp."""
    return any(e.timestamp.tzinfo is None for e in events)


def latest_event_time(events: list, device_id: str) -> datetime | None:
    """Newest event time, or None when the batch is not comparable.

    A decoder emitting both naive and aware timestamps in one batch makes
    ``max`` raise. The advancement policy already reads a missing
    ``max_event_time`` as "no last-good point to move to", which is the safe
    reading, so this degrades to that instead of taking the poller down.
    """
    try:
        return max(e.timestamp for e in events)
    except (TypeError, ValueError):
        logger.warning(
            "%s: event timestamps are not mutually comparable; "
            "no last-good point for the checkpoint", device_id,
        )
        return None


@dataclass
class _Decoded:
    """One decode, whatever produced it.

    Normalizes the in-process and remote paths onto one shape so `ingest_raw`
    has a single downstream flow: a `DecodeResult`, a 3-state outcome, and the
    decoder object (for its name and declared time semantics).
    """

    result: object
    outcome: IngestOutcome
    error: str
    segments_dropped: int
    decoder: object


# DecodeOutcome (contract) -> IngestOutcome (host). UNSPECIFIED is treated as
# FAILURE: a decoder that reports nothing has not reported success.
_DECODE_TO_INGEST = {
    1: IngestOutcome.SUCCESS,
    2: IngestOutcome.PARTIAL,
    3: IngestOutcome.FAILURE,
    0: IngestOutcome.FAILURE,
}


async def _decode_remote(decoder_name: str, raw: bytes, device_id: str) -> "_Decoded":
    """Decode through a gRPC decoder plugin and map its terminal status.

    The plugin streams Arrow batches and closes with exactly one DecodeStatus;
    a stream that ends without one is FAILURE, never success. Transport,
    registration and Arrow-decoding errors are left to raise: the single guard
    at the call site turns every one of them into a named-stage outcome.
    """
    connection = DecoderRegistry.get_connection(decoder_name)
    decoder = getattr(connection, "decoder", None) or connection
    out = await decoder.decode_remote(raw)
    return _Decoded(
        out.result,
        _DECODE_TO_INGEST.get(out.outcome, IngestOutcome.FAILURE),
        out.error,
        out.segments_dropped,
        decoder,
    )


async def _queue_review(
    session_factory,
    device_id: str,
    *,
    reason: str,
    severity: str,
    summary: str,
    detail: dict,
    source_filename: str | None = None,
    dedupe: bool = False,
) -> None:
    """Queue one operator-actionable review row (ADR-0034 ingest + flag).

    The one writer for every finding this module raises: they differ only in
    reason, severity, summary and detail, so they share a body rather than
    growing a near-copy per call site.

    Best-effort by construction - a failure here must never block ingest, which
    is the whole point of never-lose-data. ``dedupe`` collapses a recurring
    deployment-wide misconfiguration onto one open row per (signal_id, reason)
    instead of one per poll.
    """
    try:
        async with session_factory() as session:
            if dedupe:
                existing = (
                    await session.execute(
                        select(IngestReview.review_id).where(
                            IngestReview.signal_id == device_id,
                            IngestReview.reason == reason,
                            IngestReview.status == "open",
                        )
                    )
                ).first()
                if existing is not None:
                    return
            session.add(
                IngestReview(
                    signal_id=device_id,
                    reason=reason,
                    source_filename=source_filename or None,
                    severity=severity,
                    summary=summary,
                    detail=detail,
                    status="open",
                )
            )
            await session.commit()
    except Exception:
        logger.exception(
            "Could not queue the %s review for %s - ingest unaffected",
            reason, device_id,
        )


async def _flag_decode_outcome(session_factory, device_id: str, decoded: "_Decoded",
                               source_filename: str | None) -> None:
    """Queue a review for a PARTIAL or FAILED decode.

    PARTIAL means the decodable rows were persisted and the corrupt remainder
    was not - an operator needs to know a file was only partly read. FAILURE
    means the plugin declared the decode lost; any rows it had already handed
    over are still persisted, and this row is how the disagreement surfaces.
    """
    partial = decoded.outcome is IngestOutcome.PARTIAL
    await _queue_review(
        session_factory, device_id,
        reason=DECODE_REVIEW_REASON,
        severity="warning" if partial else "error",
        summary=(
            "Partial decode: some records unreadable" if partial
            else "Decode failed: no records ingested"
        ),
        detail={
            "outcome": decoded.outcome.value,
            "error": decoded.error,
            "segments_dropped": decoded.segments_dropped,
        },
        source_filename=source_filename,
    )


async def _flag_stage_failure(session_factory, device_id: str, stage: str,
                              error: str, source_filename: str | None) -> None:
    """Queue a review for a terminal failure at one named stage.

    A decode failure has always reached the worklist; a persist, timezone or
    normalization failure is just as operator-actionable and reaches it the
    same way, so an operator sees the file rather than only a log line.
    """
    await _queue_review(
        session_factory, device_id,
        reason=f"{stage}_failure",
        severity="error",
        summary=f"Ingest failed at the {stage} stage",
        detail={"stage": stage, "error": error},
        source_filename=source_filename,
    )


async def _flag_spring_forward_gap(session_factory, device_id: str, zone: str,
                                   gap_times: list, source_filename: str | None) -> None:
    """Queue a review for local times that fall in a spring-forward gap.

    The wall clock never read those times, so the controller's clock did not
    follow the transition. The events are ingested onto the valid side of the
    gap and flagged (ADR-0046 flag-never-block), never dropped.
    """
    await _queue_review(
        session_factory, device_id,
        reason=SPRING_FORWARD_REASON,
        severity="warning",
        summary="Event times fall in a DST spring-forward gap; ingested and flagged",
        detail={
            "zone": zone,
            "gap_event_count": len(gap_times),
            "first_gap_local_time": gap_times[0].isoformat(),
            "last_gap_local_time": gap_times[-1].isoformat(),
        },
        source_filename=source_filename,
    )


async def _flag_unresolved_timezone(session_factory, device_id: str, zone: str,
                                    origin: str, source_filename: str | None) -> None:
    """Queue a review for a signal with no zone of its own.

    `collection.default_timezone` is seeded at every boot, so resolution
    effectively always succeeds and the deployment default silently labels this
    controller's local time with a zone nobody chose for it. That is the
    finding: it reaches the worklist rather than passing unnoticed (ADR-0034,
    superseding the 2026-06-06 "required fallback -> no review" position).
    Deduplicated: the misconfiguration recurs on every poll.
    """
    await _queue_review(
        session_factory, device_id,
        reason=UNRESOLVED_TIMEZONE_REASON,
        severity="warning",
        summary=f"No source timezone set for this signal; local times ingested as {zone}",
        detail={
            "assumed_zone": zone,
            "zone_origin": origin,
            "fix": (
                "Set Signal.source_timezone for this signal, or the "
                "collection.default_timezone deployment setting."
            ),
        },
        source_filename=source_filename,
        dedupe=True,
    )


async def ingest_raw(
    raw: bytes,
    *,
    device_id: str,
    session_factory,
    decoder_name: str | None = None,
    filename: str | None = None,
    source_label: str = "signal",
    device_type: str | None = None,
    last_successful_poll: datetime | None = None,
) -> IngestResult:
    """Decode, normalize, run the integrity spine, and persist one raw payload.

    Exactly one of ``decoder_name`` (explicit) or ``filename`` (extension-based)
    selects the decoder, matching `IngestionTarget.resolve_decoder`.

    ``device_type`` is the caller's stated device class ("controller" /
    "sensor"). Persistence routes on it; only a caller that states nothing
    falls back to inferring the destination from the event element type.

    Returns an explicit outcome rather than a bare count so the caller's
    advancement policy can distinguish "nothing to do" from "do not advance".

    NOTHING raises out of here. Every stage failure becomes an outcome naming
    the stage it failed at, because an escaping exception leaves a watched file
    unquarantined and reprocessed forever, and leaves a checkpoint holder with
    no verdict to act on. This outer guard is the backstop for a stage nobody
    enumerated; the named stages guard themselves.
    """
    try:
        return await _run_ingest(
            raw,
            device_id=device_id,
            session_factory=session_factory,
            decoder_name=decoder_name,
            filename=filename,
            source_label=source_label,
            device_type=device_type,
            last_successful_poll=last_successful_poll,
        )
    except Exception as exc:
        logger.exception("%s: ingest failed outside a guarded stage", device_id)
        return IngestResult(
            IngestOutcome.FAILURE, 0, error=str(exc), failed_stage=STAGE_INGEST,
        )


async def _run_ingest(
    raw: bytes,
    *,
    device_id: str,
    session_factory,
    decoder_name: str | None,
    filename: str | None,
    source_label: str,
    device_type: str | None,
    last_successful_poll: datetime | None,
) -> IngestResult:
    """The staged spine behind `ingest_raw`; every stage names itself on failure."""
    # A review row identifies the SOURCE. The decoder name is not a source: an
    # operator handed "maxtime" cannot find the file it came from.
    source_filename = filename
    hint = decoder_name or filename  # for error text only, never for a row

    # A name registered over gRPC resolves to a plugin; otherwise the in-process
    # registry. Dispatch is per name (ADR-0018 coexistence), so both kinds run
    # side by side and callers above never learn which answered.
    if decoder_name and DecoderRegistry.is_remote(decoder_name):
        try:
            decoded = await _decode_remote(decoder_name, raw, device_id)
        except Exception as exc:
            # A transport error, a malformed Arrow batch, an unregistered
            # connection: the plugin plane fails like any other stage, and
            # never by escaping into the caller's poll loop.
            logger.exception("%s: remote decode failed for %s", decoder_name, device_id)
            decoded = _Decoded(
                None, IngestOutcome.FAILURE, f"remote decode failed: {exc}", 0, None,
            )
    else:
        try:
            decoder = (
                resolve_decoder_by_name(decoder_name)
                if decoder_name
                else resolve_decoder_by_extension(filename)
            )
        except (ValueError, KeyError) as exc:
            decoded = _Decoded(
                None, IngestOutcome.FAILURE, f"decoder {hint!r}: {exc}", 0, None,
            )
        else:
            try:
                decoded = _Decoded(decoder.decode(raw), IngestOutcome.SUCCESS, "", 0, decoder)
            except Exception as exc:  # a bad file must not take the poller down
                logger.exception("%s: decode failed for %s", decoder_name, device_id)
                decoded = _Decoded(
                    None, IngestOutcome.FAILURE, f"decode failed: {exc}", 0, None,
                )

    result = decoded.result
    events = list(getattr(result, "events", None) or [])
    if source_filename is None:
        source_filename = getattr(getattr(result, "metadata", None), "source_filename", None)

    if not events:
        # The decode outcome is consulted BEFORE the empty-poll shortcut. An
        # empty poll is only normal when the decoder said so: a PARTIAL or
        # FAILURE reported as SUCCESS here advances the checkpoint straight
        # past data nobody read.
        if decoded.outcome is IngestOutcome.SUCCESS:
            return IngestResult(IngestOutcome.SUCCESS, 0, events_decoded=0)
        await _flag_decode_outcome(session_factory, device_id, decoded, source_filename)
        return IngestResult(
            decoded.outcome, 0,
            events_decoded=0,
            error=decoded.error,
            failed_stage=_failed_stage_for(decoded.outcome),
        )

    if has_naive_timestamps(events):
        try:
            async with session_factory() as session:
                zone, origin = await resolve_source_timezone_with_origin(device_id, session)
        except Exception as exc:
            logger.exception("%s: source timezone lookup failed", device_id)
            await _flag_stage_failure(
                session_factory, device_id, STAGE_TIMEZONE, str(exc), source_filename,
            )
            return IngestResult(
                IngestOutcome.FAILURE, 0,
                error=f"timezone lookup failed: {exc}", failed_stage=STAGE_TIMEZONE,
            )

        if origin != ZONE_FROM_SIGNAL:
            # Ingest and flag, never withhold (ADR-0034). The deployment
            # default answering for a controller is a finding in its own right,
            # not a resolution: it labels controller-local time with a zone
            # nobody chose for this signal.
            zone = zone or LAST_RESORT_ZONE
            logger.warning(
                "%s: no per-signal source timezone; ingesting local timestamps as %s "
                "(origin %s). Set Signal.source_timezone or collection.default_timezone.",
                device_id, zone, origin,
            )
            await _flag_unresolved_timezone(
                session_factory, device_id, zone, origin, source_filename,
            )

        try:
            report = normalize_event_times(events, zone)
        except Exception as exc:
            # Signal.source_timezone is unvalidated free text and reaches
            # ZoneInfo here, so a malformed IANA identifier surfaces as a
            # ZoneInfoNotFoundError (a KeyError) at this stage and no other.
            logger.exception("%s: could not normalize event times in zone %r", device_id, zone)
            await _flag_stage_failure(
                session_factory, device_id, STAGE_NORMALIZE, str(exc), source_filename,
            )
            return IngestResult(
                IngestOutcome.FAILURE, 0,
                error=f"normalize failed in zone {zone!r}: {exc}",
                failed_stage=STAGE_NORMALIZE,
            )
        if report.gap_times:
            await _flag_spring_forward_gap(
                session_factory, device_id, zone, report.gap_times, source_filename,
            )

    try:
        await flag_if_backward_poison(
            events, device_id, source_filename or "", last_successful_poll,
        )
    except Exception:
        # The poison flag is advisory; never-lose-data outranks it. A batch
        # mixing naive and aware times makes the comparison raise.
        logger.exception(
            "%s: backward-poison check failed - events still ingested", device_id,
        )

    # After normalization on purpose: check_temporal_integrity compares event
    # times to server UTC, so running it on unconverted local timestamps
    # false-trips it - one of the symptoms the timezone bug produces today.
    try:
        await validate_and_record_provenance(
            session_factory, device_id, getattr(result, "metadata", None),
            getattr(decoded.decoder, "name", decoder_name), events,
        )
    except Exception:
        # never-lose-data outranks provenance: events still get persisted.
        logger.exception(
            "%s: validation/provenance step failed - events still ingested", device_id,
        )

    try:
        inserted = await persist_events_with_drift_check(
            events, device_id, session_factory,
            source_label=source_label, device_type=device_type,
        )
    except Exception as exc:
        # A persist failure is FAILURE, not an exception: callers decide file
        # disposition from the outcome, and an escaping exception would leave a
        # watched file unquarantined and reprocessed forever.
        logger.exception("%s: persist failed", device_id)
        await _flag_stage_failure(
            session_factory, device_id, STAGE_PERSIST, str(exc), source_filename,
        )
        return IngestResult(
            IngestOutcome.FAILURE, 0,
            error=f"persist failed: {exc}", failed_stage=STAGE_PERSIST,
        )

    if decoded.outcome is not IngestOutcome.SUCCESS:
        # Rows the plugin already handed over are persisted whatever it then
        # declared: its outcome is evidence to record and flag, never an
        # instruction to destroy data already in hand (ADR-0034 as amended).
        await _flag_decode_outcome(session_factory, device_id, decoded, source_filename)

    return IngestResult(
        decoded.outcome,
        inserted or 0,
        max_event_time=latest_event_time(events, device_id),
        events_decoded=len(events),
        error=decoded.error,
        failed_stage=_failed_stage_for(decoded.outcome),
    )


def _failed_stage_for(outcome: IngestOutcome) -> str:
    """The stage a decode verdict blames, or "" when nothing failed."""
    return STAGE_DECODE if outcome is IngestOutcome.FAILURE else ""


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


async def flag_if_backward_poison(
    events,
    device_id: str,
    filename: str,
    last_successful_poll: datetime | None,
) -> None:
    """Flag a backward-poisoned batch for review (log + notify).

    Never blocks ingest — callers always persist. A slow/reset controller
    clock (newest event before the last poll) is flagged, not dropped
    (spec principle: ingest + flag for review).
    """
    if not is_backward_poisoned(events, last_successful_poll):
        return
    last_polled_str = (
        last_successful_poll.isoformat() if last_successful_poll else "n/a"
    )
    logger.warning(
        "Backward-poisoned batch from %s for %s: newest event predates "
        "last poll (%s) — ingesting and flagging for review",
        filename, device_id, last_polled_str,
    )
    try:
        await notify(
            subject=f"Clock backward-poison: {device_id}",
            message=(
                f"Device {device_id} file {filename}: the newest event timestamp "
                f"predates the last successful poll ({last_polled_str}) — a slow "
                f"or reset controller clock. Events were ingested and flagged "
                f"for review."
            ),
            severity=WARNING,
            metadata={
                "signal_id": device_id,
                "file": filename,
                "alert_type": "clock_backward_poison",
            },
        )
    except Exception:
        logger.exception(
            "Failed to send backward-poison notification for %s (%s) — "
            "events still ingested",
            filename, device_id,
        )
