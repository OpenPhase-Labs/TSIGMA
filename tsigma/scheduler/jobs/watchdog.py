"""
Data quality and detector health watchdog.

Runs daily to identify a range of data-quality problems: silent signals,
stuck detectors, low event volume, stale data windows, stuck pedestrian
buttons, phase-termination anomalies, and per-detector low hit counts.
Findings are logged and (unless suppressed via ``alert_suppression``)
delivered via the notification registry.

This job does not modify any operational data.
"""

import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from tsigma.config import settings
from tsigma.models.alert_suppression import AlertSuppression
from tsigma.models.event import ControllerEventLog
from tsigma.notifications import notify
from tsigma.reports.sdk.events import (
    EVENT_DETECTOR_ON,
    EVENT_PED_CALL,
    EVENT_PHASE_GREEN,
)
from tsigma.scheduler.registry import JobRegistry

logger = logging.getLogger(__name__)

# A detector reporting more than this many ON events (code 82) in one hour
# is likely stuck in the active state.
STUCK_DETECTOR_THRESHOLD = 3600

# Check-name identifiers used by the alert-suppression table.
CHECK_SILENT_SIGNAL = "silent_signal"
CHECK_STUCK_DETECTOR = "stuck_detector"
CHECK_LOW_EVENT_COUNT = "low_event_count"
CHECK_MISSING_DATA_WINDOW = "missing_data_window"
CHECK_STUCK_PED = "stuck_ped"
CHECK_PHASE_TERMINATION_ANOMALY = "phase_termination_anomaly"
CHECK_LOW_HIT_COUNT = "low_hit_count"


@JobRegistry.register(name="watchdog", trigger="cron", hour="6", minute="0")
async def watchdog(session: AsyncSession) -> None:
    """Dispatch every watchdog data-quality check, isolating failures."""
    checks = (
        ("Silent-signal", _check_silent_signals),
        ("Stuck-detector", _check_stuck_detectors),
        ("Low-event-count", _check_low_event_count),
        ("Missing-data-window", _check_missing_data_window),
        ("Stuck-ped", _check_stuck_ped),
        ("Phase-termination-anomaly", _check_phase_termination_anomaly),
        ("Low-hit-count", _check_low_hit_count),
    )
    for label, check in checks:
        try:
            await check(session)
        except Exception:
            logger.exception("%s check failed", label)


# ---------------------------------------------------------------------------
# Alert suppression helper
# ---------------------------------------------------------------------------


async def _is_suppressed(
    session: AsyncSession, signal_id: str | None, check_name: str,
) -> bool:
    """
    Return True if an unexpired suppression rule covers ``(signal_id, check_name)``.

    A rule with NULL ``signal_id`` suppresses every signal for that check.
    A rule with NULL ``expires_at`` never expires.

    Fails open — database errors are logged and treated as not-suppressed so
    a broken table never silences real alerts.
    """
    now = datetime.now(timezone.utc)
    stmt = (
        select(func.count())
        .select_from(AlertSuppression)
        .where(
            AlertSuppression.check_name == check_name,
            (AlertSuppression.signal_id.is_(None))
            | (AlertSuppression.signal_id == signal_id),
            (AlertSuppression.expires_at.is_(None))
            | (AlertSuppression.expires_at > now),
        )
    )
    try:
        result = await session.execute(stmt)
        count = result.scalar() or 0
    except Exception:
        logger.exception(
            "alert_suppression lookup failed for (%s, %s) — failing open",
            signal_id, check_name,
        )
        return False
    return count > 0


async def _partition_suppressed(
    session: AsyncSession,
    rows: list,
    check_name: str,
    signal_attr: str = "signal_id",
) -> tuple[list, list]:
    """Split ``rows`` into (deliverable, suppressed) by suppression rule."""
    delivered: list = []
    suppressed: list = []
    for row in rows:
        signal_id = getattr(row, signal_attr, None)
        if await _is_suppressed(session, signal_id, check_name):
            suppressed.append(row)
        else:
            delivered.append(row)
    return delivered, suppressed


# ---------------------------------------------------------------------------
# 1. Silent signals (existing)
# ---------------------------------------------------------------------------


async def _check_silent_signals(session: AsyncSession) -> None:
    """Warn about signals with no events in the last 24 hours."""
    cutoff = datetime.now(timezone.utc) - timedelta(hours=24)

    latest = (
        select(
            ControllerEventLog.signal_id,
            func.max(ControllerEventLog.event_time).label("last_event"),
        )
        .group_by(ControllerEventLog.signal_id)
        .subquery()
    )
    stmt = select(latest.c.signal_id, latest.c.last_event).where(
        latest.c.last_event < cutoff
    )
    result = await session.execute(stmt)
    rows = result.all()

    if not rows:
        logger.info("No silent signals detected")
        return

    delivered, suppressed = await _partition_suppressed(
        session, rows, CHECK_SILENT_SIGNAL,
    )
    for row in rows:
        logger.warning(
            "Silent signal: %s — last event at %s", row.signal_id, row.last_event,
        )
    if suppressed:
        logger.info("Suppressed %d silent-signal alert(s)", len(suppressed))
    if not delivered:
        return

    await notify(
        subject="Silent Signals Detected",
        message=(
            f"{len(delivered)} signal(s) have not reported events in 24+ hours:\n"
            + "\n".join(f"  - {r.signal_id} (last: {r.last_event})" for r in delivered)
        ),
        severity="warning",
        metadata={"signal_ids": [r.signal_id for r in delivered]},
    )


# ---------------------------------------------------------------------------
# 2. Stuck detectors (existing)
# ---------------------------------------------------------------------------


async def _check_stuck_detectors(session: AsyncSession) -> None:
    """Warn about detector channels with excessive ON events in the last hour."""
    cutoff = datetime.now(timezone.utc) - timedelta(hours=1)

    stmt = (
        select(
            ControllerEventLog.signal_id,
            ControllerEventLog.event_param.label("detector_channel"),
            func.count().label("on_count"),
        )
        .where(
            ControllerEventLog.event_code == EVENT_DETECTOR_ON,
            ControllerEventLog.event_time >= cutoff,
        )
        .group_by(ControllerEventLog.signal_id, ControllerEventLog.event_param)
        .having(func.count() > STUCK_DETECTOR_THRESHOLD)
    )
    result = await session.execute(stmt)
    rows = result.all()

    if not rows:
        logger.info("No stuck detectors detected")
        return

    delivered, suppressed = await _partition_suppressed(
        session, rows, CHECK_STUCK_DETECTOR,
    )
    for row in rows:
        logger.warning(
            "Possible stuck detector: signal=%s channel=%d — %d ON events in last hour",
            row.signal_id, row.detector_channel, row.on_count,
        )
    if suppressed:
        logger.info("Suppressed %d stuck-detector alert(s)", len(suppressed))
    if not delivered:
        return

    await notify(
        subject="Stuck Detectors Detected",
        message=(
            f"{len(delivered)} detector(s) appear stuck:\n"
            + "\n".join(
                f"  - Signal {r.signal_id} channel {r.detector_channel}"
                f" ({r.on_count} events/hr)"
                for r in delivered
            )
        ),
        severity="warning",
        metadata={
            "detectors": [
                {"signal_id": r.signal_id, "channel": r.detector_channel}
                for r in delivered
            ]
        },
    )


# ---------------------------------------------------------------------------
# 3. Low event count detection
# ---------------------------------------------------------------------------


async def _check_low_event_count(session: AsyncSession) -> None:
    """Flag signals with fewer than ``watchdog_low_event_count_threshold`` events / hr."""
    cutoff = datetime.now(timezone.utc) - timedelta(hours=1)
    threshold = settings.watchdog_low_event_count_threshold

    stmt = (
        select(
            ControllerEventLog.signal_id,
            func.count().label("event_count"),
        )
        .where(ControllerEventLog.event_time >= cutoff)
        .group_by(ControllerEventLog.signal_id)
        .having(func.count() < threshold)
    )
    result = await session.execute(stmt)
    rows = result.all()

    if not rows:
        logger.info("No low-event-count signals detected")
        return

    delivered, suppressed = await _partition_suppressed(
        session, rows, CHECK_LOW_EVENT_COUNT,
    )
    for row in rows:
        logger.warning(
            "Low event count: signal=%s events=%d (threshold=%d, last hour)",
            row.signal_id, row.event_count, threshold,
        )
    if suppressed:
        logger.info("Suppressed %d low-event-count alert(s)", len(suppressed))
    if not delivered:
        return

    await notify(
        subject="Low Event Volume Detected",
        message=(
            f"{len(delivered)} signal(s) reported fewer than {threshold} "
            f"events in the last hour:\n"
            + "\n".join(
                f"  - {r.signal_id} ({r.event_count} events/hr)" for r in delivered
            )
        ),
        severity="warning",
        metadata={
            "threshold": threshold,
            "signals": [
                {"signal_id": r.signal_id, "count": r.event_count} for r in delivered
            ],
        },
    )


# ---------------------------------------------------------------------------
# 4. Missing data window detection
# ---------------------------------------------------------------------------


async def _check_missing_data_window(session: AsyncSession) -> None:
    """Flag signals whose last event is older than the configured gap."""
    gap_minutes = settings.watchdog_missing_window_minutes
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=gap_minutes)

    latest = (
        select(
            ControllerEventLog.signal_id,
            func.max(ControllerEventLog.event_time).label("last_event"),
        )
        .group_by(ControllerEventLog.signal_id)
        .subquery()
    )
    stmt = select(latest.c.signal_id, latest.c.last_event).where(
        latest.c.last_event < cutoff
    )
    result = await session.execute(stmt)
    rows = result.all()

    if not rows:
        logger.info("No missing-data-window signals detected")
        return

    delivered, suppressed = await _partition_suppressed(
        session, rows, CHECK_MISSING_DATA_WINDOW,
    )
    for row in rows:
        logger.warning(
            "Missing data window: signal=%s last_event=%s (gap > %d min)",
            row.signal_id, row.last_event, gap_minutes,
        )
    if suppressed:
        logger.info("Suppressed %d missing-data-window alert(s)", len(suppressed))
    if not delivered:
        return

    await notify(
        subject="Missing Data Window Detected",
        message=(
            f"{len(delivered)} signal(s) have a data gap longer than "
            f"{gap_minutes} minutes:\n"
            + "\n".join(
                f"  - {r.signal_id} (last: {r.last_event})" for r in delivered
            )
        ),
        severity="warning",
        metadata={
            "gap_minutes": gap_minutes,
            "signal_ids": [r.signal_id for r in delivered],
        },
    )


# ---------------------------------------------------------------------------
# 5. Stuck pedestrian button detection
# ---------------------------------------------------------------------------


async def _check_stuck_ped(session: AsyncSession) -> None:
    """
    Flag ped phases emitting ped-call events continuously for longer than
    ``watchdog_stuck_ped_minutes``.

    We look at ped-call (code ``EVENT_PED_CALL``) event volume per
    (signal, ped_phase) over the configured window. A count above the
    rate-of-one-every-30-seconds rule-of-thumb is taken as "stuck".
    """
    minutes = settings.watchdog_stuck_ped_minutes
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=minutes)
    # Stuck threshold: a healthy ped button rarely exceeds ~2 calls/min;
    # anything above 2 * minutes is clearly excessive.
    call_threshold = minutes * 2

    stmt = (
        select(
            ControllerEventLog.signal_id,
            ControllerEventLog.event_param.label("ped_phase"),
            func.count().label("call_count"),
        )
        .where(
            ControllerEventLog.event_code == EVENT_PED_CALL,
            ControllerEventLog.event_time >= cutoff,
        )
        .group_by(ControllerEventLog.signal_id, ControllerEventLog.event_param)
        .having(func.count() > call_threshold)
    )
    result = await session.execute(stmt)
    rows = result.all()

    if not rows:
        logger.info("No stuck pedestrian buttons detected")
        return

    delivered, suppressed = await _partition_suppressed(
        session, rows, CHECK_STUCK_PED,
    )
    for row in rows:
        logger.warning(
            "Possible stuck ped button: signal=%s ped_phase=%d calls=%d in %d min",
            row.signal_id, row.ped_phase, row.call_count, minutes,
        )
    if suppressed:
        logger.info("Suppressed %d stuck-ped alert(s)", len(suppressed))
    if not delivered:
        return

    await notify(
        subject="Stuck Pedestrian Buttons Detected",
        message=(
            f"{len(delivered)} pedestrian phase(s) show continuous calls "
            f"over the last {minutes} minutes:\n"
            + "\n".join(
                f"  - Signal {r.signal_id} ped_phase {r.ped_phase}"
                f" ({r.call_count} calls)"
                for r in delivered
            )
        ),
        severity="warning",
        metadata={
            "ped_phases": [
                {"signal_id": r.signal_id, "phase": r.ped_phase}
                for r in delivered
            ]
        },
    )


# ---------------------------------------------------------------------------
# 6. Phase termination anomaly detection
# ---------------------------------------------------------------------------


async def _check_phase_termination_anomaly(session: AsyncSession) -> None:
    """
    Flag phases whose last-hour termination mix (gap-out / max-out / force-off
    ratios) deviates from its 7-day baseline by more than
    ``watchdog_termination_anomaly_stddev`` standard deviations.

    The session execute returns rows shaped::

        (signal_id, phase,
         recent_gap_ratio, recent_max_ratio, recent_force_ratio,
         baseline_gap_mean, baseline_gap_stddev,
         baseline_max_mean, baseline_max_stddev,
         baseline_force_mean, baseline_force_stddev)

    Computing the hour-by-hour stddev across the 7-day baseline in SQL is
    dialect-heavy, so we delegate the aggregation to the caller via a
    single query assembled from the phase_termination_hourly aggregate.
    """
    threshold = settings.watchdog_termination_anomaly_stddev
    now = datetime.now(timezone.utc)
    recent_since = now - timedelta(hours=1)
    baseline_since = now - timedelta(days=7)

    stmt = text(
        """
        WITH recent AS (
            SELECT signal_id, phase,
                   SUM(gap_outs)   AS gap,
                   SUM(max_outs)   AS maxo,
                   SUM(force_offs) AS force,
                   SUM(gap_outs + max_outs + force_offs) AS total
            FROM phase_termination_hourly
            WHERE hour_start >= :recent_since
            GROUP BY signal_id, phase
        ), baseline AS (
            SELECT signal_id, phase,
                   AVG(CASE WHEN (gap_outs + max_outs + force_offs) = 0
                            THEN 0.0
                            ELSE gap_outs::float   / (gap_outs + max_outs + force_offs)
                       END) AS gap_mean,
                   COALESCE(STDDEV_POP(CASE WHEN (gap_outs + max_outs + force_offs) = 0
                            THEN 0.0
                            ELSE gap_outs::float   / (gap_outs + max_outs + force_offs)
                       END), 0.0) AS gap_sd,
                   AVG(CASE WHEN (gap_outs + max_outs + force_offs) = 0
                            THEN 0.0
                            ELSE max_outs::float   / (gap_outs + max_outs + force_offs)
                       END) AS max_mean,
                   COALESCE(STDDEV_POP(CASE WHEN (gap_outs + max_outs + force_offs) = 0
                            THEN 0.0
                            ELSE max_outs::float   / (gap_outs + max_outs + force_offs)
                       END), 0.0) AS max_sd,
                   AVG(CASE WHEN (gap_outs + max_outs + force_offs) = 0
                            THEN 0.0
                            ELSE force_offs::float / (gap_outs + max_outs + force_offs)
                       END) AS force_mean,
                   COALESCE(STDDEV_POP(CASE WHEN (gap_outs + max_outs + force_offs) = 0
                            THEN 0.0
                            ELSE force_offs::float / (gap_outs + max_outs + force_offs)
                       END), 0.0) AS force_sd
            FROM phase_termination_hourly
            WHERE hour_start >= :baseline_since AND hour_start < :recent_since
            GROUP BY signal_id, phase
        )
        SELECT r.signal_id, r.phase,
               CASE WHEN r.total = 0 THEN 0.0 ELSE r.gap::float   / r.total END
                   AS recent_gap_ratio,
               CASE WHEN r.total = 0 THEN 0.0 ELSE r.maxo::float  / r.total END
                   AS recent_max_ratio,
               CASE WHEN r.total = 0 THEN 0.0 ELSE r.force::float / r.total END
                   AS recent_force_ratio,
               b.gap_mean   AS baseline_gap_mean,
               b.gap_sd     AS baseline_gap_stddev,
               b.max_mean   AS baseline_max_mean,
               b.max_sd     AS baseline_max_stddev,
               b.force_mean AS baseline_force_mean,
               b.force_sd   AS baseline_force_stddev
        FROM recent r
        JOIN baseline b USING (signal_id, phase)
        """
    ).bindparams(recent_since=recent_since, baseline_since=baseline_since)

    result = await session.execute(stmt)
    rows = result.all()
    anomalies = [r for r in rows if _is_termination_anomaly(r, threshold)]

    if not anomalies:
        logger.info("No phase-termination anomalies detected")
        return

    delivered, suppressed = await _partition_suppressed(
        session, anomalies, CHECK_PHASE_TERMINATION_ANOMALY,
    )
    for row in anomalies:
        logger.warning(
            "Phase termination anomaly: signal=%s phase=%d "
            "(gap=%.2f vs %.2f±%.2f, max=%.2f vs %.2f±%.2f, force=%.2f vs %.2f±%.2f)",
            row.signal_id, row.phase,
            row.recent_gap_ratio, row.baseline_gap_mean, row.baseline_gap_stddev,
            row.recent_max_ratio, row.baseline_max_mean, row.baseline_max_stddev,
            row.recent_force_ratio, row.baseline_force_mean, row.baseline_force_stddev,
        )
    if suppressed:
        logger.info("Suppressed %d termination-anomaly alert(s)", len(suppressed))
    if not delivered:
        return

    await notify(
        subject="Phase Termination Anomaly Detected",
        message=(
            f"{len(delivered)} phase(s) show last-hour termination ratios "
            f"outside {threshold}σ of their 7-day baseline:\n"
            + "\n".join(
                f"  - Signal {r.signal_id} phase {r.phase}" for r in delivered
            )
        ),
        severity="warning",
        metadata={
            "threshold_stddev": threshold,
            "phases": [
                {"signal_id": r.signal_id, "phase": r.phase} for r in delivered
            ],
        },
    )


def _is_termination_anomaly(row, stddev_threshold: float) -> bool:
    """Return True if any of the three termination ratios exceeds the threshold."""
    components = (
        (row.recent_gap_ratio, row.baseline_gap_mean, row.baseline_gap_stddev),
        (row.recent_max_ratio, row.baseline_max_mean, row.baseline_max_stddev),
        (row.recent_force_ratio, row.baseline_force_mean, row.baseline_force_stddev),
    )
    for recent, mean, sd in components:
        if sd is None or sd <= 0:
            continue
        if abs(recent - mean) > stddev_threshold * sd:
            return True
    return False


# ---------------------------------------------------------------------------
# 7. Low hit count detection
# ---------------------------------------------------------------------------


async def _check_low_hit_count(session: AsyncSession) -> None:
    """
    Flag detectors with fewer than ``watchdog_low_hit_threshold`` ON events
    in the last hour during windows where the signal had at least one green
    phase (so legitimately quiet approaches are not flagged).
    """
    threshold = settings.watchdog_low_hit_threshold
    cutoff = datetime.now(timezone.utc) - timedelta(hours=1)

    # Signals with at least one green event in the window have "expected activity".
    active_signals = (
        select(ControllerEventLog.signal_id)
        .where(
            ControllerEventLog.event_code == EVENT_PHASE_GREEN,
            ControllerEventLog.event_time >= cutoff,
        )
        .group_by(ControllerEventLog.signal_id)
        .subquery()
    )
    stmt = (
        select(
            ControllerEventLog.signal_id,
            ControllerEventLog.event_param.label("detector_channel"),
            func.count().label("hit_count"),
        )
        .where(
            ControllerEventLog.event_code == EVENT_DETECTOR_ON,
            ControllerEventLog.event_time >= cutoff,
            ControllerEventLog.signal_id.in_(select(active_signals.c.signal_id)),
        )
        .group_by(ControllerEventLog.signal_id, ControllerEventLog.event_param)
        .having(func.count() < threshold)
    )
    result = await session.execute(stmt)
    rows = result.all()

    if not rows:
        logger.info("No low-hit-count detectors detected")
        return

    delivered, suppressed = await _partition_suppressed(
        session, rows, CHECK_LOW_HIT_COUNT,
    )
    for row in rows:
        logger.warning(
            "Low hit count: signal=%s channel=%d hits=%d (threshold=%d, last hour)",
            row.signal_id, row.detector_channel, row.hit_count, threshold,
        )
    if suppressed:
        logger.info("Suppressed %d low-hit-count alert(s)", len(suppressed))
    if not delivered:
        return

    await notify(
        subject="Low Hit Count Detectors Detected",
        message=(
            f"{len(delivered)} detector(s) reported fewer than {threshold} "
            f"activations in the last hour despite active greens:\n"
            + "\n".join(
                f"  - Signal {r.signal_id} channel {r.detector_channel}"
                f" ({r.hit_count} hits)"
                for r in delivered
            )
        ),
        severity="warning",
        metadata={
            "threshold": threshold,
            "detectors": [
                {"signal_id": r.signal_id, "channel": r.detector_channel}
                for r in delivered
            ],
        },
    )
