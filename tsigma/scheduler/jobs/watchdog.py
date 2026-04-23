"""
Data quality and detector health watchdog.

Runs daily to identify signals that have gone silent (no events in 24+ hours)
and detectors that appear stuck-on (excessive activations). Results are logged
as warnings for operator review — this job does not modify any data.
"""

import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from tsigma.models.event import ControllerEventLog
from tsigma.notifications import notify
from tsigma.scheduler.registry import JobRegistry

logger = logging.getLogger(__name__)

# A detector reporting more than this many ON events (code 82) in one hour
# is likely stuck in the active state.
STUCK_DETECTOR_THRESHOLD = 3600


@JobRegistry.register(name="watchdog", trigger="cron", hour="6", minute="0")
async def watchdog(session: AsyncSession) -> None:
    """Check for silent signals and stuck detectors."""
    try:
        await _check_silent_signals(session)
    except Exception:
        logger.exception("Silent-signal check failed")

    try:
        await _check_stuck_detectors(session)
    except Exception:
        logger.exception("Stuck-detector check failed")


async def _check_silent_signals(session: AsyncSession) -> None:
    """Warn about signals with no events in the last 24 hours."""
    cutoff = datetime.now(timezone.utc) - timedelta(hours=24)

    # Subquery: latest event per signal
    latest = (
        select(
            ControllerEventLog.signal_id,
            func.max(ControllerEventLog.event_time).label("last_event"),
        )
        .group_by(ControllerEventLog.signal_id)
        .subquery()
    )

    # Signals whose latest event is older than the cutoff
    stmt = select(latest.c.signal_id, latest.c.last_event).where(
        latest.c.last_event < cutoff
    )

    result = await session.execute(stmt)
    silent = result.all()

    if silent:
        for row in silent:
            logger.warning(
                "Silent signal: %s — last event at %s",
                row.signal_id,
                row.last_event,
            )
        logger.warning("Total silent signals (24h+): %d", len(silent))
        await notify(
            subject="Silent Signals Detected",
            message=(
                f"{len(silent)} signal(s) have not reported events in 24+ hours:\n"
                + "\n".join(
                    f"  - {r.signal_id} (last: {r.last_event})" for r in silent
                )
            ),
            severity="warning",
            metadata={"signal_ids": [r.signal_id for r in silent]},
        )
    else:
        logger.info("No silent signals detected")


async def _check_stuck_detectors(session: AsyncSession) -> None:
    """Warn about detector channels with excessive ON events in the last hour."""
    cutoff = datetime.now(timezone.utc) - timedelta(hours=1)

    # Event code 82 = detector ON per Indiana/Purdue spec
    stmt = (
        select(
            ControllerEventLog.signal_id,
            ControllerEventLog.event_param.label("detector_channel"),
            func.count().label("on_count"),
        )
        .where(
            ControllerEventLog.event_code == 82,
            ControllerEventLog.event_time >= cutoff,
        )
        .group_by(ControllerEventLog.signal_id, ControllerEventLog.event_param)
        .having(func.count() > STUCK_DETECTOR_THRESHOLD)
    )

    result = await session.execute(stmt)
    stuck = result.all()

    if stuck:
        for row in stuck:
            logger.warning(
                "Possible stuck detector: signal=%s channel=%d — %d ON events in last hour",
                row.signal_id,
                row.detector_channel,
                row.on_count,
            )
        logger.warning("Total suspected stuck detectors: %d", len(stuck))
        await notify(
            subject="Stuck Detectors Detected",
            message=(
                f"{len(stuck)} detector(s) appear stuck:\n"
                + "\n".join(
                    f"  - Signal {r.signal_id} channel {r.detector_channel}"
                    f" ({r.on_count} events/hr)"
                    for r in stuck
                )
            ),
            severity="warning",
            metadata={
                "detectors": [
                    {"signal_id": r.signal_id, "channel": r.detector_channel}
                    for r in stuck
                ]
            },
        )
    else:
        logger.info("No stuck detectors detected")
