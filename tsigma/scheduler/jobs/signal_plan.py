"""
Signal timing plan extraction job.

Scans controller_event_log for Indiana event codes 131-149 and
populates the signal_plan table.  The controller is the source of
truth — this job only records what the controller reported.

Event codes handled:
    131  CoordPatternChange   → starts a new plan activation
    132  CycleLengthChange    → updates cycle_length on current plan
    133  OffsetLengthChange   → updates offset on current plan
    134-149 Split1-16 Change  → updates splits JSON on current plan
                                (event_code - 133 = phase number)

On event 131 for a signal, the previous open plan row (effective_to
IS NULL) has effective_to set to the new 131's event_time, and a new
row is created.

Watermark: per-signal max(effective_from) from signal_plan itself.
Events at or before the watermark are skipped.  On first run for a
signal, everything in the lookback window is scanned.

Query strategy: three bulk queries per run, not per-signal.  For a
9 000-signal deployment this keeps the job at ~3 queries total instead
of ~27 000.
"""

import logging
from datetime import datetime, timedelta, timezone
from itertools import groupby

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from tsigma.config import settings
from tsigma.models.event import ControllerEventLog
from tsigma.models.signal_plan import SignalPlan
from tsigma.scheduler.registry import JobRegistry

logger = logging.getLogger(__name__)

EVENT_PLAN_CHANGE = 131
EVENT_CYCLE_LENGTH = 132
EVENT_OFFSET = 133
EVENT_SPLIT_FIRST = 134
EVENT_SPLIT_LAST = 149  # Split16 (phase 16)

_PLAN_EVENT_CODES = tuple(range(EVENT_PLAN_CHANGE, EVENT_SPLIT_LAST + 1))


def _split_phase_for(event_code: int) -> int:
    """Map Indiana split event code to phase number (134→1 ... 149→16)."""
    return event_code - (EVENT_SPLIT_FIRST - 1)


def _apply_parameter_event(plan: SignalPlan, event: ControllerEventLog) -> None:
    """Apply a 132/133/134-149 event to an open plan row."""
    if event.event_code == EVENT_CYCLE_LENGTH:
        plan.cycle_length = event.event_param
    elif event.event_code == EVENT_OFFSET:
        plan.offset = event.event_param
    elif EVENT_SPLIT_FIRST <= event.event_code <= EVENT_SPLIT_LAST:
        # SQLAlchemy JSONB change tracking is shallow — in-place mutation
        # (plan.splits[key] = value) will NOT mark the row dirty.  Build a
        # new dict and reassign so the ORM detects the change.
        splits = dict(plan.splits) if plan.splits else {}
        splits[str(_split_phase_for(event.event_code))] = event.event_param
        plan.splits = splits


async def _fetch_open_plans(session: AsyncSession) -> dict[str, SignalPlan]:
    """Load all currently-open plans keyed by signal_id."""
    stmt = select(SignalPlan).where(SignalPlan.effective_to.is_(None))
    result = await session.execute(stmt)
    return {plan.signal_id: plan for plan in result.scalars().all()}


async def _fetch_watermarks(session: AsyncSession) -> dict[str, datetime]:
    """Latest effective_from per signal, used to skip already-processed events."""
    stmt = select(
        SignalPlan.signal_id,
        func.max(SignalPlan.effective_from),
    ).group_by(SignalPlan.signal_id)
    result = await session.execute(stmt)
    return {row[0]: row[1] for row in result.all()}


async def _fetch_plan_events(
    session: AsyncSession, lookback_start: datetime
) -> list[ControllerEventLog]:
    """All plan-related events in the lookback window, ordered for groupby."""
    stmt = (
        select(ControllerEventLog)
        .where(ControllerEventLog.event_code.in_(_PLAN_EVENT_CODES))
        .where(ControllerEventLog.event_time >= lookback_start)
        .order_by(
            ControllerEventLog.signal_id,
            ControllerEventLog.event_time,
        )
    )
    result = await session.execute(stmt)
    return list(result.scalars().all())


def _process_signal_events(
    signal_id: str,
    events: list[ControllerEventLog],
    watermark: datetime | None,
    current: SignalPlan | None,
    session: AsyncSession,
) -> int:
    """Apply one signal's ordered events, adding new SignalPlan rows. Returns opened count."""
    plans_opened = 0

    for event in events:
        if watermark is not None and event.event_time <= watermark:
            continue

        if event.event_code == EVENT_PLAN_CHANGE:
            if current is not None:
                current.effective_to = event.event_time
            current = SignalPlan(
                signal_id=signal_id,
                effective_from=event.event_time,
                plan_number=event.event_param,
            )
            session.add(current)
            plans_opened += 1
        elif current is not None:
            _apply_parameter_event(current, event)

    return plans_opened


@JobRegistry.register(name="extract_signal_plans", trigger="cron", minute="*/15")
async def extract_signal_plans(session: AsyncSession) -> None:
    """Scan plan-related events and upsert signal_plan rows."""
    hours = settings.aggregation_lookback_hours
    lookback_start = datetime.now(timezone.utc) - timedelta(hours=hours)

    events = await _fetch_plan_events(session, lookback_start)
    if not events:
        return

    open_plans = await _fetch_open_plans(session)
    watermarks = await _fetch_watermarks(session)

    total_opened = 0
    for signal_id, signal_events in groupby(events, key=lambda e: e.signal_id):
        total_opened += _process_signal_events(
            signal_id,
            list(signal_events),
            watermarks.get(signal_id),
            open_plans.get(signal_id),
            session,
        )

    if total_opened:
        logger.info("Extracted %d new signal plan activation(s)", total_opened)
