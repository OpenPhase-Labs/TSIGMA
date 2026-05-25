"""Preemption analytics endpoints (summary, recovery)."""

from datetime import datetime

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from ....auth.dependencies import require_access
from ....dependencies import get_session
from ....reports.sdk.limits import (
    require_max_aggregation_days,
    require_max_lookback,
)
from ....reports.sdk.queries import fetch_events
from ..analytics_schemas import (
    PreemptionRecoveryItem,
    PreemptionRecoveryResponse,
    PreemptionSummaryResponse,
)
from ._common import _default_end, _default_start

router = APIRouter()


@router.get("/preemptions/summary", response_model=PreemptionSummaryResponse)
async def preemption_summary(
    signal_id: str = Query(...),
    start: datetime | None = None,
    end: datetime | None = None,
    session: AsyncSession = Depends(get_session),
    _access=Depends(require_access("analytics")),
):
    """
    Preemption summary statistics.

    Counts preemption events (102=begin, 104=end) and computes
    duration statistics.
    """
    t_start = start or _default_start()
    t_end = end or _default_end()

    await require_max_lookback(t_start, session=session)
    await require_max_aggregation_days(t_start, t_end, session=session)

    df = await fetch_events(
        signal_id=signal_id,
        start=t_start,
        end=t_end,
        event_codes=[102, 104],
    )
    events = list(df.itertuples(index=False))

    # Pair begin/end events by preempt number
    begins: dict[int, datetime] = {}
    durations: list[float] = []
    by_number: dict[str, int] = {}

    for evt in events:
        preempt_num = evt.event_param
        if evt.event_code == 102:
            begins[preempt_num] = evt.event_time
        elif evt.event_code == 104 and preempt_num in begins:
            dur = (evt.event_time - begins.pop(preempt_num)).total_seconds()
            durations.append(dur)
            key = str(preempt_num)
            by_number[key] = by_number.get(key, 0) + 1

    total = len(durations)
    total_time = sum(durations)
    period_seconds = (t_end - t_start).total_seconds()

    return PreemptionSummaryResponse(
        signal_id=signal_id,
        period_start=t_start,
        period_end=t_end,
        total_preemptions=total,
        by_preempt_number=by_number,
        avg_duration_seconds=round(total_time / total, 1) if total else 0.0,
        max_duration_seconds=round(max(durations), 1) if durations else 0.0,
        total_preemption_time_seconds=round(total_time, 1),
        pct_time_preempted=(
            round((total_time / period_seconds) * 100, 2) if period_seconds else 0.0
        ),
    )


@router.get("/preemptions/recovery", response_model=PreemptionRecoveryResponse)
async def preemption_recovery(
    signal_id: str = Query(...),
    start: datetime | None = None,
    end: datetime | None = None,
    session: AsyncSession = Depends(get_session),
    _access=Depends(require_access("analytics")),
):
    """
    Preemption recovery time analysis.

    Measures time from preemption end (104) to next Phase Green (1)
    on the coordinated phase (phase 2) as recovery time.
    """
    t_start = start or _default_start()
    t_end = end or _default_end()

    await require_max_lookback(t_start, session=session)
    await require_max_aggregation_days(t_start, t_end, session=session)

    # OR predicate: (event_code == 104) OR (event_code == 1 AND event_param == 2)
    # cannot be expressed with (event_codes, event_param_in) — those compose
    # as AND. Use the SDK-internal where_sql_fragment escape hatch (B8).
    # The literal codes/params are constants; no user input is interpolated.
    df = await fetch_events(
        signal_id=signal_id,
        start=t_start,
        end=t_end,
        event_codes=None,
        where_sql_fragment="event_code = 104 OR (event_code = 1 AND event_param = 2)",
    )
    events = list(df.itertuples(index=False))

    items = []
    preempt_end = None
    for evt in events:
        if evt.event_code == 104:
            preempt_end = evt.event_time
        elif evt.event_code == 1 and preempt_end is not None:
            recovery = (evt.event_time - preempt_end).total_seconds()
            items.append(
                PreemptionRecoveryItem(
                    preempt_end_time=preempt_end,
                    recovery_complete_time=evt.event_time,
                    recovery_seconds=round(recovery, 1),
                )
            )
            preempt_end = None

    recoveries = [i.recovery_seconds for i in items]

    return PreemptionRecoveryResponse(
        items=items,
        avg_recovery_seconds=(
            round(sum(recoveries) / len(recoveries), 1) if recoveries else 0.0
        ),
        max_recovery_seconds=round(max(recoveries), 1) if recoveries else 0.0,
    )
