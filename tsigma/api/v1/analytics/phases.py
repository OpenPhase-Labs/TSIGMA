"""Phase analytics endpoints (skipped, split-monitor, terminations)."""

from datetime import datetime

import pandas as pd
from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from ....auth.dependencies import require_access
from ....dependencies import get_session
from ....reports.sdk.aggregates import aggregate_events
from ....reports.sdk.limits import (
    require_max_aggregation_days,
    require_max_lookback,
)
from ....reports.sdk.queries import fetch_events
from ..analytics_schemas import (
    PhaseTerminationItem,
    SkippedPhaseItem,
    SplitMonitorItem,
)
from ._common import _default_end, _default_start

router = APIRouter()


@router.get("/phases/skipped", response_model=list[SkippedPhaseItem])
async def skipped_phases(
    signal_id: str = Query(...),
    start: datetime | None = None,
    end: datetime | None = None,
    session: AsyncSession = Depends(get_session),
    _access=Depends(require_access("analytics")),
):
    """
    Find phases that are being skipped.

    Compares Phase Green (1) count per phase vs total cycle count
    to determine skip rate.
    """
    t_start = start or _default_start()
    t_end = end or _default_end()

    await require_max_lookback(t_start, session=session)
    await require_max_aggregation_days(t_start, t_end, session=session)

    # Count Phase Green (event_code 1) events per phase via the tier-aware SDK.
    df = await aggregate_events(
        signal_id,
        t_start,
        t_end,
        agg=[("count_if", "event_code = 1")],
        group_by=["event_param"],
    )
    df = df.rename(columns={"event_param": "phase", "agg_0": "green_count"})

    greens = {
        int(row.phase): int(row.green_count)
        for row in df.itertuples(index=False)
        if row.green_count is not None and not (
            isinstance(row.green_count, float) and pd.isna(row.green_count)
        )
    }

    if not greens:
        return []

    # Estimate total cycles from max green count across all phases
    max_cycles = max(greens.values())

    items = []
    for phase, actual in greens.items():
        skip_count = max_cycles - actual
        skip_rate = round((skip_count / max_cycles) * 100, 1) if max_cycles > 0 else 0.0
        items.append(
            SkippedPhaseItem(
                signal_id=signal_id,
                phase=phase,
                expected_cycles=max_cycles,
                actual_cycles=actual,
                skip_count=skip_count,
                skip_rate_pct=skip_rate,
                period_start=t_start,
                period_end=t_end,
            )
        )

    return items


def _avg(vals) -> float:
    """Average of a list, or 0.0 if empty."""
    return round(sum(vals) / len(vals), 1) if vals else 0.0


def _pct(count: int, total: int) -> float:
    """Percentage, or 0.0 if total is zero."""
    return round((count / total) * 100, 1) if total else 0.0


def _extract_durations(evts):
    """Extract green/yellow durations and termination counts from phase events."""
    greens, yellows = [], []
    gap_outs = max_outs = force_offs = cycle_count = 0
    green_start = yellow_start = None

    for code, t in evts:
        if code == 1:
            green_start = t
            cycle_count += 1
        elif code == 8 and green_start:
            greens.append((t - green_start).total_seconds())
            green_start = None
            yellow_start = t
        elif code == 9 and yellow_start:
            yellows.append((t - yellow_start).total_seconds())
            yellow_start = None
        elif code == 4:
            gap_outs += 1
        elif code == 5:
            max_outs += 1
        elif code == 6:
            force_offs += 1

    return greens, yellows, gap_outs, max_outs, force_offs, cycle_count


def _process_phase_splits(signal_id, phase_num, evts, t_start, t_end):
    """Process events for a single phase into a SplitMonitorItem."""
    greens, yellows, gap_outs, max_outs, force_offs, cycle_count = (
        _extract_durations(evts)
    )

    if cycle_count == 0:
        return None

    total_terms = gap_outs + max_outs + force_offs

    return SplitMonitorItem(
        signal_id=signal_id,
        phase=phase_num,
        period_start=t_start,
        period_end=t_end,
        cycle_count=cycle_count,
        avg_green_seconds=_avg(greens),
        min_green_seconds=round(min(greens), 1) if greens else 0.0,
        max_green_seconds=round(max(greens), 1) if greens else 0.0,
        avg_yellow_seconds=_avg(yellows),
        avg_red_clearance_seconds=0.0,
        gap_out_pct=_pct(gap_outs, total_terms),
        max_out_pct=_pct(max_outs, total_terms),
        force_off_pct=_pct(force_offs, total_terms),
    )


@router.get("/phases/split-monitor", response_model=list[SplitMonitorItem])
async def split_monitor(
    signal_id: str = Query(...),
    phase: int | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
    session: AsyncSession = Depends(get_session),
    _access=Depends(require_access("analytics")),
):
    """
    Phase split timing analysis.

    Computes green, yellow, and red clearance durations per phase.
    Uses event codes: 1 (Green), 8 (Yellow), 9 (Red Clear), 4/5/6 (termination).
    """
    t_start = start or _default_start()
    t_end = end or _default_end()

    await require_max_lookback(t_start, session=session)
    await require_max_aggregation_days(t_start, t_end, session=session)

    df = await fetch_events(
        signal_id=signal_id,
        start=t_start,
        end=t_end,
        event_codes=[1, 4, 5, 6, 8, 9],
        event_param_in=[phase] if phase is not None else None,
    )
    # fetch_events orders by event_time only; split_monitor groups by
    # event_param (phase) then iterates events in time order within each.
    if not df.empty:
        df = df.sort_values(["event_param", "event_time"])

    # Group events by phase
    phases: dict[int, list] = {}
    for evt in df.itertuples(index=False):
        phases.setdefault(evt.event_param, []).append(
            (evt.event_code, evt.event_time)
        )

    items = []
    for p, evts in phases.items():
        item = _process_phase_splits(signal_id, p, evts, t_start, t_end)
        if item is not None:
            items.append(item)

    return items


@router.get("/phases/terminations", response_model=list[PhaseTerminationItem])
async def phase_terminations(
    signal_id: str = Query(...),
    start: datetime | None = None,
    end: datetime | None = None,
    session: AsyncSession = Depends(get_session),
    _access=Depends(require_access("analytics")),
):
    """
    Count phase termination reasons per phase.

    Counts Gap Out (4), Max Out (5), Force Off (6) events.
    Total cycles from Phase Green (1) events.
    """
    t_start = start or _default_start()
    t_end = end or _default_end()

    await require_max_lookback(t_start, session=session)
    await require_max_aggregation_days(t_start, t_end, session=session)

    df = await aggregate_events(
        signal_id,
        t_start,
        t_end,
        agg=[
            ("count_if", "event_code = 1"),
            ("count_if", "event_code = 4"),
            ("count_if", "event_code = 5"),
            ("count_if", "event_code = 6"),
        ],
        group_by=["event_param"],
    )
    df = df.rename(
        columns={
            "event_param": "phase",
            "agg_0": "total_cycles",
            "agg_1": "gap_outs",
            "agg_2": "max_outs",
            "agg_3": "force_offs",
        }
    )

    items = []
    for row in df.itertuples(index=False):
        items.append(
            PhaseTerminationItem(
                signal_id=signal_id,
                phase=int(row.phase),
                gap_outs=int(row.gap_outs or 0),
                max_outs=int(row.max_outs or 0),
                force_offs=int(row.force_offs or 0),
                skips=0,
                total_cycles=int(row.total_cycles or 0),
            )
        )

    return items
