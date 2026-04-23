"""Phase analytics endpoints (skipped, split-monitor, terminations)."""

from datetime import datetime

from fastapi import APIRouter, Depends, Query
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from ....auth.dependencies import require_access
from ....dependencies import get_session
from ..analytics_schemas import (
    PhaseTerminationItem,
    SkippedPhaseItem,
    SplitMonitorItem,
)
from ._common import CEL, _default_end, _default_start

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

    # Count Phase Green (1) events per phase
    query = (
        select(
            CEL.event_param.label("phase"),
            func.count().label("green_count"),
        )
        .where(
            CEL.signal_id == signal_id,
            CEL.event_code == 1,
            CEL.event_time.between(t_start, t_end),
        )
        .group_by(CEL.event_param)
    )
    result = await session.execute(query)
    greens = {row.phase: row.green_count for row in result.all()}

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

    filters = [
        CEL.signal_id == signal_id,
        CEL.event_code.in_([1, 4, 5, 6, 8, 9]),
        CEL.event_time.between(t_start, t_end),
    ]
    if phase is not None:
        filters.append(CEL.event_param == phase)

    query = (
        select(CEL.event_code, CEL.event_param, CEL.event_time)
        .where(*filters)
        .order_by(CEL.event_param, CEL.event_time)
    )
    result = await session.execute(query)
    events = result.all()

    # Group events by phase
    phases: dict[int, list] = {}
    for evt in events:
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

    query = (
        select(
            CEL.event_param.label("phase"),
            func.count().filter(CEL.event_code == 1).label("total_cycles"),
            func.count().filter(CEL.event_code == 4).label("gap_outs"),
            func.count().filter(CEL.event_code == 5).label("max_outs"),
            func.count().filter(CEL.event_code == 6).label("force_offs"),
        )
        .where(
            CEL.signal_id == signal_id,
            CEL.event_code.in_([1, 4, 5, 6]),
            CEL.event_time.between(t_start, t_end),
        )
        .group_by(CEL.event_param)
    )
    result = await session.execute(query)

    items = []
    for row in result.all():
        items.append(
            PhaseTerminationItem(
                signal_id=signal_id,
                phase=row.phase,
                gap_outs=row.gap_outs,
                max_outs=row.max_outs,
                force_offs=row.force_offs,
                skips=0,
                total_cycles=row.total_cycles,
            )
        )

    return items
