"""Coordination analytics endpoints (offset-drift, patterns, quality)."""

from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ....auth.dependencies import require_access
from ....dependencies import get_session
from ..analytics_schemas import (
    CoordinationQualityResponse,
    OffsetDriftResponse,
    PatternChangeItem,
)
from ._common import CEL, _compute_cycle_stats, _default_end, _default_start

router = APIRouter()


@router.get("/coordination/offset-drift", response_model=OffsetDriftResponse)
async def offset_drift(
    signal_id: str = Query(...),
    start: datetime | None = None,
    end: datetime | None = None,
    session: AsyncSession = Depends(get_session),
    _access=Depends(require_access("analytics")),
):
    """
    Analyze coordination offset drift.

    Measures cycle-to-cycle variation in coordination reference point
    timing using Phase Green (1) events on the coordinated phase (phase 2).
    """
    t_start = start or _default_start()
    t_end = end or _default_end()

    # Get coord phase (phase 2) green events as cycle markers
    query = (
        select(CEL.event_time)
        .where(
            CEL.signal_id == signal_id,
            CEL.event_code == 1,
            CEL.event_param == 2,
            CEL.event_time.between(t_start, t_end),
        )
        .order_by(CEL.event_time)
    )
    result = await session.execute(query)
    times = [row.event_time for row in result.all()]

    if len(times) < 2:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Insufficient data for offset drift analysis",
        )

    stats = _compute_cycle_stats(times)

    mean_drift = sum(stats.deviations) / len(stats.deviations)
    variance = sum((d - mean_drift) ** 2 for d in stats.deviations) / len(stats.deviations)

    return OffsetDriftResponse(
        signal_id=signal_id,
        period_start=t_start,
        period_end=t_end,
        expected_cycle_seconds=round(stats.avg_cycle),
        cycle_count=stats.count,
        avg_drift_seconds=round(mean_drift, 1),
        max_drift_seconds=round(max(stats.deviations), 1),
        drift_stddev=round(variance ** 0.5, 1),
    )


@router.get("/coordination/patterns", response_model=list[PatternChangeItem])
async def pattern_history(
    signal_id: str = Query(...),
    start: datetime | None = None,
    end: datetime | None = None,
    session: AsyncSession = Depends(get_session),
    _access=Depends(require_access("analytics")),
):
    """
    Get coordination pattern change history.

    Uses Coord Pattern Change (131) events.
    event_param = new pattern number.
    """
    t_start = start or _default_start()
    t_end = end or _default_end()

    query = (
        select(CEL.event_time, CEL.event_param)
        .where(
            CEL.signal_id == signal_id,
            CEL.event_code == 131,
            CEL.event_time.between(t_start, t_end),
        )
        .order_by(CEL.event_time)
    )
    result = await session.execute(query)
    rows = result.all()

    items = []
    for i, row in enumerate(rows):
        from_pattern = rows[i - 1].event_param if i > 0 else 0
        duration = None
        if i < len(rows) - 1:
            duration = (rows[i + 1].event_time - row.event_time).total_seconds()

        items.append(
            PatternChangeItem(
                timestamp=row.event_time,
                from_pattern=from_pattern,
                to_pattern=row.event_param,
                duration_seconds=duration,
            )
        )

    return items


@router.get("/coordination/quality", response_model=CoordinationQualityResponse)
async def coordination_quality(
    signal_id: str = Query(...),
    start: datetime | None = None,
    end: datetime | None = None,
    tolerance_seconds: float = Query(2.0, ge=0.1),
    session: AsyncSession = Depends(get_session),
    _access=Depends(require_access("analytics")),
):
    """
    Compute coordination quality score.

    Measures what percentage of cycles fall within the tolerance
    of the expected cycle length.
    """
    t_start = start or _default_start()
    t_end = end or _default_end()

    # Get coordinated phase green events
    query = (
        select(CEL.event_time)
        .where(
            CEL.signal_id == signal_id,
            CEL.event_code == 1,
            CEL.event_param == 2,
            CEL.event_time.between(t_start, t_end),
        )
        .order_by(CEL.event_time)
    )
    result = await session.execute(query)
    times = [row.event_time for row in result.all()]

    if len(times) < 2:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Insufficient data for coordination quality analysis",
        )

    stats = _compute_cycle_stats(times)
    within_tolerance = sum(1 for e in stats.deviations if e <= tolerance_seconds)

    return CoordinationQualityResponse(
        signal_id=signal_id,
        period_start=t_start,
        period_end=t_end,
        total_cycles=stats.count,
        cycles_within_tolerance=within_tolerance,
        quality_pct=round((within_tolerance / stats.count) * 100, 1),
        avg_offset_error_seconds=round(sum(stats.deviations) / len(stats.deviations), 1),
    )
