"""Detector analytics endpoints (stuck, gaps, occupancy)."""

from datetime import datetime, timedelta

from fastapi import APIRouter, Depends, Query
from sqlalchemy import and_, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from ....auth.dependencies import require_access
from ....dependencies import get_session
from ..analytics_schemas import (
    GapAnalysisItem,
    OccupancyBin,
    OccupancyResponse,
    StuckDetectorItem,
)
from ._common import CEL, _default_end, _default_start

router = APIRouter()


@router.get("/detectors/stuck", response_model=list[StuckDetectorItem])
async def stuck_detectors(
    signal_id: str | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
    threshold_minutes: int = Query(30, ge=1),
    session: AsyncSession = Depends(get_session),
    _access=Depends(require_access("analytics")),
):
    """
    Find stuck detectors (ON with no OFF for longer than threshold).

    Looks for detector channels where the last ON event (code 82) has
    no corresponding OFF event (code 81) within threshold_minutes.
    """
    t_start = start or _default_start()
    t_end = end or _default_end()

    # Find last ON event per detector channel
    last_on = (
        select(
            CEL.signal_id,
            CEL.event_param.label("detector_channel"),
            func.max(CEL.event_time).label("last_on_time"),
        )
        .where(
            CEL.event_code == 82,
            CEL.event_time.between(t_start, t_end),
        )
        .group_by(CEL.signal_id, CEL.event_param)
    )
    if signal_id:
        last_on = last_on.where(CEL.signal_id == signal_id)
    last_on = last_on.subquery("last_on")

    # Find last OFF event per detector channel
    last_off = (
        select(
            CEL.signal_id,
            CEL.event_param.label("detector_channel"),
            func.max(CEL.event_time).label("last_off_time"),
        )
        .where(
            CEL.event_code == 81,
            CEL.event_time.between(t_start, t_end),
        )
        .group_by(CEL.signal_id, CEL.event_param)
    )
    if signal_id:
        last_off = last_off.where(CEL.signal_id == signal_id)
    last_off = last_off.subquery("last_off")

    # Count ON events per detector in the period
    on_count = (
        select(
            CEL.signal_id,
            CEL.event_param.label("detector_channel"),
            func.count().label("event_count"),
        )
        .where(
            CEL.event_code == 82,
            CEL.event_time.between(t_start, t_end),
        )
        .group_by(CEL.signal_id, CEL.event_param)
    )
    if signal_id:
        on_count = on_count.where(CEL.signal_id == signal_id)
    on_count = on_count.subquery("on_count")

    # Join: stuck = last ON > last OFF (or no OFF at all)
    threshold_seconds = threshold_minutes * 60
    query = (
        select(
            last_on.c.signal_id,
            last_on.c.detector_channel,
            last_on.c.last_on_time,
            last_off.c.last_off_time,
            on_count.c.event_count,
        )
        .outerjoin(
            last_off,
            and_(
                last_on.c.signal_id == last_off.c.signal_id,
                last_on.c.detector_channel == last_off.c.detector_channel,
            ),
        )
        .outerjoin(
            on_count,
            and_(
                last_on.c.signal_id == on_count.c.signal_id,
                last_on.c.detector_channel == on_count.c.detector_channel,
            ),
        )
    )

    result = await session.execute(query)
    rows = result.all()

    items = []
    for row in rows:
        last_on_time = row.last_on_time
        last_off_time = row.last_off_time

        if last_off_time is None or last_on_time > last_off_time:
            duration = (t_end - last_on_time).total_seconds()
            if duration >= threshold_seconds:
                items.append(
                    StuckDetectorItem(
                        signal_id=row.signal_id,
                        detector_channel=row.detector_channel,
                        status="STUCK_ON",
                        duration_seconds=round(duration, 1),
                        last_event_time=last_on_time,
                        event_count=row.event_count or 0,
                    )
                )

    return items


@router.get("/detectors/gaps", response_model=list[GapAnalysisItem])
async def gap_analysis(
    signal_id: str = Query(...),
    detector_channel: int | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
    session: AsyncSession = Depends(get_session),
    _access=Depends(require_access("analytics")),
):
    """
    Analyze gaps between detector actuations.

    Computes gap statistics (avg, min, max) for detector ON events (code 82).
    Results are grouped by detector channel over the entire period.
    """
    t_start = start or _default_start()
    t_end = end or _default_end()

    filters = [
        CEL.signal_id == signal_id,
        CEL.event_code == 82,
        CEL.event_time.between(t_start, t_end),
    ]
    if detector_channel is not None:
        filters.append(CEL.event_param == detector_channel)

    # Get all ON events ordered by time
    query = (
        select(CEL.event_param, CEL.event_time)
        .where(*filters)
        .order_by(CEL.event_param, CEL.event_time)
    )
    result = await session.execute(query)
    rows = result.all()

    # Group by channel and compute gap stats
    channels: dict[int, list[datetime]] = {}
    for row in rows:
        channels.setdefault(row.event_param, []).append(row.event_time)

    items = []
    for channel, times in channels.items():
        if len(times) < 2:
            continue

        gaps = [
            (times[i + 1] - times[i]).total_seconds()
            for i in range(len(times) - 1)
        ]
        items.append(
            GapAnalysisItem(
                signal_id=signal_id,
                detector_channel=channel,
                period_start=t_start,
                period_end=t_end,
                total_actuations=len(times),
                avg_gap_seconds=round(sum(gaps) / len(gaps), 1),
                min_gap_seconds=round(min(gaps), 1),
                max_gap_seconds=round(max(gaps), 1),
                gap_out_count=0,
                max_out_count=0,
            )
        )

    return items


def _sum_on_time(events, bin_start, bin_end) -> float:
    """Sum detector ON time (seconds) within a time bin."""
    on_time = 0.0
    on_start = None

    for evt in events:
        if evt.event_time < bin_start:
            if evt.event_code == 82:
                on_start = bin_start
            continue
        if evt.event_time >= bin_end:
            break

        if evt.event_code == 82:
            on_start = evt.event_time
        elif evt.event_code == 81 and on_start is not None:
            on_time += (evt.event_time - on_start).total_seconds()
            on_start = None

    if on_start is not None:
        on_time += (bin_end - on_start).total_seconds()

    return on_time


@router.get("/detectors/occupancy", response_model=OccupancyResponse)
async def detector_occupancy(
    signal_id: str = Query(...),
    detector_channel: int = Query(...),
    start: datetime | None = None,
    end: datetime | None = None,
    bin_minutes: int = Query(15, ge=1, le=60),
    session: AsyncSession = Depends(get_session),
    _access=Depends(require_access("analytics")),
):
    """
    Compute detector occupancy percentage in time bins.

    Occupancy = (total ON time / bin duration) * 100.
    Uses ON (82) and OFF (81) event pairs.
    """
    t_start = start or _default_start()
    t_end = end or _default_end()

    # Get all ON/OFF events for this detector
    query = (
        select(CEL.event_code, CEL.event_time)
        .where(
            CEL.signal_id == signal_id,
            CEL.event_param == detector_channel,
            CEL.event_code.in_([81, 82]),
            CEL.event_time.between(t_start, t_end),
        )
        .order_by(CEL.event_time)
    )
    result = await session.execute(query)
    events = result.all()

    bin_delta = timedelta(minutes=bin_minutes)
    bins = []
    current = t_start

    while current < t_end:
        bin_end = min(current + bin_delta, t_end)
        bin_seconds = (bin_end - current).total_seconds()
        on_time = _sum_on_time(events, current, bin_end)
        occupancy_pct = round((on_time / bin_seconds) * 100, 1) if bin_seconds > 0 else 0.0

        bins.append(OccupancyBin(
            bin_start=current,
            bin_end=bin_end,
            occupancy_pct=occupancy_pct,
        ))
        current = bin_end

    return OccupancyResponse(
        signal_id=signal_id,
        detector_channel=detector_channel,
        bins=bins,
    )
