"""Detector analytics endpoints (stuck, gaps, occupancy)."""

from datetime import datetime, timedelta

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
    GapAnalysisItem,
    OccupancyBin,
    OccupancyResponse,
    StuckDetectorItem,
)
from ._common import _default_end, _default_start

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

    await require_max_lookback(t_start, session=session)
    await require_max_aggregation_days(t_start, t_end, session=session)

    # Fleet-wide scan when signal_id omitted; otherwise scope to the one signal.
    signals: str | None = signal_id if signal_id else "All"

    # 1) Last ON event per (signal_id, channel) — event_code 82.
    last_on_df = await aggregate_events(
        signals,
        t_start,
        t_end,
        agg=[("max_if", "event_time", "event_code = 82")],
        group_by=["signal_id", "event_param"],
    )
    last_on_df = last_on_df.rename(columns={"agg_0": "last_on_time"})

    # 2) Last OFF event per (signal_id, channel) — event_code 81.
    last_off_df = await aggregate_events(
        signals,
        t_start,
        t_end,
        agg=[("max_if", "event_time", "event_code = 81")],
        group_by=["signal_id", "event_param"],
    )
    last_off_df = last_off_df.rename(columns={"agg_0": "last_off_time"})

    # 3) ON-event count per (signal_id, channel) — event_code 82.
    on_count_df = await aggregate_events(
        signals,
        t_start,
        t_end,
        agg=[("count_if", "event_code = 82")],
        group_by=["signal_id", "event_param"],
    )
    on_count_df = on_count_df.rename(columns={"agg_0": "event_count"})

    if last_on_df.empty:
        return []

    # Index last_off and on_count by (signal_id, event_param) for fast lookup.
    last_off_lookup: dict[tuple, datetime] = {}
    if not last_off_df.empty:
        for row in last_off_df.itertuples(index=False):
            last_off_lookup[(row.signal_id, row.event_param)] = row.last_off_time

    on_count_lookup: dict[tuple, int] = {}
    if not on_count_df.empty:
        for row in on_count_df.itertuples(index=False):
            on_count_lookup[(row.signal_id, row.event_param)] = row.event_count

    threshold_seconds = threshold_minutes * 60
    items = []
    for row in last_on_df.itertuples(index=False):
        last_on_time = row.last_on_time
        if last_on_time is None:
            continue
        key = (row.signal_id, row.event_param)
        last_off_time = last_off_lookup.get(key)

        if last_off_time is None or last_on_time > last_off_time:
            duration = (t_end - last_on_time).total_seconds()
            if duration >= threshold_seconds:
                items.append(
                    StuckDetectorItem(
                        signal_id=row.signal_id,
                        detector_channel=row.event_param,
                        status="STUCK_ON",
                        duration_seconds=round(duration, 1),
                        last_event_time=last_on_time,
                        event_count=on_count_lookup.get(key, 0) or 0,
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

    await require_max_lookback(t_start, session=session)
    await require_max_aggregation_days(t_start, t_end, session=session)

    df = await fetch_events(
        signal_id=signal_id,
        start=t_start,
        end=t_end,
        event_codes=[82],
        event_param_in=[detector_channel] if detector_channel is not None else None,
    )
    # fetch_events orders by event_time only; gap analysis groups by
    # event_param then iterates in time order within each group.
    if not df.empty:
        df = df.sort_values(["event_param", "event_time"])
    rows = list(df.itertuples(index=False))

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

    await require_max_lookback(t_start, session=session)
    await require_max_aggregation_days(t_start, t_end, session=session)

    # Get all ON/OFF events for this detector
    df = await fetch_events(
        signal_id=signal_id,
        start=t_start,
        end=t_end,
        event_codes=[81, 82],
        event_param_in=[detector_channel],
    )
    events = list(df.itertuples(index=False))

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
