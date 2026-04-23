"""
Cycle aggregate query helpers for report plugins.

Provides functions to query pre-computed cycle_boundary,
cycle_detector_arrival, and cycle_summary_15min tables. Reports
call these instead of processing raw events for historical data.

All helpers return pandas DataFrames via db_facade.get_dataframe().
"""

from datetime import datetime
from typing import Optional

import pandas as pd
from sqlalchemy import select

from ...database.db import db_facade
from ...models.aggregates import (
    CycleBoundary,
    CycleDetectorArrival,
    CycleSummary15Min,
)


async def fetch_cycle_boundaries(
    signal_id: str,
    phase: int,
    start: datetime,
    end: datetime,
) -> pd.DataFrame:
    """
    Fetch pre-computed cycle boundaries for a signal/phase/time range.

    Args:
        signal_id: Signal identifier.
        phase: Phase number.
        start: Range start (inclusive).
        end: Range end (exclusive).

    Returns:
        DataFrame with columns: green_start, yellow_start, red_start,
        cycle_end, green_duration_seconds, yellow_duration_seconds,
        red_duration_seconds, cycle_duration_seconds, termination_type.
        Ordered by green_start.
    """
    stmt = (
        select(
            CycleBoundary.green_start,
            CycleBoundary.yellow_start,
            CycleBoundary.red_start,
            CycleBoundary.cycle_end,
            CycleBoundary.green_duration_seconds,
            CycleBoundary.yellow_duration_seconds,
            CycleBoundary.red_duration_seconds,
            CycleBoundary.cycle_duration_seconds,
            CycleBoundary.termination_type,
        )
        .where(
            CycleBoundary.signal_id == signal_id,
            CycleBoundary.phase == phase,
            CycleBoundary.green_start >= start,
            CycleBoundary.green_start < end,
        )
        .order_by(CycleBoundary.green_start)
    )
    return await db_facade.get_dataframe(stmt)


async def fetch_cycle_arrivals(
    signal_id: str,
    phase: int,
    start: datetime,
    end: datetime,
    detector_channels: Optional[list[int]] = None,
) -> pd.DataFrame:
    """
    Fetch pre-computed detector arrivals for a signal/phase/time range.

    Args:
        signal_id: Signal identifier.
        phase: Phase number.
        start: Range start (inclusive).
        end: Range end (exclusive).
        detector_channels: Optional list of detector channels to filter by.

    Returns:
        DataFrame with columns: arrival_time, detector_channel,
        green_start, time_in_cycle_seconds, phase_state.
        Ordered by arrival_time.
    """
    stmt = (
        select(
            CycleDetectorArrival.arrival_time,
            CycleDetectorArrival.detector_channel,
            CycleDetectorArrival.green_start,
            CycleDetectorArrival.time_in_cycle_seconds,
            CycleDetectorArrival.phase_state,
        )
        .where(
            CycleDetectorArrival.signal_id == signal_id,
            CycleDetectorArrival.phase == phase,
            CycleDetectorArrival.arrival_time >= start,
            CycleDetectorArrival.arrival_time < end,
        )
    )

    if detector_channels:
        stmt = stmt.where(
            CycleDetectorArrival.detector_channel.in_(detector_channels)
        )

    stmt = stmt.order_by(CycleDetectorArrival.arrival_time)
    return await db_facade.get_dataframe(stmt)


async def fetch_cycle_summary(
    signal_id: str,
    phase: int,
    start: datetime,
    end: datetime,
) -> pd.DataFrame:
    """
    Fetch 15-minute cycle summaries for a signal/phase/time range.

    Args:
        signal_id: Signal identifier.
        phase: Phase number.
        start: Range start (inclusive).
        end: Range end (exclusive).

    Returns:
        DataFrame with all CycleSummary15Min columns, ordered by bin_start.
    """
    stmt = (
        select(
            CycleSummary15Min.bin_start,
            CycleSummary15Min.total_cycles,
            CycleSummary15Min.avg_cycle_length_seconds,
            CycleSummary15Min.avg_green_seconds,
            CycleSummary15Min.total_arrivals,
            CycleSummary15Min.arrivals_on_green,
            CycleSummary15Min.arrivals_on_yellow,
            CycleSummary15Min.arrivals_on_red,
            CycleSummary15Min.arrival_on_green_pct,
        )
        .where(
            CycleSummary15Min.signal_id == signal_id,
            CycleSummary15Min.phase == phase,
            CycleSummary15Min.bin_start >= start,
            CycleSummary15Min.bin_start < end,
        )
        .order_by(CycleSummary15Min.bin_start)
    )
    return await db_facade.get_dataframe(stmt)
