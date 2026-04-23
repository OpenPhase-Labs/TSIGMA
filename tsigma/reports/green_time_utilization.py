"""
Green Time Utilization report plugin.

Measures how effectively allocated green time is consumed by vehicle
demand.  For each phase, cycles are paired (green start -> yellow
clearance) and detector activations during green are bucketed by
time-since-green-start to produce a 2D density map.  Combined with
the programmed split (pulled from SignalPlan) this answers:

    "Of the green time the controller gave this phase, how much was
     actually used by arriving vehicles, and where in the green
     interval did those arrivals fall?"

Mirrors ATSPM 5x's GreenTimeUtilizationService algorithm, adapted to
TSIGMA's plugin pattern and event sourcing.
"""

import logging
from bisect import bisect_left, bisect_right
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any

import pandas as pd
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from ..models import SignalPlan
from .registry import Report, ReportMetadata, ReportRegistry
from .sdk import (
    EVENT_DETECTOR_ON,
    EVENT_PHASE_GREEN,
    EVENT_RED_CLEARANCE,
    EVENT_YELLOW_CLEARANCE,
    bin_index,
    fetch_events_split,
    fetch_plans,
    load_channel_to_phase,
    parse_time,
    programmed_split,
    total_bins,
)

logger = logging.getLogger(__name__)

DEFAULT_X_BIN_MINUTES = 15
DEFAULT_Y_BIN_SECONDS = 2


class GreenTimeUtilizationParams(BaseModel):
    signal_id: str = Field(..., description="Signal identifier")
    start: str = Field(..., description="Analysis window start (ISO-8601)")
    end: str = Field(..., description="Analysis window end (ISO-8601)")
    phase_number: int | None = Field(default=None, description="Phase number to analyze (None = all)")
    x_bin_minutes: int = Field(default=15, description="Time-of-day bin size in minutes")
    y_bin_seconds: int = Field(default=2, description="Green-offset bin size in seconds")


@ReportRegistry.register("green-time-utilization")
class GreenTimeUtilizationReport(Report[GreenTimeUtilizationParams]):
    """How effectively allocated green time is consumed by demand."""

    metadata = ReportMetadata(
        name="green-time-utilization",
        description=(
            "Green time utilization per phase: actual green used vs programmed "
            "split, with arrival density across the green interval."
        ),
        category="standard",
        estimated_time="medium",
        export_formats=["csv", "json", "ndjson"],
    )

    async def execute(self, params: GreenTimeUtilizationParams, session: AsyncSession) -> pd.DataFrame:
        """
        Execute green time utilization analysis.

        Returns:
            DataFrame with columns: phase_number, x_bin, bin_start, cycle_count,
            avg_green_seconds, programmed_split_seconds, utilization_pct.
        """
        signal_id = params.signal_id
        start = parse_time(params.start)
        end = parse_time(params.end)
        phase_filter = params.phase_number
        x_bin_minutes = params.x_bin_minutes
        y_bin_seconds = params.y_bin_seconds

        empty_df = pd.DataFrame(columns=[
            "phase_number", "x_bin", "bin_start", "cycle_count",
            "avg_green_seconds", "programmed_split_seconds", "utilization_pct",
        ])

        logger.info(
            "Running green-time-utilization for %s from %s to %s",
            signal_id, start, end,
        )

        channel_to_phase = await load_channel_to_phase(session, signal_id, start)
        if not channel_to_phase:
            return empty_df

        target_phases = set(channel_to_phase.values())
        if phase_filter is not None:
            target_phases = {int(phase_filter)} & target_phases
        if not target_phases:
            return empty_df

        target_channels = {
            ch for ch, ph in channel_to_phase.items() if ph in target_phases
        }

        # Fetch events via SDK helper
        det_channels = list(target_channels)

        df = await fetch_events_split(
            signal_id, start, end,
            phase_codes=[EVENT_PHASE_GREEN, EVENT_YELLOW_CLEARANCE, EVENT_RED_CLEARANCE],
            det_channels=det_channels,
            det_codes=[EVENT_DETECTOR_ON],
        )
        plans = await fetch_plans(session, signal_id, start, end)

        if df.empty:
            return empty_df

        # Single pass: bucket events by phase and collect detector-on times
        phase_events_list: dict[int, list[tuple[int, datetime]]] = defaultdict(list)
        det_times_by_channel: dict[int, list[datetime]] = defaultdict(list)

        for _, row in df.iterrows():
            code = int(row["event_code"])
            param = int(row["event_param"])
            event_time = row["event_time"]

            if code == EVENT_DETECTOR_ON:
                det_times_by_channel[param].append(event_time)
            elif param in target_phases:
                phase_events_list[param].append((code, event_time))

        bins_count = total_bins(start, end, x_bin_minutes)

        all_bin_rows: list[dict[str, Any]] = []
        for phase in sorted(target_phases):
            phase_bin_rows = _analyze_phase(
                phase=phase,
                phase_events=phase_events_list.get(phase, []),
                channel_to_phase=channel_to_phase,
                det_times_by_channel=det_times_by_channel,
                plans=plans,
                start=start,
                bins_count=bins_count,
                x_bin_minutes=x_bin_minutes,
                y_bin_seconds=y_bin_seconds,
            )
            if phase_bin_rows is not None:
                all_bin_rows.extend(phase_bin_rows)

        if not all_bin_rows:
            logger.info("Green-time-utilization complete: no data")
            return empty_df

        result_df = pd.DataFrame(all_bin_rows)
        logger.info("Green-time-utilization complete: %d rows across phases", len(result_df))
        return result_df


# ---------------------------------------------------------------------------
# Per-phase analysis
# ---------------------------------------------------------------------------


def _build_cycles_and_yr(
    phase_events: list[tuple[int, datetime]],
) -> tuple[list[tuple[datetime, datetime]], float]:
    """
    Single pass over one phase's events.

    Returns:
        cycles: list of (green_start, yellow_start) pairs.
        avg_yellow_red: mean seconds from yellow_clearance -> red_clearance.
    """
    cycles: list[tuple[datetime, datetime]] = []
    yr_durations: list[float] = []
    green_start: datetime | None = None
    yellow_start: datetime | None = None

    for code, event_time in phase_events:
        if code == EVENT_PHASE_GREEN:
            green_start = event_time
        elif code == EVENT_YELLOW_CLEARANCE:
            yellow_start = event_time
            if green_start is not None:
                cycles.append((green_start, yellow_start))
                green_start = None
        elif code == EVENT_RED_CLEARANCE and yellow_start is not None:
            yr_durations.append((event_time - yellow_start).total_seconds())
            yellow_start = None

    avg_yr = sum(yr_durations) / len(yr_durations) if yr_durations else 0.0
    return cycles, avg_yr


def _phase_channels(channel_to_phase: dict[int, int], phase: int) -> list[int]:
    return [ch for ch, ph in channel_to_phase.items() if ph == phase]


def _process_cycle(
    *,
    green_start: datetime,
    yellow_start: datetime,
    start: datetime,
    bins_count: int,
    x_bin_minutes: int,
    y_bin_seconds: int,
    det_times: list[datetime],
    bin_green_totals: dict[int, list[float]],
    bin_cycle_counts: dict[int, int],
    heatmap_counts: dict[tuple[int, int], int],
) -> None:
    """Bucket one green->yellow cycle into the aggregators."""
    x_bin = bin_index(green_start, start, x_bin_minutes)
    if x_bin < 0 or x_bin >= bins_count:
        return

    green_dur = (yellow_start - green_start).total_seconds()
    if green_dur < 0:
        return
    bin_green_totals[x_bin].append(green_dur)
    bin_cycle_counts[x_bin] += 1

    # Range-scan sorted detector times instead of O(E) full walk
    lo = bisect_left(det_times, green_start)
    hi = bisect_right(det_times, yellow_start)
    max_y_bin = int(green_dur // y_bin_seconds) if green_dur > 0 else 0
    for det_time in det_times[lo:hi]:
        offset = (det_time - green_start).total_seconds()
        y_bin = int(offset // y_bin_seconds)
        if 0 <= y_bin <= max_y_bin:
            heatmap_counts[(x_bin, y_bin)] += 1


def _analyze_phase(
    *,
    phase: int,
    phase_events: list[tuple[int, datetime]],
    channel_to_phase: dict[int, int],
    det_times_by_channel: dict[int, list[datetime]],
    plans: list[SignalPlan],
    start: datetime,
    bins_count: int,
    x_bin_minutes: int,
    y_bin_seconds: int,
) -> list[dict[str, Any]] | None:
    """Build the x-bin series for one phase, returning list of row dicts."""
    cycles, avg_yr = _build_cycles_and_yr(phase_events)
    if not cycles:
        return None

    # Detection times for this phase (union of per-channel sorted lists)
    det_times: list[datetime] = []
    for channel in _phase_channels(channel_to_phase, phase):
        det_times.extend(det_times_by_channel.get(channel, []))
    det_times.sort()

    bin_green_totals: dict[int, list[float]] = defaultdict(list)
    heatmap_counts: dict[tuple[int, int], int] = defaultdict(int)
    bin_cycle_counts: dict[int, int] = defaultdict(int)

    for green_start, yellow_start in cycles:
        _process_cycle(
            green_start=green_start,
            yellow_start=yellow_start,
            start=start,
            bins_count=bins_count,
            x_bin_minutes=x_bin_minutes,
            y_bin_seconds=y_bin_seconds,
            det_times=det_times,
            bin_green_totals=bin_green_totals,
            bin_cycle_counts=bin_cycle_counts,
            heatmap_counts=heatmap_counts,
        )

    bin_rows = _build_bin_rows(
        phase=phase,
        bins_count=bins_count,
        start=start,
        x_bin_minutes=x_bin_minutes,
        bin_green_totals=bin_green_totals,
        bin_cycle_counts=bin_cycle_counts,
        plans=plans,
        avg_yr=avg_yr,
    )

    return bin_rows if bin_rows else None


def _build_bin_rows(
    *,
    phase: int,
    bins_count: int,
    start: datetime,
    x_bin_minutes: int,
    bin_green_totals: dict[int, list[float]],
    bin_cycle_counts: dict[int, int],
    plans: list[SignalPlan],
    avg_yr: float,
) -> list[dict[str, Any]]:
    """Build per-bin rows with phase_number included."""
    raw_rows = []
    for x_bin in range(bins_count):
        cycle_count = bin_cycle_counts.get(x_bin, 0)
        if cycle_count == 0:
            continue
        bin_start = start + timedelta(minutes=x_bin * x_bin_minutes)
        greens = bin_green_totals[x_bin]
        avg_green = sum(greens) / len(greens)
        split = programmed_split(plans, phase, bin_start)
        available_green = max(split - avg_yr, 0.0)
        utilization_pct = (
            round(avg_green / available_green * 100, 1)
            if available_green > 0 else 0.0
        )
        raw_rows.append({
            "phase_number": phase,
            "x_bin": x_bin,
            "bin_start": bin_start.isoformat(),
            "cycle_count": cycle_count,
            "avg_green_seconds": round(avg_green, 2),
            "programmed_split_seconds": round(split, 2),
            "utilization_pct": utilization_pct,
        })
    return raw_rows
