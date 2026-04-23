"""
Split Failure report plugin.

Detects cycles where traffic demand exceeds the allocated green time
(split) by measuring detector occupancy at the start of green and
start of red. High occupancy in both windows indicates vehicles
queued through the entire green — a split failure.

Uses pandas DataFrames for final cycle metric assembly and threshold evaluation.
"""

import logging
from datetime import datetime
from typing import Any

import pandas as pd
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from ..config_resolver import get_config_at
from .registry import Report, ReportMetadata, ReportRegistry
from .sdk import (
    EVENT_DETECTOR_OFF,
    EVENT_DETECTOR_ON,
    EVENT_PHASE_END,
    EVENT_PHASE_GREEN,
    EVENT_RED_CLEARANCE,
    EVENT_YELLOW_CLEARANCE,
    calculate_occupancy,
    fetch_events_split,
    parse_time,
)

logger = logging.getLogger(__name__)

# Occupancy measurement window
OCCUPANCY_WINDOW_SECONDS = 5.0


class SplitFailureParams(BaseModel):
    signal_id: str = Field(..., description="Signal identifier")
    start: str = Field(..., description="Analysis window start (ISO-8601)")
    end: str = Field(..., description="Analysis window end (ISO-8601)")
    phase_number: int = Field(..., description="Phase number to analyze")
    green_occ_threshold: float = Field(default=0.79, description="Green-start occupancy threshold for split failure")
    red_occ_threshold: float = Field(default=0.79, description="Red-start occupancy threshold for split failure")


@ReportRegistry.register("split-failure")
class SplitFailureReport(Report[SplitFailureParams]):
    """Detects split failures using green-start and red-start occupancy thresholds."""

    metadata = ReportMetadata(
        name="split-failure",
        description="Identifies cycles where demand exceeds green time capacity.",
        category="standard",
        estimated_time="medium",
        export_formats=["csv", "json", "ndjson"],
    )

    async def execute(self, params: SplitFailureParams, session: AsyncSession) -> pd.DataFrame:
        """
        Execute split failure analysis.

        Returns:
            DataFrame with columns: cycle_start, green_start_occupancy,
            red_start_occupancy, green_duration, cycle_length, is_split_failure.
        """
        signal_id = params.signal_id
        start = parse_time(params.start)
        end = parse_time(params.end)
        phase_number = params.phase_number
        green_occ_threshold = params.green_occ_threshold
        red_occ_threshold = params.red_occ_threshold

        logger.info(
            "Running split-failure for %s phase %d from %s to %s",
            signal_id, phase_number, start, end,
        )

        config = await get_config_at(session, signal_id, as_of=start)
        det_channels = config.detector_channels_for_phase(phase_number)

        if not det_channels:
            return pd.DataFrame(columns=[
                "cycle_start", "green_start_occupancy", "red_start_occupancy",
                "green_duration", "cycle_length", "is_split_failure",
            ])

        phase_codes = (EVENT_PHASE_GREEN, EVENT_YELLOW_CLEARANCE,
                       EVENT_RED_CLEARANCE, EVENT_PHASE_END)
        df = await fetch_events_split(
            signal_id, start, end,
            phase_codes=list(phase_codes),
            det_channels=list(det_channels),
            det_codes=(EVENT_DETECTOR_ON, EVENT_DETECTOR_OFF),
        )

        # Build cycle boundaries via sequential event parsing
        cycles = _extract_cycles(df, phase_number, det_channels)

        if not cycles:
            return pd.DataFrame(columns=[
                "cycle_start", "green_start_occupancy", "red_start_occupancy",
                "green_duration", "cycle_length", "is_split_failure",
            ])

        # Compute occupancy for each cycle and build DataFrame
        cycle_rows = []
        for cycle in cycles:
            green_start = cycle["green_start"]
            yellow_start = cycle["yellow_start"]
            red_window_start = cycle["red_window_start"]
            next_green = cycle["next_green"]

            green_duration = (yellow_start - green_start).total_seconds()
            cycle_length = (next_green - green_start).total_seconds()

            green_start_occ = calculate_occupancy(
                cycle["det_events"], green_start, OCCUPANCY_WINDOW_SECONDS,
            )
            red_start_occ = calculate_occupancy(
                cycle["det_events"], red_window_start, OCCUPANCY_WINDOW_SECONDS,
            )

            cycle_rows.append({
                "cycle_start": green_start.isoformat(),
                "green_start_occupancy": green_start_occ,
                "red_start_occupancy": red_start_occ,
                "green_duration": green_duration,
                "cycle_length": cycle_length,
            })

        # Use DataFrame for threshold evaluation and rounding
        result_df = pd.DataFrame(cycle_rows)
        result_df["is_split_failure"] = (
            (result_df["green_start_occupancy"] >= green_occ_threshold)
            & (result_df["red_start_occupancy"] >= red_occ_threshold)
        )
        result_df["green_start_occupancy"] = result_df["green_start_occupancy"].round(3)
        result_df["red_start_occupancy"] = result_df["red_start_occupancy"].round(3)
        result_df["green_duration"] = result_df["green_duration"].round(2)
        result_df["cycle_length"] = result_df["cycle_length"].round(2)

        logger.info("Split-failure complete: %d cycles", len(result_df))
        return result_df


class _SplitCycleState:
    """Mutable state for the split-failure cycle state machine."""

    __slots__ = ("green_start", "yellow_start", "red_start",
                 "phase_end", "det_events")

    def __init__(self) -> None:
        self.green_start: datetime | None = None
        self.yellow_start: datetime | None = None
        self.red_start: datetime | None = None
        self.phase_end: datetime | None = None
        self.det_events: list[tuple[datetime, int]] = []


def _resolve_red_window_start(state: _SplitCycleState) -> datetime:
    """Determine the red window start from available phase boundaries."""
    if state.red_start is not None:
        return state.red_start
    if state.phase_end is not None:
        return state.phase_end
    return state.yellow_start  # type: ignore[return-value]


def _flush_split_cycle(
    state: _SplitCycleState,
    next_green: datetime,
    cycles: list[dict[str, Any]],
) -> None:
    """Append completed cycle if it has valid green and yellow boundaries."""
    if state.green_start is not None and state.yellow_start is not None:
        cycles.append({
            "green_start": state.green_start,
            "yellow_start": state.yellow_start,
            "red_window_start": _resolve_red_window_start(state),
            "next_green": next_green,
            "det_events": state.det_events,
        })


def _reset_split_state(state: _SplitCycleState, event_time: datetime) -> None:
    """Reset cycle state for a new green phase."""
    state.green_start = event_time
    state.yellow_start = None
    state.red_start = None
    state.phase_end = None
    state.det_events = []


def _extract_cycles(
    df: pd.DataFrame,
    phase_number: int,
    det_channels: set | list,
) -> list[dict[str, Any]]:
    """
    Parse sequential events into cycle boundaries with detector events.

    Returns list of cycle dicts with green_start, yellow_start,
    red_window_start, next_green, and det_events.
    """
    cycles: list[dict[str, Any]] = []
    state = _SplitCycleState()

    for _, row in df.iterrows():
        code = int(row["event_code"])
        param = int(row["event_param"])
        event_time = row["event_time"]
        is_phase_event = param == phase_number
        is_det_event = param in det_channels

        if code == EVENT_PHASE_GREEN and is_phase_event:
            _flush_split_cycle(state, event_time, cycles)
            _reset_split_state(state, event_time)

        elif code == EVENT_YELLOW_CLEARANCE and is_phase_event:
            state.yellow_start = event_time

        elif code == EVENT_RED_CLEARANCE and is_phase_event:
            state.red_start = event_time

        elif code == EVENT_PHASE_END and is_phase_event:
            state.phase_end = event_time

        elif is_det_event and code in (EVENT_DETECTOR_ON, EVENT_DETECTOR_OFF):
            if state.green_start is not None:
                state.det_events.append((event_time, code))

    return cycles
