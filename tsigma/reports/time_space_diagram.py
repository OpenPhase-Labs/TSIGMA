"""
Time-Space Diagram report plugin.

Corridor-level report that builds phase state intervals (green, yellow, red)
for each signal along a route, enabling visualization of signal coordination
and progression.

Uses pandas DataFrames for phase interval construction.
"""

import logging

import pandas as pd
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from .registry import Report, ReportMetadata, ReportRegistry
from .sdk import (
    EVENT_PHASE_GREEN,
    EVENT_RED_CLEARANCE,
    EVENT_YELLOW_CLEARANCE,
    fetch_events,
    parse_time,
)

logger = logging.getLogger(__name__)

_CODE_TO_STATE = {
    EVENT_PHASE_GREEN: "green",
    EVENT_YELLOW_CLEARANCE: "yellow",
    EVENT_RED_CLEARANCE: "red",
}


class TimeSpaceDiagramParams(BaseModel):
    signal_ids: list[str] = Field(..., description="Ordered list of signal identifiers along the corridor")
    start_time: str = Field(..., description="Analysis window start (ISO-8601)")
    end_time: str = Field(..., description="Analysis window end (ISO-8601)")
    direction_phase_map: dict[str, int] = Field(..., description="Map of signal_id to coordinated phase number")
    distances: dict[str, float] | None = Field(
        default=None,
        description="Map of signal_id to distance from origin in feet",
    )


@ReportRegistry.register("time-space-diagram")
class TimeSpaceDiagramReport(Report[TimeSpaceDiagramParams]):
    """Corridor time-space diagram with phase intervals."""

    metadata = ReportMetadata(
        name="time-space-diagram",
        description="Phase state intervals along a corridor for time-space visualization.",
        category="detailed",
        estimated_time="medium",
        export_formats=["csv", "json", "ndjson"],
    )

    async def execute(self, params: TimeSpaceDiagramParams, session: AsyncSession) -> pd.DataFrame:
        """
        Execute time-space diagram analysis.

        Returns:
            DataFrame with columns: signal_id, start, end, state.
        """
        signal_ids = params.signal_ids
        start_time = parse_time(params.start_time)
        end_time = parse_time(params.end_time)
        direction_phase_map = params.direction_phase_map

        logger.info(
            "Running time-space-diagram for %d signals from %s to %s",
            len(signal_ids), start_time, end_time,
        )

        phase_codes = (EVENT_PHASE_GREEN, EVENT_YELLOW_CLEARANCE, EVENT_RED_CLEARANCE)

        all_intervals: list[pd.DataFrame] = []

        for signal_id in signal_ids:
            phase_number = direction_phase_map.get(signal_id)
            if phase_number is None:
                continue

            events_df = await fetch_events(
                signal_id, start_time, end_time,
                list(phase_codes),
                event_param_in=[phase_number],
            )

            interval_df = _build_phase_intervals_df(events_df, signal_id)
            if not interval_df.empty:
                all_intervals.append(interval_df)

        if not all_intervals:
            logger.info("Time-space-diagram complete: no phase intervals found")
            return pd.DataFrame(columns=["signal_id", "start", "end", "state"])

        result_df = pd.concat(all_intervals, ignore_index=True)
        logger.info("Time-space-diagram complete: %d signals, %d intervals", len(signal_ids), len(result_df))
        return result_df


def _build_phase_intervals_df(df: pd.DataFrame, signal_id: str) -> pd.DataFrame:
    """
    Build phase state intervals from phase events DataFrame.

    Constructs green, yellow, and red intervals from the sequence of
    phase green (1), yellow clearance (8), and red clearance (9) events.

    Returns DataFrame with columns: signal_id, start, end, state.
    """
    if df.empty:
        return pd.DataFrame(columns=["signal_id", "start", "end", "state"])

    # Filter to only rows with recognised phase-state codes and map to state names
    mask = df["event_code"].isin(list(_CODE_TO_STATE.keys()))
    filtered = df.loc[mask, ["event_time", "event_code"]].copy()

    if filtered.empty:
        return pd.DataFrame(columns=["signal_id", "start", "end", "state"])

    filtered["state"] = filtered["event_code"].map(_CODE_TO_STATE)

    # Each interval starts at one event and ends at the next
    filtered["end"] = filtered["event_time"].shift(-1)

    # Drop the last row (no end time)
    filtered = filtered.dropna(subset=["end"])

    filtered["start"] = filtered["event_time"]
    filtered["signal_id"] = signal_id

    return filtered[["signal_id", "start", "end", "state"]].copy()
