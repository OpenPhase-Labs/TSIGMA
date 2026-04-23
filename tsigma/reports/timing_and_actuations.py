"""
Timing and Actuations report plugin.

Provides raw phase timing visualization data with all actuations overlaid.
Returns a DataFrame of all phase and detector events with human-readable
event names. Phase summary is derivable from the events DataFrame.
"""

import logging

import pandas as pd
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from .registry import Report, ReportMetadata, ReportRegistry
from .sdk import (
    DETECTOR_EVENT_CODES,
    EVENT_FORCE_OFF,
    EVENT_GAP_OUT,
    EVENT_GREEN_TERMINATION,
    EVENT_MAX_OUT,
    EVENT_NAMES,
    EVENT_PHASE_END,
    EVENT_PHASE_GREEN,
    EVENT_RED_CLEARANCE,
    EVENT_YELLOW_CLEARANCE,
    fetch_events,
    parse_time,
)

logger = logging.getLogger(__name__)

PHASE_EVENT_CODES = (
    EVENT_PHASE_GREEN, EVENT_GAP_OUT, EVENT_MAX_OUT, EVENT_FORCE_OFF,
    EVENT_GREEN_TERMINATION, EVENT_YELLOW_CLEARANCE, EVENT_RED_CLEARANCE,
    EVENT_PHASE_END,
)

ALL_EVENT_CODES = PHASE_EVENT_CODES + DETECTOR_EVENT_CODES


class TimingAndActuationsParams(BaseModel):
    signal_id: str = Field(..., description="Signal identifier")
    start: str = Field(..., description="Analysis window start (ISO-8601)")
    end: str = Field(..., description="Analysis window end (ISO-8601)")
    phases: list[int] | None = Field(default=None, description="Phase numbers to filter (None = all)")


@ReportRegistry.register("timing-and-actuations")
class TimingAndActuationsReport(Report[TimingAndActuationsParams]):
    """Raw phase timing visualization data with detector actuations overlaid."""

    metadata = ReportMetadata(
        name="timing-and-actuations",
        description="Phase timing timeline with all actuations for visualization.",
        category="detailed",
        estimated_time="slow",
        export_formats=["csv", "json", "ndjson"],
    )

    async def execute(self, params: TimingAndActuationsParams, session: AsyncSession) -> pd.DataFrame:
        """
        Execute timing and actuations report.

        Returns:
            DataFrame with columns: event_time, event_code, event_param, event_name.
        """
        signal_id = params.signal_id
        start = parse_time(params.start)
        end = parse_time(params.end)
        phases_filter = params.phases

        logger.info("Running timing-and-actuations for %s from %s to %s", signal_id, start, end)

        df = await fetch_events(signal_id, start, end, ALL_EVENT_CODES)

        if df.empty:
            return pd.DataFrame(columns=["event_time", "event_code", "event_param", "event_name"])

        # Apply phase filter: keep detector events always, phase events only for selected phases
        if phases_filter:
            is_phase_event = df["event_code"].isin(PHASE_EVENT_CODES)
            is_filtered_phase = df["event_param"].isin(phases_filter)
            df = df[~is_phase_event | is_filtered_phase]

        # Build timeline DataFrame
        df["event_name"] = df["event_code"].map(
            lambda c: EVENT_NAMES.get(c, f"Unknown ({c})")
        )

        result_df = df[["event_time", "event_code", "event_param", "event_name"]].copy()

        logger.info(
            "Timing-and-actuations complete: %d events",
            len(result_df),
        )
        return result_df
