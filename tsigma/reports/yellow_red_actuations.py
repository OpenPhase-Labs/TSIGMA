"""
Yellow/Red Actuations report plugin.

Counts detector actuations during green, yellow, and red intervals for
a specific phase. A key safety metric — high yellow and red actuations
indicate potential red-light running or dilemma zone issues.
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
    EVENT_DETECTOR_ON,
    EVENT_PHASE_END,
    EVENT_PHASE_GREEN,
    EVENT_RED_CLEARANCE,
    EVENT_YELLOW_CLEARANCE,
    fetch_events_split,
    parse_time,
)

logger = logging.getLogger(__name__)


class YellowRedActuationsParams(BaseModel):
    signal_id: str = Field(..., description="Signal identifier")
    start: str = Field(..., description="Analysis window start (ISO-8601)")
    end: str = Field(..., description="Analysis window end (ISO-8601)")
    phase_number: int = Field(..., description="Phase number to analyze")


def _flush_cycle(
    green_start: datetime,
    green_count: int,
    yellow_count: int,
    red_count: int,
) -> dict[str, Any]:
    """Build a cycle row dict from accumulated counters."""
    return {
        "cycle_start": green_start.isoformat(),
        "green_actuations": green_count,
        "yellow_actuations": yellow_count,
        "red_actuations": red_count,
    }


def _classify_detector_event(
    green_start: datetime | None,
    phase_end: datetime | None,
    red_start: datetime | None,
    yellow_start: datetime | None,
) -> str | None:
    """Return the interval name for a detector event, or None to skip."""
    if green_start is None:
        return None
    if phase_end is not None or red_start is not None:
        return "red"
    if yellow_start is not None:
        return "yellow"
    return "green"


def _classify_actuations(
    df: pd.DataFrame, phase_number: int, det_channels: set,
) -> list[dict[str, Any]]:
    """State machine that classifies detector actuations by interval."""
    cycle_rows: list[dict[str, Any]] = []
    green_start: datetime | None = None
    yellow_start: datetime | None = None
    red_start: datetime | None = None
    phase_end: datetime | None = None
    green_count = yellow_count = red_count = 0

    for _, row in df.iterrows():
        code = int(row["event_code"])
        param = int(row["event_param"])
        event_time = row["event_time"]

        is_phase_event = param == phase_number
        is_det_event = param in det_channels

        if code == EVENT_PHASE_GREEN and is_phase_event:
            if green_start is not None:
                cycle_rows.append(_flush_cycle(green_start, green_count, yellow_count, red_count))
            green_start = event_time
            yellow_start = red_start = phase_end = None
            green_count = yellow_count = red_count = 0
        elif code == EVENT_YELLOW_CLEARANCE and is_phase_event:
            yellow_start = event_time
        elif code == EVENT_RED_CLEARANCE and is_phase_event:
            red_start = event_time
        elif code == EVENT_PHASE_END and is_phase_event:
            phase_end = event_time
        elif code == EVENT_DETECTOR_ON and is_det_event:
            interval = _classify_detector_event(green_start, phase_end, red_start, yellow_start)
            if interval == "red":
                red_count += 1
            elif interval == "yellow":
                yellow_count += 1
            elif interval == "green":
                green_count += 1

    if green_start is not None:
        cycle_rows.append(_flush_cycle(green_start, green_count, yellow_count, red_count))

    return cycle_rows


@ReportRegistry.register("yellow-red-actuations")
class YellowRedActuationsReport(Report[YellowRedActuationsParams]):
    """Counts detector actuations during green, yellow, and red intervals."""

    metadata = ReportMetadata(
        name="yellow-red-actuations",
        description="Detector actuations by signal interval (green/yellow/red) per cycle.",
        category="standard",
        estimated_time="fast",
        export_formats=["csv", "json", "ndjson"],
    )

    async def execute(self, params: YellowRedActuationsParams, session: AsyncSession) -> pd.DataFrame:
        """
        Execute yellow/red actuations analysis.

        Returns:
            DataFrame with columns: cycle_start, green_actuations,
            yellow_actuations, red_actuations, total_actuations.
        """
        signal_id = params.signal_id
        start = parse_time(params.start)
        end = parse_time(params.end)
        phase_number = params.phase_number

        logger.info(
            "Running yellow-red-actuations for %s phase %d from %s to %s",
            signal_id, phase_number, start, end,
        )

        config = await get_config_at(session, signal_id, as_of=start)
        det_channels = config.detector_channels_for_phase(phase_number)

        if not det_channels:
            return pd.DataFrame(columns=[
                "cycle_start", "green_actuations", "yellow_actuations",
                "red_actuations", "total_actuations",
            ])

        df = await fetch_events_split(
            signal_id, start, end,
            phase_codes=(EVENT_PHASE_GREEN, EVENT_YELLOW_CLEARANCE,
                         EVENT_RED_CLEARANCE, EVENT_PHASE_END),
            det_channels=det_channels,
            det_codes=(EVENT_DETECTOR_ON,),
        )
        if df.empty:
            return pd.DataFrame(columns=[
                "cycle_start", "green_actuations", "yellow_actuations",
                "red_actuations", "total_actuations",
            ])

        cycle_rows = _classify_actuations(df, phase_number, det_channels)

        if not cycle_rows:
            return pd.DataFrame(columns=[
                "cycle_start", "green_actuations", "yellow_actuations",
                "red_actuations", "total_actuations",
            ])

        # Build results DataFrame and compute total_actuations via pandas
        result_df = pd.DataFrame(cycle_rows)
        result_df["total_actuations"] = (
            result_df["green_actuations"] + result_df["yellow_actuations"] + result_df["red_actuations"]
        )

        logger.info("Yellow-red-actuations complete: %d cycles", len(result_df))
        return result_df
