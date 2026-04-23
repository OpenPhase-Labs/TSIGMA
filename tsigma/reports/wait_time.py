"""
Wait Time report plugin.

Estimates vehicle wait time at a phase by measuring the delay between
detector activations during red and the subsequent green start. Provides
per-cycle statistics on arrivals during red and their wait durations.
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
    fetch_events_split,
    parse_time,
)

logger = logging.getLogger(__name__)


class WaitTimeParams(BaseModel):
    signal_id: str = Field(..., description="Signal identifier")
    start: str = Field(..., description="Analysis window start (ISO-8601)")
    end: str = Field(..., description="Analysis window end (ISO-8601)")
    phase_number: int = Field(..., description="Phase number to analyze")


def _collect_red_arrivals(
    df: pd.DataFrame, phase_number: int, det_channels: set,
) -> list[dict[str, Any]]:
    """Walk events and collect detector arrivals during red for each cycle."""
    cycles: list[dict[str, Any]] = []
    phase_ended: datetime | None = None
    red_arrivals: list[datetime] = []

    for _, row in df.iterrows():
        code = int(row["event_code"])
        param = int(row["event_param"])
        event_time = row["event_time"]

        is_phase_event = param == phase_number
        is_det_event = param in det_channels

        if code == EVENT_PHASE_END and is_phase_event:
            phase_ended = event_time
            red_arrivals = []
        elif code == EVENT_PHASE_GREEN and is_phase_event:
            if phase_ended is not None:
                cycles.append({
                    "cycle_start": event_time,
                    "arrivals": red_arrivals.copy(),
                })
            phase_ended = None
            red_arrivals = []
        elif code == EVENT_DETECTOR_ON and is_det_event:
            if phase_ended is not None:
                red_arrivals.append(event_time)

    return cycles


def _compute_cycle_stats(cycle: dict[str, Any]) -> dict[str, Any]:
    """Compute wait time statistics for a single cycle."""
    green_start = cycle["cycle_start"]
    arrivals = cycle["arrivals"]
    if not arrivals:
        return {
            "cycle_start": green_start.isoformat(),
            "arrivals_during_red": 0,
            "avg_wait_time": 0.0,
            "max_wait_time": 0.0,
            "min_wait_time": 0.0,
        }
    wait_series = pd.Series(
        [(green_start - a).total_seconds() for a in arrivals]
    )
    return {
        "cycle_start": green_start.isoformat(),
        "arrivals_during_red": len(arrivals),
        "avg_wait_time": round(wait_series.mean(), 2),
        "max_wait_time": round(wait_series.max(), 2),
        "min_wait_time": round(wait_series.min(), 2),
    }


@ReportRegistry.register("wait-time")
class WaitTimeReport(Report[WaitTimeParams]):
    """Estimates vehicle wait time from detector-on during red to green start."""

    metadata = ReportMetadata(
        name="wait-time",
        description="Vehicle wait time estimates based on red-interval detector arrivals.",
        category="standard",
        estimated_time="fast",
        export_formats=["csv", "json", "ndjson"],
    )

    async def execute(self, params: WaitTimeParams, session: AsyncSession) -> pd.DataFrame:
        """
        Execute wait time analysis.

        Returns:
            DataFrame with columns: cycle_start, arrivals_during_red,
            avg_wait_time, max_wait_time, min_wait_time.
        """
        signal_id = params.signal_id
        start = parse_time(params.start)
        end = parse_time(params.end)
        phase_number = params.phase_number

        logger.info(
            "Running wait-time for %s phase %d from %s to %s",
            signal_id, phase_number, start, end,
        )

        config = await get_config_at(session, signal_id, as_of=start)
        det_channels = config.detector_channels_for_phase(phase_number)

        if not det_channels:
            return pd.DataFrame(columns=[
                "cycle_start", "arrivals_during_red",
                "avg_wait_time", "max_wait_time", "min_wait_time",
            ])

        df = await fetch_events_split(
            signal_id, start, end,
            phase_codes=(EVENT_PHASE_GREEN, EVENT_PHASE_END),
            det_channels=det_channels,
            det_codes=(EVENT_DETECTOR_ON,),
        )
        if df.empty:
            return pd.DataFrame(columns=[
                "cycle_start", "arrivals_during_red",
                "avg_wait_time", "max_wait_time", "min_wait_time",
            ])

        cycles = _collect_red_arrivals(df, phase_number, det_channels)

        if not cycles:
            return pd.DataFrame(columns=[
                "cycle_start", "arrivals_during_red",
                "avg_wait_time", "max_wait_time", "min_wait_time",
            ])

        rows = [_compute_cycle_stats(c) for c in cycles]
        result_df = pd.DataFrame(rows)

        logger.info("Wait-time complete: %d cycles", len(result_df))
        return result_df
