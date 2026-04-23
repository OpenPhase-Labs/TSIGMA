"""
Approach Delay report plugin.

Calculates average delay per approach by comparing detector activation
times against phase green start times. Supports configurable time binning.
"""

import logging
from datetime import datetime
from typing import Literal

import pandas as pd
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from ..config_resolver import get_config_at
from .registry import Report, ReportMetadata, ReportRegistry
from .sdk import (
    EVENT_DETECTOR_ON,
    EVENT_PHASE_GREEN,
    fetch_events_split,
    parse_time,
)

logger = logging.getLogger(__name__)

_BIN_FREQ = {"15min": "15min", "hour": "h", "day": "D"}


class ApproachDelayParams(BaseModel):
    """Parameters for approach delay analysis."""

    signal_id: str = Field(..., description="Signal identifier")
    start: str = Field(..., description="Analysis window start (ISO-8601)")
    end: str = Field(..., description="Analysis window end (ISO-8601)")
    bin_size: Literal["15min", "hour", "day"] = Field(
        "15min", description="Time bin size for aggregation"
    )


def _process_delay_event(
    row,
    green_starts: dict,
    channel_to_approach: dict,
    phase_to_approach: dict,
    approach_phases: dict,
    delay_rows: list,
) -> None:
    """Process a single event row for delay calculation."""
    code = int(row["event_code"])
    param = int(row["event_param"])
    event_time = row["event_time"]

    if code == EVENT_PHASE_GREEN and param in phase_to_approach:
        green_starts[param] = event_time
        return

    if code != EVENT_DETECTOR_ON or param not in channel_to_approach:
        return

    approach_id = channel_to_approach[param]
    phase = approach_phases.get(approach_id)
    if phase is None:
        return

    last_green = green_starts.get(phase)
    if last_green is None:
        return

    delay = (event_time - last_green).total_seconds()
    if delay >= 0:
        delay_rows.append({
            "approach_id": str(approach_id),
            "event_time": event_time,
            "delay": delay,
        })


@ReportRegistry.register("approach-delay")
class ApproachDelayReport(Report[ApproachDelayParams]):
    """Calculates average vehicular delay per approach using detector and phase data."""

    metadata = ReportMetadata(
        name="approach-delay",
        description="Average delay per approach based on detector activation vs green start.",
        category="standard",
        estimated_time="fast",
        export_formats=["csv", "json", "ndjson"],
    )

    async def execute(
        self, params: ApproachDelayParams, session: AsyncSession
    ) -> pd.DataFrame:
        signal_id = params.signal_id
        start = parse_time(params.start)
        end = parse_time(params.end)
        bin_size = params.bin_size

        logger.info("Running approach-delay for %s from %s to %s", signal_id, start, end)

        config = await get_config_at(session, signal_id, as_of=start)

        if not config.approaches:
            return pd.DataFrame(columns=["approach_id", "period", "avg_delay_seconds", "volume"])

        channel_to_approach: dict[int, int] = {}
        phase_to_approach: dict[int, int] = {}
        approach_phases: dict[int, int] = {}
        for approach in config.approaches:
            for det in config.detectors_for_approach(approach.approach_id):
                channel_to_approach[det.detector_channel] = approach.approach_id
            if approach.protected_phase_number is not None:
                phase_to_approach[approach.protected_phase_number] = approach.approach_id
                approach_phases[approach.approach_id] = approach.protected_phase_number

        if not channel_to_approach:
            return pd.DataFrame(columns=["approach_id", "period", "avg_delay_seconds", "volume"])

        df = await fetch_events_split(
            signal_id, start, end,
            phase_codes=[EVENT_PHASE_GREEN],
            det_channels=list(channel_to_approach.keys()),
            det_codes=[EVENT_DETECTOR_ON],
        )

        if df.empty:
            return pd.DataFrame(columns=["approach_id", "period", "avg_delay_seconds", "volume"])

        green_starts: dict[int, datetime] = {}
        delay_rows: list[dict] = []

        for _, row in df.iterrows():
            _process_delay_event(
                row, green_starts, channel_to_approach,
                phase_to_approach, approach_phases, delay_rows,
            )

        if not delay_rows:
            return pd.DataFrame(columns=["approach_id", "period", "avg_delay_seconds", "volume"])

        delays_df = pd.DataFrame(delay_rows)
        delays_df["event_time"] = pd.to_datetime(delays_df["event_time"])

        freq = _BIN_FREQ.get(bin_size, "15min")
        delays_df["period"] = delays_df["event_time"].dt.floor(freq).map(
            lambda dt: dt.isoformat()
        )

        grouped = delays_df.groupby(["approach_id", "period"])["delay"].agg(
            avg_delay_seconds="mean",
            volume="count",
        ).reset_index()

        grouped["avg_delay_seconds"] = grouped["avg_delay_seconds"].round(2)
        grouped = grouped.sort_values(["approach_id", "period"]).reset_index(drop=True)

        logger.info("Approach-delay complete: %d rows", len(grouped))
        return grouped
