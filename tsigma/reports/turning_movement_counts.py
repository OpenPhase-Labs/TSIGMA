"""
Turning Movement Counts report plugin.

Provides volume breakdown by approach direction per configurable time bin.
Counts detector-on events grouped by approach and time period to produce
standard turning movement count data.
"""

import logging

import pandas as pd
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from .registry import Report, ReportMetadata, ReportRegistry
from .sdk import (
    DIRECTION_MAP,
    EVENT_DETECTOR_ON,
    bin_timestamp,
    fetch_events,
    load_channel_to_approach,
    parse_time,
)

logger = logging.getLogger(__name__)

_EMPTY_COLS = ["bin_start", "direction", "volume", "approach_id"]


class TurningMovementCountsParams(BaseModel):
    """Parameters for turning movement counts."""

    signal_id: str = Field(..., description="Signal identifier")
    start: str = Field(..., description="Analysis window start (ISO-8601)")
    end: str = Field(..., description="Analysis window end (ISO-8601)")
    bin_size_minutes: int = Field(15, description="Time bin size in minutes")


@ReportRegistry.register("turning-movement-counts")
class TurningMovementCountsReport(Report[TurningMovementCountsParams]):
    """Volume breakdown by approach direction per time bin."""

    metadata = ReportMetadata(
        name="turning-movement-counts",
        description="Detector volume counts grouped by approach direction and time bin.",
        category="standard",
        estimated_time="fast",
        export_formats=["csv", "json", "ndjson"],
    )

    async def execute(
        self, params: TurningMovementCountsParams, session: AsyncSession
    ) -> pd.DataFrame:
        signal_id = params.signal_id
        start = parse_time(params.start)
        end = parse_time(params.end)
        bin_size_minutes = params.bin_size_minutes

        logger.info("Running turning-movement-counts for %s from %s to %s", signal_id, start, end)

        channel_info = await load_channel_to_approach(session, signal_id, as_of=start)
        channels = list(channel_info.keys())

        if not channels:
            logger.info("No detectors found for signal %s", signal_id)
            return pd.DataFrame(columns=_EMPTY_COLS)

        df = await fetch_events(signal_id, start, end, (EVENT_DETECTOR_ON,), event_param_in=channels)

        if df.empty:
            return pd.DataFrame(columns=_EMPTY_COLS)

        df = df.rename(columns={"event_param": "channel"})

        # Map channel to approach info
        df["approach_id"] = df["channel"].map(
            lambda ch: str(channel_info[ch]["approach_id"]) if ch in channel_info else None
        )
        df["direction"] = df["channel"].map(
            lambda ch: DIRECTION_MAP.get(channel_info[ch]["direction_type_id"], "Unknown")
            if ch in channel_info else "Unknown"
        )

        # Drop events with no channel mapping
        df = df.dropna(subset=["approach_id"])

        # Bin timestamps
        df["bin_start"] = df["event_time"].apply(
            lambda t: bin_timestamp(t, bin_size_minutes)
        )

        # Group by bin, approach, direction and count
        grouped = (
            df.groupby(["bin_start", "approach_id", "direction"])
            .size()
            .reset_index(name="volume")
            .sort_values(["bin_start", "approach_id", "direction"])
            .reset_index(drop=True)
        )

        logger.info("Turning-movement-counts complete: %d rows", len(grouped))
        return grouped[_EMPTY_COLS]
