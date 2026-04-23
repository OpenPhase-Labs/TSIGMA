"""
Approach Volume report plugin.

Counts vehicle detections per approach per time bin. Provides a
straightforward volume profile useful for capacity analysis, peak
hour identification, and turning movement counts.
"""

import logging

import pandas as pd
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from ..config_resolver import get_config_at
from .registry import Report, ReportMetadata, ReportRegistry
from .sdk import (
    DIRECTION_MAP,
    EVENT_DETECTOR_ON,
    fetch_events,
    parse_time,
)

logger = logging.getLogger(__name__)

_EMPTY_COLS = ["approach_id", "direction", "bin_start", "volume"]


class ApproachVolumeParams(BaseModel):
    """Parameters for approach volume analysis."""

    signal_id: str = Field(..., description="Signal identifier")
    start: str = Field(..., description="Analysis window start (ISO-8601)")
    end: str = Field(..., description="Analysis window end (ISO-8601)")
    bin_size_minutes: int = Field(15, description="Time bin size in minutes")


@ReportRegistry.register("approach-volume")
class ApproachVolumeReport(Report[ApproachVolumeParams]):
    """Vehicle counts per approach per time bin."""

    metadata = ReportMetadata(
        name="approach-volume",
        description="Vehicle volume counts per approach direction in configurable time bins.",
        category="standard",
        estimated_time="fast",
        export_formats=["csv", "json", "ndjson"],
    )

    async def execute(
        self, params: ApproachVolumeParams, session: AsyncSession
    ) -> pd.DataFrame:
        signal_id = params.signal_id
        start = parse_time(params.start)
        end = parse_time(params.end)
        bin_size_minutes = params.bin_size_minutes

        logger.info("Running approach-volume for %s from %s to %s", signal_id, start, end)

        # Get approaches with their detectors from historical config
        config = await get_config_at(session, signal_id, as_of=start)

        if not config.approaches:
            return pd.DataFrame(columns=_EMPTY_COLS)

        # Build mapping: detector_channel -> (approach_id, direction)
        channel_map: dict[int, tuple[str, str]] = {}
        for approach in config.approaches:
            direction = DIRECTION_MAP.get(approach.direction_type_id, "Unknown")
            for det in config.detectors_for_approach(approach.approach_id):
                channel_map[det.detector_channel] = (str(approach.approach_id), direction)

        if not channel_map:
            return pd.DataFrame(columns=_EMPTY_COLS)

        channels = list(channel_map.keys())

        df = await fetch_events(
            signal_id, start, end,
            (EVENT_DETECTOR_ON,),
            event_param_in=channels,
        )

        if df.empty:
            return pd.DataFrame(columns=_EMPTY_COLS)

        # Map channels to approach/direction and bin with pandas
        df["approach_id"] = df["event_param"].map(lambda ch: channel_map[int(ch)][0])
        df["direction"] = df["event_param"].map(lambda ch: channel_map[int(ch)][1])
        df["event_time"] = pd.to_datetime(df["event_time"])
        df["bin_start"] = df["event_time"].dt.floor(f"{bin_size_minutes}min").map(
            lambda dt: dt.isoformat()
        )

        grouped = (
            df.groupby(["approach_id", "direction", "bin_start"])
            .size()
            .reset_index(name="volume")
        )

        # Sort by bin_start then direction to match original output ordering
        grouped = grouped.sort_values(["bin_start", "direction", "approach_id"]).reset_index(drop=True)

        logger.info("Approach-volume complete: %d rows", len(grouped))
        return grouped[_EMPTY_COLS]
