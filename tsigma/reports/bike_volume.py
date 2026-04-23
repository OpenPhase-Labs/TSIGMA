"""
Bike Volume report plugin.

Bicycle detection volume analysis using specified detector channels.
Since the data model does not have an explicit bicycle flag on detectors,
the caller must provide detector_channels identifying which channels are
configured for bicycle detection.
"""

import logging
from typing import Optional

import pandas as pd
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from .registry import Report, ReportMetadata, ReportRegistry
from .sdk import (
    EVENT_DETECTOR_ON,
    fetch_events,
    parse_time,
)

logger = logging.getLogger(__name__)

_EMPTY_COLS = ["bin_start", "channel", "volume"]


class BikeVolumeParams(BaseModel):
    """Parameters for bike volume analysis."""

    signal_id: str = Field(..., description="Signal identifier")
    start: str = Field(..., description="Analysis window start (ISO-8601)")
    end: str = Field(..., description="Analysis window end (ISO-8601)")
    detector_channels: Optional[list[int]] = Field(
        None, description="Detector channels configured for bicycle detection"
    )
    bin_size_minutes: int = Field(15, description="Time bin size in minutes")


@ReportRegistry.register("bike-volume")
class BikeVolumeReport(Report[BikeVolumeParams]):
    """Bicycle detection volume analysis by time bin."""

    metadata = ReportMetadata(
        name="bike-volume",
        description="Bicycle volume counts for specified detector channels per time bin.",
        category="standard",
        estimated_time="fast",
        export_formats=["csv", "json", "ndjson"],
    )

    async def execute(
        self, params: BikeVolumeParams, session: AsyncSession
    ) -> pd.DataFrame:
        signal_id = params.signal_id
        start = parse_time(params.start)
        end = parse_time(params.end)
        detector_channels = params.detector_channels
        bin_size_minutes = params.bin_size_minutes

        logger.info("Running bike-volume for %s from %s to %s", signal_id, start, end)

        if not detector_channels:
            logger.info("No detector_channels provided for bike-volume")
            return pd.DataFrame(columns=_EMPTY_COLS)

        df = await fetch_events(
            signal_id, start, end,
            (EVENT_DETECTOR_ON,),
            event_param_in=detector_channels,
        )

        if df.empty:
            return pd.DataFrame(columns=_EMPTY_COLS)

        # Use pandas for binning and counting
        df["event_time"] = pd.to_datetime(df["event_time"])
        df["bin_start"] = df["event_time"].dt.floor(f"{bin_size_minutes}min").map(
            lambda dt: dt.isoformat()
        )
        df["channel"] = df["event_param"].astype(int)

        grouped = (
            df.groupby(["bin_start", "channel"])
            .size()
            .reset_index(name="volume")
        )
        grouped = grouped.sort_values(["bin_start", "channel"]).reset_index(drop=True)

        logger.info("Bike-volume complete: %d rows", len(grouped))
        return grouped[_EMPTY_COLS]
