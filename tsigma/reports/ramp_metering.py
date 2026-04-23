"""
Ramp Metering report plugin.

Analyzes ramp meter operations by calculating metering rate, green time,
demand/passage volumes from detector events, and queue occupancy from
queue detector occupancy measurements per time bin.

Uses pandas DataFrames for binning, grouping, and aggregation.
"""

import logging
from typing import Optional

import pandas as pd
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from .registry import Report, ReportMetadata, ReportRegistry
from .sdk import (
    EVENT_DETECTOR_OFF,
    EVENT_DETECTOR_ON,
    EVENT_PHASE_GREEN,
    EVENT_YELLOW_CLEARANCE,
    bin_occupancy_pct,
    bin_timestamp,
    fetch_events_split,
    parse_time,
)

logger = logging.getLogger(__name__)


class RampMeteringParams(BaseModel):
    signal_id: str = Field(..., description="Signal identifier")
    start_time: str = Field(..., description="Analysis window start (ISO-8601)")
    end_time: str = Field(..., description="Analysis window end (ISO-8601)")
    demand_detector_channel: int = Field(..., description="Detector channel for demand counts")
    passage_detector_channel: int = Field(..., description="Detector channel for passage counts")
    meter_phase: int = Field(default=1, description="Phase number for the ramp meter")
    queue_detector_channel: Optional[int] = Field(default=None, description="Detector channel for queue occupancy")
    time_bin_minutes: int = Field(default=15, description="Time bin width in minutes")


@ReportRegistry.register("ramp-metering")
class RampMeteringReport(Report[RampMeteringParams]):
    """Ramp meter performance analysis with metering rate and queue metrics."""

    metadata = ReportMetadata(
        name="ramp-metering",
        description="Ramp metering rate, demand/passage volumes, and queue occupancy per time bin.",
        category="standard",
        estimated_time="medium",
        export_formats=["csv", "json", "ndjson"],
    )

    async def execute(self, params: RampMeteringParams, session: AsyncSession) -> pd.DataFrame:
        """
        Execute ramp metering analysis.

        Returns:
            DataFrame with columns: time_bin, metering_rate, avg_green_seconds,
            demand_volume, passage_volume, queue_occupancy_pct.
        """
        signal_id = params.signal_id
        start_time = parse_time(params.start_time)
        end_time = parse_time(params.end_time)
        meter_phase = params.meter_phase
        demand_channel = params.demand_detector_channel
        passage_channel = params.passage_detector_channel
        queue_channel = params.queue_detector_channel
        time_bin_minutes = params.time_bin_minutes

        logger.info(
            "Running ramp-metering for %s from %s to %s",
            signal_id, start_time, end_time,
        )

        det_channels = [demand_channel, passage_channel]
        if queue_channel is not None:
            det_channels.append(queue_channel)

        df = await fetch_events_split(
            signal_id, start_time, end_time,
            phase_codes=(EVENT_PHASE_GREEN, EVENT_YELLOW_CLEARANCE),
            det_channels=det_channels,
            det_codes=(EVENT_DETECTOR_ON, EVENT_DETECTOR_OFF),
        )
        if df.empty:
            return pd.DataFrame(columns=[
                "time_bin", "metering_rate", "avg_green_seconds",
                "demand_volume", "passage_volume", "queue_occupancy_pct",
            ])
        df["time_bin"] = df["event_time"].apply(lambda t: bin_timestamp(t, time_bin_minutes))

        bin_avg_green = _compute_green_durations(df, meter_phase, time_bin_minutes)

        # --- Demand and passage volumes per bin ---
        demand_mask = (df["event_code"] == EVENT_DETECTOR_ON) & (df["event_param"] == demand_channel)
        passage_mask = (df["event_code"] == EVENT_DETECTOR_ON) & (df["event_param"] == passage_channel)

        bin_demand = df[demand_mask].groupby("time_bin").size()
        bin_passage = df[passage_mask].groupby("time_bin").size()

        bin_queue_occ = _compute_queue_occupancy(
            df, queue_channel, end_time, time_bin_minutes,
        )

        result_df = _assemble_results(
            bin_avg_green, bin_demand, bin_passage,
            bin_queue_occ, time_bin_minutes,
        )

        logger.info("Ramp-metering complete: %d bins", len(result_df))
        return result_df


def _compute_green_durations(
    df: pd.DataFrame, meter_phase: int, time_bin_minutes: int,
) -> pd.Series:
    """Pair green-start events with next yellow to compute per-bin avg green duration."""
    greens = df[
        (df["event_code"] == EVENT_PHASE_GREEN) & (df["event_param"] == meter_phase)
    ]
    yellows = df[
        (df["event_code"] == EVENT_YELLOW_CLEARANCE) & (df["event_param"] == meter_phase)
    ]

    green_durations = []
    yellow_idx = 0
    yellow_times = yellows["event_time"].tolist()
    for _, row in greens.iterrows():
        green_time = row["event_time"]
        while yellow_idx < len(yellow_times) and yellow_times[yellow_idx] <= green_time:
            yellow_idx += 1
        if yellow_idx >= len(yellow_times):
            break
        duration = (yellow_times[yellow_idx] - green_time).total_seconds()
        if 0 < duration <= 60:
            green_durations.append({
                "time_bin": bin_timestamp(green_time, time_bin_minutes),
                "green_duration": duration,
            })

    if green_durations:
        df_green = pd.DataFrame(green_durations)
        return df_green.groupby("time_bin")["green_duration"].mean().round(2)
    return pd.Series(dtype=float)


def _compute_queue_occupancy(
    df: pd.DataFrame, queue_channel, end_time, time_bin_minutes: int,
) -> dict[str, float]:
    """Extract queue detector events and compute per-bin occupancy percentage."""
    if queue_channel is None:
        return {}

    queue_mask = (df["event_param"] == queue_channel) & (
        df["event_code"].isin([EVENT_DETECTOR_ON, EVENT_DETECTOR_OFF])
    )
    queue_det_events = [
        (row["event_time"], row["event_code"])
        for _, row in df[queue_mask].iterrows()
    ]

    if not queue_det_events:
        return {}

    return bin_occupancy_pct(queue_det_events, end_time, time_bin_minutes)


def _assemble_results(
    bin_avg_green: pd.Series,
    bin_demand: pd.Series,
    bin_passage: pd.Series,
    bin_queue_occ: dict[str, float],
    time_bin_minutes: int,
) -> pd.DataFrame:
    """Combine per-bin metrics into the final result DataFrame."""
    all_bins = sorted(
        set(bin_avg_green.index)
        | set(bin_demand.index)
        | set(bin_passage.index)
        | set(bin_queue_occ.keys())
    )

    bin_seconds = time_bin_minutes * 60
    rows = []
    for b in all_bins:
        passage_vol = int(bin_passage.get(b, 0))
        metering_rate = round(passage_vol * (3600 / bin_seconds), 1)

        rows.append({
            "time_bin": b,
            "metering_rate": metering_rate,
            "avg_green_seconds": float(bin_avg_green.get(b, 0.0)),
            "demand_volume": int(bin_demand.get(b, 0)),
            "passage_volume": passage_vol,
            "queue_occupancy_pct": (
                round(bin_queue_occ[b], 1)
                if b in bin_queue_occ
                else None
            ),
        })

    return pd.DataFrame(rows)
