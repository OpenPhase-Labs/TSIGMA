"""
Approach Speed report plugin.

Estimates vehicle speeds using detector occupancy time and distance from stop bar.
Speed is approximated as distance_from_stop_bar / occupancy_time for detectors
with a configured distance and minimum speed filter.
"""

import logging
from datetime import datetime
from typing import Optional

import pandas as pd
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from ..config_resolver import get_config_at
from .registry import Report, ReportMetadata, ReportRegistry
from .sdk import (
    DIRECTION_MAP,
    EVENT_DETECTOR_OFF,
    EVENT_DETECTOR_ON,
    fetch_events,
    parse_time,
)

logger = logging.getLogger(__name__)

_EMPTY_COLS = [
    "approach_id", "direction", "avg_speed", "p85_speed",
    "p15_speed", "sample_count", "speed_limit",
]


class ApproachSpeedParams(BaseModel):
    """Parameters for approach speed analysis."""

    signal_id: str = Field(..., description="Signal identifier")
    start: str = Field(..., description="Analysis window start (ISO-8601)")
    end: str = Field(..., description="Analysis window end (ISO-8601)")
    approach_id: Optional[int] = Field(None, description="Filter to a single approach")


def _process_speed_event(
    row,
    detector_config: dict,
    pending_on: dict,
    speed_rows: list,
) -> None:
    """Process a single detector event for speed calculation."""
    channel = int(row["event_param"])
    code = int(row["event_code"])
    event_time = row["event_time"]

    if channel not in detector_config:
        return

    if code == EVENT_DETECTOR_ON:
        pending_on[channel] = event_time
        return

    if code != EVENT_DETECTOR_OFF:
        return

    on_time = pending_on.pop(channel, None)
    if on_time is None:
        return

    occupancy_seconds = (event_time - on_time).total_seconds()
    if occupancy_seconds <= 0:
        return

    cfg = detector_config[channel]
    speed_fps = cfg["distance_ft"] / occupancy_seconds
    speed_mph = speed_fps / 1.467

    if speed_mph < cfg["min_speed"] or speed_mph > 150:
        return

    speed_rows.append({
        "approach_id": str(cfg["approach_id"]),
        "speed_mph": speed_mph,
    })


def _enrich_speed_results(
    agg: pd.DataFrame,
    detector_config: dict,
) -> pd.DataFrame:
    """Enrich aggregated speed data with config info (direction, speed limit)."""
    rows = []
    for _, row in agg.iterrows():
        app_id_str = row["approach_id"]
        cfg = None
        for ch_cfg in detector_config.values():
            if str(ch_cfg["approach_id"]) == app_id_str:
                cfg = ch_cfg
                break

        rows.append({
            "approach_id": app_id_str,
            "direction": (
                DIRECTION_MAP.get(cfg["direction_type_id"], "Unknown")
                if cfg else "Unknown"
            ),
            "avg_speed": float(row["avg_speed"]),
            "p85_speed": float(row["p85_speed"]),
            "p15_speed": float(row["p15_speed"]),
            "sample_count": int(row["sample_count"]),
            "speed_limit": cfg["mph"] if cfg else None,
        })
    return pd.DataFrame(rows)


@ReportRegistry.register("approach-speed")
class ApproachSpeedReport(Report[ApproachSpeedParams]):
    """Estimates vehicle speeds using detector occupancy and distance from stop bar."""

    metadata = ReportMetadata(
        name="approach-speed",
        description="Speed analysis using detector occupancy time and distance from stop bar.",
        category="standard",
        estimated_time="medium",
        export_formats=["csv", "json", "ndjson"],
    )

    async def execute(
        self, params: ApproachSpeedParams, session: AsyncSession
    ) -> pd.DataFrame:
        signal_id = params.signal_id
        start = parse_time(params.start)
        end = parse_time(params.end)
        approach_id = params.approach_id

        logger.info("Running approach-speed for %s from %s to %s", signal_id, start, end)

        # Get qualifying detectors from historical config
        config = await get_config_at(session, signal_id, as_of=start)

        # Build detector config lookup:
        # must have distance_from_stop_bar > 0 and min_speed_filter set
        detector_config: dict[int, dict] = {}
        for appr in config.approaches:
            if approach_id is not None and appr.approach_id != approach_id:
                continue
            for det in config.detectors_for_approach(appr.approach_id):
                if (det.distance_from_stop_bar
                        and det.distance_from_stop_bar > 0
                        and det.min_speed_filter is not None):
                    detector_config[det.detector_channel] = {
                        "distance_ft": det.distance_from_stop_bar,
                        "min_speed": det.min_speed_filter,
                        "approach_id": appr.approach_id,
                        "direction_type_id": appr.direction_type_id,
                        "mph": appr.mph,
                    }

        if not detector_config:
            logger.info("No qualifying detectors found for speed analysis")
            return pd.DataFrame(columns=_EMPTY_COLS)

        channels = list(detector_config.keys())

        df = await fetch_events(
            signal_id, start, end,
            (EVENT_DETECTOR_ON, EVENT_DETECTOR_OFF),
            event_param_in=channels,
        )

        if df.empty:
            return pd.DataFrame(columns=_EMPTY_COLS)

        # Sequential pass: pair on/off events to calculate speed
        pending_on: dict[int, datetime] = {}
        speed_rows: list[dict] = []

        for _, row in df.iterrows():
            _process_speed_event(row, detector_config, pending_on, speed_rows)

        if not speed_rows:
            return pd.DataFrame(columns=_EMPTY_COLS)

        # Use pandas for per-approach aggregation
        speeds_df = pd.DataFrame(speed_rows)

        def _percentile(p):
            def fn(x):
                sorted_vals = x.sort_values().values
                count = len(sorted_vals)
                idx = int(count * p / 100)
                return float(sorted_vals[min(idx, count - 1)])
            fn.__name__ = f"p{p}"
            return fn

        agg = speeds_df.groupby("approach_id")["speed_mph"].agg(
            avg_speed="mean",
            sample_count="count",
            p85_speed=_percentile(85),
            p15_speed=_percentile(15),
        ).reset_index()

        agg["avg_speed"] = agg["avg_speed"].round(1)
        agg["p85_speed"] = agg["p85_speed"].round(1)
        agg["p15_speed"] = agg["p15_speed"].round(1)
        agg = agg.sort_values("approach_id")

        # Enrich with config data (direction, speed limit)
        result_df = _enrich_speed_results(agg, detector_config)

        logger.info("Approach-speed complete: %d approaches", len(result_df))
        return result_df
