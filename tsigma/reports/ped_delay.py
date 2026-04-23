"""
Pedestrian Delay report plugin.

Analyzes pedestrian delay by measuring the time between pedestrian detector
actuations (button presses) and the start of the walk phase for each
pedestrian phase at a signal.
"""

import logging
from collections import defaultdict
from datetime import datetime

import pandas as pd
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from .registry import Report, ReportMetadata, ReportRegistry
from .sdk import (
    EVENT_DETECTOR_ON,
    EVENT_PED_WALK,
    fetch_events_split,
    load_channel_to_ped_phase,
    parse_time,
)

logger = logging.getLogger(__name__)

_EMPTY_COLS = ["phase_number", "ped_actuations", "avg_delay_seconds", "max_delay_seconds"]


class PedDelayParams(BaseModel):
    """Parameters for pedestrian delay analysis."""

    signal_id: str = Field(..., description="Signal identifier")
    start: str = Field(..., description="Analysis window start (ISO-8601)")
    end: str = Field(..., description="Analysis window end (ISO-8601)")


def _process_ped_event(
    row,
    channel_to_ped_phase: dict,
    ped_phases: set,
    pending_actuations: dict,
    phase_delays: dict,
) -> None:
    """Process a single event row for pedestrian delay calculation."""
    code = int(row["event_code"])
    param = int(row["event_param"])
    event_time = row["event_time"]

    if code == EVENT_DETECTOR_ON:
        ped_phase = channel_to_ped_phase.get(param)
        if ped_phase is not None:
            pending_actuations[ped_phase].append(event_time)
        return

    if code == EVENT_PED_WALK and param in ped_phases:
        for actuation_time in pending_actuations.get(param, []):
            delay = (event_time - actuation_time).total_seconds()
            if delay >= 0:
                phase_delays[param].append(delay)
        pending_actuations[param] = []


@ReportRegistry.register("ped-delay")
class PedDelayReport(Report[PedDelayParams]):
    """Calculates pedestrian delay from button press to walk indication."""

    metadata = ReportMetadata(
        name="ped-delay",
        description="Pedestrian delay analysis — time from actuation to walk phase.",
        category="standard",
        estimated_time="fast",
        export_formats=["csv", "json", "ndjson"],
    )

    async def execute(
        self, params: PedDelayParams, session: AsyncSession
    ) -> pd.DataFrame:
        signal_id = params.signal_id
        start = parse_time(params.start)
        end = parse_time(params.end)

        logger.info("Running ped-delay for %s from %s to %s", signal_id, start, end)

        # Get ped phases and their detector channels from historical config
        channel_to_ped_phase = await load_channel_to_ped_phase(session, signal_id, as_of=start)
        ped_phases = set(channel_to_ped_phase.values())

        if not ped_phases:
            return pd.DataFrame(columns=_EMPTY_COLS)

        det_channels = list(channel_to_ped_phase.keys())

        df = await fetch_events_split(
            signal_id, start, end,
            phase_codes=(EVENT_PED_WALK,),
            det_channels=det_channels,
            det_codes=(EVENT_DETECTOR_ON,),
        )
        if df.empty:
            return pd.DataFrame(columns=_EMPTY_COLS)

        # Track pending ped actuations (detector on events awaiting walk)
        pending_actuations: dict[int, list[datetime]] = defaultdict(list)
        phase_delays: dict[int, list[float]] = defaultdict(list)

        for _, row in df.iterrows():
            _process_ped_event(
                row, channel_to_ped_phase, ped_phases,
                pending_actuations, phase_delays,
            )

        # Build a flat DataFrame of all (phase, delay) pairs for aggregation
        delay_rows = [
            {"phase_number": phase, "delay": d}
            for phase, delays in phase_delays.items()
            for d in delays
        ]

        if delay_rows:
            delays_df = pd.DataFrame(delay_rows)
            agg = delays_df.groupby("phase_number")["delay"].agg(
                ped_actuations="count",
                avg_delay_seconds="mean",
                max_delay_seconds="max",
            ).reindex(sorted(ped_phases), fill_value=0).reset_index()
            agg["avg_delay_seconds"] = agg["avg_delay_seconds"].round(2).fillna(0.0)
            agg["max_delay_seconds"] = agg["max_delay_seconds"].round(2).fillna(0.0)
            agg["ped_actuations"] = agg["ped_actuations"].astype(int)
        else:
            agg = pd.DataFrame([
                {"phase_number": p, "ped_actuations": 0,
                 "avg_delay_seconds": 0.0, "max_delay_seconds": 0.0}
                for p in sorted(ped_phases)
            ])

        logger.info("Ped-delay complete: %d phases", len(agg))
        return agg[_EMPTY_COLS].reset_index(drop=True)
