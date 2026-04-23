"""
Purdue Coordination Diagram report plugin.

Produces cycle-level phase timing data suitable for rendering a
Purdue Coordination Diagram. Uses pre-computed cycle_boundary and
cycle_detector_arrival tables for historical queries (fast). Falls back
to raw event processing if aggregate tables are empty.
"""

import logging
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
    fetch_cycle_boundaries,
    fetch_events_split,
    parse_time,
)

logger = logging.getLogger(__name__)


class PurdueDiagramParams(BaseModel):
    signal_id: str = Field(..., description="Signal identifier")
    start: str = Field(..., description="Analysis window start (ISO-8601)")
    end: str = Field(..., description="Analysis window end (ISO-8601)")
    phase_number: int = Field(..., description="Phase number to analyze")


@ReportRegistry.register("purdue-diagram")
class PurdueDiagramReport(Report[PurdueDiagramParams]):
    """Cycle-level data for Purdue Coordination Diagram rendering."""

    metadata = ReportMetadata(
        name="purdue-diagram",
        description="Cycle-level phase timing and detector activations for Purdue diagram.",
        category="detailed",
        estimated_time="fast",
        export_formats=["csv", "json", "ndjson"],
    )

    async def execute(self, params: PurdueDiagramParams, session: AsyncSession) -> pd.DataFrame:
        """
        Execute Purdue diagram data extraction.

        Tries pre-computed aggregate tables first (fast path). Falls
        back to raw event processing if no aggregate data exists.

        Returns:
            DataFrame with columns: cycle_start, green_start, yellow_start,
            red_start, cycle_end, green_duration, yellow_duration, red_duration,
            cycle_duration, termination_type.
        """
        signal_id = params.signal_id
        start = parse_time(params.start)
        end = parse_time(params.end)
        phase_number = params.phase_number

        logger.info(
            "Running purdue-diagram for %s phase %d from %s to %s",
            signal_id, phase_number, start, end,
        )

        # Try aggregate tables first (fast path)
        result_df = await self._from_aggregates(
            signal_id, phase_number, start, end
        )

        if not result_df.empty:
            logger.info(
                "Purdue-diagram from aggregates: %d cycles", len(result_df)
            )
            return result_df

        # Fall back to raw events (slow path — real-time or no aggregates)
        logger.info("No aggregate data — falling back to raw events")
        config = await get_config_at(session, signal_id, as_of=start)
        det_channels = config.detector_channels_for_phase(phase_number)
        return await self._from_raw_events(
            signal_id, phase_number, start, end, det_channels
        )

    async def _from_aggregates(
        self,
        signal_id: str,
        phase: int,
        start,
        end,
    ) -> pd.DataFrame:
        """
        Build PCD data from pre-computed cycle_boundary and
        cycle_detector_arrival tables via the report SDK.
        """
        _COLUMNS = [
            "cycle_start", "green_start", "yellow_start", "red_start",
            "cycle_end", "green_duration", "yellow_duration", "red_duration",
            "cycle_duration", "termination_type",
        ]

        boundary_df = await fetch_cycle_boundaries(signal_id, phase, start, end)

        if boundary_df.empty:
            return pd.DataFrame(columns=_COLUMNS)

        # Build result DataFrame from boundary data
        rows = []
        for _, cb in boundary_df.iterrows():
            gs = cb["green_start"]
            rows.append({
                "cycle_start": gs,
                "green_start": gs,
                "yellow_start": _nullable_value(cb, "yellow_start"),
                "red_start": _nullable_value(cb, "red_start"),
                "cycle_end": _nullable_value(cb, "cycle_end"),
                "green_duration": _nullable_float(cb.get("green_duration_seconds")),
                "yellow_duration": _nullable_float(cb.get("yellow_duration_seconds")),
                "red_duration": _nullable_float(cb.get("red_duration_seconds")),
                "cycle_duration": _nullable_float(cb.get("cycle_duration_seconds")),
                "termination_type": cb["termination_type"],
            })

        return pd.DataFrame(rows, columns=_COLUMNS)

    async def _from_raw_events(
        self,
        signal_id: str,
        phase_number: int,
        start,
        end,
        det_channels: set[int],
    ) -> pd.DataFrame:
        """
        Build PCD data from raw ControllerEventLog events.

        Used for real-time data or when aggregate tables haven't been
        populated yet.
        """
        _COLUMNS = [
            "cycle_start", "green_start", "yellow_start", "red_start",
            "cycle_end", "green_duration", "yellow_duration", "red_duration",
            "cycle_duration", "termination_type",
        ]

        phase_codes = [EVENT_PHASE_GREEN, EVENT_YELLOW_CLEARANCE,
                       EVENT_RED_CLEARANCE, EVENT_PHASE_END]
        det_channel_list = list(det_channels)

        df = await fetch_events_split(
            signal_id, start, end,
            phase_codes=phase_codes,
            det_channels=det_channel_list,
            det_codes=(EVENT_DETECTOR_ON,),
        )

        if df.empty:
            return pd.DataFrame(columns=_COLUMNS)

        # Split into phase events and detector events
        phase_df = df[
            (df["event_param"] == phase_number)
            & (df["event_code"] != EVENT_DETECTOR_ON)
        ]

        # Identify cycle boundaries from green starts
        green_times = phase_df.loc[
            phase_df["event_code"] == EVENT_PHASE_GREEN, "event_time"
        ].tolist()

        if not green_times:
            return pd.DataFrame(columns=_COLUMNS)

        cycles = _assemble_cycles(green_times, phase_df)

        logger.info("Purdue-diagram from raw events: %d cycles", len(cycles))
        return pd.DataFrame(cycles, columns=_COLUMNS)


def _nullable_value(series: pd.Series, key: str):
    """Return value if not NaT/NaN, else None."""
    val = series.get(key)
    return val if pd.notna(val) else None


def _nullable_float(value) -> float | None:
    """Return float if value is not NaN, else None."""
    if value is None:
        return None
    return float(value) if pd.notna(value) else None


def _assemble_cycles(
    green_times: list,
    phase_df: pd.DataFrame,
) -> list[dict[str, Any]]:
    """
    Assemble cycle dicts from green boundary times and phase events
    using DataFrame slicing.
    """
    cycles: list[dict[str, Any]] = []

    for i, gs in enumerate(green_times):
        cycles.append(_build_raw_cycle(i, gs, green_times, phase_df))

    return cycles


def _build_raw_cycle(
    index: int,
    gs,
    green_times: list,
    phase_df: pd.DataFrame,
) -> dict[str, Any]:
    """Build a single cycle dict from raw events for one green interval."""
    ge = green_times[index + 1] if index + 1 < len(green_times) else pd.Timestamp.max
    window = phase_df[
        (phase_df["event_time"] > gs) & (phase_df["event_time"] < ge)
    ]

    yellow = window.loc[
        window["event_code"] == EVENT_YELLOW_CLEARANCE, "event_time"
    ]
    red = window.loc[
        window["event_code"] == EVENT_RED_CLEARANCE, "event_time"
    ]
    end_ev = window.loc[
        window["event_code"] == EVENT_PHASE_END, "event_time"
    ]

    yellow_start = yellow.iloc[0] if len(yellow) else None
    red_start = red.iloc[0] if len(red) else None
    cycle_end = end_ev.iloc[0] if len(end_ev) else None

    green_duration = (yellow_start - gs).total_seconds() if yellow_start is not None else None
    yellow_duration = (
        (red_start - yellow_start).total_seconds()
        if (yellow_start is not None and red_start is not None)
        else None
    )
    red_duration = (
        (cycle_end - red_start).total_seconds()
        if (red_start is not None and cycle_end is not None)
        else None
    )
    cycle_duration = (cycle_end - gs).total_seconds() if cycle_end is not None else None

    return {
        "cycle_start": gs,
        "green_start": gs,
        "yellow_start": yellow_start,
        "red_start": red_start,
        "cycle_end": cycle_end,
        "green_duration": round(green_duration, 2) if green_duration is not None else None,
        "yellow_duration": round(yellow_duration, 2) if yellow_duration is not None else None,
        "red_duration": round(red_duration, 2) if red_duration is not None else None,
        "cycle_duration": round(cycle_duration, 2) if cycle_duration is not None else None,
        "termination_type": None,
    }
