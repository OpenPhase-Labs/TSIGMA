"""
Red Light Monitor report plugin.

Detects potential red light running events by identifying detector actuations
that occur after red clearance begins and before (or shortly after) phase end.

Uses pandas DataFrames for final result filtering and assembly.
"""

import logging
from datetime import datetime, timedelta
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

# Grace period after phase end to still count as a potential violation
RED_LIGHT_GRACE_SECONDS = 2.0


class RedLightMonitorParams(BaseModel):
    signal_id: str = Field(..., description="Signal identifier")
    start: str = Field(..., description="Analysis window start (ISO-8601)")
    end: str = Field(..., description="Analysis window end (ISO-8601)")
    phase_number: int = Field(..., description="Phase number to monitor")


@ReportRegistry.register("red-light-monitor")
class RedLightMonitorReport(Report[RedLightMonitorParams]):
    """Detects potential red light running events from detector actuations during red."""

    metadata = ReportMetadata(
        name="red-light-monitor",
        description="Identifies detector actuations during red clearance as potential violations.",
        category="detailed",
        estimated_time="medium",
        export_formats=["csv", "json", "ndjson"],
    )

    async def execute(self, params: RedLightMonitorParams, session: AsyncSession) -> pd.DataFrame:
        """
        Execute red light monitor analysis.

        Returns:
            DataFrame with columns: cycle_start, red_clearance_start,
            violations, violation_count.
        """
        signal_id = params.signal_id
        start = parse_time(params.start)
        end = parse_time(params.end)
        phase_number = params.phase_number

        logger.info(
            "Running red-light-monitor for %s phase %d from %s to %s",
            signal_id, phase_number, start, end,
        )

        config = await get_config_at(session, signal_id, as_of=start)
        detector_channels = list(config.detector_channels_for_phase(phase_number))

        if not detector_channels:
            logger.info("No detectors found for phase %d", phase_number)
            return pd.DataFrame(columns=[
                "cycle_start", "red_clearance_start", "violations", "violation_count",
            ])

        phase_codes = (EVENT_PHASE_GREEN, EVENT_YELLOW_CLEARANCE,
                       EVENT_RED_CLEARANCE, EVENT_PHASE_END)
        df = await fetch_events_split(
            signal_id, start, end,
            phase_codes=list(phase_codes),
            det_channels=detector_channels,
            det_codes=(EVENT_DETECTOR_ON,),
        )

        # Sequential state machine to parse cycles and violations
        cycle_records = _parse_cycles(df, phase_number, detector_channels)

        if not cycle_records:
            return pd.DataFrame(columns=[
                "cycle_start", "red_clearance_start", "violations", "violation_count",
            ])

        # Use DataFrame to filter to only cycles with violations
        result_df = pd.DataFrame(cycle_records)
        result_df["violation_count"] = result_df["violations"].apply(len)
        result_df = result_df[result_df["violation_count"] > 0]

        result_df = result_df[
            ["cycle_start", "red_clearance_start", "violations", "violation_count"]
        ].reset_index(drop=True)

        logger.info("Red-light-monitor complete: %d cycles with violations", len(result_df))
        return result_df


class _CycleState:
    """Mutable state for the red-light cycle state machine."""

    __slots__ = ("cycle_start", "red_clearance_start", "phase_end_time",
                 "current_violations")

    def __init__(self) -> None:
        self.cycle_start: datetime | None = None
        self.red_clearance_start: datetime | None = None
        self.phase_end_time: datetime | None = None
        self.current_violations: list[str] = []


def _flush_cycle(state: _CycleState, records: list[dict[str, Any]]) -> None:
    """Append the current cycle record if it has valid boundaries."""
    if state.cycle_start is not None and state.red_clearance_start is not None:
        records.append({
            "cycle_start": state.cycle_start.isoformat(),
            "red_clearance_start": state.red_clearance_start.isoformat(),
            "violations": state.current_violations,
        })


def _handle_phase_green(state: _CycleState, event_time: datetime,
                        records: list[dict[str, Any]]) -> None:
    """Handle a green phase event: flush previous cycle and reset state."""
    _flush_cycle(state, records)
    state.cycle_start = event_time
    state.red_clearance_start = None
    state.phase_end_time = None
    state.current_violations = []


def _handle_detector_on(state: _CycleState, event_time: datetime) -> None:
    """Check whether a detector activation qualifies as a violation."""
    if state.red_clearance_start is None:
        return

    if state.phase_end_time is None:
        if event_time >= state.red_clearance_start:
            state.current_violations.append(event_time.isoformat())
    else:
        grace_limit = state.phase_end_time + timedelta(seconds=RED_LIGHT_GRACE_SECONDS)
        if state.red_clearance_start <= event_time <= grace_limit:
            state.current_violations.append(event_time.isoformat())


def _parse_cycles(
    df: pd.DataFrame,
    phase_number: int,
    detector_channels: list[int],
) -> list[dict[str, Any]]:
    """
    Parse sequential events into cycle records with violation timestamps.

    Returns all cycles (including those with zero violations).
    """
    records: list[dict[str, Any]] = []
    state = _CycleState()

    for _, row in df.iterrows():
        code = int(row["event_code"])
        param = int(row["event_param"])
        event_time = row["event_time"]

        if code in (EVENT_PHASE_GREEN, EVENT_YELLOW_CLEARANCE,
                    EVENT_RED_CLEARANCE, EVENT_PHASE_END):
            if param != phase_number:
                continue
            if code == EVENT_PHASE_GREEN:
                _handle_phase_green(state, event_time, records)
            elif code == EVENT_RED_CLEARANCE:
                state.red_clearance_start = event_time
            elif code == EVENT_PHASE_END:
                state.phase_end_time = event_time

        elif code == EVENT_DETECTOR_ON and param in detector_channels:
            _handle_detector_on(state, event_time)

    # Flush final cycle
    _flush_cycle(state, records)

    return records
