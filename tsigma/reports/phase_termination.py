"""
Phase Termination report plugin.

Tracks how each phase terminates over time (gap-out, max-out, force-off,
skip) in configurable time bins. Useful for identifying phases that
consistently max out or get skipped, indicating timing plan issues.

Uses pre-computed cycle_boundary tables for historical queries (fast).
Falls back to raw event processing if aggregate tables are empty
(needed for skip detection anyway).
"""

import logging
from typing import Optional

import pandas as pd
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from .registry import Report, ReportMetadata, ReportRegistry
from .sdk import (
    EVENT_FORCE_OFF,
    EVENT_GAP_OUT,
    EVENT_MAX_OUT,
    EVENT_PHASE_GREEN,
    bin_timestamp,
    fetch_cycle_boundaries,
    fetch_events,
    parse_time,
)

logger = logging.getLogger(__name__)

BIN_SIZE_MINUTES = 15

_EMPTY_COLS = [
    "phase_number", "bin_start", "gap_out_count", "max_out_count",
    "force_off_count", "skip_count", "total_cycles",
]


class PhaseTerminationParams(BaseModel):
    """Parameters for phase termination analysis."""

    signal_id: str = Field(..., description="Signal identifier")
    start: str = Field(..., description="Analysis window start (ISO-8601)")
    end: str = Field(..., description="Analysis window end (ISO-8601)")
    phase_number: Optional[int] = Field(None, description="Filter to a single phase")


def _process_termination_event(
    row,
    phases_seen_green: set,
    all_phases: set,
    rows: list,
) -> None:
    """Process a single event row for phase termination classification."""
    phase = int(row["event_param"])
    code = int(row["event_code"])
    event_time = row["event_time"]
    bin_start = bin_timestamp(event_time, BIN_SIZE_MINUTES)

    if code == EVENT_PHASE_GREEN:
        all_phases.add(phase)
        if phase in phases_seen_green and len(all_phases) > 1:
            skipped = all_phases - phases_seen_green
            for skipped_phase in skipped:
                rows.append({"phase_number": skipped_phase,
                             "bin_start": bin_start, "type": "skip"})
            phases_seen_green.clear()
        phases_seen_green.add(phase)
        rows.append({"phase_number": phase, "bin_start": bin_start,
                     "type": "cycle"})
    elif code == EVENT_GAP_OUT:
        rows.append({"phase_number": phase, "bin_start": bin_start,
                     "type": "gap_out"})
    elif code == EVENT_MAX_OUT:
        rows.append({"phase_number": phase, "bin_start": bin_start,
                     "type": "max_out"})
    elif code == EVENT_FORCE_OFF:
        rows.append({"phase_number": phase, "bin_start": bin_start,
                     "type": "force_off"})


@ReportRegistry.register("phase-termination")
class PhaseTerminationReport(Report[PhaseTerminationParams]):
    """Tracks phase termination types (gap-out, max-out, force-off, skip) over time."""

    metadata = ReportMetadata(
        name="phase-termination",
        description="Phase termination type distribution in time bins.",
        category="standard",
        estimated_time="fast",
        export_formats=["csv", "json", "ndjson"],
    )

    async def execute(
        self, params: PhaseTerminationParams, session: AsyncSession
    ) -> pd.DataFrame:
        signal_id = params.signal_id
        start = parse_time(params.start)
        end = parse_time(params.end)
        phase_number = params.phase_number

        logger.info("Running phase-termination for %s from %s to %s", signal_id, start, end)

        # Try aggregate tables first (fast path) — requires phase_number
        if phase_number is not None:
            result_df = await self._from_aggregates(
                signal_id, phase_number, start, end
            )
            if not result_df.empty:
                logger.info(
                    "Phase-termination from aggregates: %d rows", len(result_df)
                )
                return result_df

        # Fall back to raw events (slow path — real-time or no aggregates)
        logger.info("No aggregate data — falling back to raw events")
        return await self._from_raw_events(signal_id, start, end)

    async def _from_aggregates(
        self,
        signal_id: str,
        phase: int,
        start,
        end,
    ) -> pd.DataFrame:
        """
        Build phase termination data from pre-computed cycle_boundary table.

        Each boundary row has a termination_type and green_start timestamp.
        Uses pandas for binning and pivot-based counting.
        """
        df = await fetch_cycle_boundaries(signal_id, phase, start, end)

        if df.empty:
            return pd.DataFrame(columns=_EMPTY_COLS)

        df["bin_start"] = df["green_start"].apply(
            lambda ts: bin_timestamp(ts, BIN_SIZE_MINUTES)
        )

        # Pivot: count each termination type per bin
        pivot = df.pivot_table(
            index="bin_start",
            columns="termination_type",
            aggfunc="size",
            fill_value=0,
        )

        # Ensure expected columns exist
        for col in ("gap_out", "max_out", "force_off"):
            if col not in pivot.columns:
                pivot[col] = 0

        result_df = pd.DataFrame({
            "phase_number": phase,
            "bin_start": pivot.index,
            "gap_out_count": pivot["gap_out"].values,
            "max_out_count": pivot["max_out"].values,
            "force_off_count": pivot["force_off"].values,
            "skip_count": 0,
            "total_cycles": pivot.sum(axis=1).values,
        })
        result_df = result_df.sort_values("bin_start").reset_index(drop=True)

        # Convert numpy int types to Python int for JSON serialisation
        for col in ("gap_out_count", "max_out_count", "force_off_count",
                     "skip_count", "total_cycles"):
            result_df[col] = result_df[col].astype(int)

        return result_df

    async def _from_raw_events(
        self,
        signal_id: str,
        start,
        end,
    ) -> pd.DataFrame:
        """
        Build phase termination data from raw ControllerEventLog events.

        Used for real-time data or when aggregate tables haven't been
        populated yet. Supports skip detection via ring-cycle tracking.

        Sequential event processing is required for skip detection, but
        final aggregation uses pandas for cleaner binning.
        """
        codes = [EVENT_PHASE_GREEN, EVENT_GAP_OUT, EVENT_MAX_OUT, EVENT_FORCE_OFF]
        df = await fetch_events(signal_id, start, end, codes)

        if df.empty:
            return pd.DataFrame(columns=_EMPTY_COLS)

        # Sequential pass: classify each event into (phase, bin, type) rows.
        # Skip detection requires ring-cycle state tracking.
        rows: list[dict] = []
        phases_seen_green: set[int] = set()
        all_phases: set[int] = set()

        for _, row in df.iterrows():
            _process_termination_event(
                row, phases_seen_green, all_phases, rows,
            )

        if not rows:
            return pd.DataFrame(columns=_EMPTY_COLS)

        result = pd.DataFrame(rows)

        # Pivot to get counts per (phase, bin, type)
        pivot = result.pivot_table(
            index=["phase_number", "bin_start"],
            columns="type",
            aggfunc="size",
            fill_value=0,
        ).reset_index()

        for col in ("gap_out", "max_out", "force_off", "skip", "cycle"):
            if col not in pivot.columns:
                pivot[col] = 0

        result_df = pd.DataFrame({
            "phase_number": pivot["phase_number"],
            "bin_start": pivot["bin_start"],
            "gap_out_count": pivot["gap_out"].astype(int),
            "max_out_count": pivot["max_out"].astype(int),
            "force_off_count": pivot["force_off"].astype(int),
            "skip_count": pivot["skip"].astype(int),
            "total_cycles": pivot["cycle"].astype(int),
        })
        result_df = result_df.sort_values(
            ["phase_number", "bin_start"]
        ).reset_index(drop=True)

        logger.info("Phase-termination complete: %d rows", len(result_df))
        return result_df
