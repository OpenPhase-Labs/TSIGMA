"""
Split Monitor report plugin.

Analyzes phase split timing and termination type distribution for
each phase at a signal. Provides green, yellow, and red clearance
durations along with force-off, gap-out, and max-out percentages.

Uses pre-computed cycle_boundary tables for historical queries (fast).
Falls back to raw event processing if aggregate tables are empty.

Uses pandas DataFrames for aggregation and metric computation.
"""

import logging
from datetime import datetime
from typing import Any, Optional

import pandas as pd
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from .registry import Report, ReportMetadata, ReportRegistry
from .sdk import (
    EVENT_FORCE_OFF,
    EVENT_GAP_OUT,
    EVENT_MAX_OUT,
    EVENT_PHASE_GREEN,
    EVENT_RED_CLEARANCE,
    EVENT_YELLOW_CLEARANCE,
    TERMINATION_CODES,
    fetch_cycle_boundaries,
    fetch_events,
    parse_time,
    pct,
)

logger = logging.getLogger(__name__)


class SplitMonitorParams(BaseModel):
    signal_id: str = Field(..., description="Signal identifier")
    start: str = Field(..., description="Analysis window start (ISO-8601)")
    end: str = Field(..., description="Analysis window end (ISO-8601)")
    phase_number: Optional[int] = Field(default=None, description="Phase number to filter (all phases if omitted)")


@ReportRegistry.register("split-monitor")
class SplitMonitorReport(Report[SplitMonitorParams]):
    """Analyzes phase split durations and green termination types."""

    metadata = ReportMetadata(
        name="split-monitor",
        description="Phase split timing with force-off, gap-out, and max-out percentages.",
        category="standard",
        estimated_time="fast",
        export_formats=["csv", "json", "ndjson"],
    )

    async def execute(self, params: SplitMonitorParams, session: AsyncSession) -> pd.DataFrame:
        """
        Execute split monitor analysis.

        Tries pre-computed aggregate tables first (fast path). Falls
        back to raw event processing if no aggregate data exists.

        Returns:
            DataFrame with columns: phase_number, cycles, green_time, yellow_time,
            red_clearance_time, total_split, force_off_pct, gap_out_pct, max_out_pct.
        """
        signal_id = params.signal_id
        start = parse_time(params.start)
        end = parse_time(params.end)
        phase_number = params.phase_number

        logger.info("Running split-monitor for %s from %s to %s", signal_id, start, end)

        empty_cols = [
            "phase_number", "cycles", "green_time", "yellow_time",
            "red_clearance_time", "total_split", "force_off_pct",
            "gap_out_pct", "max_out_pct",
        ]

        # Try aggregate tables first (fast path) — requires phase_number
        if phase_number is not None:
            result = await self._from_aggregates(
                signal_id, phase_number, start, end
            )
            if result:
                logger.info("Split-monitor from aggregates: %d phases", len(result))
                return pd.DataFrame(result)

        # Fall back to raw events (slow path — real-time or no aggregates)
        logger.info("No aggregate data — falling back to raw events")
        return await self._from_raw_events(signal_id, start, end, empty_cols)

    async def _from_aggregates(
        self,
        signal_id: str,
        phase: int,
        start: datetime,
        end: datetime,
    ) -> list[dict[str, Any]]:
        """
        Build split monitor data from pre-computed cycle_boundary table.

        Uses pandas for vectorized aggregation of boundary metrics.
        """
        df = await fetch_cycle_boundaries(signal_id, phase, start, end)

        if df.empty:
            return []

        cycle_count = len(df)

        # Compute total_split = green + yellow + red clearance
        df["total_split"] = (
            df["green_duration_seconds"].fillna(0)
            + df["yellow_duration_seconds"].fillna(0)
            + df["red_duration_seconds"].fillna(0)
        )

        avg_green = (
            round(df["green_duration_seconds"].dropna().mean(), 2)
            if df["green_duration_seconds"].notna().any()
            else 0.0
        )
        avg_yellow = (
            round(df["yellow_duration_seconds"].dropna().mean(), 2)
            if df["yellow_duration_seconds"].notna().any()
            else 0.0
        )

        # Only compute avg_total from rows that have cycle_duration
        has_cycle = df["cycle_duration_seconds"].notna()
        avg_total = round(df.loc[has_cycle, "total_split"].mean(), 2) if has_cycle.any() else 0.0

        avg_red_clearance = (
            round(max(avg_total - avg_green - avg_yellow, 0.0), 2)
            if has_cycle.any() else 0.0
        )

        # Termination counts via value_counts
        term_counts = df["termination_type"].value_counts()
        gap_out_count = int(term_counts.get("gap_out", 0))
        max_out_count = int(term_counts.get("max_out", 0))
        force_off_count = int(term_counts.get("force_off", 0))

        return [{
            "phase_number": phase,
            "cycles": cycle_count,
            "green_time": avg_green,
            "yellow_time": avg_yellow,
            "red_clearance_time": avg_red_clearance,
            "total_split": avg_total,
            "force_off_pct": pct(force_off_count, cycle_count),
            "gap_out_pct": pct(gap_out_count, cycle_count),
            "max_out_pct": pct(max_out_count, cycle_count),
        }]

    async def _from_raw_events(
        self,
        signal_id: str,
        start: datetime,
        end: datetime,
        empty_cols: list[str],
    ) -> pd.DataFrame:
        """
        Build split data from raw ControllerEventLog events.

        Uses pandas for final metric aggregation after sequential event parsing.
        """
        codes = [
            EVENT_PHASE_GREEN, EVENT_GAP_OUT, EVENT_MAX_OUT,
            EVENT_FORCE_OFF, EVENT_YELLOW_CLEARANCE, EVENT_RED_CLEARANCE,
        ]
        df = await fetch_events(signal_id, start, end, codes)

        if df.empty:
            return pd.DataFrame(columns=empty_cols)

        phase_metrics = _accumulate_phase_metrics(df)

        results = _build_split_results(phase_metrics)

        logger.info("Split-monitor complete: %d phases", len(results))
        return pd.DataFrame(results) if results else pd.DataFrame(columns=empty_cols)


def _accumulate_phase_metrics(df: pd.DataFrame) -> dict[int, list[dict]]:
    """
    Walk events and accumulate per-phase cycle records.

    Returns a dict mapping phase number to a list of cycle dicts,
    each containing green_dur, yellow_dur, total_split, and termination.
    """
    phase_green_start: dict[int, Any] = {}
    phase_yellow_start: dict[int, Any] = {}
    # Collect per-cycle records per phase for DataFrame conversion
    phase_cycles: dict[int, list[dict]] = {}
    # Track current cycle termination per phase
    phase_termination: dict[int, str | None] = {}

    for _, row in df.iterrows():
        phase = int(row["event_param"])
        code = int(row["event_code"])
        event_time = row["event_time"]

        if code == EVENT_PHASE_GREEN:
            _handle_green_event(phase, event_time, phase_green_start, phase_termination)
        elif code in TERMINATION_CODES:
            phase_termination[phase] = TERMINATION_CODES[code]
        elif code == EVENT_YELLOW_CLEARANCE:
            _handle_yellow_event(phase, event_time, phase_green_start, phase_yellow_start, phase_cycles)
        elif code == EVENT_RED_CLEARANCE:
            _handle_red_clearance_event(
                phase, event_time, phase_green_start, phase_yellow_start,
                phase_cycles, phase_termination,
            )

    return phase_cycles


def _handle_green_event(
    phase: int, event_time: Any,
    phase_green_start: dict, phase_termination: dict,
) -> None:
    """Record green start and reset termination for a phase."""
    phase_green_start[phase] = event_time
    phase_termination[phase] = None


def _handle_yellow_event(
    phase: int, event_time: Any,
    phase_green_start: dict, phase_yellow_start: dict,
    phase_cycles: dict,
) -> None:
    """Record yellow start for a phase."""
    phase_yellow_start[phase] = event_time
    if phase not in phase_cycles:
        phase_cycles[phase] = []


def _nonneg_duration(start: Any, end: Any) -> float | None:
    """Return duration in seconds if non-negative, else None."""
    dur = (end - start).total_seconds()
    return dur if dur >= 0 else None


def _handle_red_clearance_event(
    phase: int, event_time: Any,
    phase_green_start: dict, phase_yellow_start: dict,
    phase_cycles: dict, phase_termination: dict,
) -> None:
    """Compute cycle durations at red clearance and append to phase_cycles."""
    yellow_dur = (
        _nonneg_duration(phase_yellow_start[phase], event_time)
        if phase in phase_yellow_start else None
    )
    total_split = (
        _nonneg_duration(phase_green_start[phase], event_time)
        if phase in phase_green_start else None
    )
    green_dur = (
        _nonneg_duration(phase_green_start[phase], phase_yellow_start[phase])
        if phase in phase_green_start and phase in phase_yellow_start else None
    )

    if phase not in phase_cycles:
        phase_cycles[phase] = []

    if total_split is not None:
        phase_cycles[phase].append({
            "green_dur": green_dur,
            "yellow_dur": yellow_dur,
            "total_split": total_split,
            "termination": phase_termination.get(phase),
        })


_TERMINATION_KEY = {code: f"{name}_count" for code, name in TERMINATION_CODES.items()}


def _build_split_results(phase_cycles: dict[int, list[dict]]) -> list[dict[str, Any]]:
    """Convert accumulated phase cycle records into result dicts using pandas."""
    results = []
    for phase_num in sorted(phase_cycles.keys()):
        cycles = phase_cycles[phase_num]
        if not cycles:
            continue

        df = pd.DataFrame(cycles)
        cycle_count = len(df)

        avg_green = round(df["green_dur"].dropna().mean(), 2) if df["green_dur"].notna().any() else 0.0
        avg_yellow = round(df["yellow_dur"].dropna().mean(), 2) if df["yellow_dur"].notna().any() else 0.0
        avg_total = round(df["total_split"].mean(), 2)

        avg_red_clearance = round(max(avg_total - avg_green - avg_yellow, 0.0), 2)

        term_counts = df["termination"].value_counts()
        force_off_count = int(term_counts.get("force_off", 0))
        gap_out_count = int(term_counts.get("gap_out", 0))
        max_out_count = int(term_counts.get("max_out", 0))

        results.append({
            "phase_number": phase_num,
            "cycles": cycle_count,
            "green_time": avg_green,
            "yellow_time": avg_yellow,
            "red_clearance_time": avg_red_clearance,
            "total_split": avg_total,
            "force_off_pct": pct(force_off_count, cycle_count),
            "gap_out_pct": pct(gap_out_count, cycle_count),
            "max_out_pct": pct(max_out_count, cycle_count),
        })

    return results
