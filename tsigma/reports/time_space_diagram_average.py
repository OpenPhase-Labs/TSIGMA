"""
Time-Space Diagram Average report plugin.

Cross-day averaged Time-Space Diagram.  Given a calendar range, a
daily time window, and a set of weekdays, this report:

    1. Enumerates matching calendar days.
    2. Validates that every selected day operated under an identical
       signal plan (cycle length, offset, splits).
    3. Aggregates phase green/yellow/red events across every selected
       day for each corridor signal's coordinated phase.
    4. Selects the MEDIAN cycle (the middle element after sorting by
       green duration) — intentionally NOT the mean — so the output is
       resilient to one-day anomalies.
    5. Synthesises a repeating green→yellow→red pattern that spans the
       daily window (plus a 2-minute tail).
    6. Emits one row per phase-interval boundary per signal per cycle,
       including each signal's distance from the corridor origin so a
       visualiser can project green arrivals downstream.

Weekday convention
------------------
This report uses the **Python** weekday convention where Monday=0 and
Sunday=6 (``datetime.weekday()``).  ATSPM 5.x's DayOfWeek enum uses
Sunday=0; callers porting from ATSPM must shift by ``(day_of_week + 6) % 7``.

Coordinated vs non-coordinated reference point
----------------------------------------------
The ATSPM 5.x source exposes two reference-point formulas.  Because
``direction_phase_map`` always names the *coordinated* phase per signal,
this plugin only needs the coordinated branch::

    ref_point = offset - (median_green + median_yellow - programmed_split)

The non-coordinated branch is documented in the upstream algorithm but
unreachable from this report's inputs.

Complementary single-window sibling: ``time_space_diagram.py``.  That
report does not deduplicate across days or synthesise a repeating
pattern; it emits raw observed intervals from one analysis window.
The two reports share no code because their data-flow shapes (raw
event intervals vs synthesised median cycles) diverge at the first
step.
"""

import logging
from datetime import date, datetime, time, timedelta

import pandas as pd
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from .registry import Report, ReportMetadata, ReportRegistry
from .sdk import (
    EVENT_PHASE_GREEN,
    EVENT_RED_CLEARANCE,
    EVENT_YELLOW_CLEARANCE,
    fetch_events,
    fetch_plans,
    plan_at,
    programmed_split,
)

logger = logging.getLogger(__name__)

_COLUMNS = [
    "signal_id",
    "phase_number",
    "cycle_index",
    "event",
    "event_time",
    "distance_ft",
    "cycle_length_seconds",
    "median_green_seconds",
    "median_yellow_seconds",
    "median_red_seconds",
    "programmed_split_seconds",
    "days_included",
    "speed_limit_applied",
]

_PHASE_CODES = (EVENT_PHASE_GREEN, EVENT_YELLOW_CLEARANCE, EVENT_RED_CLEARANCE)

# How long past the daily window end to keep synthesising cycles.
_TAIL_SECONDS = 120


class TimeSpaceDiagramAverageParams(BaseModel):
    signal_ids: list[str] = Field(
        ...,
        description="Ordered list of corridor signals, upstream to downstream.",
    )
    start_date: str = Field(..., description="Calendar range start (ISO date, YYYY-MM-DD).")
    end_date: str = Field(..., description="Calendar range end (ISO date, YYYY-MM-DD).")
    start_time: str = Field(..., description="Daily window start (HH:MM).")
    end_time: str = Field(..., description="Daily window end (HH:MM).")
    days_of_week: list[int] = Field(
        ...,
        description="Python weekday numbers (Monday=0..Sunday=6).",
    )
    direction_phase_map: dict[str, int] = Field(
        ...,
        description="signal_id -> coordinated phase number.",
    )
    distances: dict[str, float] | None = Field(
        default=None,
        description="signal_id -> distance from corridor origin in feet.",
    )
    speed_limit_mph: int = Field(
        default=30,
        description="Fallback speed limit (mph) when config does not provide one.",
    )


@ReportRegistry.register("time-space-diagram-average")
class TimeSpaceDiagramAverageReport(Report[TimeSpaceDiagramAverageParams]):
    """Multi-day median cycle synthesis for corridor time-space visualisation."""

    metadata = ReportMetadata(
        name="time-space-diagram-average",
        description=(
            "Cross-day averaged Time-Space Diagram using the median cycle "
            "observed across matched weekdays and peak periods."
        ),
        category="detailed",
        estimated_time="medium",
        export_formats=["csv", "json", "ndjson"],
    )

    async def execute(
        self,
        params: TimeSpaceDiagramAverageParams,
        session: AsyncSession,
    ) -> pd.DataFrame:
        """
        Execute the multi-day median time-space synthesis.

        Returns an empty DataFrame with the full schema if no matching
        weekdays fall in the range, if no cycles are observed, or if the
        daily window is empty.  Raises ``ValueError`` if the signal plan
        changes between selected days.
        """
        signal_ids = list(params.signal_ids)
        phase_map = params.direction_phase_map
        distances = params.distances or {}
        speed_limit = float(params.speed_limit_mph)

        days = _eligible_days(params)
        if not days:
            logger.info("time-space-diagram-average: no days match filter")
            return _empty_result()

        daily_start = _parse_hhmm(params.start_time)
        daily_end = _parse_hhmm(params.end_time)

        logger.info(
            "time-space-diagram-average: %d signals, %d days, %s..%s",
            len(signal_ids), len(days), daily_start, daily_end,
        )

        anchor_dt = datetime.combine(days[0], daily_start)
        await _validate_plans_across_days(session, signal_ids, days, anchor_dt)

        plan_summary = await _load_reference_plan(session, signal_ids[0], anchor_dt)

        cycle_stats = await _collect_cycles(
            signal_ids, phase_map, days, daily_start, daily_end,
        )
        if not cycle_stats.has_any():
            logger.info("time-space-diagram-average: no cycles observed")
            return _empty_result()

        rows = _synthesise_rows(
            signal_ids=signal_ids,
            phase_map=phase_map,
            distances=distances,
            speed_limit=speed_limit,
            days=days,
            daily_start=daily_start,
            daily_end=daily_end,
            plan_summary=plan_summary,
            cycle_stats=cycle_stats,
        )
        if not rows:
            return _empty_result()

        return pd.DataFrame(rows, columns=_COLUMNS)


# ---------------------------------------------------------------------------
# Helpers — day enumeration and plan validation
# ---------------------------------------------------------------------------


def _parse_hhmm(value: str) -> time:
    """Parse an ``HH:MM`` string into a ``time`` object."""
    hour_str, minute_str = value.split(":", 1)
    return time(int(hour_str), int(minute_str))


def _parse_iso_date(value: str) -> date:
    """Parse an ``YYYY-MM-DD`` string into a ``date`` object."""
    return date.fromisoformat(value)


def _eligible_days(params: TimeSpaceDiagramAverageParams) -> list[date]:
    """Dates in [start_date, end_date] whose Python weekday is in ``days_of_week``."""
    start = _parse_iso_date(params.start_date)
    end = _parse_iso_date(params.end_date)
    wanted = set(params.days_of_week)

    out: list[date] = []
    cursor = start
    while cursor <= end:
        if cursor.weekday() in wanted:
            out.append(cursor)
        cursor = cursor + timedelta(days=1)
    return out


async def _validate_plans_across_days(
    session: AsyncSession,
    signal_ids: list[str],
    days: list[date],
    anchor_dt: datetime,
) -> None:
    """
    Raise ValueError if the reference signal's plan is not identical on
    every selected day.
    """
    if len(days) < 2:
        return

    # Use the first signal as the plan reference — the corridor runs under
    # the same timing programme across signals by design.
    signal_id = signal_ids[0]
    plans = await fetch_plans(
        session,
        signal_id,
        datetime.combine(days[0], time.min),
        datetime.combine(days[-1], time.max),
    )
    if not plans:
        return

    ref_plan = plan_at(plans, anchor_dt)
    if ref_plan is None:
        return

    anchor_day = anchor_dt.date().isoformat()
    mismatches: list[str] = []
    for day in days:
        plan = plan_at(plans, datetime.combine(day, anchor_dt.time()))
        if plan is None:
            mismatches.append(day.isoformat())
            continue
        if not _plans_equivalent(ref_plan, plan):
            mismatches.append(day.isoformat())

    if mismatches:
        raise ValueError(
            f"Signal plan mismatch across selected days (anchor day: {anchor_day}): "
            + ", ".join(mismatches)
            + " (cycle_length, offset, or splits differ from the anchor day)"
        )


def _plans_equivalent(a, b) -> bool:
    """Two plans are equivalent when cycle_length, offset, and splits match."""
    if a.cycle_length != b.cycle_length:
        return False
    if a.offset != b.offset:
        return False
    if (a.splits or {}) != (b.splits or {}):
        return False
    return True


async def _load_reference_plan(
    session: AsyncSession,
    signal_id: str,
    anchor_dt: datetime,
) -> "_PlanSummary":
    """Extract the cycle length / offset / programmed-split summary at anchor_dt."""
    plans = await fetch_plans(
        session,
        signal_id,
        anchor_dt - timedelta(days=30),
        anchor_dt + timedelta(days=1),
    )
    plan = plan_at(plans, anchor_dt)
    if plan is None:
        return _PlanSummary(cycle_length=0, offset=0, splits={})
    return _PlanSummary(
        cycle_length=int(plan.cycle_length or 0),
        offset=int(plan.offset or 0),
        splits=dict(plan.splits or {}),
    )


class _PlanSummary:
    __slots__ = ("cycle_length", "offset", "splits")

    def __init__(self, cycle_length: int, offset: int, splits: dict) -> None:
        self.cycle_length = cycle_length
        self.offset = offset
        self.splits = splits

    def split_for(self, phase: int) -> float:
        """Programmed split seconds for ``phase``, 0.0 if not recorded."""
        value = self.splits.get(str(phase))
        return float(value) if value is not None else 0.0


# ---------------------------------------------------------------------------
# Helpers — cycle collection and median selection
# ---------------------------------------------------------------------------


class _CycleStats:
    """Median cycle timings per (signal_id, phase_number) pair."""

    def __init__(self) -> None:
        self._by_signal: dict[str, _SignalMedian] = {}

    def record(self, signal_id: str, median: "_SignalMedian") -> None:
        self._by_signal[signal_id] = median

    def get(self, signal_id: str) -> "_SignalMedian | None":
        return self._by_signal.get(signal_id)

    def has_any(self) -> bool:
        return any(m.cycle_count > 0 for m in self._by_signal.values())


class _SignalMedian:
    __slots__ = ("green", "yellow", "red", "cycle_count", "days_contributing")

    def __init__(
        self,
        green: float,
        yellow: float,
        red: float,
        cycle_count: int,
        days_contributing: int,
    ) -> None:
        self.green = green
        self.yellow = yellow
        self.red = red
        self.cycle_count = cycle_count
        self.days_contributing = days_contributing


async def _collect_cycles(
    signal_ids: list[str],
    phase_map: dict[str, int],
    days: list[date],
    daily_start: time,
    daily_end: time,
) -> _CycleStats:
    """Fetch events for every signal/day and compute the per-signal median cycle."""
    stats = _CycleStats()

    for signal_id in signal_ids:
        phase = phase_map.get(signal_id)
        if phase is None:
            continue
        cycles, days_hit = await _gather_signal_cycles(
            signal_id, phase, days, daily_start, daily_end,
        )
        if not cycles:
            continue
        stats.record(signal_id, _median_from_cycles(cycles, days_hit))

    return stats


async def _gather_signal_cycles(
    signal_id: str,
    phase: int,
    days: list[date],
    daily_start: time,
    daily_end: time,
) -> tuple[list[tuple[float, float, float]], int]:
    """Return (list of (green,yellow,red) seconds, distinct days hit)."""
    all_cycles: list[tuple[float, float, float]] = []
    days_hit = 0

    for day in days:
        win_start = datetime.combine(day, daily_start)
        win_end = datetime.combine(day, daily_end)
        events_df = await fetch_events(
            signal_id, win_start, win_end,
            list(_PHASE_CODES),
            event_param_in=[phase],
        )
        day_cycles = _parse_cycles(events_df)
        if day_cycles:
            days_hit += 1
            all_cycles.extend(day_cycles)

    return all_cycles, days_hit


def _parse_cycles(df: pd.DataFrame) -> list[tuple[float, float, float]]:
    """
    Extract green→yellow→red→next-green cycles from an events DataFrame.

    Returns a list of (green_seconds, yellow_seconds, red_seconds) tuples.
    Rows are assumed ordered by event_time.  Partial cycles at the window
    boundary are skipped.
    """
    if df.empty:
        return []

    greens = df.loc[df["event_code"] == EVENT_PHASE_GREEN, "event_time"].tolist()
    yellows = df.loc[df["event_code"] == EVENT_YELLOW_CLEARANCE, "event_time"].tolist()
    reds = df.loc[df["event_code"] == EVENT_RED_CLEARANCE, "event_time"].tolist()

    if len(greens) < 2:
        return []

    cycles: list[tuple[float, float, float]] = []
    for i in range(len(greens) - 1):
        cycle = _assemble_cycle(greens[i], greens[i + 1], yellows, reds)
        if cycle is not None:
            cycles.append(cycle)
    return cycles


def _assemble_cycle(
    green_start,
    next_green_start,
    yellows: list,
    reds: list,
) -> tuple[float, float, float] | None:
    """Find the yellow and red that fall between two green starts; compute durations."""
    yellow = _first_between(yellows, green_start, next_green_start)
    red = _first_between(reds, green_start, next_green_start)
    if yellow is None or red is None:
        return None
    green_s = (yellow - green_start).total_seconds()
    yellow_s = (red - yellow).total_seconds()
    red_s = (next_green_start - red).total_seconds()
    if green_s <= 0 or yellow_s <= 0 or red_s <= 0:
        return None
    return green_s, yellow_s, red_s


def _first_between(values: list, lo, hi):
    """Return the first value strictly after lo and strictly before hi, else None."""
    for v in values:
        if v > lo and v < hi:
            return v
    return None


def _median_from_cycles(
    cycles: list[tuple[float, float, float]],
    days_contributing: int,
) -> _SignalMedian:
    """Pick the middle cycle after sorting by green duration."""
    sorted_cycles = sorted(cycles, key=lambda c: c[0])
    mid_index = len(sorted_cycles) // 2
    green, yellow, red = sorted_cycles[mid_index]
    return _SignalMedian(
        green=green,
        yellow=yellow,
        red=red,
        cycle_count=len(sorted_cycles),
        days_contributing=days_contributing,
    )


# ---------------------------------------------------------------------------
# Helpers — synthesis
# ---------------------------------------------------------------------------


def _synthesise_rows(
    *,
    signal_ids: list[str],
    phase_map: dict[str, int],
    distances: dict[str, float],
    speed_limit: float,
    days: list[date],
    daily_start: time,
    daily_end: time,
    plan_summary: _PlanSummary,
    cycle_stats: _CycleStats,
) -> list[dict]:
    """Build the output row list from the per-signal median cycles."""
    anchor = datetime.combine(days[0], daily_start)
    window_end = datetime.combine(days[0], daily_end) + timedelta(seconds=_TAIL_SECONDS)
    cycle_length = float(plan_summary.cycle_length) or 0.0

    rows: list[dict] = []
    for signal_id in signal_ids:
        phase = phase_map.get(signal_id)
        if phase is None:
            continue
        median = cycle_stats.get(signal_id)
        if median is None:
            continue

        programmed = plan_summary.split_for(phase)
        ref_offset = _coordinated_ref_offset(
            offset=float(plan_summary.offset),
            green=median.green,
            yellow=median.yellow,
            programmed=programmed,
        )
        cycle_duration = max(
            cycle_length,
            median.green + median.yellow + median.red,
        )
        rows.extend(_synthesise_signal_rows(
            signal_id=signal_id,
            phase=phase,
            median=median,
            programmed=programmed,
            cycle_duration=cycle_duration,
            ref_offset=ref_offset,
            anchor=anchor,
            window_end=window_end,
            distance=distances.get(signal_id),
            speed_limit=speed_limit,
            days_included=median.days_contributing,
        ))
    return rows


def _coordinated_ref_offset(
    *, offset: float, green: float, yellow: float, programmed: float,
) -> float:
    """
    Coordinated-phase reference offset.

    Per ATSPM 5.x::

        ref_point = offset - (median_green + median_yellow - programmed_split)
    """
    return offset - (green + yellow - programmed)


def _synthesise_signal_rows(
    *,
    signal_id: str,
    phase: int,
    median: _SignalMedian,
    programmed: float,
    cycle_duration: float,
    ref_offset: float,
    anchor: datetime,
    window_end: datetime,
    distance: float | None,
    speed_limit: float,
    days_included: int,
) -> list[dict]:
    """Emit green/yellow/red rows for every synthesised cycle of one signal."""
    if cycle_duration <= 0:
        return []

    first_green = anchor + timedelta(seconds=ref_offset)
    # Step back until we start at or before the window anchor.
    while first_green > anchor:
        first_green = first_green - timedelta(seconds=cycle_duration)

    broadcast = {
        "cycle_length_seconds": round(cycle_duration, 3),
        "median_green_seconds": round(median.green, 3),
        "median_yellow_seconds": round(median.yellow, 3),
        "median_red_seconds": round(median.red, 3),
        "programmed_split_seconds": round(programmed, 3),
        "days_included": int(days_included),
        "speed_limit_applied": float(speed_limit),
    }

    rows: list[dict] = []
    cycle_index = 0
    green_start = first_green
    while green_start <= window_end:
        yellow_start = green_start + timedelta(seconds=median.green)
        red_start = yellow_start + timedelta(seconds=median.yellow)
        rows.append(_event_row(
            signal_id, phase, cycle_index, "green", green_start, distance, broadcast,
        ))
        rows.append(_event_row(
            signal_id, phase, cycle_index, "yellow", yellow_start, distance, broadcast,
        ))
        rows.append(_event_row(
            signal_id, phase, cycle_index, "red", red_start, distance, broadcast,
        ))
        cycle_index += 1
        green_start = green_start + timedelta(seconds=cycle_duration)
    return rows


def _event_row(
    signal_id: str,
    phase: int,
    cycle_index: int,
    event: str,
    event_time: datetime,
    distance: float | None,
    broadcast: dict,
) -> dict:
    """Assemble one output row with the broadcast summary columns attached."""
    row = {
        "signal_id": signal_id,
        "phase_number": int(phase),
        "cycle_index": int(cycle_index),
        "event": event,
        "event_time": event_time.isoformat(),
        "distance_ft": float(distance) if distance is not None else None,
    }
    row.update(broadcast)
    return row


def _empty_result() -> pd.DataFrame:
    """Empty DataFrame carrying the full output schema."""
    return pd.DataFrame(columns=_COLUMNS)


# ``programmed_split`` is re-exported to keep the optional non-coordinated
# reference-offset formula documented in docstrings reachable from the
# plugin module without an implicit-cycle import path.
__all__ = [
    "TimeSpaceDiagramAverageParams",
    "TimeSpaceDiagramAverageReport",
    "programmed_split",
]
