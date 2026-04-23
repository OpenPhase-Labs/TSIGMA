"""
Left Turn Gap report plugin.

Comprehensive analysis of permissive left turn feasibility by measuring
inter-vehicle gaps on the opposing through movement during green. Includes:
- Gap classification (sufficient/marginal/insufficient)
- Pedestrian actuation correlation (ped calls during through green)
- Data quality checks (minimum detector hits, data completeness)
- Split failure metrics for the left turn phase (inline, no second report)

Covers the functionality of all 7 ATSPM 5.x left-turn-gap variants in
a single unified report.
"""

import logging
from datetime import datetime
from typing import Any

import pandas as pd
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from ..config_resolver import SignalConfig, get_config_at
from .registry import Report, ReportMetadata, ReportRegistry
from .sdk import (
    EVENT_DETECTOR_OFF,
    EVENT_DETECTOR_ON,
    EVENT_FORCE_OFF,
    EVENT_GAP_OUT,
    EVENT_MAX_OUT,
    EVENT_PED_CALL,
    EVENT_PHASE_END,
    EVENT_PHASE_GREEN,
    EVENT_RED_CLEARANCE,
    EVENT_YELLOW_CLEARANCE,
    calculate_occupancy,
    fetch_events_split,
    parse_time,
)

logger = logging.getLogger(__name__)

_DET_EVENTS = frozenset({EVENT_DETECTOR_ON, EVENT_DETECTOR_OFF})

# HCM critical gap thresholds by opposing through lane count (seconds).
# Matches ATSPM 4.x/5.x: GetCriticalGap(numberOfOpposingLanes).
# ≤2 opposing lanes → 4.1s, 3+ opposing lanes → 5.3s.
CRITICAL_GAP_LOW = 4.1   # 1-2 opposing through lanes
CRITICAL_GAP_HIGH = 5.3  # 3+ opposing through lanes
CRITICAL_GAP_LANE_THRESHOLD = 3  # At this many lanes, switch to high

# Marginal gap is defined as a fixed offset below the critical gap.
# Gaps between (critical - MARGINAL_OFFSET) and critical are "marginal".
MARGINAL_OFFSET = 1.5

# Data quality thresholds
MIN_DETECTOR_HITS_PER_HOUR = 10
MIN_CYCLES_FOR_VALID_ANALYSIS = 5

# Split failure occupancy measurement
OCCUPANCY_WINDOW_SECONDS = 5.0

# Phase-level event codes for the main query (includes PED_CALL since
# its event_param is a phase number, not a detector channel).
_PHASE_CODES = (
    EVENT_PHASE_GREEN, EVENT_YELLOW_CLEARANCE,
    EVENT_RED_CLEARANCE, EVENT_PHASE_END,
    EVENT_GAP_OUT, EVENT_MAX_OUT, EVENT_FORCE_OFF,
    EVENT_PED_CALL,
)

# Per-cycle columns produced by _analyse_cycles. Summary columns
# (dq_*, ped_*, peak_*, lt_*) are appended by _attach_summary_columns.
_PER_CYCLE_COLUMNS = [
    "cycle_start", "green_duration", "total_gaps",
    "sufficient_gaps", "marginal_gaps", "insufficient_gaps",
    "avg_gap_duration", "max_gap_duration", "ped_calls_in_cycle",
]

# Peak-hour summary columns (broadcast across all rows). Populated with
# None values when no cycles exist.
_PEAK_COLUMNS = [
    "peak_hour", "peak_cycles", "peak_total_gaps",
    "peak_sufficient_gaps", "peak_marginal_gaps",
    "peak_insufficient_gaps", "peak_sufficient_pct",
]

# Left-turn split-failure summary columns. Populated with None values
# when left_turn_phase is not provided.
_LT_COLUMNS = [
    "lt_phase", "lt_split_total_cycles",
    "lt_split_failures", "lt_split_failure_pct",
]


class LeftTurnGapParams(BaseModel):
    signal_id: str = Field(..., description="Signal identifier")
    start: str = Field(..., description="Analysis window start (ISO-8601)")
    end: str = Field(..., description="Analysis window end (ISO-8601)")
    phase_number: int = Field(..., description="Opposing through phase number")
    opposing_lanes: int | None = Field(default=None, description="Manual override for opposing through lane count")
    left_turn_phase: int | None = Field(default=None, description="Left turn phase for split failure analysis")
    green_occ_threshold: float = Field(default=0.79, description="Green occupancy threshold for split failure")
    red_occ_threshold: float = Field(default=0.79, description="Red occupancy threshold for split failure")


@ReportRegistry.register("left-turn-gap")
class LeftTurnGapReport(Report[LeftTurnGapParams]):
    """Comprehensive left turn gap analysis with ped actuation, data checks, and split failure."""

    metadata = ReportMetadata(
        name="left-turn-gap",
        description=(
            "Gap analysis on through movement for permissive left turn evaluation. "
            "Includes ped actuation, data quality checks, and left turn split failure."
        ),
        category="detailed",
        estimated_time="medium",
        export_formats=["csv", "json", "ndjson"],
    )

    async def execute(self, params: LeftTurnGapParams, session: AsyncSession) -> pd.DataFrame:
        """
        Execute comprehensive left turn gap analysis.

        Returns:
            DataFrame of per-cycle gap classification results.

            Per-cycle columns:
                cycle_start, green_duration, total_gaps, sufficient_gaps,
                marginal_gaps, insufficient_gaps, avg_gap_duration,
                max_gap_duration, ped_calls_in_cycle.

            Summary columns (broadcast identically across every row — ATSPM 4.x/5.x
            treat these as per-window summaries, not per-cycle):
                dq_*                  — data quality gate (analysis_period_hours,
                                        total_detector_hits, detector_hits_per_hour,
                                        total_cycles, sufficient_detector_data,
                                        sufficient_cycles, is_valid).
                ped_total_calls, ped_calls_during_green, ped_cycles_with_ped,
                ped_pct_cycles_with_ped
                                      — pedestrian actuation summary.
                peak_*                — peak-hour summary (hour, cycles, total_gaps,
                                        sufficient_gaps, marginal_gaps,
                                        insufficient_gaps, sufficient_pct). All
                                        ``None`` when no cycles exist.
                lt_phase, lt_split_total_cycles, lt_split_failures,
                lt_split_failure_pct
                                      — left-turn split-failure summary. All ``None``
                                        when ``left_turn_phase`` is not provided or
                                        when the left-turn phase has no detectors.
        """
        signal_id = params.signal_id
        start = parse_time(params.start)
        end = parse_time(params.end)
        phase_number = params.phase_number
        opposing_lanes_override = params.opposing_lanes
        left_turn_phase = params.left_turn_phase
        green_occ_threshold = params.green_occ_threshold
        red_occ_threshold = params.red_occ_threshold

        logger.info(
            "Running left-turn-gap for %s phase %d from %s to %s",
            signal_id, phase_number, start, end,
        )

        config = await get_config_at(session, signal_id, as_of=start)
        det_channels = config.detector_channels_for_phase(phase_number)

        # Derive opposing lane count from detector config (ATSPM pattern:
        # count of detectors assigned to the opposing through phase).
        # Manual override via opposing_lanes param takes precedence.
        if opposing_lanes_override is not None:
            opposing_lanes = int(opposing_lanes_override)
        else:
            opposing_lanes = len(det_channels)
        gap_sufficient = _critical_gap_for_lanes(opposing_lanes)
        gap_marginal = gap_sufficient - MARGINAL_OFFSET

        if not det_channels:
            empty = pd.DataFrame(columns=_PER_CYCLE_COLUMNS)
            _attach_summary_columns(
                empty,
                data_quality=_empty_data_quality(start, end),
                ped_summary=_build_ped_summary(0, 0, 0, 0),
                peak=None,
                split_fail=None,
            )
            return empty

        lt_det_channels = _get_lt_det_channels(config, left_turn_phase)
        ped_phase = _find_ped_phase(config, phase_number)
        all_det_channels = list(det_channels | lt_det_channels)

        df = await fetch_events_split(
            signal_id, start, end,
            phase_codes=list(_PHASE_CODES),
            det_channels=all_det_channels,
            det_codes=(EVENT_DETECTOR_ON, EVENT_DETECTOR_OFF),
        )

        cycles, total_ped_calls, total_det_hits = _build_cycles(
            df, phase_number, det_channels, lt_det_channels, ped_phase,
        )
        result_df, ped_calls_during_green, cycles_with_ped = _analyse_cycles(
            cycles, gap_sufficient, gap_marginal,
        )

        total_cycles = len(cycles)
        data_quality = (
            _build_data_quality(start, end, total_det_hits, total_cycles)
            if total_cycles > 0
            else _empty_data_quality(start, end)
        )
        ped_summary = _build_ped_summary(
            total_ped_calls, ped_calls_during_green, cycles_with_ped, total_cycles,
        )
        peak = _find_peak_hour(result_df.to_dict("records"))

        split_fail: dict[str, Any] | None = None
        if left_turn_phase is not None and lt_det_channels:
            split_fail = _left_turn_split_failure(
                df, left_turn_phase, lt_det_channels,
                green_occ_threshold, red_occ_threshold,
            )

        _attach_summary_columns(
            result_df,
            data_quality=data_quality,
            ped_summary=ped_summary,
            peak=peak,
            split_fail=split_fail,
        )

        if split_fail is not None:
            logger.info(
                "Left-turn split-failure: phase %d, %d/%d cycles (%.1f%%) "
                "[green_occ>=%.2f, red_occ>=%.2f]",
                left_turn_phase, split_fail["split_failures"],
                split_fail["total_cycles"], split_fail["split_failure_pct"],
                green_occ_threshold, red_occ_threshold,
            )
        logger.info(
            "Left-turn-gap complete: %d cycles, critical_gap=%.1fs (%d opposing lanes), "
            "dq_valid=%s, peak_hour=%s",
            len(result_df), gap_sufficient, opposing_lanes,
            data_quality["is_valid"], peak["hour"] if peak else None,
        )
        return result_df


# ---------------------------------------------------------------------------
# HCM threshold helpers
# ---------------------------------------------------------------------------

def _critical_gap_for_lanes(opposing_lanes: int) -> float:
    """
    Return HCM critical gap based on opposing through lane count.

    Matches ATSPM 4.x/5.x GetCriticalGap():
        ≤2 lanes → 4.1s
        3+ lanes → 5.3s
    """
    if opposing_lanes >= CRITICAL_GAP_LANE_THRESHOLD:
        return CRITICAL_GAP_HIGH
    return CRITICAL_GAP_LOW


def _get_lt_det_channels(config: SignalConfig, left_turn_phase: int | None) -> set[int]:
    """Get left turn detector channels if left_turn_phase provided."""
    if left_turn_phase is not None:
        return config.detector_channels_for_phase(left_turn_phase)
    return set()


def _find_ped_phase(config: SignalConfig, through_phase: int) -> int | None:
    """Find pedestrian phase number for the through phase approach."""
    for approach in config.approaches:
        if approach.protected_phase_number == through_phase:
            ped = config.ped_phase_for_approach(approach.approach_id)
            if ped is not None:
                return ped
    return None


# ---------------------------------------------------------------------------
# Cycle building (through phase)
# ---------------------------------------------------------------------------

def _flush_cycle(
    cycles: list[dict[str, Any]],
    green_start: datetime | None,
    green_end: datetime | None,
    det_events: list[tuple[datetime, int]],
    ped_calls: list[datetime],
    lt_det_events: list[tuple[datetime, int]],
) -> None:
    """Append a completed cycle if both green_start and green_end are set."""
    if green_start is not None and green_end is not None:
        cycles.append({
            "green_start": green_start,
            "green_end": green_end,
            "det_events": det_events,
            "ped_calls": ped_calls,
            "lt_det_events": lt_det_events,
        })


def _handle_det_event(
    code: int,
    param: int,
    event_time: datetime,
    det_channels: set[int],
    lt_det_channels: set[int],
    cycle_det_events: list[tuple[datetime, int]],
    cycle_lt_det_events: list[tuple[datetime, int]],
) -> int:
    """
    Route a detector on/off event to the right accumulator.

    Returns 1 if this was a through-detector ON (for hit counting), else 0.
    """
    if param in det_channels:
        cycle_det_events.append((event_time, code))
        return int(code == EVENT_DETECTOR_ON)
    if param in lt_det_channels:
        cycle_lt_det_events.append((event_time, code))
    return 0


def _build_cycles(
    df: pd.DataFrame,
    phase_number: int,
    det_channels: set[int],
    lt_det_channels: set[int],
    ped_phase: int | None,
) -> tuple[list[dict[str, Any]], int, int]:
    """
    Walk events and build green-interval cycle dicts.

    Returns (cycles, total_ped_calls, total_det_hits).
    """
    cycles: list[dict[str, Any]] = []
    green_start: datetime | None = None
    green_end: datetime | None = None
    cycle_det_events: list[tuple[datetime, int]] = []
    cycle_ped_calls: list[datetime] = []
    cycle_lt_det_events: list[tuple[datetime, int]] = []
    total_ped_calls = 0
    total_det_hits = 0

    for _, row in df.iterrows():
        code = int(row["event_code"])
        param = int(row["event_param"])
        event_time = row["event_time"]
        in_green = green_start is not None

        if code == EVENT_PHASE_GREEN and param == phase_number:
            _flush_cycle(cycles, green_start, green_end,
                         cycle_det_events, cycle_ped_calls, cycle_lt_det_events)
            green_start = event_time
            green_end = None
            cycle_det_events = []
            cycle_ped_calls = []
            cycle_lt_det_events = []

        elif code == EVENT_YELLOW_CLEARANCE and param == phase_number:
            green_end = event_time

        elif code == EVENT_PED_CALL and param == ped_phase:
            total_ped_calls += 1
            if in_green:
                cycle_ped_calls.append(event_time)

        elif in_green and code in _DET_EVENTS:
            total_det_hits += _handle_det_event(
                code, param, event_time,
                det_channels, lt_det_channels,
                cycle_det_events, cycle_lt_det_events,
            )

    _flush_cycle(cycles, green_start, green_end,
                 cycle_det_events, cycle_ped_calls, cycle_lt_det_events)
    return cycles, total_ped_calls, total_det_hits


# ---------------------------------------------------------------------------
# Gap classification per cycle
# ---------------------------------------------------------------------------

def _analyse_cycles(
    cycles: list[dict[str, Any]],
    gap_sufficient: float,
    gap_marginal: float,
) -> tuple[pd.DataFrame, int, int]:
    """
    Run gap classification and ped correlation on each cycle.

    Uses pandas for vectorised gap classification and aggregation.

    Returns (result_df, ped_calls_during_green, cycles_with_ped).
    """
    empty_df = pd.DataFrame(columns=[
        "cycle_start", "green_duration", "total_gaps",
        "sufficient_gaps", "marginal_gaps", "insufficient_gaps",
        "avg_gap_duration", "max_gap_duration", "ped_calls_in_cycle",
    ])

    if not cycles:
        return empty_df, 0, 0

    # Build per-cycle rows with measured gaps and ped counts
    rows = []
    for cycle in cycles:
        gs = cycle["green_start"]
        ge = cycle["green_end"]
        gaps = _measure_gaps(cycle["det_events"], gs, ge)
        ped_in_green = [p for p in cycle["ped_calls"] if gs <= p <= ge]
        rows.append({
            "cycle_start": gs,
            "green_duration": (ge - gs).total_seconds(),
            "gaps": gaps,
            "ped_calls_in_cycle": len(ped_in_green),
        })

    df = pd.DataFrame(rows)

    # Vectorised gap classification using Series of lists
    gap_series = df["gaps"]
    df["total_gaps"] = gap_series.apply(len)
    df["sufficient_gaps"] = gap_series.apply(
        lambda g: sum(1 for x in g if x >= gap_sufficient)
    )
    df["marginal_gaps"] = gap_series.apply(
        lambda g: sum(1 for x in g if gap_marginal <= x < gap_sufficient)
    )
    df["insufficient_gaps"] = gap_series.apply(
        lambda g: sum(1 for x in g if x < gap_marginal)
    )
    df["avg_gap_duration"] = gap_series.apply(
        lambda g: round(sum(g) / len(g), 2) if g else 0.0
    )
    df["max_gap_duration"] = gap_series.apply(
        lambda g: round(max(g), 2) if g else 0.0
    )
    df["green_duration"] = df["green_duration"].round(2)
    df["cycle_start"] = df["cycle_start"].apply(lambda t: t.isoformat())

    ped_calls_during_green = int(df["ped_calls_in_cycle"].sum())
    cycles_with_ped = int((df["ped_calls_in_cycle"] > 0).sum())

    result_cols = [
        "cycle_start", "green_duration", "total_gaps",
        "sufficient_gaps", "marginal_gaps", "insufficient_gaps",
        "avg_gap_duration", "max_gap_duration", "ped_calls_in_cycle",
    ]
    result_df = df[result_cols].copy()

    return result_df, ped_calls_during_green, cycles_with_ped


# ---------------------------------------------------------------------------
# Result builders (kept for internal use but no longer returned directly)
# ---------------------------------------------------------------------------

def _build_data_quality(
    start: datetime,
    end: datetime,
    total_det_hits: int,
    total_cycles: int,
) -> dict[str, Any]:
    """Assemble data quality dict."""
    analysis_hours = max((end - start).total_seconds() / 3600, 0.001)
    det_hits_per_hour = total_det_hits / analysis_hours
    return {
        "analysis_period_hours": round(analysis_hours, 2),
        "total_detector_hits": total_det_hits,
        "detector_hits_per_hour": round(det_hits_per_hour, 1),
        "total_cycles": total_cycles,
        "sufficient_detector_data": det_hits_per_hour >= MIN_DETECTOR_HITS_PER_HOUR,
        "sufficient_cycles": total_cycles >= MIN_CYCLES_FOR_VALID_ANALYSIS,
        "is_valid": (
            det_hits_per_hour >= MIN_DETECTOR_HITS_PER_HOUR
            and total_cycles >= MIN_CYCLES_FOR_VALID_ANALYSIS
        ),
    }


def _build_ped_summary(
    total_ped_calls: int,
    ped_calls_during_green: int,
    cycles_with_ped: int,
    total_cycles: int,
) -> dict[str, Any]:
    """Assemble pedestrian actuation summary."""
    return {
        "total_ped_calls": total_ped_calls,
        "ped_calls_during_green": ped_calls_during_green,
        "cycles_with_ped": cycles_with_ped,
        "pct_cycles_with_ped": round(
            cycles_with_ped / total_cycles * 100, 1
        ) if total_cycles > 0 else 0.0,
    }


def _empty_data_quality(start: datetime, end: datetime) -> dict[str, Any]:
    """Return an empty data quality result."""
    hours = max((end - start).total_seconds() / 3600, 0.001)
    return {
        "analysis_period_hours": round(hours, 2),
        "total_detector_hits": 0,
        "detector_hits_per_hour": 0.0,
        "total_cycles": 0,
        "sufficient_detector_data": False,
        "sufficient_cycles": False,
        "is_valid": False,
    }


def _attach_summary_columns(
    result_df: pd.DataFrame,
    *,
    data_quality: dict[str, Any],
    ped_summary: dict[str, Any],
    peak: dict[str, Any] | None,
    split_fail: dict[str, Any] | None,
) -> None:
    """
    Broadcast summary dicts onto every row of `result_df`.

    ATSPM 4.x/5.x produce these as per-window summaries rather than per-cycle
    values. We flatten them into broadcast columns so a single DataFrame carries
    both shapes through the framework's CSV/JSON serializer.
    """
    for key, value in data_quality.items():
        result_df[f"dq_{key}"] = value

    result_df["ped_total_calls"] = ped_summary["total_ped_calls"]
    result_df["ped_calls_during_green"] = ped_summary["ped_calls_during_green"]
    result_df["ped_cycles_with_ped"] = ped_summary["cycles_with_ped"]
    result_df["ped_pct_cycles_with_ped"] = ped_summary["pct_cycles_with_ped"]

    if peak is not None:
        result_df["peak_hour"] = peak["hour"]
        result_df["peak_cycles"] = peak["cycles"]
        result_df["peak_total_gaps"] = peak["total_gaps"]
        result_df["peak_sufficient_gaps"] = peak["sufficient_gaps"]
        result_df["peak_marginal_gaps"] = peak["marginal_gaps"]
        result_df["peak_insufficient_gaps"] = peak["insufficient_gaps"]
        result_df["peak_sufficient_pct"] = peak["sufficient_pct"]
    else:
        for col in _PEAK_COLUMNS:
            result_df[col] = None

    if split_fail is not None:
        result_df["lt_phase"] = split_fail["left_turn_phase"]
        result_df["lt_split_total_cycles"] = split_fail["total_cycles"]
        result_df["lt_split_failures"] = split_fail["split_failures"]
        result_df["lt_split_failure_pct"] = split_fail["split_failure_pct"]
    else:
        for col in _LT_COLUMNS:
            result_df[col] = None


# ---------------------------------------------------------------------------
# Split failure analysis
# ---------------------------------------------------------------------------

def _left_turn_split_failure(
    df: pd.DataFrame,
    lt_phase: int,
    lt_det_channels: set[int],
    green_occ_threshold: float,
    red_occ_threshold: float,
) -> dict[str, Any]:
    """
    Compute split failure metrics for the left turn phase.

    Filters the already-fetched events in-memory rather than issuing
    a separate DB query. Uses the same algorithm as the standalone
    split-failure report but returns a summary rather than per-cycle data.
    """
    lt_cycles = _build_lt_cycles(df, lt_phase, lt_det_channels)
    failure_count = _count_split_failures(lt_cycles, green_occ_threshold, red_occ_threshold)

    total_cycles = len(lt_cycles)
    return {
        "left_turn_phase": lt_phase,
        "total_cycles": total_cycles,
        "split_failures": failure_count,
        "split_failure_pct": round(
            failure_count / total_cycles * 100, 1
        ) if total_cycles > 0 else 0.0,
        "green_occ_threshold": green_occ_threshold,
        "red_occ_threshold": red_occ_threshold,
    }


def _flush_lt_cycle(
    lt_cycles: list[dict[str, Any]],
    green_start: datetime | None,
    yellow_start: datetime | None,
    red_start: datetime | None,
    next_green: datetime,
    det_events: list[tuple[datetime, int]],
) -> None:
    """Append a completed left-turn cycle if green and yellow are set."""
    if green_start is not None and yellow_start is not None:
        lt_cycles.append({
            "green_start": green_start,
            "yellow_start": yellow_start,
            "red_start": red_start,
            "next_green": next_green,
            "det_events": det_events,
        })


def _build_lt_cycles(
    df: pd.DataFrame,
    lt_phase: int,
    lt_det_channels: set[int],
) -> list[dict[str, Any]]:
    """Build left turn phase cycles from events."""
    lt_cycles: list[dict[str, Any]] = []
    green_start: datetime | None = None
    yellow_start: datetime | None = None
    red_start: datetime | None = None
    det_events: list[tuple[datetime, int]] = []

    for _, row in df.iterrows():
        code = int(row["event_code"])
        param = int(row["event_param"])
        event_time = row["event_time"]

        if code == EVENT_PHASE_GREEN and param == lt_phase:
            _flush_lt_cycle(lt_cycles, green_start, yellow_start,
                            red_start, event_time, det_events)
            green_start = event_time
            yellow_start = None
            red_start = None
            det_events = []

        elif code == EVENT_YELLOW_CLEARANCE and param == lt_phase:
            yellow_start = event_time

        elif code == EVENT_RED_CLEARANCE and param == lt_phase:
            red_start = event_time

        elif param in lt_det_channels and code in _DET_EVENTS:
            if green_start is not None:
                det_events.append((event_time, code))

    return lt_cycles


def _count_split_failures(
    lt_cycles: list[dict[str, Any]],
    green_occ_threshold: float,
    red_occ_threshold: float,
) -> int:
    """Count cycles that meet split failure criteria."""
    failure_count = 0
    for cycle in lt_cycles:
        red_window = cycle["red_start"] if cycle["red_start"] else cycle["yellow_start"]
        green_occ = calculate_occupancy(
            cycle["det_events"], cycle["green_start"], OCCUPANCY_WINDOW_SECONDS,
        )
        red_occ = calculate_occupancy(
            cycle["det_events"], red_window, OCCUPANCY_WINDOW_SECONDS,
        )
        if green_occ >= green_occ_threshold and red_occ >= red_occ_threshold:
            failure_count += 1
    return failure_count


# ---------------------------------------------------------------------------
# Peak hour
# ---------------------------------------------------------------------------

def _find_peak_hour(cycles: list[dict[str, Any]]) -> dict[str, Any] | None:
    """Find the peak hour based on through volume (total gaps as proxy for vehicle count)."""
    if not cycles:
        return None

    df = pd.DataFrame(cycles)
    df["hour"] = df["cycle_start"].str[:13]  # "YYYY-MM-DDTHH"

    hourly = df.groupby("hour").agg(
        cycles=("hour", "size"),
        total_gaps=("total_gaps", "sum"),
        sufficient_gaps=("sufficient_gaps", "sum"),
        marginal_gaps=("marginal_gaps", "sum"),
        insufficient_gaps=("insufficient_gaps", "sum"),
    )

    if hourly.empty:
        return None

    peak = hourly.loc[hourly["total_gaps"].idxmax()]
    total_gaps = int(peak["total_gaps"])

    return {
        "hour": peak.name,
        "cycles": int(peak["cycles"]),
        "total_gaps": total_gaps,
        "sufficient_gaps": int(peak["sufficient_gaps"]),
        "marginal_gaps": int(peak["marginal_gaps"]),
        "insufficient_gaps": int(peak["insufficient_gaps"]),
        "sufficient_pct": round(int(peak["sufficient_gaps"]) / total_gaps * 100, 1) if total_gaps > 0 else 0.0,
    }


# ---------------------------------------------------------------------------
# Gap measurement
# ---------------------------------------------------------------------------

def _record_gap(
    gaps: list[float],
    last_off_time: datetime | None,
    detectors_on: int,
    event_time: datetime,
) -> tuple[datetime | None, int]:
    """
    Handle a detector-ON event: record gap if transitioning from all-off,
    then increment detector count.

    Returns updated (last_off_time, detectors_on).
    """
    if last_off_time is not None and detectors_on == 0:
        gap_duration = (event_time - last_off_time).total_seconds()
        if gap_duration > 0:
            gaps.append(gap_duration)
        last_off_time = None
    return last_off_time, detectors_on + 1


def _measure_gaps(
    det_events: list[tuple[datetime, int]],
    green_start: datetime,
    green_end: datetime,
) -> list[float]:
    """
    Measure gaps between successive detector-off and detector-on events
    during the green interval.

    A gap is the time when no vehicle is present on the through detectors
    (between detector-off and the next detector-on).
    """
    gaps: list[float] = []
    last_off_time: datetime | None = None
    detectors_on = 0

    for event_time, event_code in det_events:
        if event_time < green_start or event_time > green_end:
            continue

        if event_code == EVENT_DETECTOR_OFF:
            detectors_on = max(detectors_on - 1, 0)
            if detectors_on == 0:
                last_off_time = event_time

        elif event_code == EVENT_DETECTOR_ON:
            last_off_time, detectors_on = _record_gap(
                gaps, last_off_time, detectors_on, event_time,
            )

    return gaps
