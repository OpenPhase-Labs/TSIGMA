"""
Left Turn Gap Data Check report plugin.

Lightweight pre-flight eligibility gate for the full ``left-turn-gap``
report. Tells a caller whether a given signal/approach has enough
detector configuration and hi-res event data to produce a meaningful
permissive-left-turn gap analysis — WITHOUT running the expensive gap
measurement pass.

Shape: scalar report (single-row DataFrame). Nine boolean readiness
flags plus six summary metrics (AM/PM peak volume, gap-out pct, ped
pct). The six ``insufficient_*`` flags short-circuit the metric
computations when data is missing.
"""

import logging
from datetime import date, datetime, time, timedelta
from typing import Any

import pandas as pd
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from ..config_resolver import ApproachSnapshot, SignalConfig, get_config_at
from .registry import (
    Report,
    ReportMetadata,
    ReportRegistry,
    ReportResourceNotFoundError,
)
from .sdk import (
    EVENT_DETECTOR_OFF,
    EVENT_DETECTOR_ON,
    EVENT_FORCE_OFF,
    EVENT_GAP_OUT,
    EVENT_MAX_OUT,
    EVENT_PED_CALL,
    EVENT_PHASE_END,
    EVENT_PHASE_GREEN,
    EVENT_YELLOW_CLEARANCE,
    fetch_events_split,
    parse_time,
)

logger = logging.getLogger(__name__)

# AM / PM peak windows (hours, inclusive-exclusive like ATSPM 5.x).
_AM_START_HOUR = 6
_AM_END_HOUR = 9
_PM_START_HOUR = 15
_PM_END_HOUR = 19

# 15-minute bin for peak-hour volume estimation.
_BIN_MINUTES = 15
_BINS_PER_HOUR = 60 // _BIN_MINUTES

# Phase-level codes we need to confirm aggregation/termination signals.
_PHASE_CODES = (
    EVENT_PHASE_GREEN, EVENT_YELLOW_CLEARANCE, EVENT_PHASE_END,
    EVENT_GAP_OUT, EVENT_MAX_OUT, EVENT_FORCE_OFF,
    EVENT_PED_CALL,
)
_TERMINATION_CODES = frozenset({EVENT_GAP_OUT, EVENT_MAX_OUT, EVENT_FORCE_OFF})

# Output schema. Fixed column order for CSV/JSON stability.
_OUTPUT_COLUMNS = [
    "signal_id", "approach_id", "start", "end",
    "left_turn_volume_ok", "gap_out_ok", "ped_cycle_ok",
    "insufficient_detector_event_count",
    "insufficient_cycle_aggregation",
    "insufficient_phase_termination",
    "insufficient_ped_aggregations",
    "insufficient_split_fail_aggregations",
    "insufficient_left_turn_gap_aggregations",
    "am_peak_left_turn_volume", "pm_peak_left_turn_volume",
    "am_gap_out_pct", "pm_gap_out_pct",
    "am_ped_pct", "pm_ped_pct",
    "overall_ready",
]


class LeftTurnGapDataCheckParams(BaseModel):
    """Parameters for the Left Turn Gap pre-flight eligibility check."""

    signal_id: str = Field(..., description="Signal identifier")
    approach_id: str = Field(..., description="Left-turn approach identifier")
    start: str = Field(..., description="Analysis window start (ISO-8601)")
    end: str = Field(..., description="Analysis window end (ISO-8601)")
    days_of_week: list[int] = Field(
        default_factory=lambda: [0, 1, 2, 3, 4],
        description="Python weekday numbers (0=Monday) to include",
    )
    volume_per_hour_threshold: int = Field(
        default=60,
        description="Minimum acceptable peak-hour left-turn volume (veh/hr)",
    )
    gap_out_threshold: float = Field(
        default=0.5,
        description="Max acceptable gap-out rate per window (0.0-1.0)",
    )
    pedestrian_threshold: float = Field(
        default=0.25,
        description="Max acceptable ped-cycle rate per window (0.0-1.0)",
    )


@ReportRegistry.register("left-turn-gap-data-check")
class LeftTurnGapDataCheckReport(Report[LeftTurnGapDataCheckParams]):
    """Pre-flight eligibility check for left-turn-gap analysis."""

    metadata = ReportMetadata(
        name="left-turn-gap-data-check",
        description=(
            "Lightweight pre-check: does this signal/approach have enough "
            "detector config and event data to run a meaningful left-turn "
            "gap analysis? Returns readiness flags and coarse metrics."
        ),
        category="dashboard",
        estimated_time="fast",
        export_formats=["csv", "json", "ndjson"],
    )

    @classmethod
    def preferred_http_status(cls, result: pd.DataFrame) -> int | None:
        """
        422 when ``overall_ready`` is False (signal exists but is not
        eligible for left-turn-gap analysis yet); 200 otherwise.  The
        Reports API honors this return value when serializing the
        response so clients can route on status code.
        """
        if result.empty:
            return None
        ready = result.iloc[0].get("overall_ready")
        return 422 if ready is False or bool(ready) is False else None

    async def execute(
        self, params: LeftTurnGapDataCheckParams, session: AsyncSession,
    ) -> pd.DataFrame:
        """Run the data-check and return a single-row DataFrame."""
        signal_id = params.signal_id
        approach_id = params.approach_id
        start = parse_time(params.start)
        end = parse_time(params.end)

        logger.info(
            "Running left-turn-gap-data-check for %s/%s from %s to %s",
            signal_id, approach_id, start, end,
        )

        config = await get_config_at(session, signal_id, as_of=start)
        approach = _find_approach(config, approach_id)

        # Signal/approach not in config → resource does not exist.  The
        # Reports API surfaces this as HTTP 404 (see ``ReportResourceNotFoundError``).
        if approach is None:
            raise ReportResourceNotFoundError(
                f"Approach '{approach_id}' not found on signal '{signal_id}' "
                f"as of {start.isoformat()}"
            )

        lt_channels = _lt_detector_channels(config, approach)

        # Approach exists but has no left-turn detectors configured.
        # This is a "not ready" result, not a missing resource — return
        # the populated not-ready row so the caller sees which check
        # failed (``insufficient_detector_event_count=True``).
        if not lt_channels:
            return _not_ready_frame(signal_id, approach_id, start, end)

        through_phase = approach.permissive_phase_number
        lt_phase = approach.protected_phase_number
        ped_phase = approach.ped_phase_number

        df = await _fetch_events(
            signal_id, start, end, lt_channels,
        )
        df = _filter_days_of_week(df, params.days_of_week)

        am_stats, pm_stats = _collect_window_stats(
            df,
            lt_channels=lt_channels,
            through_phase=through_phase,
            lt_phase=lt_phase,
            ped_phase=ped_phase,
        )
        insufficient = _compute_insufficient_flags(
            am_stats, pm_stats,
            ped_phase_configured=ped_phase is not None,
        )

        if _all_insufficient(insufficient):
            row = _build_row(
                signal_id, approach_id, start, end,
                insufficient=insufficient,
                am_peak=None, pm_peak=None,
                am_gap=None, pm_gap=None,
                am_ped=None, pm_ped=None,
                vol_threshold=params.volume_per_hour_threshold,
                gap_threshold=params.gap_out_threshold,
                ped_threshold=params.pedestrian_threshold,
            )
            return pd.DataFrame([row], columns=_OUTPUT_COLUMNS)

        am_peak = _peak_hour_volume(am_stats["bin_counts"])
        pm_peak = _peak_hour_volume(pm_stats["bin_counts"])
        am_gap = _termination_rate(am_stats)
        pm_gap = _termination_rate(pm_stats)
        am_ped = _ped_rate(am_stats)
        pm_ped = _ped_rate(pm_stats)

        row = _build_row(
            signal_id, approach_id, start, end,
            insufficient=insufficient,
            am_peak=am_peak, pm_peak=pm_peak,
            am_gap=am_gap, pm_gap=pm_gap,
            am_ped=am_ped, pm_ped=pm_ped,
            vol_threshold=params.volume_per_hour_threshold,
            gap_threshold=params.gap_out_threshold,
            ped_threshold=params.pedestrian_threshold,
        )
        logger.info(
            "Data-check result for %s/%s: overall_ready=%s",
            signal_id, approach_id, row["overall_ready"],
        )
        return pd.DataFrame([row], columns=_OUTPUT_COLUMNS)


# ---------------------------------------------------------------------------
# Config resolution
# ---------------------------------------------------------------------------


def _find_approach(config: SignalConfig, approach_id: str) -> ApproachSnapshot | None:
    """Return the approach snapshot with the given id or None."""
    for approach in config.approaches:
        if approach.approach_id == approach_id:
            return approach
    return None


def _lt_detector_channels(
    config: SignalConfig, approach: ApproachSnapshot | None,
) -> set[int]:
    """Set of detector channels for the left-turn approach."""
    if approach is None:
        return set()
    return {
        det.detector_channel
        for det in config.detectors_for_approach(approach.approach_id)
    }


# ---------------------------------------------------------------------------
# Event fetch and day-of-week filter
# ---------------------------------------------------------------------------


async def _fetch_events(
    signal_id: str, start: datetime, end: datetime, lt_channels: set[int],
) -> pd.DataFrame:
    """Fetch phase + detector events covering the analysis window."""
    return await fetch_events_split(
        signal_id, start, end,
        phase_codes=list(_PHASE_CODES),
        det_channels=list(lt_channels),
        det_codes=(EVENT_DETECTOR_ON, EVENT_DETECTOR_OFF),
    )


def _filter_days_of_week(df: pd.DataFrame, days_of_week: list[int]) -> pd.DataFrame:
    """Keep only rows whose ``event_time`` weekday is in ``days_of_week``."""
    if df.empty:
        return df
    allowed = set(days_of_week)
    times = pd.to_datetime(df["event_time"])
    mask = times.dt.weekday.isin(allowed)
    return df.loc[mask].reset_index(drop=True)


# ---------------------------------------------------------------------------
# Window statistics
# ---------------------------------------------------------------------------


def _window_for_day(day: date, am: bool) -> tuple[datetime, datetime]:
    """AM or PM window boundaries for a calendar day."""
    if am:
        start_h, end_h = _AM_START_HOUR, _AM_END_HOUR
    else:
        start_h, end_h = _PM_START_HOUR, _PM_END_HOUR
    start = datetime.combine(day, time(start_h, 0))
    end = datetime.combine(day, time(end_h, 0))
    return start, end


def _in_window(ts: datetime, am: bool) -> bool:
    """Return True if ``ts`` falls inside the AM or PM window (any day)."""
    hour = ts.hour
    if am:
        return _AM_START_HOUR <= hour < _AM_END_HOUR
    return _PM_START_HOUR <= hour < _PM_END_HOUR


def _empty_window_stats() -> dict[str, Any]:
    """Empty per-window accumulator."""
    return {
        "detector_hits": 0,                  # int — detector-ON count on LT channels
        "cycles": 0,                         # int — through-phase green cycles
        "lt_cycles": 0,                      # int — left-turn green cycles
        "terminations": 0,                   # int — any 4/5/6 on through phase
        "gap_outs": 0,                       # int — 4 on through phase
        "ped_calls": 0,                      # int — any ped calls seen
        "cycles_with_ped": 0,                # int — cycles with >=1 ped call
        "bin_counts": {},                    # dict[datetime_bin_start, int]
        "day_seen": set(),                   # days that had any data in window
    }


def _collect_window_stats(
    df: pd.DataFrame,
    *,
    lt_channels: set[int],
    through_phase: int | None,
    lt_phase: int | None,
    ped_phase: int | None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Single pass over events → AM + PM aggregate dicts."""
    am = _empty_window_stats()
    pm = _empty_window_stats()
    if df.empty:
        return am, pm

    times = pd.to_datetime(df["event_time"])
    # Build per-cycle ped tracker: key = (is_am, through_green_start_ts)
    current_cycle_key_am: datetime | None = None
    current_cycle_key_pm: datetime | None = None
    cycle_ped_am: dict[datetime, int] = {}
    cycle_ped_pm: dict[datetime, int] = {}

    for ts, code, param in zip(times, df["event_code"], df["event_param"], strict=False):
        py_ts = ts.to_pydatetime() if hasattr(ts, "to_pydatetime") else ts
        code = int(code)
        param = int(param)
        is_am = _in_window(py_ts, am=True)
        is_pm = _in_window(py_ts, am=False)
        if not (is_am or is_pm):
            continue

        stats = am if is_am else pm
        stats["day_seen"].add(py_ts.date())

        if code in (EVENT_DETECTOR_ON, EVENT_DETECTOR_OFF) and param in lt_channels:
            if code == EVENT_DETECTOR_ON:
                stats["detector_hits"] += 1
                _increment_bin(stats["bin_counts"], py_ts)
            continue

        if through_phase is not None and param == through_phase:
            if code == EVENT_PHASE_GREEN:
                stats["cycles"] += 1
                if is_am:
                    current_cycle_key_am = py_ts
                    cycle_ped_am[py_ts] = 0
                else:
                    current_cycle_key_pm = py_ts
                    cycle_ped_pm[py_ts] = 0
            elif code in _TERMINATION_CODES:
                stats["terminations"] += 1
                if code == EVENT_GAP_OUT:
                    stats["gap_outs"] += 1
            continue

        if lt_phase is not None and param == lt_phase and code == EVENT_PHASE_GREEN:
            stats["lt_cycles"] += 1
            continue

        if ped_phase is not None and code == EVENT_PED_CALL and param == ped_phase:
            stats["ped_calls"] += 1
            key = current_cycle_key_am if is_am else current_cycle_key_pm
            pool = cycle_ped_am if is_am else cycle_ped_pm
            if key is not None:
                pool[key] = pool.get(key, 0) + 1

    am["cycles_with_ped"] = sum(1 for v in cycle_ped_am.values() if v > 0)
    pm["cycles_with_ped"] = sum(1 for v in cycle_ped_pm.values() if v > 0)
    return am, pm


def _increment_bin(bin_counts: dict[datetime, int], ts: datetime) -> None:
    """Floor ``ts`` to the nearest _BIN_MINUTES bucket and increment count."""
    bucket_min = (ts.minute // _BIN_MINUTES) * _BIN_MINUTES
    bucket = ts.replace(minute=bucket_min, second=0, microsecond=0)
    bin_counts[bucket] = bin_counts.get(bucket, 0) + 1


# ---------------------------------------------------------------------------
# Insufficient-flag computation
# ---------------------------------------------------------------------------


def _compute_insufficient_flags(
    am: dict[str, Any], pm: dict[str, Any],
    *,
    ped_phase_configured: bool,
) -> dict[str, bool]:
    """Compute the 6 insufficient_* flags from per-window stats."""
    # Detector events: require some detector hits in both windows.
    det_missing = am["detector_hits"] == 0 or pm["detector_hits"] == 0
    # Cycle aggregation: through-phase greens seen in both windows.
    cycle_missing = am["cycles"] == 0 or pm["cycles"] == 0
    # Phase termination: any termination code on through phase.
    term_missing = am["terminations"] == 0 or pm["terminations"] == 0
    # Ped aggregations: only flag when a ped phase is configured on the
    # approach but no ped calls were recorded — an intersection without a
    # ped phase (rural highway, protected-only turn) is not "missing" data.
    ped_missing = (ped_phase_configured
                   and am["cycles"] > 0 and pm["cycles"] > 0
                   and am["ped_calls"] == 0 and pm["ped_calls"] == 0)
    # Split-fail eligibility: needs LT cycles AND detector events (to
    # measure occupancy). Insufficient if either is missing in either window.
    split_missing = (
        am["lt_cycles"] == 0 or pm["lt_cycles"] == 0
        or am["detector_hits"] == 0 or pm["detector_hits"] == 0
    )
    # Left-turn gap aggregation: needs through cycles + detector hits.
    gap_agg_missing = (
        am["cycles"] == 0 or pm["cycles"] == 0
        or am["detector_hits"] == 0 or pm["detector_hits"] == 0
    )
    return {
        "insufficient_detector_event_count": det_missing,
        "insufficient_cycle_aggregation": cycle_missing,
        "insufficient_phase_termination": term_missing,
        "insufficient_ped_aggregations": ped_missing,
        "insufficient_split_fail_aggregations": split_missing,
        "insufficient_left_turn_gap_aggregations": gap_agg_missing,
    }


def _all_insufficient(flags: dict[str, bool]) -> bool:
    """Early-exit gate: true when all six flags are True."""
    return all(flags.values())


# ---------------------------------------------------------------------------
# Metric computations
# ---------------------------------------------------------------------------


def _peak_hour_volume(bin_counts: dict[datetime, int]) -> int | None:
    """Peak 1-hour (4×15-min) rolling volume across ``bin_counts``.

    Returns None when there are no bins. Otherwise returns the maximum
    sum of any four consecutive 15-minute bins within a single window-day.
    """
    if not bin_counts:
        return None
    # Group bins by (date, window) — each window per day gives its own series.
    by_day: dict[date, list[tuple[datetime, int]]] = {}
    for bucket, count in bin_counts.items():
        by_day.setdefault(bucket.date(), []).append((bucket, count))

    peak = 0
    for buckets in by_day.values():
        buckets.sort(key=lambda p: p[0])
        counts = [c for _, c in buckets]
        # Rolling 4-bin sum
        if len(counts) < _BINS_PER_HOUR:
            # Scale partial hour up to a full-hour flow rate
            scaled = int(sum(counts) * _BINS_PER_HOUR / max(len(counts), 1))
            peak = max(peak, scaled)
            continue
        for i in range(len(counts) - _BINS_PER_HOUR + 1):
            window_sum = sum(counts[i:i + _BINS_PER_HOUR])
            peak = max(peak, window_sum)
    return peak


def _termination_rate(stats: dict[str, Any]) -> float | None:
    """Gap-out rate = gap_outs / cycles. None if no cycles."""
    if stats["cycles"] == 0:
        return None
    return stats["gap_outs"] / stats["cycles"]


def _ped_rate(stats: dict[str, Any]) -> float | None:
    """Ped-cycle rate = cycles_with_ped / cycles. None if no cycles."""
    if stats["cycles"] == 0:
        return None
    return stats["cycles_with_ped"] / stats["cycles"]


# ---------------------------------------------------------------------------
# Row builders
# ---------------------------------------------------------------------------


def _not_ready_frame(
    signal_id: str, approach_id: str, start: datetime, end: datetime,
) -> pd.DataFrame:
    """Single-row not-ready frame (no detectors / unknown approach)."""
    insufficient = {
        "insufficient_detector_event_count": True,
        "insufficient_cycle_aggregation": True,
        "insufficient_phase_termination": True,
        "insufficient_ped_aggregations": True,
        "insufficient_split_fail_aggregations": True,
        "insufficient_left_turn_gap_aggregations": True,
    }
    row = _build_row(
        signal_id, approach_id, start, end,
        insufficient=insufficient,
        am_peak=None, pm_peak=None,
        am_gap=None, pm_gap=None,
        am_ped=None, pm_ped=None,
        vol_threshold=0, gap_threshold=0.0, ped_threshold=0.0,
    )
    return pd.DataFrame([row], columns=_OUTPUT_COLUMNS)


def _build_row(
    signal_id: str, approach_id: str, start: datetime, end: datetime,
    *,
    insufficient: dict[str, bool],
    am_peak: int | None, pm_peak: int | None,
    am_gap: float | None, pm_gap: float | None,
    am_ped: float | None, pm_ped: float | None,
    vol_threshold: int,
    gap_threshold: float,
    ped_threshold: float,
) -> dict[str, Any]:
    """Assemble the output row, computing the three *_ok flags + overall."""
    volume_ok = _volume_ok(am_peak, pm_peak, vol_threshold)
    gap_ok = _gap_ok(am_gap, pm_gap, gap_threshold)
    ped_ok = _ped_ok(am_ped, pm_ped, ped_threshold)
    any_insufficient = any(insufficient.values())
    overall_ready = bool(volume_ok and gap_ok and ped_ok and not any_insufficient)

    return {
        "signal_id": signal_id,
        "approach_id": approach_id,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "left_turn_volume_ok": volume_ok,
        "gap_out_ok": gap_ok,
        "ped_cycle_ok": ped_ok,
        **insufficient,
        "am_peak_left_turn_volume": am_peak,
        "pm_peak_left_turn_volume": pm_peak,
        "am_gap_out_pct": None if am_gap is None else round(am_gap, 4),
        "pm_gap_out_pct": None if pm_gap is None else round(pm_gap, 4),
        "am_ped_pct": None if am_ped is None else round(am_ped, 4),
        "pm_ped_pct": None if pm_ped is None else round(pm_ped, 4),
        "overall_ready": overall_ready,
    }


def _volume_ok(am: int | None, pm: int | None, threshold: int) -> bool:
    """Volume OK = either window meets or exceeds the threshold."""
    am_ok = am is not None and am >= threshold
    pm_ok = pm is not None and pm >= threshold
    return bool(am_ok or pm_ok)


def _gap_ok(am: float | None, pm: float | None, threshold: float) -> bool:
    """Gap OK = BOTH windows at or below threshold (None treated as fail)."""
    if am is None or pm is None:
        return False
    return am <= threshold and pm <= threshold


def _ped_ok(am: float | None, pm: float | None, threshold: float) -> bool:
    """Ped OK = BOTH windows at or below threshold (None treated as fail)."""
    if am is None or pm is None:
        return False
    return am <= threshold and pm <= threshold


# Silence unused-import warning: ``timedelta`` is kept available for callers.
_ = timedelta
