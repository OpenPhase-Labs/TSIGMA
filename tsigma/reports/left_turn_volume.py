"""
Left Turn Volume report plugin.

HCM decision-boundary volume analysis. The missing 8th variant in the
left-turn suite. Computes left-turn volume on a target approach and
opposing-through volume on the opposing phase's approach, then applies
HCM-style decision-boundary formulas to flag intersections that may
warrant a left-turn phase study.

Detector filtering
------------------
Left-turn detectors are identified by ``movement_type_code == "L"`` on
the target approach; opposing-through detectors by movement code in
``{"T", "TR", "TL"}`` on the opposing-phase approach. ``opposing_lanes``
equals the count of opposing-through detectors.

For historical data where ``movement_type_code`` is not populated on a
detector (snapshot pre-dates movement-type tracking), the detector is
*excluded* from counts. If no detectors on either approach have movement
codes set, the report falls back to the legacy "all detectors on the
approach" behavior so old configurations still produce a useful result.
"""

import logging
from collections.abc import Iterable
from datetime import datetime, time, timedelta
from typing import Any

import pandas as pd
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from ..config_resolver import SignalConfig, get_config_at
from .registry import Report, ReportMetadata, ReportRegistry
from .sdk import (
    DIRECTION_MAP,
    EVENT_DETECTOR_ON,
    fetch_events,
    parse_time,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# ATSPM opposing phase mapping: NEMA 1 <-> 2, 3 <-> 4, 5 <-> 6, 7 <-> 8.
_OPPOSING_PHASE: dict[int, int] = {
    1: 2, 2: 1,
    3: 4, 4: 3,
    5: 6, 6: 5,
    7: 8, 8: 7,
}

# Cross-product review thresholds (ATSPM 5.x): 1 opposing lane uses the
# lower bound, 2+ use the higher bound.
_XPROD_LIMIT_SINGLE_LANE = 50_000
_XPROD_LIMIT_MULTI_LANE = 100_000

# HCM decision-boundary formula table. Key: (approach_type, is_multi_lane).
# Value: (exponent applied to opposing-through volume,
#         scale factor applied to LT_V * OPP_V**exp,
#         HCM threshold for the boundary review).
_HCM_FORMULAS: dict[tuple[str, bool], tuple[float, float, float]] = {
    ("permissive", False):             (0.706, 1.0, 9519.0),
    ("permissive", True):              (0.642, 2.0, 7974.0),
    ("permissive_protected", False):   (0.500, 1.0, 4638.0),
    ("permissive_protected", True):    (0.404, 2.0, 3782.0),
    ("protected", False):              (0.425, 1.0, 3693.0),
    ("protected", True):               (0.404, 2.0, 3782.0),
}

_VALID_APPROACH_TYPES = frozenset(t for t, _ in _HCM_FORMULAS.keys())

# 15-min bin size for the demand list.
_BIN_MINUTES = 15

# AM / PM peak-hour search windows (inclusive start, exclusive end).
_AM_PEAK_WINDOW = (6, 9)
_PM_PEAK_WINDOW = (15, 18)

# Output schema — the empty DataFrame must carry all of these columns.
_PER_BIN_COLUMNS = [
    "bin_start",
    "left_turn_volume_bin",
    "opposing_through_volume_bin",
]

_SUMMARY_COLUMNS = [
    "approach_id",
    "direction",
    "opposing_direction",
    "left_turn_volume",
    "opposing_through_volume",
    "opposing_lanes",
    "cross_product_value",
    "cross_product_review",
    "calculated_volume_boundary",
    "decision_boundary_threshold",
    "decision_boundaries_review",
    "am_peak_hour",
    "am_peak_left_turn_volume",
    "pm_peak_hour",
    "pm_peak_left_turn_volume",
    "approach_type",
]

_EMPTY_COLUMNS = _PER_BIN_COLUMNS + _SUMMARY_COLUMNS


# ---------------------------------------------------------------------------
# Params
# ---------------------------------------------------------------------------


class LeftTurnVolumeParams(BaseModel):
    """Parameters for left-turn volume analysis."""

    signal_id: str = Field(..., description="Signal identifier")
    approach_id: str = Field(..., description="Approach whose left turn is analyzed")
    start: str = Field(..., description="Analysis window start (ISO-8601)")
    end: str = Field(..., description="Analysis window end (ISO-8601)")
    days_of_week: list[int] = Field(
        default=[0, 1, 2, 3, 4],
        description="Python weekday numbers to include (0=Mon..6=Sun)",
    )
    start_hour: int = Field(default=6, ge=0, le=23)
    start_minute: int = Field(default=0, ge=0, le=59)
    end_hour: int = Field(default=18, ge=0, le=23)
    end_minute: int = Field(default=0, ge=0, le=59)
    approach_type: str = Field(
        default="permissive",
        description="One of: 'permissive', 'permissive_protected', 'protected'",
    )


# ---------------------------------------------------------------------------
# Plugin
# ---------------------------------------------------------------------------


@ReportRegistry.register("left-turn-volume")
class LeftTurnVolumeReport(Report[LeftTurnVolumeParams]):
    """HCM decision-boundary left-turn volume analysis."""

    metadata = ReportMetadata(
        name="left-turn-volume",
        description=(
            "Left-turn vs opposing-through volume comparison with HCM "
            "decision-boundary formulas to flag intersections needing a "
            "left-turn phase study."
        ),
        category="detailed",
        estimated_time="fast",
        export_formats=["csv", "json", "ndjson"],
    )

    async def execute(
        self, params: LeftTurnVolumeParams, session: AsyncSession
    ) -> pd.DataFrame:
        start = parse_time(params.start)
        end = parse_time(params.end)

        if params.approach_type not in _VALID_APPROACH_TYPES:
            raise ValueError(
                f"Invalid approach_type: {params.approach_type!r}. "
                f"Expected one of {sorted(_VALID_APPROACH_TYPES)}"
            )

        logger.info(
            "Running left-turn-volume for %s approach=%s type=%s %s..%s",
            params.signal_id, params.approach_id, params.approach_type, start, end,
        )

        config = await get_config_at(session, params.signal_id, as_of=start)
        resolved = _resolve_detectors(config, params.approach_id)
        if resolved is None:
            return _empty_frame()

        lt_channels, opp_channels, lt_direction, opp_direction = resolved

        if not lt_channels or not opp_channels:
            return _empty_frame()

        df = await fetch_events(
            params.signal_id, start, end,
            (EVENT_DETECTOR_ON,),
            event_param_in=list(lt_channels | opp_channels),
        )
        if df.empty:
            return _empty_frame()

        time_filters = _TimeFilters(
            days_of_week=set(params.days_of_week),
            start_time=time(params.start_hour, params.start_minute),
            end_time=time(params.end_hour, params.end_minute),
        )
        df = _apply_time_filters(df, time_filters)
        if df.empty:
            return _empty_frame()

        df = _classify_detector_side(df, lt_channels, opp_channels)

        bins_df = _bin_15min(df, start, end)
        if bins_df.empty:
            return _empty_frame()

        lt_total = int(bins_df["left_turn_volume_bin"].sum())
        opp_total = int(bins_df["opposing_through_volume_bin"].sum())
        opp_lanes = len(opp_channels)

        summary = _build_summary(
            approach_id=params.approach_id,
            direction=lt_direction,
            opposing_direction=opp_direction,
            approach_type=params.approach_type,
            lt_total=lt_total,
            opp_total=opp_total,
            opp_lanes=opp_lanes,
            bins_df=bins_df,
        )

        return _attach_summary(bins_df, summary)


# ---------------------------------------------------------------------------
# Config resolution
# ---------------------------------------------------------------------------


def _resolve_detectors(
    config: SignalConfig, approach_id: str,
) -> tuple[set[int], set[int], str, str] | None:
    """
    Find LT and opposing-through detector channels plus compass directions.

    Returns (lt_channels, opp_channels, lt_direction, opposing_direction) or
    ``None`` if the LT approach cannot be located.
    """
    lt_approach = next(
        (a for a in config.approaches if str(a.approach_id) == str(approach_id)),
        None,
    )
    if lt_approach is None:
        return None

    lt_phase = (
        lt_approach.protected_phase_number
        if lt_approach.protected_phase_number is not None
        else lt_approach.permissive_phase_number
    )
    if lt_phase is None:
        return None

    opp_phase = _OPPOSING_PHASE.get(int(lt_phase))
    if opp_phase is None:
        return None

    opp_approach = next(
        (
            a for a in config.approaches
            if a.protected_phase_number == opp_phase
            or a.permissive_phase_number == opp_phase
        ),
        None,
    )

    lt_channels = _channels_for_movements(
        config, lt_approach.approach_id, {"L"},
    )
    opp_channels: set[int] = set()
    opp_direction = "Unknown"
    if opp_approach is not None:
        opp_channels = _channels_for_movements(
            config, opp_approach.approach_id, {"T", "TR", "TL"},
        )
        opp_direction = DIRECTION_MAP.get(opp_approach.direction_type_id, "Unknown")

    lt_direction = DIRECTION_MAP.get(lt_approach.direction_type_id, "Unknown")
    return lt_channels, opp_channels, lt_direction, opp_direction


def _channels_for_movements(
    config: SignalConfig, approach_id: str, movement_codes: set[str],
) -> set[int]:
    """Channels on ``approach_id`` whose ``movement_type_code`` is in ``movement_codes``.

    Falls back to "every detector on the approach" when none of the
    approach's detectors have a movement type set — preserves the legacy
    behavior for historical configs that predate movement-type tracking.
    """
    detectors = list(config.detectors_for_approach(approach_id))
    any_typed = any(d.movement_type_code is not None for d in detectors)
    if not any_typed:
        return {d.detector_channel for d in detectors}
    return {
        d.detector_channel
        for d in detectors
        if d.movement_type_code in movement_codes
    }


# ---------------------------------------------------------------------------
# Filtering and binning
# ---------------------------------------------------------------------------


class _TimeFilters:
    """Bundle of time-of-day / day-of-week filter settings."""

    def __init__(
        self,
        *,
        days_of_week: set[int],
        start_time: time,
        end_time: time,
    ) -> None:
        self.days_of_week = days_of_week
        self.start_time = start_time
        self.end_time = end_time


def _apply_time_filters(df: pd.DataFrame, tf: _TimeFilters) -> pd.DataFrame:
    """Filter events by day-of-week and time-of-day."""
    df = df.copy()
    df["event_time"] = pd.to_datetime(df["event_time"])
    mask = df["event_time"].dt.weekday.isin(tf.days_of_week)
    df = df[mask]
    if df.empty:
        return df

    tod = df["event_time"].dt.time
    if tf.start_time <= tf.end_time:
        tod_mask = (tod >= tf.start_time) & (tod <= tf.end_time)
    else:
        # Overnight window — include wrap-around.
        tod_mask = (tod >= tf.start_time) | (tod <= tf.end_time)
    return df[tod_mask]


def _classify_detector_side(
    df: pd.DataFrame, lt_channels: set[int], opp_channels: set[int],
) -> pd.DataFrame:
    """Tag each event as LT or opposing-through, drop anything else."""
    df = df.copy()
    param = df["event_param"].astype(int)
    is_lt = param.isin(lt_channels)
    is_opp = param.isin(opp_channels)
    df = df[is_lt | is_opp].copy()
    df["is_lt"] = df["event_param"].astype(int).isin(lt_channels)
    return df


def _bin_15min(
    df: pd.DataFrame, start: datetime, end: datetime,
) -> pd.DataFrame:
    """Count LT / opposing events per 15-minute bin."""
    df = df.copy()
    df["bin_start"] = df["event_time"].dt.floor(f"{_BIN_MINUTES}min")
    grouped = (
        df.groupby(["bin_start", "is_lt"])
        .size()
        .unstack(fill_value=0)
        .reset_index()
    )
    grouped = grouped.rename(columns={True: "left_turn_volume_bin",
                                      False: "opposing_through_volume_bin"})
    for col in ("left_turn_volume_bin", "opposing_through_volume_bin"):
        if col not in grouped.columns:
            grouped[col] = 0

    grouped["bin_start"] = grouped["bin_start"].apply(lambda t: t.isoformat())
    grouped = grouped.sort_values("bin_start").reset_index(drop=True)
    return grouped[_PER_BIN_COLUMNS]


# ---------------------------------------------------------------------------
# Summary computation
# ---------------------------------------------------------------------------


def _compute_cross_product(lt_total: int, opp_total: int, opp_lanes: int,
                           ) -> tuple[float, bool]:
    """Cross-product value and the HCM review flag (1-lane vs 2+-lane threshold)."""
    value = float(lt_total) * float(opp_total)
    limit = (
        _XPROD_LIMIT_SINGLE_LANE
        if opp_lanes <= 1
        else _XPROD_LIMIT_MULTI_LANE
    )
    return value, value > limit


def _compute_decision_boundary(
    approach_type: str, lt_total: int, opp_total: int, opp_lanes: int,
) -> tuple[float, float, bool]:
    """Apply the HCM formula. Returns (calc_value, threshold, review_flag)."""
    is_multi = opp_lanes > 1
    exponent, scale, threshold = _HCM_FORMULAS[(approach_type, is_multi)]
    calc = scale * float(lt_total) * (float(opp_total) ** exponent)
    return calc, threshold, calc > threshold


def _peak_hour(
    bins_df: pd.DataFrame, window: tuple[int, int],
) -> tuple[str | None, int | None]:
    """
    1-hour sliding peak within [window[0]:00, window[1]:00).

    Returns ISO-8601 start of the 15-min bin that begins the peak hour plus
    the total LT volume in that hour, or (None, None) if no bins fall in
    the window.
    """
    if bins_df.empty:
        return None, None

    ts = pd.to_datetime(bins_df["bin_start"])
    in_window = (ts.dt.hour >= window[0]) & (ts.dt.hour < window[1])
    candidates = bins_df.loc[in_window].copy()
    if candidates.empty:
        return None, None

    candidates = candidates.sort_values("bin_start").reset_index(drop=True)
    ts_sorted = pd.to_datetime(candidates["bin_start"])
    best_start: str | None = None
    best_volume = -1
    bins_per_hour = 60 // _BIN_MINUTES

    for i in range(len(candidates)):
        hour_end = ts_sorted.iloc[i] + timedelta(hours=1)
        slab = candidates[(ts_sorted >= ts_sorted.iloc[i]) & (ts_sorted < hour_end)]
        if len(slab) < 1:
            continue
        # Require the slab to stay inside the window — if fewer than
        # bins_per_hour bins, we still accept the partial-hour slab
        # because upstream filters may have dropped bins.
        vol = int(slab["left_turn_volume_bin"].sum())
        if vol > best_volume and len(slab) <= bins_per_hour:
            best_volume = vol
            best_start = candidates.iloc[i]["bin_start"]

    if best_start is None or best_volume < 0:
        return None, None
    return best_start, best_volume


def _build_summary(
    *,
    approach_id: str,
    direction: str,
    opposing_direction: str,
    approach_type: str,
    lt_total: int,
    opp_total: int,
    opp_lanes: int,
    bins_df: pd.DataFrame,
) -> dict[str, Any]:
    """Build the broadcast-summary dict attached to every row."""
    xprod_value, xprod_review = _compute_cross_product(lt_total, opp_total, opp_lanes)
    calc_boundary, threshold, boundary_review = _compute_decision_boundary(
        approach_type, lt_total, opp_total, opp_lanes,
    )
    am_hour, am_vol = _peak_hour(bins_df, _AM_PEAK_WINDOW)
    pm_hour, pm_vol = _peak_hour(bins_df, _PM_PEAK_WINDOW)

    return {
        "approach_id": str(approach_id),
        "direction": direction,
        "opposing_direction": opposing_direction,
        "left_turn_volume": lt_total,
        "opposing_through_volume": opp_total,
        "opposing_lanes": opp_lanes,
        "cross_product_value": xprod_value,
        "cross_product_review": xprod_review,
        "calculated_volume_boundary": calc_boundary,
        "decision_boundary_threshold": threshold,
        "decision_boundaries_review": boundary_review,
        "am_peak_hour": am_hour,
        "am_peak_left_turn_volume": am_vol,
        "pm_peak_hour": pm_hour,
        "pm_peak_left_turn_volume": pm_vol,
        "approach_type": approach_type,
    }


def _attach_summary(
    bins_df: pd.DataFrame, summary: dict[str, Any],
) -> pd.DataFrame:
    """Broadcast summary columns to every row of the per-bin DataFrame."""
    out = bins_df.copy()
    for col in _SUMMARY_COLUMNS:
        out[col] = summary[col]
    return out[_EMPTY_COLUMNS]


def _empty_frame() -> pd.DataFrame:
    """Empty DataFrame with the full output schema."""
    return pd.DataFrame(columns=_EMPTY_COLUMNS)


__all__: Iterable[str] = [
    "LeftTurnVolumeParams",
    "LeftTurnVolumeReport",
]
