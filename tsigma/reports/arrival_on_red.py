"""
Arrival on Red report plugin.

Quantifies the percentage and count of vehicle arrivals that occur
during the red phase — the complement of Arrival on Green. Together
the two reports partition the vehicle stream into mutually exclusive
arrival bins, a core coordination-quality metric.

Algorithm:
1. Fetch vehicle detector ON events plus phase boundary events
   (green, yellow, red) for the analysis window.
2. Build red-to-red cycles from phase events for each target phase.
3. For each time bin of `bin_size_minutes`:
   - Enumerate detector events within the bin.
   - Classify each detection as arrival-on-red if its timestamp is
     before the green start of its cycle.
   - Tally total detections and arrivals on red.
4. Compute bin-level pct and hourly-normalised rates.
5. Broadcast overall summary counts as constant columns on every row.

Follows ATSPM 5.x semantics and the SDK conventions used by the
sibling `arrivals_on_green.py` and threshold-based `split_failure.py`
reports.
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
    EVENT_DETECTOR_ON,
    EVENT_PHASE_GREEN,
    EVENT_RED_CLEARANCE,
    EVENT_YELLOW_CLEARANCE,
    bin_timestamp,
    fetch_events_split,
    parse_time,
)

logger = logging.getLogger(__name__)

# Phase-event codes used to build red-to-red cycles.
_PHASE_CODES = (EVENT_PHASE_GREEN, EVENT_YELLOW_CLEARANCE, EVENT_RED_CLEARANCE)

# Per-bin columns produced by the main aggregation.
_PER_BIN_COLUMNS = [
    "bin_start",
    "phase_number",
    "total_detections",
    "arrivals_on_red",
    "pct_arrivals_on_red",
    "total_vehicles_per_hour",
    "arrivals_on_red_per_hour",
]

# Broadcast summary columns (same value on every row — matches the
# dq_* / ped_* / peak_* pattern from left_turn_gap.py).
_SUMMARY_COLUMNS = [
    "total_detector_hits",
    "total_arrival_on_red",
    "pct_arrival_on_red_overall",
]

_ALL_COLUMNS = _PER_BIN_COLUMNS + _SUMMARY_COLUMNS


class ArrivalOnRedParams(BaseModel):
    """Parameters for arrival-on-red analysis."""

    signal_id: str = Field(..., description="Signal identifier")
    start: str = Field(..., description="Analysis window start (ISO-8601)")
    end: str = Field(..., description="Analysis window end (ISO-8601)")
    bin_size_minutes: int = Field(15, description="Time bin size in minutes")
    include_permissive: bool = Field(
        False, description="Include permissive/split-phase movements"
    )
    yellow_as_red: bool = Field(
        True,
        description=(
            "Classify detector activations during yellow clearance as arrival-on-red "
            "(TSIGMA default — safety-focused semantics). Set to False to match "
            "ATSPM 5.x, which treats yellow as part of the green interval so only "
            "detections strictly before green start are AOR."
        ),
    )


@ReportRegistry.register("arrival-on-red")
class ArrivalOnRedReport(Report[ArrivalOnRedParams]):
    """Percentage and count of detector activations occurring during red phase."""

    metadata = ReportMetadata(
        name="arrival-on-red",
        description=(
            "Arrivals on red per phase in configurable time bins. Complement of "
            "arrivals-on-green. Reports per-bin counts, percentage, and hourly-normalised rates."
        ),
        category="standard",
        estimated_time="fast",
        export_formats=["csv", "json", "ndjson"],
    )

    async def execute(
        self, params: ArrivalOnRedParams, session: AsyncSession,
    ) -> pd.DataFrame:
        """
        Execute arrival-on-red analysis.

        Returns a DataFrame with one row per (phase, bin) combination.
        Each row carries the per-bin counts plus broadcast summary columns.
        """
        signal_id = params.signal_id
        start = parse_time(params.start)
        end = parse_time(params.end)
        bin_size_minutes = params.bin_size_minutes

        logger.info(
            "Running arrival-on-red for %s from %s to %s (bin=%dmin)",
            signal_id, start, end, bin_size_minutes,
        )

        config = await get_config_at(session, signal_id, as_of=start)
        channel_to_phase, det_channels = _build_channel_map(
            config, include_permissive=params.include_permissive,
        )

        if not det_channels:
            return _empty_result()

        target_phases = set(channel_to_phase.values())

        df = await fetch_events_split(
            signal_id, start, end,
            phase_codes=list(_PHASE_CODES),
            det_channels=list(det_channels),
            det_codes=(EVENT_DETECTOR_ON,),
        )

        if df.empty:
            return _empty_result()

        classified = _classify_detections(
            df, target_phases, channel_to_phase,
            yellow_as_red=params.yellow_as_red,
        )
        if not classified:
            return _empty_result()

        result_df = _aggregate_bins(classified, bin_size_minutes)
        _attach_summary_columns(result_df)

        logger.info(
            "Arrival-on-red complete: %d bin-rows, total_det=%d, aor=%d",
            len(result_df),
            int(result_df["total_detector_hits"].iloc[0]) if not result_df.empty else 0,
            int(result_df["total_arrival_on_red"].iloc[0]) if not result_df.empty else 0,
        )
        return result_df


# ---------------------------------------------------------------------------
# Config / channel mapping
# ---------------------------------------------------------------------------


def _build_channel_map(
    config: SignalConfig,
    *,
    include_permissive: bool,
) -> tuple[dict[int, int], set[int]]:
    """
    Build detector-channel -> phase-number map from signal config.

    Uses the approach's protected phase by default. When
    ``include_permissive`` is true and the approach has no protected
    phase, falls back to the permissive phase. Returns
    (channel_to_phase, det_channels).
    """
    channel_to_phase: dict[int, int] = {}
    for approach in config.approaches:
        phase = approach.protected_phase_number
        if phase is None and include_permissive:
            phase = approach.permissive_phase_number
        if phase is None:
            continue
        for det in config.detectors_for_approach(approach.approach_id):
            channel_to_phase[int(det.detector_channel)] = int(phase)
    return channel_to_phase, set(channel_to_phase.keys())


# ---------------------------------------------------------------------------
# Detection classification (red vs green per cycle)
# ---------------------------------------------------------------------------


def _classify_detections(
    df: pd.DataFrame,
    target_phases: set[int],
    channel_to_phase: dict[int, int],
    *,
    yellow_as_red: bool,
) -> list[dict[str, Any]]:
    """
    Single-pass walk of phase+detector events.

    Tracks green state per phase. A detector-on is arrival-on-red when
    the phase is not currently green (the detection precedes green start
    within its red-to-red cycle).

    When ``yellow_as_red`` is True, ``EVENT_YELLOW_CLEARANCE`` ends the
    green state so detections during yellow are classified as AOR
    (TSIGMA default). When False, yellow keeps the phase "green" until
    ``EVENT_RED_CLEARANCE`` arrives — matching ATSPM 5.x, where only
    strictly-before-green detections are AOR.
    """
    phase_is_green = dict.fromkeys(target_phases, False)
    rows: list[dict[str, Any]] = []

    for _, row in df.iterrows():
        code = int(row["event_code"])
        param = int(row["event_param"])
        event_time = row["event_time"]

        if code == EVENT_PHASE_GREEN and param in target_phases:
            phase_is_green[param] = True
            continue

        if (code == EVENT_YELLOW_CLEARANCE and param in target_phases
                and yellow_as_red):
            phase_is_green[param] = False
            continue

        if code == EVENT_RED_CLEARANCE and param in target_phases:
            # Explicit red clearance — phase is not green.
            phase_is_green[param] = False
            continue

        if code != EVENT_DETECTOR_ON:
            continue

        phase = channel_to_phase.get(param)
        if phase is None or phase not in target_phases:
            continue

        rows.append({
            "event_time": event_time,
            "phase_number": phase,
            "is_aor": not phase_is_green.get(phase, False),
        })

    return rows


# ---------------------------------------------------------------------------
# Binning and per-bin aggregation
# ---------------------------------------------------------------------------


def _aggregate_bins(
    classified: list[dict[str, Any]],
    bin_size_minutes: int,
) -> pd.DataFrame:
    """Group detection rows by (phase, bin) and compute per-bin metrics."""
    df = pd.DataFrame(classified)
    df["event_time"] = pd.to_datetime(df["event_time"])
    df["bin_start"] = df["event_time"].map(
        lambda dt: bin_timestamp(_to_datetime(dt), bin_size_minutes)
    )
    df["aor_int"] = df["is_aor"].astype(int)

    grouped = (
        df.groupby(["phase_number", "bin_start"])
        .agg(
            total_detections=("aor_int", "size"),
            arrivals_on_red=("aor_int", "sum"),
        )
        .reset_index()
    )

    grouped["total_detections"] = grouped["total_detections"].astype(int)
    grouped["arrivals_on_red"] = grouped["arrivals_on_red"].astype(int)
    grouped["pct_arrivals_on_red"] = _safe_pct(
        grouped["arrivals_on_red"], grouped["total_detections"],
    )

    scale = 60.0 / max(bin_size_minutes, 1)
    grouped["total_vehicles_per_hour"] = (
        grouped["total_detections"].astype(float) * scale
    ).round(2)
    grouped["arrivals_on_red_per_hour"] = (
        grouped["arrivals_on_red"].astype(float) * scale
    ).round(2)

    grouped = grouped.sort_values(["bin_start", "phase_number"]).reset_index(drop=True)
    return grouped[_PER_BIN_COLUMNS]


def _to_datetime(value: Any) -> datetime:
    """Coerce pandas Timestamp / datetime to a plain datetime."""
    if isinstance(value, datetime):
        return value
    return value.to_pydatetime()


def _safe_pct(numer: pd.Series, denom: pd.Series) -> pd.Series:
    """Percentage with zero-denominator guard — returns 0.0, never NaN."""
    numer_f = numer.astype(float)
    denom_f = denom.astype(float)
    pct = pd.Series(0.0, index=numer_f.index, dtype=float)
    mask = denom_f != 0
    pct.loc[mask] = (numer_f.loc[mask] / denom_f.loc[mask]) * 100.0
    return pct.round(1)


# ---------------------------------------------------------------------------
# Summary broadcast
# ---------------------------------------------------------------------------


def _attach_summary_columns(result_df: pd.DataFrame) -> None:
    """
    Broadcast overall totals across every row.

    Matches the dq_* / ped_* / peak_* pattern from left_turn_gap.py.
    """
    total_det = int(result_df["total_detections"].sum())
    total_aor = int(result_df["arrivals_on_red"].sum())
    pct_overall = round(total_aor / total_det * 100.0, 1) if total_det > 0 else 0.0

    result_df["total_detector_hits"] = total_det
    result_df["total_arrival_on_red"] = total_aor
    result_df["pct_arrival_on_red_overall"] = pct_overall


def _empty_result() -> pd.DataFrame:
    """Return an empty DataFrame with the full per-bin + summary schema."""
    return pd.DataFrame(columns=_ALL_COLUMNS)
