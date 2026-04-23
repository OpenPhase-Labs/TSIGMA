"""
Transit Signal Priority (TSP) report plugin.

Analyzes TSP activity by counting TSP check-in, adjustment, and check-out
events per time bin. When a phase number is provided, measures the impact
of TSP on green duration by comparing cycles with and without TSP activity.
"""

import logging
from datetime import datetime, timedelta

import pandas as pd
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from .registry import Report, ReportMetadata, ReportRegistry
from .sdk import (
    EVENT_PHASE_GREEN,
    EVENT_PREEMPTION_CALL_INPUT_OFF,
    EVENT_PREEMPTION_CALL_INPUT_ON,
    EVENT_PREEMPTION_ENTRY_STARTED,
    EVENT_PREEMPTION_GATE_DOWN,
    EVENT_TSP_CHECK_IN,
    EVENT_TSP_EARLY_GREEN,
    EVENT_TSP_EXTEND_GREEN,
    EVENT_YELLOW_CLEARANCE,
    bin_timestamp,
    fetch_events,
    parse_time,
)

logger = logging.getLogger(__name__)

_TSP_CODES = (
    EVENT_TSP_CHECK_IN, EVENT_TSP_EARLY_GREEN, EVENT_TSP_EXTEND_GREEN,
    EVENT_PREEMPTION_CALL_INPUT_ON, EVENT_PREEMPTION_GATE_DOWN,
    EVENT_PREEMPTION_CALL_INPUT_OFF, EVENT_PREEMPTION_ENTRY_STARTED,
)


class TransitSignalPriorityParams(BaseModel):
    signal_id: str = Field(..., description="Signal identifier")
    start_time: str = Field(..., description="Analysis window start (ISO-8601)")
    end_time: str = Field(..., description="Analysis window end (ISO-8601)")
    phase_number: int | None = Field(default=None, description="Phase number for green duration impact analysis")
    time_bin_minutes: int = Field(default=15, description="Time bin size in minutes")


@ReportRegistry.register("transit-signal-priority")
class TransitSignalPriorityReport(Report[TransitSignalPriorityParams]):
    """Analyzes transit signal priority requests, adjustments, and green time impact."""

    metadata = ReportMetadata(
        name="transit-signal-priority",
        description="TSP event counts per time bin with optional green duration impact analysis.",
        category="standard",
        estimated_time="medium",
        export_formats=["csv", "json", "ndjson"],
    )

    async def execute(self, params: TransitSignalPriorityParams, session: AsyncSession) -> pd.DataFrame:
        """
        Execute transit signal priority analysis.

        Args:
            params: Validated TSP params.
            session: Database session.

        Returns:
            DataFrame with columns: time_bin, tsp_requests, tsp_adjustments,
            tsp_early_green, avg_green_with_tsp, avg_green_without_tsp.
        """
        signal_id = params.signal_id
        start_time = parse_time(params.start_time)
        end_time = parse_time(params.end_time)
        phase_number = params.phase_number
        time_bin_minutes = params.time_bin_minutes

        logger.info(
            "Running transit-signal-priority for %s from %s to %s",
            signal_id, start_time, end_time,
        )

        event_codes = list(_TSP_CODES)
        if phase_number is not None:
            event_codes.extend([EVENT_PHASE_GREEN, EVENT_YELLOW_CLEARANCE])

        df = await fetch_events(signal_id, start_time, end_time, event_codes)

        tsp_events, phase_greens, phase_yellows = _classify_tsp_events(
            df, phase_number,
        )

        # --- TSP event counts per bin via DataFrame ---
        bin_counts_df = _bin_tsp_counts_df(tsp_events, time_bin_minutes)

        # --- Green duration impact analysis (stateful, kept procedural) ---
        bin_green_with, bin_green_without = _bin_green_durations(
            phase_number, phase_greens, phase_yellows,
            tsp_events, time_bin_minutes,
        )

        # --- Assemble results via DataFrame merge ---
        result_df = _assemble_tsp_results_df(
            bin_counts_df, bin_green_with, bin_green_without,
        )

        logger.info("Transit-signal-priority complete: %d bins", len(result_df))
        return result_df


def _classify_tsp_events(
    df: pd.DataFrame, phase_number: int | None,
) -> tuple[list[dict], list[datetime], list[datetime]]:
    """Separate raw events into TSP events and phase green/yellow timestamps."""
    tsp_events: list[dict] = []
    phase_greens: list[datetime] = []
    phase_yellows: list[datetime] = []

    for _, row in df.iterrows():
        code = int(row["event_code"])
        param = int(row["event_param"])
        event_time = row["event_time"]

        if code in _TSP_CODES:
            tsp_events.append({
                "event_code": code,
                "event_param": param,
                "event_time": event_time,
            })
        elif phase_number is not None and param == phase_number:
            if code == EVENT_PHASE_GREEN:
                phase_greens.append(event_time)
            elif code == EVENT_YELLOW_CLEARANCE:
                phase_yellows.append(event_time)

    return tsp_events, phase_greens, phase_yellows


_TSP_BIN_MAP = {
    EVENT_TSP_CHECK_IN: "tsp_requests",             # 112 — priority request received
    EVENT_TSP_EARLY_GREEN: "tsp_early_green",       # 113 — early green adjustment
    EVENT_TSP_EXTEND_GREEN: "tsp_adjustments",      # 114 — extend green adjustment
    EVENT_PREEMPTION_CALL_INPUT_ON: "tsp_requests",  # 102 — preempt request
}


def _bin_tsp_counts_df(
    tsp_events: list[dict], time_bin_minutes: int,
) -> pd.DataFrame:
    """Count TSP events per time bin, returning a DataFrame with bin counts."""
    if not tsp_events:
        return pd.DataFrame(columns=["time_bin", "tsp_requests", "tsp_adjustments", "tsp_early_green"])

    rows = []
    for event in tsp_events:
        metric = _TSP_BIN_MAP.get(event["event_code"])
        if metric is not None:
            rows.append({
                "time_bin": bin_timestamp(event["event_time"], time_bin_minutes),
                "metric": metric,
            })

    if not rows:
        return pd.DataFrame(columns=["time_bin", "tsp_requests", "tsp_adjustments", "tsp_early_green"])

    df = pd.DataFrame(rows)
    pivoted = (
        df.groupby(["time_bin", "metric"])
        .size()
        .unstack(fill_value=0)
        .reindex(columns=["tsp_requests", "tsp_adjustments", "tsp_early_green"], fill_value=0)
        .reset_index()
    )
    return pivoted


def _pair_green_yellow(
    phase_greens: list[datetime],
    phase_yellows: list[datetime],
) -> list[tuple[datetime, datetime, float]]:
    """Pair each green start with its next yellow, returning (green, yellow, duration) tuples."""
    pairs = []
    yellow_idx = 0
    for green_time in phase_greens:
        while yellow_idx < len(phase_yellows) and phase_yellows[yellow_idx] <= green_time:
            yellow_idx += 1
        if yellow_idx >= len(phase_yellows):
            break
        yellow_time = phase_yellows[yellow_idx]
        duration = (yellow_time - green_time).total_seconds()
        if 0 < duration <= 300:
            pairs.append((green_time, yellow_time, duration))
    return pairs


def _classify_green_pair(
    green_time: datetime,
    yellow_time: datetime,
    green_duration: float,
    tsp_active_periods: list[tuple[datetime, datetime]],
    time_bin_minutes: int,
    with_rows: list[dict],
    without_rows: list[dict],
) -> None:
    """Classify a single green/yellow pair as TSP-active or not and append to the right list."""
    bin_key = bin_timestamp(green_time, time_bin_minutes)
    row = {"time_bin": bin_key, "green_duration": green_duration}
    if _is_tsp_active(green_time, yellow_time, tsp_active_periods):
        with_rows.append(row)
    else:
        without_rows.append(row)


def _bin_green_durations(
    phase_number: int | None,
    phase_greens: list[datetime],
    phase_yellows: list[datetime],
    tsp_events: list,
    time_bin_minutes: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Classify green durations into TSP-active and non-TSP, returned as DataFrames."""
    empty = pd.DataFrame(columns=["time_bin", "green_duration"])

    if phase_number is None or not phase_greens or not phase_yellows:
        return empty, empty

    tsp_active_periods = _build_tsp_active_periods(tsp_events)
    pairs = _pair_green_yellow(phase_greens, phase_yellows)
    with_rows: list[dict] = []
    without_rows: list[dict] = []

    for green_time, yellow_time, duration in pairs:
        _classify_green_pair(
            green_time, yellow_time, duration,
            tsp_active_periods, time_bin_minutes,
            with_rows, without_rows,
        )

    df_with = pd.DataFrame(with_rows, columns=["time_bin", "green_duration"]) if with_rows else empty.copy()
    df_without = pd.DataFrame(without_rows, columns=["time_bin", "green_duration"]) if without_rows else empty.copy()
    return df_with, df_without


def _avg_green_per_bin(df: pd.DataFrame, col_name: str) -> pd.Series:
    """Compute average green duration per time bin, or empty Series."""
    if df.empty:
        return pd.Series(dtype=float, name=col_name)
    return (
        df.groupby("time_bin")["green_duration"]
        .mean()
        .round(2)
        .rename(col_name)
    )


def _base_result_df(
    bin_counts_df: pd.DataFrame,
    avg_with: pd.Series,
    avg_without: pd.Series,
) -> pd.DataFrame | None:
    """Build the base result DataFrame from bin counts or green-duration bins."""
    if not bin_counts_df.empty:
        return bin_counts_df.copy()

    all_bins: set = set()
    if not avg_with.empty:
        all_bins.update(avg_with.index)
    if not avg_without.empty:
        all_bins.update(avg_without.index)
    if not all_bins:
        return None

    result_df = pd.DataFrame({"time_bin": sorted(all_bins)})
    for col in ["tsp_requests", "tsp_adjustments", "tsp_early_green"]:
        result_df[col] = 0
    return result_df


def _expand_to_all_bins(
    result_df: pd.DataFrame,
    avg_with: pd.Series,
    avg_without: pd.Series,
) -> pd.DataFrame:
    """Ensure result_df covers every time bin present in the green averages."""
    all_time_bins = set(result_df["time_bin"])
    if not avg_with.empty:
        all_time_bins.update(avg_with.index)
    if not avg_without.empty:
        all_time_bins.update(avg_without.index)

    missing = all_time_bins - set(result_df["time_bin"])
    if missing:
        extra = pd.DataFrame({"time_bin": sorted(missing)})
        for col in ["tsp_requests", "tsp_adjustments", "tsp_early_green"]:
            extra[col] = 0
        result_df = pd.concat([result_df, extra], ignore_index=True)
    return result_df


def _finalize_result_df(
    result_df: pd.DataFrame,
    avg_with: pd.Series,
    avg_without: pd.Series,
) -> pd.DataFrame:
    """Merge green averages, cast columns, and return DataFrame."""
    result_df = result_df.set_index("time_bin")
    result_df["avg_green_with_tsp"] = avg_with
    result_df["avg_green_without_tsp"] = avg_without
    result_df = result_df.sort_index().reset_index()

    for col in ["tsp_requests", "tsp_adjustments", "tsp_early_green"]:
        result_df[col] = result_df[col].astype(int)

    return result_df


def _assemble_tsp_results_df(
    bin_counts_df: pd.DataFrame,
    df_green_with: pd.DataFrame,
    df_green_without: pd.DataFrame,
) -> pd.DataFrame:
    """Build final result DataFrame using DataFrame operations."""
    avg_with = _avg_green_per_bin(df_green_with, "avg_green_with_tsp")
    avg_without = _avg_green_per_bin(df_green_without, "avg_green_without_tsp")

    result_df = _base_result_df(bin_counts_df, avg_with, avg_without)
    if result_df is None:
        return pd.DataFrame(columns=[
            "time_bin", "tsp_requests", "tsp_adjustments", "tsp_early_green",
            "avg_green_with_tsp", "avg_green_without_tsp",
        ])

    result_df = _expand_to_all_bins(result_df, avg_with, avg_without)
    return _finalize_result_df(result_df, avg_with, avg_without)


def _build_tsp_active_periods(
    tsp_events: list[dict],
) -> list[tuple[datetime, datetime]]:
    """Build TSP active periods from check-in/check-out event pairs."""
    periods = []
    check_in_time: datetime | None = None

    for event in tsp_events:
        if event["event_code"] in (EVENT_TSP_CHECK_IN, EVENT_PREEMPTION_CALL_INPUT_ON):
            check_in_time = event["event_time"]
        elif event["event_code"] in (EVENT_TSP_EXTEND_GREEN, EVENT_PREEMPTION_CALL_INPUT_OFF):
            if check_in_time is not None:
                periods.append((check_in_time, event["event_time"]))
                check_in_time = None

    # If check-in without check-out, extend to 2 minutes after check-in
    if check_in_time is not None:
        periods.append((check_in_time, check_in_time + timedelta(minutes=2)))

    return periods


def _is_tsp_active(
    green_start: datetime,
    yellow_start: datetime,
    tsp_periods: list[tuple[datetime, datetime]],
) -> bool:
    """Check if any TSP period overlaps with the green interval."""
    for period_start, period_end in tsp_periods:
        if period_start < yellow_start and period_end > green_start:
            return True
    return False
