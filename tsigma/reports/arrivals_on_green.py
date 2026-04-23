"""
Arrivals on Green report plugin.

Calculates the percentage of vehicle arrivals that occur during the green
phase for each phase at a signal. A key measure of coordination quality.

Uses pre-computed cycle_detector_arrival tables for historical queries
(fast). Falls back to raw event processing via db_facade.get_dataframe()
if aggregate tables are empty, with pandas-based aggregation.
"""

import logging
from typing import Optional

import pandas as pd
from pydantic import BaseModel, Field
from sqlalchemy import and_, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from ..database.db import db_facade
from ..models.event import ControllerEventLog as CEL
from .registry import Report, ReportMetadata, ReportRegistry
from .sdk import (
    EVENT_DETECTOR_ON,
    EVENT_PHASE_GREEN,
    EVENT_YELLOW_CLEARANCE,
    fetch_cycle_arrivals,
    load_channel_to_phase,
    parse_time,
)

logger = logging.getLogger(__name__)

_EMPTY_COLS = ["phase_number", "total_arrivals", "arrivals_on_green", "aog_percentage"]


class ArrivalsOnGreenParams(BaseModel):
    """Parameters for arrivals on green analysis."""

    signal_id: str = Field(..., description="Signal identifier")
    start: str = Field(..., description="Analysis window start (ISO-8601)")
    end: str = Field(..., description="Analysis window end (ISO-8601)")
    phase_number: Optional[int] = Field(None, description="Filter to a single phase")


@ReportRegistry.register("arrivals-on-green")
class ArrivalsOnGreenReport(Report[ArrivalsOnGreenParams]):
    """Percentage of detector activations occurring during green phase."""

    metadata = ReportMetadata(
        name="arrivals-on-green",
        description="Arrivals on green percentage per phase — a coordination quality metric.",
        category="standard",
        estimated_time="fast",
        export_formats=["csv", "json", "ndjson"],
    )

    async def execute(
        self, params: ArrivalsOnGreenParams, session: AsyncSession
    ) -> pd.DataFrame:
        signal_id = params.signal_id
        start = parse_time(params.start)
        end = parse_time(params.end)
        phase_filter = params.phase_number

        logger.info("Running arrivals-on-green for %s from %s to %s", signal_id, start, end)

        # Try aggregate tables first (fast path) -- requires phase_number
        if phase_filter is not None:
            result_df = await self._from_aggregates(
                signal_id, phase_filter, start, end
            )
            if not result_df.empty:
                logger.info(
                    "Arrivals-on-green from aggregates: %d phases", len(result_df)
                )
                return result_df

        # Fall back to raw events (slow path -- real-time or no aggregates)
        logger.info("No aggregate data -- falling back to raw events")
        return await self._from_raw_events(
            session, signal_id, start, end, phase_filter
        )

    async def _from_aggregates(
        self,
        signal_id: str,
        phase: int,
        start,
        end,
    ) -> pd.DataFrame:
        """
        Build arrivals-on-green data from pre-computed
        cycle_detector_arrival table. Uses pandas for aggregation.
        """
        arr_df = await fetch_cycle_arrivals(signal_id, phase, start, end)

        if arr_df.empty:
            return pd.DataFrame(columns=_EMPTY_COLS)

        total = len(arr_df)
        on_green = int((arr_df["phase_state"] == "green").sum())

        return pd.DataFrame([{
            "phase_number": phase,
            "total_arrivals": total,
            "arrivals_on_green": on_green,
            "aog_percentage": round(on_green / total * 100, 1) if total > 0 else 0.0,
        }])

    async def _from_raw_events(
        self,
        session: AsyncSession,
        signal_id: str,
        start,
        end,
        phase_filter: int | None,
    ) -> pd.DataFrame:
        """
        Build arrivals-on-green data from raw ControllerEventLog events
        via db_facade.get_dataframe().
        """
        channel_to_phase = await load_channel_to_phase(session, signal_id, start)
        if not channel_to_phase:
            return pd.DataFrame(columns=_EMPTY_COLS)

        target_phases = set(channel_to_phase.values())
        if phase_filter is not None:
            target_phases = {phase_filter} & target_phases
        if not target_phases:
            return pd.DataFrame(columns=_EMPTY_COLS)

        df = await self._fetch_raw_events(signal_id, start, end, channel_to_phase)
        if df.empty:
            return pd.DataFrame(columns=_EMPTY_COLS)

        arrival_rows = _classify_arrivals(df, target_phases, channel_to_phase)
        return _aggregate_aog(arrival_rows, target_phases)

    async def _fetch_raw_events(
        self, signal_id: str, start, end, channel_to_phase: dict[int, int],
    ) -> pd.DataFrame:
        """Fetch phase + detector events via db_facade."""
        det_channels = list(channel_to_phase.keys())

        stmt = (
            select(CEL.event_code, CEL.event_param, CEL.event_time)
            .where(
                CEL.signal_id == signal_id,
                CEL.event_time >= start,
                CEL.event_time <= end,
                or_(
                    CEL.event_code.in_([EVENT_PHASE_GREEN, EVENT_YELLOW_CLEARANCE]),
                    and_(
                        CEL.event_code.in_([EVENT_DETECTOR_ON]),
                        CEL.event_param.in_(det_channels),
                    ),
                ),
            )
            .order_by(CEL.event_time)
        )

        return await db_facade.get_dataframe(stmt)


def _classify_arrivals(
    df: pd.DataFrame,
    target_phases: set[int],
    channel_to_phase: dict[int, int],
) -> list[dict]:
    """Sequential pass: track green state per phase, classify detector arrivals."""
    phase_is_green = dict.fromkeys(target_phases, False)
    arrival_rows: list[dict] = []

    for _, row in df.iterrows():
        code = int(row["event_code"])
        param = int(row["event_param"])

        if code == EVENT_PHASE_GREEN and param in target_phases:
            phase_is_green[param] = True
        elif code == EVENT_YELLOW_CLEARANCE and param in target_phases:
            phase_is_green[param] = False
        elif code == EVENT_DETECTOR_ON:
            phase = channel_to_phase.get(param)
            if phase is not None and phase in target_phases:
                arrival_rows.append({
                    "phase_number": phase,
                    "on_green": phase_is_green.get(phase, False),
                })

    return arrival_rows


def _zero_phase_row(phase_num: int) -> dict:
    return {
        "phase_number": phase_num,
        "total_arrivals": 0,
        "arrivals_on_green": 0,
        "aog_percentage": 0.0,
    }


def _aggregate_aog(
    arrival_rows: list[dict], target_phases: set[int],
) -> pd.DataFrame:
    """Aggregate arrival classifications into per-phase AOG stats using pandas."""
    if not arrival_rows:
        return pd.DataFrame([_zero_phase_row(p) for p in sorted(target_phases)])

    arr_df = pd.DataFrame(arrival_rows)
    grouped = arr_df.groupby("phase_number").agg(
        total_arrivals=("on_green", "count"),
        arrivals_on_green=("on_green", "sum"),
    ).reset_index()

    grouped["arrivals_on_green"] = grouped["arrivals_on_green"].astype(int)
    grouped["aog_percentage"] = (
        grouped["arrivals_on_green"] / grouped["total_arrivals"] * 100
    ).round(1).fillna(0.0)

    result_map = {int(r["phase_number"]): r for _, r in grouped.iterrows()}

    rows = []
    for phase_num in sorted(target_phases):
        if phase_num in result_map:
            r = result_map[phase_num]
            rows.append({
                "phase_number": int(r["phase_number"]),
                "total_arrivals": int(r["total_arrivals"]),
                "arrivals_on_green": int(r["arrivals_on_green"]),
                "aog_percentage": float(r["aog_percentage"]),
            })
        else:
            rows.append(_zero_phase_row(phase_num))

    logger.info("Arrivals-on-green complete: %d phases", len(rows))
    return pd.DataFrame(rows)
