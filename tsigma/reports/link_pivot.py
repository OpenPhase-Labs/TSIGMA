"""
Link Pivot report plugin.

Corridor-level coordination analysis across multiple signals in a route.
Calculates cycle-by-cycle offsets between adjacent signals based on green
start times for the coordinated phase in the specified direction.
"""

import logging
from collections import defaultdict
from datetime import datetime

import pandas as pd
from pydantic import BaseModel, Field
from sqlalchemy import and_, select
from sqlalchemy.ext.asyncio import AsyncSession

from ..models import Route, RoutePhase, RouteSignal
from .registry import Report, ReportMetadata, ReportRegistry
from .sdk import EVENT_PHASE_GREEN, fetch_events, parse_time

logger = logging.getLogger(__name__)


class LinkPivotParams(BaseModel):
    route_id: str = Field(..., description="Route identifier")
    start: str = Field(..., description="Analysis window start (ISO-8601)")
    end: str = Field(..., description="Analysis window end (ISO-8601)")
    direction: int = Field(..., description="Direction type (1-4)")


@ReportRegistry.register("link-pivot")
class LinkPivotReport(Report[LinkPivotParams]):
    """Corridor coordination analysis with cycle-by-cycle offset computation."""

    metadata = ReportMetadata(
        name="link-pivot",
        description="Offset analysis between adjacent signals in a coordinated route.",
        category="detailed",
        estimated_time="slow",
        export_formats=["csv", "json", "ndjson"],
    )

    async def execute(self, params: LinkPivotParams, session: AsyncSession) -> pd.DataFrame:
        """
        Execute link pivot analysis.

        Returns:
            DataFrame with columns: from_signal, to_signal, avg_offset,
            stddev_offset, sample_count.
        """
        route_id = params.route_id
        start = parse_time(params.start)
        end = parse_time(params.end)
        direction = params.direction

        empty_df = pd.DataFrame(columns=[
            "from_signal", "to_signal", "avg_offset", "stddev_offset", "sample_count",
        ])

        logger.info(
            "Running link-pivot for route %s direction %d from %s to %s",
            route_id, direction, start, end,
        )

        # Get route name
        route_query = select(Route.name).where(Route.route_id == route_id)
        route_result = await session.execute(route_query)
        route_row = route_result.one_or_none()
        if route_row is None:
            logger.warning("Route %s not found", route_id)
            return empty_df

        # Get signals in route order with their coordinated phase for this direction
        signals_query = (
            select(
                RouteSignal.signal_id,
                RouteSignal.sequence_order,
                RoutePhase.phase_number,
            )
            .join(RoutePhase, RoutePhase.route_signal_id == RouteSignal.route_signal_id)
            .where(
                and_(
                    RouteSignal.route_id == route_id,
                    RoutePhase.direction_type_id == direction,
                )
            )
            .order_by(RouteSignal.sequence_order)
        )
        signals_result = await session.execute(signals_query)
        signal_rows = signals_result.all()

        if not signal_rows:
            logger.info("No signals found for route %s direction %d", route_id, direction)
            return empty_df

        # Build ordered signal list
        signals = []
        for signal_id, order, phase_number in signal_rows:
            signals.append({
                "signal_id": signal_id,
                "order": order,
                "phase_number": phase_number,
            })

        # For each signal, get green start times for the coordinated phase
        green_starts_by_signal: dict[str, list[datetime]] = defaultdict(list)

        for sig in signals:
            df = await fetch_events(
                sig["signal_id"], start, end,
                [EVENT_PHASE_GREEN],
                event_param_in=[sig["phase_number"]],
            )
            green_starts_by_signal[sig["signal_id"]] = df["event_time"].tolist()

        # Calculate offsets between adjacent signal pairs
        offset_rows = []
        for i in range(len(signals) - 1):
            from_sig = signals[i]
            to_sig = signals[i + 1]

            from_greens = green_starts_by_signal[from_sig["signal_id"]]
            to_greens = green_starts_by_signal[to_sig["signal_id"]]

            if not from_greens or not to_greens:
                continue

            # Match cycles: for each green at from_signal, find nearest green at to_signal
            pair_offsets = _compute_pair_offsets(from_greens, to_greens)

            if not pair_offsets:
                continue

            s = pd.Series(pair_offsets)
            offset_rows.append({
                "from_signal": from_sig["signal_id"],
                "to_signal": to_sig["signal_id"],
                "avg_offset": round(float(s.mean()), 2),
                "stddev_offset": round(float(s.std(ddof=0)), 2),
                "sample_count": len(pair_offsets),
            })

        if not offset_rows:
            logger.info("Link-pivot complete: no signal pairs with data")
            return empty_df

        result_df = pd.DataFrame(offset_rows)
        logger.info("Link-pivot complete: %d signal pairs analyzed", len(result_df))
        return result_df


def _compute_pair_offsets(from_greens: list[datetime], to_greens: list[datetime]) -> list[float]:
    """
    Compute cycle-by-cycle offsets between two signals.

    For each green at the upstream signal, find the nearest subsequent green
    at the downstream signal and calculate the time difference.

    Args:
        from_greens: Sorted green start times at upstream signal.
        to_greens: Sorted green start times at downstream signal.

    Returns:
        List of offset values in seconds.
    """
    offsets = []
    to_idx = 0

    for from_time in from_greens:
        # Advance to_idx to the first green at or after from_time
        while to_idx < len(to_greens) and to_greens[to_idx] < from_time:
            to_idx += 1

        if to_idx >= len(to_greens):
            break

        offset = (to_greens[to_idx] - from_time).total_seconds()

        # Only include reasonable offsets (within 5 minutes = one cycle max)
        if 0 <= offset <= 300:
            offsets.append(offset)

    return offsets
