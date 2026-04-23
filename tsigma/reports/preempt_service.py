"""
Preempt Service report plugin.

Plan-indexed summary of preemption services granted.  Counts event
code 105 (Preempt Entry Started) per active signal plan interval and
emits a timeline-shaped DataFrame: one row per raw 105 event, with
the active plan number and the total count for that plan broadcast
onto each row.

This report is intentionally lightweight — it does not match
entry/exit pairs (see ``preemption.py`` for that) and does not compute
durations.  It exists to answer "how many preempt services were
serviced under plan N during this window?" for retiming and
enforcement reporting.
"""

import logging
from datetime import datetime

import pandas as pd
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from .registry import Report, ReportMetadata, ReportRegistry
from .sdk import (
    EVENT_PREEMPTION_ENTRY_STARTED,
    fetch_events,
    fetch_plans,
    parse_time,
)

logger = logging.getLogger(__name__)

_COLUMNS = [
    "event_time",
    "event_param",
    "plan_number",
    "plan_start",
    "plan_end",
    "plan_preempt_count",
]

_UNKNOWN_PLAN = "unknown"


class PreemptServiceParams(BaseModel):
    signal_id: str = Field(..., description="Signal identifier")
    start: str = Field(..., description="Analysis window start (ISO-8601)")
    end: str = Field(..., description="Analysis window end (ISO-8601)")


@ReportRegistry.register("preempt-service")
class PreemptServiceReport(Report[PreemptServiceParams]):
    """Plan-indexed counter of Preempt Entry Started (event code 105) events."""

    metadata = ReportMetadata(
        name="preempt-service",
        description=(
            "Plan-indexed count of preempt services granted (event code 105) "
            "with raw event timeline."
        ),
        category="standard",
        estimated_time="fast",
        export_formats=["csv", "json", "ndjson"],
    )

    async def execute(
        self,
        params: PreemptServiceParams,
        session: AsyncSession,
    ) -> pd.DataFrame:
        """
        Execute the preempt-service analysis.

        Returns:
            DataFrame with columns: event_time, event_param, plan_number,
            plan_start, plan_end, plan_preempt_count.  One row per 105
            event.  Empty DataFrame with schema if no events are found.
        """
        signal_id = params.signal_id
        start = parse_time(params.start)
        end = parse_time(params.end)

        logger.info(
            "Running preempt-service for %s from %s to %s",
            signal_id, start, end,
        )

        events_df = await fetch_events(
            signal_id, start, end,
            (EVENT_PREEMPTION_ENTRY_STARTED,),
        )

        if events_df.empty:
            logger.info("Preempt-service complete: 0 events")
            return pd.DataFrame(columns=_COLUMNS)

        plans = await fetch_plans(session, signal_id, start, end)

        rows = [_build_row(row, plans, start, end) for _, row in events_df.iterrows()]
        result_df = pd.DataFrame(rows, columns=_COLUMNS)

        # Broadcast the per-plan count onto every row.
        counts = result_df.groupby("plan_number").size().to_dict()
        result_df["plan_preempt_count"] = result_df["plan_number"].map(counts).astype(int)

        logger.info(
            "Preempt-service complete: %d events across %d plan(s)",
            len(result_df), len(counts),
        )
        return result_df


def _build_row(
    event_row: pd.Series,
    plans: list,
    window_start: datetime,
    window_end: datetime,
) -> dict:
    """Assemble one output row from an event and the plan history.

    When no plan is active at ``event_time``, the row is labeled with the
    ``"unknown"`` sentinel and the plan bounds fall back to the analysis
    window bounds (matches ``preempt_service_request.py``).
    """
    event_time: datetime = event_row["event_time"]
    event_param = int(event_row["event_param"])
    plan = _active_plan_at(plans, event_time)

    if plan is None:
        return {
            "event_time": event_time.isoformat(),
            "event_param": event_param,
            "plan_number": _UNKNOWN_PLAN,
            "plan_start": window_start.isoformat(),
            "plan_end": window_end.isoformat(),
            "plan_preempt_count": 0,
        }

    plan_end = plan.effective_to if plan.effective_to is not None else window_end
    return {
        "event_time": event_time.isoformat(),
        "event_param": event_param,
        "plan_number": str(plan.plan_number),
        "plan_start": plan.effective_from.isoformat(),
        "plan_end": plan_end.isoformat(),
        "plan_preempt_count": 0,
    }


def _active_plan_at(plans: list, moment: datetime):
    """
    Return the plan whose ``[effective_from, effective_to)`` contains moment.

    Boundary convention: an event at ``effective_from`` belongs to the new
    plan; an event at ``effective_to`` belongs to the next plan.  This is
    the standard half-open interval used by ATSPM plan boundaries.
    """
    for plan in plans:
        if plan.effective_from > moment:
            continue
        if plan.effective_to is None or plan.effective_to > moment:
            return plan
    return None
