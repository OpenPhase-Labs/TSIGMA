"""
Preempt Service Request report plugin.

Analyzes preemption DEMAND (event code 102 — ``PreemptCallInputOn``)
indexed by the active signal timing plan.  This is the sibling of the
Preempt Service report (event code 105, SUPPLY / granted services): the
classic demand-vs-supply split.

Each request event produces one row with plan attribution and a
broadcast plan-level request count, so downstream consumers can read
either the timeline or the plan-level aggregate from the same frame.
"""

import logging
from collections import Counter
from datetime import datetime
from typing import Any

import pandas as pd
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from ..models import SignalPlan
from .registry import Report, ReportMetadata, ReportRegistry
from .sdk import (
    EVENT_PREEMPTION_CALL_INPUT_ON,
    fetch_events,
    fetch_plans,
    parse_time,
    plan_at,
)

logger = logging.getLogger(__name__)

_OUTPUT_COLUMNS = [
    "event_time",
    "event_param",
    "plan_number",
    "plan_start",
    "plan_end",
    "plan_request_count",
]

_UNKNOWN_PLAN = "unknown"


class PreemptServiceRequestParams(BaseModel):
    signal_id: str = Field(..., description="Signal identifier")
    start: str = Field(..., description="Analysis window start (ISO-8601)")
    end: str = Field(..., description="Analysis window end (ISO-8601)")


def _plan_key(
    plan: SignalPlan | None, window_start: datetime, window_end: datetime,
) -> tuple[str, str, str]:
    """
    Build the (plan_number, plan_start, plan_end) triple for a row.

    When no plan is active, fall back to the analysis window bounds
    under the sentinel plan label ``"unknown"``.
    """
    if plan is None:
        return (_UNKNOWN_PLAN, window_start.isoformat(), window_end.isoformat())

    plan_end = plan.effective_to if plan.effective_to is not None else window_end
    return (str(plan.plan_number), plan.effective_from.isoformat(), plan_end.isoformat())


@ReportRegistry.register("preempt-service-request")
class PreemptServiceRequestReport(Report[PreemptServiceRequestParams]):
    """Preempt service REQUESTS (demand), indexed by timing plan."""

    metadata = ReportMetadata(
        name="preempt-service-request",
        description=(
            "Preemption request (demand) events attributed to the active "
            "signal timing plan, with a broadcast per-plan request count."
        ),
        category="standard",
        estimated_time="fast",
        export_formats=["csv", "json", "ndjson"],
    )

    async def execute(
        self, params: PreemptServiceRequestParams, session: AsyncSession,
    ) -> pd.DataFrame:
        """
        Execute preempt-service-request analysis.

        Returns:
            DataFrame with one row per event code 102 occurrence, columns:
            event_time, event_param, plan_number, plan_start, plan_end,
            plan_request_count.
        """
        signal_id = params.signal_id
        start = parse_time(params.start)
        end = parse_time(params.end)

        logger.info(
            "Running preempt-service-request for %s from %s to %s",
            signal_id, start, end,
        )

        df = await fetch_events(
            signal_id, start, end, (EVENT_PREEMPTION_CALL_INPUT_ON,),
        )
        plans = await fetch_plans(session, signal_id, start, end)

        if df.empty:
            logger.info("Preempt-service-request complete: 0 events")
            return pd.DataFrame(columns=_OUTPUT_COLUMNS)

        # First pass: attribute each 102 event to its plan, tally counts.
        prelim: list[dict[str, Any]] = []
        plan_counts: Counter[str] = Counter()

        for _, row in df.iterrows():
            code = int(row["event_code"])
            if code != EVENT_PREEMPTION_CALL_INPUT_ON:
                continue

            event_time = row["event_time"]
            active_plan = plan_at(plans, event_time)
            plan_number, plan_start, plan_end = _plan_key(active_plan, start, end)

            plan_counts[plan_number] += 1
            prelim.append({
                "event_time": event_time.isoformat(),
                "event_param": int(row["event_param"]),
                "plan_number": plan_number,
                "plan_start": plan_start,
                "plan_end": plan_end,
            })

        if not prelim:
            logger.info("Preempt-service-request complete: 0 events")
            return pd.DataFrame(columns=_OUTPUT_COLUMNS)

        # Second pass: broadcast the per-plan count onto each row.
        for record in prelim:
            record["plan_request_count"] = plan_counts[record["plan_number"]]

        result_df = pd.DataFrame(prelim, columns=_OUTPUT_COLUMNS)
        logger.info(
            "Preempt-service-request complete: %d events across %d plan(s)",
            len(result_df), len(plan_counts),
        )
        return result_df
