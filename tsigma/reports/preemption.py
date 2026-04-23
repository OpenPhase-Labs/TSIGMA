"""
Preemption report plugin.

Analyzes preemption events (typically emergency vehicle preemption) at a
signal, returning individual event start/end pairs with durations.
Summary statistics (count, avg, max) are derivable by the consumer.
"""

import logging
from datetime import datetime
from typing import Any

import pandas as pd
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from .registry import Report, ReportMetadata, ReportRegistry
from .sdk import (
    EVENT_PREEMPTION_CALL_INPUT_OFF,
    EVENT_PREEMPTION_CALL_INPUT_ON,
    fetch_events,
    parse_time,
)

logger = logging.getLogger(__name__)


class PreemptionParams(BaseModel):
    signal_id: str = Field(..., description="Signal identifier")
    start: str = Field(..., description="Analysis window start (ISO-8601)")
    end: str = Field(..., description="Analysis window end (ISO-8601)")


@ReportRegistry.register("preemption")
class PreemptionReport(Report[PreemptionParams]):
    """Preemption event pairs with durations."""

    metadata = ReportMetadata(
        name="preemption",
        description="Preemption event start/end pairs with duration in seconds.",
        category="standard",
        estimated_time="fast",
        export_formats=["csv", "json", "ndjson"],
    )

    async def execute(self, params: PreemptionParams, session: AsyncSession) -> pd.DataFrame:
        """
        Execute preemption analysis.

        Returns:
            DataFrame with columns: channel, start, end, duration_seconds.
        """
        signal_id = params.signal_id
        start = parse_time(params.start)
        end = parse_time(params.end)

        logger.info("Running preemption for %s from %s to %s", signal_id, start, end)

        df = await fetch_events(
            signal_id, start, end,
            (EVENT_PREEMPTION_CALL_INPUT_ON, EVENT_PREEMPTION_CALL_INPUT_OFF),
        )

        # Match entry/exit pairs by preemption channel (event_param)
        pending_entries: dict[int, datetime] = {}
        matched_events: list[dict[str, Any]] = []

        for _, row in df.iterrows():
            code = int(row["event_code"])
            channel = int(row["event_param"])
            event_time = row["event_time"]

            if code == EVENT_PREEMPTION_CALL_INPUT_ON:
                pending_entries[channel] = event_time

            elif code == EVENT_PREEMPTION_CALL_INPUT_OFF:
                entry_time = pending_entries.pop(channel, None)
                if entry_time is not None:
                    duration = (event_time - entry_time).total_seconds()
                    matched_events.append({
                        "channel": channel,
                        "start": entry_time.isoformat(),
                        "end": event_time.isoformat(),
                        "duration_seconds": round(duration, 2),
                    })

        if not matched_events:
            logger.info("Preemption complete: 0 events")
            return pd.DataFrame(columns=["channel", "start", "end", "duration_seconds"])

        result_df = pd.DataFrame(matched_events)

        logger.info("Preemption complete: %d events", len(result_df))
        return result_df
