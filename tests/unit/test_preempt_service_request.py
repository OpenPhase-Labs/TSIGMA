"""
Unit tests for the Preempt Service Request report plugin.

This report analyzes preemption DEMAND (event code 102 —
PreemptCallInputOn) plan-indexed, complementing the Preempt Service
report which analyzes SUPPLY (event code 105, granted services).
"""

from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest

from tsigma.reports.sdk.events import (
    EVENT_PREEMPTION_CALL_INPUT_ON,
    EVENT_PREEMPTION_ENTRY_STARTED,
)

_START_ISO = "2025-06-15T08:00:00"
_END_ISO = "2025-06-15T09:00:00"


def _event(code: int, param: int, time: datetime) -> SimpleNamespace:
    """Build a fake ControllerEventLog row."""
    return SimpleNamespace(
        signal_id="SIG-001",
        event_code=code,
        event_param=param,
        event_time=time,
        device_id=1,
        validation_metadata=None,
    )


def _events_to_df(events: list[SimpleNamespace]) -> pd.DataFrame:
    """Convert list of _event() SimpleNamespace objects to a DataFrame."""
    if not events:
        return pd.DataFrame(columns=["event_code", "event_param", "event_time"])
    return pd.DataFrame([
        {
            "event_code": e.event_code,
            "event_param": e.event_param,
            "event_time": e.event_time,
        }
        for e in events
    ])


def _mock_session():
    """AsyncSession mock — not used when fetch_plans is patched."""
    session = AsyncMock()
    mock_result = MagicMock()
    mock_result.all.return_value = []
    mock_scalars = MagicMock()
    mock_scalars.all.return_value = []
    mock_result.scalars.return_value = mock_scalars
    session.execute = AsyncMock(return_value=mock_result)
    return session


def _plan(plan_number: int, effective_from: datetime, effective_to: datetime | None):
    """Build a SignalPlan-like stub — plan_at only needs these three fields."""
    return SimpleNamespace(
        plan_number=plan_number,
        effective_from=effective_from,
        effective_to=effective_to,
        splits=None,
    )


# =========================================================================
# Preempt Service Request — empty / schema
# =========================================================================


class TestPreemptServiceRequestSchema:

    @pytest.mark.asyncio
    @patch("tsigma.reports.preempt_service_request.fetch_plans", new_callable=AsyncMock)
    @patch("tsigma.reports.preempt_service_request.fetch_events", new_callable=AsyncMock)
    async def test_empty_data_returns_empty_schema(self, mock_fetch, mock_plans):
        """No events at all -> empty DataFrame with correct columns."""
        from tsigma.reports.preempt_service_request import (
            PreemptServiceRequestParams,
            PreemptServiceRequestReport,
        )

        mock_fetch.return_value = _events_to_df([])
        mock_plans.return_value = []

        report = PreemptServiceRequestReport()
        params = PreemptServiceRequestParams(
            signal_id="SIG-001", start=_START_ISO, end=_END_ISO,
        )
        result = await report.execute(params, _mock_session())

        assert isinstance(result, pd.DataFrame)
        assert result.empty
        expected_cols = {
            "event_time", "event_param", "plan_number",
            "plan_start", "plan_end", "plan_request_count",
        }
        assert expected_cols.issubset(set(result.columns))


# =========================================================================
# Preempt Service Request — with data
# =========================================================================


class TestPreemptServiceRequestWithData:

    @pytest.mark.asyncio
    @patch("tsigma.reports.preempt_service_request.fetch_plans", new_callable=AsyncMock)
    @patch("tsigma.reports.preempt_service_request.fetch_events", new_callable=AsyncMock)
    async def test_single_request_single_plan(self, mock_fetch, mock_plans):
        """One 102 event inside one plan -> one row, plan_request_count=1."""
        from tsigma.reports.preempt_service_request import (
            PreemptServiceRequestParams,
            PreemptServiceRequestReport,
        )

        t0 = datetime(2025, 6, 15, 8, 0, 0)
        plan_from = datetime(2025, 6, 15, 7, 0, 0)
        plan_to = datetime(2025, 6, 15, 10, 0, 0)
        mock_plans.return_value = [_plan(3, plan_from, plan_to)]

        events = [_event(EVENT_PREEMPTION_CALL_INPUT_ON, 2, t0 + timedelta(seconds=30))]
        mock_fetch.return_value = _events_to_df(events)

        report = PreemptServiceRequestReport()
        params = PreemptServiceRequestParams(
            signal_id="SIG-001", start=_START_ISO, end=_END_ISO,
        )
        result = await report.execute(params, _mock_session())

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        row = result.iloc[0]
        assert row["event_param"] == 2
        assert str(row["plan_number"]) == "3"
        assert row["plan_request_count"] == 1
        assert row["plan_start"] == plan_from.isoformat()
        assert row["plan_end"] == plan_to.isoformat()

    @pytest.mark.asyncio
    @patch("tsigma.reports.preempt_service_request.fetch_plans", new_callable=AsyncMock)
    @patch("tsigma.reports.preempt_service_request.fetch_events", new_callable=AsyncMock)
    async def test_multiple_requests_across_plans(self, mock_fetch, mock_plans):
        """Requests spread across two plans -> correct count per plan."""
        from tsigma.reports.preempt_service_request import (
            PreemptServiceRequestParams,
            PreemptServiceRequestReport,
        )

        # Plan 1: 07:30 - 08:30, Plan 2: 08:30 - 09:30
        plan1_from = datetime(2025, 6, 15, 7, 30, 0)
        plan1_to = datetime(2025, 6, 15, 8, 30, 0)
        plan2_from = plan1_to
        plan2_to = datetime(2025, 6, 15, 9, 30, 0)
        mock_plans.return_value = [
            _plan(1, plan1_from, plan1_to),
            _plan(2, plan2_from, plan2_to),
        ]

        # Two requests in plan 1 (before 08:30), three in plan 2 (after)
        base = datetime(2025, 6, 15, 8, 0, 0)
        events = [
            _event(EVENT_PREEMPTION_CALL_INPUT_ON, 1, base + timedelta(minutes=5)),
            _event(EVENT_PREEMPTION_CALL_INPUT_ON, 1, base + timedelta(minutes=15)),
            _event(EVENT_PREEMPTION_CALL_INPUT_ON, 1, base + timedelta(minutes=35)),
            _event(EVENT_PREEMPTION_CALL_INPUT_ON, 1, base + timedelta(minutes=45)),
            _event(EVENT_PREEMPTION_CALL_INPUT_ON, 1, base + timedelta(minutes=55)),
        ]
        mock_fetch.return_value = _events_to_df(events)

        report = PreemptServiceRequestReport()
        params = PreemptServiceRequestParams(
            signal_id="SIG-001", start=_START_ISO, end=_END_ISO,
        )
        result = await report.execute(params, _mock_session())

        assert len(result) == 5

        plan1_rows = result[result["plan_number"].astype(str) == "1"]
        plan2_rows = result[result["plan_number"].astype(str) == "2"]
        assert len(plan1_rows) == 2
        assert len(plan2_rows) == 3
        assert (plan1_rows["plan_request_count"] == 2).all()
        assert (plan2_rows["plan_request_count"] == 3).all()

    @pytest.mark.asyncio
    @patch("tsigma.reports.preempt_service_request.fetch_plans", new_callable=AsyncMock)
    @patch("tsigma.reports.preempt_service_request.fetch_events", new_callable=AsyncMock)
    async def test_event_param_preserved(self, mock_fetch, mock_plans):
        """event_param (preempt channel) is preserved per-row."""
        from tsigma.reports.preempt_service_request import (
            PreemptServiceRequestParams,
            PreemptServiceRequestReport,
        )

        plan_from = datetime(2025, 6, 15, 7, 0, 0)
        plan_to = datetime(2025, 6, 15, 10, 0, 0)
        mock_plans.return_value = [_plan(5, plan_from, plan_to)]

        t0 = datetime(2025, 6, 15, 8, 15, 0)
        events = [
            _event(EVENT_PREEMPTION_CALL_INPUT_ON, 1, t0),
            _event(EVENT_PREEMPTION_CALL_INPUT_ON, 4, t0 + timedelta(seconds=10)),
            _event(EVENT_PREEMPTION_CALL_INPUT_ON, 2, t0 + timedelta(seconds=20)),
        ]
        mock_fetch.return_value = _events_to_df(events)

        report = PreemptServiceRequestReport()
        params = PreemptServiceRequestParams(
            signal_id="SIG-001", start=_START_ISO, end=_END_ISO,
        )
        result = await report.execute(params, _mock_session())

        assert len(result) == 3
        # Event params preserved (in event-time order)
        assert list(result["event_param"]) == [1, 4, 2]
        # All three attributed to plan 5 with count=3
        assert (result["plan_number"].astype(str) == "5").all()
        assert (result["plan_request_count"] == 3).all()

    @pytest.mark.asyncio
    @patch("tsigma.reports.preempt_service_request.fetch_plans", new_callable=AsyncMock)
    @patch("tsigma.reports.preempt_service_request.fetch_events", new_callable=AsyncMock)
    async def test_event_time_iso_format(self, mock_fetch, mock_plans):
        """event_time column is ISO-8601 string."""
        from tsigma.reports.preempt_service_request import (
            PreemptServiceRequestParams,
            PreemptServiceRequestReport,
        )

        plan_from = datetime(2025, 6, 15, 7, 0, 0)
        mock_plans.return_value = [_plan(1, plan_from, None)]

        event_time = datetime(2025, 6, 15, 8, 12, 34)
        mock_fetch.return_value = _events_to_df([
            _event(EVENT_PREEMPTION_CALL_INPUT_ON, 1, event_time),
        ])

        report = PreemptServiceRequestReport()
        params = PreemptServiceRequestParams(
            signal_id="SIG-001", start=_START_ISO, end=_END_ISO,
        )
        result = await report.execute(params, _mock_session())

        assert len(result) == 1
        assert result.iloc[0]["event_time"] == event_time.isoformat()

    @pytest.mark.asyncio
    @patch("tsigma.reports.preempt_service_request.fetch_plans", new_callable=AsyncMock)
    @patch("tsigma.reports.preempt_service_request.fetch_events", new_callable=AsyncMock)
    async def test_only_102_events_fetched(self, mock_fetch, mock_plans):
        """
        Guard: we call fetch_events with EVENT_PREEMPTION_CALL_INPUT_ON
        only, NOT EVENT_PREEMPTION_ENTRY_STARTED (the Preempt Service
        event code). Verifies isolation from the sibling report.
        """
        from tsigma.reports.preempt_service_request import (
            PreemptServiceRequestParams,
            PreemptServiceRequestReport,
        )

        mock_plans.return_value = []
        mock_fetch.return_value = _events_to_df([])

        report = PreemptServiceRequestReport()
        params = PreemptServiceRequestParams(
            signal_id="SIG-001", start=_START_ISO, end=_END_ISO,
        )
        await report.execute(params, _mock_session())

        assert mock_fetch.call_count == 1
        call_args = mock_fetch.call_args
        # The event_codes tuple is the 4th positional arg or a keyword arg.
        # Inspect both possibilities robustly.
        event_codes_arg = call_args.args[3] if len(call_args.args) >= 4 else call_args.kwargs.get("event_codes")
        codes = tuple(event_codes_arg)
        assert EVENT_PREEMPTION_CALL_INPUT_ON in codes
        assert EVENT_PREEMPTION_ENTRY_STARTED not in codes

    @pytest.mark.asyncio
    @patch("tsigma.reports.preempt_service_request.fetch_plans", new_callable=AsyncMock)
    @patch("tsigma.reports.preempt_service_request.fetch_events", new_callable=AsyncMock)
    async def test_code_105_not_counted_when_fed_in(self, mock_fetch, mock_plans):
        """
        Even if an EVENT_PREEMPTION_ENTRY_STARTED (105) row somehow leaks
        into the fetched frame, the report must ignore it — only 102
        contributes to plan_request_count and to rows emitted.
        """
        from tsigma.reports.preempt_service_request import (
            PreemptServiceRequestParams,
            PreemptServiceRequestReport,
        )

        plan_from = datetime(2025, 6, 15, 7, 0, 0)
        plan_to = datetime(2025, 6, 15, 10, 0, 0)
        mock_plans.return_value = [_plan(7, plan_from, plan_to)]

        t0 = datetime(2025, 6, 15, 8, 10, 0)
        events = [
            _event(EVENT_PREEMPTION_CALL_INPUT_ON, 1, t0),
            _event(EVENT_PREEMPTION_ENTRY_STARTED, 1, t0 + timedelta(seconds=2)),
            _event(EVENT_PREEMPTION_CALL_INPUT_ON, 1, t0 + timedelta(seconds=30)),
            _event(EVENT_PREEMPTION_ENTRY_STARTED, 1, t0 + timedelta(seconds=32)),
        ]
        mock_fetch.return_value = _events_to_df(events)

        report = PreemptServiceRequestReport()
        params = PreemptServiceRequestParams(
            signal_id="SIG-001", start=_START_ISO, end=_END_ISO,
        )
        result = await report.execute(params, _mock_session())

        # Only the two 102 events produce rows — 105 events are ignored
        assert len(result) == 2
        assert (result["plan_request_count"] == 2).all()

    @pytest.mark.asyncio
    @patch("tsigma.reports.preempt_service_request.fetch_plans", new_callable=AsyncMock)
    @patch("tsigma.reports.preempt_service_request.fetch_events", new_callable=AsyncMock)
    async def test_no_plans_falls_back_to_window(self, mock_fetch, mock_plans):
        """With no plans known, requests still appear — attributed to a sentinel plan."""
        from tsigma.reports.preempt_service_request import (
            PreemptServiceRequestParams,
            PreemptServiceRequestReport,
        )

        mock_plans.return_value = []
        t0 = datetime(2025, 6, 15, 8, 20, 0)
        mock_fetch.return_value = _events_to_df([
            _event(EVENT_PREEMPTION_CALL_INPUT_ON, 1, t0),
            _event(EVENT_PREEMPTION_CALL_INPUT_ON, 2, t0 + timedelta(seconds=10)),
        ])

        report = PreemptServiceRequestReport()
        params = PreemptServiceRequestParams(
            signal_id="SIG-001", start=_START_ISO, end=_END_ISO,
        )
        result = await report.execute(params, _mock_session())

        assert len(result) == 2
        # All events attributed together (one sentinel bucket)
        assert (result["plan_request_count"] == 2).all()

    @pytest.mark.asyncio
    async def test_report_registered(self):
        """The report is auto-discovered and registered under its name."""
        # Force auto-discovery
        from tsigma.reports.registry import ReportRegistry

        assert "preempt-service-request" in ReportRegistry.list_all()
