"""
Unit tests for the Preempt Service report plugin.

Verifies that plan-indexed preempt service counts are computed from
event code 105 (Preempt Entry Started) events against the SignalPlan
activation history.
"""

from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest

from tsigma.reports.sdk.events import EVENT_PREEMPTION_ENTRY_STARTED

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _event(code: int, param: int, time: datetime) -> SimpleNamespace:
    """Build a fake ControllerEventLog row."""
    return SimpleNamespace(
        signal_id="SIG-001",
        event_code=code,
        event_param=param,
        event_time=time,
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


def _make_plan(effective_from, effective_to=None, plan_number=1):
    """Build a fake SignalPlan object."""
    plan = type("FakePlan", (), {})()
    plan.signal_id = "SIG-001"
    plan.effective_from = effective_from
    plan.effective_to = effective_to
    plan.plan_number = plan_number
    plan.cycle_length = 120
    plan.offset = 0
    plan.splits = {}
    return plan


def _mock_session():
    """AsyncSession mock."""
    session = AsyncMock()
    mock_result = MagicMock()
    mock_result.all.return_value = []
    mock_scalars = MagicMock()
    mock_scalars.all.return_value = []
    mock_result.scalars.return_value = mock_scalars
    session.execute = AsyncMock(return_value=mock_result)
    return session


_START_ISO = "2025-06-15T08:00:00"
_END_ISO = "2025-06-15T09:00:00"

_EXPECTED_COLUMNS = [
    "event_time",
    "event_param",
    "plan_number",
    "plan_start",
    "plan_end",
    "plan_preempt_count",
]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestPreemptServiceWithData:

    @pytest.mark.asyncio
    @patch("tsigma.reports.preempt_service.fetch_plans", new_callable=AsyncMock)
    @patch("tsigma.reports.preempt_service.fetch_events", new_callable=AsyncMock)
    async def test_empty_events_returns_empty_with_schema(self, mock_fetch, mock_plans):
        """No 105 events -> empty DataFrame carrying the full schema."""
        from tsigma.reports.preempt_service import PreemptServiceParams, PreemptServiceReport

        mock_fetch.return_value = _events_to_df([])
        mock_plans.return_value = []

        report = PreemptServiceReport()
        params = PreemptServiceParams(signal_id="SIG-001", start=_START_ISO, end=_END_ISO)
        result = await report.execute(params, _mock_session())

        assert isinstance(result, pd.DataFrame)
        assert list(result.columns) == _EXPECTED_COLUMNS
        assert len(result) == 0

    @pytest.mark.asyncio
    @patch("tsigma.reports.preempt_service.fetch_plans", new_callable=AsyncMock)
    @patch("tsigma.reports.preempt_service.fetch_events", new_callable=AsyncMock)
    async def test_single_event_in_plan(self, mock_fetch, mock_plans):
        """One 105 event during an active plan -> one row with count 1."""
        from tsigma.reports.preempt_service import PreemptServiceParams, PreemptServiceReport

        t_plan = datetime(2025, 6, 15, 7, 0, 0)
        t_plan_end = datetime(2025, 6, 15, 10, 0, 0)
        t_event = datetime(2025, 6, 15, 8, 15, 0)

        mock_fetch.return_value = _events_to_df([
            _event(EVENT_PREEMPTION_ENTRY_STARTED, 3, t_event),
        ])
        mock_plans.return_value = [
            _make_plan(t_plan, t_plan_end, plan_number=2),
        ]

        report = PreemptServiceReport()
        params = PreemptServiceParams(signal_id="SIG-001", start=_START_ISO, end=_END_ISO)
        result = await report.execute(params, _mock_session())

        assert list(result.columns) == _EXPECTED_COLUMNS
        assert len(result) == 1
        row = result.iloc[0]
        assert row["event_param"] == 3
        assert row["plan_number"] == "2"
        assert row["plan_preempt_count"] == 1
        assert row["event_time"] == t_event.isoformat()
        assert row["plan_start"] == t_plan.isoformat()
        assert row["plan_end"] == t_plan_end.isoformat()

    @pytest.mark.asyncio
    @patch("tsigma.reports.preempt_service.fetch_plans", new_callable=AsyncMock)
    @patch("tsigma.reports.preempt_service.fetch_events", new_callable=AsyncMock)
    async def test_multiple_events_across_two_plans(self, mock_fetch, mock_plans):
        """Events distributed across two plans get counted per plan."""
        from tsigma.reports.preempt_service import PreemptServiceParams, PreemptServiceReport

        t_plan_a_start = datetime(2025, 6, 15, 7, 0, 0)
        t_plan_b_start = datetime(2025, 6, 15, 8, 30, 0)
        t_plan_b_end = datetime(2025, 6, 15, 10, 0, 0)

        # Two events in plan A (7:00-8:30), one event in plan B (8:30-10:00).
        t_a1 = datetime(2025, 6, 15, 8, 0, 0)
        t_a2 = datetime(2025, 6, 15, 8, 10, 0)
        t_b1 = datetime(2025, 6, 15, 8, 45, 0)

        mock_fetch.return_value = _events_to_df([
            _event(EVENT_PREEMPTION_ENTRY_STARTED, 1, t_a1),
            _event(EVENT_PREEMPTION_ENTRY_STARTED, 1, t_a2),
            _event(EVENT_PREEMPTION_ENTRY_STARTED, 2, t_b1),
        ])
        mock_plans.return_value = [
            _make_plan(t_plan_a_start, t_plan_b_start, plan_number=1),
            _make_plan(t_plan_b_start, t_plan_b_end, plan_number=2),
        ]

        report = PreemptServiceReport()
        params = PreemptServiceParams(signal_id="SIG-001", start=_START_ISO, end=_END_ISO)
        result = await report.execute(params, _mock_session())

        assert len(result) == 3

        plan_a_rows = result[result["plan_number"] == "1"]
        plan_b_rows = result[result["plan_number"] == "2"]

        assert len(plan_a_rows) == 2
        assert len(plan_b_rows) == 1
        assert (plan_a_rows["plan_preempt_count"] == 2).all()
        assert (plan_b_rows["plan_preempt_count"] == 1).all()

    @pytest.mark.asyncio
    @patch("tsigma.reports.preempt_service.fetch_plans", new_callable=AsyncMock)
    @patch("tsigma.reports.preempt_service.fetch_events", new_callable=AsyncMock)
    async def test_event_on_plan_boundary(self, mock_fetch, mock_plans):
        """An event whose timestamp equals a plan's effective_from belongs to that plan."""
        from tsigma.reports.preempt_service import PreemptServiceParams, PreemptServiceReport

        t_plan_a_start = datetime(2025, 6, 15, 7, 0, 0)
        t_boundary = datetime(2025, 6, 15, 8, 30, 0)
        t_plan_b_end = datetime(2025, 6, 15, 10, 0, 0)

        mock_fetch.return_value = _events_to_df([
            _event(EVENT_PREEMPTION_ENTRY_STARTED, 1, t_boundary),
        ])
        mock_plans.return_value = [
            _make_plan(t_plan_a_start, t_boundary, plan_number=1),
            _make_plan(t_boundary, t_plan_b_end, plan_number=2),
        ]

        report = PreemptServiceReport()
        params = PreemptServiceParams(signal_id="SIG-001", start=_START_ISO, end=_END_ISO)
        result = await report.execute(params, _mock_session())

        assert len(result) == 1
        # Event at the boundary is assigned to the plan starting AT that time.
        assert result.iloc[0]["plan_number"] == "2"
        assert result.iloc[0]["plan_start"] == t_boundary.isoformat()

    @pytest.mark.asyncio
    @patch("tsigma.reports.preempt_service.fetch_plans", new_callable=AsyncMock)
    @patch("tsigma.reports.preempt_service.fetch_events", new_callable=AsyncMock)
    async def test_event_param_preserved(self, mock_fetch, mock_plans):
        """event_param (preempt channel) is preserved from the raw event row."""
        from tsigma.reports.preempt_service import PreemptServiceParams, PreemptServiceReport

        t_plan = datetime(2025, 6, 15, 7, 0, 0)
        t_plan_end = datetime(2025, 6, 15, 10, 0, 0)
        t0 = datetime(2025, 6, 15, 8, 0, 0)

        mock_fetch.return_value = _events_to_df([
            _event(EVENT_PREEMPTION_ENTRY_STARTED, 4, t0),
            _event(EVENT_PREEMPTION_ENTRY_STARTED, 7, t0 + timedelta(minutes=5)),
        ])
        mock_plans.return_value = [_make_plan(t_plan, t_plan_end, plan_number=3)]

        report = PreemptServiceReport()
        params = PreemptServiceParams(signal_id="SIG-001", start=_START_ISO, end=_END_ISO)
        result = await report.execute(params, _mock_session())

        params_returned = sorted(result["event_param"].tolist())
        assert params_returned == [4, 7]

    @pytest.mark.asyncio
    @patch("tsigma.reports.preempt_service.fetch_plans", new_callable=AsyncMock)
    @patch("tsigma.reports.preempt_service.fetch_events", new_callable=AsyncMock)
    async def test_event_outside_any_plan(self, mock_fetch, mock_plans):
        """Event with no active plan at its timestamp -> plan_number is 'unknown'."""
        from tsigma.reports.preempt_service import PreemptServiceParams, PreemptServiceReport

        t_event = datetime(2025, 6, 15, 8, 0, 0)
        # No plans returned — e.g. plan history not captured for this interval.
        mock_fetch.return_value = _events_to_df([
            _event(EVENT_PREEMPTION_ENTRY_STARTED, 1, t_event),
        ])
        mock_plans.return_value = []

        report = PreemptServiceReport()
        params = PreemptServiceParams(signal_id="SIG-001", start=_START_ISO, end=_END_ISO)
        result = await report.execute(params, _mock_session())

        assert len(result) == 1
        assert result.iloc[0]["plan_number"] == "unknown"
        assert result.iloc[0]["plan_preempt_count"] == 1

    @pytest.mark.asyncio
    @patch("tsigma.reports.preempt_service.fetch_plans", new_callable=AsyncMock)
    @patch("tsigma.reports.preempt_service.fetch_events", new_callable=AsyncMock)
    async def test_registered_in_registry(self, mock_fetch, mock_plans):
        """Report is discoverable under the name 'preempt-service'."""
        from tsigma.reports.registry import ReportRegistry

        cls = ReportRegistry.get("preempt-service")
        assert cls.metadata.name == "preempt-service"
