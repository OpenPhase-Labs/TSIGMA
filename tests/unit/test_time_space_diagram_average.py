"""
Unit tests for the Time-Space Diagram Average report plugin.

Verifies multi-day median cycle synthesis: day filtering, plan-mismatch
detection, median selection (not mean), downstream distance projection,
and weekday filtering.  All external I/O is mocked — no database or
config resolver is exercised.
"""

from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest

from tsigma.reports.sdk.events import (
    EVENT_PHASE_GREEN,
    EVENT_RED_CLEARANCE,
    EVENT_YELLOW_CLEARANCE,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


_EXPECTED_COLUMNS = [
    "signal_id",
    "phase_number",
    "cycle_index",
    "event",
    "event_time",
    "distance_ft",
    "cycle_length_seconds",
    "median_green_seconds",
    "median_yellow_seconds",
    "median_red_seconds",
    "programmed_split_seconds",
    "days_included",
    "speed_limit_applied",
]


def _event_row(code: int, param: int, time: datetime) -> dict:
    return {"event_code": code, "event_param": param, "event_time": time}


def _events_to_df(events: list[dict]) -> pd.DataFrame:
    if not events:
        return pd.DataFrame(columns=["event_code", "event_param", "event_time"])
    return pd.DataFrame(events)


def _make_plan(
    effective_from: datetime,
    effective_to: datetime | None,
    *,
    cycle_length: int = 120,
    offset: int = 0,
    splits: dict | None = None,
    plan_number: int = 1,
):
    """Fake SignalPlan-like object."""
    plan = SimpleNamespace()
    plan.signal_id = "SIG-001"
    plan.effective_from = effective_from
    plan.effective_to = effective_to
    plan.plan_number = plan_number
    plan.cycle_length = cycle_length
    plan.offset = offset
    plan.splits = splits if splits is not None else {"2": 40.0, "6": 40.0}
    return plan


def _mock_session() -> AsyncMock:
    session = AsyncMock()
    mock_result = MagicMock()
    mock_result.all.return_value = []
    mock_scalars = MagicMock()
    mock_scalars.all.return_value = []
    mock_result.scalars.return_value = mock_scalars
    session.execute = AsyncMock(return_value=mock_result)
    return session


def _cycle_events(start: datetime, green_s: float, yellow_s: float, red_s: float,
                  phase: int = 2) -> list[dict]:
    """One green→yellow→red→next-green interval worth of events."""
    t = start
    return [
        _event_row(EVENT_PHASE_GREEN, phase, t),
        _event_row(EVENT_YELLOW_CLEARANCE, phase, t + timedelta(seconds=green_s)),
        _event_row(EVENT_RED_CLEARANCE, phase, t + timedelta(seconds=green_s + yellow_s)),
        _event_row(EVENT_PHASE_GREEN, phase, t + timedelta(seconds=green_s + yellow_s + red_s)),
    ]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestEmptyAndSchema:

    @pytest.mark.asyncio
    @patch("tsigma.reports.time_space_diagram_average.fetch_plans", new_callable=AsyncMock)
    @patch("tsigma.reports.time_space_diagram_average.fetch_events", new_callable=AsyncMock)
    async def test_empty_events_returns_empty_with_full_schema(self, mock_fetch, mock_plans):
        """No events on any selected day -> empty DataFrame with full schema."""
        from tsigma.reports.time_space_diagram_average import (
            TimeSpaceDiagramAverageParams,
            TimeSpaceDiagramAverageReport,
        )

        mock_fetch.return_value = _events_to_df([])
        mock_plans.return_value = [_make_plan(datetime(2025, 6, 1), None)]

        report = TimeSpaceDiagramAverageReport()
        params = TimeSpaceDiagramAverageParams(
            signal_ids=["SIG-001"],
            start_date="2025-06-16",
            end_date="2025-06-18",
            start_time="08:00",
            end_time="09:00",
            days_of_week=[0, 1, 2],
            direction_phase_map={"SIG-001": 2},
        )
        result = await report.execute(params, _mock_session())

        assert isinstance(result, pd.DataFrame)
        assert list(result.columns) == _EXPECTED_COLUMNS
        assert len(result) == 0

    @pytest.mark.asyncio
    @patch("tsigma.reports.time_space_diagram_average.fetch_plans", new_callable=AsyncMock)
    @patch("tsigma.reports.time_space_diagram_average.fetch_events", new_callable=AsyncMock)
    async def test_no_days_match_filter(self, mock_fetch, mock_plans):
        """Calendar range contains no matching weekdays -> empty DataFrame."""
        from tsigma.reports.time_space_diagram_average import (
            TimeSpaceDiagramAverageParams,
            TimeSpaceDiagramAverageReport,
        )

        mock_fetch.return_value = _events_to_df([])
        mock_plans.return_value = [_make_plan(datetime(2025, 6, 1), None)]

        report = TimeSpaceDiagramAverageReport()
        # 2025-06-16 is a Monday, 2025-06-17 is a Tuesday — no Sundays (6) in range.
        params = TimeSpaceDiagramAverageParams(
            signal_ids=["SIG-001"],
            start_date="2025-06-16",
            end_date="2025-06-17",
            start_time="08:00",
            end_time="09:00",
            days_of_week=[6],
            direction_phase_map={"SIG-001": 2},
        )
        result = await report.execute(params, _mock_session())

        assert len(result) == 0
        # fetch_events should never have been called because no days qualified.
        mock_fetch.assert_not_called()


class TestPlanValidation:

    @pytest.mark.asyncio
    @patch("tsigma.reports.time_space_diagram_average.fetch_plans", new_callable=AsyncMock)
    @patch("tsigma.reports.time_space_diagram_average.fetch_events", new_callable=AsyncMock)
    async def test_plan_mismatch_cycle_length_raises(self, mock_fetch, mock_plans):
        """Different cycle lengths on selected days -> clear error listing days."""
        from tsigma.reports.time_space_diagram_average import (
            TimeSpaceDiagramAverageParams,
            TimeSpaceDiagramAverageReport,
        )

        # Two plans with different cycle lengths: 120 before 2025-06-17, 90 after.
        mock_plans.return_value = [
            _make_plan(
                datetime(2025, 6, 1),
                datetime(2025, 6, 17, 0, 0),
                cycle_length=120,
            ),
            _make_plan(
                datetime(2025, 6, 17, 0, 0),
                None,
                cycle_length=90,
            ),
        ]
        mock_fetch.return_value = _events_to_df([])

        report = TimeSpaceDiagramAverageReport()
        params = TimeSpaceDiagramAverageParams(
            signal_ids=["SIG-001"],
            start_date="2025-06-16",  # Monday, plan 120
            end_date="2025-06-17",    # Tuesday, plan 90
            start_time="08:00",
            end_time="09:00",
            days_of_week=[0, 1],  # Mon + Tue
            direction_phase_map={"SIG-001": 2},
        )

        with pytest.raises(ValueError) as exc:
            await report.execute(params, _mock_session())

        msg = str(exc.value)
        assert "2025-06-16" in msg
        assert "2025-06-17" in msg
        # Should mention the mismatching attribute
        assert "cycle_length" in msg or "cycle length" in msg


class TestMedianSelection:

    @pytest.mark.asyncio
    @patch("tsigma.reports.time_space_diagram_average.fetch_plans", new_callable=AsyncMock)
    @patch("tsigma.reports.time_space_diagram_average.fetch_events", new_callable=AsyncMock)
    async def test_median_cycle_picked_from_sorted_greens(self, mock_fetch, mock_plans):
        """Median (middle) cycle green duration, not mean, is used."""
        from tsigma.reports.time_space_diagram_average import (
            TimeSpaceDiagramAverageParams,
            TimeSpaceDiagramAverageReport,
        )

        mock_plans.return_value = [_make_plan(datetime(2025, 6, 1), None, cycle_length=100)]

        # 3 days, 5 cycles per day = 15 cycles total.  Green durations:
        # Day 1 (Mon 16): 20, 22, 24, 26, 28
        # Day 2 (Tue 17): 30, 32, 34, 36, 38
        # Day 3 (Wed 18): 40, 42, 44, 46, 48
        # Sorted = 20,22,24,26,28,30,32,34,36,38,40,42,44,46,48
        # Mean = 34.  Median (index 15//2 = 7) = 34 also — use anomalous distribution.
        # Use: 10, 10, 10, 10, 10, 30, 30, 30, 30, 30, 50, 80, 100, 120, 140
        # Mean=46.  Median (index 7) = 30.  Yellow=4, Red=cycle - green - yellow.
        greens_by_day = [
            [10, 10, 10, 10, 10],
            [30, 30, 30, 30, 30],
            [50, 80, 100, 120, 140],
        ]
        # Map signal_id → {date → events}
        all_events: list[dict] = []
        for day_index, greens in enumerate(greens_by_day):
            day_start = datetime(2025, 6, 16 + day_index, 8, 0, 0)
            cursor = day_start
            for g in greens:
                y = 4.0
                # Fill remainder to advance to next cycle slot.
                r = 6.0
                all_events.extend(_cycle_events(cursor, g, y, r))
                cursor = cursor + timedelta(seconds=g + y + r)

        async def fetch_side_effect(signal_id, start, end, codes, **kw):
            df = _events_to_df(all_events)
            if df.empty:
                return df
            mask = (df["event_time"] >= start) & (df["event_time"] <= end)
            return df.loc[mask].reset_index(drop=True)

        mock_fetch.side_effect = fetch_side_effect

        report = TimeSpaceDiagramAverageReport()
        params = TimeSpaceDiagramAverageParams(
            signal_ids=["SIG-001"],
            start_date="2025-06-16",  # Monday
            end_date="2025-06-18",    # Wednesday
            start_time="08:00",
            end_time="09:00",
            days_of_week=[0, 1, 2],  # Mon, Tue, Wed
            direction_phase_map={"SIG-001": 2},
        )
        result = await report.execute(params, _mock_session())

        assert len(result) > 0
        assert result["median_green_seconds"].iloc[0] == 30.0
        assert result["median_yellow_seconds"].iloc[0] == 4.0
        assert result["days_included"].iloc[0] == 3


class TestCorridorProjection:

    @pytest.mark.asyncio
    @patch("tsigma.reports.time_space_diagram_average.fetch_plans", new_callable=AsyncMock)
    @patch("tsigma.reports.time_space_diagram_average.fetch_events", new_callable=AsyncMock)
    async def test_multi_signal_distances_emitted(self, mock_fetch, mock_plans):
        """Multi-signal corridor emits distance_ft per signal-row for downstream projection."""
        from tsigma.reports.time_space_diagram_average import (
            TimeSpaceDiagramAverageParams,
            TimeSpaceDiagramAverageReport,
        )

        mock_plans.return_value = [_make_plan(datetime(2025, 6, 1), None, cycle_length=100)]

        # One cycle per signal per day.
        t0 = datetime(2025, 6, 16, 8, 0, 0)

        async def fetch_side_effect(signal_id, start, end, codes, **kw):
            return _events_to_df(_cycle_events(t0, 30, 4, 66))

        mock_fetch.side_effect = fetch_side_effect

        report = TimeSpaceDiagramAverageReport()
        params = TimeSpaceDiagramAverageParams(
            signal_ids=["SIG-001", "SIG-002", "SIG-003"],
            start_date="2025-06-16",
            end_date="2025-06-16",
            start_time="08:00",
            end_time="08:30",
            days_of_week=[0],
            direction_phase_map={"SIG-001": 2, "SIG-002": 2, "SIG-003": 2},
            distances={"SIG-001": 0.0, "SIG-002": 500.0, "SIG-003": 1000.0},
            speed_limit_mph=30,
        )
        result = await report.execute(params, _mock_session())

        assert not result.empty
        sig1 = result[result["signal_id"] == "SIG-001"]
        sig2 = result[result["signal_id"] == "SIG-002"]
        sig3 = result[result["signal_id"] == "SIG-003"]

        assert not sig1.empty and sig1["distance_ft"].iloc[0] == 0.0
        assert not sig2.empty and sig2["distance_ft"].iloc[0] == 500.0
        assert not sig3.empty and sig3["distance_ft"].iloc[0] == 1000.0
        assert result["speed_limit_applied"].iloc[0] == 30.0


class TestWeekdayFilter:

    @pytest.mark.asyncio
    @patch("tsigma.reports.time_space_diagram_average.fetch_plans", new_callable=AsyncMock)
    @patch("tsigma.reports.time_space_diagram_average.fetch_events", new_callable=AsyncMock)
    async def test_only_mondays_contribute(self, mock_fetch, mock_plans):
        """days_of_week=[0] (Monday only) skips Tue/Wed in the range."""
        from tsigma.reports.time_space_diagram_average import (
            TimeSpaceDiagramAverageParams,
            TimeSpaceDiagramAverageReport,
        )

        mock_plans.return_value = [_make_plan(datetime(2025, 6, 1), None, cycle_length=100)]

        fetch_calls: list[tuple[datetime, datetime]] = []

        async def fetch_side_effect(signal_id, start, end, codes, **kw):
            fetch_calls.append((start, end))
            return _events_to_df(_cycle_events(start, 30, 4, 66))

        mock_fetch.side_effect = fetch_side_effect

        report = TimeSpaceDiagramAverageReport()
        # Range 2025-06-16 (Mon) through 2025-06-22 (Sun).
        params = TimeSpaceDiagramAverageParams(
            signal_ids=["SIG-001"],
            start_date="2025-06-16",
            end_date="2025-06-22",
            start_time="08:00",
            end_time="09:00",
            days_of_week=[0],  # Only Mondays
            direction_phase_map={"SIG-001": 2},
        )
        result = await report.execute(params, _mock_session())

        # Only one day (Mon 2025-06-16) should have been queried.
        assert len(fetch_calls) == 1
        start_dt, _end_dt = fetch_calls[0]
        assert start_dt.date().isoformat() == "2025-06-16"
        assert result["days_included"].iloc[0] == 1


class TestSpeedLimitOverride:

    @pytest.mark.asyncio
    @patch("tsigma.reports.time_space_diagram_average.fetch_plans", new_callable=AsyncMock)
    @patch("tsigma.reports.time_space_diagram_average.fetch_events", new_callable=AsyncMock)
    async def test_speed_limit_param_is_used(self, mock_fetch, mock_plans):
        """speed_limit_mph param value is surfaced in the speed_limit_applied column."""
        from tsigma.reports.time_space_diagram_average import (
            TimeSpaceDiagramAverageParams,
            TimeSpaceDiagramAverageReport,
        )

        mock_plans.return_value = [_make_plan(datetime(2025, 6, 1), None, cycle_length=100)]

        t0 = datetime(2025, 6, 16, 8, 0, 0)

        async def fetch_side_effect(signal_id, start, end, codes, **kw):
            return _events_to_df(_cycle_events(t0, 30, 4, 66))

        mock_fetch.side_effect = fetch_side_effect

        report = TimeSpaceDiagramAverageReport()
        params = TimeSpaceDiagramAverageParams(
            signal_ids=["SIG-001"],
            start_date="2025-06-16",
            end_date="2025-06-16",
            start_time="08:00",
            end_time="08:30",
            days_of_week=[0],
            direction_phase_map={"SIG-001": 2},
            speed_limit_mph=45,
        )
        result = await report.execute(params, _mock_session())

        assert not result.empty
        assert result["speed_limit_applied"].iloc[0] == 45.0


class TestRegistryAutodiscovery:

    def test_registered_under_kebab_name(self):
        """Registry auto-discovers the kebab-case name."""
        # Force auto-discovery
        import tsigma.reports  # noqa: F401
        from tsigma.reports.registry import ReportRegistry

        reports = ReportRegistry.list_all()
        assert "time-space-diagram-average" in reports
