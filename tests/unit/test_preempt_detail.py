"""
Unit tests for the preempt-detail report plugin.

Exercises the full 10-event preemption state machine:
- Cycle pairing (102 request -> 105 entry)
- Delay vs no-delay cycles
- Track clearance / dwell / max-presence metrics
- 20-minute timeout termination
- Per-preempt-channel splitting
"""

from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest

from tsigma.reports.sdk.events import (
    EVENT_PREEMPTION_BEGIN_DWELL,
    EVENT_PREEMPTION_BEGIN_EXIT,
    EVENT_PREEMPTION_BEGIN_TRACK_CLEARANCE,
    EVENT_PREEMPTION_CALL_INPUT_OFF,
    EVENT_PREEMPTION_CALL_INPUT_ON,
    EVENT_PREEMPTION_ENTRY_STARTED,
    EVENT_PREEMPTION_GATE_DOWN,
    EVENT_PREEMPTION_LINK_ACTIVE_OFF,
    EVENT_PREEMPTION_LINK_ACTIVE_ON,
    EVENT_PREEMPTION_MAX_PRESENCE,
)

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
    """Convert event namespaces to the DataFrame shape returned by fetch_events."""
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
    """AsyncSession mock (unused by report but required by the ABC signature)."""
    session = AsyncMock()
    mock_result = MagicMock()
    mock_result.all.return_value = []
    session.execute = AsyncMock(return_value=mock_result)
    return session


_START_ISO = "2025-06-15T08:00:00"
_END_ISO = "2025-06-15T12:00:00"

EXPECTED_COLUMNS = [
    "preempt_number",
    "cycle_start",
    "cycle_end",
    "input_on",
    "input_off",
    "gate_down",
    "entry_started",
    "begin_track_clearance",
    "begin_dwell_service",
    "max_presence_exceeded",
    "has_delay",
    "delay_seconds",
    "time_to_service_seconds",
    "dwell_time_seconds",
    "track_clear_seconds",
    "call_max_out_seconds",
    "terminated_by_timeout",
]


# =========================================================================
# Registry
# =========================================================================


class TestPreemptDetailRegistration:

    def test_registered_under_expected_name(self):
        """The report registers itself under 'preempt-detail'."""
        # Trigger plugin auto-discovery.
        from tsigma.reports.registry import ReportRegistry

        cls = ReportRegistry.get("preempt-detail")
        assert cls is not None
        assert cls.metadata.name == "preempt-detail"

    def test_preemption_still_registered(self):
        """The legacy 'preemption' report remains registered alongside."""
        from tsigma.reports.registry import ReportRegistry

        cls = ReportRegistry.get("preemption")
        assert cls is not None
        assert cls.metadata.name == "preemption"


# =========================================================================
# Empty data
# =========================================================================


class TestPreemptDetailEmpty:

    @pytest.mark.asyncio
    @patch("tsigma.reports.preempt_detail.fetch_events", new_callable=AsyncMock)
    async def test_empty_events_returns_empty_schema(self, mock_fetch):
        """No events yield an empty DataFrame with the full expected schema."""
        from tsigma.reports.preempt_detail import (
            PreemptDetailParams,
            PreemptDetailReport,
        )

        mock_fetch.return_value = _events_to_df([])

        report = PreemptDetailReport()
        params = PreemptDetailParams(signal_id="SIG-001", start=_START_ISO, end=_END_ISO)
        result = await report.execute(params, _mock_session())

        assert isinstance(result, pd.DataFrame)
        assert result.empty
        assert list(result.columns) == EXPECTED_COLUMNS


# =========================================================================
# Simple cycles
# =========================================================================


class TestPreemptDetailSimpleCycle:

    @pytest.mark.asyncio
    @patch("tsigma.reports.preempt_detail.fetch_events", new_callable=AsyncMock)
    async def test_cycle_with_delay(self, mock_fetch):
        """102 -> 105 -> 106 -> 107 -> 111 computes delay, TTS, dwell, track_clear."""
        from tsigma.reports.preempt_detail import (
            PreemptDetailParams,
            PreemptDetailReport,
        )

        t0 = datetime(2025, 6, 15, 8, 0, 0)
        events = [
            _event(EVENT_PREEMPTION_CALL_INPUT_ON, 1, t0),
            _event(EVENT_PREEMPTION_ENTRY_STARTED, 1, t0 + timedelta(seconds=5)),
            _event(EVENT_PREEMPTION_BEGIN_TRACK_CLEARANCE, 1, t0 + timedelta(seconds=10)),
            _event(EVENT_PREEMPTION_BEGIN_DWELL, 1, t0 + timedelta(seconds=18)),
            _event(EVENT_PREEMPTION_BEGIN_EXIT, 1, t0 + timedelta(seconds=60)),
        ]
        mock_fetch.return_value = _events_to_df(events)

        report = PreemptDetailReport()
        params = PreemptDetailParams(signal_id="SIG-001", start=_START_ISO, end=_END_ISO)
        result = await report.execute(params, _mock_session())

        assert len(result) == 1
        row = result.iloc[0]
        assert row["preempt_number"] == 1
        assert bool(row["has_delay"]) is True
        # delay = entry_started - cycle_start = 5 s
        assert row["delay_seconds"] == pytest.approx(5.0)
        # time_to_service = max(106, 107) - entry_started = 18 - 5 = 13 s
        assert row["time_to_service_seconds"] == pytest.approx(13.0)
        # dwell = cycle_end - begin_dwell_service = 60 - 18 = 42 s
        assert row["dwell_time_seconds"] == pytest.approx(42.0)
        # track_clear = begin_dwell_service - begin_track_clearance = 18 - 10 = 8 s
        assert row["track_clear_seconds"] == pytest.approx(8.0)
        assert row["call_max_out_seconds"] is None
        assert bool(row["terminated_by_timeout"]) is False

    @pytest.mark.asyncio
    @patch("tsigma.reports.preempt_detail.fetch_events", new_callable=AsyncMock)
    async def test_cycle_without_delay(self, mock_fetch):
        """105 without a preceding 102 starts a cycle with has_delay=False."""
        from tsigma.reports.preempt_detail import (
            PreemptDetailParams,
            PreemptDetailReport,
        )

        t0 = datetime(2025, 6, 15, 8, 0, 0)
        events = [
            _event(EVENT_PREEMPTION_ENTRY_STARTED, 1, t0),
            _event(EVENT_PREEMPTION_BEGIN_TRACK_CLEARANCE, 1, t0 + timedelta(seconds=3)),
            _event(EVENT_PREEMPTION_BEGIN_DWELL, 1, t0 + timedelta(seconds=9)),
            _event(EVENT_PREEMPTION_BEGIN_EXIT, 1, t0 + timedelta(seconds=50)),
        ]
        mock_fetch.return_value = _events_to_df(events)

        report = PreemptDetailReport()
        params = PreemptDetailParams(signal_id="SIG-001", start=_START_ISO, end=_END_ISO)
        result = await report.execute(params, _mock_session())

        assert len(result) == 1
        row = result.iloc[0]
        assert bool(row["has_delay"]) is False
        assert row["delay_seconds"] == pytest.approx(0.0)
        # time_to_service = max(106, 107) - cycle_start = 9 s (entry = cycle_start)
        assert row["time_to_service_seconds"] == pytest.approx(9.0)
        assert row["dwell_time_seconds"] == pytest.approx(41.0)
        assert row["track_clear_seconds"] == pytest.approx(6.0)


# =========================================================================
# Max-presence and optional fields
# =========================================================================


class TestPreemptDetailMaxPresence:

    @pytest.mark.asyncio
    @patch("tsigma.reports.preempt_detail.fetch_events", new_callable=AsyncMock)
    async def test_max_presence_observed(self, mock_fetch):
        """110 before 111 populates call_max_out_seconds."""
        from tsigma.reports.preempt_detail import (
            PreemptDetailParams,
            PreemptDetailReport,
        )

        t0 = datetime(2025, 6, 15, 8, 0, 0)
        events = [
            _event(EVENT_PREEMPTION_CALL_INPUT_ON, 1, t0),
            _event(EVENT_PREEMPTION_ENTRY_STARTED, 1, t0 + timedelta(seconds=5)),
            _event(EVENT_PREEMPTION_BEGIN_TRACK_CLEARANCE, 1, t0 + timedelta(seconds=10)),
            _event(EVENT_PREEMPTION_BEGIN_DWELL, 1, t0 + timedelta(seconds=18)),
            _event(EVENT_PREEMPTION_MAX_PRESENCE, 1, t0 + timedelta(seconds=120)),
            _event(EVENT_PREEMPTION_BEGIN_EXIT, 1, t0 + timedelta(seconds=180)),
        ]
        mock_fetch.return_value = _events_to_df(events)

        report = PreemptDetailReport()
        params = PreemptDetailParams(signal_id="SIG-001", start=_START_ISO, end=_END_ISO)
        result = await report.execute(params, _mock_session())

        assert len(result) == 1
        row = result.iloc[0]
        # call_max_out = max_presence_exceeded - cycle_start = 120 s
        assert row["call_max_out_seconds"] == pytest.approx(120.0)
        assert row["max_presence_exceeded"] is not None


# =========================================================================
# Gate-down / link-active capture
# =========================================================================


class TestPreemptDetailOptionalCaptures:

    @pytest.mark.asyncio
    @patch("tsigma.reports.preempt_detail.fetch_events", new_callable=AsyncMock)
    async def test_gate_down_and_input_off(self, mock_fetch):
        """103 captures gate_down; 104 populates input_off."""
        from tsigma.reports.preempt_detail import (
            PreemptDetailParams,
            PreemptDetailReport,
        )

        t0 = datetime(2025, 6, 15, 8, 0, 0)
        events = [
            _event(EVENT_PREEMPTION_CALL_INPUT_ON, 1, t0),
            _event(EVENT_PREEMPTION_GATE_DOWN, 1, t0 + timedelta(seconds=2)),
            _event(EVENT_PREEMPTION_ENTRY_STARTED, 1, t0 + timedelta(seconds=5)),
            _event(EVENT_PREEMPTION_BEGIN_TRACK_CLEARANCE, 1, t0 + timedelta(seconds=10)),
            _event(EVENT_PREEMPTION_BEGIN_DWELL, 1, t0 + timedelta(seconds=18)),
            _event(EVENT_PREEMPTION_CALL_INPUT_OFF, 1, t0 + timedelta(seconds=30)),
            _event(EVENT_PREEMPTION_LINK_ACTIVE_ON, 1, t0 + timedelta(seconds=32)),
            _event(EVENT_PREEMPTION_LINK_ACTIVE_OFF, 1, t0 + timedelta(seconds=45)),
            _event(EVENT_PREEMPTION_BEGIN_EXIT, 1, t0 + timedelta(seconds=60)),
        ]
        mock_fetch.return_value = _events_to_df(events)

        report = PreemptDetailReport()
        params = PreemptDetailParams(signal_id="SIG-001", start=_START_ISO, end=_END_ISO)
        result = await report.execute(params, _mock_session())

        assert len(result) == 1
        row = result.iloc[0]
        assert row["gate_down"] is not None
        assert row["input_off"] is not None
        # input_off is the FIRST 104 timestamp
        assert row["input_off"] == (t0 + timedelta(seconds=30)).isoformat()


# =========================================================================
# Timeout termination
# =========================================================================


class TestPreemptDetailTimeout:

    @pytest.mark.asyncio
    @patch("tsigma.reports.preempt_detail.fetch_events", new_callable=AsyncMock)
    async def test_timeout_force_ends_cycle(self, mock_fetch):
        """A >20-minute gap between events force-ends the current cycle."""
        from tsigma.reports.preempt_detail import (
            PreemptDetailParams,
            PreemptDetailReport,
        )

        t0 = datetime(2025, 6, 15, 8, 0, 0)
        events = [
            _event(EVENT_PREEMPTION_CALL_INPUT_ON, 1, t0),
            _event(EVENT_PREEMPTION_ENTRY_STARTED, 1, t0 + timedelta(seconds=5)),
            _event(EVENT_PREEMPTION_BEGIN_TRACK_CLEARANCE, 1, t0 + timedelta(seconds=10)),
            _event(EVENT_PREEMPTION_BEGIN_DWELL, 1, t0 + timedelta(seconds=18)),
            # 25-minute gap, then a brand-new 102 — should close the first cycle by timeout.
            _event(EVENT_PREEMPTION_CALL_INPUT_ON, 1, t0 + timedelta(minutes=25)),
            _event(EVENT_PREEMPTION_ENTRY_STARTED, 1, t0 + timedelta(minutes=25, seconds=3)),
            _event(EVENT_PREEMPTION_BEGIN_EXIT, 1, t0 + timedelta(minutes=26)),
        ]
        mock_fetch.return_value = _events_to_df(events)

        report = PreemptDetailReport()
        params = PreemptDetailParams(signal_id="SIG-001", start=_START_ISO, end=_END_ISO)
        result = await report.execute(params, _mock_session())

        assert len(result) == 2
        first = result.iloc[0]
        assert bool(first["terminated_by_timeout"]) is True
        second = result.iloc[1]
        assert bool(second["terminated_by_timeout"]) is False


# =========================================================================
# Multiple preempt channels
# =========================================================================


class TestPreemptDetailMultipleChannels:

    @pytest.mark.asyncio
    @patch("tsigma.reports.preempt_detail.fetch_events", new_callable=AsyncMock)
    async def test_interleaved_channels_split(self, mock_fetch):
        """Interleaved events on two preempt channels yield one row per channel."""
        from tsigma.reports.preempt_detail import (
            PreemptDetailParams,
            PreemptDetailReport,
        )

        t0 = datetime(2025, 6, 15, 8, 0, 0)
        events = [
            _event(EVENT_PREEMPTION_CALL_INPUT_ON, 1, t0),
            _event(EVENT_PREEMPTION_CALL_INPUT_ON, 2, t0 + timedelta(seconds=2)),
            _event(EVENT_PREEMPTION_ENTRY_STARTED, 1, t0 + timedelta(seconds=5)),
            _event(EVENT_PREEMPTION_ENTRY_STARTED, 2, t0 + timedelta(seconds=8)),
            _event(EVENT_PREEMPTION_BEGIN_DWELL, 1, t0 + timedelta(seconds=12)),
            _event(EVENT_PREEMPTION_BEGIN_DWELL, 2, t0 + timedelta(seconds=15)),
            _event(EVENT_PREEMPTION_BEGIN_EXIT, 1, t0 + timedelta(seconds=60)),
            _event(EVENT_PREEMPTION_BEGIN_EXIT, 2, t0 + timedelta(seconds=70)),
        ]
        mock_fetch.return_value = _events_to_df(events)

        report = PreemptDetailReport()
        params = PreemptDetailParams(signal_id="SIG-001", start=_START_ISO, end=_END_ISO)
        result = await report.execute(params, _mock_session())

        assert len(result) == 2
        channels = sorted(result["preempt_number"].tolist())
        assert channels == [1, 2]

    @pytest.mark.asyncio
    @patch("tsigma.reports.preempt_detail.fetch_events", new_callable=AsyncMock)
    async def test_preempt_number_filter(self, mock_fetch):
        """preempt_number param filters results to just that channel."""
        from tsigma.reports.preempt_detail import (
            PreemptDetailParams,
            PreemptDetailReport,
        )

        t0 = datetime(2025, 6, 15, 8, 0, 0)
        events = [
            _event(EVENT_PREEMPTION_CALL_INPUT_ON, 1, t0),
            _event(EVENT_PREEMPTION_CALL_INPUT_ON, 2, t0 + timedelta(seconds=2)),
            _event(EVENT_PREEMPTION_ENTRY_STARTED, 1, t0 + timedelta(seconds=5)),
            _event(EVENT_PREEMPTION_ENTRY_STARTED, 2, t0 + timedelta(seconds=8)),
            _event(EVENT_PREEMPTION_BEGIN_DWELL, 1, t0 + timedelta(seconds=12)),
            _event(EVENT_PREEMPTION_BEGIN_DWELL, 2, t0 + timedelta(seconds=15)),
            _event(EVENT_PREEMPTION_BEGIN_EXIT, 1, t0 + timedelta(seconds=60)),
            _event(EVENT_PREEMPTION_BEGIN_EXIT, 2, t0 + timedelta(seconds=70)),
        ]
        mock_fetch.return_value = _events_to_df(events)

        report = PreemptDetailReport()
        params = PreemptDetailParams(
            signal_id="SIG-001", start=_START_ISO, end=_END_ISO, preempt_number=2,
        )
        result = await report.execute(params, _mock_session())

        assert len(result) == 1
        assert int(result.iloc[0]["preempt_number"]) == 2
