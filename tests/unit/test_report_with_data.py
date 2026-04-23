"""
Unit tests for report execute() methods with realistic mock data.

Each test provides actual event data so the processing logic inside
execute() is exercised — not just the empty-data early-return path.
"""

from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest

from tsigma.config_resolver import ApproachSnapshot, DetectorSnapshot, SignalConfig
from tsigma.reports.sdk.events import (
    EVENT_DETECTOR_OFF,
    EVENT_DETECTOR_ON,
    EVENT_FORCE_OFF,
    EVENT_GAP_OUT,
    EVENT_MAX_OUT,
    EVENT_PED_CALL,
    EVENT_PED_WALK,
    EVENT_PHASE_END,
    EVENT_PHASE_GREEN,
    EVENT_PREEMPTION_CALL_INPUT_OFF,
    EVENT_PREEMPTION_CALL_INPUT_ON,
    EVENT_RED_CLEARANCE,
    EVENT_TSP_CHECK_IN,
    EVENT_TSP_EARLY_GREEN,
    EVENT_TSP_EXTEND_GREEN,
    EVENT_YELLOW_CLEARANCE,
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
        device_id=1,
        validation_metadata=None,
    )


def _events_to_df(events: list[SimpleNamespace]) -> pd.DataFrame:
    """Convert list of _event() SimpleNamespace objects to a DataFrame.

    Matches the column format returned by db_facade.get_dataframe().
    """
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


# ISO-8601 string constants for the standard analysis window.
_START_ISO = "2025-06-15T08:00:00"
_END_ISO = "2025-06-15T09:00:00"


def _mock_session():
    """AsyncSession mock — not used by reports that get events via patched helpers."""
    session = AsyncMock()
    mock_result = MagicMock()
    mock_result.all.return_value = []
    mock_scalars = MagicMock()
    mock_scalars.all.return_value = []
    mock_result.scalars.return_value = mock_scalars
    session.execute = AsyncMock(return_value=mock_result)
    return session


def _config_with_phase(phase: int = 2, channels: tuple[int, ...] = (5, 6),
                       *, distance: int | None = None, min_speed: int | None = None,
                       mph: int | None = None):
    """SignalConfig with one approach on the given phase and detector channels."""
    approach = ApproachSnapshot(
        approach_id="APP-1",
        signal_id="SIG-001",
        direction_type_id=1,  # NB
        protected_phase_number=phase,
        permissive_phase_number=None,
        is_protected_phase_overlap=False,
        is_permissive_phase_overlap=False,
        ped_phase_number=None,
        mph=mph,
        description="Northbound",
    )
    detectors = [
        DetectorSnapshot(
            detector_id=f"DET-{ch}",
            approach_id="APP-1",
            detector_channel=ch,
            distance_from_stop_bar=distance,
            min_speed_filter=min_speed,
            lane_number=i + 1,
        )
        for i, ch in enumerate(channels)
    ]
    return SignalConfig(
        signal_id="SIG-001",
        as_of=datetime(2025, 6, 15),
        from_audit=False,
        approaches=[approach],
        detectors=detectors,
    )


# =========================================================================
# Approach Delay — with data
# =========================================================================


class TestApproachDelayWithData:

    @pytest.mark.asyncio
    @patch("tsigma.reports.sdk.queries.db_facade")
    @patch("tsigma.reports.approach_delay.get_config_at", new_callable=AsyncMock)
    async def test_single_cycle_delay(self, mock_config, mock_facade):
        """One green + one detector-on produces a single delay measurement."""
        from tsigma.reports.approach_delay import ApproachDelayParams, ApproachDelayReport

        mock_config.return_value = _config_with_phase(2, (5,))

        t0 = datetime(2025, 6, 15, 8, 0, 0)
        events = [
            _event(EVENT_PHASE_GREEN, 2, t0),
            _event(EVENT_DETECTOR_ON, 5, t0 + timedelta(seconds=12)),
        ]
        mock_facade.get_dataframe = AsyncMock(return_value=_events_to_df(events))

        report = ApproachDelayReport()
        params = ApproachDelayParams(signal_id="SIG-001", start=_START_ISO, end=_END_ISO)
        result = await report.execute(params, _mock_session())

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert result.iloc[0]["avg_delay_seconds"] == 12.0
        assert result.iloc[0]["volume"] == 1
        assert result.iloc[0]["approach_id"] == "APP-1"

    @pytest.mark.asyncio
    @patch("tsigma.reports.sdk.queries.db_facade")
    @patch("tsigma.reports.approach_delay.get_config_at", new_callable=AsyncMock)
    async def test_multiple_actuations_average(self, mock_config, mock_facade):
        """Multiple detector-on events in one bin are averaged."""
        from tsigma.reports.approach_delay import ApproachDelayParams, ApproachDelayReport

        mock_config.return_value = _config_with_phase(2, (5,))

        t0 = datetime(2025, 6, 15, 8, 0, 0)
        events = [
            _event(EVENT_PHASE_GREEN, 2, t0),
            _event(EVENT_DETECTOR_ON, 5, t0 + timedelta(seconds=10)),
            _event(EVENT_DETECTOR_ON, 5, t0 + timedelta(seconds=20)),
        ]
        mock_facade.get_dataframe = AsyncMock(return_value=_events_to_df(events))

        report = ApproachDelayReport()
        params = ApproachDelayParams(signal_id="SIG-001", start=_START_ISO, end=_END_ISO)
        result = await report.execute(params, _mock_session())

        assert len(result) == 1
        assert result.iloc[0]["avg_delay_seconds"] == 15.0
        assert result.iloc[0]["volume"] == 2

    @pytest.mark.asyncio
    @patch("tsigma.reports.sdk.queries.db_facade")
    @patch("tsigma.reports.approach_delay.get_config_at", new_callable=AsyncMock)
    async def test_detector_before_green_ignored(self, mock_config, mock_facade):
        """Detector on before any green start produces no delay rows."""
        from tsigma.reports.approach_delay import ApproachDelayParams, ApproachDelayReport

        mock_config.return_value = _config_with_phase(2, (5,))

        t0 = datetime(2025, 6, 15, 8, 0, 0)
        events = [
            _event(EVENT_DETECTOR_ON, 5, t0 - timedelta(seconds=5)),
            _event(EVENT_PHASE_GREEN, 2, t0),
        ]
        mock_facade.get_dataframe = AsyncMock(return_value=_events_to_df(events))

        report = ApproachDelayReport()
        params = ApproachDelayParams(signal_id="SIG-001", start=_START_ISO, end=_END_ISO)
        result = await report.execute(params, _mock_session())

        assert result.empty

    @pytest.mark.asyncio
    @patch("tsigma.reports.sdk.queries.db_facade")
    @patch("tsigma.reports.approach_delay.get_config_at", new_callable=AsyncMock)
    async def test_hour_bin_size(self, mock_config, mock_facade):
        """bin_size='hour' groups all events in the same hour."""
        from tsigma.reports.approach_delay import ApproachDelayParams, ApproachDelayReport

        mock_config.return_value = _config_with_phase(2, (5,))

        t0 = datetime(2025, 6, 15, 8, 0, 0)
        events = [
            _event(EVENT_PHASE_GREEN, 2, t0),
            _event(EVENT_DETECTOR_ON, 5, t0 + timedelta(seconds=5)),
            _event(EVENT_DETECTOR_ON, 5, t0 + timedelta(minutes=30, seconds=10)),
        ]
        mock_facade.get_dataframe = AsyncMock(return_value=_events_to_df(events))

        report = ApproachDelayReport()
        params = ApproachDelayParams(signal_id="SIG-001", start=_START_ISO, end=_END_ISO, bin_size="hour")
        result = await report.execute(params, _mock_session())

        # Both events in same hour bin
        assert len(result) == 1
        assert result.iloc[0]["volume"] == 2


# =========================================================================
# Approach Speed — with data
# =========================================================================

class TestApproachSpeedWithData:

    @pytest.mark.asyncio
    @patch("tsigma.reports.sdk.queries.db_facade")
    @patch("tsigma.reports.approach_speed.get_config_at", new_callable=AsyncMock)
    async def test_speed_calculation(self, mock_config, mock_facade):
        """On/off pair with known distance produces correct speed."""
        from tsigma.reports.approach_speed import ApproachSpeedParams, ApproachSpeedReport

        # 400 ft distance, min_speed 5 mph, speed limit 45 mph
        mock_config.return_value = _config_with_phase(
            2, (5,), distance=400, min_speed=5, mph=45,
        )

        t0 = datetime(2025, 6, 15, 8, 0, 0)
        # 400 ft in 6 seconds = 66.67 ft/s = ~45.4 mph
        events = [
            _event(EVENT_DETECTOR_ON, 5, t0),
            _event(EVENT_DETECTOR_OFF, 5, t0 + timedelta(seconds=6)),
        ]
        mock_facade.get_dataframe = AsyncMock(return_value=_events_to_df(events))

        report = ApproachSpeedReport()
        params = ApproachSpeedParams(signal_id="SIG-001", start=_START_ISO, end=_END_ISO)
        result = await report.execute(params, _mock_session())

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        r = result.iloc[0]
        assert r["approach_id"] == "APP-1"
        assert r["direction"] == "NB"
        assert r["sample_count"] == 1
        assert r["speed_limit"] == 45
        # 400/6 / 1.467 ≈ 45.4
        assert 45.0 <= r["avg_speed"] <= 46.0

    @pytest.mark.asyncio
    @patch("tsigma.reports.sdk.queries.db_facade")
    @patch("tsigma.reports.approach_speed.get_config_at", new_callable=AsyncMock)
    async def test_speed_too_low_filtered(self, mock_config, mock_facade):
        """Speed below min_speed_filter is excluded."""
        from tsigma.reports.approach_speed import ApproachSpeedParams, ApproachSpeedReport

        mock_config.return_value = _config_with_phase(
            2, (5,), distance=400, min_speed=30, mph=45,
        )

        t0 = datetime(2025, 6, 15, 8, 0, 0)
        # 400 ft in 60 seconds = 6.67 ft/s = ~4.5 mph (below min_speed=30)
        events = [
            _event(EVENT_DETECTOR_ON, 5, t0),
            _event(EVENT_DETECTOR_OFF, 5, t0 + timedelta(seconds=60)),
        ]
        mock_facade.get_dataframe = AsyncMock(return_value=_events_to_df(events))

        report = ApproachSpeedReport()
        params = ApproachSpeedParams(signal_id="SIG-001", start=_START_ISO, end=_END_ISO)
        result = await report.execute(params, _mock_session())

        assert result.empty

    @pytest.mark.asyncio
    @patch("tsigma.reports.sdk.queries.db_facade")
    @patch("tsigma.reports.approach_speed.get_config_at", new_callable=AsyncMock)
    async def test_percentile_calculation(self, mock_config, mock_facade):
        """Multiple speeds produce correct p85 and p15."""
        from tsigma.reports.approach_speed import ApproachSpeedParams, ApproachSpeedReport

        mock_config.return_value = _config_with_phase(
            2, (5,), distance=400, min_speed=5, mph=45,
        )

        t0 = datetime(2025, 6, 15, 8, 0, 0)
        # Generate 10 vehicle pairs at different speeds
        events = []
        for i in range(10):
            on_time = t0 + timedelta(minutes=i * 2)
            # occupancy from 4s to 13s -> speeds from ~68 mph down to ~21 mph
            occ_seconds = 4 + i
            events.append(_event(EVENT_DETECTOR_ON, 5, on_time))
            events.append(_event(EVENT_DETECTOR_OFF, 5, on_time + timedelta(seconds=occ_seconds)))

        mock_facade.get_dataframe = AsyncMock(return_value=_events_to_df(events))

        report = ApproachSpeedReport()
        params = ApproachSpeedParams(signal_id="SIG-001", start=_START_ISO, end=_END_ISO)
        result = await report.execute(params, _mock_session())

        assert len(result) == 1
        r = result.iloc[0]
        assert r["sample_count"] == 10
        assert r["p85_speed"] >= r["p15_speed"]
        assert r["avg_speed"] > 0

    @pytest.mark.asyncio
    @patch("tsigma.reports.sdk.queries.db_facade")
    @patch("tsigma.reports.approach_speed.get_config_at", new_callable=AsyncMock)
    async def test_no_distance_no_results(self, mock_config, mock_facade):
        """Detector with no distance_from_stop_bar yields no results."""
        from tsigma.reports.approach_speed import ApproachSpeedParams, ApproachSpeedReport

        # distance=None means detector does not qualify
        mock_config.return_value = _config_with_phase(
            2, (5,), distance=None, min_speed=5, mph=45,
        )
        mock_facade.get_dataframe = AsyncMock(return_value=_events_to_df([]))

        report = ApproachSpeedReport()
        params = ApproachSpeedParams(signal_id="SIG-001", start=_START_ISO, end=_END_ISO)
        result = await report.execute(params, _mock_session())

        assert result.empty


# =========================================================================
# Yellow/Red Actuations — with data
# =========================================================================

class TestYellowRedActuationsWithData:

    @pytest.mark.asyncio
    @patch("tsigma.reports.yellow_red_actuations.fetch_events_split", new_callable=AsyncMock)
    @patch("tsigma.reports.yellow_red_actuations.get_config_at", new_callable=AsyncMock)
    async def test_full_cycle_counts(self, mock_config, mock_fetch):
        """A complete cycle with actuations in each interval."""
        from tsigma.reports.yellow_red_actuations import YellowRedActuationsParams, YellowRedActuationsReport

        mock_config.return_value = _config_with_phase(2, (5,))

        t0 = datetime(2025, 6, 15, 8, 0, 0)
        events = [
            # Cycle 1
            _event(EVENT_PHASE_GREEN, 2, t0),
            _event(EVENT_DETECTOR_ON, 5, t0 + timedelta(seconds=5)),   # green
            _event(EVENT_DETECTOR_ON, 5, t0 + timedelta(seconds=15)),  # green
            _event(EVENT_YELLOW_CLEARANCE, 2, t0 + timedelta(seconds=30)),
            _event(EVENT_DETECTOR_ON, 5, t0 + timedelta(seconds=32)),  # yellow
            _event(EVENT_RED_CLEARANCE, 2, t0 + timedelta(seconds=35)),
            _event(EVENT_DETECTOR_ON, 5, t0 + timedelta(seconds=36)),  # red
            _event(EVENT_PHASE_END, 2, t0 + timedelta(seconds=37)),
            _event(EVENT_DETECTOR_ON, 5, t0 + timedelta(seconds=38)),  # after end -> red
            # Cycle 2 start (closes cycle 1) — trailing empty cycle flushed too
            _event(EVENT_PHASE_GREEN, 2, t0 + timedelta(seconds=60)),
        ]
        mock_fetch.return_value = _events_to_df(events)

        report = YellowRedActuationsReport()
        params = YellowRedActuationsParams(signal_id="SIG-001", start=_START_ISO, end=_END_ISO, phase_number=2)
        result = await report.execute(params, _mock_session())

        assert isinstance(result, pd.DataFrame)
        # Cycle 1 closed by second green; trailing empty cycle 2 also flushed
        assert len(result) == 2
        c = result.iloc[0]
        assert c["green_actuations"] == 2
        assert c["yellow_actuations"] == 1
        assert c["red_actuations"] == 2
        assert c["total_actuations"] == 5
        # Cycle 2 has no actuations
        assert result.iloc[1]["total_actuations"] == 0

    @pytest.mark.asyncio
    @patch("tsigma.reports.yellow_red_actuations.fetch_events_split", new_callable=AsyncMock)
    @patch("tsigma.reports.yellow_red_actuations.get_config_at", new_callable=AsyncMock)
    async def test_trailing_cycle_flushed(self, mock_config, mock_fetch):
        """The last cycle (no second green to close it) is still emitted."""
        from tsigma.reports.yellow_red_actuations import YellowRedActuationsParams, YellowRedActuationsReport

        mock_config.return_value = _config_with_phase(2, (5,))

        t0 = datetime(2025, 6, 15, 8, 0, 0)
        events = [
            _event(EVENT_PHASE_GREEN, 2, t0),
            _event(EVENT_DETECTOR_ON, 5, t0 + timedelta(seconds=10)),
            _event(EVENT_YELLOW_CLEARANCE, 2, t0 + timedelta(seconds=30)),
            _event(EVENT_DETECTOR_ON, 5, t0 + timedelta(seconds=31)),
        ]
        mock_fetch.return_value = _events_to_df(events)

        report = YellowRedActuationsReport()
        params = YellowRedActuationsParams(signal_id="SIG-001", start=_START_ISO, end=_END_ISO, phase_number=2)
        result = await report.execute(params, _mock_session())

        assert len(result) == 1
        assert result.iloc[0]["green_actuations"] == 1
        assert result.iloc[0]["yellow_actuations"] == 1

    @pytest.mark.asyncio
    @patch("tsigma.reports.yellow_red_actuations.fetch_events_split", new_callable=AsyncMock)
    @patch("tsigma.reports.yellow_red_actuations.get_config_at", new_callable=AsyncMock)
    async def test_two_cycles(self, mock_config, mock_fetch):
        """Two consecutive cycles each produce a result row."""
        from tsigma.reports.yellow_red_actuations import YellowRedActuationsParams, YellowRedActuationsReport

        mock_config.return_value = _config_with_phase(2, (5,))

        t0 = datetime(2025, 6, 15, 8, 0, 0)
        events = [
            _event(EVENT_PHASE_GREEN, 2, t0),
            _event(EVENT_DETECTOR_ON, 5, t0 + timedelta(seconds=5)),
            _event(EVENT_YELLOW_CLEARANCE, 2, t0 + timedelta(seconds=30)),
            # Cycle 2
            _event(EVENT_PHASE_GREEN, 2, t0 + timedelta(seconds=60)),
            _event(EVENT_DETECTOR_ON, 5, t0 + timedelta(seconds=65)),
            _event(EVENT_YELLOW_CLEARANCE, 2, t0 + timedelta(seconds=90)),
        ]
        mock_fetch.return_value = _events_to_df(events)

        report = YellowRedActuationsReport()
        params = YellowRedActuationsParams(signal_id="SIG-001", start=_START_ISO, end=_END_ISO, phase_number=2)
        result = await report.execute(params, _mock_session())

        # Cycle 1 is closed by cycle 2 green; cycle 2 trailing flush
        assert len(result) == 2


# =========================================================================
# Split Failure — with data
# =========================================================================

class TestSplitFailureWithData:

    @pytest.mark.asyncio
    @patch("tsigma.reports.split_failure.fetch_events_split", new_callable=AsyncMock)
    @patch("tsigma.reports.split_failure.get_config_at", new_callable=AsyncMock)
    async def test_split_failure_detected(self, mock_config, mock_fetch):
        """High occupancy at green start and red start flags a split failure."""
        from tsigma.reports.split_failure import SplitFailureParams, SplitFailureReport

        mock_config.return_value = _config_with_phase(2, (5,))

        t0 = datetime(2025, 6, 15, 8, 0, 0)
        # Detector stays on the entire green interval (occupancy ~1.0 at
        # both green start and red start windows).
        events = [
            _event(EVENT_PHASE_GREEN, 2, t0),
            _event(EVENT_DETECTOR_ON, 5, t0),
            _event(EVENT_YELLOW_CLEARANCE, 2, t0 + timedelta(seconds=30)),
            _event(EVENT_RED_CLEARANCE, 2, t0 + timedelta(seconds=34)),
            _event(EVENT_DETECTOR_OFF, 5, t0 + timedelta(seconds=40)),
            _event(EVENT_PHASE_END, 2, t0 + timedelta(seconds=36)),
            # Next green closes cycle
            _event(EVENT_PHASE_GREEN, 2, t0 + timedelta(seconds=60)),
        ]
        mock_fetch.return_value = _events_to_df(events)

        report = SplitFailureReport()
        params = SplitFailureParams(signal_id="SIG-001", start=_START_ISO, end=_END_ISO, phase_number=2)
        result = await report.execute(params, _mock_session())

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        r = result.iloc[0]
        assert bool(r["is_split_failure"]) is True
        assert r["green_start_occupancy"] > 0.79
        assert r["red_start_occupancy"] > 0.79
        assert r["green_duration"] == 30.0

    @pytest.mark.asyncio
    @patch("tsigma.reports.split_failure.fetch_events_split", new_callable=AsyncMock)
    @patch("tsigma.reports.split_failure.get_config_at", new_callable=AsyncMock)
    async def test_no_failure_low_occupancy(self, mock_config, mock_fetch):
        """Detector off early -> low occupancy -> no split failure."""
        from tsigma.reports.split_failure import SplitFailureParams, SplitFailureReport

        mock_config.return_value = _config_with_phase(2, (5,))

        t0 = datetime(2025, 6, 15, 8, 0, 0)
        events = [
            _event(EVENT_PHASE_GREEN, 2, t0),
            _event(EVENT_DETECTOR_ON, 5, t0 + timedelta(seconds=0)),
            _event(EVENT_DETECTOR_OFF, 5, t0 + timedelta(seconds=1)),  # brief
            _event(EVENT_YELLOW_CLEARANCE, 2, t0 + timedelta(seconds=30)),
            _event(EVENT_RED_CLEARANCE, 2, t0 + timedelta(seconds=34)),
            _event(EVENT_PHASE_END, 2, t0 + timedelta(seconds=36)),
            _event(EVENT_PHASE_GREEN, 2, t0 + timedelta(seconds=60)),
        ]
        mock_fetch.return_value = _events_to_df(events)

        report = SplitFailureReport()
        params = SplitFailureParams(signal_id="SIG-001", start=_START_ISO, end=_END_ISO, phase_number=2)
        result = await report.execute(params, _mock_session())

        assert len(result) == 1
        assert bool(result.iloc[0]["is_split_failure"]) is False

    @pytest.mark.asyncio
    @patch("tsigma.reports.split_failure.fetch_events_split", new_callable=AsyncMock)
    @patch("tsigma.reports.split_failure.get_config_at", new_callable=AsyncMock)
    async def test_cycle_length_calculated(self, mock_config, mock_fetch):
        """cycle_length is the gap between successive green starts."""
        from tsigma.reports.split_failure import SplitFailureParams, SplitFailureReport

        mock_config.return_value = _config_with_phase(2, (5,))

        t0 = datetime(2025, 6, 15, 8, 0, 0)
        events = [
            _event(EVENT_PHASE_GREEN, 2, t0),
            _event(EVENT_YELLOW_CLEARANCE, 2, t0 + timedelta(seconds=25)),
            _event(EVENT_RED_CLEARANCE, 2, t0 + timedelta(seconds=29)),
            _event(EVENT_PHASE_END, 2, t0 + timedelta(seconds=31)),
            _event(EVENT_PHASE_GREEN, 2, t0 + timedelta(seconds=90)),
        ]
        mock_fetch.return_value = _events_to_df(events)

        report = SplitFailureReport()
        params = SplitFailureParams(signal_id="SIG-001", start=_START_ISO, end=_END_ISO, phase_number=2)
        result = await report.execute(params, _mock_session())

        assert len(result) == 1
        assert result.iloc[0]["cycle_length"] == 90.0
        assert result.iloc[0]["green_duration"] == 25.0


# =========================================================================
# Split Monitor — with data
# =========================================================================

class TestSplitMonitorWithData:

    @pytest.mark.asyncio
    @patch("tsigma.reports.split_monitor.fetch_events", new_callable=AsyncMock)
    async def test_single_phase_metrics(self, mock_fetch):
        """A complete cycle produces correct green, yellow, and split times."""
        from tsigma.reports.split_monitor import SplitMonitorParams, SplitMonitorReport

        t0 = datetime(2025, 6, 15, 8, 0, 0)
        events = [
            _event(EVENT_PHASE_GREEN, 2, t0),
            _event(EVENT_GAP_OUT, 2, t0 + timedelta(seconds=20)),
            _event(EVENT_YELLOW_CLEARANCE, 2, t0 + timedelta(seconds=25)),
            _event(EVENT_RED_CLEARANCE, 2, t0 + timedelta(seconds=29)),
        ]
        mock_fetch.return_value = _events_to_df(events)

        report = SplitMonitorReport()
        params = SplitMonitorParams(signal_id="SIG-001", start=_START_ISO, end=_END_ISO)
        result = await report.execute(params, _mock_session())

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        r = result.iloc[0]
        assert r["phase_number"] == 2
        assert r["cycles"] == 1
        assert r["green_time"] == 25.0
        assert r["yellow_time"] == 4.0
        assert r["total_split"] == 29.0
        assert r["gap_out_pct"] == 100.0
        assert r["force_off_pct"] == 0.0
        assert r["max_out_pct"] == 0.0

    @pytest.mark.asyncio
    @patch("tsigma.reports.split_monitor.fetch_events", new_callable=AsyncMock)
    async def test_termination_percentages(self, mock_fetch):
        """Two cycles — one gap-out, one force-off — yield 50/50 split."""
        from tsigma.reports.split_monitor import SplitMonitorParams, SplitMonitorReport

        t0 = datetime(2025, 6, 15, 8, 0, 0)
        events = [
            # Cycle 1: gap-out
            _event(EVENT_PHASE_GREEN, 2, t0),
            _event(EVENT_GAP_OUT, 2, t0 + timedelta(seconds=20)),
            _event(EVENT_YELLOW_CLEARANCE, 2, t0 + timedelta(seconds=25)),
            _event(EVENT_RED_CLEARANCE, 2, t0 + timedelta(seconds=29)),
            # Cycle 2: force-off
            _event(EVENT_PHASE_GREEN, 2, t0 + timedelta(seconds=60)),
            _event(EVENT_FORCE_OFF, 2, t0 + timedelta(seconds=90)),
            _event(EVENT_YELLOW_CLEARANCE, 2, t0 + timedelta(seconds=95)),
            _event(EVENT_RED_CLEARANCE, 2, t0 + timedelta(seconds=99)),
        ]
        mock_fetch.return_value = _events_to_df(events)

        report = SplitMonitorReport()
        params = SplitMonitorParams(signal_id="SIG-001", start=_START_ISO, end=_END_ISO)
        result = await report.execute(params, _mock_session())

        assert len(result) == 1
        r = result.iloc[0]
        assert r["cycles"] == 2
        assert r["gap_out_pct"] == 50.0
        assert r["force_off_pct"] == 50.0
        assert r["max_out_pct"] == 0.0

    @pytest.mark.asyncio
    @patch("tsigma.reports.split_monitor.fetch_events", new_callable=AsyncMock)
    async def test_multi_phase(self, mock_fetch):
        """Events for two different phases produce two result rows."""
        from tsigma.reports.split_monitor import SplitMonitorParams, SplitMonitorReport

        t0 = datetime(2025, 6, 15, 8, 0, 0)
        events = [
            # Phase 2
            _event(EVENT_PHASE_GREEN, 2, t0),
            _event(EVENT_GAP_OUT, 2, t0 + timedelta(seconds=20)),
            _event(EVENT_YELLOW_CLEARANCE, 2, t0 + timedelta(seconds=25)),
            _event(EVENT_RED_CLEARANCE, 2, t0 + timedelta(seconds=29)),
            # Phase 4
            _event(EVENT_PHASE_GREEN, 4, t0 + timedelta(seconds=30)),
            _event(EVENT_MAX_OUT, 4, t0 + timedelta(seconds=60)),
            _event(EVENT_YELLOW_CLEARANCE, 4, t0 + timedelta(seconds=65)),
            _event(EVENT_RED_CLEARANCE, 4, t0 + timedelta(seconds=69)),
        ]
        mock_fetch.return_value = _events_to_df(events)

        report = SplitMonitorReport()
        params = SplitMonitorParams(signal_id="SIG-001", start=_START_ISO, end=_END_ISO)
        result = await report.execute(params, _mock_session())

        assert len(result) == 2
        phases = set(result["phase_number"].tolist())
        assert phases == {2, 4}


# =========================================================================
# Red Light Monitor — with data
# =========================================================================

class TestRedLightMonitorWithData:

    @pytest.mark.asyncio
    @patch("tsigma.reports.red_light_monitor.fetch_events_split", new_callable=AsyncMock)
    @patch("tsigma.reports.red_light_monitor.get_config_at", new_callable=AsyncMock)
    async def test_violation_during_red_clearance(self, mock_config, mock_fetch):
        """Detector actuation during red clearance is a violation."""
        from tsigma.reports.red_light_monitor import RedLightMonitorParams, RedLightMonitorReport

        mock_config.return_value = _config_with_phase(2, (5,))

        t0 = datetime(2025, 6, 15, 8, 0, 0)
        events = [
            _event(EVENT_PHASE_GREEN, 2, t0),
            _event(EVENT_YELLOW_CLEARANCE, 2, t0 + timedelta(seconds=25)),
            _event(EVENT_RED_CLEARANCE, 2, t0 + timedelta(seconds=29)),
            _event(EVENT_DETECTOR_ON, 5, t0 + timedelta(seconds=30)),  # violation!
            _event(EVENT_PHASE_END, 2, t0 + timedelta(seconds=31)),
            # Close cycle
            _event(EVENT_PHASE_GREEN, 2, t0 + timedelta(seconds=60)),
        ]
        mock_fetch.return_value = _events_to_df(events)

        report = RedLightMonitorReport()
        params = RedLightMonitorParams(signal_id="SIG-001", start=_START_ISO, end=_END_ISO, phase_number=2)
        result = await report.execute(params, _mock_session())

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert result.iloc[0]["violation_count"] == 1

    @pytest.mark.asyncio
    @patch("tsigma.reports.red_light_monitor.fetch_events_split", new_callable=AsyncMock)
    @patch("tsigma.reports.red_light_monitor.get_config_at", new_callable=AsyncMock)
    async def test_no_violation_during_green(self, mock_config, mock_fetch):
        """Detector actuation during green is NOT a violation."""
        from tsigma.reports.red_light_monitor import RedLightMonitorParams, RedLightMonitorReport

        mock_config.return_value = _config_with_phase(2, (5,))

        t0 = datetime(2025, 6, 15, 8, 0, 0)
        events = [
            _event(EVENT_PHASE_GREEN, 2, t0),
            _event(EVENT_DETECTOR_ON, 5, t0 + timedelta(seconds=10)),  # during green
            _event(EVENT_YELLOW_CLEARANCE, 2, t0 + timedelta(seconds=25)),
            _event(EVENT_RED_CLEARANCE, 2, t0 + timedelta(seconds=29)),
            _event(EVENT_PHASE_END, 2, t0 + timedelta(seconds=31)),
            _event(EVENT_PHASE_GREEN, 2, t0 + timedelta(seconds=60)),
        ]
        mock_fetch.return_value = _events_to_df(events)

        report = RedLightMonitorReport()
        params = RedLightMonitorParams(signal_id="SIG-001", start=_START_ISO, end=_END_ISO, phase_number=2)
        result = await report.execute(params, _mock_session())

        # No violations, so empty result (filter removes 0-count cycles)
        assert result.empty

    @pytest.mark.asyncio
    @patch("tsigma.reports.red_light_monitor.fetch_events_split", new_callable=AsyncMock)
    @patch("tsigma.reports.red_light_monitor.get_config_at", new_callable=AsyncMock)
    async def test_violation_after_phase_end_within_grace(self, mock_config, mock_fetch):
        """Detector actuation within grace period after phase end is a violation."""
        from tsigma.reports.red_light_monitor import RedLightMonitorParams, RedLightMonitorReport

        mock_config.return_value = _config_with_phase(2, (5,))

        t0 = datetime(2025, 6, 15, 8, 0, 0)
        events = [
            _event(EVENT_PHASE_GREEN, 2, t0),
            _event(EVENT_YELLOW_CLEARANCE, 2, t0 + timedelta(seconds=25)),
            _event(EVENT_RED_CLEARANCE, 2, t0 + timedelta(seconds=29)),
            _event(EVENT_PHASE_END, 2, t0 + timedelta(seconds=31)),
            # 1 second after phase end — within 2-second grace
            _event(EVENT_DETECTOR_ON, 5, t0 + timedelta(seconds=32)),
            _event(EVENT_PHASE_GREEN, 2, t0 + timedelta(seconds=60)),
        ]
        mock_fetch.return_value = _events_to_df(events)

        report = RedLightMonitorReport()
        params = RedLightMonitorParams(signal_id="SIG-001", start=_START_ISO, end=_END_ISO, phase_number=2)
        result = await report.execute(params, _mock_session())

        assert len(result) == 1
        assert result.iloc[0]["violation_count"] == 1

    @pytest.mark.asyncio
    @patch("tsigma.reports.red_light_monitor.fetch_events_split", new_callable=AsyncMock)
    @patch("tsigma.reports.red_light_monitor.get_config_at", new_callable=AsyncMock)
    async def test_violation_after_grace_excluded(self, mock_config, mock_fetch):
        """Detector actuation beyond grace period is not counted."""
        from tsigma.reports.red_light_monitor import RedLightMonitorParams, RedLightMonitorReport

        mock_config.return_value = _config_with_phase(2, (5,))

        t0 = datetime(2025, 6, 15, 8, 0, 0)
        events = [
            _event(EVENT_PHASE_GREEN, 2, t0),
            _event(EVENT_YELLOW_CLEARANCE, 2, t0 + timedelta(seconds=25)),
            _event(EVENT_RED_CLEARANCE, 2, t0 + timedelta(seconds=29)),
            _event(EVENT_PHASE_END, 2, t0 + timedelta(seconds=31)),
            # 5 seconds after phase end — beyond 2-second grace
            _event(EVENT_DETECTOR_ON, 5, t0 + timedelta(seconds=36)),
            _event(EVENT_PHASE_GREEN, 2, t0 + timedelta(seconds=60)),
        ]
        mock_fetch.return_value = _events_to_df(events)

        report = RedLightMonitorReport()
        params = RedLightMonitorParams(signal_id="SIG-001", start=_START_ISO, end=_END_ISO, phase_number=2)
        result = await report.execute(params, _mock_session())

        assert result.empty

    @pytest.mark.asyncio
    @patch("tsigma.reports.red_light_monitor.fetch_events_split", new_callable=AsyncMock)
    @patch("tsigma.reports.red_light_monitor.get_config_at", new_callable=AsyncMock)
    async def test_multiple_violations_one_cycle(self, mock_config, mock_fetch):
        """Multiple detector hits during red clearance are counted."""
        from tsigma.reports.red_light_monitor import RedLightMonitorParams, RedLightMonitorReport

        mock_config.return_value = _config_with_phase(2, (5, 6))

        t0 = datetime(2025, 6, 15, 8, 0, 0)
        events = [
            _event(EVENT_PHASE_GREEN, 2, t0),
            _event(EVENT_YELLOW_CLEARANCE, 2, t0 + timedelta(seconds=25)),
            _event(EVENT_RED_CLEARANCE, 2, t0 + timedelta(seconds=29)),
            _event(EVENT_DETECTOR_ON, 5, t0 + timedelta(seconds=30)),
            _event(EVENT_DETECTOR_ON, 6, t0 + timedelta(seconds=30.5)),
            _event(EVENT_PHASE_END, 2, t0 + timedelta(seconds=31)),
            _event(EVENT_PHASE_GREEN, 2, t0 + timedelta(seconds=60)),
        ]
        mock_fetch.return_value = _events_to_df(events)

        report = RedLightMonitorReport()
        params = RedLightMonitorParams(signal_id="SIG-001", start=_START_ISO, end=_END_ISO, phase_number=2)
        result = await report.execute(params, _mock_session())

        assert len(result) == 1
        assert result.iloc[0]["violation_count"] == 2

    @pytest.mark.asyncio
    @patch("tsigma.reports.red_light_monitor.fetch_events_split", new_callable=AsyncMock)
    @patch("tsigma.reports.red_light_monitor.get_config_at", new_callable=AsyncMock)
    async def test_trailing_cycle_flushed(self, mock_config, mock_fetch):
        """Last cycle with violations is emitted even without a closing green."""
        from tsigma.reports.red_light_monitor import RedLightMonitorParams, RedLightMonitorReport

        mock_config.return_value = _config_with_phase(2, (5,))

        t0 = datetime(2025, 6, 15, 8, 0, 0)
        events = [
            _event(EVENT_PHASE_GREEN, 2, t0),
            _event(EVENT_YELLOW_CLEARANCE, 2, t0 + timedelta(seconds=25)),
            _event(EVENT_RED_CLEARANCE, 2, t0 + timedelta(seconds=29)),
            _event(EVENT_DETECTOR_ON, 5, t0 + timedelta(seconds=30)),
        ]
        mock_fetch.return_value = _events_to_df(events)

        report = RedLightMonitorReport()
        params = RedLightMonitorParams(signal_id="SIG-001", start=_START_ISO, end=_END_ISO, phase_number=2)
        result = await report.execute(params, _mock_session())

        assert len(result) == 1
        assert result.iloc[0]["violation_count"] == 1


# =========================================================================
# Approach Volume — with data
# =========================================================================

class TestApproachVolumeWithData:

    @pytest.mark.asyncio
    @patch("tsigma.reports.sdk.queries.db_facade")
    @patch("tsigma.reports.approach_volume.get_config_at", new_callable=AsyncMock)
    async def test_counts_per_approach(self, mock_config, mock_facade):
        """Detector-on events are counted per approach per time bin."""
        from tsigma.reports.approach_volume import ApproachVolumeParams, ApproachVolumeReport

        mock_config.return_value = _config_with_phase(2, (5, 6))

        t0 = datetime(2025, 6, 15, 8, 0, 0)
        events = [
            _event(EVENT_DETECTOR_ON, 5, t0 + timedelta(seconds=10)),
            _event(EVENT_DETECTOR_ON, 5, t0 + timedelta(seconds=30)),
            _event(EVENT_DETECTOR_ON, 6, t0 + timedelta(seconds=45)),
        ]
        mock_facade.get_dataframe = AsyncMock(return_value=_events_to_df(events))

        report = ApproachVolumeReport()
        params = ApproachVolumeParams(signal_id="SIG-001", start=_START_ISO, end=_END_ISO)
        result = await report.execute(params, _mock_session())

        assert isinstance(result, pd.DataFrame)
        assert len(result) >= 1
        total_vol = result["volume"].sum()
        assert total_vol == 3
        assert (result["direction"] == "NB").all()

    @pytest.mark.asyncio
    @patch("tsigma.reports.sdk.queries.db_facade")
    @patch("tsigma.reports.approach_volume.get_config_at", new_callable=AsyncMock)
    async def test_different_bins(self, mock_config, mock_facade):
        """Events in different 15-min bins produce separate rows."""
        from tsigma.reports.approach_volume import ApproachVolumeParams, ApproachVolumeReport

        mock_config.return_value = _config_with_phase(2, (5,))

        t0 = datetime(2025, 6, 15, 8, 0, 0)
        events = [
            _event(EVENT_DETECTOR_ON, 5, t0 + timedelta(minutes=5)),
            _event(EVENT_DETECTOR_ON, 5, t0 + timedelta(minutes=20)),
        ]
        mock_facade.get_dataframe = AsyncMock(return_value=_events_to_df(events))

        report = ApproachVolumeReport()
        params = ApproachVolumeParams(signal_id="SIG-001", start=_START_ISO, end=_END_ISO)
        result = await report.execute(params, _mock_session())

        assert len(result) == 2
        assert result.iloc[0]["volume"] == 1
        assert result.iloc[1]["volume"] == 1


# =========================================================================
# Arrivals on Green — with data
# =========================================================================

class TestArrivalsOnGreenWithData:

    @pytest.mark.asyncio
    @patch("tsigma.reports.arrivals_on_green.db_facade")
    @patch("tsigma.reports.arrivals_on_green.load_channel_to_phase", new_callable=AsyncMock)
    async def test_arrivals_during_green(self, mock_ch_map, mock_facade):
        """Detector-on during green is counted as arrival on green."""
        from tsigma.reports.arrivals_on_green import ArrivalsOnGreenParams, ArrivalsOnGreenReport

        mock_ch_map.return_value = {5: 2}

        t0 = datetime(2025, 6, 15, 8, 0, 0)
        events = [
            _event(EVENT_PHASE_GREEN, 2, t0),
            _event(EVENT_DETECTOR_ON, 5, t0 + timedelta(seconds=5)),
            _event(EVENT_DETECTOR_ON, 5, t0 + timedelta(seconds=10)),
            _event(EVENT_YELLOW_CLEARANCE, 2, t0 + timedelta(seconds=30)),
            _event(EVENT_DETECTOR_ON, 5, t0 + timedelta(seconds=35)),
        ]
        mock_facade.get_dataframe = AsyncMock(return_value=_events_to_df(events))

        report = ArrivalsOnGreenReport()
        params = ArrivalsOnGreenParams(signal_id="SIG-001", start=_START_ISO, end=_END_ISO)
        result = await report.execute(params, _mock_session())

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        r = result.iloc[0]
        assert r["phase_number"] == 2
        assert r["total_arrivals"] == 3
        assert r["arrivals_on_green"] == 2
        assert r["aog_percentage"] == pytest.approx(66.7, abs=0.1)

    @pytest.mark.asyncio
    @patch("tsigma.reports.arrivals_on_green.db_facade")
    @patch("tsigma.reports.arrivals_on_green.load_channel_to_phase", new_callable=AsyncMock)
    async def test_all_arrivals_on_red(self, mock_ch_map, mock_facade):
        """Detector-on only during red gives 0% AOG."""
        from tsigma.reports.arrivals_on_green import ArrivalsOnGreenParams, ArrivalsOnGreenReport

        mock_ch_map.return_value = {5: 2}

        t0 = datetime(2025, 6, 15, 8, 0, 0)
        events = [
            _event(EVENT_DETECTOR_ON, 5, t0 + timedelta(seconds=5)),
            _event(EVENT_PHASE_GREEN, 2, t0 + timedelta(seconds=10)),
            _event(EVENT_YELLOW_CLEARANCE, 2, t0 + timedelta(seconds=40)),
        ]
        mock_facade.get_dataframe = AsyncMock(return_value=_events_to_df(events))

        report = ArrivalsOnGreenReport()
        params = ArrivalsOnGreenParams(signal_id="SIG-001", start=_START_ISO, end=_END_ISO)
        result = await report.execute(params, _mock_session())

        assert len(result) == 1
        assert result.iloc[0]["total_arrivals"] == 1
        assert result.iloc[0]["arrivals_on_green"] == 0
        assert result.iloc[0]["aog_percentage"] == 0.0


# =========================================================================
# Green Time Utilization — with data
# =========================================================================

class TestGreenTimeUtilizationWithData:

    @pytest.mark.asyncio
    @patch("tsigma.reports.green_time_utilization.fetch_plans", new_callable=AsyncMock)
    @patch("tsigma.reports.sdk.queries.db_facade")
    @patch("tsigma.reports.green_time_utilization.load_channel_to_phase", new_callable=AsyncMock)
    async def test_single_cycle(self, mock_ch_map, mock_facade, mock_plans):
        """One green-yellow-red cycle produces utilization data."""
        from tsigma.reports.green_time_utilization import GreenTimeUtilizationParams, GreenTimeUtilizationReport

        mock_ch_map.return_value = {5: 2}
        mock_plans.return_value = []  # no plan -> programmed_split = 0

        t0 = datetime(2025, 6, 15, 8, 0, 0)
        events = [
            _event(EVENT_PHASE_GREEN, 2, t0),
            _event(EVENT_DETECTOR_ON, 5, t0 + timedelta(seconds=5)),
            _event(EVENT_DETECTOR_ON, 5, t0 + timedelta(seconds=10)),
            _event(EVENT_YELLOW_CLEARANCE, 2, t0 + timedelta(seconds=25)),
            _event(EVENT_RED_CLEARANCE, 2, t0 + timedelta(seconds=29)),
        ]
        mock_facade.get_dataframe = AsyncMock(return_value=_events_to_df(events))

        report = GreenTimeUtilizationReport()
        params = GreenTimeUtilizationParams(signal_id="SIG-001", start=_START_ISO, end=_END_ISO)
        result = await report.execute(params, _mock_session())

        assert isinstance(result, pd.DataFrame)
        assert len(result) >= 1
        # DataFrame has columns: phase_number, x_bin, bin_start, cycle_count,
        # avg_green_seconds, programmed_split_seconds, utilization_pct
        assert (result["phase_number"] == 2).all()
        assert result.iloc[0]["cycle_count"] == 1
        assert result.iloc[0]["avg_green_seconds"] == 25.0


# =========================================================================
# Left Turn Gap — with data
# =========================================================================

class TestLeftTurnGapWithData:

    @pytest.mark.asyncio
    @patch("tsigma.reports.left_turn_gap.fetch_events_split", new_callable=AsyncMock)
    @patch("tsigma.reports.left_turn_gap.get_config_at", new_callable=AsyncMock)
    async def test_gap_classification(self, mock_config, mock_fetch):
        """Gaps between detector-off and detector-on are classified."""
        from tsigma.reports.left_turn_gap import LeftTurnGapParams, LeftTurnGapReport

        mock_config.return_value = _config_with_phase(2, (5,))

        t0 = datetime(2025, 6, 15, 8, 0, 0)
        # One cycle with two vehicles creating one gap.
        # Det on -> det off -> gap of 5s -> det on -> det off
        events = [
            _event(EVENT_PHASE_GREEN, 2, t0),
            _event(EVENT_DETECTOR_ON, 5, t0 + timedelta(seconds=2)),
            _event(EVENT_DETECTOR_OFF, 5, t0 + timedelta(seconds=4)),
            # 5-second gap (sufficient for 1-2 opposing lanes: threshold 4.1s)
            _event(EVENT_DETECTOR_ON, 5, t0 + timedelta(seconds=9)),
            _event(EVENT_DETECTOR_OFF, 5, t0 + timedelta(seconds=11)),
            _event(EVENT_YELLOW_CLEARANCE, 2, t0 + timedelta(seconds=30)),
        ]
        mock_fetch.return_value = _events_to_df(events)

        report = LeftTurnGapReport()
        params = LeftTurnGapParams(signal_id="SIG-001", start=_START_ISO, end=_END_ISO, phase_number=2)
        result = await report.execute(params, _mock_session())

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        c = result.iloc[0]
        assert c["total_gaps"] == 1
        assert c["sufficient_gaps"] == 1  # 5s >= 4.1s critical gap
        assert c["green_duration"] == 30.0

    @pytest.mark.asyncio
    @patch("tsigma.reports.left_turn_gap.fetch_events_split", new_callable=AsyncMock)
    @patch("tsigma.reports.left_turn_gap.get_config_at", new_callable=AsyncMock)
    async def test_single_cycle_has_data(self, mock_config, mock_fetch):
        """Single cycle with detector activity produces a row."""
        from tsigma.reports.left_turn_gap import LeftTurnGapParams, LeftTurnGapReport

        mock_config.return_value = _config_with_phase(2, (5,))

        t0 = datetime(2025, 6, 15, 8, 0, 0)
        events = [
            _event(EVENT_PHASE_GREEN, 2, t0),
            _event(EVENT_DETECTOR_ON, 5, t0 + timedelta(seconds=5)),
            _event(EVENT_DETECTOR_OFF, 5, t0 + timedelta(seconds=7)),
            _event(EVENT_YELLOW_CLEARANCE, 2, t0 + timedelta(seconds=30)),
        ]
        mock_fetch.return_value = _events_to_df(events)

        report = LeftTurnGapReport()
        params = LeftTurnGapParams(signal_id="SIG-001", start=_START_ISO, end=_END_ISO, phase_number=2)
        result = await report.execute(params, _mock_session())

        assert len(result) == 1

    @pytest.mark.asyncio
    @patch("tsigma.reports.left_turn_gap.fetch_events_split", new_callable=AsyncMock)
    @patch("tsigma.reports.left_turn_gap.get_config_at", new_callable=AsyncMock)
    async def test_split_failure_analysis(self, mock_config, mock_fetch):
        """Split failure is computed when left_turn_phase is provided."""
        from tsigma.reports.left_turn_gap import LeftTurnGapParams, LeftTurnGapReport

        # Config: phase 2 = through (det ch 5), phase 4 = left turn (det ch 7)
        approach_thru = ApproachSnapshot(
            approach_id="APP-1", signal_id="SIG-001", direction_type_id=1,
            protected_phase_number=2, permissive_phase_number=None,
            is_protected_phase_overlap=False, is_permissive_phase_overlap=False,
            ped_phase_number=None, mph=None, description="NB Thru",
        )
        approach_lt = ApproachSnapshot(
            approach_id="APP-2", signal_id="SIG-001", direction_type_id=1,
            protected_phase_number=4, permissive_phase_number=None,
            is_protected_phase_overlap=False, is_permissive_phase_overlap=False,
            ped_phase_number=None, mph=None, description="NB Left",
        )
        det_thru = DetectorSnapshot(
            detector_id="DET-5", approach_id="APP-1",
            detector_channel=5, distance_from_stop_bar=None,
            min_speed_filter=None, lane_number=1,
        )
        det_lt = DetectorSnapshot(
            detector_id="DET-7", approach_id="APP-2",
            detector_channel=7, distance_from_stop_bar=None,
            min_speed_filter=None, lane_number=1,
        )
        config = SignalConfig(
            signal_id="SIG-001", as_of=datetime(2025, 6, 15),
            from_audit=False,
            approaches=[approach_thru, approach_lt],
            detectors=[det_thru, det_lt],
        )
        mock_config.return_value = config

        t0 = datetime(2025, 6, 15, 8, 0, 0)
        events = [
            # Through phase cycle (phase 2)
            _event(EVENT_PHASE_GREEN, 2, t0),
            _event(EVENT_DETECTOR_ON, 5, t0 + timedelta(seconds=2)),
            _event(EVENT_DETECTOR_OFF, 5, t0 + timedelta(seconds=4)),
            _event(EVENT_YELLOW_CLEARANCE, 2, t0 + timedelta(seconds=30)),
            # Left turn phase cycle (phase 4) — high occupancy = split failure
            _event(EVENT_PHASE_GREEN, 4, t0 + timedelta(seconds=32)),
            _event(EVENT_DETECTOR_ON, 7, t0 + timedelta(seconds=32)),
            _event(EVENT_DETECTOR_OFF, 7, t0 + timedelta(seconds=60)),
            _event(EVENT_YELLOW_CLEARANCE, 4, t0 + timedelta(seconds=60)),
            _event(EVENT_RED_CLEARANCE, 4, t0 + timedelta(seconds=64)),
        ]
        mock_fetch.return_value = _events_to_df(events)

        report = LeftTurnGapReport()
        params = LeftTurnGapParams(
            signal_id="SIG-001", start=_START_ISO, end=_END_ISO,
            phase_number=2, left_turn_phase=4,
        )
        result = await report.execute(params, _mock_session())

        # Returns cycles DataFrame — through phase cycles should be present
        assert isinstance(result, pd.DataFrame)
        assert len(result) >= 1

    @pytest.mark.asyncio
    @patch("tsigma.reports.left_turn_gap.fetch_events_split", new_callable=AsyncMock)
    @patch("tsigma.reports.left_turn_gap.get_config_at", new_callable=AsyncMock)
    async def test_ped_actuation_during_green(self, mock_config, mock_fetch):
        """Ped calls during green are counted in per-cycle rows."""
        from tsigma.reports.left_turn_gap import LeftTurnGapParams, LeftTurnGapReport

        approach = ApproachSnapshot(
            approach_id="APP-1", signal_id="SIG-001", direction_type_id=1,
            protected_phase_number=2, permissive_phase_number=None,
            is_protected_phase_overlap=False, is_permissive_phase_overlap=False,
            ped_phase_number=8, mph=None, description="NB Thru",
        )
        det = DetectorSnapshot(
            detector_id="DET-5", approach_id="APP-1",
            detector_channel=5, distance_from_stop_bar=None,
            min_speed_filter=None, lane_number=1,
        )
        config = SignalConfig(
            signal_id="SIG-001", as_of=datetime(2025, 6, 15),
            from_audit=False, approaches=[approach], detectors=[det],
        )
        mock_config.return_value = config

        t0 = datetime(2025, 6, 15, 8, 0, 0)
        events = [
            _event(EVENT_PHASE_GREEN, 2, t0),
            _event(EVENT_PED_CALL, 8, t0 + timedelta(seconds=5)),
            _event(EVENT_DETECTOR_ON, 5, t0 + timedelta(seconds=6)),
            _event(EVENT_DETECTOR_OFF, 5, t0 + timedelta(seconds=8)),
            _event(EVENT_YELLOW_CLEARANCE, 2, t0 + timedelta(seconds=30)),
        ]
        mock_fetch.return_value = _events_to_df(events)

        report = LeftTurnGapReport()
        params = LeftTurnGapParams(signal_id="SIG-001", start=_START_ISO, end=_END_ISO, phase_number=2)
        result = await report.execute(params, _mock_session())

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        # ped_calls_in_cycle column tracks per-cycle ped calls
        assert "ped_calls_in_cycle" in result.columns
        assert result.iloc[0]["ped_calls_in_cycle"] == 1

    @pytest.mark.asyncio
    @patch("tsigma.reports.left_turn_gap.fetch_events_split", new_callable=AsyncMock)
    @patch("tsigma.reports.left_turn_gap.get_config_at", new_callable=AsyncMock)
    async def test_multiple_cycles(self, mock_config, mock_fetch):
        """Multiple cycles produce multiple rows."""
        from tsigma.reports.left_turn_gap import LeftTurnGapParams, LeftTurnGapReport

        mock_config.return_value = _config_with_phase(2, (5,))

        t0 = datetime(2025, 6, 15, 8, 0, 0)
        # Multiple cycles to produce enough data
        events = []
        for i in range(6):
            base = t0 + timedelta(minutes=i * 2)
            events.extend([
                _event(EVENT_PHASE_GREEN, 2, base),
                _event(EVENT_DETECTOR_ON, 5, base + timedelta(seconds=3)),
                _event(EVENT_DETECTOR_OFF, 5, base + timedelta(seconds=5)),
                _event(EVENT_DETECTOR_ON, 5, base + timedelta(seconds=12)),
                _event(EVENT_DETECTOR_OFF, 5, base + timedelta(seconds=14)),
                _event(EVENT_YELLOW_CLEARANCE, 2, base + timedelta(seconds=30)),
            ])
        mock_fetch.return_value = _events_to_df(events)

        report = LeftTurnGapReport()
        params = LeftTurnGapParams(signal_id="SIG-001", start=_START_ISO, end=_END_ISO, phase_number=2)
        result = await report.execute(params, _mock_session())

        assert len(result) == 6
        assert (result["total_gaps"] > 0).all()

    @pytest.mark.asyncio
    @patch("tsigma.reports.left_turn_gap.fetch_events_split", new_callable=AsyncMock)
    @patch("tsigma.reports.left_turn_gap.get_config_at", new_callable=AsyncMock)
    async def test_marginal_and_insufficient_gaps(self, mock_config, mock_fetch):
        """Short gaps are classified as marginal or insufficient."""
        from tsigma.reports.left_turn_gap import LeftTurnGapParams, LeftTurnGapReport

        # 3+ opposing lanes => critical gap = 5.3s, marginal = 3.8s
        mock_config.return_value = _config_with_phase(2, (5, 6, 7))

        t0 = datetime(2025, 6, 15, 8, 0, 0)
        events = [
            _event(EVENT_PHASE_GREEN, 2, t0),
            # First vehicle
            _event(EVENT_DETECTOR_ON, 5, t0 + timedelta(seconds=1)),
            _event(EVENT_DETECTOR_OFF, 5, t0 + timedelta(seconds=3)),
            # 2s gap -> insufficient (< 3.8s marginal threshold)
            _event(EVENT_DETECTOR_ON, 5, t0 + timedelta(seconds=5)),
            _event(EVENT_DETECTOR_OFF, 5, t0 + timedelta(seconds=7)),
            # 4s gap -> marginal (>= 3.8s but < 5.3s)
            _event(EVENT_DETECTOR_ON, 5, t0 + timedelta(seconds=11)),
            _event(EVENT_DETECTOR_OFF, 5, t0 + timedelta(seconds=13)),
            # 6s gap -> sufficient (>= 5.3s)
            _event(EVENT_DETECTOR_ON, 5, t0 + timedelta(seconds=19)),
            _event(EVENT_DETECTOR_OFF, 5, t0 + timedelta(seconds=21)),
            _event(EVENT_YELLOW_CLEARANCE, 2, t0 + timedelta(seconds=30)),
        ]
        mock_fetch.return_value = _events_to_df(events)

        report = LeftTurnGapReport()
        params = LeftTurnGapParams(
            signal_id="SIG-001", start=_START_ISO, end=_END_ISO,
            phase_number=2, opposing_lanes=3,
        )
        result = await report.execute(params, _mock_session())

        c = result.iloc[0]
        assert c["insufficient_gaps"] == 1
        assert c["marginal_gaps"] == 1
        assert c["sufficient_gaps"] == 1

    @pytest.mark.asyncio
    @patch("tsigma.reports.left_turn_gap.fetch_events_split", new_callable=AsyncMock)
    @patch("tsigma.reports.left_turn_gap.get_config_at", new_callable=AsyncMock)
    async def test_no_detectors_returns_empty(self, mock_config, mock_fetch):
        """When no detectors configured, returns empty result."""
        from tsigma.reports.left_turn_gap import LeftTurnGapParams, LeftTurnGapReport

        # Config with phase 2 but no detectors on phase 99
        mock_config.return_value = _config_with_phase(2, (5,))

        report = LeftTurnGapReport()
        params = LeftTurnGapParams(signal_id="SIG-001", start=_START_ISO, end=_END_ISO, phase_number=99)
        result = await report.execute(params, _mock_session())

        assert result.empty


# =========================================================================
# Link Pivot — with data
# =========================================================================

class TestLinkPivotWithData:

    @pytest.mark.asyncio
    @patch("tsigma.reports.link_pivot.fetch_events", new_callable=AsyncMock)
    async def test_offset_between_two_signals(self, mock_fetch):
        """Two signals with green starts produce an offset calculation."""
        from tsigma.reports.link_pivot import LinkPivotParams, LinkPivotReport

        t0 = datetime(2025, 6, 15, 8, 0, 0)

        # fetch_events is called once per signal; return appropriate greens
        async def side_effect(signal_id, start, end, codes, **kw):
            if signal_id == "SIG-001":
                return _events_to_df([
                    _event(EVENT_PHASE_GREEN, 2, t0),
                    _event(EVENT_PHASE_GREEN, 2, t0 + timedelta(seconds=90)),
                ])
            else:
                return _events_to_df([
                    _event(EVENT_PHASE_GREEN, 2, t0 + timedelta(seconds=15)),
                    _event(EVENT_PHASE_GREEN, 2, t0 + timedelta(seconds=105)),
                ])

        mock_fetch.side_effect = side_effect

        session = _mock_session()
        # Mock route query: route name
        route_row = MagicMock()
        route_row.__getitem__ = lambda self, idx: "Test Corridor"
        # Mock signals query
        signal_rows = [
            ("SIG-001", 1, 2),
            ("SIG-002", 2, 2),
        ]
        mock_result_route = MagicMock()
        mock_result_route.one_or_none.return_value = route_row
        mock_result_signals = MagicMock()
        mock_result_signals.all.return_value = signal_rows

        session.execute = AsyncMock(
            side_effect=[mock_result_route, mock_result_signals]
        )

        report = LinkPivotReport()
        params = LinkPivotParams(
            route_id="ROUTE-1", start=_START_ISO, end=_END_ISO, direction=1,
        )
        result = await report.execute(params, session)

        # Returns DataFrame with columns: from_signal, to_signal, avg_offset, stddev_offset, sample_count
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert result.iloc[0]["avg_offset"] == 15.0
        assert result.iloc[0]["sample_count"] == 2


# =========================================================================
# Ped Delay — with data
# =========================================================================

class TestPedDelayWithData:

    @pytest.mark.asyncio
    @patch("tsigma.reports.ped_delay.fetch_events_split", new_callable=AsyncMock)
    @patch("tsigma.reports.ped_delay.load_channel_to_ped_phase", new_callable=AsyncMock)
    async def test_ped_delay_measured(self, mock_ch_map, mock_fetch):
        """Delay from ped detector-on to walk event is measured."""
        from tsigma.reports.ped_delay import PedDelayParams, PedDelayReport

        mock_ch_map.return_value = {10: 4}  # channel 10 -> ped phase 4

        t0 = datetime(2025, 6, 15, 8, 0, 0)
        events = [
            _event(EVENT_DETECTOR_ON, 10, t0),                         # ped press
            _event(EVENT_PED_WALK, 4, t0 + timedelta(seconds=20)),     # walk starts
            _event(EVENT_DETECTOR_ON, 10, t0 + timedelta(seconds=60)), # second press
            _event(EVENT_PED_WALK, 4, t0 + timedelta(seconds=75)),     # walk starts
        ]
        mock_fetch.return_value = _events_to_df(events)

        report = PedDelayReport()
        params = PedDelayParams(signal_id="SIG-001", start=_START_ISO, end=_END_ISO)
        result = await report.execute(params, _mock_session())

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        r = result.iloc[0]
        assert r["phase_number"] == 4
        assert r["ped_actuations"] == 2
        assert r["avg_delay_seconds"] == pytest.approx(17.5)  # (20+15)/2
        assert r["max_delay_seconds"] == 20.0

    @pytest.mark.asyncio
    @patch("tsigma.reports.ped_delay.fetch_events_split", new_callable=AsyncMock)
    @patch("tsigma.reports.ped_delay.load_channel_to_ped_phase", new_callable=AsyncMock)
    async def test_multiple_presses_before_walk(self, mock_ch_map, mock_fetch):
        """Multiple ped presses before one walk are each measured."""
        from tsigma.reports.ped_delay import PedDelayParams, PedDelayReport

        mock_ch_map.return_value = {10: 4}

        t0 = datetime(2025, 6, 15, 8, 0, 0)
        events = [
            _event(EVENT_DETECTOR_ON, 10, t0),
            _event(EVENT_DETECTOR_ON, 10, t0 + timedelta(seconds=10)),
            _event(EVENT_PED_WALK, 4, t0 + timedelta(seconds=30)),
        ]
        mock_fetch.return_value = _events_to_df(events)

        report = PedDelayReport()
        params = PedDelayParams(signal_id="SIG-001", start=_START_ISO, end=_END_ISO)
        result = await report.execute(params, _mock_session())

        assert result.iloc[0]["ped_actuations"] == 2
        assert result.iloc[0]["avg_delay_seconds"] == pytest.approx(25.0)  # (30+20)/2
        assert result.iloc[0]["max_delay_seconds"] == 30.0


# =========================================================================
# Phase Termination — with data
# =========================================================================

class TestPhaseTerminationWithData:

    @pytest.mark.asyncio
    @patch("tsigma.reports.phase_termination.fetch_events", new_callable=AsyncMock)
    async def test_termination_types_counted(self, mock_fetch):
        """Gap-out, max-out, force-off events are counted per phase per bin."""
        from tsigma.reports.phase_termination import PhaseTerminationParams, PhaseTerminationReport

        t0 = datetime(2025, 6, 15, 8, 0, 0)
        events = [
            _event(EVENT_PHASE_GREEN, 2, t0),
            _event(EVENT_GAP_OUT, 2, t0 + timedelta(seconds=20)),
            _event(EVENT_PHASE_GREEN, 2, t0 + timedelta(seconds=60)),
            _event(EVENT_MAX_OUT, 2, t0 + timedelta(seconds=100)),
            _event(EVENT_PHASE_GREEN, 2, t0 + timedelta(seconds=120)),
            _event(EVENT_FORCE_OFF, 2, t0 + timedelta(seconds=150)),
        ]
        mock_fetch.return_value = _events_to_df(events)

        report = PhaseTerminationReport()
        params = PhaseTerminationParams(signal_id="SIG-001", start=_START_ISO, end=_END_ISO)
        result = await report.execute(params, _mock_session())

        assert isinstance(result, pd.DataFrame)
        # All in same 15-min bin
        phase2_rows = result[result["phase_number"] == 2]
        assert len(phase2_rows) == 1
        r = phase2_rows.iloc[0]
        assert r["gap_out_count"] == 1
        assert r["max_out_count"] == 1
        assert r["force_off_count"] == 1
        assert r["total_cycles"] == 3

    @pytest.mark.asyncio
    @patch("tsigma.reports.phase_termination.fetch_events", new_callable=AsyncMock)
    async def test_multiple_phases(self, mock_fetch):
        """Events for different phases produce separate rows."""
        from tsigma.reports.phase_termination import PhaseTerminationParams, PhaseTerminationReport

        t0 = datetime(2025, 6, 15, 8, 0, 0)
        events = [
            _event(EVENT_PHASE_GREEN, 2, t0),
            _event(EVENT_GAP_OUT, 2, t0 + timedelta(seconds=20)),
            _event(EVENT_PHASE_GREEN, 4, t0 + timedelta(seconds=25)),
            _event(EVENT_FORCE_OFF, 4, t0 + timedelta(seconds=55)),
        ]
        mock_fetch.return_value = _events_to_df(events)

        report = PhaseTerminationReport()
        params = PhaseTerminationParams(signal_id="SIG-001", start=_START_ISO, end=_END_ISO)
        result = await report.execute(params, _mock_session())

        phases = set(result["phase_number"].tolist())
        assert 2 in phases
        assert 4 in phases


# =========================================================================
# Purdue Diagram — with data
# =========================================================================

class TestPurdueDiagramWithData:

    @pytest.mark.asyncio
    @patch("tsigma.reports.purdue_diagram.fetch_events_split", new_callable=AsyncMock)
    @patch("tsigma.reports.sdk.cycles.db_facade")
    @patch("tsigma.reports.purdue_diagram.get_config_at", new_callable=AsyncMock)
    async def test_single_cycle(self, mock_config, mock_cycles_facade, mock_fetch):
        """A complete cycle with detector activations is returned."""
        from tsigma.reports.purdue_diagram import PurdueDiagramParams, PurdueDiagramReport

        mock_config.return_value = _config_with_phase(2, (5,))
        # Aggregate path returns empty -> falls through to raw events
        mock_cycles_facade.get_dataframe = AsyncMock(return_value=pd.DataFrame())

        t0 = datetime(2025, 6, 15, 8, 0, 0)
        events = [
            _event(EVENT_PHASE_GREEN, 2, t0),
            _event(EVENT_DETECTOR_ON, 5, t0 + timedelta(seconds=5)),
            _event(EVENT_DETECTOR_ON, 5, t0 + timedelta(seconds=12)),
            _event(EVENT_YELLOW_CLEARANCE, 2, t0 + timedelta(seconds=25)),
            _event(EVENT_RED_CLEARANCE, 2, t0 + timedelta(seconds=29)),
            _event(EVENT_PHASE_END, 2, t0 + timedelta(seconds=31)),
        ]
        mock_fetch.return_value = _events_to_df(events)

        report = PurdueDiagramReport()
        params = PurdueDiagramParams(signal_id="SIG-001", start=_START_ISO, end=_END_ISO, phase_number=2)
        result = await report.execute(params, _mock_session())

        assert isinstance(result, pd.DataFrame)
        assert len(result) >= 1
        # DataFrame has cycle-level columns like green_start, yellow_start, etc.
        assert "green_start" in result.columns or "cycle_start" in result.columns

    @pytest.mark.asyncio
    @patch("tsigma.reports.purdue_diagram.fetch_events_split", new_callable=AsyncMock)
    @patch("tsigma.reports.sdk.cycles.db_facade")
    @patch("tsigma.reports.purdue_diagram.get_config_at", new_callable=AsyncMock)
    async def test_two_cycles(self, mock_config, mock_cycles_facade, mock_fetch):
        """Two consecutive cycles are both captured."""
        from tsigma.reports.purdue_diagram import PurdueDiagramParams, PurdueDiagramReport

        mock_config.return_value = _config_with_phase(2, (5,))
        # Aggregate path returns empty -> falls through to raw events
        mock_cycles_facade.get_dataframe = AsyncMock(return_value=pd.DataFrame())

        t0 = datetime(2025, 6, 15, 8, 0, 0)
        events = [
            _event(EVENT_PHASE_GREEN, 2, t0),
            _event(EVENT_DETECTOR_ON, 5, t0 + timedelta(seconds=5)),
            _event(EVENT_YELLOW_CLEARANCE, 2, t0 + timedelta(seconds=25)),
            _event(EVENT_PHASE_GREEN, 2, t0 + timedelta(seconds=60)),
            _event(EVENT_DETECTOR_ON, 5, t0 + timedelta(seconds=65)),
            _event(EVENT_YELLOW_CLEARANCE, 2, t0 + timedelta(seconds=85)),
        ]
        mock_fetch.return_value = _events_to_df(events)

        report = PurdueDiagramReport()
        params = PurdueDiagramParams(signal_id="SIG-001", start=_START_ISO, end=_END_ISO, phase_number=2)
        result = await report.execute(params, _mock_session())

        assert len(result) == 2


# =========================================================================
# Time-Space Diagram — with data
# =========================================================================

class TestTimeSpaceDiagramWithData:

    @pytest.mark.asyncio
    @patch("tsigma.reports.time_space_diagram.fetch_events", new_callable=AsyncMock)
    async def test_phase_intervals_built(self, mock_fetch):
        """Phase green/yellow/red events produce intervals for each signal."""
        from tsigma.reports.time_space_diagram import TimeSpaceDiagramParams, TimeSpaceDiagramReport

        t0 = datetime(2025, 6, 15, 8, 0, 0)

        async def side_effect(signal_id, start, end, codes, **kw):
            return _events_to_df([
                _event(EVENT_PHASE_GREEN, 2, t0),
                _event(EVENT_YELLOW_CLEARANCE, 2, t0 + timedelta(seconds=25)),
                _event(EVENT_RED_CLEARANCE, 2, t0 + timedelta(seconds=29)),
            ])

        mock_fetch.side_effect = side_effect

        report = TimeSpaceDiagramReport()
        params = TimeSpaceDiagramParams(
            signal_ids=["SIG-001"],
            start_time=_START_ISO,
            end_time=_END_ISO,
            direction_phase_map={"SIG-001": 2},
        )
        result = await report.execute(params, _mock_session())

        # Returns DataFrame with columns: signal_id, start, end, state
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2  # green->yellow, yellow->red
        assert result.iloc[0]["state"] == "green"
        assert result.iloc[1]["state"] == "yellow"

    @pytest.mark.asyncio
    @patch("tsigma.reports.time_space_diagram.fetch_events", new_callable=AsyncMock)
    async def test_multiple_signals(self, mock_fetch):
        """Multiple signals produce rows for each."""
        from tsigma.reports.time_space_diagram import TimeSpaceDiagramParams, TimeSpaceDiagramReport

        t0 = datetime(2025, 6, 15, 8, 0, 0)
        mock_fetch.return_value = _events_to_df([
            _event(EVENT_PHASE_GREEN, 2, t0),
            _event(EVENT_YELLOW_CLEARANCE, 2, t0 + timedelta(seconds=25)),
        ])

        report = TimeSpaceDiagramReport()
        params = TimeSpaceDiagramParams(
            signal_ids=["SIG-001", "SIG-002"],
            start_time=_START_ISO,
            end_time=_END_ISO,
            direction_phase_map={"SIG-001": 2, "SIG-002": 2},
        )
        result = await report.execute(params, _mock_session())

        assert isinstance(result, pd.DataFrame)
        # Each signal has 1 interval (green->yellow), so 2 total
        assert len(result) == 2
        assert set(result["signal_id"].tolist()) == {"SIG-001", "SIG-002"}


# =========================================================================
# Transit Signal Priority — with data
# =========================================================================

class TestTransitSignalPriorityWithData:

    @pytest.mark.asyncio
    @patch("tsigma.reports.transit_signal_priority.fetch_events", new_callable=AsyncMock)
    async def test_tsp_event_counts(self, mock_fetch):
        """TSP check-in / early-green / extend-green events are counted per bin."""
        from tsigma.reports.sdk.events import EVENT_TSP_EXTEND_GREEN
        from tsigma.reports.transit_signal_priority import TransitSignalPriorityParams, TransitSignalPriorityReport

        t0 = datetime(2025, 6, 15, 8, 0, 0)
        events = [
            _event(EVENT_TSP_CHECK_IN, 1, t0),              # 112 — request
            _event(EVENT_TSP_EARLY_GREEN, 1, t0 + timedelta(seconds=5)),   # 113 — early green
            _event(EVENT_TSP_EXTEND_GREEN, 1, t0 + timedelta(seconds=30)), # 114 — extend green
        ]
        mock_fetch.return_value = _events_to_df(events)

        report = TransitSignalPriorityReport()
        params = TransitSignalPriorityParams(
            signal_id="SIG-001", start_time=_START_ISO, end_time=_END_ISO,
        )
        result = await report.execute(params, _mock_session())

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        r = result.iloc[0]
        assert r["tsp_requests"] == 1
        assert r["tsp_adjustments"] == 1
        assert r["tsp_early_green"] == 1

    @pytest.mark.asyncio
    @patch("tsigma.reports.transit_signal_priority.fetch_events", new_callable=AsyncMock)
    async def test_green_duration_with_tsp(self, mock_fetch):
        """Phase green duration is classified as with/without TSP."""
        from tsigma.reports.transit_signal_priority import TransitSignalPriorityParams, TransitSignalPriorityReport

        t0 = datetime(2025, 6, 15, 8, 0, 0)
        events = [
            # TSP active period
            _event(EVENT_TSP_CHECK_IN, 1, t0),
            # Phase green during TSP
            _event(EVENT_PHASE_GREEN, 2, t0 + timedelta(seconds=5)),
            _event(EVENT_YELLOW_CLEARANCE, 2, t0 + timedelta(seconds=40)),
            _event(EVENT_TSP_EXTEND_GREEN, 1, t0 + timedelta(seconds=45)),
            # Phase green without TSP
            _event(EVENT_PHASE_GREEN, 2, t0 + timedelta(seconds=90)),
            _event(EVENT_YELLOW_CLEARANCE, 2, t0 + timedelta(seconds=115)),
        ]
        mock_fetch.return_value = _events_to_df(events)

        report = TransitSignalPriorityReport()
        params = TransitSignalPriorityParams(
            signal_id="SIG-001", start_time=_START_ISO, end_time=_END_ISO,
            phase_number=2,
        )
        result = await report.execute(params, _mock_session())

        assert len(result) >= 1
        r = result.iloc[0]
        assert r["avg_green_with_tsp"] == 35.0
        assert r["avg_green_without_tsp"] == 25.0


# =========================================================================
# Turning Movement Counts — with data
# =========================================================================

class TestTurningMovementCountsWithData:

    @pytest.mark.asyncio
    @patch("tsigma.reports.turning_movement_counts.fetch_events", new_callable=AsyncMock)
    @patch("tsigma.reports.turning_movement_counts.load_channel_to_approach", new_callable=AsyncMock)
    async def test_counts_by_approach(self, mock_ch_map, mock_fetch):
        """Detector-on events are counted per approach direction per bin."""
        from tsigma.reports.turning_movement_counts import TurningMovementCountsParams, TurningMovementCountsReport

        mock_ch_map.return_value = {
            5: {"approach_id": "APP-1", "direction_type_id": 1},
            6: {"approach_id": "APP-2", "direction_type_id": 3},
        }

        t0 = datetime(2025, 6, 15, 8, 0, 0)
        events = [
            _event(EVENT_DETECTOR_ON, 5, t0 + timedelta(seconds=10)),
            _event(EVENT_DETECTOR_ON, 5, t0 + timedelta(seconds=20)),
            _event(EVENT_DETECTOR_ON, 6, t0 + timedelta(seconds=15)),
        ]
        mock_fetch.return_value = _events_to_df(events)

        report = TurningMovementCountsReport()
        params = TurningMovementCountsParams(signal_id="SIG-001", start=_START_ISO, end=_END_ISO)
        result = await report.execute(params, _mock_session())

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        nb_row = result[result["direction"] == "NB"].iloc[0]
        eb_row = result[result["direction"] == "EB"].iloc[0]
        assert nb_row["volume"] == 2
        assert eb_row["volume"] == 1

    @pytest.mark.asyncio
    @patch("tsigma.reports.turning_movement_counts.fetch_events", new_callable=AsyncMock)
    @patch("tsigma.reports.turning_movement_counts.load_channel_to_approach", new_callable=AsyncMock)
    async def test_empty_channels(self, mock_ch_map, mock_fetch):
        """No detector channels returns empty DataFrame."""
        from tsigma.reports.turning_movement_counts import TurningMovementCountsParams, TurningMovementCountsReport

        mock_ch_map.return_value = {}
        mock_fetch.return_value = _events_to_df([])

        report = TurningMovementCountsReport()
        params = TurningMovementCountsParams(signal_id="SIG-001", start=_START_ISO, end=_END_ISO)
        result = await report.execute(params, _mock_session())

        assert result.empty


# =========================================================================
# Wait Time — with data
# =========================================================================

class TestWaitTimeWithData:

    @pytest.mark.asyncio
    @patch("tsigma.reports.wait_time.fetch_events_split", new_callable=AsyncMock)
    @patch("tsigma.reports.wait_time.get_config_at", new_callable=AsyncMock)
    async def test_wait_time_measured(self, mock_config, mock_fetch):
        """Detector-on during red produces wait time until green."""
        from tsigma.reports.wait_time import WaitTimeParams, WaitTimeReport

        mock_config.return_value = _config_with_phase(2, (5,))

        t0 = datetime(2025, 6, 15, 8, 0, 0)
        events = [
            _event(EVENT_PHASE_GREEN, 2, t0),
            _event(EVENT_PHASE_END, 2, t0 + timedelta(seconds=35)),
            # Red interval: vehicles arrive
            _event(EVENT_DETECTOR_ON, 5, t0 + timedelta(seconds=40)),
            _event(EVENT_DETECTOR_ON, 5, t0 + timedelta(seconds=50)),
            # Next green
            _event(EVENT_PHASE_GREEN, 2, t0 + timedelta(seconds=60)),
        ]
        mock_fetch.return_value = _events_to_df(events)

        report = WaitTimeReport()
        params = WaitTimeParams(signal_id="SIG-001", start=_START_ISO, end=_END_ISO, phase_number=2)
        result = await report.execute(params, _mock_session())

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        r = result.iloc[0]
        assert r["arrivals_during_red"] == 2
        assert r["avg_wait_time"] == pytest.approx(15.0)  # (20+10)/2
        assert r["max_wait_time"] == 20.0
        assert r["min_wait_time"] == 10.0

    @pytest.mark.asyncio
    @patch("tsigma.reports.wait_time.fetch_events_split", new_callable=AsyncMock)
    @patch("tsigma.reports.wait_time.get_config_at", new_callable=AsyncMock)
    async def test_no_arrivals_during_red(self, mock_config, mock_fetch):
        """Red interval with no detector hits yields zero wait time."""
        from tsigma.reports.wait_time import WaitTimeParams, WaitTimeReport

        mock_config.return_value = _config_with_phase(2, (5,))

        t0 = datetime(2025, 6, 15, 8, 0, 0)
        events = [
            _event(EVENT_PHASE_GREEN, 2, t0),
            _event(EVENT_PHASE_END, 2, t0 + timedelta(seconds=35)),
            _event(EVENT_PHASE_GREEN, 2, t0 + timedelta(seconds=60)),
        ]
        mock_fetch.return_value = _events_to_df(events)

        report = WaitTimeReport()
        params = WaitTimeParams(signal_id="SIG-001", start=_START_ISO, end=_END_ISO, phase_number=2)
        result = await report.execute(params, _mock_session())

        assert len(result) == 1
        assert result.iloc[0]["arrivals_during_red"] == 0
        assert result.iloc[0]["avg_wait_time"] == 0.0


# =========================================================================
# Timing and Actuations — with data
# =========================================================================

class TestTimingAndActuationsWithData:

    @pytest.mark.asyncio
    @patch("tsigma.reports.timing_and_actuations.fetch_events", new_callable=AsyncMock)
    async def test_events_returned(self, mock_fetch):
        """Phase events are returned as a DataFrame."""
        from tsigma.reports.timing_and_actuations import TimingAndActuationsParams, TimingAndActuationsReport

        t0 = datetime(2025, 6, 15, 8, 0, 0)
        events = [
            _event(EVENT_PHASE_GREEN, 2, t0),
            _event(EVENT_DETECTOR_ON, 5, t0 + timedelta(seconds=5)),
            _event(EVENT_GAP_OUT, 2, t0 + timedelta(seconds=20)),
            _event(EVENT_YELLOW_CLEARANCE, 2, t0 + timedelta(seconds=25)),
            _event(EVENT_RED_CLEARANCE, 2, t0 + timedelta(seconds=29)),
            _event(EVENT_PHASE_END, 2, t0 + timedelta(seconds=31)),
        ]
        mock_fetch.return_value = _events_to_df(events)

        report = TimingAndActuationsReport()
        params = TimingAndActuationsParams(signal_id="SIG-001", start=_START_ISO, end=_END_ISO)
        result = await report.execute(params, _mock_session())

        # Returns events DataFrame with columns: event_time, event_code, event_param, event_name
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 6
        assert "event_code" in result.columns
        assert "event_param" in result.columns
        assert "event_name" in result.columns

    @pytest.mark.asyncio
    @patch("tsigma.reports.timing_and_actuations.fetch_events", new_callable=AsyncMock)
    async def test_phase_filter(self, mock_fetch):
        """Phase filter excludes non-matching phase events."""
        from tsigma.reports.timing_and_actuations import TimingAndActuationsParams, TimingAndActuationsReport

        t0 = datetime(2025, 6, 15, 8, 0, 0)
        events = [
            _event(EVENT_PHASE_GREEN, 2, t0),
            _event(EVENT_PHASE_GREEN, 4, t0 + timedelta(seconds=30)),
            _event(EVENT_DETECTOR_ON, 5, t0 + timedelta(seconds=35)),
        ]
        mock_fetch.return_value = _events_to_df(events)

        report = TimingAndActuationsReport()
        params = TimingAndActuationsParams(signal_id="SIG-001", start=_START_ISO, end=_END_ISO, phases=[2])
        result = await report.execute(params, _mock_session())

        # Phase 4 green should be filtered out; detector events always included
        phase_greens = result[result["event_code"] == EVENT_PHASE_GREEN]
        assert len(phase_greens) == 1
        assert phase_greens.iloc[0]["event_param"] == 2


# =========================================================================
# Ramp Metering — with data
# =========================================================================

class TestRampMeteringWithData:

    @pytest.mark.asyncio
    @patch("tsigma.reports.ramp_metering.fetch_events_split", new_callable=AsyncMock)
    async def test_metering_rate(self, mock_fetch):
        """Green durations and passage volume produce metering rate."""
        from tsigma.reports.ramp_metering import RampMeteringParams, RampMeteringReport

        t0 = datetime(2025, 6, 15, 8, 0, 0)
        events = [
            # Meter cycle: green -> yellow (2s green)
            _event(EVENT_PHASE_GREEN, 1, t0),
            _event(EVENT_DETECTOR_ON, 10, t0 + timedelta(seconds=1)),  # demand
            _event(EVENT_DETECTOR_ON, 11, t0 + timedelta(seconds=1.5)),  # passage
            _event(EVENT_YELLOW_CLEARANCE, 1, t0 + timedelta(seconds=2)),
            # Second cycle
            _event(EVENT_PHASE_GREEN, 1, t0 + timedelta(seconds=5)),
            _event(EVENT_DETECTOR_ON, 10, t0 + timedelta(seconds=6)),
            _event(EVENT_DETECTOR_ON, 11, t0 + timedelta(seconds=6.5)),
            _event(EVENT_YELLOW_CLEARANCE, 1, t0 + timedelta(seconds=7)),
        ]
        mock_fetch.return_value = _events_to_df(events)

        report = RampMeteringReport()
        params = RampMeteringParams(
            signal_id="SIG-001", start_time=_START_ISO, end_time=_END_ISO,
            demand_detector_channel=10, passage_detector_channel=11, meter_phase=1,
        )
        result = await report.execute(params, _mock_session())

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        r = result.iloc[0]
        assert r["demand_volume"] == 2
        assert r["passage_volume"] == 2
        assert r["avg_green_seconds"] == 2.0
        # Metering rate: 2 passages * (3600/900) = 8.0 veh/hr
        assert r["metering_rate"] == 8.0

    @pytest.mark.asyncio
    @patch("tsigma.reports.ramp_metering.fetch_events_split", new_callable=AsyncMock)
    async def test_queue_occupancy(self, mock_fetch):
        """Queue detector on/off events produce occupancy percentage."""
        from tsigma.reports.ramp_metering import RampMeteringParams, RampMeteringReport

        t0 = datetime(2025, 6, 15, 8, 0, 0)
        events = [
            _event(EVENT_PHASE_GREEN, 1, t0),
            _event(EVENT_DETECTOR_ON, 10, t0 + timedelta(seconds=1)),
            _event(EVENT_DETECTOR_ON, 11, t0 + timedelta(seconds=1)),
            _event(EVENT_YELLOW_CLEARANCE, 1, t0 + timedelta(seconds=3)),
            # Queue detector on for 5 seconds
            _event(EVENT_DETECTOR_ON, 12, t0 + timedelta(seconds=10)),
            _event(EVENT_DETECTOR_OFF, 12, t0 + timedelta(seconds=15)),
        ]
        mock_fetch.return_value = _events_to_df(events)

        report = RampMeteringReport()
        params = RampMeteringParams(
            signal_id="SIG-001", start_time=_START_ISO, end_time=_END_ISO,
            demand_detector_channel=10, passage_detector_channel=11,
            queue_detector_channel=12, meter_phase=1,
        )
        result = await report.execute(params, _mock_session())

        assert len(result) >= 1
        # At least one bin should have queue occupancy
        occ_rows = result[result["queue_occupancy_pct"].notna()]
        assert len(occ_rows) >= 1


# =========================================================================
# Preemption — with data
# =========================================================================

class TestPreemptionWithData:

    @pytest.mark.asyncio
    @patch("tsigma.reports.preemption.fetch_events", new_callable=AsyncMock)
    async def test_entry_exit_pair(self, mock_fetch):
        """Entry/exit pair produces duration measurement."""
        from tsigma.reports.preemption import PreemptionParams, PreemptionReport

        t0 = datetime(2025, 6, 15, 8, 0, 0)
        events = [
            _event(EVENT_PREEMPTION_CALL_INPUT_ON, 1, t0),
            _event(EVENT_PREEMPTION_CALL_INPUT_OFF, 1, t0 + timedelta(seconds=15)),
        ]
        mock_fetch.return_value = _events_to_df(events)

        report = PreemptionReport()
        params = PreemptionParams(signal_id="SIG-001", start=_START_ISO, end=_END_ISO)
        result = await report.execute(params, _mock_session())

        # Returns events DataFrame with columns: channel, start, end, duration_seconds
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert result.iloc[0]["channel"] == 1
        assert result.iloc[0]["duration_seconds"] == 15.0

    @pytest.mark.asyncio
    @patch("tsigma.reports.preemption.fetch_events", new_callable=AsyncMock)
    async def test_multiple_channels(self, mock_fetch):
        """Entry/exit on different channels are matched independently."""
        from tsigma.reports.preemption import PreemptionParams, PreemptionReport

        t0 = datetime(2025, 6, 15, 8, 0, 0)
        events = [
            _event(EVENT_PREEMPTION_CALL_INPUT_ON, 1, t0),
            _event(EVENT_PREEMPTION_CALL_INPUT_ON, 2, t0 + timedelta(seconds=5)),
            _event(EVENT_PREEMPTION_CALL_INPUT_OFF, 1, t0 + timedelta(seconds=10)),
            _event(EVENT_PREEMPTION_CALL_INPUT_OFF, 2, t0 + timedelta(seconds=20)),
        ]
        mock_fetch.return_value = _events_to_df(events)

        report = PreemptionReport()
        params = PreemptionParams(signal_id="SIG-001", start=_START_ISO, end=_END_ISO)
        result = await report.execute(params, _mock_session())

        assert len(result) == 2
        assert result["duration_seconds"].mean() == pytest.approx(12.5)  # (10+15)/2
        assert result["duration_seconds"].max() == 15.0

    @pytest.mark.asyncio
    @patch("tsigma.reports.preemption.fetch_events", new_callable=AsyncMock)
    async def test_unmatched_entry(self, mock_fetch):
        """Entry without exit is not counted."""
        from tsigma.reports.preemption import PreemptionParams, PreemptionReport

        t0 = datetime(2025, 6, 15, 8, 0, 0)
        events = [
            _event(EVENT_PREEMPTION_CALL_INPUT_ON, 1, t0),
        ]
        mock_fetch.return_value = _events_to_df(events)

        report = PreemptionReport()
        params = PreemptionParams(signal_id="SIG-001", start=_START_ISO, end=_END_ISO)
        result = await report.execute(params, _mock_session())

        assert result.empty
