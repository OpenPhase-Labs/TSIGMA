"""
Unit tests for the Left Turn Gap Data Check report plugin.

The data-check is a lightweight pre-flight eligibility gate that runs
BEFORE the full ``left-turn-gap`` report. It tells callers whether a
signal/approach has enough detector config and data to produce a
meaningful gap analysis.
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
    EVENT_PHASE_END,
    EVENT_PHASE_GREEN,
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
    """AsyncSession mock — unused by reports that patch helpers directly."""
    session = AsyncMock()
    mock_result = MagicMock()
    mock_result.all.return_value = []
    mock_scalars = MagicMock()
    mock_scalars.all.return_value = []
    mock_result.scalars.return_value = mock_scalars
    session.execute = AsyncMock(return_value=mock_result)
    return session


def _make_config(
    *,
    approach_id: str = "APP-LT-1",
    signal_id: str = "SIG-001",
    lt_phase: int = 1,
    through_phase: int = 2,
    lt_channels: tuple[int, ...] = (7,),
    through_channels: tuple[int, ...] = (5,),
    include_ped: bool = False,
) -> SignalConfig:
    """Build a SignalConfig with a left-turn approach + opposing through approach.

    The left-turn approach holds ``protected_phase_number = lt_phase`` and
    ``permissive_phase_number = through_phase`` (the opposing through).
    """
    lt_approach = ApproachSnapshot(
        approach_id=approach_id,
        signal_id=signal_id,
        direction_type_id=1,
        protected_phase_number=lt_phase,
        permissive_phase_number=through_phase,
        is_protected_phase_overlap=False,
        is_permissive_phase_overlap=False,
        ped_phase_number=8 if include_ped else None,
        mph=None,
        description="NB Left",
    )
    through_approach = ApproachSnapshot(
        approach_id="APP-TH-1",
        signal_id=signal_id,
        direction_type_id=2,
        protected_phase_number=through_phase,
        permissive_phase_number=None,
        is_protected_phase_overlap=False,
        is_permissive_phase_overlap=False,
        ped_phase_number=8 if include_ped else None,
        mph=None,
        description="SB Thru (opposing)",
    )
    detectors: list[DetectorSnapshot] = []
    for i, ch in enumerate(lt_channels):
        detectors.append(DetectorSnapshot(
            detector_id=f"DET-LT-{ch}",
            approach_id=approach_id,
            detector_channel=ch,
            distance_from_stop_bar=None,
            min_speed_filter=None,
            lane_number=i + 1,
        ))
    for i, ch in enumerate(through_channels):
        detectors.append(DetectorSnapshot(
            detector_id=f"DET-TH-{ch}",
            approach_id="APP-TH-1",
            detector_channel=ch,
            distance_from_stop_bar=None,
            min_speed_filter=None,
            lane_number=i + 1,
        ))
    return SignalConfig(
        signal_id=signal_id,
        as_of=datetime(2025, 6, 16),
        from_audit=False,
        approaches=[lt_approach, through_approach],
        detectors=detectors,
    )


def _make_config_no_detectors() -> SignalConfig:
    """Config where the left-turn approach has no detectors."""
    lt_approach = ApproachSnapshot(
        approach_id="APP-LT-1",
        signal_id="SIG-001",
        direction_type_id=1,
        protected_phase_number=1,
        permissive_phase_number=2,
        is_protected_phase_overlap=False,
        is_permissive_phase_overlap=False,
        ped_phase_number=None,
        mph=None,
        description="NB Left (no detectors)",
    )
    return SignalConfig(
        signal_id="SIG-001",
        as_of=datetime(2025, 6, 16),
        from_audit=False,
        approaches=[lt_approach],
        detectors=[],
    )


def _cycle_events(
    *,
    lt_phase: int,
    through_phase: int,
    start: datetime,
    lt_channel: int,
    detector_hits_per_cycle: int = 5,
    cycles: int = 6,
    cycle_seconds: int = 120,
    green_seconds: int = 30,
    termination_code: int = EVENT_GAP_OUT,
    ped_phase: int | None = None,
    ped_cycles: int = 0,
    include_opposing: bool = True,
) -> list[SimpleNamespace]:
    """Generate synthetic cycles + detector + termination events for a window."""
    events: list[SimpleNamespace] = []
    for i in range(cycles):
        base = start + timedelta(seconds=i * cycle_seconds)
        # Through phase green (opposing) — presence confirms cycle aggregation
        if include_opposing:
            events.append(_event(EVENT_PHASE_GREEN, through_phase, base))
            events.append(_event(EVENT_YELLOW_CLEARANCE, through_phase,
                                 base + timedelta(seconds=green_seconds)))
            events.append(_event(EVENT_PHASE_END, through_phase,
                                 base + timedelta(seconds=green_seconds + 4)))
            # Termination event (gap_out, max_out, or force_off)
            events.append(_event(termination_code, through_phase,
                                 base + timedelta(seconds=green_seconds)))
        # Left-turn phase green
        lt_green_base = base + timedelta(seconds=green_seconds + 10)
        events.append(_event(EVENT_PHASE_GREEN, lt_phase, lt_green_base))
        events.append(_event(EVENT_YELLOW_CLEARANCE, lt_phase,
                             lt_green_base + timedelta(seconds=20)))
        # Detector hits on left-turn channel (spread across cycle)
        for d in range(detector_hits_per_cycle):
            on_time = base + timedelta(seconds=5 + d * 10)
            off_time = on_time + timedelta(seconds=1)
            events.append(_event(EVENT_DETECTOR_ON, lt_channel, on_time))
            events.append(_event(EVENT_DETECTOR_OFF, lt_channel, off_time))
        # Ped calls for the requested number of cycles
        if ped_phase is not None and i < ped_cycles:
            events.append(_event(EVENT_PED_CALL, ped_phase,
                                 base + timedelta(seconds=3)))
    return sorted(events, key=lambda e: e.event_time)


def _both_windows_events(
    *, lt_phase: int, through_phase: int, day: datetime, lt_channel: int,
    detector_hits_per_cycle: int = 5, cycles: int = 8,
    termination_code: int = EVENT_GAP_OUT,
    ped_phase: int | None = None, ped_cycles: int = 0,
) -> list[SimpleNamespace]:
    """Build events for both AM and PM windows on a single day."""
    am_start = day.replace(hour=6, minute=30, second=0)
    pm_start = day.replace(hour=15, minute=30, second=0)
    am = _cycle_events(
        lt_phase=lt_phase, through_phase=through_phase,
        start=am_start, lt_channel=lt_channel,
        detector_hits_per_cycle=detector_hits_per_cycle, cycles=cycles,
        termination_code=termination_code,
        ped_phase=ped_phase, ped_cycles=ped_cycles,
    )
    pm = _cycle_events(
        lt_phase=lt_phase, through_phase=through_phase,
        start=pm_start, lt_channel=lt_channel,
        detector_hits_per_cycle=detector_hits_per_cycle, cycles=cycles,
        termination_code=termination_code,
        ped_phase=ped_phase, ped_cycles=ped_cycles,
    )
    return sorted(am + pm, key=lambda e: e.event_time)


# ISO-8601 string constants for the standard analysis window (a Monday).
_DAY = datetime(2025, 6, 16)  # Monday
_START_ISO = "2025-06-16T00:00:00"
_END_ISO = "2025-06-16T23:59:59"


# =========================================================================
# Registration
# =========================================================================


class TestRegistry:

    def test_registered_by_discovery(self):
        """Registry auto-discovers ``left-turn-gap-data-check``."""
        # Trigger discovery
        import tsigma.reports  # noqa: F401
        from tsigma.reports.registry import ReportRegistry

        cls = ReportRegistry.get("left-turn-gap-data-check")
        assert cls is not None
        assert cls.metadata.name == "left-turn-gap-data-check"


# =========================================================================
# Params
# =========================================================================


class TestParams:

    def test_defaults(self):
        from tsigma.reports.left_turn_gap_data_check import LeftTurnGapDataCheckParams

        p = LeftTurnGapDataCheckParams(
            signal_id="SIG-001", approach_id="APP-LT-1",
            start=_START_ISO, end=_END_ISO,
        )
        assert p.days_of_week == [0, 1, 2, 3, 4]
        assert p.volume_per_hour_threshold == 60
        assert p.gap_out_threshold == 0.5
        assert p.pedestrian_threshold == 0.25


# =========================================================================
# No-detectors / unknown-approach path
# =========================================================================


class TestNoDetectors:

    @pytest.mark.asyncio
    @patch("tsigma.reports.left_turn_gap_data_check.fetch_events_split",
           new_callable=AsyncMock)
    @patch("tsigma.reports.left_turn_gap_data_check.get_config_at",
           new_callable=AsyncMock)
    async def test_no_detectors_returns_not_ready(self, mock_config, mock_fetch):
        """No detectors on the approach → overall_ready=False, all insufficient."""
        from tsigma.reports.left_turn_gap_data_check import (
            LeftTurnGapDataCheckParams,
            LeftTurnGapDataCheckReport,
        )

        mock_config.return_value = _make_config_no_detectors()
        mock_fetch.return_value = _events_to_df([])

        report = LeftTurnGapDataCheckReport()
        params = LeftTurnGapDataCheckParams(
            signal_id="SIG-001", approach_id="APP-LT-1",
            start=_START_ISO, end=_END_ISO,
        )
        result = await report.execute(params, _mock_session())

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        row = result.iloc[0]
        assert row["signal_id"] == "SIG-001"
        assert row["approach_id"] == "APP-LT-1"
        assert bool(row["overall_ready"]) is False
        assert bool(row["left_turn_volume_ok"]) is False
        assert bool(row["gap_out_ok"]) is False
        assert bool(row["ped_cycle_ok"]) is False
        assert bool(row["insufficient_detector_event_count"]) is True
        assert bool(row["insufficient_cycle_aggregation"]) is True
        assert bool(row["insufficient_phase_termination"]) is True
        assert bool(row["insufficient_ped_aggregations"]) is True
        assert bool(row["insufficient_split_fail_aggregations"]) is True
        assert bool(row["insufficient_left_turn_gap_aggregations"]) is True

    @pytest.mark.asyncio
    @patch("tsigma.reports.left_turn_gap_data_check.fetch_events_split",
           new_callable=AsyncMock)
    @patch("tsigma.reports.left_turn_gap_data_check.get_config_at",
           new_callable=AsyncMock)
    async def test_unknown_approach_returns_not_ready(self, mock_config, mock_fetch):
        """Approach ID not in config → same empty not-ready row."""
        from tsigma.reports.left_turn_gap_data_check import (
            LeftTurnGapDataCheckParams,
            LeftTurnGapDataCheckReport,
        )

        mock_config.return_value = _make_config()
        mock_fetch.return_value = _events_to_df([])

        report = LeftTurnGapDataCheckReport()
        params = LeftTurnGapDataCheckParams(
            signal_id="SIG-001", approach_id="APP-DOES-NOT-EXIST",
            start=_START_ISO, end=_END_ISO,
        )
        result = await report.execute(params, _mock_session())

        assert len(result) == 1
        row = result.iloc[0]
        assert bool(row["overall_ready"]) is False
        assert bool(row["insufficient_detector_event_count"]) is True


# =========================================================================
# Happy path: all thresholds met
# =========================================================================


class TestAllReady:

    @pytest.mark.asyncio
    @patch("tsigma.reports.left_turn_gap_data_check.fetch_events_split",
           new_callable=AsyncMock)
    @patch("tsigma.reports.left_turn_gap_data_check.get_config_at",
           new_callable=AsyncMock)
    async def test_all_thresholds_met(self, mock_config, mock_fetch):
        """Enough data + thresholds satisfied → overall_ready=True."""
        from tsigma.reports.left_turn_gap_data_check import (
            LeftTurnGapDataCheckParams,
            LeftTurnGapDataCheckReport,
        )

        mock_config.return_value = _make_config(lt_channels=(7,))
        # High-volume day: 20 cycles per window, 4 detections per cycle =
        # 80 hits/window — peak-hour >= 60/hr. Use gap_out termination only
        # on some cycles so gap-out pct stays under 0.5.
        events = _both_windows_events(
            lt_phase=1, through_phase=2, day=_DAY, lt_channel=7,
            detector_hits_per_cycle=4, cycles=20,
            termination_code=EVENT_FORCE_OFF,  # not gap-out
            ped_phase=8, ped_cycles=2,  # low ped count → ped_pct stays under 0.25
        )
        mock_fetch.return_value = _events_to_df(events)

        report = LeftTurnGapDataCheckReport()
        params = LeftTurnGapDataCheckParams(
            signal_id="SIG-001", approach_id="APP-LT-1",
            start=_START_ISO, end=_END_ISO,
            volume_per_hour_threshold=60,
        )
        result = await report.execute(params, _mock_session())

        assert len(result) == 1
        row = result.iloc[0]
        assert bool(row["left_turn_volume_ok"]) is True
        assert bool(row["gap_out_ok"]) is True
        assert bool(row["ped_cycle_ok"]) is True
        assert bool(row["insufficient_detector_event_count"]) is False
        assert bool(row["insufficient_cycle_aggregation"]) is False
        assert bool(row["insufficient_phase_termination"]) is False
        assert bool(row["overall_ready"]) is True
        assert row["am_peak_left_turn_volume"] is not None
        assert row["pm_peak_left_turn_volume"] is not None
        assert row["am_peak_left_turn_volume"] >= 60
        assert row["pm_peak_left_turn_volume"] >= 60


# =========================================================================
# Volume threshold failure
# =========================================================================


class TestVolumeFails:

    @pytest.mark.asyncio
    @patch("tsigma.reports.left_turn_gap_data_check.fetch_events_split",
           new_callable=AsyncMock)
    @patch("tsigma.reports.left_turn_gap_data_check.get_config_at",
           new_callable=AsyncMock)
    async def test_low_volume_fails_threshold(self, mock_config, mock_fetch):
        """Volume below threshold in both windows → left_turn_volume_ok=False."""
        from tsigma.reports.left_turn_gap_data_check import (
            LeftTurnGapDataCheckParams,
            LeftTurnGapDataCheckReport,
        )

        mock_config.return_value = _make_config(lt_channels=(7,))
        # Only 2 hits/cycle × 5 cycles = 10 hits in 3 hours = ~3/hour
        events = _both_windows_events(
            lt_phase=1, through_phase=2, day=_DAY, lt_channel=7,
            detector_hits_per_cycle=2, cycles=5,
            termination_code=EVENT_FORCE_OFF,
        )
        mock_fetch.return_value = _events_to_df(events)

        report = LeftTurnGapDataCheckReport()
        params = LeftTurnGapDataCheckParams(
            signal_id="SIG-001", approach_id="APP-LT-1",
            start=_START_ISO, end=_END_ISO,
            volume_per_hour_threshold=100,  # high bar
        )
        result = await report.execute(params, _mock_session())

        row = result.iloc[0]
        assert bool(row["left_turn_volume_ok"]) is False
        assert bool(row["overall_ready"]) is False


# =========================================================================
# Gap-out threshold — both windows required
# =========================================================================


class TestGapOut:

    @pytest.mark.asyncio
    @patch("tsigma.reports.left_turn_gap_data_check.fetch_events_split",
           new_callable=AsyncMock)
    @patch("tsigma.reports.left_turn_gap_data_check.get_config_at",
           new_callable=AsyncMock)
    async def test_am_only_gap_out_high_fails(self, mock_config, mock_fetch):
        """Gap-out percentage high in AM only → gap_out_ok=False (both required)."""
        from tsigma.reports.left_turn_gap_data_check import (
            LeftTurnGapDataCheckParams,
            LeftTurnGapDataCheckReport,
        )

        mock_config.return_value = _make_config(lt_channels=(7,))

        # AM: all cycles gap-out. PM: all cycles force-off.
        am_start = _DAY.replace(hour=6, minute=30, second=0)
        pm_start = _DAY.replace(hour=15, minute=30, second=0)
        am_events = _cycle_events(
            lt_phase=1, through_phase=2, start=am_start, lt_channel=7,
            detector_hits_per_cycle=4, cycles=15,
            termination_code=EVENT_GAP_OUT,
        )
        pm_events = _cycle_events(
            lt_phase=1, through_phase=2, start=pm_start, lt_channel=7,
            detector_hits_per_cycle=4, cycles=15,
            termination_code=EVENT_FORCE_OFF,
        )
        mock_fetch.return_value = _events_to_df(
            sorted(am_events + pm_events, key=lambda e: e.event_time)
        )

        report = LeftTurnGapDataCheckReport()
        params = LeftTurnGapDataCheckParams(
            signal_id="SIG-001", approach_id="APP-LT-1",
            start=_START_ISO, end=_END_ISO,
            gap_out_threshold=0.5,
        )
        result = await report.execute(params, _mock_session())

        row = result.iloc[0]
        assert row["am_gap_out_pct"] is not None and row["am_gap_out_pct"] > 0.5
        assert row["pm_gap_out_pct"] is not None and row["pm_gap_out_pct"] < 0.5
        assert bool(row["gap_out_ok"]) is False
        assert bool(row["overall_ready"]) is False


# =========================================================================
# Ped-cycle threshold — both windows required
# =========================================================================


class TestPedCycle:

    @pytest.mark.asyncio
    @patch("tsigma.reports.left_turn_gap_data_check.fetch_events_split",
           new_callable=AsyncMock)
    @patch("tsigma.reports.left_turn_gap_data_check.get_config_at",
           new_callable=AsyncMock)
    async def test_pm_only_ped_high_fails(self, mock_config, mock_fetch):
        """High ped pct in PM only → ped_cycle_ok=False (both required)."""
        from tsigma.reports.left_turn_gap_data_check import (
            LeftTurnGapDataCheckParams,
            LeftTurnGapDataCheckReport,
        )

        mock_config.return_value = _make_config(lt_channels=(7,), include_ped=True)

        am_start = _DAY.replace(hour=6, minute=30, second=0)
        pm_start = _DAY.replace(hour=15, minute=30, second=0)
        # AM: 0 ped cycles out of 10. PM: 8 ped cycles out of 10.
        am_events = _cycle_events(
            lt_phase=1, through_phase=2, start=am_start, lt_channel=7,
            detector_hits_per_cycle=4, cycles=10,
            termination_code=EVENT_FORCE_OFF,
            ped_phase=8, ped_cycles=0,
        )
        pm_events = _cycle_events(
            lt_phase=1, through_phase=2, start=pm_start, lt_channel=7,
            detector_hits_per_cycle=4, cycles=10,
            termination_code=EVENT_FORCE_OFF,
            ped_phase=8, ped_cycles=8,
        )
        mock_fetch.return_value = _events_to_df(
            sorted(am_events + pm_events, key=lambda e: e.event_time)
        )

        report = LeftTurnGapDataCheckReport()
        params = LeftTurnGapDataCheckParams(
            signal_id="SIG-001", approach_id="APP-LT-1",
            start=_START_ISO, end=_END_ISO,
            pedestrian_threshold=0.25,
        )
        result = await report.execute(params, _mock_session())

        row = result.iloc[0]
        assert row["am_ped_pct"] is not None and row["am_ped_pct"] <= 0.25
        assert row["pm_ped_pct"] is not None and row["pm_ped_pct"] > 0.25
        assert bool(row["ped_cycle_ok"]) is False
        assert bool(row["overall_ready"]) is False


# =========================================================================
# Empty AM window
# =========================================================================


class TestInsufficientDetectorEvents:

    @pytest.mark.asyncio
    @patch("tsigma.reports.left_turn_gap_data_check.fetch_events_split",
           new_callable=AsyncMock)
    @patch("tsigma.reports.left_turn_gap_data_check.get_config_at",
           new_callable=AsyncMock)
    async def test_no_am_events_flags_insufficient(self, mock_config, mock_fetch):
        """No detector events in AM → insufficient_detector_event_count=True."""
        from tsigma.reports.left_turn_gap_data_check import (
            LeftTurnGapDataCheckParams,
            LeftTurnGapDataCheckReport,
        )

        mock_config.return_value = _make_config(lt_channels=(7,))
        # Only PM window has events
        pm_start = _DAY.replace(hour=15, minute=30, second=0)
        events = _cycle_events(
            lt_phase=1, through_phase=2, start=pm_start, lt_channel=7,
            detector_hits_per_cycle=4, cycles=10,
            termination_code=EVENT_FORCE_OFF,
        )
        mock_fetch.return_value = _events_to_df(events)

        report = LeftTurnGapDataCheckReport()
        params = LeftTurnGapDataCheckParams(
            signal_id="SIG-001", approach_id="APP-LT-1",
            start=_START_ISO, end=_END_ISO,
        )
        result = await report.execute(params, _mock_session())

        row = result.iloc[0]
        assert bool(row["insufficient_detector_event_count"]) is True
        assert bool(row["overall_ready"]) is False


# =========================================================================
# days_of_week filter
# =========================================================================


class TestDaysOfWeekFilter:

    @pytest.mark.asyncio
    @patch("tsigma.reports.left_turn_gap_data_check.fetch_events_split",
           new_callable=AsyncMock)
    @patch("tsigma.reports.left_turn_gap_data_check.get_config_at",
           new_callable=AsyncMock)
    async def test_only_tuesdays_counted(self, mock_config, mock_fetch):
        """Tuesday-only filter excludes other weekdays' events."""
        from tsigma.reports.left_turn_gap_data_check import (
            LeftTurnGapDataCheckParams,
            LeftTurnGapDataCheckReport,
        )

        mock_config.return_value = _make_config(lt_channels=(7,))
        # A Monday (skipped) and a Tuesday (kept)
        monday = datetime(2025, 6, 16)       # weekday 0
        tuesday = datetime(2025, 6, 17)      # weekday 1
        monday_events = _both_windows_events(
            lt_phase=1, through_phase=2, day=monday, lt_channel=7,
            detector_hits_per_cycle=10, cycles=20,  # lots of data
            termination_code=EVENT_GAP_OUT,          # would fail gap_out
        )
        tuesday_events = _both_windows_events(
            lt_phase=1, through_phase=2, day=tuesday, lt_channel=7,
            detector_hits_per_cycle=4, cycles=15,
            termination_code=EVENT_FORCE_OFF,        # passes gap_out
        )
        mock_fetch.return_value = _events_to_df(
            sorted(monday_events + tuesday_events, key=lambda e: e.event_time)
        )

        report = LeftTurnGapDataCheckReport()
        params = LeftTurnGapDataCheckParams(
            signal_id="SIG-001", approach_id="APP-LT-1",
            start="2025-06-16T00:00:00", end="2025-06-17T23:59:59",
            days_of_week=[1],  # Tuesday only
            gap_out_threshold=0.5,
        )
        result = await report.execute(params, _mock_session())

        row = result.iloc[0]
        # Monday gap-outs are excluded — Tuesday is FORCE_OFF so gap_out_pct is low
        assert bool(row["gap_out_ok"]) is True
        # Ensure there IS AM data (came from Tuesday)
        assert bool(row["insufficient_detector_event_count"]) is False


# =========================================================================
# DataFrame schema
# =========================================================================


class TestOutputSchema:

    @pytest.mark.asyncio
    @patch("tsigma.reports.left_turn_gap_data_check.fetch_events_split",
           new_callable=AsyncMock)
    @patch("tsigma.reports.left_turn_gap_data_check.get_config_at",
           new_callable=AsyncMock)
    async def test_output_has_all_expected_columns(self, mock_config, mock_fetch):
        from tsigma.reports.left_turn_gap_data_check import (
            LeftTurnGapDataCheckParams,
            LeftTurnGapDataCheckReport,
        )

        mock_config.return_value = _make_config_no_detectors()
        mock_fetch.return_value = _events_to_df([])

        report = LeftTurnGapDataCheckReport()
        params = LeftTurnGapDataCheckParams(
            signal_id="SIG-001", approach_id="APP-LT-1",
            start=_START_ISO, end=_END_ISO,
        )
        result = await report.execute(params, _mock_session())

        expected = {
            "signal_id", "approach_id", "start", "end",
            "left_turn_volume_ok", "gap_out_ok", "ped_cycle_ok",
            "insufficient_detector_event_count",
            "insufficient_cycle_aggregation",
            "insufficient_phase_termination",
            "insufficient_ped_aggregations",
            "insufficient_split_fail_aggregations",
            "insufficient_left_turn_gap_aggregations",
            "am_peak_left_turn_volume", "pm_peak_left_turn_volume",
            "am_gap_out_pct", "pm_gap_out_pct",
            "am_ped_pct", "pm_ped_pct",
            "overall_ready",
        }
        assert expected.issubset(set(result.columns))


# Silence unused import warnings — codes are used indirectly via helpers.
_ = (EVENT_MAX_OUT, EVENT_PHASE_END)
