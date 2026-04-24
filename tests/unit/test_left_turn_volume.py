"""
Unit tests for the Left Turn Volume report.

Covers:
- Empty-data path and schema shape.
- Each of the six HCM decision-boundary formulas
  (approach_type x opposing_lanes).
- Cross-product review threshold boundaries (50k / 100k).
- Opposing phase mapping for all eight phase pairs (1..8).
- AM / PM peak-hour selection from 15-min bins.
- Full-window 15-min demand list resolution.
- Days-of-week filter (Mondays only).
"""

from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest

from tsigma.config_resolver import ApproachSnapshot, DetectorSnapshot, SignalConfig
from tsigma.reports.sdk.events import EVENT_DETECTOR_ON

_START_ISO = "2025-06-16T00:00:00"  # Monday
_END_ISO = "2025-06-16T23:59:59"


def _event(code: int, param: int, time: datetime) -> SimpleNamespace:
    return SimpleNamespace(
        signal_id="SIG-001",
        event_code=code,
        event_param=param,
        event_time=time,
        device_id=1,
        validation_metadata=None,
    )


def _events_to_df(events: list[SimpleNamespace]) -> pd.DataFrame:
    if not events:
        return pd.DataFrame(columns=["event_code", "event_param", "event_time"])
    return pd.DataFrame(
        [
            {
                "event_code": e.event_code,
                "event_param": e.event_param,
                "event_time": e.event_time,
            }
            for e in events
        ]
    )


def _mock_session():
    session = AsyncMock()
    mock_result = MagicMock()
    mock_result.all.return_value = []
    mock_scalars = MagicMock()
    mock_scalars.all.return_value = []
    mock_result.scalars.return_value = mock_scalars
    session.execute = AsyncMock(return_value=mock_result)
    return session


def _config_two_approaches(
    *,
    lt_phase: int,
    opp_phase: int,
    lt_channels: tuple[int, ...] = (10,),
    opp_channels: tuple[int, ...] = (20,),
    lt_direction: int = 1,
    opp_direction: int = 2,
) -> SignalConfig:
    """
    Build a SignalConfig with two approaches:
      - LT approach (protected phase = lt_phase) with lt_channels detectors.
      - Opposing-through approach (protected phase = opp_phase) with opp_channels detectors.
    """
    lt_approach = ApproachSnapshot(
        approach_id="APP-LT",
        signal_id="SIG-001",
        direction_type_id=lt_direction,
        protected_phase_number=lt_phase,
        permissive_phase_number=None,
        is_protected_phase_overlap=False,
        is_permissive_phase_overlap=False,
        ped_phase_number=None,
        mph=None,
        description="Left turn approach",
    )
    opp_approach = ApproachSnapshot(
        approach_id="APP-OPP",
        signal_id="SIG-001",
        direction_type_id=opp_direction,
        protected_phase_number=opp_phase,
        permissive_phase_number=None,
        is_protected_phase_overlap=False,
        is_permissive_phase_overlap=False,
        ped_phase_number=None,
        mph=None,
        description="Opposing through approach",
    )
    detectors: list[DetectorSnapshot] = []
    for i, ch in enumerate(lt_channels):
        detectors.append(
            DetectorSnapshot(
                detector_id=f"DET-LT-{ch}",
                approach_id="APP-LT",
                detector_channel=ch,
                distance_from_stop_bar=None,
                min_speed_filter=None,
                lane_number=i + 1,
            )
        )
    for i, ch in enumerate(opp_channels):
        detectors.append(
            DetectorSnapshot(
                detector_id=f"DET-OPP-{ch}",
                approach_id="APP-OPP",
                detector_channel=ch,
                distance_from_stop_bar=None,
                min_speed_filter=None,
                lane_number=i + 1,
            )
        )
    return SignalConfig(
        signal_id="SIG-001",
        as_of=datetime(2025, 6, 16),
        from_audit=False,
        approaches=[lt_approach, opp_approach],
        detectors=detectors,
    )


def _make_detector_events(
    lt_channel: int,
    opp_channel: int,
    day: datetime,
    lt_times: list[int],
    opp_times: list[int],
) -> list[SimpleNamespace]:
    """Build detector-on events at the given minute-offsets within `day`."""
    events: list[SimpleNamespace] = []
    for minute in lt_times:
        events.append(_event(EVENT_DETECTOR_ON, lt_channel, day + timedelta(minutes=minute)))
    for minute in opp_times:
        events.append(_event(EVENT_DETECTOR_ON, opp_channel, day + timedelta(minutes=minute)))
    return events


# =========================================================================
# Registry / metadata
# =========================================================================


class TestRegistry:
    def test_registered(self):
        # Importing the package auto-registers all plugins.
        from tsigma.reports.registry import ReportRegistry

        assert "left-turn-volume" in ReportRegistry.list_all()

    def test_metadata_shape(self):
        from tsigma.reports.left_turn_volume import LeftTurnVolumeReport

        meta = LeftTurnVolumeReport.metadata
        assert meta.name == "left-turn-volume"
        assert "csv" in (meta.export_formats or [])


# =========================================================================
# Empty data / empty config
# =========================================================================


class TestEmptyCases:

    @pytest.mark.asyncio
    @patch("tsigma.reports.sdk.queries.db_facade")
    @patch("tsigma.reports.left_turn_volume.get_config_at", new_callable=AsyncMock)
    async def test_no_detectors_returns_empty_schema(self, mock_config, mock_facade):
        from tsigma.reports.left_turn_volume import (
            _EMPTY_COLUMNS,
            LeftTurnVolumeParams,
            LeftTurnVolumeReport,
        )

        mock_config.return_value = SignalConfig(
            signal_id="SIG-001",
            as_of=datetime(2025, 6, 16),
            from_audit=False,
            approaches=[],
            detectors=[],
        )
        mock_facade.get_dataframe = AsyncMock(return_value=_events_to_df([]))

        report = LeftTurnVolumeReport()
        params = LeftTurnVolumeParams(
            signal_id="SIG-001",
            approach_id="APP-LT",
            start=_START_ISO,
            end=_END_ISO,
            approach_type="permissive",
        )
        result = await report.execute(params, _mock_session())
        assert isinstance(result, pd.DataFrame)
        assert result.empty
        for col in _EMPTY_COLUMNS:
            assert col in result.columns

    @pytest.mark.asyncio
    @patch("tsigma.reports.sdk.queries.db_facade")
    @patch("tsigma.reports.left_turn_volume.get_config_at", new_callable=AsyncMock)
    async def test_no_events_returns_empty(self, mock_config, mock_facade):
        from tsigma.reports.left_turn_volume import LeftTurnVolumeParams, LeftTurnVolumeReport

        mock_config.return_value = _config_two_approaches(lt_phase=1, opp_phase=2)
        mock_facade.get_dataframe = AsyncMock(return_value=_events_to_df([]))

        report = LeftTurnVolumeReport()
        params = LeftTurnVolumeParams(
            signal_id="SIG-001",
            approach_id="APP-LT",
            start=_START_ISO,
            end=_END_ISO,
            approach_type="permissive",
        )
        result = await report.execute(params, _mock_session())
        assert isinstance(result, pd.DataFrame)
        assert result.empty


# =========================================================================
# Opposing phase mapping — 8 pairs
# =========================================================================


class TestOpposingPhaseMapping:

    @pytest.mark.parametrize(
        "lt_phase,opp_phase",
        [(1, 2), (2, 1), (3, 4), (4, 3), (5, 6), (6, 5), (7, 8), (8, 7)],
    )
    @pytest.mark.asyncio
    @patch("tsigma.reports.sdk.queries.db_facade")
    @patch("tsigma.reports.left_turn_volume.get_config_at", new_callable=AsyncMock)
    async def test_all_eight_pairs(self, mock_config, mock_facade, lt_phase, opp_phase):
        from tsigma.reports.left_turn_volume import LeftTurnVolumeParams, LeftTurnVolumeReport

        mock_config.return_value = _config_two_approaches(
            lt_phase=lt_phase, opp_phase=opp_phase,
        )

        monday = datetime(2025, 6, 16, 8, 0, 0)  # within 6-18 window
        events = _make_detector_events(10, 20, monday, [5], [5])
        mock_facade.get_dataframe = AsyncMock(return_value=_events_to_df(events))

        report = LeftTurnVolumeReport()
        params = LeftTurnVolumeParams(
            signal_id="SIG-001",
            approach_id="APP-LT",
            start=_START_ISO,
            end=_END_ISO,
            approach_type="permissive",
        )
        result = await report.execute(params, _mock_session())
        assert not result.empty
        # Opposing-phase lookup picked up the opp approach (1 detector)
        assert int(result.iloc[0]["opposing_lanes"]) == 1
        assert int(result.iloc[0]["left_turn_volume"]) == 1
        assert int(result.iloc[0]["opposing_through_volume"]) == 1


# =========================================================================
# HCM decision-boundary formulas — six cases
# =========================================================================


class TestDecisionBoundaryFormulas:
    """Six formula cases: approach_type x opposing_lanes."""

    @staticmethod
    def _run_formula_case(
        mock_config, mock_facade, *, approach_type: str, opp_channels: tuple[int, ...],
        lt_count: int, opp_count: int,
    ) -> pd.DataFrame:
        from tsigma.reports.left_turn_volume import LeftTurnVolumeParams, LeftTurnVolumeReport

        mock_config.return_value = _config_two_approaches(
            lt_phase=1, opp_phase=2, opp_channels=opp_channels,
        )
        monday = datetime(2025, 6, 16, 8, 0, 0)
        # Spread events evenly through allowed window
        lt_times = [i % 600 for i in range(lt_count)]
        opp_times = [(i * 2) % 600 for i in range(opp_count)]
        # Build opposing events across all opp_channels equally
        events = []
        for minute in lt_times:
            events.append(_event(EVENT_DETECTOR_ON, 10, monday + timedelta(minutes=minute)))
        # Distribute the opp_count events across opp_channels
        for i, minute in enumerate(opp_times):
            ch = opp_channels[i % len(opp_channels)]
            events.append(_event(EVENT_DETECTOR_ON, ch, monday + timedelta(minutes=minute)))

        mock_facade.get_dataframe = AsyncMock(return_value=_events_to_df(events))

        report = LeftTurnVolumeReport()
        params = LeftTurnVolumeParams(
            signal_id="SIG-001",
            approach_id="APP-LT",
            start=_START_ISO,
            end=_END_ISO,
            approach_type=approach_type,
        )
        import asyncio
        return asyncio.get_event_loop().run_until_complete(
            report.execute(params, _mock_session())
        )

    @pytest.mark.asyncio
    @patch("tsigma.reports.sdk.queries.db_facade")
    @patch("tsigma.reports.left_turn_volume.get_config_at", new_callable=AsyncMock)
    async def test_permissive_one_lane(self, mock_config, mock_facade):
        from tsigma.reports.left_turn_volume import LeftTurnVolumeParams, LeftTurnVolumeReport

        mock_config.return_value = _config_two_approaches(
            lt_phase=1, opp_phase=2, opp_channels=(20,),
        )
        monday = datetime(2025, 6, 16, 8, 0, 0)
        events = []
        for i in range(50):
            events.append(_event(EVENT_DETECTOR_ON, 10, monday + timedelta(minutes=i % 600)))
        for i in range(60):
            events.append(_event(EVENT_DETECTOR_ON, 20, monday + timedelta(minutes=(i * 2) % 600)))
        mock_facade.get_dataframe = AsyncMock(return_value=_events_to_df(events))

        report = LeftTurnVolumeReport()
        params = LeftTurnVolumeParams(
            signal_id="SIG-001", approach_id="APP-LT",
            start=_START_ISO, end=_END_ISO, approach_type="permissive",
        )
        result = await report.execute(params, _mock_session())
        r = result.iloc[0]
        assert int(r["left_turn_volume"]) == 50
        assert int(r["opposing_through_volume"]) == 60
        assert int(r["opposing_lanes"]) == 1
        # permissive, 1 lane: LT_V * OPP_V**0.706
        expected = 50 * (60 ** 0.706)
        assert float(r["calculated_volume_boundary"]) == pytest.approx(expected)
        assert float(r["decision_boundary_threshold"]) == pytest.approx(9519)

    @pytest.mark.asyncio
    @patch("tsigma.reports.sdk.queries.db_facade")
    @patch("tsigma.reports.left_turn_volume.get_config_at", new_callable=AsyncMock)
    async def test_permissive_multi_lane(self, mock_config, mock_facade):
        from tsigma.reports.left_turn_volume import LeftTurnVolumeParams, LeftTurnVolumeReport

        mock_config.return_value = _config_two_approaches(
            lt_phase=1, opp_phase=2, opp_channels=(20, 21),
        )
        monday = datetime(2025, 6, 16, 8, 0, 0)
        events = []
        for i in range(70):
            events.append(_event(EVENT_DETECTOR_ON, 10, monday + timedelta(minutes=i % 600)))
        for i in range(80):
            ch = (20, 21)[i % 2]
            events.append(_event(EVENT_DETECTOR_ON, ch, monday + timedelta(minutes=(i * 2) % 600)))
        mock_facade.get_dataframe = AsyncMock(return_value=_events_to_df(events))

        report = LeftTurnVolumeReport()
        params = LeftTurnVolumeParams(
            signal_id="SIG-001", approach_id="APP-LT",
            start=_START_ISO, end=_END_ISO, approach_type="permissive",
        )
        result = await report.execute(params, _mock_session())
        r = result.iloc[0]
        assert int(r["left_turn_volume"]) == 70
        assert int(r["opposing_through_volume"]) == 80
        assert int(r["opposing_lanes"]) == 2
        # permissive, >1 lane: 2 * LT_V * OPP_V**0.642
        expected = 2 * 70 * (80 ** 0.642)
        assert float(r["calculated_volume_boundary"]) == pytest.approx(expected)
        assert float(r["decision_boundary_threshold"]) == pytest.approx(7974)

    @pytest.mark.asyncio
    @patch("tsigma.reports.sdk.queries.db_facade")
    @patch("tsigma.reports.left_turn_volume.get_config_at", new_callable=AsyncMock)
    async def test_permissive_protected_one_lane(self, mock_config, mock_facade):
        from tsigma.reports.left_turn_volume import LeftTurnVolumeParams, LeftTurnVolumeReport

        mock_config.return_value = _config_two_approaches(
            lt_phase=1, opp_phase=2, opp_channels=(20,),
        )
        monday = datetime(2025, 6, 16, 8, 0, 0)
        events = []
        for i in range(100):
            events.append(_event(EVENT_DETECTOR_ON, 10, monday + timedelta(minutes=i % 600)))
        for i in range(200):
            events.append(_event(EVENT_DETECTOR_ON, 20, monday + timedelta(minutes=(i * 2) % 600)))
        mock_facade.get_dataframe = AsyncMock(return_value=_events_to_df(events))

        report = LeftTurnVolumeReport()
        params = LeftTurnVolumeParams(
            signal_id="SIG-001", approach_id="APP-LT",
            start=_START_ISO, end=_END_ISO, approach_type="permissive_protected",
        )
        result = await report.execute(params, _mock_session())
        r = result.iloc[0]
        # formula: LT_V * OPP_V**0.500
        expected = 100 * (200 ** 0.500)
        assert float(r["calculated_volume_boundary"]) == pytest.approx(expected)
        assert float(r["decision_boundary_threshold"]) == pytest.approx(4638)

    @pytest.mark.asyncio
    @patch("tsigma.reports.sdk.queries.db_facade")
    @patch("tsigma.reports.left_turn_volume.get_config_at", new_callable=AsyncMock)
    async def test_permissive_protected_multi_lane(self, mock_config, mock_facade):
        from tsigma.reports.left_turn_volume import LeftTurnVolumeParams, LeftTurnVolumeReport

        mock_config.return_value = _config_two_approaches(
            lt_phase=1, opp_phase=2, opp_channels=(20, 21),
        )
        monday = datetime(2025, 6, 16, 8, 0, 0)
        events = []
        for i in range(40):
            events.append(_event(EVENT_DETECTOR_ON, 10, monday + timedelta(minutes=i % 600)))
        for i in range(50):
            ch = (20, 21)[i % 2]
            events.append(_event(EVENT_DETECTOR_ON, ch, monday + timedelta(minutes=(i * 2) % 600)))
        mock_facade.get_dataframe = AsyncMock(return_value=_events_to_df(events))

        report = LeftTurnVolumeReport()
        params = LeftTurnVolumeParams(
            signal_id="SIG-001", approach_id="APP-LT",
            start=_START_ISO, end=_END_ISO, approach_type="permissive_protected",
        )
        result = await report.execute(params, _mock_session())
        r = result.iloc[0]
        # formula: 2 * LT_V * OPP_V**0.404
        expected = 2 * 40 * (50 ** 0.404)
        assert float(r["calculated_volume_boundary"]) == pytest.approx(expected)
        assert float(r["decision_boundary_threshold"]) == pytest.approx(3782)

    @pytest.mark.asyncio
    @patch("tsigma.reports.sdk.queries.db_facade")
    @patch("tsigma.reports.left_turn_volume.get_config_at", new_callable=AsyncMock)
    async def test_protected_one_lane(self, mock_config, mock_facade):
        from tsigma.reports.left_turn_volume import LeftTurnVolumeParams, LeftTurnVolumeReport

        mock_config.return_value = _config_two_approaches(
            lt_phase=1, opp_phase=2, opp_channels=(20,),
        )
        monday = datetime(2025, 6, 16, 8, 0, 0)
        events = []
        for i in range(30):
            events.append(_event(EVENT_DETECTOR_ON, 10, monday + timedelta(minutes=i % 600)))
        for i in range(90):
            events.append(_event(EVENT_DETECTOR_ON, 20, monday + timedelta(minutes=(i * 2) % 600)))
        mock_facade.get_dataframe = AsyncMock(return_value=_events_to_df(events))

        report = LeftTurnVolumeReport()
        params = LeftTurnVolumeParams(
            signal_id="SIG-001", approach_id="APP-LT",
            start=_START_ISO, end=_END_ISO, approach_type="protected",
        )
        result = await report.execute(params, _mock_session())
        r = result.iloc[0]
        # formula: LT_V * OPP_V**0.425
        expected = 30 * (90 ** 0.425)
        assert float(r["calculated_volume_boundary"]) == pytest.approx(expected)
        assert float(r["decision_boundary_threshold"]) == pytest.approx(3693)

    @pytest.mark.asyncio
    @patch("tsigma.reports.sdk.queries.db_facade")
    @patch("tsigma.reports.left_turn_volume.get_config_at", new_callable=AsyncMock)
    async def test_protected_multi_lane(self, mock_config, mock_facade):
        from tsigma.reports.left_turn_volume import LeftTurnVolumeParams, LeftTurnVolumeReport

        mock_config.return_value = _config_two_approaches(
            lt_phase=1, opp_phase=2, opp_channels=(20, 21, 22),
        )
        monday = datetime(2025, 6, 16, 8, 0, 0)
        events = []
        for i in range(25):
            events.append(_event(EVENT_DETECTOR_ON, 10, monday + timedelta(minutes=i % 600)))
        for i in range(75):
            ch = (20, 21, 22)[i % 3]
            events.append(_event(EVENT_DETECTOR_ON, ch, monday + timedelta(minutes=(i * 2) % 600)))
        mock_facade.get_dataframe = AsyncMock(return_value=_events_to_df(events))

        report = LeftTurnVolumeReport()
        params = LeftTurnVolumeParams(
            signal_id="SIG-001", approach_id="APP-LT",
            start=_START_ISO, end=_END_ISO, approach_type="protected",
        )
        result = await report.execute(params, _mock_session())
        r = result.iloc[0]
        assert int(r["opposing_lanes"]) == 3
        # formula: 2 * LT_V * OPP_V**0.404
        expected = 2 * 25 * (75 ** 0.404)
        assert float(r["calculated_volume_boundary"]) == pytest.approx(expected)
        assert float(r["decision_boundary_threshold"]) == pytest.approx(3782)


# =========================================================================
# Cross-product review thresholds
# =========================================================================


class TestCrossProductThreshold:

    @pytest.mark.asyncio
    @patch("tsigma.reports.sdk.queries.db_facade")
    @patch("tsigma.reports.left_turn_volume.get_config_at", new_callable=AsyncMock)
    async def test_single_lane_under_threshold(self, mock_config, mock_facade):
        from tsigma.reports.left_turn_volume import LeftTurnVolumeParams, LeftTurnVolumeReport

        mock_config.return_value = _config_two_approaches(
            lt_phase=1, opp_phase=2, opp_channels=(20,),
        )
        monday = datetime(2025, 6, 16, 8, 0, 0)
        # Cross product = 100 * 100 = 10_000 <= 50_000
        events = []
        for i in range(100):
            events.append(_event(EVENT_DETECTOR_ON, 10, monday + timedelta(minutes=i % 600)))
            events.append(_event(EVENT_DETECTOR_ON, 20, monday + timedelta(minutes=i % 600)))
        mock_facade.get_dataframe = AsyncMock(return_value=_events_to_df(events))

        report = LeftTurnVolumeReport()
        params = LeftTurnVolumeParams(
            signal_id="SIG-001", approach_id="APP-LT",
            start=_START_ISO, end=_END_ISO, approach_type="permissive",
        )
        result = await report.execute(params, _mock_session())
        assert bool(result.iloc[0]["cross_product_review"]) is False

    @pytest.mark.asyncio
    @patch("tsigma.reports.sdk.queries.db_facade")
    @patch("tsigma.reports.left_turn_volume.get_config_at", new_callable=AsyncMock)
    async def test_single_lane_at_threshold(self, mock_config, mock_facade):
        """cross_product exactly 50_000 is NOT > 50_000 → False."""
        from tsigma.reports.left_turn_volume import LeftTurnVolumeParams, LeftTurnVolumeReport

        mock_config.return_value = _config_two_approaches(
            lt_phase=1, opp_phase=2, opp_channels=(20,),
        )
        monday = datetime(2025, 6, 16, 8, 0, 0)
        # 250 * 200 = 50_000
        events = []
        for i in range(250):
            events.append(_event(EVENT_DETECTOR_ON, 10, monday + timedelta(minutes=i % 600)))
        for i in range(200):
            events.append(_event(EVENT_DETECTOR_ON, 20, monday + timedelta(minutes=i % 600)))
        mock_facade.get_dataframe = AsyncMock(return_value=_events_to_df(events))

        report = LeftTurnVolumeReport()
        params = LeftTurnVolumeParams(
            signal_id="SIG-001", approach_id="APP-LT",
            start=_START_ISO, end=_END_ISO, approach_type="permissive",
        )
        result = await report.execute(params, _mock_session())
        assert int(result.iloc[0]["cross_product_value"]) == 50_000
        assert bool(result.iloc[0]["cross_product_review"]) is False

    @pytest.mark.asyncio
    @patch("tsigma.reports.sdk.queries.db_facade")
    @patch("tsigma.reports.left_turn_volume.get_config_at", new_callable=AsyncMock)
    async def test_single_lane_above_threshold(self, mock_config, mock_facade):
        """cross_product > 50_000 → True for 1 lane."""
        from tsigma.reports.left_turn_volume import LeftTurnVolumeParams, LeftTurnVolumeReport

        mock_config.return_value = _config_two_approaches(
            lt_phase=1, opp_phase=2, opp_channels=(20,),
        )
        monday = datetime(2025, 6, 16, 8, 0, 0)
        # 251 * 200 = 50_200
        events = []
        for i in range(251):
            events.append(_event(EVENT_DETECTOR_ON, 10, monday + timedelta(minutes=i % 600)))
        for i in range(200):
            events.append(_event(EVENT_DETECTOR_ON, 20, monday + timedelta(minutes=i % 600)))
        mock_facade.get_dataframe = AsyncMock(return_value=_events_to_df(events))

        report = LeftTurnVolumeReport()
        params = LeftTurnVolumeParams(
            signal_id="SIG-001", approach_id="APP-LT",
            start=_START_ISO, end=_END_ISO, approach_type="permissive",
        )
        result = await report.execute(params, _mock_session())
        assert bool(result.iloc[0]["cross_product_review"]) is True

    @pytest.mark.asyncio
    @patch("tsigma.reports.sdk.queries.db_facade")
    @patch("tsigma.reports.left_turn_volume.get_config_at", new_callable=AsyncMock)
    async def test_multi_lane_under_100k_threshold(self, mock_config, mock_facade):
        from tsigma.reports.left_turn_volume import LeftTurnVolumeParams, LeftTurnVolumeReport

        mock_config.return_value = _config_two_approaches(
            lt_phase=1, opp_phase=2, opp_channels=(20, 21),
        )
        monday = datetime(2025, 6, 16, 8, 0, 0)
        # 300 * 300 = 90_000 <= 100_000
        events = []
        for i in range(300):
            events.append(_event(EVENT_DETECTOR_ON, 10, monday + timedelta(minutes=i % 600)))
        for i in range(300):
            ch = (20, 21)[i % 2]
            events.append(_event(EVENT_DETECTOR_ON, ch, monday + timedelta(minutes=i % 600)))
        mock_facade.get_dataframe = AsyncMock(return_value=_events_to_df(events))

        report = LeftTurnVolumeReport()
        params = LeftTurnVolumeParams(
            signal_id="SIG-001", approach_id="APP-LT",
            start=_START_ISO, end=_END_ISO, approach_type="permissive",
        )
        result = await report.execute(params, _mock_session())
        assert int(result.iloc[0]["cross_product_value"]) == 90_000
        assert bool(result.iloc[0]["cross_product_review"]) is False

    @pytest.mark.asyncio
    @patch("tsigma.reports.sdk.queries.db_facade")
    @patch("tsigma.reports.left_turn_volume.get_config_at", new_callable=AsyncMock)
    async def test_multi_lane_above_100k_threshold(self, mock_config, mock_facade):
        from tsigma.reports.left_turn_volume import LeftTurnVolumeParams, LeftTurnVolumeReport

        mock_config.return_value = _config_two_approaches(
            lt_phase=1, opp_phase=2, opp_channels=(20, 21),
        )
        monday = datetime(2025, 6, 16, 8, 0, 0)
        # 400 * 300 = 120_000 > 100_000
        events = []
        for i in range(400):
            events.append(_event(EVENT_DETECTOR_ON, 10, monday + timedelta(minutes=i % 600)))
        for i in range(300):
            ch = (20, 21)[i % 2]
            events.append(_event(EVENT_DETECTOR_ON, ch, monday + timedelta(minutes=i % 600)))
        mock_facade.get_dataframe = AsyncMock(return_value=_events_to_df(events))

        report = LeftTurnVolumeReport()
        params = LeftTurnVolumeParams(
            signal_id="SIG-001", approach_id="APP-LT",
            start=_START_ISO, end=_END_ISO, approach_type="permissive",
        )
        result = await report.execute(params, _mock_session())
        assert int(result.iloc[0]["cross_product_value"]) == 120_000
        assert bool(result.iloc[0]["cross_product_review"]) is True


# =========================================================================
# AM / PM peak-hour detection
# =========================================================================


class TestPeakHours:

    @pytest.mark.asyncio
    @patch("tsigma.reports.sdk.queries.db_facade")
    @patch("tsigma.reports.left_turn_volume.get_config_at", new_callable=AsyncMock)
    async def test_am_and_pm_peak_selection(self, mock_config, mock_facade):
        from tsigma.reports.left_turn_volume import LeftTurnVolumeParams, LeftTurnVolumeReport

        mock_config.return_value = _config_two_approaches(lt_phase=1, opp_phase=2)
        monday = datetime(2025, 6, 16, 0, 0, 0)

        events = []
        # AM spike: 7:30 - 8:30 = 10 events (peak hour starts at 7:30 with most vol)
        for minute in range(7 * 60 + 30, 8 * 60 + 30):
            events.append(_event(EVENT_DETECTOR_ON, 10, monday + timedelta(minutes=minute)))
        # Small baseline at other AM times (within 6-9)
        for minute in (6 * 60 + 10, 6 * 60 + 20):
            events.append(_event(EVENT_DETECTOR_ON, 10, monday + timedelta(minutes=minute)))

        # PM spike: 17:00 - 18:00 = 20 events (inside 15-18 window)
        for minute in range(17 * 60, 17 * 60 + 20):
            events.append(_event(EVENT_DETECTOR_ON, 10, monday + timedelta(minutes=minute)))
        for minute in range(15 * 60 + 10, 15 * 60 + 13):
            events.append(_event(EVENT_DETECTOR_ON, 10, monday + timedelta(minutes=minute)))

        # Some opposing events so we don't zero out
        for minute in (8 * 60, 17 * 60):
            events.append(_event(EVENT_DETECTOR_ON, 20, monday + timedelta(minutes=minute)))

        mock_facade.get_dataframe = AsyncMock(return_value=_events_to_df(events))

        report = LeftTurnVolumeReport()
        params = LeftTurnVolumeParams(
            signal_id="SIG-001", approach_id="APP-LT",
            start=_START_ISO, end=_END_ISO, approach_type="permissive",
        )
        result = await report.execute(params, _mock_session())
        r = result.iloc[0]
        assert r["am_peak_hour"] is not None
        assert r["pm_peak_hour"] is not None
        # AM peak starts at 07:30 (1-hour sliding window)
        assert "07:30" in str(r["am_peak_hour"])
        assert int(r["am_peak_left_turn_volume"]) == 60
        # PM peak starts at 17:00
        assert "17:00" in str(r["pm_peak_hour"])
        assert int(r["pm_peak_left_turn_volume"]) == 20


# =========================================================================
# Demand list: 15-minute resolution, full window
# =========================================================================


class TestDemandList:

    @pytest.mark.asyncio
    @patch("tsigma.reports.sdk.queries.db_facade")
    @patch("tsigma.reports.left_turn_volume.get_config_at", new_callable=AsyncMock)
    async def test_demand_list_bins_15min(self, mock_config, mock_facade):
        from tsigma.reports.left_turn_volume import LeftTurnVolumeParams, LeftTurnVolumeReport

        mock_config.return_value = _config_two_approaches(lt_phase=1, opp_phase=2)
        monday = datetime(2025, 6, 16, 8, 0, 0)

        # 3 LT events in 08:00-08:15, 5 in 08:15-08:30, 1 opp event in each
        events = []
        for m in (1, 2, 3):
            events.append(_event(EVENT_DETECTOR_ON, 10, monday + timedelta(minutes=m)))
        for m in (16, 17, 18, 19, 20):
            events.append(_event(EVENT_DETECTOR_ON, 10, monday + timedelta(minutes=m)))
        events.append(_event(EVENT_DETECTOR_ON, 20, monday + timedelta(minutes=5)))
        events.append(_event(EVENT_DETECTOR_ON, 20, monday + timedelta(minutes=18)))
        mock_facade.get_dataframe = AsyncMock(return_value=_events_to_df(events))

        report = LeftTurnVolumeReport()
        params = LeftTurnVolumeParams(
            signal_id="SIG-001", approach_id="APP-LT",
            start=_START_ISO, end=_END_ISO, approach_type="permissive",
        )
        result = await report.execute(params, _mock_session())

        # Bin 08:00 = 3 LT, bin 08:15 = 5 LT
        bin_08_00 = result[result["bin_start"].str.startswith("2025-06-16T08:00")]
        bin_08_15 = result[result["bin_start"].str.startswith("2025-06-16T08:15")]
        assert not bin_08_00.empty
        assert not bin_08_15.empty
        assert int(bin_08_00.iloc[0]["left_turn_volume_bin"]) == 3
        assert int(bin_08_15.iloc[0]["left_turn_volume_bin"]) == 5
        assert int(bin_08_00.iloc[0]["opposing_through_volume_bin"]) == 1
        assert int(bin_08_15.iloc[0]["opposing_through_volume_bin"]) == 1


# =========================================================================
# Days-of-week filter (Mondays only)
# =========================================================================


class TestDaysOfWeekFilter:

    @pytest.mark.asyncio
    @patch("tsigma.reports.sdk.queries.db_facade")
    @patch("tsigma.reports.left_turn_volume.get_config_at", new_callable=AsyncMock)
    async def test_only_mondays_counted(self, mock_config, mock_facade):
        from tsigma.reports.left_turn_volume import LeftTurnVolumeParams, LeftTurnVolumeReport

        mock_config.return_value = _config_two_approaches(lt_phase=1, opp_phase=2)

        # 2025-06-16 Monday, 2025-06-17 Tuesday, 2025-06-18 Wednesday
        monday = datetime(2025, 6, 16, 8, 0, 0)
        tuesday = datetime(2025, 6, 17, 8, 0, 0)
        wednesday = datetime(2025, 6, 18, 8, 0, 0)

        events = [
            _event(EVENT_DETECTOR_ON, 10, monday + timedelta(minutes=5)),
            _event(EVENT_DETECTOR_ON, 10, monday + timedelta(minutes=10)),
            _event(EVENT_DETECTOR_ON, 10, tuesday + timedelta(minutes=5)),
            _event(EVENT_DETECTOR_ON, 10, wednesday + timedelta(minutes=5)),
            _event(EVENT_DETECTOR_ON, 20, monday + timedelta(minutes=6)),
            _event(EVENT_DETECTOR_ON, 20, tuesday + timedelta(minutes=6)),
            _event(EVENT_DETECTOR_ON, 20, wednesday + timedelta(minutes=6)),
        ]
        mock_facade.get_dataframe = AsyncMock(return_value=_events_to_df(events))

        report = LeftTurnVolumeReport()
        params = LeftTurnVolumeParams(
            signal_id="SIG-001", approach_id="APP-LT",
            start="2025-06-16T00:00:00", end="2025-06-18T23:59:59",
            approach_type="permissive",
            days_of_week=[0],  # Monday only
        )
        result = await report.execute(params, _mock_session())
        r = result.iloc[0]
        assert int(r["left_turn_volume"]) == 2
        assert int(r["opposing_through_volume"]) == 1


# =========================================================================
# Time-of-day filter
# =========================================================================


class TestTimeOfDayFilter:

    @pytest.mark.asyncio
    @patch("tsigma.reports.sdk.queries.db_facade")
    @patch("tsigma.reports.left_turn_volume.get_config_at", new_callable=AsyncMock)
    async def test_outside_hour_window_excluded(self, mock_config, mock_facade):
        from tsigma.reports.left_turn_volume import LeftTurnVolumeParams, LeftTurnVolumeReport

        mock_config.return_value = _config_two_approaches(lt_phase=1, opp_phase=2)
        monday = datetime(2025, 6, 16, 0, 0, 0)
        events = [
            # Before 06:00
            _event(EVENT_DETECTOR_ON, 10, monday + timedelta(hours=5, minutes=30)),
            # Inside 06:00-18:00
            _event(EVENT_DETECTOR_ON, 10, monday + timedelta(hours=9, minutes=0)),
            # After 18:00
            _event(EVENT_DETECTOR_ON, 10, monday + timedelta(hours=19, minutes=0)),
            _event(EVENT_DETECTOR_ON, 20, monday + timedelta(hours=9, minutes=0)),
        ]
        mock_facade.get_dataframe = AsyncMock(return_value=_events_to_df(events))

        report = LeftTurnVolumeReport()
        params = LeftTurnVolumeParams(
            signal_id="SIG-001", approach_id="APP-LT",
            start=_START_ISO, end=_END_ISO,
            approach_type="permissive",
        )
        result = await report.execute(params, _mock_session())
        r = result.iloc[0]
        assert int(r["left_turn_volume"]) == 1
        assert int(r["opposing_through_volume"]) == 1
