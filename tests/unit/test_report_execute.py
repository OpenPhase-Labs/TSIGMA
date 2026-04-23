"""
Unit tests for report execute() methods.

Each test calls execute() with minimal valid params and a mocked session
that returns empty result sets.  The goal is to verify:
1. execute() is callable with valid Pydantic params and a mock session
2. execute() returns a pd.DataFrame
3. execute() handles empty result sets gracefully (no crash)
"""

from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest

from tsigma.config_resolver import SignalConfig

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


def _make_mock_session():
    """Create a mock AsyncSession that returns empty results."""
    mock_session = AsyncMock()
    mock_result = MagicMock()
    mock_result.all.return_value = []
    mock_result.scalar.return_value = None
    mock_result.one_or_none.return_value = None
    mock_scalars = MagicMock()
    mock_scalars.all.return_value = []
    mock_result.scalars.return_value = mock_scalars
    mock_session.execute = AsyncMock(return_value=mock_result)
    return mock_session


def _empty_df():
    """Empty DataFrame matching ControllerEventLog query shape."""
    return pd.DataFrame(columns=["event_code", "event_param", "event_time"])


# ISO-8601 string constants for params
_START = "2025-01-01T00:00:00"
_END = "2025-01-01T01:00:00"
_SIGNAL = "SIG-001"


def _empty_config():
    """SignalConfig with no approaches/detectors."""
    return SignalConfig(
        signal_id=_SIGNAL,
        as_of=datetime(2025, 1, 1),
        from_audit=False,
        approaches=[],
        detectors=[],
    )


# ---------------------------------------------------------------------------
# Tests — previously existing (8 reports)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
@patch("tsigma.reports.approach_delay.get_config_at", new_callable=AsyncMock)
async def test_approach_delay_execute(mock_get_config):
    from tsigma.reports.approach_delay import ApproachDelayParams, ApproachDelayReport

    mock_get_config.return_value = _empty_config()
    session = _make_mock_session()

    report = ApproachDelayReport()
    params = ApproachDelayParams(signal_id=_SIGNAL, start=_START, end=_END)
    result = await report.execute(params, session)

    assert isinstance(result, pd.DataFrame)


@pytest.mark.asyncio
@patch("tsigma.reports.approach_volume.get_config_at", new_callable=AsyncMock)
async def test_approach_volume_execute(mock_get_config):
    from tsigma.reports.approach_volume import ApproachVolumeParams, ApproachVolumeReport

    mock_get_config.return_value = _empty_config()
    session = _make_mock_session()

    report = ApproachVolumeReport()
    params = ApproachVolumeParams(signal_id=_SIGNAL, start=_START, end=_END)
    result = await report.execute(params, session)

    assert isinstance(result, pd.DataFrame)


@pytest.mark.asyncio
@patch("tsigma.reports.sdk.queries.db_facade")
async def test_split_monitor_execute(mock_queries_facade):
    from tsigma.reports.split_monitor import SplitMonitorParams, SplitMonitorReport

    mock_queries_facade.get_dataframe = AsyncMock(return_value=_empty_df())
    session = _make_mock_session()

    report = SplitMonitorReport()
    params = SplitMonitorParams(signal_id=_SIGNAL, start=_START, end=_END)
    result = await report.execute(params, session)

    assert isinstance(result, pd.DataFrame)


@pytest.mark.asyncio
@patch("tsigma.reports.sdk.queries.db_facade")
@patch("tsigma.reports.sdk.cycles.db_facade")
@patch("tsigma.reports.purdue_diagram.get_config_at", new_callable=AsyncMock)
async def test_purdue_diagram_execute(mock_get_config, mock_cycles_facade, mock_queries_facade):
    from tsigma.reports.purdue_diagram import PurdueDiagramParams, PurdueDiagramReport

    mock_get_config.return_value = _empty_config()
    mock_cycles_facade.get_dataframe = AsyncMock(return_value=pd.DataFrame())
    mock_queries_facade.get_dataframe = AsyncMock(return_value=_empty_df())
    session = _make_mock_session()

    report = PurdueDiagramReport()
    params = PurdueDiagramParams(signal_id=_SIGNAL, start=_START, end=_END, phase_number=2)
    result = await report.execute(params, session)

    assert isinstance(result, pd.DataFrame)


@pytest.mark.asyncio
@patch("tsigma.reports.sdk.queries.db_facade")
async def test_phase_termination_execute(mock_queries_facade):
    from tsigma.reports.phase_termination import PhaseTerminationParams, PhaseTerminationReport

    mock_queries_facade.get_dataframe = AsyncMock(return_value=_empty_df())
    session = _make_mock_session()

    report = PhaseTerminationReport()
    params = PhaseTerminationParams(signal_id=_SIGNAL, start=_START, end=_END)
    result = await report.execute(params, session)

    assert isinstance(result, pd.DataFrame)


@pytest.mark.asyncio
@patch("tsigma.reports.arrivals_on_green.load_channel_to_phase", new_callable=AsyncMock)
async def test_arrivals_on_green_execute(mock_load):
    from tsigma.reports.arrivals_on_green import ArrivalsOnGreenParams, ArrivalsOnGreenReport

    mock_load.return_value = {}  # no channels -> early return
    session = _make_mock_session()

    report = ArrivalsOnGreenReport()
    params = ArrivalsOnGreenParams(signal_id=_SIGNAL, start=_START, end=_END)
    result = await report.execute(params, session)

    assert isinstance(result, pd.DataFrame)


@pytest.mark.asyncio
@patch("tsigma.reports.yellow_red_actuations.get_config_at", new_callable=AsyncMock)
async def test_yellow_red_actuations_execute(mock_get_config):
    from tsigma.reports.yellow_red_actuations import YellowRedActuationsParams, YellowRedActuationsReport

    mock_get_config.return_value = _empty_config()
    session = _make_mock_session()

    report = YellowRedActuationsReport()
    params = YellowRedActuationsParams(signal_id=_SIGNAL, start=_START, end=_END, phase_number=2)
    result = await report.execute(params, session)

    assert isinstance(result, pd.DataFrame)


@pytest.mark.asyncio
@patch("tsigma.reports.ped_delay.load_channel_to_ped_phase", new_callable=AsyncMock)
async def test_ped_delay_execute(mock_load):
    from tsigma.reports.ped_delay import PedDelayParams, PedDelayReport

    mock_load.return_value = {}  # no ped channels -> early return
    session = _make_mock_session()

    report = PedDelayReport()
    params = PedDelayParams(signal_id=_SIGNAL, start=_START, end=_END)
    result = await report.execute(params, session)

    assert isinstance(result, pd.DataFrame)


# ---------------------------------------------------------------------------
# Tests — newly added reports
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
@patch("tsigma.reports.approach_speed.get_config_at", new_callable=AsyncMock)
async def test_approach_speed_execute_empty(mock_get_config):
    from tsigma.reports.approach_speed import ApproachSpeedParams, ApproachSpeedReport

    mock_get_config.return_value = _empty_config()
    session = _make_mock_session()

    report = ApproachSpeedReport()
    params = ApproachSpeedParams(signal_id=_SIGNAL, start=_START, end=_END)
    result = await report.execute(params, session)

    assert isinstance(result, pd.DataFrame)
    assert result.empty


@pytest.mark.asyncio
async def test_bike_volume_execute_no_channels():
    from tsigma.reports.bike_volume import BikeVolumeParams, BikeVolumeReport

    session = _make_mock_session()

    report = BikeVolumeReport()
    # No detector_channels provided — early return with empty DataFrame
    params = BikeVolumeParams(signal_id=_SIGNAL, start=_START, end=_END)
    result = await report.execute(params, session)

    assert isinstance(result, pd.DataFrame)
    assert result.empty


@pytest.mark.asyncio
@patch("tsigma.reports.sdk.queries.db_facade")
async def test_bike_volume_execute_with_channels(mock_queries_facade):
    from tsigma.reports.bike_volume import BikeVolumeParams, BikeVolumeReport

    mock_queries_facade.get_dataframe = AsyncMock(return_value=_empty_df())
    session = _make_mock_session()

    report = BikeVolumeReport()
    params = BikeVolumeParams(
        signal_id=_SIGNAL, start=_START, end=_END,
        detector_channels=[5, 6],
    )
    result = await report.execute(params, session)

    assert isinstance(result, pd.DataFrame)
    assert result.empty


@pytest.mark.asyncio
@patch("tsigma.reports.green_time_utilization.load_channel_to_phase", new_callable=AsyncMock)
async def test_green_time_utilization_execute_empty(mock_load):
    from tsigma.reports.green_time_utilization import GreenTimeUtilizationParams, GreenTimeUtilizationReport

    mock_load.return_value = {}  # no channels -> early return
    session = _make_mock_session()

    report = GreenTimeUtilizationReport()
    params = GreenTimeUtilizationParams(signal_id=_SIGNAL, start=_START, end=_END)
    result = await report.execute(params, session)

    assert isinstance(result, pd.DataFrame)
    assert result.empty


@pytest.mark.asyncio
@patch("tsigma.reports.left_turn_gap.get_config_at", new_callable=AsyncMock)
async def test_left_turn_gap_execute_empty(mock_get_config):
    from tsigma.reports.left_turn_gap import LeftTurnGapParams, LeftTurnGapReport

    mock_get_config.return_value = _empty_config()
    session = _make_mock_session()

    report = LeftTurnGapReport()
    # phase_number required; empty config means no det_channels -> early return
    params = LeftTurnGapParams(signal_id=_SIGNAL, start=_START, end=_END, phase_number=2)
    result = await report.execute(params, session)

    assert isinstance(result, pd.DataFrame)
    assert result.empty


@pytest.mark.asyncio
async def test_link_pivot_execute_empty():
    from tsigma.reports.link_pivot import LinkPivotParams, LinkPivotReport

    session = _make_mock_session()

    report = LinkPivotReport()
    params = LinkPivotParams(
        route_id="ROUTE-001",
        start=_START,
        end=_END,
        direction=1,
    )
    result = await report.execute(params, session)

    assert isinstance(result, pd.DataFrame)
    assert result.empty


@pytest.mark.asyncio
@patch("tsigma.reports.preemption.fetch_events", new_callable=AsyncMock)
async def test_preemption_execute_empty(mock_fetch):
    from tsigma.reports.preemption import PreemptionParams, PreemptionReport

    mock_fetch.return_value = _empty_df()
    session = _make_mock_session()

    report = PreemptionReport()
    params = PreemptionParams(signal_id=_SIGNAL, start=_START, end=_END)
    result = await report.execute(params, session)

    assert isinstance(result, pd.DataFrame)
    assert result.empty


@pytest.mark.asyncio
@patch("tsigma.reports.ramp_metering.fetch_events_split", new_callable=AsyncMock)
async def test_ramp_metering_execute_empty(mock_fetch):
    from tsigma.reports.ramp_metering import RampMeteringParams, RampMeteringReport

    mock_fetch.return_value = _empty_df()
    session = _make_mock_session()

    report = RampMeteringReport()
    params = RampMeteringParams(
        signal_id=_SIGNAL,
        start_time=_START,
        end_time=_END,
        demand_detector_channel=1,
        passage_detector_channel=2,
    )
    result = await report.execute(params, session)

    assert isinstance(result, pd.DataFrame)
    assert result.empty


@pytest.mark.asyncio
@patch("tsigma.reports.red_light_monitor.get_config_at", new_callable=AsyncMock)
async def test_red_light_monitor_execute_empty(mock_get_config):
    from tsigma.reports.red_light_monitor import RedLightMonitorParams, RedLightMonitorReport

    mock_get_config.return_value = _empty_config()
    session = _make_mock_session()

    report = RedLightMonitorReport()
    params = RedLightMonitorParams(signal_id=_SIGNAL, start=_START, end=_END, phase_number=2)
    result = await report.execute(params, session)

    assert isinstance(result, pd.DataFrame)
    assert result.empty


@pytest.mark.asyncio
@patch("tsigma.reports.split_failure.get_config_at", new_callable=AsyncMock)
async def test_split_failure_execute_empty(mock_get_config):
    from tsigma.reports.split_failure import SplitFailureParams, SplitFailureReport

    mock_get_config.return_value = _empty_config()
    session = _make_mock_session()

    report = SplitFailureReport()
    params = SplitFailureParams(signal_id=_SIGNAL, start=_START, end=_END, phase_number=2)
    result = await report.execute(params, session)

    assert isinstance(result, pd.DataFrame)
    assert result.empty


@pytest.mark.asyncio
@patch("tsigma.reports.time_space_diagram.fetch_events", new_callable=AsyncMock)
async def test_time_space_diagram_execute_empty(mock_fetch):
    from tsigma.reports.time_space_diagram import TimeSpaceDiagramParams, TimeSpaceDiagramReport

    mock_fetch.return_value = _empty_df()
    session = _make_mock_session()

    report = TimeSpaceDiagramReport()
    params = TimeSpaceDiagramParams(
        signal_ids=["SIG-001", "SIG-002"],
        start_time=_START,
        end_time=_END,
        direction_phase_map={"SIG-001": 2, "SIG-002": 2},
    )
    result = await report.execute(params, session)

    assert isinstance(result, pd.DataFrame)
    assert result.empty


@pytest.mark.asyncio
@patch("tsigma.reports.timing_and_actuations.fetch_events", new_callable=AsyncMock)
async def test_timing_and_actuations_execute_empty(mock_fetch):
    from tsigma.reports.timing_and_actuations import TimingAndActuationsParams, TimingAndActuationsReport

    mock_fetch.return_value = _empty_df()
    session = _make_mock_session()

    report = TimingAndActuationsReport()
    params = TimingAndActuationsParams(signal_id=_SIGNAL, start=_START, end=_END)
    result = await report.execute(params, session)

    assert isinstance(result, pd.DataFrame)
    assert result.empty


@pytest.mark.asyncio
@patch("tsigma.reports.transit_signal_priority.fetch_events", new_callable=AsyncMock)
async def test_transit_signal_priority_execute_empty(mock_fetch):
    from tsigma.reports.transit_signal_priority import TransitSignalPriorityParams, TransitSignalPriorityReport

    mock_fetch.return_value = _empty_df()
    session = _make_mock_session()

    report = TransitSignalPriorityReport()
    params = TransitSignalPriorityParams(
        signal_id=_SIGNAL,
        start_time=_START,
        end_time=_END,
    )
    result = await report.execute(params, session)

    assert isinstance(result, pd.DataFrame)
    assert result.empty


@pytest.mark.asyncio
@patch("tsigma.reports.turning_movement_counts.load_channel_to_approach", new_callable=AsyncMock)
async def test_turning_movement_counts_execute_empty(mock_load):
    from tsigma.reports.turning_movement_counts import TurningMovementCountsParams, TurningMovementCountsReport

    mock_load.return_value = {}  # no channels -> early return
    session = _make_mock_session()

    report = TurningMovementCountsReport()
    params = TurningMovementCountsParams(signal_id=_SIGNAL, start=_START, end=_END)
    result = await report.execute(params, session)

    assert isinstance(result, pd.DataFrame)
    assert result.empty


@pytest.mark.asyncio
@patch("tsigma.reports.wait_time.get_config_at", new_callable=AsyncMock)
async def test_wait_time_execute_empty(mock_get_config):
    from tsigma.reports.wait_time import WaitTimeParams, WaitTimeReport

    mock_get_config.return_value = _empty_config()
    session = _make_mock_session()

    report = WaitTimeReport()
    params = WaitTimeParams(signal_id=_SIGNAL, start=_START, end=_END, phase_number=2)
    result = await report.execute(params, session)

    assert isinstance(result, pd.DataFrame)
    assert result.empty


# ---------------------------------------------------------------------------
# Tests — SDK config helpers
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
@patch("tsigma.reports.sdk.config.get_config_at", new_callable=AsyncMock)
async def test_load_channel_to_phase_empty(mock_get_config):
    from tsigma.reports.sdk.config import load_channel_to_phase

    mock_get_config.return_value = _empty_config()
    session = _make_mock_session()

    result = await load_channel_to_phase(session, "SIG-001", datetime(2025, 1, 1))

    assert isinstance(result, dict)
    assert result == {}


@pytest.mark.asyncio
@patch("tsigma.reports.sdk.config.get_config_at", new_callable=AsyncMock)
async def test_load_channels_for_phase_empty(mock_get_config):
    from tsigma.reports.sdk.config import load_channels_for_phase

    mock_get_config.return_value = _empty_config()
    session = _make_mock_session()

    result = await load_channels_for_phase(session, "SIG-001", 2, datetime(2025, 1, 1))

    assert isinstance(result, set)
    assert result == set()


@pytest.mark.asyncio
@patch("tsigma.reports.sdk.config.get_config_at", new_callable=AsyncMock)
async def test_load_channel_to_ped_phase_empty(mock_get_config):
    from tsigma.reports.sdk.config import load_channel_to_ped_phase

    mock_get_config.return_value = _empty_config()
    session = _make_mock_session()

    result = await load_channel_to_ped_phase(session, "SIG-001", datetime(2025, 1, 1))

    assert isinstance(result, dict)
    assert result == {}


@pytest.mark.asyncio
@patch("tsigma.reports.sdk.config.get_config_at", new_callable=AsyncMock)
async def test_load_channel_to_approach_empty(mock_get_config):
    from tsigma.reports.sdk.config import load_channel_to_approach

    mock_get_config.return_value = _empty_config()
    session = _make_mock_session()

    result = await load_channel_to_approach(session, "SIG-001", datetime(2025, 1, 1))

    assert isinstance(result, dict)
    assert result == {}


# ---------------------------------------------------------------------------
# Tests — ReportRegistry export()
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_registry_export_json():
    """export(format='json') returns valid JSON bytes for a DataFrame-returning report."""
    from tsigma.reports.registry import Report, ReportMetadata

    class _StubParams(pd.DataFrame):
        """Not a real params class — just a placeholder for the stub."""
        pass

    class _StubReport(Report):
        metadata = ReportMetadata(
            name="stub",
            description="stub",
            category="standard",
            estimated_time="fast",
        )

        async def execute(self, params, session):
            return pd.DataFrame([{"a": 1, "b": "two"}])

    session = _make_mock_session()
    report = _StubReport()
    raw = await report.export({}, session, format="json")

    assert isinstance(raw, bytes)
    import json
    data = json.loads(raw)
    assert data == [{"a": 1, "b": "two"}]


@pytest.mark.asyncio
async def test_registry_export_csv():
    """export(format='csv') returns CSV bytes with header + data rows."""
    from tsigma.reports.registry import Report, ReportMetadata

    class _StubReport(Report):
        metadata = ReportMetadata(
            name="stub",
            description="stub",
            category="standard",
            estimated_time="fast",
        )

        async def execute(self, params, session):
            return pd.DataFrame([{"col1": "val1", "col2": "val2"}])

    session = _make_mock_session()
    report = _StubReport()
    raw = await report.export({}, session, format="csv")

    assert isinstance(raw, bytes)
    text = raw.decode()
    assert "col1" in text
    assert "val1" in text


@pytest.mark.asyncio
async def test_registry_export_csv_empty_df():
    """export(format='csv') returns header-only bytes when execute returns empty DataFrame."""
    from tsigma.reports.registry import Report, ReportMetadata

    class _StubReport(Report):
        metadata = ReportMetadata(
            name="stub",
            description="stub",
            category="standard",
            estimated_time="fast",
        )

        async def execute(self, params, session):
            return pd.DataFrame()

    session = _make_mock_session()
    report = _StubReport()
    raw = await report.export({}, session, format="csv")

    # Empty DataFrame with no columns -> empty bytes
    assert raw == b""


@pytest.mark.asyncio
async def test_registry_export_unsupported_format():
    """export() raises ValueError for unsupported format."""
    from tsigma.reports.registry import Report, ReportMetadata

    class _StubReport(Report):
        metadata = ReportMetadata(
            name="stub",
            description="stub",
            category="standard",
            estimated_time="fast",
        )

        async def execute(self, params, session):
            return pd.DataFrame()

    session = _make_mock_session()
    report = _StubReport()

    with pytest.raises(ValueError, match="does not support format"):
        await report.export({}, session, format="xml")


def test_registry_register_and_get():
    """ReportRegistry.register() and get() round-trip."""
    from tsigma.reports.registry import Report, ReportMetadata, ReportRegistry

    @ReportRegistry.register("_test_dummy")
    class _DummyReport(Report):
        metadata = ReportMetadata(
            name="_test_dummy",
            description="test",
            category="standard",
            estimated_time="fast",
        )

        async def execute(self, params, session):
            return pd.DataFrame()

    cls = ReportRegistry.get("_test_dummy")
    assert cls is _DummyReport


def test_registry_get_unknown():
    """ReportRegistry.get() raises ValueError for unknown report."""
    from tsigma.reports.registry import ReportRegistry

    with pytest.raises(ValueError, match="Unknown report"):
        ReportRegistry.get("nonexistent-report-xyz")


def test_registry_list_all():
    """ReportRegistry.list_all() returns a dict copy."""
    from tsigma.reports.registry import ReportRegistry

    all_reports = ReportRegistry.list_all()
    assert isinstance(all_reports, dict)
    # Should contain at least the reports we imported above
    assert len(all_reports) > 0
