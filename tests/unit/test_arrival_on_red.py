"""
Unit tests for the Arrival on Red report plugin.

Mirrors the structure of test_report_with_data.py — class-based tests
with mocked fetch_events_split / get_config_at AsyncMocks. The target
module is tsigma.reports.arrival_on_red.

Test cases cover:
- Empty data / no detections -> empty DataFrame with full schema
- Single detection before green -> 1 arrival on red
- Single detection after green -> 0 arrivals on red
- Multi-bin aggregation -> values split across bins correctly
- Pct calculation with zero detections -> 0.0 not NaN
- Hourly normalization with 15-min bins -> value * 4
"""

from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest

from tsigma.config_resolver import ApproachSnapshot, DetectorSnapshot, SignalConfig
from tsigma.reports.sdk.events import (
    EVENT_DETECTOR_ON,
    EVENT_PHASE_GREEN,
    EVENT_RED_CLEARANCE,
    EVENT_YELLOW_CLEARANCE,
)

# ---------------------------------------------------------------------------
# Helpers (copied from test_report_with_data.py pattern)
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
    """Convert list of fake events to a DataFrame matching fetch_events_split output."""
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


_START_ISO = "2025-06-15T08:00:00"
_END_ISO = "2025-06-15T09:00:00"


def _mock_session():
    """AsyncSession mock — not exercised directly."""
    session = AsyncMock()
    mock_result = MagicMock()
    mock_result.all.return_value = []
    mock_scalars = MagicMock()
    mock_scalars.all.return_value = []
    mock_result.scalars.return_value = mock_scalars
    session.execute = AsyncMock(return_value=mock_result)
    return session


def _config_with_phase(phase: int = 2, channels: tuple[int, ...] = (5,)):
    """SignalConfig with one approach on the given phase and detector channels."""
    approach = ApproachSnapshot(
        approach_id="APP-1",
        signal_id="SIG-001",
        direction_type_id=1,
        protected_phase_number=phase,
        permissive_phase_number=None,
        is_protected_phase_overlap=False,
        is_permissive_phase_overlap=False,
        ped_phase_number=None,
        mph=None,
        description="Northbound",
    )
    detectors = [
        DetectorSnapshot(
            detector_id=f"DET-{ch}",
            approach_id="APP-1",
            detector_channel=ch,
            distance_from_stop_bar=None,
            min_speed_filter=None,
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


_EXPECTED_COLUMNS = {
    "bin_start",
    "phase_number",
    "total_detections",
    "arrivals_on_red",
    "pct_arrivals_on_red",
    "total_vehicles_per_hour",
    "arrivals_on_red_per_hour",
    "total_detector_hits",
    "total_arrival_on_red",
    "pct_arrival_on_red_overall",
}


# =========================================================================
# Arrival on Red — with data
# =========================================================================


class TestArrivalOnRedWithData:

    @pytest.mark.asyncio
    @patch("tsigma.reports.arrival_on_red.fetch_events_split", new_callable=AsyncMock)
    @patch("tsigma.reports.arrival_on_red.get_config_at", new_callable=AsyncMock)
    async def test_empty_data_returns_empty_df_with_schema(self, mock_config, mock_fetch):
        """No events -> empty DataFrame with the full column schema."""
        from tsigma.reports.arrival_on_red import ArrivalOnRedParams, ArrivalOnRedReport

        mock_config.return_value = _config_with_phase(2, (5,))
        mock_fetch.return_value = _events_to_df([])

        report = ArrivalOnRedReport()
        params = ArrivalOnRedParams(signal_id="SIG-001", start=_START_ISO, end=_END_ISO)
        result = await report.execute(params, _mock_session())

        assert isinstance(result, pd.DataFrame)
        assert result.empty
        assert _EXPECTED_COLUMNS.issubset(set(result.columns))

    @pytest.mark.asyncio
    @patch("tsigma.reports.arrival_on_red.fetch_events_split", new_callable=AsyncMock)
    @patch("tsigma.reports.arrival_on_red.get_config_at", new_callable=AsyncMock)
    async def test_no_detector_channels_returns_empty(self, mock_config, mock_fetch):
        """No detector channels for phase -> empty DataFrame."""
        from tsigma.reports.arrival_on_red import ArrivalOnRedParams, ArrivalOnRedReport

        # Config has a phase but no detectors assigned to it
        mock_config.return_value = _config_with_phase(3, (5,))
        mock_fetch.return_value = _events_to_df([])

        report = ArrivalOnRedReport()
        # phase 2 doesn't exist in the config
        params = ArrivalOnRedParams(signal_id="SIG-001", start=_START_ISO, end=_END_ISO)
        result = await report.execute(params, _mock_session())

        assert isinstance(result, pd.DataFrame)
        assert result.empty
        assert _EXPECTED_COLUMNS.issubset(set(result.columns))

    @pytest.mark.asyncio
    @patch("tsigma.reports.arrival_on_red.fetch_events_split", new_callable=AsyncMock)
    @patch("tsigma.reports.arrival_on_red.get_config_at", new_callable=AsyncMock)
    async def test_single_detection_before_green_is_aor(self, mock_config, mock_fetch):
        """A detection timestamped before green start counts as arrival on red."""
        from tsigma.reports.arrival_on_red import ArrivalOnRedParams, ArrivalOnRedReport

        mock_config.return_value = _config_with_phase(2, (5,))

        t0 = datetime(2025, 6, 15, 8, 0, 0)
        # Detection occurs during red (before green_start)
        events = [
            _event(EVENT_RED_CLEARANCE, 2, t0),
            _event(EVENT_DETECTOR_ON, 5, t0 + timedelta(seconds=5)),
            _event(EVENT_PHASE_GREEN, 2, t0 + timedelta(seconds=30)),
            _event(EVENT_YELLOW_CLEARANCE, 2, t0 + timedelta(seconds=60)),
            _event(EVENT_RED_CLEARANCE, 2, t0 + timedelta(seconds=65)),
            _event(EVENT_PHASE_GREEN, 2, t0 + timedelta(seconds=90)),
        ]
        mock_fetch.return_value = _events_to_df(events)

        report = ArrivalOnRedReport()
        params = ArrivalOnRedParams(signal_id="SIG-001", start=_START_ISO, end=_END_ISO)
        result = await report.execute(params, _mock_session())

        assert isinstance(result, pd.DataFrame)
        assert not result.empty
        assert int(result["total_detections"].sum()) == 1
        assert int(result["arrivals_on_red"].sum()) == 1
        assert int(result.iloc[0]["total_arrival_on_red"]) == 1
        assert float(result.iloc[0]["pct_arrival_on_red_overall"]) == 100.0

    @pytest.mark.asyncio
    @patch("tsigma.reports.arrival_on_red.fetch_events_split", new_callable=AsyncMock)
    @patch("tsigma.reports.arrival_on_red.get_config_at", new_callable=AsyncMock)
    async def test_single_detection_after_green_not_aor(self, mock_config, mock_fetch):
        """A detection timestamped after green start is not arrival on red."""
        from tsigma.reports.arrival_on_red import ArrivalOnRedParams, ArrivalOnRedReport

        mock_config.return_value = _config_with_phase(2, (5,))

        t0 = datetime(2025, 6, 15, 8, 0, 0)
        events = [
            _event(EVENT_PHASE_GREEN, 2, t0),
            _event(EVENT_DETECTOR_ON, 5, t0 + timedelta(seconds=10)),
            _event(EVENT_YELLOW_CLEARANCE, 2, t0 + timedelta(seconds=30)),
            _event(EVENT_RED_CLEARANCE, 2, t0 + timedelta(seconds=35)),
            _event(EVENT_PHASE_GREEN, 2, t0 + timedelta(seconds=60)),
        ]
        mock_fetch.return_value = _events_to_df(events)

        report = ArrivalOnRedReport()
        params = ArrivalOnRedParams(signal_id="SIG-001", start=_START_ISO, end=_END_ISO)
        result = await report.execute(params, _mock_session())

        assert int(result["total_detections"].sum()) == 1
        assert int(result["arrivals_on_red"].sum()) == 0
        assert float(result.iloc[0]["pct_arrival_on_red_overall"]) == 0.0

    @pytest.mark.asyncio
    @patch("tsigma.reports.arrival_on_red.fetch_events_split", new_callable=AsyncMock)
    @patch("tsigma.reports.arrival_on_red.get_config_at", new_callable=AsyncMock)
    async def test_multi_bin_aggregation(self, mock_config, mock_fetch):
        """Detections in different 15-min bins produce separate rows."""
        from tsigma.reports.arrival_on_red import ArrivalOnRedParams, ArrivalOnRedReport

        mock_config.return_value = _config_with_phase(2, (5,))

        t0 = datetime(2025, 6, 15, 8, 0, 0)
        events = [
            # Cycle in bin 0 (08:00-08:15)
            _event(EVENT_RED_CLEARANCE, 2, t0),
            _event(EVENT_DETECTOR_ON, 5, t0 + timedelta(minutes=2)),      # red
            _event(EVENT_PHASE_GREEN, 2, t0 + timedelta(minutes=3)),
            _event(EVENT_DETECTOR_ON, 5, t0 + timedelta(minutes=5)),      # green
            _event(EVENT_YELLOW_CLEARANCE, 2, t0 + timedelta(minutes=6)),
            _event(EVENT_RED_CLEARANCE, 2, t0 + timedelta(minutes=7)),
            # Cycle in bin 1 (08:15-08:30)
            _event(EVENT_DETECTOR_ON, 5, t0 + timedelta(minutes=18)),     # red
            _event(EVENT_PHASE_GREEN, 2, t0 + timedelta(minutes=20)),
            _event(EVENT_DETECTOR_ON, 5, t0 + timedelta(minutes=22)),     # green
            _event(EVENT_YELLOW_CLEARANCE, 2, t0 + timedelta(minutes=25)),
            _event(EVENT_RED_CLEARANCE, 2, t0 + timedelta(minutes=26)),
            _event(EVENT_PHASE_GREEN, 2, t0 + timedelta(minutes=35)),
        ]
        mock_fetch.return_value = _events_to_df(events)

        report = ArrivalOnRedReport()
        params = ArrivalOnRedParams(
            signal_id="SIG-001", start=_START_ISO, end=_END_ISO,
            bin_size_minutes=15,
        )
        result = await report.execute(params, _mock_session())

        assert len(result) >= 2
        assert int(result["total_detections"].sum()) == 4
        assert int(result["arrivals_on_red"].sum()) == 2
        # Broadcast summary: same value on every row
        assert result["total_detector_hits"].nunique() == 1
        assert int(result.iloc[0]["total_detector_hits"]) == 4
        assert int(result.iloc[0]["total_arrival_on_red"]) == 2
        assert float(result.iloc[0]["pct_arrival_on_red_overall"]) == 50.0

    @pytest.mark.asyncio
    @patch("tsigma.reports.arrival_on_red.fetch_events_split", new_callable=AsyncMock)
    @patch("tsigma.reports.arrival_on_red.get_config_at", new_callable=AsyncMock)
    async def test_pct_is_zero_not_nan_when_no_detections_in_bin(
        self, mock_config, mock_fetch,
    ):
        """pct_arrivals_on_red must be 0.0 (not NaN) when total_detections is 0."""
        from tsigma.reports.arrival_on_red import ArrivalOnRedParams, ArrivalOnRedReport

        mock_config.return_value = _config_with_phase(2, (5,))

        t0 = datetime(2025, 6, 15, 8, 0, 0)
        # A cycle with no detections in the cycle
        events = [
            _event(EVENT_RED_CLEARANCE, 2, t0),
            _event(EVENT_PHASE_GREEN, 2, t0 + timedelta(seconds=30)),
            _event(EVENT_YELLOW_CLEARANCE, 2, t0 + timedelta(seconds=60)),
            _event(EVENT_RED_CLEARANCE, 2, t0 + timedelta(seconds=65)),
            _event(EVENT_PHASE_GREEN, 2, t0 + timedelta(seconds=90)),
        ]
        mock_fetch.return_value = _events_to_df(events)

        report = ArrivalOnRedReport()
        params = ArrivalOnRedParams(signal_id="SIG-001", start=_START_ISO, end=_END_ISO)
        result = await report.execute(params, _mock_session())

        # If any rows, they must have 0.0 not NaN
        for _, row in result.iterrows():
            assert row["pct_arrivals_on_red"] == 0.0 or row["pct_arrivals_on_red"] >= 0.0
            assert not pd.isna(row["pct_arrivals_on_red"])
        # Overall pct when zero detections exist
        if not result.empty:
            assert float(result.iloc[0]["pct_arrival_on_red_overall"]) == 0.0

    @pytest.mark.asyncio
    @patch("tsigma.reports.arrival_on_red.fetch_events_split", new_callable=AsyncMock)
    @patch("tsigma.reports.arrival_on_red.get_config_at", new_callable=AsyncMock)
    async def test_hourly_normalization_15min_bin(self, mock_config, mock_fetch):
        """With 15-min bins, per-hour rate should equal bin count * 4."""
        from tsigma.reports.arrival_on_red import ArrivalOnRedParams, ArrivalOnRedReport

        mock_config.return_value = _config_with_phase(2, (5,))

        t0 = datetime(2025, 6, 15, 8, 0, 0)
        events = [
            _event(EVENT_RED_CLEARANCE, 2, t0),
            _event(EVENT_DETECTOR_ON, 5, t0 + timedelta(seconds=5)),      # red
            _event(EVENT_DETECTOR_ON, 5, t0 + timedelta(seconds=10)),     # red
            _event(EVENT_PHASE_GREEN, 2, t0 + timedelta(seconds=30)),
            _event(EVENT_DETECTOR_ON, 5, t0 + timedelta(seconds=35)),     # green
            _event(EVENT_YELLOW_CLEARANCE, 2, t0 + timedelta(seconds=60)),
            _event(EVENT_RED_CLEARANCE, 2, t0 + timedelta(seconds=65)),
            _event(EVENT_PHASE_GREEN, 2, t0 + timedelta(seconds=90)),
        ]
        mock_fetch.return_value = _events_to_df(events)

        report = ArrivalOnRedReport()
        params = ArrivalOnRedParams(
            signal_id="SIG-001", start=_START_ISO, end=_END_ISO,
            bin_size_minutes=15,
        )
        result = await report.execute(params, _mock_session())

        assert len(result) >= 1
        row = result.iloc[0]
        # 3 detections in a single 15-min bin -> 12 per hour
        assert int(row["total_detections"]) == 3
        assert float(row["total_vehicles_per_hour"]) == 12.0
        # 2 arrivals on red -> 8 per hour
        assert int(row["arrivals_on_red"]) == 2
        assert float(row["arrivals_on_red_per_hour"]) == 8.0

    @pytest.mark.asyncio
    @patch("tsigma.reports.arrival_on_red.fetch_events_split", new_callable=AsyncMock)
    @patch("tsigma.reports.arrival_on_red.get_config_at", new_callable=AsyncMock)
    async def test_output_schema_complete(self, mock_config, mock_fetch):
        """Non-empty result has every expected column."""
        from tsigma.reports.arrival_on_red import ArrivalOnRedParams, ArrivalOnRedReport

        mock_config.return_value = _config_with_phase(2, (5,))

        t0 = datetime(2025, 6, 15, 8, 0, 0)
        events = [
            _event(EVENT_RED_CLEARANCE, 2, t0),
            _event(EVENT_DETECTOR_ON, 5, t0 + timedelta(seconds=5)),
            _event(EVENT_PHASE_GREEN, 2, t0 + timedelta(seconds=30)),
            _event(EVENT_YELLOW_CLEARANCE, 2, t0 + timedelta(seconds=60)),
            _event(EVENT_RED_CLEARANCE, 2, t0 + timedelta(seconds=65)),
            _event(EVENT_PHASE_GREEN, 2, t0 + timedelta(seconds=90)),
        ]
        mock_fetch.return_value = _events_to_df(events)

        report = ArrivalOnRedReport()
        params = ArrivalOnRedParams(signal_id="SIG-001", start=_START_ISO, end=_END_ISO)
        result = await report.execute(params, _mock_session())

        missing = _EXPECTED_COLUMNS - set(result.columns)
        assert not missing, f"Missing columns: {missing}"


# =========================================================================
# Registration
# =========================================================================


class TestArrivalOnRedRegistration:

    def test_registered(self):
        """Report registers itself under the 'arrival-on-red' name."""
        from tsigma.reports.registry import ReportRegistry

        assert "arrival-on-red" in ReportRegistry._reports

    def test_metadata(self):
        """Metadata has the expected name and category."""
        from tsigma.reports.arrival_on_red import ArrivalOnRedReport

        assert ArrivalOnRedReport.metadata.name == "arrival-on-red"
        assert ArrivalOnRedReport.metadata.category in ("dashboard", "standard", "detailed")
