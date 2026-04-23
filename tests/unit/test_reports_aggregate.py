"""Tests for reports using cycle aggregate tables instead of raw events."""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest

from tsigma.reports.arrivals_on_green import ArrivalsOnGreenParams
from tsigma.reports.phase_termination import PhaseTerminationParams
from tsigma.reports.purdue_diagram import PurdueDiagramParams
from tsigma.reports.split_monitor import SplitMonitorParams


def _boundary_df(**kwargs):
    """Create a DataFrame mimicking fetch_cycle_boundaries output."""
    defaults = {
        "green_start": [datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)],
        "yellow_start": [datetime(2026, 1, 1, 12, 0, 30, tzinfo=timezone.utc)],
        "red_start": [datetime(2026, 1, 1, 12, 0, 34, tzinfo=timezone.utc)],
        "cycle_end": [datetime(2026, 1, 1, 12, 1, 0, tzinfo=timezone.utc)],
        "green_duration_seconds": [30.0],
        "yellow_duration_seconds": [4.0],
        "red_duration_seconds": [26.0],
        "cycle_duration_seconds": [60.0],
        "termination_type": ["gap_out"],
    }
    defaults.update(kwargs)
    return pd.DataFrame(defaults)


def _arrival_df(**kwargs):
    """Create a DataFrame mimicking fetch_cycle_arrivals output."""
    defaults = {
        "arrival_time": [datetime(2026, 1, 1, 12, 0, 10, tzinfo=timezone.utc)],
        "detector_channel": [5],
        "green_start": [datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)],
        "time_in_cycle_seconds": [10.0],
        "phase_state": ["green"],
    }
    defaults.update(kwargs)
    return pd.DataFrame(defaults)


class TestPCDFromAggregates:
    """PCD report uses cycle_boundary + cycle_detector_arrival."""

    @pytest.mark.asyncio
    @patch("tsigma.reports.sdk.cycles.db_facade")
    async def test_returns_cycles_from_aggregates(self, mock_cycles_facade):
        """Historical PCD builds cycles from aggregate tables."""
        from tsigma.reports.purdue_diagram import PurdueDiagramReport

        report = PurdueDiagramReport()

        boundary = _boundary_df()
        arrival1 = _arrival_df(
            time_in_cycle_seconds=[5.0], phase_state=["green"],
            arrival_time=[datetime(2026, 1, 1, 12, 0, 5, tzinfo=timezone.utc)],
        )
        arrival2 = _arrival_df(
            time_in_cycle_seconds=[32.0], phase_state=["yellow"],
            arrival_time=[datetime(2026, 1, 1, 12, 0, 32, tzinfo=timezone.utc)],
        )
        arrivals = pd.concat([arrival1, arrival2], ignore_index=True)

        # get_dataframe is called twice: first for boundaries, then for arrivals
        mock_cycles_facade.get_dataframe = AsyncMock(
            side_effect=[boundary, arrivals]
        )

        with patch(
            "tsigma.reports.purdue_diagram.get_config_at",
            new_callable=AsyncMock,
        ) as mock_config:
            mock_cfg = MagicMock()
            mock_cfg.detector_channels_for_phase.return_value = {5}
            mock_config.return_value = mock_cfg

            params = PurdueDiagramParams(
                signal_id="SIG-001",
                start="2026-01-01T00:00:00Z",
                end="2026-01-01T23:59:59Z",
                phase_number=2,
            )
            result = await report.execute(params, AsyncMock())

        assert isinstance(result, pd.DataFrame)
        assert len(result) >= 1
        assert "green_start" in result.columns
        assert "termination_type" in result.columns

    @pytest.mark.asyncio
    @patch("tsigma.reports.sdk.cycles.db_facade")
    async def test_empty_aggregates_returns_empty(self, mock_cycles_facade):
        """No cycles in range returns empty DataFrame."""
        from tsigma.reports.purdue_diagram import PurdueDiagramReport

        report = PurdueDiagramReport()

        # Boundaries empty -> falls through to raw events path
        # Raw events also empty -> returns empty DataFrame
        mock_cycles_facade.get_dataframe = AsyncMock(return_value=pd.DataFrame())

        with patch(
            "tsigma.reports.purdue_diagram.get_config_at",
            new_callable=AsyncMock,
        ) as mock_config:
            mock_cfg = MagicMock()
            mock_cfg.detector_channels_for_phase.return_value = {5}
            mock_config.return_value = mock_cfg

            with patch(
                "tsigma.reports.sdk.queries.db_facade"
            ) as mock_queries_facade:
                mock_queries_facade.get_dataframe = AsyncMock(
                    return_value=pd.DataFrame()
                )

                params = PurdueDiagramParams(
                    signal_id="SIG-001",
                    start="2026-01-01T00:00:00Z",
                    end="2026-01-01T23:59:59Z",
                    phase_number=2,
                )
                result = await report.execute(params, AsyncMock())

        assert isinstance(result, pd.DataFrame)
        assert result.empty


class TestSplitMonitorFromAggregates:
    """Split monitor uses cycle_boundary for durations."""

    @pytest.mark.asyncio
    @patch("tsigma.reports.sdk.cycles.db_facade")
    async def test_uses_cycle_boundary(self, mock_cycles_facade):
        from tsigma.reports.split_monitor import SplitMonitorReport

        report = SplitMonitorReport()

        boundary = _boundary_df(termination_type=["max_out"])
        mock_cycles_facade.get_dataframe = AsyncMock(return_value=boundary)

        params = SplitMonitorParams(
            signal_id="SIG-001",
            start="2026-01-01T00:00:00Z",
            end="2026-01-01T23:59:59Z",
            phase_number=2,
        )
        result = await report.execute(params, AsyncMock())

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        row = result.iloc[0]
        assert row["phase_number"] == 2
        assert row["cycles"] == 1
        assert row["green_time"] == 30.0
        assert row["yellow_time"] == 4.0
        assert row["max_out_pct"] == 100.0


class TestPhaseTerminationFromAggregates:
    """Phase termination uses cycle_boundary for termination types."""

    @pytest.mark.asyncio
    @patch("tsigma.reports.sdk.cycles.db_facade")
    async def test_uses_cycle_boundary(self, mock_cycles_facade):
        from tsigma.reports.phase_termination import PhaseTerminationReport

        report = PhaseTerminationReport()

        boundaries = pd.concat([
            _boundary_df(termination_type=["gap_out"]),
            _boundary_df(
                termination_type=["max_out"],
                green_start=[datetime(2026, 1, 1, 12, 1, 0, tzinfo=timezone.utc)],
            ),
            _boundary_df(
                termination_type=["force_off"],
                green_start=[datetime(2026, 1, 1, 12, 2, 0, tzinfo=timezone.utc)],
            ),
        ], ignore_index=True)

        mock_cycles_facade.get_dataframe = AsyncMock(return_value=boundaries)

        params = PhaseTerminationParams(
            signal_id="SIG-001",
            start="2026-01-01T00:00:00Z",
            end="2026-01-01T23:59:59Z",
            phase_number=2,
        )
        result = await report.execute(params, AsyncMock())

        assert isinstance(result, pd.DataFrame)
        assert len(result) >= 1
        # All three boundaries fall in the 12:00 bin
        row = result.iloc[0]
        assert row["phase_number"] == 2
        assert row["gap_out_count"] == 1
        assert row["max_out_count"] == 1
        assert row["force_off_count"] == 1
        assert row["total_cycles"] == 3


class TestArrivalsOnGreenFromAggregates:
    """Arrivals on green uses cycle_detector_arrival for phase_state."""

    @pytest.mark.asyncio
    @patch("tsigma.reports.sdk.cycles.db_facade")
    async def test_uses_cycle_detector_arrival(self, mock_cycles_facade):
        from tsigma.reports.arrivals_on_green import ArrivalsOnGreenReport

        report = ArrivalsOnGreenReport()

        arrivals = pd.concat([
            _arrival_df(phase_state=["green"]),
            _arrival_df(phase_state=["green"]),
            _arrival_df(phase_state=["red"]),
        ], ignore_index=True)

        mock_cycles_facade.get_dataframe = AsyncMock(return_value=arrivals)

        params = ArrivalsOnGreenParams(
            signal_id="SIG-001",
            start="2026-01-01T00:00:00Z",
            end="2026-01-01T23:59:59Z",
            phase_number=2,
        )
        result = await report.execute(params, AsyncMock())

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        row = result.iloc[0]
        assert row["phase_number"] == 2
        assert row["total_arrivals"] == 3
        assert row["arrivals_on_green"] == 2
        assert row["aog_percentage"] == 66.7
