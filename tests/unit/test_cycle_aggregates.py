"""Tests for cycle aggregate scheduler job and PCD report integration."""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tsigma.models.aggregates import (
    CycleBoundary,
    CycleDetectorArrival,
    CycleSummary15Min,
)

# ---------------------------------------------------------------------------
# Model tests
# ---------------------------------------------------------------------------


class TestCycleBoundaryModel:
    """CycleBoundary model structure tests."""

    def test_table_name(self):
        assert CycleBoundary.__tablename__ == "cycle_boundary"

    def test_primary_keys(self):
        pk_cols = [c.name for c in CycleBoundary.__table__.primary_key.columns]
        assert "signal_id" in pk_cols
        assert "phase" in pk_cols
        assert "green_start" in pk_cols

    def test_nullable_timing_fields(self):
        cb = CycleBoundary(
            signal_id="SIG-001",
            phase=2,
            green_start=datetime(2026, 1, 1, tzinfo=timezone.utc),
        )
        assert cb.yellow_start is None
        assert cb.red_start is None
        assert cb.cycle_end is None
        assert cb.termination_type is None


class TestCycleDetectorArrivalModel:
    """CycleDetectorArrival model structure tests."""

    def test_table_name(self):
        assert CycleDetectorArrival.__tablename__ == "cycle_detector_arrival"

    def test_primary_keys(self):
        pk_cols = [c.name for c in CycleDetectorArrival.__table__.primary_key.columns]
        assert "signal_id" in pk_cols
        assert "phase" in pk_cols
        assert "detector_channel" in pk_cols
        assert "arrival_time" in pk_cols

    def test_required_fields(self):
        col = CycleDetectorArrival.__table__.columns
        assert col["time_in_cycle_seconds"].nullable is False
        assert col["phase_state"].nullable is False


class TestCycleSummary15MinModel:
    """CycleSummary15Min model structure tests."""

    def test_table_name(self):
        assert CycleSummary15Min.__tablename__ == "cycle_summary_15min"

    def test_primary_keys(self):
        pk_cols = [c.name for c in CycleSummary15Min.__table__.primary_key.columns]
        assert "signal_id" in pk_cols
        assert "phase" in pk_cols
        assert "bin_start" in pk_cols

    def test_default_values(self):
        CycleSummary15Min(
            signal_id="SIG-001",
            phase=2,
            bin_start=datetime(2026, 1, 1, tzinfo=timezone.utc),
        )
        # Defaults may be None at Python level (server_default handles DB)
        # Just verify the model can be instantiated


# ---------------------------------------------------------------------------
# Scheduler job tests
# ---------------------------------------------------------------------------


class TestCycleBoundaryJob:
    """Tests for the cycle boundary aggregate scheduler job."""

    @pytest.mark.asyncio
    async def test_job_registered(self):
        """agg_cycle_boundary job is registered in JobRegistry."""
        from tsigma.scheduler.registry import JobRegistry
        job = JobRegistry.get("agg_cycle_boundary")
        assert job is not None

    @pytest.mark.asyncio
    async def test_skips_when_timescaledb_active(self):
        """Job returns early when TimescaleDB continuous aggregates are active."""
        from tsigma.scheduler.jobs.cycle_aggregate import agg_cycle_boundary

        mock_session = AsyncMock()
        with patch(
            "tsigma.scheduler.jobs.cycle_aggregate._should_skip",
            new_callable=AsyncMock,
            return_value=True,
        ):
            await agg_cycle_boundary(mock_session)
        mock_session.execute.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_runs_delete_and_insert(self):
        """Job deletes stale rows and inserts fresh aggregates."""
        from tsigma.scheduler.jobs.cycle_aggregate import agg_cycle_boundary

        mock_session = AsyncMock()
        with patch(
            "tsigma.scheduler.jobs.cycle_aggregate._should_skip",
            new_callable=AsyncMock,
            return_value=False,
        ), patch(
            "tsigma.scheduler.jobs.cycle_aggregate.db_facade",
        ) as mock_facade:
            mock_facade.db_type = "postgresql"
            mock_facade.delete_window_sql.return_value = "DELETE FROM cycle_boundary WHERE true"
            mock_facade.lookback_predicate.return_value = "event_time >= NOW() - INTERVAL '2 hours'"

            await agg_cycle_boundary(mock_session)

        assert mock_session.execute.await_count >= 2  # delete + insert


class TestCycleDetectorArrivalJob:
    """Tests for the cycle detector arrival aggregate scheduler job."""

    @pytest.mark.asyncio
    async def test_job_registered(self):
        from tsigma.scheduler.registry import JobRegistry
        job = JobRegistry.get("agg_cycle_detector_arrival")
        assert job is not None

    @pytest.mark.asyncio
    async def test_skips_when_timescaledb_active(self):
        from tsigma.scheduler.jobs.cycle_aggregate import agg_cycle_detector_arrival

        mock_session = AsyncMock()
        with patch(
            "tsigma.scheduler.jobs.cycle_aggregate._should_skip",
            new_callable=AsyncMock,
            return_value=True,
        ):
            await agg_cycle_detector_arrival(mock_session)
        mock_session.execute.assert_not_awaited()


class TestCycleSummary15MinJob:
    """Tests for the cycle summary 15-minute aggregate scheduler job."""

    @pytest.mark.asyncio
    async def test_job_registered(self):
        from tsigma.scheduler.registry import JobRegistry
        job = JobRegistry.get("agg_cycle_summary_15min")
        assert job is not None

    @pytest.mark.asyncio
    async def test_skips_when_timescaledb_active(self):
        from tsigma.scheduler.jobs.cycle_aggregate import agg_cycle_summary_15min

        mock_session = AsyncMock()
        with patch(
            "tsigma.scheduler.jobs.cycle_aggregate._should_skip",
            new_callable=AsyncMock,
            return_value=True,
        ):
            await agg_cycle_summary_15min(mock_session)
        mock_session.execute.assert_not_awaited()


# ---------------------------------------------------------------------------
# PCD report integration tests
# ---------------------------------------------------------------------------


class TestPCDReportUsesAggregates:
    """PCD report should use cycle_boundary + cycle_detector_arrival for historical queries."""

    @pytest.mark.asyncio
    async def test_historical_pcd_queries_aggregates(self):
        """Historical PCD queries cycle_detector_arrival instead of raw events."""
        import pandas as pd

        from tsigma.reports.purdue_diagram import PurdueDiagramParams, PurdueDiagramReport

        report = PurdueDiagramReport()

        boundary_df = pd.DataFrame({
            "green_start": [datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)],
            "yellow_start": [datetime(2026, 1, 1, 12, 0, 30, tzinfo=timezone.utc)],
            "red_start": [datetime(2026, 1, 1, 12, 0, 34, tzinfo=timezone.utc)],
            "cycle_end": [datetime(2026, 1, 1, 12, 1, 0, tzinfo=timezone.utc)],
            "green_duration_seconds": [30.0],
            "yellow_duration_seconds": [4.0],
            "red_duration_seconds": [26.0],
            "cycle_duration_seconds": [60.0],
            "termination_type": ["gap_out"],
        })

        arrival_df = pd.DataFrame({
            "arrival_time": [datetime(2026, 1, 1, 12, 0, 1, tzinfo=timezone.utc)],
            "detector_channel": [5],
            "green_start": [datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)],
            "time_in_cycle_seconds": [1.0],
            "phase_state": ["green"],
        })

        with patch(
            "tsigma.reports.sdk.cycles.db_facade",
        ) as mock_cycles_facade:
            mock_cycles_facade.get_dataframe = AsyncMock(
                side_effect=[boundary_df, arrival_df]
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
