"""
Tests for scheduler job plugins and the JobRegistry.

Covers registry presence, watchdog silent-signal / stuck-detector detection,
and callable checks for aggregate, compress, export, and refresh jobs.
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tsigma.scheduler.jobs.watchdog import STUCK_DETECTOR_THRESHOLD, watchdog

# Import the registry first, then trigger job auto-registration via the
# jobs package __init__ (which glob-imports all job modules).
from tsigma.scheduler.registry import JobRegistry

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _mock_session() -> AsyncMock:
    """Return an AsyncMock that behaves like an AsyncSession."""
    session = AsyncMock()
    session.execute = AsyncMock()
    session.commit = AsyncMock()
    session.rollback = AsyncMock()
    return session


def _row(**kwargs):
    """Create a simple namespace object mimicking a SQLAlchemy Row."""
    return MagicMock(**kwargs)


# ---------------------------------------------------------------------------
# Registry / Service tests
# ---------------------------------------------------------------------------

class TestJobRegistry:

    def test_job_registry_has_watchdog(self):
        """'watchdog' is present in the registry."""
        reg = JobRegistry.get("watchdog")
        assert reg is not None
        assert callable(reg["func"])

    def test_job_registry_list(self):
        """list_all returns a dict containing all known jobs."""
        all_jobs = JobRegistry.list_all()
        assert isinstance(all_jobs, dict)
        # Minimum expected job names
        expected = {
            "watchdog",
            "compress_chunks",
            "export_cold",
            "refresh_views",
            "extract_signal_plans",
        }
        assert expected.issubset(set(all_jobs.keys())), (
            f"Missing jobs: {expected - set(all_jobs.keys())}"
        )


# ---------------------------------------------------------------------------
# Watchdog tests
# ---------------------------------------------------------------------------

class TestWatchdog:

    @pytest.mark.asyncio
    @patch("tsigma.scheduler.jobs.watchdog.notify", new_callable=AsyncMock)
    async def test_watchdog_no_silent_signals(self, mock_notify):
        """No signals older than 24h  ->  no notification sent."""
        session = _mock_session()

        # _check_silent_signals query returns empty (no silent signals)
        # _check_stuck_detectors query returns empty (no stuck detectors)
        silent_result = MagicMock()
        silent_result.all.return_value = []

        stuck_result = MagicMock()
        stuck_result.all.return_value = []

        session.execute = AsyncMock(side_effect=[silent_result, stuck_result])

        await watchdog(session)

        mock_notify.assert_not_called()

    @pytest.mark.asyncio
    @patch("tsigma.scheduler.jobs.watchdog.notify", new_callable=AsyncMock)
    async def test_watchdog_detects_silent(self, mock_notify):
        """Signal with last event >24h ago  ->  triggers notification."""
        session = _mock_session()

        silent_row = _row(signal_id="SIG-001", last_event="2025-01-01 00:00:00")
        silent_result = MagicMock()
        silent_result.all.return_value = [silent_row]

        stuck_result = MagicMock()
        stuck_result.all.return_value = []

        session.execute = AsyncMock(side_effect=[silent_result, stuck_result])

        await watchdog(session)

        # notify should have been called once for the silent signal
        assert mock_notify.call_count >= 1
        first_call = mock_notify.call_args_list[0]
        assert "Silent" in first_call.kwargs.get("subject", first_call[1].get("subject", ""))

    @pytest.mark.asyncio
    @patch("tsigma.scheduler.jobs.watchdog.notify", new_callable=AsyncMock)
    async def test_watchdog_detects_stuck_detector(self, mock_notify):
        """Detector with >3600 ON events/hour  ->  triggers notification."""
        session = _mock_session()

        # Silent signals check -- no results
        silent_result = MagicMock()
        silent_result.all.return_value = []

        # Stuck detectors check -- one stuck detector
        stuck_row = _row(
            signal_id="SIG-002",
            detector_channel=3,
            on_count=STUCK_DETECTOR_THRESHOLD + 500,
        )
        stuck_result = MagicMock()
        stuck_result.all.return_value = [stuck_row]

        session.execute = AsyncMock(side_effect=[silent_result, stuck_result])

        await watchdog(session)

        assert mock_notify.call_count >= 1
        # Find the stuck-detector notification
        stuck_call = [
            c for c in mock_notify.call_args_list
            if "Stuck" in c.kwargs.get("subject", c[1].get("subject", ""))
        ]
        assert len(stuck_call) == 1


# ---------------------------------------------------------------------------
# Aggregate job tests
# ---------------------------------------------------------------------------

class TestAggregateJob:

    def test_aggregate_job_registered(self):
        """The aggregate jobs are registered in JobRegistry."""
        # Check at least one aggregate job
        reg = JobRegistry.get("agg_detector_volume")
        assert reg is not None
        assert callable(reg["func"])
        assert reg["trigger"] == "cron"

    def test_all_aggregate_jobs_registered(self):
        """All seven aggregate jobs are registered."""
        expected_agg_jobs = [
            "agg_detector_volume",
            "agg_detector_occupancy",
            "agg_split_failure",
            "agg_approach_delay",
            "agg_arrival_on_red",
            "agg_coordination_quality",
            "agg_phase_termination",
        ]
        all_jobs = JobRegistry.list_all()
        for name in expected_agg_jobs:
            assert name in all_jobs, f"Aggregate job {name!r} not registered"
            assert all_jobs[name]["trigger"] == "cron"

    @pytest.mark.asyncio
    @patch("tsigma.scheduler.jobs.aggregate.db_facade")
    @patch("tsigma.scheduler.jobs.aggregate.settings")
    async def test_aggregate_runs_queries(self, mock_settings, mock_facade):
        """Aggregate job executes delete + insert SQL via session.execute."""
        mock_settings.aggregation_enabled = True
        mock_settings.aggregation_lookback_hours = 2
        mock_facade.has_timescaledb = AsyncMock(return_value=False)
        mock_facade.db_type = "postgresql"
        mock_facade.time_bucket.return_value = "time_bucket('1 hour', event_time)"
        mock_facade.lookback_predicate.return_value = (
            "event_time >= NOW() - INTERVAL '2 hours'"
        )
        mock_facade.delete_window_sql.return_value = (
            "DELETE FROM detector_volume_hourly "
            "WHERE hour_start >= NOW() - INTERVAL '2 hours'"
        )

        # Reset module-level flags so _should_skip actually calls has_timescaledb
        import tsigma.scheduler.jobs.aggregate as agg_mod
        agg_mod._timescaledb_checked = False
        agg_mod._timescaledb_active = False

        session = _mock_session()
        from tsigma.scheduler.jobs.aggregate import agg_detector_volume
        await agg_detector_volume(session)

        # Should have called execute twice: once for DELETE, once for INSERT
        assert session.execute.call_count == 2

    @pytest.mark.asyncio
    @patch("tsigma.scheduler.jobs.aggregate.db_facade")
    @patch("tsigma.scheduler.jobs.aggregate.settings")
    async def test_aggregate_skips_when_timescaledb_active(
        self, mock_settings, mock_facade
    ):
        """Aggregate jobs skip when TimescaleDB continuous aggregates are active."""
        mock_settings.aggregation_enabled = True
        mock_facade.has_timescaledb = AsyncMock(return_value=True)

        import tsigma.scheduler.jobs.aggregate as agg_mod
        agg_mod._timescaledb_checked = False
        agg_mod._timescaledb_active = False

        session = _mock_session()
        from tsigma.scheduler.jobs.aggregate import agg_detector_volume
        await agg_detector_volume(session)

        # Should NOT have called execute (skipped entirely)
        session.execute.assert_not_called()

    @pytest.mark.asyncio
    @patch("tsigma.scheduler.jobs.aggregate.db_facade")
    @patch("tsigma.scheduler.jobs.aggregate.settings")
    async def test_aggregate_skips_when_disabled(self, mock_settings, mock_facade):
        """Aggregate job returns early when aggregation_enabled is False."""
        mock_settings.aggregation_enabled = False

        import tsigma.scheduler.jobs.aggregate as agg_mod
        agg_mod._timescaledb_checked = False
        agg_mod._timescaledb_active = False

        session = _mock_session()
        from tsigma.scheduler.jobs.aggregate import agg_detector_volume
        await agg_detector_volume(session)

        session.execute.assert_not_called()

    @pytest.mark.asyncio
    @patch("tsigma.scheduler.jobs.aggregate.db_facade")
    @patch("tsigma.scheduler.jobs.aggregate.settings")
    async def test_aggregate_non_postgresql_uses_case(self, mock_settings, mock_facade):
        """Non-PostgreSQL aggregate SQL uses SUM(CASE ...) instead of FILTER."""
        mock_settings.aggregation_enabled = True
        mock_settings.aggregation_lookback_hours = 2
        mock_facade.has_timescaledb = AsyncMock(return_value=False)
        mock_facade.db_type = "mssql"
        mock_facade.time_bucket.return_value = "DATEADD(hour, DATEDIFF(hour, 0, event_time), 0)"
        mock_facade.lookback_predicate.return_value = (
            "event_time >= DATEADD(hour, -2, GETUTCDATE())"
        )
        mock_facade.delete_window_sql.return_value = (
            "DELETE FROM detector_volume_hourly "
            "WHERE hour_start >= DATEADD(hour, -2, GETUTCDATE())"
        )

        import tsigma.scheduler.jobs.aggregate as agg_mod
        agg_mod._timescaledb_checked = False
        agg_mod._timescaledb_active = False

        session = _mock_session()
        from tsigma.scheduler.jobs.aggregate import agg_detector_volume
        await agg_detector_volume(session)

        # Verify the INSERT SQL used CASE WHEN instead of FILTER
        insert_call = session.execute.call_args_list[1]
        sql_text = str(insert_call[0][0].text)
        assert "CASE WHEN" in sql_text
        assert "FILTER" not in sql_text

    @pytest.mark.asyncio
    @patch("tsigma.scheduler.jobs.aggregate.db_facade")
    @patch("tsigma.scheduler.jobs.aggregate.settings")
    async def test_aggregate_split_failure_runs(self, mock_settings, mock_facade):
        """Split failure aggregate job completes with mocked session."""
        mock_settings.aggregation_enabled = True
        mock_settings.aggregation_lookback_hours = 2
        mock_facade.has_timescaledb = AsyncMock(return_value=False)
        mock_facade.db_type = "postgresql"
        mock_facade.time_bucket.return_value = "time_bucket('1 hour', event_time)"
        mock_facade.lookback_predicate.return_value = (
            "event_time >= NOW() - INTERVAL '2 hours'"
        )
        mock_facade.delete_window_sql.return_value = (
            "DELETE FROM split_failure_hourly "
            "WHERE hour_start >= NOW() - INTERVAL '2 hours'"
        )

        import tsigma.scheduler.jobs.aggregate as agg_mod
        agg_mod._timescaledb_checked = False
        agg_mod._timescaledb_active = False

        session = _mock_session()
        from tsigma.scheduler.jobs.aggregate import agg_split_failure
        await agg_split_failure(session)

        assert session.execute.call_count == 2

    @pytest.mark.asyncio
    @patch("tsigma.scheduler.jobs.aggregate.db_facade")
    @patch("tsigma.scheduler.jobs.aggregate.settings")
    async def test_aggregate_phase_termination_runs(self, mock_settings, mock_facade):
        """Phase termination aggregate job completes with mocked session."""
        mock_settings.aggregation_enabled = True
        mock_settings.aggregation_lookback_hours = 2
        mock_facade.has_timescaledb = AsyncMock(return_value=False)
        mock_facade.db_type = "postgresql"
        mock_facade.time_bucket.return_value = "time_bucket('1 hour', event_time)"
        mock_facade.lookback_predicate.return_value = (
            "event_time >= NOW() - INTERVAL '2 hours'"
        )
        mock_facade.delete_window_sql.return_value = (
            "DELETE FROM phase_termination_hourly "
            "WHERE hour_start >= NOW() - INTERVAL '2 hours'"
        )

        import tsigma.scheduler.jobs.aggregate as agg_mod
        agg_mod._timescaledb_checked = False
        agg_mod._timescaledb_active = False

        session = _mock_session()
        from tsigma.scheduler.jobs.aggregate import agg_phase_termination
        await agg_phase_termination(session)

        assert session.execute.call_count == 2

    @pytest.mark.asyncio
    @patch("tsigma.scheduler.jobs.aggregate.db_facade")
    @patch("tsigma.scheduler.jobs.aggregate.settings")
    async def test_aggregate_coordination_quality_runs(self, mock_settings, mock_facade):
        """Coordination quality aggregate job completes (no db_type branch)."""
        mock_settings.aggregation_enabled = True
        mock_settings.aggregation_lookback_hours = 2
        mock_facade.has_timescaledb = AsyncMock(return_value=False)
        mock_facade.db_type = "postgresql"
        mock_facade.time_bucket.return_value = "time_bucket('1 hour', event_time)"
        mock_facade.lookback_predicate.return_value = (
            "event_time >= NOW() - INTERVAL '2 hours'"
        )
        mock_facade.delete_window_sql.return_value = (
            "DELETE FROM coordination_quality_hourly "
            "WHERE hour_start >= NOW() - INTERVAL '2 hours'"
        )

        import tsigma.scheduler.jobs.aggregate as agg_mod
        agg_mod._timescaledb_checked = False
        agg_mod._timescaledb_active = False

        session = _mock_session()
        from tsigma.scheduler.jobs.aggregate import agg_coordination_quality
        await agg_coordination_quality(session)

        assert session.execute.call_count == 2


# ---------------------------------------------------------------------------
# Signal plan job tests
# ---------------------------------------------------------------------------

class TestSignalPlanJob:

    def test_signal_plan_registered(self):
        """extract_signal_plans is registered in JobRegistry."""
        reg = JobRegistry.get("extract_signal_plans")
        assert reg is not None
        assert callable(reg["func"])
        assert reg["trigger"] == "cron"

    @pytest.mark.asyncio
    async def test_signal_plan_job_runs_no_events(self):
        """Signal plan job returns early when no plan events found."""
        session = _mock_session()

        # _fetch_plan_events returns empty list
        plan_events_result = MagicMock()
        plan_events_result.scalars.return_value.all.return_value = []
        session.execute = AsyncMock(return_value=plan_events_result)

        from tsigma.scheduler.jobs.signal_plan import extract_signal_plans
        with patch("tsigma.scheduler.jobs.signal_plan.settings") as mock_settings:
            mock_settings.aggregation_lookback_hours = 2
            await extract_signal_plans(session)

        # Only one execute call (the plan events query); no further queries
        assert session.execute.call_count == 1

    @pytest.mark.asyncio
    async def test_signal_plan_job_processes_events(self):
        """Signal plan job processes plan-change events and adds new rows."""
        session = _mock_session()

        # Create mock events
        event1 = MagicMock()
        event1.signal_id = "SIG-001"
        event1.event_time = datetime(2025, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
        event1.event_code = 131  # Plan change
        event1.event_param = 5   # Plan number 5

        event2 = MagicMock()
        event2.signal_id = "SIG-001"
        event2.event_time = datetime(2025, 6, 1, 12, 5, 0, tzinfo=timezone.utc)
        event2.event_code = 132  # Cycle length change
        event2.event_param = 90  # 90 seconds

        # _fetch_plan_events returns events
        plan_events_result = MagicMock()
        plan_events_result.scalars.return_value.all.return_value = [event1, event2]

        # _fetch_open_plans returns no open plans
        open_plans_result = MagicMock()
        open_plans_result.scalars.return_value.all.return_value = []

        # _fetch_watermarks returns no watermarks
        watermarks_result = MagicMock()
        watermarks_result.all.return_value = []

        session.execute = AsyncMock(
            side_effect=[plan_events_result, open_plans_result, watermarks_result]
        )

        from tsigma.scheduler.jobs.signal_plan import extract_signal_plans
        with patch("tsigma.scheduler.jobs.signal_plan.settings") as mock_settings:
            mock_settings.aggregation_lookback_hours = 2
            await extract_signal_plans(session)

        # session.add should have been called for the new plan row
        session.add.assert_called_once()
        added_plan = session.add.call_args[0][0]
        assert added_plan.signal_id == "SIG-001"
        assert added_plan.plan_number == 5
        assert added_plan.cycle_length == 90

    @pytest.mark.asyncio
    async def test_signal_plan_skips_events_before_watermark(self):
        """Events at or before watermark are skipped."""
        session = _mock_session()

        wm_time = datetime(2025, 6, 1, 12, 0, 0, tzinfo=timezone.utc)

        # Event before watermark — should be skipped
        event_old = MagicMock()
        event_old.signal_id = "SIG-001"
        event_old.event_time = wm_time
        event_old.event_code = 131
        event_old.event_param = 3

        # Event after watermark — should be processed
        event_new = MagicMock()
        event_new.signal_id = "SIG-001"
        event_new.event_time = wm_time + timedelta(seconds=10)
        event_new.event_code = 131
        event_new.event_param = 7

        plan_events_result = MagicMock()
        plan_events_result.scalars.return_value.all.return_value = [event_old, event_new]

        open_plans_result = MagicMock()
        open_plans_result.scalars.return_value.all.return_value = []

        watermarks_result = MagicMock()
        watermarks_result.all.return_value = [("SIG-001", wm_time)]

        session.execute = AsyncMock(
            side_effect=[plan_events_result, open_plans_result, watermarks_result]
        )

        from tsigma.scheduler.jobs.signal_plan import extract_signal_plans
        with patch("tsigma.scheduler.jobs.signal_plan.settings") as mock_settings:
            mock_settings.aggregation_lookback_hours = 2
            await extract_signal_plans(session)

        # Only one plan added (the event after watermark)
        session.add.assert_called_once()
        added_plan = session.add.call_args[0][0]
        assert added_plan.plan_number == 7

    @pytest.mark.asyncio
    async def test_signal_plan_closes_previous_plan(self):
        """New 131 event closes existing open plan."""
        session = _mock_session()

        event1 = MagicMock()
        event1.signal_id = "SIG-001"
        event1.event_time = datetime(2025, 6, 1, 14, 0, 0, tzinfo=timezone.utc)
        event1.event_code = 131
        event1.event_param = 9

        plan_events_result = MagicMock()
        plan_events_result.scalars.return_value.all.return_value = [event1]

        # Existing open plan
        existing_plan = MagicMock()
        existing_plan.signal_id = "SIG-001"
        existing_plan.effective_to = None

        open_plans_result = MagicMock()
        open_plans_result.scalars.return_value.all.return_value = [existing_plan]

        watermarks_result = MagicMock()
        watermarks_result.all.return_value = []

        session.execute = AsyncMock(
            side_effect=[plan_events_result, open_plans_result, watermarks_result]
        )

        from tsigma.scheduler.jobs.signal_plan import extract_signal_plans
        with patch("tsigma.scheduler.jobs.signal_plan.settings") as mock_settings:
            mock_settings.aggregation_lookback_hours = 2
            await extract_signal_plans(session)

        # Previous plan should have its effective_to set
        assert existing_plan.effective_to == event1.event_time

    @pytest.mark.asyncio
    async def test_signal_plan_applies_offset_event(self):
        """Event 133 (offset) applied to current open plan."""
        session = _mock_session()

        event_plan = MagicMock()
        event_plan.signal_id = "SIG-001"
        event_plan.event_time = datetime(2025, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
        event_plan.event_code = 131
        event_plan.event_param = 5

        event_offset = MagicMock()
        event_offset.signal_id = "SIG-001"
        event_offset.event_time = datetime(2025, 6, 1, 12, 1, 0, tzinfo=timezone.utc)
        event_offset.event_code = 133
        event_offset.event_param = 45

        plan_events_result = MagicMock()
        plan_events_result.scalars.return_value.all.return_value = [event_plan, event_offset]

        open_plans_result = MagicMock()
        open_plans_result.scalars.return_value.all.return_value = []

        watermarks_result = MagicMock()
        watermarks_result.all.return_value = []

        session.execute = AsyncMock(
            side_effect=[plan_events_result, open_plans_result, watermarks_result]
        )

        from tsigma.scheduler.jobs.signal_plan import extract_signal_plans
        with patch("tsigma.scheduler.jobs.signal_plan.settings") as mock_settings:
            mock_settings.aggregation_lookback_hours = 2
            await extract_signal_plans(session)

        added_plan = session.add.call_args[0][0]
        assert added_plan.offset == 45

    @pytest.mark.asyncio
    async def test_signal_plan_applies_split_event(self):
        """Event 134 (split phase 1) applied to current open plan."""
        session = _mock_session()

        event_plan = MagicMock()
        event_plan.signal_id = "SIG-001"
        event_plan.event_time = datetime(2025, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
        event_plan.event_code = 131
        event_plan.event_param = 5

        event_split = MagicMock()
        event_split.signal_id = "SIG-001"
        event_split.event_time = datetime(2025, 6, 1, 12, 1, 0, tzinfo=timezone.utc)
        event_split.event_code = 134  # Split for phase 1
        event_split.event_param = 30

        plan_events_result = MagicMock()
        plan_events_result.scalars.return_value.all.return_value = [event_plan, event_split]

        open_plans_result = MagicMock()
        open_plans_result.scalars.return_value.all.return_value = []

        watermarks_result = MagicMock()
        watermarks_result.all.return_value = []

        session.execute = AsyncMock(
            side_effect=[plan_events_result, open_plans_result, watermarks_result]
        )

        from tsigma.scheduler.jobs.signal_plan import extract_signal_plans
        with patch("tsigma.scheduler.jobs.signal_plan.settings") as mock_settings:
            mock_settings.aggregation_lookback_hours = 2
            await extract_signal_plans(session)

        added_plan = session.add.call_args[0][0]
        assert added_plan.splits == {"1": 30}


# ---------------------------------------------------------------------------
# Compress / Export / Refresh -- registered and callable
# ---------------------------------------------------------------------------

class TestCompressChunks:

    def test_compress_chunks_registered(self):
        """compress_chunks is registered in the registry."""
        reg = JobRegistry.get("compress_chunks")
        assert reg is not None
        assert callable(reg["func"])

    @pytest.mark.asyncio
    async def test_compress_chunks_callable(self):
        """compress_chunks can be called with a mocked session."""
        session = _mock_session()
        func = JobRegistry.get("compress_chunks")["func"]
        # Should not raise (it will return early for non-postgresql)
        with patch("tsigma.scheduler.jobs.compress_chunks.settings") as mock_settings:
            mock_settings.db_type = "sqlite"
            await func(session)

    @pytest.mark.asyncio
    async def test_compress_chunks_postgresql(self):
        """compress_chunks queries for uncompressed chunks on PostgreSQL."""
        session = _mock_session()

        # Simulate finding one chunk to compress
        chunk_row = MagicMock()
        chunk_row.chunk_full_name = "_timescaledb_internal.chunk_42"

        find_result = MagicMock()
        find_result.all.return_value = [chunk_row]

        compress_result = MagicMock()

        session.execute = AsyncMock(side_effect=[find_result, compress_result])

        func = JobRegistry.get("compress_chunks")["func"]
        with patch("tsigma.scheduler.jobs.compress_chunks.settings") as mock_settings:
            mock_settings.db_type = "postgresql"
            mock_settings.storage_warm_after = "7 days"
            await func(session)

        # Should have called execute twice: find chunks + compress one chunk
        assert session.execute.call_count == 2

    @pytest.mark.asyncio
    async def test_compress_chunks_skips_non_postgresql(self):
        """compress_chunks returns early for non-PostgreSQL databases."""
        session = _mock_session()
        func = JobRegistry.get("compress_chunks")["func"]

        for db_type in ("mssql", "oracle", "mysql", "sqlite"):
            session.execute.reset_mock()
            with patch("tsigma.scheduler.jobs.compress_chunks.settings") as mock_settings:
                mock_settings.db_type = db_type
                await func(session)
            session.execute.assert_not_called()

    @pytest.mark.asyncio
    async def test_compress_chunks_no_eligible_chunks(self):
        """compress_chunks does nothing when no uncompressed chunks found."""
        session = _mock_session()

        find_result = MagicMock()
        find_result.all.return_value = []
        session.execute = AsyncMock(return_value=find_result)

        func = JobRegistry.get("compress_chunks")["func"]
        with patch("tsigma.scheduler.jobs.compress_chunks.settings") as mock_settings:
            mock_settings.db_type = "postgresql"
            mock_settings.storage_warm_after = "7 days"
            await func(session)

        # Only one call: the query for eligible chunks
        assert session.execute.call_count == 1

    @pytest.mark.asyncio
    async def test_compress_chunks_handles_per_chunk_error(self, caplog):
        """Individual chunk compression failure is caught and logged."""
        session = _mock_session()

        chunk1 = MagicMock()
        chunk1.chunk_full_name = "_timescaledb_internal.chunk_10"
        chunk2 = MagicMock()
        chunk2.chunk_full_name = "_timescaledb_internal.chunk_11"

        find_result = MagicMock()
        find_result.all.return_value = [chunk1, chunk2]

        call_count = 0

        async def _execute_side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return find_result
            if call_count == 2:
                raise RuntimeError("compress failed")
            return MagicMock()

        session.execute = AsyncMock(side_effect=_execute_side_effect)

        func = JobRegistry.get("compress_chunks")["func"]
        with patch("tsigma.scheduler.jobs.compress_chunks.settings") as mock_settings:
            mock_settings.db_type = "postgresql"
            mock_settings.storage_warm_after = "7 days"
            await func(session)

        # Should have attempted both chunks (3 calls: find + 2 compress attempts)
        assert call_count == 3
        assert "failed to compress" in caplog.text.lower()

    @pytest.mark.asyncio
    async def test_compress_chunks_job_failure_raises(self):
        """Top-level exception in compress_chunks is re-raised."""
        session = _mock_session()
        session.execute = AsyncMock(side_effect=RuntimeError("db down"))

        func = JobRegistry.get("compress_chunks")["func"]
        with patch("tsigma.scheduler.jobs.compress_chunks.settings") as mock_settings:
            mock_settings.db_type = "postgresql"
            mock_settings.storage_warm_after = "7 days"
            with pytest.raises(RuntimeError, match="db down"):
                await func(session)


class TestExportCold:

    def test_export_cold_registered(self):
        """export_cold is registered in the registry."""
        reg = JobRegistry.get("export_cold")
        assert reg is not None
        assert callable(reg["func"])

    @pytest.mark.asyncio
    async def test_export_cold_callable(self):
        """export_cold can be called with a mocked session."""
        session = _mock_session()
        func = JobRegistry.get("export_cold")["func"]
        with patch("tsigma.scheduler.jobs.export_cold.settings") as mock_settings:
            mock_settings.storage_cold_enabled = False
            await func(session)

    @pytest.mark.asyncio
    async def test_export_cold_disabled(self):
        """export_cold returns early when cold storage is disabled."""
        session = _mock_session()
        func = JobRegistry.get("export_cold")["func"]

        with patch("tsigma.scheduler.jobs.export_cold.settings") as mock_settings:
            mock_settings.storage_cold_enabled = False
            await func(session)

        session.execute.assert_not_called()

    @pytest.mark.asyncio
    async def test_export_cold_runs(self):
        """export_cold queries and exports data when cold storage enabled."""
        session = _mock_session()

        # Simulate query returning rows
        row1 = MagicMock()
        row1._mapping = {
            "signal_id": "SIG-001",
            "event_time": datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc),
            "event_code": 82,
            "event_param": 3,
            "device_id": 1,
        }

        query_result = MagicMock()
        query_result.all.return_value = [row1]
        session.execute = AsyncMock(return_value=query_result)

        func = JobRegistry.get("export_cold")["func"]

        with (
            patch("tsigma.scheduler.jobs.export_cold.settings") as mock_settings,
            patch("tsigma.scheduler.jobs.export_cold.pd") as mock_pd,
        ):
            mock_settings.storage_cold_enabled = True
            mock_settings.storage_cold_after = "6 months"
            mock_settings.storage_cold_path = "/tmp/tsigma_cold_test"

            # Mock DataFrame behavior
            mock_df = MagicMock()
            mock_pd.DataFrame.return_value = mock_df
            mock_pd.to_datetime.return_value = MagicMock(
                dt=MagicMock(date=MagicMock(return_value="2024-01-15"))
            )
            mock_df.__setitem__ = MagicMock()
            mock_df.__getitem__ = MagicMock(
                return_value=MagicMock(nunique=MagicMock(return_value=1))
            )
            mock_df.groupby.return_value = [
                (("SIG-001", "2024-01-15"), MagicMock(
                    drop=MagicMock(return_value=MagicMock()),
                    __len__=MagicMock(return_value=1),
                ))
            ]

            await func(session)

        # The session should have been queried
        session.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_export_cold_no_data(self):
        """export_cold does nothing when no rows are eligible for export."""
        session = _mock_session()

        query_result = MagicMock()
        query_result.all.return_value = []
        session.execute = AsyncMock(return_value=query_result)

        func = JobRegistry.get("export_cold")["func"]
        with patch("tsigma.scheduler.jobs.export_cold.settings") as mock_settings:
            mock_settings.storage_cold_enabled = True
            mock_settings.storage_cold_after = "6 months"
            mock_settings.storage_cold_path = "/tmp/tsigma_cold_test"
            await func(session)

        # Only the initial query, no Parquet writes
        assert session.execute.call_count == 1


class TestRefreshViews:

    def test_refresh_views_registered(self):
        """refresh_views is registered in the registry."""
        reg = JobRegistry.get("refresh_views")
        assert reg is not None
        assert callable(reg["func"])

    @pytest.mark.asyncio
    async def test_refresh_views_callable(self):
        """refresh_views can be called with a mocked session."""
        session = _mock_session()
        func = JobRegistry.get("refresh_views")["func"]
        with patch("tsigma.scheduler.jobs.refresh_views.settings") as mock_settings:
            mock_settings.db_type = "sqlite"
            await func(session)

    @pytest.mark.asyncio
    async def test_refresh_views_skips_non_postgresql(self):
        """refresh_views returns early for non-PostgreSQL databases."""
        session = _mock_session()
        func = JobRegistry.get("refresh_views")["func"]
        for db_type in ("mssql", "oracle", "mysql"):
            session.execute.reset_mock()
            with patch("tsigma.scheduler.jobs.refresh_views.settings") as mock_settings:
                mock_settings.db_type = db_type
                await func(session)
            session.execute.assert_not_called()

    @pytest.mark.asyncio
    async def test_refresh_views_refreshes_existing_views(self):
        """refresh_views refreshes materialized views that exist."""
        session = _mock_session()

        # Each view: first execute checks existence (returns 1), second refreshes
        existence_result = MagicMock()
        existence_result.scalar.return_value = 1
        session.execute = AsyncMock(return_value=existence_result)

        func = JobRegistry.get("refresh_views")["func"]
        with patch("tsigma.scheduler.jobs.refresh_views.settings") as mock_settings:
            mock_settings.db_type = "postgresql"
            await func(session)

        # 3 views x 2 calls each (existence check + refresh) = 6
        assert session.execute.call_count == 6

    @pytest.mark.asyncio
    async def test_refresh_views_skips_missing_views(self):
        """refresh_views skips views that do not exist in pg_matviews."""
        session = _mock_session()

        existence_result = MagicMock()
        existence_result.scalar.return_value = None
        session.execute = AsyncMock(return_value=existence_result)

        func = JobRegistry.get("refresh_views")["func"]
        with patch("tsigma.scheduler.jobs.refresh_views.settings") as mock_settings:
            mock_settings.db_type = "postgresql"
            await func(session)

        # 3 existence checks only, no refresh calls
        assert session.execute.call_count == 3

    @pytest.mark.asyncio
    async def test_refresh_views_handles_exception(self):
        """refresh_views logs exception and continues on failure."""
        session = _mock_session()

        existence_result = MagicMock()
        existence_result.scalar.return_value = 1
        # First view: exists, then refresh raises; remaining views should still run
        call_count = 0

        async def side_effect_fn(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 2:
                raise RuntimeError("refresh failed")
            return existence_result

        session.execute = AsyncMock(side_effect=side_effect_fn)

        func = JobRegistry.get("refresh_views")["func"]
        with patch("tsigma.scheduler.jobs.refresh_views.settings") as mock_settings:
            mock_settings.db_type = "postgresql"
            await func(session)

        # Should have continued past the failure
        assert call_count >= 3


class TestAggregateOccupancy:
    """Tests for detector occupancy aggregate job."""

    @pytest.mark.asyncio
    @patch("tsigma.scheduler.jobs.aggregate.db_facade")
    @patch("tsigma.scheduler.jobs.aggregate.settings")
    async def test_occupancy_postgresql(self, mock_settings, mock_facade):
        """Detector occupancy aggregate runs on PostgreSQL."""
        mock_settings.aggregation_enabled = True
        mock_settings.aggregation_lookback_hours = 2
        mock_facade.has_timescaledb = AsyncMock(return_value=False)
        mock_facade.db_type = "postgresql"
        mock_facade.time_bucket.return_value = "time_bucket('1 hour', event_time)"
        mock_facade.lookback_predicate.return_value = (
            "event_time >= NOW() - INTERVAL '2 hours'"
        )
        mock_facade.delete_window_sql.return_value = (
            "DELETE FROM detector_occupancy_hourly "
            "WHERE hour_start >= NOW() - INTERVAL '2 hours'"
        )

        import tsigma.scheduler.jobs.aggregate as agg_mod
        agg_mod._timescaledb_checked = False
        agg_mod._timescaledb_active = False

        session = _mock_session()
        from tsigma.scheduler.jobs.aggregate import agg_detector_occupancy
        await agg_detector_occupancy(session)

        assert session.execute.call_count == 2

    @pytest.mark.asyncio
    @patch("tsigma.scheduler.jobs.aggregate.db_facade")
    @patch("tsigma.scheduler.jobs.aggregate.settings")
    async def test_occupancy_non_postgresql(self, mock_settings, mock_facade):
        """Detector occupancy aggregate uses CASE on non-PostgreSQL."""
        mock_settings.aggregation_enabled = True
        mock_settings.aggregation_lookback_hours = 2
        mock_facade.has_timescaledb = AsyncMock(return_value=False)
        mock_facade.db_type = "mssql"
        mock_facade.time_bucket.return_value = "DATEADD(hour, DATEDIFF(hour, 0, event_time), 0)"
        mock_facade.lookback_predicate.return_value = (
            "event_time >= DATEADD(hour, -2, GETUTCDATE())"
        )
        mock_facade.delete_window_sql.return_value = (
            "DELETE FROM detector_occupancy_hourly "
            "WHERE hour_start >= DATEADD(hour, -2, GETUTCDATE())"
        )

        import tsigma.scheduler.jobs.aggregate as agg_mod
        agg_mod._timescaledb_checked = False
        agg_mod._timescaledb_active = False

        session = _mock_session()
        from tsigma.scheduler.jobs.aggregate import agg_detector_occupancy
        await agg_detector_occupancy(session)

        assert session.execute.call_count == 2
        insert_call = session.execute.call_args_list[1]
        sql_text = str(insert_call[0][0].text)
        assert "CASE WHEN" in sql_text


class TestAggregateApproachDelay:
    """Tests for approach delay aggregate job."""

    @pytest.mark.asyncio
    @patch("tsigma.scheduler.jobs.aggregate.db_facade")
    @patch("tsigma.scheduler.jobs.aggregate.settings")
    async def test_approach_delay_postgresql(self, mock_settings, mock_facade):
        """Approach delay aggregate runs on PostgreSQL."""
        mock_settings.aggregation_enabled = True
        mock_settings.aggregation_lookback_hours = 2
        mock_facade.has_timescaledb = AsyncMock(return_value=False)
        mock_facade.db_type = "postgresql"
        mock_facade.time_bucket.return_value = "time_bucket('1 hour', event_time)"
        mock_facade.lookback_predicate.return_value = (
            "event_time >= NOW() - INTERVAL '2 hours'"
        )
        mock_facade.delete_window_sql.return_value = (
            "DELETE FROM approach_delay_15min "
            "WHERE bin_start >= NOW() - INTERVAL '2 hours'"
        )

        import tsigma.scheduler.jobs.aggregate as agg_mod
        agg_mod._timescaledb_checked = False
        agg_mod._timescaledb_active = False

        session = _mock_session()
        from tsigma.scheduler.jobs.aggregate import agg_approach_delay
        await agg_approach_delay(session)

        assert session.execute.call_count == 2

    @pytest.mark.asyncio
    @patch("tsigma.scheduler.jobs.aggregate.db_facade")
    @patch("tsigma.scheduler.jobs.aggregate.settings")
    async def test_approach_delay_mssql(self, mock_settings, mock_facade):
        """Approach delay uses DATEADD bucketing on MS-SQL."""
        mock_settings.aggregation_enabled = True
        mock_settings.aggregation_lookback_hours = 2
        mock_facade.has_timescaledb = AsyncMock(return_value=False)
        mock_facade.db_type = "mssql"
        mock_facade.time_bucket.return_value = "DATEADD(hour, DATEDIFF(hour, 0, event_time), 0)"
        mock_facade.lookback_predicate.return_value = (
            "event_time >= DATEADD(hour, -2, GETUTCDATE())"
        )
        mock_facade.delete_window_sql.return_value = (
            "DELETE FROM approach_delay_15min "
            "WHERE bin_start >= DATEADD(hour, -2, GETUTCDATE())"
        )

        import tsigma.scheduler.jobs.aggregate as agg_mod
        agg_mod._timescaledb_checked = False
        agg_mod._timescaledb_active = False

        session = _mock_session()
        from tsigma.scheduler.jobs.aggregate import agg_approach_delay
        await agg_approach_delay(session)

        assert session.execute.call_count == 2
        insert_call = session.execute.call_args_list[1]
        sql_text = str(insert_call[0][0].text)
        assert "DATEADD" in sql_text


class TestAggregateArrivalOnRed:
    """Tests for arrival-on-red aggregate job."""

    @pytest.mark.asyncio
    @patch("tsigma.scheduler.jobs.aggregate.db_facade")
    @patch("tsigma.scheduler.jobs.aggregate.settings")
    async def test_arrival_on_red_postgresql(self, mock_settings, mock_facade):
        """Arrival on red aggregate runs on PostgreSQL."""
        mock_settings.aggregation_enabled = True
        mock_settings.aggregation_lookback_hours = 2
        mock_facade.has_timescaledb = AsyncMock(return_value=False)
        mock_facade.db_type = "postgresql"
        mock_facade.time_bucket.return_value = "time_bucket('1 hour', event_time)"
        mock_facade.lookback_predicate.return_value = (
            "event_time >= NOW() - INTERVAL '2 hours'"
        )
        mock_facade.delete_window_sql.return_value = (
            "DELETE FROM arrival_on_red_hourly "
            "WHERE hour_start >= NOW() - INTERVAL '2 hours'"
        )

        import tsigma.scheduler.jobs.aggregate as agg_mod
        agg_mod._timescaledb_checked = False
        agg_mod._timescaledb_active = False

        session = _mock_session()
        from tsigma.scheduler.jobs.aggregate import agg_arrival_on_red
        await agg_arrival_on_red(session)

        assert session.execute.call_count == 2

    @pytest.mark.asyncio
    @patch("tsigma.scheduler.jobs.aggregate.db_facade")
    @patch("tsigma.scheduler.jobs.aggregate.settings")
    async def test_arrival_on_red_non_postgresql(self, mock_settings, mock_facade):
        """Arrival on red uses CASE on non-PostgreSQL."""
        mock_settings.aggregation_enabled = True
        mock_settings.aggregation_lookback_hours = 2
        mock_facade.has_timescaledb = AsyncMock(return_value=False)
        mock_facade.db_type = "mssql"
        mock_facade.time_bucket.return_value = "DATEADD(hour, DATEDIFF(hour, 0, event_time), 0)"
        mock_facade.lookback_predicate.return_value = (
            "event_time >= DATEADD(hour, -2, GETUTCDATE())"
        )
        mock_facade.delete_window_sql.return_value = (
            "DELETE FROM arrival_on_red_hourly "
            "WHERE hour_start >= DATEADD(hour, -2, GETUTCDATE())"
        )

        import tsigma.scheduler.jobs.aggregate as agg_mod
        agg_mod._timescaledb_checked = False
        agg_mod._timescaledb_active = False

        session = _mock_session()
        from tsigma.scheduler.jobs.aggregate import agg_arrival_on_red
        await agg_arrival_on_red(session)

        assert session.execute.call_count == 2
        insert_call = session.execute.call_args_list[1]
        sql_text = str(insert_call[0][0].text)
        assert "CASE WHEN" in sql_text
