"""
Unit tests for CollectorService.

Tests orchestration of polling ingestion methods with
JobRegistry registration and semaphore-bounded concurrency.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tsigma.collection.registry import (
    ExecutionMode,
    IngestionMethodRegistry,
    PollingIngestionMethod,
)
from tsigma.collection.service import CollectorService
from tsigma.config import Settings
from tsigma.scheduler.registry import JobRegistry


def _make_settings(**overrides) -> Settings:
    """Create Settings with test defaults."""
    defaults = {
        "collector_max_concurrent": 5,
        "collector_poll_interval": 60,
    }
    defaults.update(overrides)
    return Settings(**defaults)


def _mock_session_factory():
    """Create a mock async session factory that supports 'async with'."""
    mock_session = AsyncMock()
    mock_session.execute = AsyncMock(return_value=MagicMock())

    mock_ctx = AsyncMock()
    mock_ctx.__aenter__ = AsyncMock(return_value=mock_session)
    mock_ctx.__aexit__ = AsyncMock(return_value=False)

    def factory():
        return mock_ctx

    return factory, mock_session


class TestCollectorServiceInit:
    """Tests for CollectorService constructor."""

    def test_stores_session_factory(self):
        """Test session factory is stored."""
        sf = AsyncMock()
        svc = CollectorService(sf, _make_settings())
        assert svc._session_factory is sf

    def test_stores_settings(self):
        """Test settings are stored."""
        s = _make_settings(collector_max_concurrent=10)
        svc = CollectorService(AsyncMock(), s)
        assert svc._settings is s

    def test_creates_semaphore(self):
        """Test semaphore is created from settings."""
        svc = CollectorService(AsyncMock(), _make_settings(collector_max_concurrent=25))
        assert isinstance(svc._semaphore, asyncio.Semaphore)
        # Semaphore initial value matches setting
        assert svc._semaphore._value == 25

    def test_empty_instances(self):
        """Test polling instances start empty."""
        svc = CollectorService(AsyncMock(), _make_settings())
        assert svc._polling_instances == {}


class TestCollectorServiceStart:
    """Tests for CollectorService.start()."""

    @pytest.mark.asyncio
    async def test_instantiates_polling_methods(self):
        """Test start() creates instances of registered polling methods."""
        mock_poller_cls = MagicMock(spec=PollingIngestionMethod)
        mock_poller_cls.execution_mode = ExecutionMode.POLLING
        mock_instance = AsyncMock(spec=PollingIngestionMethod)
        mock_poller_cls.return_value = mock_instance

        with patch.object(
            IngestionMethodRegistry, "get_polling_methods",
            return_value={"test_poll": mock_poller_cls},
        ):
            svc = CollectorService(AsyncMock(), _make_settings())
            await svc.start()
            try:
                assert "test_poll" in svc._polling_instances
                assert svc._polling_instances["test_poll"] is mock_instance
            finally:
                await svc.stop()

    @pytest.mark.asyncio
    async def test_registers_with_job_registry(self):
        """Test start() registers poll cycles with JobRegistry."""
        mock_cls = MagicMock(spec=PollingIngestionMethod)
        mock_cls.execution_mode = ExecutionMode.POLLING
        mock_cls.return_value = AsyncMock(spec=PollingIngestionMethod)

        with patch.object(
            IngestionMethodRegistry, "get_polling_methods",
            return_value={"poll_a": mock_cls, "poll_b": mock_cls},
        ):
            svc = CollectorService(AsyncMock(), _make_settings(collector_poll_interval=120))
            await svc.start()
            try:
                jobs = JobRegistry.list_all()
                assert "poll_cycle_poll_a" in jobs
                assert "poll_cycle_poll_b" in jobs
                assert jobs["poll_cycle_poll_a"]["trigger"] == "interval"
                assert jobs["poll_cycle_poll_a"]["trigger_kwargs"]["seconds"] == 120
                assert jobs["poll_cycle_poll_a"]["needs_session"] is False
            finally:
                await svc.stop()


class TestCollectorServiceStop:
    """Tests for CollectorService.stop()."""

    @pytest.mark.asyncio
    async def test_unregisters_from_job_registry(self):
        """Test stop() unregisters poll cycle jobs from JobRegistry."""
        mock_cls = MagicMock(spec=PollingIngestionMethod)
        mock_cls.execution_mode = ExecutionMode.POLLING
        mock_cls.return_value = AsyncMock(spec=PollingIngestionMethod)

        with patch.object(
            IngestionMethodRegistry, "get_polling_methods",
            return_value={"test_poll": mock_cls},
        ):
            svc = CollectorService(AsyncMock(), _make_settings())
            await svc.start()
            assert "poll_cycle_test_poll" in JobRegistry.list_all()
            await svc.stop()
            assert "poll_cycle_test_poll" not in JobRegistry.list_all()

    @pytest.mark.asyncio
    async def test_stop_idempotent(self):
        """Test stop() is safe to call multiple times."""
        with patch.object(
            IngestionMethodRegistry, "get_polling_methods", return_value={},
        ):
            svc = CollectorService(AsyncMock(), _make_settings())
            await svc.start()
            await svc.stop()
            await svc.stop()  # Should not raise


class TestPollCycle:
    """Tests for _run_poll_cycle (fan-out to signals)."""

    @pytest.mark.asyncio
    async def test_queries_enabled_signals(self):
        """Test poll cycle queries signals with matching method."""
        sf, mock_session = _mock_session_factory()

        mock_row = MagicMock()
        mock_row.signal_id = "SIG-001"
        mock_row.ip_address = "10.0.0.1"
        mock_row.signal_metadata = {
            "collection": {"method": "test_poll", "protocol": "ftp"}
        }
        mock_result = MagicMock()
        mock_result.all.return_value = [mock_row]
        mock_session.execute.return_value = mock_result

        mock_method = AsyncMock(spec=PollingIngestionMethod)

        svc = CollectorService(sf, _make_settings())
        svc._polling_instances["test_poll"] = mock_method
        svc._session_factory = sf

        await svc._run_poll_cycle("test_poll")
        mock_method.poll_once.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_injects_host_from_ip_address(self):
        """Test poll cycle sets host from signal ip_address."""
        sf, mock_session = _mock_session_factory()

        mock_row = MagicMock()
        mock_row.signal_id = "SIG-001"
        mock_row.ip_address = "192.168.1.50"
        mock_row.signal_metadata = {
            "collection": {"method": "test_poll", "protocol": "ftp"}
        }
        mock_result = MagicMock()
        mock_result.all.return_value = [mock_row]
        mock_session.execute.return_value = mock_result

        mock_method = AsyncMock(spec=PollingIngestionMethod)

        svc = CollectorService(sf, _make_settings())
        svc._polling_instances["test_poll"] = mock_method
        svc._session_factory = sf

        await svc._run_poll_cycle("test_poll")

        call_args = mock_method.poll_once.call_args
        config_arg = call_args[0][1]
        assert config_arg["host"] == "192.168.1.50"

    @pytest.mark.asyncio
    async def test_fans_out_to_multiple_signals(self):
        """Test poll cycle processes multiple signals."""
        sf, mock_session = _mock_session_factory()

        rows = []
        for i in range(3):
            row = MagicMock()
            row.signal_id = f"SIG-{i:03d}"
            row.ip_address = f"10.0.0.{i}"
            row.signal_metadata = {
                "collection": {"method": "test_poll", "protocol": "ftp"}
            }
            rows.append(row)

        mock_result = MagicMock()
        mock_result.all.return_value = rows
        mock_session.execute.return_value = mock_result

        mock_method = AsyncMock(spec=PollingIngestionMethod)

        svc = CollectorService(sf, _make_settings())
        svc._polling_instances["test_poll"] = mock_method
        svc._session_factory = sf

        await svc._run_poll_cycle("test_poll")
        assert mock_method.poll_once.await_count == 3

    @pytest.mark.asyncio
    async def test_empty_signal_list(self):
        """Test poll cycle handles no matching signals gracefully."""
        sf, mock_session = _mock_session_factory()

        mock_result = MagicMock()
        mock_result.all.return_value = []
        mock_session.execute.return_value = mock_result

        mock_method = AsyncMock(spec=PollingIngestionMethod)

        svc = CollectorService(sf, _make_settings())
        svc._polling_instances["test_poll"] = mock_method
        svc._session_factory = sf

        await svc._run_poll_cycle("test_poll")
        mock_method.poll_once.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_continues_on_individual_signal_failure(self):
        """Test poll cycle continues when one signal fails."""
        sf, mock_session = _mock_session_factory()

        rows = []
        for i in range(3):
            row = MagicMock()
            row.signal_id = f"SIG-{i:03d}"
            row.ip_address = f"10.0.0.{i}"
            row.signal_metadata = {
                "collection": {"method": "test_poll", "protocol": "ftp"}
            }
            rows.append(row)

        mock_result = MagicMock()
        mock_result.all.return_value = rows
        mock_session.execute.return_value = mock_result

        mock_method = AsyncMock(spec=PollingIngestionMethod)
        mock_method.name = "test_poll"
        mock_method.poll_once.side_effect = [
            None,
            Exception("connection timeout"),
            None,
        ]

        svc = CollectorService(sf, _make_settings())
        svc._polling_instances["test_poll"] = mock_method
        svc._session_factory = sf

        await svc._run_poll_cycle("test_poll")
        assert mock_method.poll_once.await_count == 3

    @pytest.mark.asyncio
    async def test_respects_semaphore_limit(self):
        """Test poll cycle limits concurrent signals via semaphore."""
        sf, mock_session = _mock_session_factory()

        rows = []
        for i in range(10):
            row = MagicMock()
            row.signal_id = f"SIG-{i:03d}"
            row.ip_address = f"10.0.0.{i}"
            row.signal_metadata = {
                "collection": {"method": "test_poll", "protocol": "ftp"}
            }
            rows.append(row)

        mock_result = MagicMock()
        mock_result.all.return_value = rows
        mock_session.execute.return_value = mock_result

        max_concurrent_seen = 0
        current_concurrent = 0
        lock = asyncio.Lock()

        original_semaphore_value = 3

        async def tracked_poll(signal_id, config, session_factory):
            nonlocal max_concurrent_seen, current_concurrent
            async with lock:
                current_concurrent += 1
                if current_concurrent > max_concurrent_seen:
                    max_concurrent_seen = current_concurrent
            await asyncio.sleep(0.01)
            async with lock:
                current_concurrent -= 1

        mock_method = AsyncMock(spec=PollingIngestionMethod)
        mock_method.poll_once = tracked_poll

        svc = CollectorService(
            sf, _make_settings(collector_max_concurrent=original_semaphore_value)
        )
        svc._polling_instances["test_poll"] = mock_method
        svc._session_factory = sf

        await svc._run_poll_cycle("test_poll")
        assert max_concurrent_seen <= original_semaphore_value

    @pytest.mark.asyncio
    async def test_skips_signals_without_collection_config(self):
        """Test poll cycle skips signals with no collection metadata."""
        sf, mock_session = _mock_session_factory()

        row_with = MagicMock()
        row_with.signal_id = "SIG-001"
        row_with.ip_address = "10.0.0.1"
        row_with.signal_metadata = {
            "collection": {"method": "test_poll", "protocol": "ftp"}
        }

        row_without = MagicMock()
        row_without.signal_id = "SIG-002"
        row_without.ip_address = "10.0.0.2"
        row_without.signal_metadata = None

        mock_result = MagicMock()
        mock_result.all.return_value = [row_with, row_without]
        mock_session.execute.return_value = mock_result

        mock_method = AsyncMock(spec=PollingIngestionMethod)

        svc = CollectorService(sf, _make_settings())
        svc._polling_instances["test_poll"] = mock_method
        svc._session_factory = sf

        await svc._run_poll_cycle("test_poll")
        assert mock_method.poll_once.await_count == 1

    @pytest.mark.asyncio
    async def test_skips_signals_with_wrong_method(self):
        """Test poll cycle skips signals configured for a different method."""
        sf, mock_session = _mock_session_factory()

        row = MagicMock()
        row.signal_id = "SIG-001"
        row.ip_address = "10.0.0.1"
        row.signal_metadata = {
            "collection": {"method": "other_method", "protocol": "other"}
        }

        mock_result = MagicMock()
        mock_result.all.return_value = [row]
        mock_session.execute.return_value = mock_result

        mock_method = AsyncMock(spec=PollingIngestionMethod)

        svc = CollectorService(sf, _make_settings())
        svc._polling_instances["test_poll"] = mock_method
        svc._session_factory = sf

        await svc._run_poll_cycle("test_poll")
        mock_method.poll_once.assert_not_awaited()


class TestProcessSignal:
    """Tests for _process_signal (semaphore-bounded worker)."""

    @pytest.mark.asyncio
    async def test_calls_poll_once_with_correct_args(self):
        """Test _process_signal calls poll_once with signal_id, config, and factory."""
        sf = AsyncMock()
        mock_method = AsyncMock(spec=PollingIngestionMethod)

        svc = CollectorService(sf, _make_settings())

        config = {"protocol": "ftp", "host": "10.0.0.1"}
        await svc._process_signal(mock_method, "SIG-001", config)

        mock_method.poll_once.assert_awaited_once_with("SIG-001", config, sf)

    @pytest.mark.asyncio
    async def test_catches_exceptions(self):
        """Test _process_signal catches and logs exceptions."""
        sf = AsyncMock()
        mock_method = AsyncMock(spec=PollingIngestionMethod)
        mock_method.name = "test_poll"
        mock_method.poll_once.side_effect = ConnectionError("refused")

        svc = CollectorService(sf, _make_settings())

        # Should not raise
        await svc._process_signal(mock_method, "SIG-001", {"host": "10.0.0.1"})

    @pytest.mark.asyncio
    async def test_acquires_semaphore(self):
        """Test _process_signal acquires and releases semaphore."""
        sf = AsyncMock()
        mock_method = AsyncMock(spec=PollingIngestionMethod)

        svc = CollectorService(sf, _make_settings(collector_max_concurrent=1))
        assert svc._semaphore._value == 1

        await svc._process_signal(mock_method, "SIG-001", {"host": "10.0.0.1"})
        # Semaphore should be released after call
        assert svc._semaphore._value == 1


class TestGetMethod:
    """Tests for CollectorService.get_method()."""

    def test_returns_running_instance(self):
        """Test get_method returns an existing polling instance."""
        svc = CollectorService(AsyncMock(), _make_settings())
        mock_method = AsyncMock(spec=PollingIngestionMethod)
        svc._polling_instances["test_poll"] = mock_method

        result = svc.get_method("test_poll")
        assert result is mock_method

    def test_raises_for_unknown_method(self):
        """Test get_method raises ValueError for unregistered method."""
        svc = CollectorService(AsyncMock(), _make_settings())
        with pytest.raises(ValueError, match="No polling instance"):
            svc.get_method("nonexistent")


class TestSessionFactoryProperty:
    """Tests for session_factory property."""

    def test_exposes_session_factory(self):
        """Test session_factory property returns the stored factory."""
        sf = AsyncMock()
        svc = CollectorService(sf, _make_settings())
        assert svc.session_factory is sf


class TestCheckSilentSignals:
    """Tests for _check_silent_signals and _handle_silent_signal."""

    @pytest.mark.asyncio
    async def test_check_silent_signals_increments_counter(self):
        """Signal with old last_successful_poll gets counter incremented."""
        from datetime import datetime, timedelta, timezone

        from tsigma.models.checkpoint import PollingCheckpoint

        now = datetime.now(timezone.utc)
        old_poll = now - timedelta(seconds=300)  # well past 1.5x poll interval

        cp = MagicMock(spec=PollingCheckpoint)
        cp.signal_id = "SIG-001"
        cp.method = "test_poll"
        cp.last_successful_poll = old_poll
        cp.consecutive_silent_cycles = 0
        cp.last_event_timestamp = now - timedelta(hours=1)
        cp.updated_at = old_poll

        mock_session = AsyncMock()
        mock_result = MagicMock()
        mock_scalars = MagicMock()
        mock_scalars.__iter__ = MagicMock(return_value=iter([cp]))
        mock_result.scalars.return_value = mock_scalars
        mock_session.execute = AsyncMock(return_value=mock_result)
        mock_session.flush = AsyncMock()

        mock_ctx = AsyncMock()
        mock_ctx.__aenter__ = AsyncMock(return_value=mock_session)
        mock_ctx.__aexit__ = AsyncMock(return_value=False)

        sf = MagicMock(return_value=mock_ctx)

        settings = _make_settings(
            collector_poll_interval=60,
            checkpoint_silent_cycles_threshold=5,
            checkpoint_future_tolerance_seconds=300,
        )
        svc = CollectorService(sf, settings)

        await svc._check_silent_signals("test_poll", ["SIG-001"])

        assert cp.consecutive_silent_cycles == 1

    @pytest.mark.asyncio
    async def test_handle_silent_signal_poisoned(self):
        """Checkpoint in future triggers auto-recovery + CRITICAL notification."""
        from datetime import datetime, timedelta, timezone

        now = datetime.now(timezone.utc)
        future_ts = now + timedelta(hours=2)  # well past tolerance

        cp = MagicMock()
        cp.signal_id = "SIG-POISON"
        cp.method = "http_pull"
        cp.last_event_timestamp = future_ts
        cp.consecutive_silent_cycles = 5
        cp.last_successful_poll = now - timedelta(minutes=10)
        cp.updated_at = now

        settings = _make_settings(
            collector_poll_interval=60,
            checkpoint_silent_cycles_threshold=3,
            checkpoint_future_tolerance_seconds=300,
        )
        svc = CollectorService(AsyncMock(), settings)

        with patch(
            "tsigma.collection.service.notify", new_callable=AsyncMock
        ) as mock_notify:
            await svc._handle_silent_signal(cp, now)

        # Checkpoint rolled back to now
        assert cp.last_event_timestamp == now
        assert cp.consecutive_silent_cycles == 0

        mock_notify.assert_awaited_once()
        call_kwargs = mock_notify.call_args
        subject = call_kwargs[1].get("subject") or call_kwargs.kwargs.get("subject", "")
        assert "Poisoned checkpoint" in subject
        from tsigma.notifications.registry import CRITICAL
        severity = call_kwargs[1].get("severity") or call_kwargs.kwargs.get("severity")
        assert severity == CRITICAL

    @pytest.mark.asyncio
    async def test_handle_silent_signal_not_poisoned(self):
        """Checkpoint not in future triggers WARNING notification."""
        from datetime import datetime, timedelta, timezone

        now = datetime.now(timezone.utc)
        past_ts = now - timedelta(hours=1)

        cp = MagicMock()
        cp.signal_id = "SIG-SILENT"
        cp.method = "http_pull"
        cp.last_event_timestamp = past_ts
        cp.consecutive_silent_cycles = 5
        cp.last_successful_poll = now - timedelta(minutes=10)
        cp.updated_at = now

        settings = _make_settings(
            collector_poll_interval=60,
            checkpoint_silent_cycles_threshold=3,
            checkpoint_future_tolerance_seconds=300,
        )
        svc = CollectorService(AsyncMock(), settings)

        with patch(
            "tsigma.collection.service.notify", new_callable=AsyncMock
        ) as mock_notify:
            await svc._handle_silent_signal(cp, now)

        # Checkpoint NOT rolled back
        assert cp.last_event_timestamp == past_ts

        mock_notify.assert_awaited_once()
        call_kwargs = mock_notify.call_args
        subject = call_kwargs[1].get("subject") or call_kwargs.kwargs.get("subject", "")
        assert "Silent signal" in subject
        from tsigma.notifications.registry import WARNING
        severity = call_kwargs[1].get("severity") or call_kwargs.kwargs.get("severity")
        assert severity == WARNING

    @pytest.mark.asyncio
    async def test_poll_cycle_no_matching_signals(self):
        """No signals with matching method results in debug log only."""
        sf, mock_session = _mock_session_factory()

        row = MagicMock()
        row.signal_id = "SIG-001"
        row.ip_address = "10.0.0.1"
        row.signal_metadata = {
            "collection": {"method": "other_method"}
        }

        mock_result = MagicMock()
        mock_result.all.return_value = [row]
        mock_session.execute.return_value = mock_result

        mock_method = AsyncMock(spec=PollingIngestionMethod)

        svc = CollectorService(sf, _make_settings())
        svc._polling_instances["test_poll"] = mock_method
        svc._session_factory = sf

        # Should not crash, should log debug, no poll_once calls
        await svc._run_poll_cycle("test_poll")
        mock_method.poll_once.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_poll_cycle_unknown_method_returns_early(self):
        """Poll cycle for unknown method returns early with error log."""
        sf, _ = _mock_session_factory()

        svc = CollectorService(sf, _make_settings())
        # No instances registered

        # Should not crash
        await svc._run_poll_cycle("nonexistent_method")


class TestHealthCheckExceptionHandling:
    """Tests for health_check exception paths."""

    @pytest.mark.asyncio
    async def test_health_check_exception_returns_false(self):
        """Test health_check returns False when method raises."""
        svc = CollectorService(AsyncMock(), _make_settings())

        mock_poll = AsyncMock(spec=PollingIngestionMethod)
        mock_poll.health_check.side_effect = RuntimeError("boom")
        svc._polling_instances["broken"] = mock_poll

        result = await svc.health_check()
        assert result["broken"] is False


class TestHealthCheck:
    """Tests for CollectorService.health_check()."""

    @pytest.mark.asyncio
    async def test_aggregates_polling_health(self):
        """Test health_check includes polling method results."""
        svc = CollectorService(AsyncMock(), _make_settings())

        mock_poll = AsyncMock(spec=PollingIngestionMethod)
        mock_poll.health_check.return_value = True
        svc._polling_instances["ftp_pull"] = mock_poll

        result = await svc.health_check()
        assert result["ftp_pull"] is True

    @pytest.mark.asyncio
    async def test_unhealthy_method(self):
        """Test health_check reports unhealthy methods."""
        svc = CollectorService(AsyncMock(), _make_settings())

        mock_poll = AsyncMock(spec=PollingIngestionMethod)
        mock_poll.health_check.return_value = False
        svc._polling_instances["broken"] = mock_poll

        result = await svc.health_check()
        assert result["broken"] is False

    @pytest.mark.asyncio
    async def test_empty_when_no_methods(self):
        """Test health_check returns empty dict with no methods."""
        svc = CollectorService(AsyncMock(), _make_settings())
        result = await svc.health_check()
        assert result == {}
