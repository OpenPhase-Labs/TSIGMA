"""
Tests for SchedulerService.

Tests the APScheduler abstraction layer that provides consistent
job lifecycle management for all TSIGMA services.
"""

from unittest.mock import AsyncMock, patch

import pytest

from tsigma.scheduler.service import SchedulerService


def _mock_session_factory():
    """Create a mock async session factory that supports 'async with'."""
    mock_session = AsyncMock()
    mock_session.commit = AsyncMock()
    mock_session.rollback = AsyncMock()

    mock_ctx = AsyncMock()
    mock_ctx.__aenter__ = AsyncMock(return_value=mock_session)
    mock_ctx.__aexit__ = AsyncMock(return_value=False)

    def factory():
        return mock_ctx

    return factory, mock_session


class TestSchedulerServiceInit:
    """Tests for SchedulerService constructor."""

    def test_starts_with_no_scheduler(self):
        """Test internal scheduler is None before start."""
        svc = SchedulerService()
        assert svc._scheduler is None

    def test_stores_session_factory(self):
        """Test session factory is stored."""
        sf = AsyncMock()
        svc = SchedulerService(session_factory=sf)
        assert svc._session_factory is sf

    def test_no_session_factory_defaults_to_none(self):
        """Test session factory defaults to None."""
        svc = SchedulerService()
        assert svc._session_factory is None


class TestSchedulerServiceStart:
    """Tests for SchedulerService.start()."""

    @pytest.mark.asyncio
    async def test_creates_apscheduler(self):
        """Test start creates an APScheduler instance."""
        svc = SchedulerService()
        await svc.start()
        assert svc._scheduler is not None
        await svc.stop()

    @pytest.mark.asyncio
    async def test_scheduler_is_running(self):
        """Test running property is True after start."""
        svc = SchedulerService()
        await svc.start()
        assert svc.running is True
        await svc.stop()

    @pytest.mark.asyncio
    async def test_start_is_idempotent(self):
        """Test calling start twice does not raise."""
        svc = SchedulerService()
        await svc.start()
        await svc.start()
        assert svc.running is True
        await svc.stop()


class TestSchedulerServiceStop:
    """Tests for SchedulerService.stop()."""

    @pytest.mark.asyncio
    async def test_stops_scheduler(self):
        """Test running is False after stop."""
        svc = SchedulerService()
        await svc.start()
        await svc.stop()
        assert svc.running is False

    @pytest.mark.asyncio
    async def test_scheduler_set_to_none(self):
        """Test internal scheduler is None after stop."""
        svc = SchedulerService()
        await svc.start()
        await svc.stop()
        assert svc._scheduler is None

    @pytest.mark.asyncio
    async def test_stop_when_not_started(self):
        """Test stop does not error when scheduler was never started."""
        svc = SchedulerService()
        await svc.stop()

    @pytest.mark.asyncio
    async def test_stop_is_idempotent(self):
        """Test calling stop twice does not raise."""
        svc = SchedulerService()
        await svc.start()
        await svc.stop()
        await svc.stop()


class TestRunningProperty:
    """Tests for SchedulerService.running property."""

    def test_false_before_start(self):
        """Test running is False before start."""
        svc = SchedulerService()
        assert svc.running is False

    @pytest.mark.asyncio
    async def test_true_after_start(self):
        """Test running is True after start."""
        svc = SchedulerService()
        await svc.start()
        assert svc.running is True
        await svc.stop()

    @pytest.mark.asyncio
    async def test_false_after_stop(self):
        """Test running is False after stop."""
        svc = SchedulerService()
        await svc.start()
        await svc.stop()
        assert svc.running is False


class TestAddJob:
    """Tests for SchedulerService.add_job()."""

    @pytest.mark.asyncio
    async def test_add_interval_job(self):
        """Test adding an interval-triggered job."""
        svc = SchedulerService()
        await svc.start()

        async def my_job():
            pass

        svc.add_job(my_job, "interval", job_id="test_int", seconds=60)

        job = svc._scheduler.get_job("test_int")
        assert job is not None
        assert job.id == "test_int"
        await svc.stop()

    @pytest.mark.asyncio
    async def test_add_cron_job(self):
        """Test adding a cron-triggered job."""
        svc = SchedulerService()
        await svc.start()

        async def my_job():
            pass

        svc.add_job(my_job, "cron", job_id="test_cron", hour=3, minute=0)

        job = svc._scheduler.get_job("test_cron")
        assert job is not None
        await svc.stop()

    @pytest.mark.asyncio
    async def test_add_job_with_args(self):
        """Test adding a job with positional arguments."""
        svc = SchedulerService()
        await svc.start()

        async def my_job(x, y):
            pass

        svc.add_job(
            my_job, "interval", job_id="test_args",
            args=["hello", 42], seconds=60,
        )

        job = svc._scheduler.get_job("test_args")
        assert job is not None
        assert job.args == ("hello", 42)
        await svc.stop()

    @pytest.mark.asyncio
    async def test_add_job_with_custom_id(self):
        """Test job_id is set correctly."""
        svc = SchedulerService()
        await svc.start()

        async def my_job():
            pass

        svc.add_job(my_job, "interval", job_id="custom_id_123", seconds=30)

        job = svc._scheduler.get_job("custom_id_123")
        assert job is not None
        assert job.id == "custom_id_123"
        await svc.stop()

    @pytest.mark.asyncio
    async def test_add_job_with_name(self):
        """Test job name is set correctly."""
        svc = SchedulerService()
        await svc.start()

        async def my_job():
            pass

        svc.add_job(
            my_job, "interval", job_id="test_name",
            name="My Named Job", seconds=60,
        )

        job = svc._scheduler.get_job("test_name")
        assert job.name == "My Named Job"
        await svc.stop()

    @pytest.mark.asyncio
    async def test_max_instances_default(self):
        """Test max_instances defaults to 1."""
        svc = SchedulerService()
        await svc.start()

        async def my_job():
            pass

        svc.add_job(my_job, "interval", job_id="test_max", seconds=60)

        job = svc._scheduler.get_job("test_max")
        assert job.max_instances == 1
        await svc.stop()

    @pytest.mark.asyncio
    async def test_max_instances_override(self):
        """Test max_instances can be overridden."""
        svc = SchedulerService()
        await svc.start()

        async def my_job():
            pass

        svc.add_job(
            my_job, "interval", job_id="test_max2",
            max_instances=3, seconds=60,
        )

        job = svc._scheduler.get_job("test_max2")
        assert job.max_instances == 3
        await svc.stop()


class TestRemoveJob:
    """Tests for SchedulerService.remove_job()."""

    @pytest.mark.asyncio
    async def test_remove_existing_job(self):
        """Test removing an existing job."""
        svc = SchedulerService()
        await svc.start()

        async def my_job():
            pass

        svc.add_job(my_job, "interval", job_id="to_remove", seconds=60)
        assert svc._scheduler.get_job("to_remove") is not None

        svc.remove_job("to_remove")
        assert svc._scheduler.get_job("to_remove") is None
        await svc.stop()

    @pytest.mark.asyncio
    async def test_remove_nonexistent_job(self):
        """Test removing a nonexistent job does not raise."""
        svc = SchedulerService()
        await svc.start()
        svc.remove_job("does_not_exist")
        await svc.stop()


class TestLoadRegistry:
    """Tests for SchedulerService.load_registry()."""

    @pytest.mark.asyncio
    async def test_loads_all_registered_jobs(self):
        """Test load_registry adds all JobRegistry jobs to scheduler."""
        mock_jobs = {
            "job_a": {
                "func": AsyncMock(),
                "trigger": "cron",
                "trigger_kwargs": {"hour": 3},
            },
            "job_b": {
                "func": AsyncMock(),
                "trigger": "interval",
                "trigger_kwargs": {"minutes": 15},
            },
        }

        svc = SchedulerService()
        await svc.start()

        with patch("tsigma.scheduler.service.JobRegistry") as mock_registry:
            mock_registry.list_all.return_value = mock_jobs
            svc.load_registry()

        assert svc._scheduler.get_job("job_a") is not None
        assert svc._scheduler.get_job("job_b") is not None
        await svc.stop()

    @pytest.mark.asyncio
    async def test_wraps_with_session_injection(self):
        """Test job function receives a DB session when factory is set."""
        captured_session = None

        async def my_job(session):
            nonlocal captured_session
            captured_session = session

        mock_jobs = {
            "session_job": {
                "func": my_job,
                "trigger": "interval",
                "trigger_kwargs": {"seconds": 60},
            },
        }

        sf, mock_session = _mock_session_factory()
        svc = SchedulerService(session_factory=sf)
        await svc.start()

        with patch("tsigma.scheduler.service.JobRegistry") as mock_registry:
            mock_registry.list_all.return_value = mock_jobs
            svc.load_registry()

        # Get the wrapper and call it directly
        job = svc._scheduler.get_job("session_job")
        await job.func()

        assert captured_session is mock_session

    @pytest.mark.asyncio
    async def test_session_commit_on_success(self):
        """Test session is committed after successful job execution."""
        async def my_job(session):
            pass

        mock_jobs = {
            "commit_job": {
                "func": my_job,
                "trigger": "interval",
                "trigger_kwargs": {"seconds": 60},
            },
        }

        sf, mock_session = _mock_session_factory()
        svc = SchedulerService(session_factory=sf)
        await svc.start()

        with patch("tsigma.scheduler.service.JobRegistry") as mock_registry:
            mock_registry.list_all.return_value = mock_jobs
            svc.load_registry()

        job = svc._scheduler.get_job("commit_job")
        await job.func()

        mock_session.commit.assert_awaited_once()
        mock_session.rollback.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_session_rollback_on_error(self):
        """Test session is rolled back when job raises an exception."""
        async def my_job(session):
            raise RuntimeError("job failed")

        mock_jobs = {
            "fail_job": {
                "func": my_job,
                "trigger": "interval",
                "trigger_kwargs": {"seconds": 60},
            },
        }

        sf, mock_session = _mock_session_factory()
        svc = SchedulerService(session_factory=sf)
        await svc.start()

        with patch("tsigma.scheduler.service.JobRegistry") as mock_registry:
            mock_registry.list_all.return_value = mock_jobs
            svc.load_registry()

        job = svc._scheduler.get_job("fail_job")
        with pytest.raises(RuntimeError, match="job failed"):
            await job.func()

        mock_session.rollback.assert_awaited_once()
        mock_session.commit.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_no_wrap_without_session_factory(self):
        """Test jobs run without session wrapper if no factory provided."""
        called = False

        async def my_job():
            nonlocal called
            called = True

        mock_jobs = {
            "no_session_job": {
                "func": my_job,
                "trigger": "interval",
                "trigger_kwargs": {"seconds": 60},
            },
        }

        svc = SchedulerService()
        await svc.start()

        with patch("tsigma.scheduler.service.JobRegistry") as mock_registry:
            mock_registry.list_all.return_value = mock_jobs
            svc.load_registry()

        job = svc._scheduler.get_job("no_session_job")
        await job.func()

        assert called is True

    @pytest.mark.asyncio
    async def test_skips_session_wrap_when_needs_session_false(self):
        """Test jobs with needs_session=False are not wrapped."""
        called_with_args = []

        async def no_session_job():
            called_with_args.append("called")

        mock_jobs = {
            "poll_job": {
                "func": no_session_job,
                "trigger": "interval",
                "trigger_kwargs": {"seconds": 60},
                "needs_session": False,
            },
        }

        sf, mock_session = _mock_session_factory()
        svc = SchedulerService(session_factory=sf)
        await svc.start()

        with patch("tsigma.scheduler.service.JobRegistry") as mock_registry:
            mock_registry.list_all.return_value = mock_jobs
            svc.load_registry()

        job = svc._scheduler.get_job("poll_job")
        await job.func()

        assert called_with_args == ["called"]
        mock_session.commit.assert_not_awaited()
        await svc.stop()

    @pytest.mark.asyncio
    async def test_wraps_session_when_needs_session_true(self):
        """Test jobs with needs_session=True get session injection."""
        captured_session = None

        async def session_job(session):
            nonlocal captured_session
            captured_session = session

        mock_jobs = {
            "cron_job": {
                "func": session_job,
                "trigger": "cron",
                "trigger_kwargs": {"hour": 3},
                "needs_session": True,
            },
        }

        sf, mock_session = _mock_session_factory()
        svc = SchedulerService(session_factory=sf)
        await svc.start()

        with patch("tsigma.scheduler.service.JobRegistry") as mock_registry:
            mock_registry.list_all.return_value = mock_jobs
            svc.load_registry()

        job = svc._scheduler.get_job("cron_job")
        await job.func()

        assert captured_session is mock_session
        mock_session.commit.assert_awaited_once()
        await svc.stop()

    @pytest.mark.asyncio
    async def test_wraps_session_by_default_when_flag_missing(self):
        """Test jobs without needs_session key default to session wrapping."""
        captured_session = None

        async def legacy_job(session):
            nonlocal captured_session
            captured_session = session

        mock_jobs = {
            "legacy": {
                "func": legacy_job,
                "trigger": "interval",
                "trigger_kwargs": {"seconds": 60},
            },
        }

        sf, mock_session = _mock_session_factory()
        svc = SchedulerService(session_factory=sf)
        await svc.start()

        with patch("tsigma.scheduler.service.JobRegistry") as mock_registry:
            mock_registry.list_all.return_value = mock_jobs
            svc.load_registry()

        job = svc._scheduler.get_job("legacy")
        await job.func()

        assert captured_session is mock_session
        await svc.stop()

    @pytest.mark.asyncio
    async def test_empty_registry(self):
        """Test load_registry with no jobs registered."""
        svc = SchedulerService()
        await svc.start()

        with patch("tsigma.scheduler.service.JobRegistry") as mock_registry:
            mock_registry.list_all.return_value = {}
            svc.load_registry()

        assert svc._scheduler.get_jobs() == []
        await svc.stop()
