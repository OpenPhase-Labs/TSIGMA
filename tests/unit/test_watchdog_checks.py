"""
Tests for the six new watchdog data-quality checks and the alert-suppression
gate that fronts them.

Each check has its own helper in ``tsigma.scheduler.jobs.watchdog``; these
tests patch the module-level ``notify`` and ``settings`` to verify the right
alerts fire (or don't) for each scenario.
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tsigma.scheduler.jobs import watchdog as wd

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


def _row(**kwargs) -> MagicMock:
    """Return a MagicMock with the given attributes (simulates a SQLAlchemy Row)."""
    return MagicMock(**kwargs)


def _result(rows: list) -> MagicMock:
    """Wrap a row list into a MagicMock that behaves like a Result."""
    r = MagicMock()
    r.all.return_value = rows
    r.scalar.return_value = rows[0] if rows else None
    r.scalar_one_or_none.return_value = rows[0] if rows else None
    return r


def _empty_result() -> MagicMock:
    r = MagicMock()
    r.all.return_value = []
    r.scalar.return_value = None
    r.scalar_one_or_none.return_value = None
    return r


# ---------------------------------------------------------------------------
# Alert suppression
# ---------------------------------------------------------------------------


class TestIsSuppressed:

    @pytest.mark.asyncio
    async def test_no_rule_returns_false(self):
        session = _mock_session()
        session.execute = AsyncMock(return_value=_empty_result())
        assert await wd._is_suppressed(session, "SIG-1", "low_event_count") is False

    @pytest.mark.asyncio
    async def test_matching_rule_returns_true(self):
        session = _mock_session()
        # scalar() returns something truthy to indicate a matching rule
        result = MagicMock()
        result.scalar.return_value = 1
        session.execute = AsyncMock(return_value=result)
        assert await wd._is_suppressed(session, "SIG-1", "low_event_count") is True

    @pytest.mark.asyncio
    async def test_db_error_fails_open(self):
        """DB failure must not block the watchdog — behaves as not-suppressed."""
        session = _mock_session()
        session.execute = AsyncMock(side_effect=RuntimeError("db down"))
        assert await wd._is_suppressed(session, "SIG-1", "low_event_count") is False


# ---------------------------------------------------------------------------
# 1. Low event count detection
# ---------------------------------------------------------------------------


class TestLowEventCount:

    @pytest.mark.asyncio
    @patch("tsigma.scheduler.jobs.watchdog.notify", new_callable=AsyncMock)
    @patch("tsigma.scheduler.jobs.watchdog.settings")
    async def test_flags_low_volume_signal(self, mock_settings, mock_notify):
        mock_settings.watchdog_low_event_count_threshold = 100
        session = _mock_session()
        session.execute = AsyncMock(side_effect=[
            _result([_row(signal_id="SIG-1", event_count=40)]),
            _empty_result(),  # _is_suppressed lookup
        ])
        await wd._check_low_event_count(session)
        assert mock_notify.call_count == 1
        assert "Low Event" in mock_notify.call_args.kwargs["subject"]

    @pytest.mark.asyncio
    @patch("tsigma.scheduler.jobs.watchdog.notify", new_callable=AsyncMock)
    @patch("tsigma.scheduler.jobs.watchdog.settings")
    async def test_no_flag_when_above_threshold(self, mock_settings, mock_notify):
        mock_settings.watchdog_low_event_count_threshold = 100
        session = _mock_session()
        session.execute = AsyncMock(return_value=_empty_result())
        await wd._check_low_event_count(session)
        mock_notify.assert_not_called()

    @pytest.mark.asyncio
    @patch("tsigma.scheduler.jobs.watchdog.notify", new_callable=AsyncMock)
    @patch("tsigma.scheduler.jobs.watchdog.settings")
    async def test_suppressed_signal_is_not_notified(self, mock_settings, mock_notify):
        mock_settings.watchdog_low_event_count_threshold = 100
        session = _mock_session()
        suppressed = MagicMock()
        suppressed.scalar.return_value = 1  # suppression match
        session.execute = AsyncMock(side_effect=[
            _result([_row(signal_id="SIG-1", event_count=40)]),
            suppressed,
        ])
        await wd._check_low_event_count(session)
        mock_notify.assert_not_called()


# ---------------------------------------------------------------------------
# 2. Missing data window detection
# ---------------------------------------------------------------------------


class TestMissingDataWindow:

    @pytest.mark.asyncio
    @patch("tsigma.scheduler.jobs.watchdog.notify", new_callable=AsyncMock)
    @patch("tsigma.scheduler.jobs.watchdog.settings")
    async def test_flags_signal_with_stale_data(self, mock_settings, mock_notify):
        mock_settings.watchdog_missing_window_minutes = 30
        session = _mock_session()
        stale_time = datetime.now(timezone.utc) - timedelta(minutes=45)
        session.execute = AsyncMock(side_effect=[
            _result([_row(signal_id="SIG-2", last_event=stale_time)]),
            _empty_result(),
        ])
        await wd._check_missing_data_window(session)
        assert mock_notify.call_count == 1
        assert "Missing Data" in mock_notify.call_args.kwargs["subject"]

    @pytest.mark.asyncio
    @patch("tsigma.scheduler.jobs.watchdog.notify", new_callable=AsyncMock)
    @patch("tsigma.scheduler.jobs.watchdog.settings")
    async def test_no_flag_when_recent(self, mock_settings, mock_notify):
        mock_settings.watchdog_missing_window_minutes = 30
        session = _mock_session()
        session.execute = AsyncMock(return_value=_empty_result())
        await wd._check_missing_data_window(session)
        mock_notify.assert_not_called()


# ---------------------------------------------------------------------------
# 3. Stuck pedestrian button detection
# ---------------------------------------------------------------------------


class TestStuckPed:

    @pytest.mark.asyncio
    @patch("tsigma.scheduler.jobs.watchdog.notify", new_callable=AsyncMock)
    @patch("tsigma.scheduler.jobs.watchdog.settings")
    async def test_flags_continuous_ped_call(self, mock_settings, mock_notify):
        mock_settings.watchdog_stuck_ped_minutes = 120
        session = _mock_session()
        session.execute = AsyncMock(side_effect=[
            _result([_row(signal_id="SIG-3", ped_phase=2, call_count=250)]),
            _empty_result(),
        ])
        await wd._check_stuck_ped(session)
        assert mock_notify.call_count == 1
        assert "Ped" in mock_notify.call_args.kwargs["subject"]

    @pytest.mark.asyncio
    @patch("tsigma.scheduler.jobs.watchdog.notify", new_callable=AsyncMock)
    @patch("tsigma.scheduler.jobs.watchdog.settings")
    async def test_no_flag_when_normal(self, mock_settings, mock_notify):
        mock_settings.watchdog_stuck_ped_minutes = 120
        session = _mock_session()
        session.execute = AsyncMock(return_value=_empty_result())
        await wd._check_stuck_ped(session)
        mock_notify.assert_not_called()


# ---------------------------------------------------------------------------
# 4. Phase termination anomaly detection
# ---------------------------------------------------------------------------


class TestPhaseTerminationAnomaly:

    @pytest.mark.asyncio
    @patch("tsigma.scheduler.jobs.watchdog.notify", new_callable=AsyncMock)
    @patch("tsigma.scheduler.jobs.watchdog.settings")
    async def test_flags_anomalous_phase(self, mock_settings, mock_notify):
        mock_settings.watchdog_termination_anomaly_stddev = 3.0
        session = _mock_session()
        # Row: last-hour ratio plus 7-day baseline mean/stddev.
        # gap_out ratio = 0.1 last hour; baseline mean 0.8 stddev 0.05
        # => z-score = (0.1 - 0.8) / 0.05 = -14 (|z| >> 3)
        anomaly_row = _row(
            signal_id="SIG-4",
            phase=2,
            recent_gap_ratio=0.1,
            recent_max_ratio=0.5,
            recent_force_ratio=0.4,
            baseline_gap_mean=0.8,
            baseline_gap_stddev=0.05,
            baseline_max_mean=0.1,
            baseline_max_stddev=0.02,
            baseline_force_mean=0.1,
            baseline_force_stddev=0.02,
        )
        session.execute = AsyncMock(side_effect=[
            _result([anomaly_row]),
            _empty_result(),
        ])
        await wd._check_phase_termination_anomaly(session)
        assert mock_notify.call_count == 1
        assert "Termination" in mock_notify.call_args.kwargs["subject"]

    @pytest.mark.asyncio
    @patch("tsigma.scheduler.jobs.watchdog.notify", new_callable=AsyncMock)
    @patch("tsigma.scheduler.jobs.watchdog.settings")
    async def test_no_flag_when_within_tolerance(self, mock_settings, mock_notify):
        mock_settings.watchdog_termination_anomaly_stddev = 3.0
        session = _mock_session()
        normal_row = _row(
            signal_id="SIG-4",
            phase=2,
            recent_gap_ratio=0.79,
            recent_max_ratio=0.11,
            recent_force_ratio=0.10,
            baseline_gap_mean=0.8,
            baseline_gap_stddev=0.05,
            baseline_max_mean=0.1,
            baseline_max_stddev=0.02,
            baseline_force_mean=0.1,
            baseline_force_stddev=0.02,
        )
        session.execute = AsyncMock(return_value=_result([normal_row]))
        await wd._check_phase_termination_anomaly(session)
        mock_notify.assert_not_called()

    @pytest.mark.asyncio
    @patch("tsigma.scheduler.jobs.watchdog.notify", new_callable=AsyncMock)
    @patch("tsigma.scheduler.jobs.watchdog.settings")
    async def test_skips_zero_stddev_baseline(self, mock_settings, mock_notify):
        """Phases with zero baseline stddev cannot be z-scored — skip them."""
        mock_settings.watchdog_termination_anomaly_stddev = 3.0
        session = _mock_session()
        zero_row = _row(
            signal_id="SIG-4",
            phase=3,
            recent_gap_ratio=0.5,
            recent_max_ratio=0.3,
            recent_force_ratio=0.2,
            baseline_gap_mean=0.8,
            baseline_gap_stddev=0.0,
            baseline_max_mean=0.1,
            baseline_max_stddev=0.0,
            baseline_force_mean=0.1,
            baseline_force_stddev=0.0,
        )
        session.execute = AsyncMock(return_value=_result([zero_row]))
        await wd._check_phase_termination_anomaly(session)
        mock_notify.assert_not_called()


# ---------------------------------------------------------------------------
# 5. Low hit count detection
# ---------------------------------------------------------------------------


class TestLowHitCount:

    @pytest.mark.asyncio
    @patch("tsigma.scheduler.jobs.watchdog.notify", new_callable=AsyncMock)
    @patch("tsigma.scheduler.jobs.watchdog.settings")
    async def test_flags_low_hit_detector(self, mock_settings, mock_notify):
        mock_settings.watchdog_low_hit_threshold = 5
        session = _mock_session()
        # Detector saw 1 ON event in the last hour but the signal had green.
        session.execute = AsyncMock(side_effect=[
            _result([
                _row(signal_id="SIG-5", detector_channel=4, hit_count=1),
            ]),
            _empty_result(),
        ])
        await wd._check_low_hit_count(session)
        assert mock_notify.call_count == 1
        assert "Low Hit" in mock_notify.call_args.kwargs["subject"]

    @pytest.mark.asyncio
    @patch("tsigma.scheduler.jobs.watchdog.notify", new_callable=AsyncMock)
    @patch("tsigma.scheduler.jobs.watchdog.settings")
    async def test_no_flag_when_above_threshold(self, mock_settings, mock_notify):
        mock_settings.watchdog_low_hit_threshold = 5
        session = _mock_session()
        session.execute = AsyncMock(return_value=_empty_result())
        await wd._check_low_hit_count(session)
        mock_notify.assert_not_called()


# ---------------------------------------------------------------------------
# 6. Controller clock drift detection (inc-4 Task 4)
# ---------------------------------------------------------------------------


def _drift_get_int(key, session):
    """Return fixed ints per watchdog clock-drift settings key."""
    mapping = {
        "watchdog.controller_clock_drift_threshold_seconds": 120,
        "watchdog.controller_clock_drift_window_hours": 168,
        "watchdog.controller_clock_drift_min_samples": 5,
        "watchdog.controller_clock_offset_retention_days": 30,
    }
    return mapping[key]


class TestControllerClockDrift:

    @pytest.mark.asyncio
    @patch("tsigma.scheduler.jobs.watchdog.notify", new_callable=AsyncMock)
    @patch("tsigma.scheduler.jobs.watchdog.settings_service")
    async def test_drift_over_threshold_notifies(self, mock_settings_service, mock_notify):
        mock_settings_service.get_int = AsyncMock(side_effect=_drift_get_int)
        session = _mock_session()
        # First execute = prune delete (no rows), second = trend query, third =
        # per-row suppression lookup (empty => not suppressed).
        session.execute = AsyncMock(side_effect=[
            _empty_result(),  # prune delete
            _result([
                _row(signal_id="SIG-001", mean_abs_offset_seconds=200.0, samples=10),
            ]),
            _empty_result(),  # _is_suppressed lookup
        ])
        await wd._check_controller_clock_drift(session)
        assert mock_notify.call_count == 1
        assert mock_notify.call_args.kwargs["severity"] == "warning"
        metadata = mock_notify.call_args.kwargs["metadata"]
        assert "SIG-001" in repr(metadata)

    @pytest.mark.asyncio
    @patch("tsigma.scheduler.jobs.watchdog.notify", new_callable=AsyncMock)
    @patch("tsigma.scheduler.jobs.watchdog.settings_service")
    async def test_no_drift_no_notify(self, mock_settings_service, mock_notify):
        mock_settings_service.get_int = AsyncMock(side_effect=_drift_get_int)
        session = _mock_session()
        # Trend query (HAVING filters server-side) returns no rows.
        session.execute = AsyncMock(side_effect=[
            _empty_result(),  # prune delete
            _empty_result(),  # trend query: no drifting signals
        ])
        await wd._check_controller_clock_drift(session)
        mock_notify.assert_not_called()

    @pytest.mark.asyncio
    @patch("tsigma.scheduler.jobs.watchdog.notify", new_callable=AsyncMock)
    @patch("tsigma.scheduler.jobs.watchdog.settings_service")
    async def test_suppressed_no_notify(self, mock_settings_service, mock_notify):
        mock_settings_service.get_int = AsyncMock(side_effect=_drift_get_int)
        session = _mock_session()
        suppressed = MagicMock()
        suppressed.scalar.return_value = 1  # suppression match
        session.execute = AsyncMock(side_effect=[
            _empty_result(),  # prune delete
            _result([
                _row(signal_id="SIG-001", mean_abs_offset_seconds=200.0, samples=10),
            ]),
            suppressed,  # _is_suppressed lookup => suppressed
        ])
        await wd._check_controller_clock_drift(session)
        mock_notify.assert_not_called()

    @pytest.mark.asyncio
    @patch("tsigma.scheduler.jobs.watchdog.notify", new_callable=AsyncMock)
    @patch("tsigma.scheduler.jobs.watchdog.settings_service")
    async def test_prune_executed(self, mock_settings_service, mock_notify):
        mock_settings_service.get_int = AsyncMock(side_effect=_drift_get_int)
        session = _mock_session()
        session.execute = AsyncMock(side_effect=[
            _empty_result(),  # prune delete
            _empty_result(),  # trend query: no drifting signals
        ])
        await wd._check_controller_clock_drift(session)
        # The check must prune old rows before the trend query, so at least the
        # prune + trend executes must have fired.
        assert session.execute.await_count >= 2


# ---------------------------------------------------------------------------
# Top-level watchdog: all checks wired in
# ---------------------------------------------------------------------------


class TestWatchdogWiresAllChecks:

    @pytest.mark.asyncio
    async def test_watchdog_invokes_each_check(self):
        """``watchdog()`` must dispatch to every known check helper."""
        session = _mock_session()

        with (
            patch.object(wd, "_check_silent_signals", new_callable=AsyncMock) as a,
            patch.object(wd, "_check_stuck_detectors", new_callable=AsyncMock) as b,
            patch.object(wd, "_check_low_event_count", new_callable=AsyncMock) as c,
            patch.object(wd, "_check_missing_data_window", new_callable=AsyncMock) as d,
            patch.object(wd, "_check_stuck_ped", new_callable=AsyncMock) as e,
            patch.object(wd, "_check_phase_termination_anomaly", new_callable=AsyncMock) as f,
            patch.object(wd, "_check_low_hit_count", new_callable=AsyncMock) as g,
            patch.object(wd, "_check_controller_clock_drift", new_callable=AsyncMock) as h,
        ):
            await wd.watchdog(session)
            for mock in (a, b, c, d, e, f, g, h):
                mock.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_watchdog_continues_when_a_check_fails(self):
        """An exception inside one check must not stop the others."""
        session = _mock_session()

        with (
            patch.object(wd, "_check_silent_signals", new_callable=AsyncMock,
                         side_effect=RuntimeError("boom")),
            patch.object(wd, "_check_stuck_detectors", new_callable=AsyncMock) as b,
            patch.object(wd, "_check_low_event_count", new_callable=AsyncMock) as c,
            patch.object(wd, "_check_missing_data_window", new_callable=AsyncMock) as d,
            patch.object(wd, "_check_stuck_ped", new_callable=AsyncMock) as e,
            patch.object(wd, "_check_phase_termination_anomaly", new_callable=AsyncMock) as f,
            patch.object(wd, "_check_low_hit_count", new_callable=AsyncMock) as g,
            patch.object(wd, "_check_controller_clock_drift", new_callable=AsyncMock) as h,
        ):
            await wd.watchdog(session)
            # Remaining checks still ran
            for mock in (b, c, d, e, f, g, h):
                mock.assert_awaited_once()
