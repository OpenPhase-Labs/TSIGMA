"""
Tests for the shared alert-suppression helper ``tsigma.notifications.suppression``.

This helper is the public, reusable form of the logic that previously lived
privately as ``_is_suppressed`` inside ``tsigma.scheduler.jobs.watchdog``. It
is consumed both by the daily watchdog job and by the per-ingest path.

The async-session mocking idiom here mirrors
``tests/unit/test_watchdog_checks.py`` (``TestIsSuppressed``): an ``AsyncMock``
session whose ``execute`` returns a ``MagicMock`` result exposing ``.scalar()``,
and a ``side_effect`` RuntimeError to simulate a database failure.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

from tsigma.notifications.suppression import (
    CHECK_CONFIG_PHASE_DRIFT,
    CHECK_CONTROLLER_REPLACEMENT,
    is_suppressed,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _mock_session() -> AsyncMock:
    """Return an AsyncMock that behaves like an AsyncSession."""
    session = AsyncMock()
    session.execute = AsyncMock()
    return session


def _result(scalar_value) -> MagicMock:
    """Wrap a scalar value into a MagicMock that behaves like a Result."""
    r = MagicMock()
    r.scalar.return_value = scalar_value
    return r


# ---------------------------------------------------------------------------
# is_suppressed
# ---------------------------------------------------------------------------


class TestIsSuppressed:

    @pytest.mark.asyncio
    async def test_is_suppressed_true_when_matching_row(self):
        session = _mock_session()
        session.execute = AsyncMock(return_value=_result(1))
        assert await is_suppressed(session, "1234", "some_check") is True

    @pytest.mark.asyncio
    async def test_is_suppressed_false_when_no_row(self):
        session = _mock_session()
        session.execute = AsyncMock(return_value=_result(0))
        assert await is_suppressed(session, "1234", "some_check") is False

    @pytest.mark.asyncio
    async def test_is_suppressed_false_on_none_count(self):
        session = _mock_session()
        session.execute = AsyncMock(return_value=_result(None))
        assert await is_suppressed(session, "1234", "some_check") is False

    @pytest.mark.asyncio
    async def test_is_suppressed_fails_open_on_db_error(self):
        """DB failure must not silence alerts — behaves as not-suppressed."""
        session = _mock_session()
        session.execute = AsyncMock(side_effect=RuntimeError("db down"))
        assert await is_suppressed(session, "1234", "some_check") is False


# ---------------------------------------------------------------------------
# Check-name constants
# ---------------------------------------------------------------------------


def test_check_name_constants():
    assert CHECK_CONTROLLER_REPLACEMENT == "controller_replacement"
    assert CHECK_CONFIG_PHASE_DRIFT == "config_phase_drift"
