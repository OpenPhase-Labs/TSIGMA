"""Tests for the three request-limit guards in tsigma.reports.sdk.limits (B11)."""

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException

from tsigma.reports.sdk.limits import (
    require_max_aggregation_days,
    require_max_lookback,
    require_max_signals_per_request,
)

# ---------------------------------------------------------------------------
# require_max_aggregation_days
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_require_max_aggregation_days_allows_within_window():
    start = datetime(2025, 6, 1)
    end = start + timedelta(days=30)
    with patch(
        "tsigma.settings_service.get_int",
        new=AsyncMock(return_value=92),
    ):
        # Must not raise
        await require_max_aggregation_days(start, end, session=None)


@pytest.mark.asyncio
async def test_require_max_aggregation_days_rejects_over_window():
    start = datetime(2025, 1, 1)
    end = start + timedelta(days=180)
    with patch(
        "tsigma.settings_service.get_int",
        new=AsyncMock(return_value=92),
    ):
        with pytest.raises(HTTPException) as exc:
            await require_max_aggregation_days(start, end, session=None)
    assert exc.value.status_code == 400
    assert "api.max_aggregation_days" in exc.value.detail


# ---------------------------------------------------------------------------
# require_max_signals_per_request
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_require_max_signals_allows_within_limit_list():
    with patch(
        "tsigma.settings_service.get_int",
        new=AsyncMock(return_value=100),
    ):
        await require_max_signals_per_request(["A", "B", "C"], session=None)


@pytest.mark.asyncio
async def test_require_max_signals_allows_single_string_as_one():
    with patch(
        "tsigma.settings_service.get_int",
        new=AsyncMock(return_value=1),
    ):
        await require_max_signals_per_request("SIG-001", session=None)


@pytest.mark.asyncio
async def test_require_max_signals_rejects_over_limit():
    with patch(
        "tsigma.settings_service.get_int",
        new=AsyncMock(return_value=2),
    ):
        with pytest.raises(HTTPException) as exc:
            await require_max_signals_per_request(["A", "B", "C"], session=None)
    assert exc.value.status_code == 400
    assert "api.max_signals_per_request" in exc.value.detail


# ---------------------------------------------------------------------------
# require_max_lookback — re-exported from pagination.py
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_require_max_lookback_reexport_rejects_too_old():
    start = datetime.now(tz=timezone.utc) - timedelta(days=120)
    with patch(
        "tsigma.reports.sdk.pagination.settings_service.get_int",
        new=AsyncMock(return_value=30),
    ):
        with pytest.raises(HTTPException) as exc:
            await require_max_lookback(start, session=None)
    assert exc.value.status_code == 400
    assert "api.max_lookback_days" in exc.value.detail
