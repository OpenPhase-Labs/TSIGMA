"""Tests for cursor pagination + require_max_lookback (B6)."""

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest
from fastapi import HTTPException

from tsigma.reports.sdk.pagination import (
    paginated_event_list,
    require_max_lookback,
)


def _make_rows(n: int, *, signal_id: str = "SIG_001") -> pd.DataFrame:
    """Build n rows with monotonically-increasing event_time, fixed code/param/signal."""
    base = datetime(2025, 6, 1, 8, 0, 0)
    return pd.DataFrame(
        {
            "signal_id": [signal_id] * n,
            "event_time": [base + timedelta(seconds=i) for i in range(n)],
            "event_code": [1] * n,
            "event_param": [0] * n,
        }
    )


# ---------------------------------------------------------------------------
# paginated_event_list
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_paginated_event_list_first_page_returns_limit_items_and_cursor():
    rows = _make_rows(50)
    with patch(
        "tsigma.reports.sdk.pagination.settings_service.get_int",
        new=AsyncMock(return_value=1000),
    ):
        items, cursor = await paginated_event_list(
            rows, after=None, limit=10, session=None
        )
    assert len(items) == 10
    assert cursor is not None and cursor != ""


@pytest.mark.asyncio
async def test_paginated_event_list_subsequent_page_continues_after_cursor():
    rows = _make_rows(50)
    with patch(
        "tsigma.reports.sdk.pagination.settings_service.get_int",
        new=AsyncMock(return_value=1000),
    ):
        page1, cursor1 = await paginated_event_list(
            rows, after=None, limit=10, session=None
        )
        page2, cursor2 = await paginated_event_list(
            rows, after=cursor1, limit=10, session=None
        )
    # Distinct rows on the two pages
    page1_times = [item["event_time"] for item in page1]
    page2_times = [item["event_time"] for item in page2]
    assert set(page1_times).isdisjoint(set(page2_times))
    # page2 picks up where page1 ended (the next 10 rows)
    assert page2_times[0] > page1_times[-1]


@pytest.mark.asyncio
async def test_paginated_event_list_last_page_returns_none_cursor():
    rows = _make_rows(10)
    with patch(
        "tsigma.reports.sdk.pagination.settings_service.get_int",
        new=AsyncMock(return_value=1000),
    ):
        items, cursor = await paginated_event_list(
            rows, after=None, limit=20, session=None
        )
    assert len(items) == 10
    assert cursor is None


@pytest.mark.asyncio
async def test_paginated_event_list_clamps_to_max_page_size():
    rows = _make_rows(50)
    with patch(
        "tsigma.reports.sdk.pagination.settings_service.get_int",
        new=AsyncMock(return_value=5),
    ):
        items, _cursor = await paginated_event_list(
            rows, after=None, limit=10_000, session=None
        )
    assert len(items) == 5


@pytest.mark.asyncio
async def test_paginated_event_list_invalid_cursor_raises_400():
    rows = _make_rows(10)
    with patch(
        "tsigma.reports.sdk.pagination.settings_service.get_int",
        new=AsyncMock(return_value=1000),
    ):
        with pytest.raises(HTTPException) as exc:
            await paginated_event_list(
                rows, after="not-a-real-cursor!", limit=10, session=None
            )
    assert exc.value.status_code == 400


# ---------------------------------------------------------------------------
# require_max_lookback
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_require_max_lookback_rejects_too_old():
    start = datetime.now(tz=timezone.utc) - timedelta(days=120)
    with patch(
        "tsigma.reports.sdk.pagination.settings_service.get_int",
        new=AsyncMock(return_value=30),
    ):
        with pytest.raises(HTTPException) as exc:
            await require_max_lookback(start, session=None)
    assert exc.value.status_code == 400
    assert "api.max_lookback_days" in exc.value.detail


@pytest.mark.asyncio
async def test_require_max_lookback_allows_within_window():
    start = datetime.now(tz=timezone.utc) - timedelta(days=15)
    with patch(
        "tsigma.reports.sdk.pagination.settings_service.get_int",
        new=AsyncMock(return_value=30),
    ):
        await require_max_lookback(start, session=None)  # must not raise
