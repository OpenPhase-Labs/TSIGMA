"""Tests for the GraphQL ``events`` resolver after Stage 3 (B12.4) pagination.

After Stage 3, ``Query.events`` returns ``EventListPage`` (items + next_cursor)
instead of ``list[EventType]``. The ``limit: int = 10000`` arg is replaced with
``first: int | None = None`` (page size, clamped by ``api.max_page_size``) and
``after: str | None = None`` (opaque cursor). The resolver attaches the
requested ``signal_id`` to the DataFrame before invoking
``paginated_event_list`` so the cursor tuple is stable.
"""

import asyncio
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd

from tests._helpers import make_mock_session
from tsigma.api.graphql.schema import Query
from tsigma.api.graphql.types import EventListPage, EventType


def _run(coro):
    return asyncio.run(coro)


def _make_info(session):
    info = MagicMock()
    info.context = {"session": session}
    return info


def _make_df(n: int, *, start_time: datetime | None = None) -> pd.DataFrame:
    """Build a DataFrame with n rows of canonical event columns.

    Does NOT include a ``signal_id`` column — the SDK helper
    ``fetch_events`` returns single-signal results without one, mirroring
    what the production resolver receives.
    """
    base = start_time or datetime(2025, 6, 15, 8, 0, 0)
    return pd.DataFrame(
        {
            "event_time": [base + timedelta(seconds=i) for i in range(n)],
            "event_code": [1] * n,
            "event_param": [0] * n,
        }
    )


def _patch_max_page_size(value: int):
    """Patch the api.max_page_size lookup inside ``paginated_event_list``.

    Note the patch target is the ``pagination`` module's local
    ``settings_service`` binding — ``paginated_event_list`` imports it
    into its own namespace, so patching at the graphql schema layer
    would not intercept the call.
    """
    return patch(
        "tsigma.reports.sdk.pagination.settings_service.get_int",
        new=AsyncMock(return_value=value),
    )


class TestEventsResolverPagination:
    """Stage 3: GraphQL ``events`` honors cursor pagination."""

    def test_events_returns_event_list_page_with_no_cursor_when_under_limit(self):
        df = _make_df(3)
        session = make_mock_session()
        info = _make_info(session)

        with patch(
            "tsigma.api.graphql.schema.fetch_events",
            new=AsyncMock(return_value=df),
        ), _patch_max_page_size(1000):
            query = Query()
            result = _run(
                query.events(
                    info,
                    signal_id="sig-1",
                    start=datetime(2025, 6, 15, 7, 0, 0),
                    end=datetime(2025, 6, 15, 9, 0, 0),
                )
            )

        assert isinstance(result, EventListPage)
        assert len(result.items) == 3
        assert result.next_cursor is None
        for item in result.items:
            assert isinstance(item, EventType)
            assert item.signal_id == "sig-1"

    def test_events_returns_next_cursor_when_more_rows_than_first(self):
        df = _make_df(50)
        session = make_mock_session()
        info = _make_info(session)

        with patch(
            "tsigma.api.graphql.schema.fetch_events",
            new=AsyncMock(return_value=df),
        ), _patch_max_page_size(1000):
            query = Query()
            result = _run(
                query.events(
                    info,
                    signal_id="sig-1",
                    start=datetime(2025, 6, 15, 7, 0, 0),
                    end=datetime(2025, 6, 15, 9, 0, 0),
                    first=10,
                )
            )

        assert isinstance(result, EventListPage)
        assert len(result.items) == 10
        assert result.next_cursor is not None
        assert isinstance(result.next_cursor, str)
        assert len(result.next_cursor) > 0

    def test_events_clamps_first_to_max_page_size(self):
        df = _make_df(200)
        session = make_mock_session()
        info = _make_info(session)

        with patch(
            "tsigma.api.graphql.schema.fetch_events",
            new=AsyncMock(return_value=df),
        ), _patch_max_page_size(100):
            query = Query()
            result = _run(
                query.events(
                    info,
                    signal_id="sig-1",
                    start=datetime(2025, 6, 15, 7, 0, 0),
                    end=datetime(2025, 6, 15, 9, 0, 0),
                    first=500,
                )
            )

        assert isinstance(result, EventListPage)
        assert len(result.items) == 100
        assert result.next_cursor is not None

    def test_events_after_cursor_skips_prior_rows(self):
        df = _make_df(20)
        session = make_mock_session()
        info = _make_info(session)

        with patch(
            "tsigma.api.graphql.schema.fetch_events",
            new=AsyncMock(return_value=df),
        ), _patch_max_page_size(1000):
            query = Query()
            page1 = _run(
                query.events(
                    info,
                    signal_id="sig-1",
                    start=datetime(2025, 6, 15, 7, 0, 0),
                    end=datetime(2025, 6, 15, 9, 0, 0),
                    first=5,
                )
            )
            assert page1.next_cursor is not None
            page2 = _run(
                query.events(
                    info,
                    signal_id="sig-1",
                    start=datetime(2025, 6, 15, 7, 0, 0),
                    end=datetime(2025, 6, 15, 9, 0, 0),
                    first=5,
                    after=page1.next_cursor,
                )
            )

        assert len(page2.items) == 5
        last_page1_time = page1.items[-1].event_time
        first_page2_time = page2.items[0].event_time
        assert first_page2_time > last_page1_time

    def test_events_with_event_codes_filter_propagates_to_fetch_events(self):
        df = _make_df(0)
        session = make_mock_session()
        info = _make_info(session)

        with patch(
            "tsigma.api.graphql.schema.fetch_events",
            new=AsyncMock(return_value=df),
        ) as mock_fetch, _patch_max_page_size(1000):
            query = Query()
            result = _run(
                query.events(
                    info,
                    signal_id="sig-1",
                    start=datetime(2025, 6, 15, 7, 0, 0),
                    end=datetime(2025, 6, 15, 9, 0, 0),
                    event_codes=[1, 8],
                )
            )

        assert mock_fetch.await_count == 1
        assert mock_fetch.await_args.kwargs["event_codes"] == [1, 8]
        assert isinstance(result, EventListPage)
        assert result.items == []
        assert result.next_cursor is None

    def test_events_attaches_signal_id_to_dataframe_before_pagination(self):
        """Cursor stability requires a constant signal_id column on the
        DataFrame BEFORE paginated_event_list is invoked. The SDK returns
        single-signal results without one, so the resolver must add it.
        """
        df = _make_df(3)
        assert "signal_id" not in df.columns  # sanity: SDK shape

        captured_df: dict = {}

        async def _fake_paginate(rows, *, after, limit, session):
            captured_df["df"] = rows
            return [], None

        session = make_mock_session()
        info = _make_info(session)

        with patch(
            "tsigma.api.graphql.schema.fetch_events",
            new=AsyncMock(return_value=df),
        ), patch(
            "tsigma.api.graphql.schema.paginated_event_list",
            new=AsyncMock(side_effect=_fake_paginate),
        ):
            query = Query()
            _run(
                query.events(
                    info,
                    signal_id="sig-1",
                    start=datetime(2025, 6, 15, 7, 0, 0),
                    end=datetime(2025, 6, 15, 9, 0, 0),
                )
            )

        passed = captured_df["df"]
        assert "signal_id" in passed.columns
        assert (passed["signal_id"] == "sig-1").all()

    def test_events_default_first_uses_max_page_size(self):
        df = _make_df(30)
        session = make_mock_session()
        info = _make_info(session)

        with patch(
            "tsigma.api.graphql.schema.fetch_events",
            new=AsyncMock(return_value=df),
        ), _patch_max_page_size(25):
            query = Query()
            result = _run(
                query.events(
                    info,
                    signal_id="sig-1",
                    start=datetime(2025, 6, 15, 7, 0, 0),
                    end=datetime(2025, 6, 15, 9, 0, 0),
                )
            )

        assert isinstance(result, EventListPage)
        assert len(result.items) == 25
        assert result.next_cursor is not None
