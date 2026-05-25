"""Tier-aware routing tests for tsigma.reports.sdk.queries.fetch_events (B7)
and fetch_events_split (B8)."""

from contextlib import ExitStack, asynccontextmanager
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest

from tsigma.reports.sdk.queries import fetch_events, fetch_events_split


def _utcnow_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _hot_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "event_code": [1],
            "event_param": [0],
            "event_time": [pd.Timestamp("2025-10-01 12:00:00")],
        }
    )


def _cold_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "event_code": [10],
            "event_param": [1],
            "event_time": [pd.Timestamp("2024-01-01 12:00:00")],
        }
    )


@asynccontextmanager
async def _fake_session():
    """Stand-in for `db_facade.session()` — settings_service is mocked separately."""
    yield None


def _enter_tier_mocks(
    stack: ExitStack,
    *,
    query_enabled: bool,
    threshold_days: int,
    hot_mock: AsyncMock,
    cold_mock: AsyncMock,
) -> None:
    """Enter all the patches needed for tier-aware fetch_events tests via ExitStack.

    db_facade is None at module load (gets set during app startup), so we
    replace the reference wholesale with a MagicMock carrying the methods
    we need rather than patching attributes on a None object.
    """
    fake_facade = MagicMock()
    fake_facade.session = _fake_session
    fake_facade.get_dataframe = hot_mock

    stack.enter_context(
        patch("tsigma.reports.sdk.queries.db_facade", new=fake_facade)
    )
    stack.enter_context(
        patch(
            "tsigma.settings_service.get_bool",
            new=AsyncMock(return_value=query_enabled),
        )
    )
    stack.enter_context(
        patch(
            "tsigma.settings_service.get_int",
            new=AsyncMock(return_value=threshold_days),
        )
    )
    stack.enter_context(
        patch("tsigma.database.cold_tier.ColdTierQuery.fetch_events", new=cold_mock)
    )


@pytest.mark.asyncio
async def test_all_hot_window_calls_hot_path_only():
    """start AND end within threshold → only db_facade.get_dataframe runs."""
    now = _utcnow_naive()
    start = now - timedelta(days=5)
    end = now - timedelta(days=1)
    hot_mock = AsyncMock(return_value=_hot_df())
    cold_mock = AsyncMock(return_value=_cold_df())

    with ExitStack() as stack:
        _enter_tier_mocks(
            stack,
            query_enabled=True,
            threshold_days=180,
            hot_mock=hot_mock,
            cold_mock=cold_mock,
        )
        df = await fetch_events("SIG_001", start, end, event_codes=[1])

    assert hot_mock.await_count == 1
    assert cold_mock.await_count == 0
    assert len(df) == len(_hot_df())


@pytest.mark.asyncio
async def test_all_cold_window_calls_cold_path_only():
    """end before threshold → only ColdTierQuery.fetch_events runs."""
    now = _utcnow_naive()
    start = now - timedelta(days=400)
    end = now - timedelta(days=300)
    hot_mock = AsyncMock(return_value=_hot_df())
    cold_mock = AsyncMock(return_value=_cold_df())

    with ExitStack() as stack:
        _enter_tier_mocks(
            stack,
            query_enabled=True,
            threshold_days=180,
            hot_mock=hot_mock,
            cold_mock=cold_mock,
        )
        df = await fetch_events("SIG_001", start, end, event_codes=[1])

    assert hot_mock.await_count == 0
    assert cold_mock.await_count == 1
    assert len(df) == len(_cold_df())


@pytest.mark.asyncio
async def test_spanning_window_unions_both_paths():
    """start before threshold, end after → both paths called, concat & sorted."""
    now = _utcnow_naive()
    start = now - timedelta(days=400)
    end = now - timedelta(days=1)
    hot_mock = AsyncMock(return_value=_hot_df())
    cold_mock = AsyncMock(return_value=_cold_df())

    with ExitStack() as stack:
        _enter_tier_mocks(
            stack,
            query_enabled=True,
            threshold_days=180,
            hot_mock=hot_mock,
            cold_mock=cold_mock,
        )
        df = await fetch_events("SIG_001", start, end, event_codes=[1, 10])

    assert hot_mock.await_count == 1
    assert cold_mock.await_count == 1
    # Cold (2024-01-01) before hot (2025-10-01) → sorted ascending
    assert list(df["event_time"]) == [
        pd.Timestamp("2024-01-01 12:00:00"),
        pd.Timestamp("2025-10-01 12:00:00"),
    ]


@pytest.mark.asyncio
async def test_cold_disabled_calls_hot_path_only_even_for_old_window():
    """cold_tier.query_enabled=false → hot-only regardless of window age."""
    now = _utcnow_naive()
    start = now - timedelta(days=400)
    end = now - timedelta(days=300)
    hot_mock = AsyncMock(return_value=_hot_df())
    cold_mock = AsyncMock(return_value=_cold_df())

    with ExitStack() as stack:
        _enter_tier_mocks(
            stack,
            query_enabled=False,
            threshold_days=180,
            hot_mock=hot_mock,
            cold_mock=cold_mock,
        )
        df = await fetch_events("SIG_001", start, end, event_codes=[1])

    assert hot_mock.await_count == 1
    assert cold_mock.await_count == 0
    assert len(df) == len(_hot_df())


# ---------------------------------------------------------------------------
# fetch_events_split tier-routing (B8)
# ---------------------------------------------------------------------------

_SPLIT_KWARGS = dict(phase_codes=[1, 8], det_channels=[10, 11])


@pytest.mark.asyncio
async def test_split_all_hot_window_calls_hot_path_only():
    now = _utcnow_naive()
    start = now - timedelta(days=5)
    end = now - timedelta(days=1)
    hot_mock = AsyncMock(return_value=_hot_df())
    cold_mock = AsyncMock(return_value=_cold_df())

    with ExitStack() as stack:
        _enter_tier_mocks(
            stack,
            query_enabled=True,
            threshold_days=180,
            hot_mock=hot_mock,
            cold_mock=cold_mock,
        )
        df = await fetch_events_split("SIG_001", start, end, **_SPLIT_KWARGS)

    assert hot_mock.await_count == 1
    assert cold_mock.await_count == 0
    assert len(df) == len(_hot_df())


@pytest.mark.asyncio
async def test_split_all_cold_window_calls_cold_path_only():
    now = _utcnow_naive()
    start = now - timedelta(days=400)
    end = now - timedelta(days=300)
    hot_mock = AsyncMock(return_value=_hot_df())
    cold_mock = AsyncMock(return_value=_cold_df())

    with ExitStack() as stack:
        _enter_tier_mocks(
            stack,
            query_enabled=True,
            threshold_days=180,
            hot_mock=hot_mock,
            cold_mock=cold_mock,
        )
        df = await fetch_events_split("SIG_001", start, end, **_SPLIT_KWARGS)

    assert hot_mock.await_count == 0
    assert cold_mock.await_count == 1
    # Verify the cold call used the where_sql_fragment mode (single-scan OR predicate).
    cold_call = cold_mock.await_args
    assert "where_sql_fragment" in cold_call.kwargs
    fragment = cold_call.kwargs["where_sql_fragment"]
    assert "OR" in fragment
    assert "1" in fragment and "8" in fragment      # phase_codes inlined
    assert "10" in fragment and "11" in fragment    # det_channels inlined


@pytest.mark.asyncio
async def test_split_spanning_window_unions_both_paths():
    now = _utcnow_naive()
    start = now - timedelta(days=400)
    end = now - timedelta(days=1)
    hot_mock = AsyncMock(return_value=_hot_df())
    cold_mock = AsyncMock(return_value=_cold_df())

    with ExitStack() as stack:
        _enter_tier_mocks(
            stack,
            query_enabled=True,
            threshold_days=180,
            hot_mock=hot_mock,
            cold_mock=cold_mock,
        )
        df = await fetch_events_split("SIG_001", start, end, **_SPLIT_KWARGS)

    assert hot_mock.await_count == 1
    assert cold_mock.await_count == 1
    assert list(df["event_time"]) == [
        pd.Timestamp("2024-01-01 12:00:00"),
        pd.Timestamp("2025-10-01 12:00:00"),
    ]


@pytest.mark.asyncio
async def test_split_cold_disabled_calls_hot_path_only():
    now = _utcnow_naive()
    start = now - timedelta(days=400)
    end = now - timedelta(days=300)
    hot_mock = AsyncMock(return_value=_hot_df())
    cold_mock = AsyncMock(return_value=_cold_df())

    with ExitStack() as stack:
        _enter_tier_mocks(
            stack,
            query_enabled=False,
            threshold_days=180,
            hot_mock=hot_mock,
            cold_mock=cold_mock,
        )
        df = await fetch_events_split("SIG_001", start, end, **_SPLIT_KWARGS)

    assert hot_mock.await_count == 1
    assert cold_mock.await_count == 0
    assert len(df) == len(_hot_df())


# ---------------------------------------------------------------------------
# fetch_events — where_sql_fragment kwarg (SDK-level extension)
#
# These tests target an SDK-level extension that lifts the
# ``where_sql_fragment`` kwarg from ``ColdTierQuery.fetch_events`` onto the
# tier-aware ``fetch_events`` in ``tsigma/reports/sdk/queries.py``. Stage 2a
# (preemption_recovery) needs an OR-predicate that ``event_codes IN (...)``
# cannot express; the kwarg must propagate to both hot and cold paths.
# ---------------------------------------------------------------------------

_OR_FRAGMENT = "event_code = 104 OR (event_code = 1 AND event_param = 2)"


def _compile_stmt(stmt) -> str:
    """Render a SQLAlchemy stmt to its compiled SQL string with literals inlined."""
    return str(stmt.compile(compile_kwargs={"literal_binds": True}))


@pytest.mark.asyncio
async def test_fetch_events_cold_only_passes_where_sql_fragment():
    """Cold-only window with fragment → ColdTierQuery.fetch_events receives
    the fragment via the where_sql_fragment kwarg (and event_codes is None)."""
    now = _utcnow_naive()
    start = now - timedelta(days=400)
    end = now - timedelta(days=300)
    hot_mock = AsyncMock(return_value=_hot_df())
    cold_mock = AsyncMock(return_value=_cold_df())

    with ExitStack() as stack:
        _enter_tier_mocks(
            stack,
            query_enabled=True,
            threshold_days=180,
            hot_mock=hot_mock,
            cold_mock=cold_mock,
        )
        await fetch_events(
            "SIG_001",
            start,
            end,
            event_codes=None,
            where_sql_fragment=_OR_FRAGMENT,
        )

    assert hot_mock.await_count == 0
    assert cold_mock.await_count == 1
    cold_call = cold_mock.await_args
    assert "where_sql_fragment" in cold_call.kwargs
    assert cold_call.kwargs["where_sql_fragment"] == _OR_FRAGMENT


@pytest.mark.asyncio
async def test_fetch_events_hot_only_includes_where_sql_fragment_in_query():
    """Hot-only window with fragment → the fragment text appears in the
    compiled SQL of the SQLAlchemy stmt passed to db_facade.get_dataframe."""
    now = _utcnow_naive()
    start = now - timedelta(days=5)
    end = now - timedelta(days=1)
    hot_mock = AsyncMock(return_value=_hot_df())
    cold_mock = AsyncMock(return_value=_cold_df())

    with ExitStack() as stack:
        _enter_tier_mocks(
            stack,
            query_enabled=True,
            threshold_days=180,
            hot_mock=hot_mock,
            cold_mock=cold_mock,
        )
        await fetch_events(
            "SIG_001",
            start,
            end,
            event_codes=None,
            where_sql_fragment=_OR_FRAGMENT,
        )

    assert hot_mock.await_count == 1
    assert cold_mock.await_count == 0
    # Capture the stmt that was passed and verify the fragment is in the
    # compiled WHERE clause. The hot path is the only positional arg.
    hot_call = hot_mock.await_args
    stmt = hot_call.args[0]
    compiled = _compile_stmt(stmt)
    assert "event_code = 104" in compiled
    assert "event_code = 1 AND event_param = 2" in compiled


@pytest.mark.asyncio
async def test_fetch_events_spanning_passes_fragment_to_both_halves():
    """Spanning window with fragment → both hot and cold paths run and both
    receive the fragment."""
    now = _utcnow_naive()
    start = now - timedelta(days=400)
    end = now - timedelta(days=1)
    hot_mock = AsyncMock(return_value=_hot_df())
    cold_mock = AsyncMock(return_value=_cold_df())

    with ExitStack() as stack:
        _enter_tier_mocks(
            stack,
            query_enabled=True,
            threshold_days=180,
            hot_mock=hot_mock,
            cold_mock=cold_mock,
        )
        await fetch_events(
            "SIG_001",
            start,
            end,
            event_codes=None,
            where_sql_fragment=_OR_FRAGMENT,
        )

    assert hot_mock.await_count == 1
    assert cold_mock.await_count == 1

    # Cold path: kwarg propagated.
    cold_call = cold_mock.await_args
    assert cold_call.kwargs.get("where_sql_fragment") == _OR_FRAGMENT

    # Hot path: fragment text rendered into the compiled SQL.
    hot_call = hot_mock.await_args
    stmt = hot_call.args[0]
    compiled = _compile_stmt(stmt)
    assert "event_code = 104" in compiled
    assert "event_code = 1 AND event_param = 2" in compiled


@pytest.mark.asyncio
async def test_fetch_events_rejects_event_codes_and_fragment_together():
    """Mirroring ColdTierQuery.fetch_events: passing BOTH event_codes and
    where_sql_fragment is ambiguous and must raise ValueError."""
    now = _utcnow_naive()
    start = now - timedelta(days=5)
    end = now - timedelta(days=1)
    hot_mock = AsyncMock(return_value=_hot_df())
    cold_mock = AsyncMock(return_value=_cold_df())

    with ExitStack() as stack:
        _enter_tier_mocks(
            stack,
            query_enabled=True,
            threshold_days=180,
            hot_mock=hot_mock,
            cold_mock=cold_mock,
        )
        with pytest.raises(ValueError, match="at most one"):
            await fetch_events(
                "SIG_001",
                start,
                end,
                event_codes=[1],
                where_sql_fragment="event_code = 2",
            )


@pytest.mark.asyncio
async def test_fetch_events_no_fragment_unchanged_behavior():
    """Default behavior sanity: explicitly passing where_sql_fragment=None
    must not change routing or the cold-side call shape — cold receives
    event_codes via positional/keyword as before, and the fragment kwarg is
    either absent or None (NOT a stray string).
    """
    now = _utcnow_naive()
    start = now - timedelta(days=400)
    end = now - timedelta(days=300)
    hot_mock = AsyncMock(return_value=_hot_df())
    cold_mock = AsyncMock(return_value=_cold_df())

    with ExitStack() as stack:
        _enter_tier_mocks(
            stack,
            query_enabled=True,
            threshold_days=180,
            hot_mock=hot_mock,
            cold_mock=cold_mock,
        )
        await fetch_events(
            "SIG_001",
            start,
            end,
            event_codes=[1],
            where_sql_fragment=None,
        )

    assert hot_mock.await_count == 0
    assert cold_mock.await_count == 1
    cold_call = cold_mock.await_args
    # If the kwarg is forwarded, it must be None (not a string). If it's
    # not forwarded at all, that's also fine — the cold default is None.
    fragment_kwarg = cold_call.kwargs.get("where_sql_fragment", None)
    assert fragment_kwarg is None


@pytest.mark.asyncio
async def test_fetch_events_tier_disabled_with_fragment():
    """cold_tier.query_enabled=false → hot-only path even for old windows,
    and the fragment must still be applied to the hot query."""
    now = _utcnow_naive()
    start = now - timedelta(days=400)
    end = now - timedelta(days=300)
    hot_mock = AsyncMock(return_value=_hot_df())
    cold_mock = AsyncMock(return_value=_cold_df())

    with ExitStack() as stack:
        _enter_tier_mocks(
            stack,
            query_enabled=False,
            threshold_days=180,
            hot_mock=hot_mock,
            cold_mock=cold_mock,
        )
        await fetch_events(
            "SIG_001",
            start,
            end,
            event_codes=None,
            where_sql_fragment=_OR_FRAGMENT,
        )

    assert hot_mock.await_count == 1
    assert cold_mock.await_count == 0
    hot_call = hot_mock.await_args
    stmt = hot_call.args[0]
    compiled = _compile_stmt(stmt)
    assert "event_code = 104" in compiled
    assert "event_code = 1 AND event_param = 2" in compiled
