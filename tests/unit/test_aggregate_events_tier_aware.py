"""Tier-aware routing tests for tsigma.reports.sdk.aggregates.aggregate_events (B10)."""

from contextlib import ExitStack, asynccontextmanager
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest

from tsigma.reports.sdk.aggregates import aggregate_events


def _utcnow_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


@asynccontextmanager
async def _fake_session():
    yield None


def _enter_tier_mocks(
    stack: ExitStack,
    *,
    query_enabled: bool,
    threshold_days: int,
    hot_df: pd.DataFrame,
    cold_df: pd.DataFrame,
) -> dict:
    """Patch all mocks for aggregate_events routing tests. Returns the
    individual mocks so tests can inspect await_count / await_args."""
    fake_facade = MagicMock()
    fake_facade.session = _fake_session
    hot_mock = AsyncMock(return_value=hot_df)
    fake_facade.get_dataframe = hot_mock
    cold_mock = AsyncMock(return_value=cold_df)

    stack.enter_context(
        patch("tsigma.reports.sdk.aggregates.db_facade", new=fake_facade)
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
        patch(
            "tsigma.database.cold_tier.ColdTierQuery.aggregate_events",
            new=cold_mock,
        )
    )
    return {"hot": hot_mock, "cold": cold_mock}


# ---------------------------------------------------------------------------
# Routing
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_all_hot_window_calls_hot_path_only():
    now = _utcnow_naive()
    start = now - timedelta(days=5)
    end = now - timedelta(days=1)

    hot_df = pd.DataFrame([{"agg_0": 42}])
    cold_df = pd.DataFrame([{"agg_0": 0}])

    with ExitStack() as stack:
        m = _enter_tier_mocks(
            stack,
            query_enabled=True,
            threshold_days=180,
            hot_df=hot_df,
            cold_df=cold_df,
        )
        df = await aggregate_events(
            "SIG_001", start, end, agg=[("count", "*")]
        )

    assert m["hot"].await_count == 1
    assert m["cold"].await_count == 0
    assert df["agg_0"].iloc[0] == 42


@pytest.mark.asyncio
async def test_all_cold_window_calls_cold_path_only():
    now = _utcnow_naive()
    start = now - timedelta(days=400)
    end = now - timedelta(days=300)

    hot_df = pd.DataFrame([{"agg_0": 0}])
    cold_df = pd.DataFrame([{"agg_0": 17}])

    with ExitStack() as stack:
        m = _enter_tier_mocks(
            stack,
            query_enabled=True,
            threshold_days=180,
            hot_df=hot_df,
            cold_df=cold_df,
        )
        df = await aggregate_events(
            "SIG_001", start, end, agg=[("count", "*")]
        )

    assert m["hot"].await_count == 0
    assert m["cold"].await_count == 1
    assert df["agg_0"].iloc[0] == 17


@pytest.mark.asyncio
async def test_cold_disabled_calls_hot_path_only():
    now = _utcnow_naive()
    start = now - timedelta(days=400)
    end = now - timedelta(days=300)

    hot_df = pd.DataFrame([{"agg_0": 3}])
    cold_df = pd.DataFrame([{"agg_0": 99}])

    with ExitStack() as stack:
        m = _enter_tier_mocks(
            stack,
            query_enabled=False,
            threshold_days=180,
            hot_df=hot_df,
            cold_df=cold_df,
        )
        df = await aggregate_events(
            "SIG_001", start, end, agg=[("count", "*")]
        )

    assert m["hot"].await_count == 1
    assert m["cold"].await_count == 0
    assert df["agg_0"].iloc[0] == 3


# ---------------------------------------------------------------------------
# Spanning — cross-tier combinator correctness
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_spanning_scalar_count_sums_across_tiers():
    """Spanning window with scalar count: combinator is sum-of-counts."""
    now = _utcnow_naive()
    start = now - timedelta(days=400)
    end = now - timedelta(days=1)

    hot_df = pd.DataFrame([{"agg_0": 50}])
    cold_df = pd.DataFrame([{"agg_0": 100}])

    with ExitStack() as stack:
        m = _enter_tier_mocks(
            stack,
            query_enabled=True,
            threshold_days=180,
            hot_df=hot_df,
            cold_df=cold_df,
        )
        df = await aggregate_events(
            "SIG_001", start, end, agg=[("count", "*")]
        )

    assert m["hot"].await_count == 1
    assert m["cold"].await_count == 1
    assert df["agg_0"].iloc[0] == 150


@pytest.mark.asyncio
async def test_spanning_grouped_count_merges_group_keys():
    """Spanning grouped: rows for the same group_by key get summed; distinct
    keys land in separate result rows."""
    now = _utcnow_naive()
    start = now - timedelta(days=400)
    end = now - timedelta(days=1)

    # Hot has codes [1, 10]; cold has codes [1, 100]. Merged: 1 sums, 10 and
    # 100 stay distinct.
    hot_df = pd.DataFrame(
        [{"event_code": 1, "agg_0": 5}, {"event_code": 10, "agg_0": 7}]
    )
    cold_df = pd.DataFrame(
        [{"event_code": 1, "agg_0": 3}, {"event_code": 100, "agg_0": 11}]
    )

    with ExitStack() as stack:
        _enter_tier_mocks(
            stack,
            query_enabled=True,
            threshold_days=180,
            hot_df=hot_df,
            cold_df=cold_df,
        )
        df = await aggregate_events(
            "SIG_001",
            start,
            end,
            agg=[("count", "*")],
            group_by=["event_code"],
        )

    df_sorted = df.sort_values("event_code").reset_index(drop=True)
    assert df_sorted["event_code"].tolist() == [1, 10, 100]
    assert df_sorted["agg_0"].tolist() == [8, 7, 11]


@pytest.mark.asyncio
async def test_spanning_max_and_max_if_multi_spec():
    """Spanning multi-spec: max takes the per-tier max, max_if same."""
    now = _utcnow_naive()
    start = now - timedelta(days=400)
    end = now - timedelta(days=1)

    hot_df = pd.DataFrame([{"agg_0": 20, "agg_1": 5}])
    cold_df = pd.DataFrame([{"agg_0": 15, "agg_1": 9}])

    with ExitStack() as stack:
        _enter_tier_mocks(
            stack,
            query_enabled=True,
            threshold_days=180,
            hot_df=hot_df,
            cold_df=cold_df,
        )
        df = await aggregate_events(
            "SIG_001",
            start,
            end,
            agg=[("max", "event_param"), ("max_if", "event_param", "event_code = 1")],
        )

    assert df["agg_0"].iloc[0] == 20   # max across tiers
    assert df["agg_1"].iloc[0] == 9    # max_if across tiers


# ---------------------------------------------------------------------------
# signal_id_or_ids="All" (fleet-wide / all-signals mode) — Stage 2b
#
# These tests target an extension that lets ``aggregate_events`` accept
# ``signal_id_or_ids="All"`` to mean "aggregate over ALL signals in storage."
# Required so the upcoming ``stuck_detectors`` migration (which has a
# fleet-wide scan mode) can move off direct DB queries onto the SDK.
# ---------------------------------------------------------------------------


def _compile_stmt(stmt) -> str:
    """Render a SQLAlchemy stmt to its compiled SQL string with literals inlined."""
    return str(stmt.compile(compile_kwargs={"literal_binds": True}))


@pytest.mark.asyncio
async def test_aggregate_events_all_signal_routes_to_hot_when_disabled():
    """cold_tier.query_enabled=false → hot-only path; "All" signal_id must be
    accepted and forwarded down the hot path."""
    now = _utcnow_naive()
    start = now - timedelta(days=400)
    end = now - timedelta(days=300)

    hot_df = pd.DataFrame([{"agg_0": 7}])
    cold_df = pd.DataFrame([{"agg_0": 99}])

    with ExitStack() as stack:
        m = _enter_tier_mocks(
            stack,
            query_enabled=False,
            threshold_days=180,
            hot_df=hot_df,
            cold_df=cold_df,
        )
        df = await aggregate_events(
            "All", start, end, agg=[("count", "*")]
        )

    assert m["hot"].await_count == 1
    assert m["cold"].await_count == 0
    assert df["agg_0"].iloc[0] == 7


@pytest.mark.asyncio
async def test_aggregate_events_all_signal_all_hot_window():
    """All-hot window with signal_id_or_ids="All" → only hot path called, with "All"."""
    now = _utcnow_naive()
    start = now - timedelta(days=5)
    end = now - timedelta(days=1)

    hot_df = pd.DataFrame([{"agg_0": 11}])
    cold_df = pd.DataFrame([{"agg_0": 0}])

    with ExitStack() as stack:
        m = _enter_tier_mocks(
            stack,
            query_enabled=True,
            threshold_days=180,
            hot_df=hot_df,
            cold_df=cold_df,
        )
        df = await aggregate_events(
            "All", start, end, agg=[("count", "*")]
        )

    assert m["hot"].await_count == 1
    assert m["cold"].await_count == 0
    assert df["agg_0"].iloc[0] == 11


@pytest.mark.asyncio
async def test_aggregate_events_all_signal_all_cold_window():
    """All-cold window with signal_id_or_ids="All" → only cold path called.

    The cold mock receives the "All" signal as its first positional argument
    (matches ColdTierQuery.aggregate_events signature)."""
    now = _utcnow_naive()
    start = now - timedelta(days=400)
    end = now - timedelta(days=300)

    hot_df = pd.DataFrame([{"agg_0": 0}])
    cold_df = pd.DataFrame([{"agg_0": 23}])

    with ExitStack() as stack:
        m = _enter_tier_mocks(
            stack,
            query_enabled=True,
            threshold_days=180,
            hot_df=hot_df,
            cold_df=cold_df,
        )
        df = await aggregate_events(
            "All", start, end, agg=[("count", "*")]
        )

    assert m["hot"].await_count == 0
    assert m["cold"].await_count == 1
    # First positional arg to ColdTierQuery.aggregate_events is signal_id_or_ids.
    cold_call = m["cold"].await_args
    assert cold_call.args[0] == "All"
    assert df["agg_0"].iloc[0] == 23


@pytest.mark.asyncio
async def test_aggregate_events_all_signal_spanning_window():
    """Spanning window with signal_id_or_ids="All" → both halves called with "All",
    cross-tier combinator merges the results."""
    now = _utcnow_naive()
    start = now - timedelta(days=400)
    end = now - timedelta(days=1)

    hot_df = pd.DataFrame([{"agg_0": 30}])
    cold_df = pd.DataFrame([{"agg_0": 70}])

    with ExitStack() as stack:
        m = _enter_tier_mocks(
            stack,
            query_enabled=True,
            threshold_days=180,
            hot_df=hot_df,
            cold_df=cold_df,
        )
        df = await aggregate_events(
            "All", start, end, agg=[("count", "*")]
        )

    assert m["hot"].await_count == 1
    assert m["cold"].await_count == 1
    # Cold path receives "All" as signal_id_or_ids.
    cold_call = m["cold"].await_args
    assert cold_call.args[0] == "All"
    # Combinator: sum-of-counts → 30 + 70 = 100.
    assert df["agg_0"].iloc[0] == 100


@pytest.mark.asyncio
async def test_aggregate_events_hot_path_omits_signal_filter_when_all():
    """Hot-path SQL with signal_id_or_ids="All" must NOT include a
    ``CEL.signal_id IN (...)`` WHERE condition. Verified by inspecting the
    compiled SQLAlchemy stmt passed to db_facade.get_dataframe."""
    now = _utcnow_naive()
    start = now - timedelta(days=5)
    end = now - timedelta(days=1)

    hot_df = pd.DataFrame([{"agg_0": 0}])
    cold_df = pd.DataFrame([{"agg_0": 0}])

    with ExitStack() as stack:
        m = _enter_tier_mocks(
            stack,
            query_enabled=True,
            threshold_days=180,
            hot_df=hot_df,
            cold_df=cold_df,
        )
        await aggregate_events(
            "All", start, end, agg=[("count", "*")]
        )

    assert m["hot"].await_count == 1
    hot_call = m["hot"].await_args
    stmt = hot_call.args[0]
    compiled = _compile_stmt(stmt)
    # No "signal_id IN" predicate — fleet-wide mode means scan every signal.
    assert "signal_id IN" not in compiled
    # Time bounds must still be present (sanity).
    assert "event_time" in compiled


# ---------------------------------------------------------------------------
# signal_id_or_ids=None / [] (empty-result short-circuit)
#
# ``None`` and ``[]`` both mean "no signals requested" — return an empty,
# correctly-shaped DataFrame immediately without any I/O or settings round-
# trip. This is distinct from ``"All"`` which means "every signal in storage."
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_aggregate_events_none_signal_returns_empty():
    """``signal_id_or_ids=None`` → empty DataFrame, no hot/cold dispatch,
    no settings round-trip."""
    now = _utcnow_naive()
    start = now - timedelta(days=5)
    end = now - timedelta(days=1)

    hot_df = pd.DataFrame([{"agg_0": 42}])
    cold_df = pd.DataFrame([{"agg_0": 99}])

    with ExitStack() as stack:
        m = _enter_tier_mocks(
            stack,
            query_enabled=True,
            threshold_days=180,
            hot_df=hot_df,
            cold_df=cold_df,
        )
        # Patch settings_service accessors at module scope so we can verify
        # the short-circuit skipped the settings round-trip entirely.
        bool_mock = AsyncMock(return_value=True)
        int_mock = AsyncMock(return_value=180)
        stack.enter_context(
            patch("tsigma.settings_service.get_bool", new=bool_mock)
        )
        stack.enter_context(
            patch("tsigma.settings_service.get_int", new=int_mock)
        )

        df = await aggregate_events(
            None, start, end, agg=[("count", "*")], group_by=[]
        )

    # Empty-but-shaped DataFrame.
    assert list(df.columns) == ["agg_0"]
    assert len(df) == 0
    # No I/O on either tier.
    assert m["hot"].await_count == 0
    assert m["cold"].await_count == 0
    # No settings round-trip.
    assert bool_mock.await_count == 0
    assert int_mock.await_count == 0


@pytest.mark.asyncio
async def test_aggregate_events_empty_list_signals_returns_empty():
    """``signal_id_or_ids=[]`` → empty DataFrame, no hot/cold dispatch,
    no settings round-trip."""
    now = _utcnow_naive()
    start = now - timedelta(days=5)
    end = now - timedelta(days=1)

    hot_df = pd.DataFrame([{"agg_0": 42}])
    cold_df = pd.DataFrame([{"agg_0": 99}])

    with ExitStack() as stack:
        m = _enter_tier_mocks(
            stack,
            query_enabled=True,
            threshold_days=180,
            hot_df=hot_df,
            cold_df=cold_df,
        )
        bool_mock = AsyncMock(return_value=True)
        int_mock = AsyncMock(return_value=180)
        stack.enter_context(
            patch("tsigma.settings_service.get_bool", new=bool_mock)
        )
        stack.enter_context(
            patch("tsigma.settings_service.get_int", new=int_mock)
        )

        df = await aggregate_events(
            [], start, end, agg=[("count", "*")], group_by=[]
        )

    assert list(df.columns) == ["agg_0"]
    assert len(df) == 0
    assert m["hot"].await_count == 0
    assert m["cold"].await_count == 0
    assert bool_mock.await_count == 0
    assert int_mock.await_count == 0
