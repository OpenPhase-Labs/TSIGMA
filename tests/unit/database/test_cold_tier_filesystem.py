"""ColdTierQuery tests against the FilesystemBackend."""

from datetime import date, datetime
from pathlib import Path

import pandas as pd
import pytest

from tsigma.database.cold_tier import ColdPartition, ColdTierQuery
from tsigma.storage.filesystem import FilesystemBackend


@pytest.fixture
def cold_tree(tmp_path: Path) -> Path:
    """Build a fixture cold tree:

        {tmp_path}/SIG_001/2025-01-01/events.parquet   (3 rows)
        {tmp_path}/SIG_001/2025-01-02/events.parquet   (3 rows)
        {tmp_path}/SIG_001/2025-06-15/events.parquet   (3 rows)
        {tmp_path}/SIG_002/2025-06-15/events.parquet   (3 rows)

    Each partition holds three rows at 08:00, 10:00, 14:00 with
    event_code/event_param values [(1, 0), (10, 1), (100, 2)].
    """
    structure = {
        "SIG_001": ["2025-01-01", "2025-01-02", "2025-06-15"],
        "SIG_002": ["2025-06-15"],
    }
    for sig, dates in structure.items():
        for d in dates:
            part_dir = tmp_path / sig / d
            part_dir.mkdir(parents=True)
            df = pd.DataFrame(
                {
                    "signal_id": [sig, sig, sig],
                    "event_time": [
                        pd.Timestamp(f"{d} 08:00:00"),
                        pd.Timestamp(f"{d} 10:00:00"),
                        pd.Timestamp(f"{d} 14:00:00"),
                    ],
                    "event_code": [1, 10, 100],
                    "event_param": [0, 1, 2],
                    "device_id": ["dev1", "dev1", "dev1"],
                }
            )
            df.to_parquet(part_dir / "events.parquet", index=False)
    return tmp_path


@pytest.fixture
def cold(cold_tree: Path) -> ColdTierQuery:
    return ColdTierQuery(FilesystemBackend(base_path=str(cold_tree)))


@pytest.mark.asyncio
async def test_list_partitions_for_signal(cold: ColdTierQuery) -> None:
    partitions = await cold.list_partitions(signal_id="SIG_001")
    assert [p.event_date for p in partitions] == [
        date(2025, 1, 1),
        date(2025, 1, 2),
        date(2025, 6, 15),
    ]
    assert all(isinstance(p, ColdPartition) for p in partitions)
    assert all(p.signal_id == "SIG_001" for p in partitions)


@pytest.mark.asyncio
async def test_list_partitions_filters_by_date_range(cold: ColdTierQuery) -> None:
    partitions = await cold.list_partitions(
        signal_id="SIG_001",
        start_date=date(2025, 1, 2),
        end_date=date(2025, 6, 15),
    )
    assert [p.event_date for p in partitions] == [
        date(2025, 1, 2),
        date(2025, 6, 15),
    ]


@pytest.mark.asyncio
async def test_list_partitions_for_missing_signal_returns_empty(
    cold: ColdTierQuery,
) -> None:
    partitions = await cold.list_partitions(signal_id="SIG_NOPE")
    assert partitions == []


# ---------------------------------------------------------------------------
# fetch_events
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_events_filters_by_code(cold: ColdTierQuery) -> None:
    df = await cold.fetch_events(
        signal_id="SIG_001",
        start=datetime(2025, 1, 1),
        end=datetime(2025, 12, 31, 23, 59, 59),
        event_codes=[1],
    )
    assert list(df.columns) == ["event_code", "event_param", "event_time"]
    # 3 partitions × 1 matching row (code=1) per partition
    assert len(df) == 3
    assert set(df["event_code"].tolist()) == {1}
    # Sorted by event_time ascending
    times = df["event_time"].tolist()
    assert times == sorted(times)


@pytest.mark.asyncio
async def test_fetch_events_filters_by_time_window(cold: ColdTierQuery) -> None:
    df = await cold.fetch_events(
        signal_id="SIG_001",
        start=datetime(2025, 1, 1, 9, 0, 0),
        end=datetime(2025, 1, 2, 23, 59, 59),
        event_codes=[1, 10, 100],
    )
    # 01-01 partition: 10:00 and 14:00 rows (08:00 excluded), 01-02 partition:
    # all 3 rows. 06-15 partition excluded by end bound.
    assert len(df) == 5
    times = df["event_time"].tolist()
    assert times == sorted(times)


@pytest.mark.asyncio
async def test_fetch_events_filters_by_event_param(cold: ColdTierQuery) -> None:
    df = await cold.fetch_events(
        signal_id="SIG_001",
        start=datetime(2025, 1, 1),
        end=datetime(2025, 12, 31, 23, 59, 59),
        event_codes=[1, 10, 100],
        event_param_in=[0, 1],
    )
    # 3 partitions × 2 matching rows (params 0, 1; param 2 excluded) = 6
    assert len(df) == 6
    assert set(df["event_param"].tolist()) == {0, 1}


@pytest.mark.asyncio
async def test_fetch_events_empty_when_no_partitions(cold: ColdTierQuery) -> None:
    df = await cold.fetch_events(
        signal_id="SIG_NOPE",
        start=datetime(2025, 1, 1),
        end=datetime(2025, 12, 31),
        event_codes=[1],
    )
    assert list(df.columns) == ["event_code", "event_param", "event_time"]
    assert len(df) == 0


@pytest.mark.asyncio
async def test_fetch_events_empty_when_no_rows_match(cold: ColdTierQuery) -> None:
    df = await cold.fetch_events(
        signal_id="SIG_001",
        start=datetime(2026, 1, 1),
        end=datetime(2026, 12, 31),
        event_codes=[1, 10, 100],
    )
    assert list(df.columns) == ["event_code", "event_param", "event_time"]
    assert len(df) == 0


# ---------------------------------------------------------------------------
# aggregate_events
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_aggregate_count_scalar(cold: ColdTierQuery) -> None:
    df = await cold.aggregate_events(
        signal_id_or_ids="SIG_001",
        start=datetime(2025, 1, 1),
        end=datetime(2025, 12, 31, 23, 59, 59),
        agg=[("count", "*")],
    )
    assert list(df.columns) == ["agg_0"]
    assert len(df) == 1
    # 3 partitions × 3 rows each
    assert df["agg_0"].iloc[0] == 9


@pytest.mark.asyncio
async def test_aggregate_count_if_with_expr(cold: ColdTierQuery) -> None:
    df = await cold.aggregate_events(
        signal_id_or_ids="SIG_001",
        start=datetime(2025, 1, 1),
        end=datetime(2025, 12, 31, 23, 59, 59),
        agg=[("count_if", "event_code = 1")],
    )
    assert list(df.columns) == ["agg_0"]
    # One matching row per partition (code=1)
    assert df["agg_0"].iloc[0] == 3


@pytest.mark.asyncio
async def test_aggregate_max_grouped(cold: ColdTierQuery) -> None:
    df = await cold.aggregate_events(
        signal_id_or_ids="SIG_001",
        start=datetime(2025, 1, 1),
        end=datetime(2025, 12, 31, 23, 59, 59),
        agg=[("max", "event_param")],
        group_by=["event_code"],
    )
    assert list(df.columns) == ["event_code", "agg_0"]
    # 3 distinct codes (1, 10, 100); MAX(event_param) per code: 0, 1, 2
    df_sorted = df.sort_values("event_code").reset_index(drop=True)
    assert df_sorted["event_code"].tolist() == [1, 10, 100]
    assert df_sorted["agg_0"].tolist() == [0, 1, 2]


@pytest.mark.asyncio
async def test_aggregate_multi_spec_in_one_call(cold: ColdTierQuery) -> None:
    df = await cold.aggregate_events(
        signal_id_or_ids="SIG_001",
        start=datetime(2025, 1, 1),
        end=datetime(2025, 12, 31, 23, 59, 59),
        agg=[("count", "*"), ("max", "event_param")],
    )
    assert list(df.columns) == ["agg_0", "agg_1"]
    assert df["agg_0"].iloc[0] == 9
    assert df["agg_1"].iloc[0] == 2


@pytest.mark.asyncio
async def test_aggregate_with_filters(cold: ColdTierQuery) -> None:
    df = await cold.aggregate_events(
        signal_id_or_ids="SIG_001",
        start=datetime(2025, 1, 1),
        end=datetime(2025, 12, 31, 23, 59, 59),
        agg=[("count", "*")],
        filters={"event_code": [1, 10]},
    )
    # 3 partitions × 2 matching codes = 6 rows
    assert df["agg_0"].iloc[0] == 6


@pytest.mark.asyncio
async def test_aggregate_multi_signal(cold: ColdTierQuery) -> None:
    df = await cold.aggregate_events(
        signal_id_or_ids=["SIG_001", "SIG_002"],
        start=datetime(2025, 1, 1),
        end=datetime(2025, 12, 31, 23, 59, 59),
        agg=[("count", "*")],
    )
    # SIG_001: 9 rows, SIG_002: 3 rows = 12
    assert df["agg_0"].iloc[0] == 12


@pytest.mark.asyncio
async def test_aggregate_empty(cold: ColdTierQuery) -> None:
    df = await cold.aggregate_events(
        signal_id_or_ids="SIG_NOPE",
        start=datetime(2025, 1, 1),
        end=datetime(2025, 12, 31, 23, 59, 59),
        agg=[("count", "*")],
        group_by=["event_code"],
    )
    assert list(df.columns) == ["event_code", "agg_0"]
    assert len(df) == 0


@pytest.mark.asyncio
async def test_aggregate_events_with_all_signal_id_aggregates_all_signals(
    cold: ColdTierQuery,
) -> None:
    """signal_id_or_ids="All" → aggregate across every signal in cold storage.

    Fixture contains SIG_001 (3 partitions × 3 rows = 9) and SIG_002
    (1 partition × 3 rows = 3). Grouped by signal_id with COUNT(*) we expect
    one row per signal with the correct count.
    """
    df = await cold.aggregate_events(
        signal_id_or_ids="All",
        start=datetime(2025, 1, 1),
        end=datetime(2025, 12, 31, 23, 59, 59),
        agg=[("count", "*")],
        group_by=["signal_id"],
    )
    assert list(df.columns) == ["signal_id", "agg_0"]
    df_sorted = df.sort_values("signal_id").reset_index(drop=True)
    assert df_sorted["signal_id"].tolist() == ["SIG_001", "SIG_002"]
    assert df_sorted["agg_0"].tolist() == [9, 3]


@pytest.mark.asyncio
async def test_aggregate_events_with_all_signal_id_no_signals_returns_empty(
    tmp_path: Path,
) -> None:
    """signal_id_or_ids="All" against an empty cold storage tree → empty
    DataFrame with the canonical column shape."""
    empty_cold = ColdTierQuery(FilesystemBackend(base_path=str(tmp_path)))
    df = await empty_cold.aggregate_events(
        signal_id_or_ids="All",
        start=datetime(2025, 1, 1),
        end=datetime(2025, 12, 31, 23, 59, 59),
        agg=[("count", "*")],
        group_by=["signal_id"],
    )
    assert list(df.columns) == ["signal_id", "agg_0"]
    assert len(df) == 0


@pytest.mark.asyncio
async def test_aggregate_events_with_none_signal_id_returns_empty(
    cold: ColdTierQuery,
) -> None:
    """signal_id_or_ids=None → empty DataFrame with the canonical column
    shape, with no partition enumeration. The non-empty cold fixture
    proves the short-circuit beats any storage scanning — if I/O ran,
    we'd see non-zero rows."""
    df = await cold.aggregate_events(
        signal_id_or_ids=None,
        start=datetime(2025, 1, 1),
        end=datetime(2025, 12, 31, 23, 59, 59),
        agg=[("count", "*")],
        group_by=[],
    )
    assert list(df.columns) == ["agg_0"]
    assert len(df) == 0


@pytest.mark.asyncio
async def test_aggregate_events_with_empty_list_signal_ids_returns_empty(
    cold: ColdTierQuery,
) -> None:
    """signal_id_or_ids=[] → empty DataFrame with the canonical column
    shape, with no partition enumeration."""
    df = await cold.aggregate_events(
        signal_id_or_ids=[],
        start=datetime(2025, 1, 1),
        end=datetime(2025, 12, 31, 23, 59, 59),
        agg=[("count", "*")],
        group_by=["event_code"],
    )
    assert list(df.columns) == ["event_code", "agg_0"]
    assert len(df) == 0


# ---------------------------------------------------------------------------
# fetch_events — where_sql_fragment mode (B8)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_events_with_where_sql_fragment_or_predicate(
    cold: ColdTierQuery,
) -> None:
    """B8 fragment mode: OR predicate returns the union of two filters in one scan."""
    df = await cold.fetch_events(
        signal_id="SIG_001",
        start=datetime(2025, 1, 1),
        end=datetime(2025, 12, 31, 23, 59, 59),
        where_sql_fragment=(
            "event_code IN (1) "
            "OR (event_code IN (10) AND event_param IN (1))"
        ),
    )
    # SIG_001 has 3 partitions; each partition contains one row matching
    # code=1 and one row matching code=10/param=1. Union = 6 rows.
    assert len(df) == 6
    assert set(df["event_code"].tolist()) == {1, 10}


@pytest.mark.asyncio
async def test_fetch_events_no_predicate_returns_all_in_window(
    cold: ColdTierQuery,
) -> None:
    """B9 third mode: passing neither event_codes nor where_sql_fragment
    returns every row in the time window."""
    df = await cold.fetch_events(
        signal_id="SIG_001",
        start=datetime(2025, 1, 1),
        end=datetime(2025, 12, 31, 23, 59, 59),
    )
    # SIG_001 has 3 partitions × 3 rows each = 9 total, all returned.
    assert len(df) == 9
    assert set(df["event_code"].tolist()) == {1, 10, 100}


@pytest.mark.asyncio
async def test_fetch_events_rejects_both_predicates(cold: ColdTierQuery) -> None:
    """B9 contract: passing both event_codes and where_sql_fragment is ambiguous."""
    with pytest.raises(ValueError, match="at most one"):
        await cold.fetch_events(
            signal_id="SIG_001",
            start=datetime(2025, 1, 1),
            end=datetime(2025, 12, 31, 23, 59, 59),
            event_codes=[1],
            where_sql_fragment="event_code IN (1)",
        )
