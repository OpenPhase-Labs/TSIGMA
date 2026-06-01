"""ColdTierQuery tests against the S3Backend via moto's threaded server.

We use `moto.server.ThreadedMotoServer` rather than the `@mock_aws` decorator
because `aiobotocore` (which `S3Backend` uses) does not compose with moto's
in-process monkey-patch — async response bodies come back as plain bytes
and `aiobotocore` tries to `await` them. Running moto as a real HTTP server
and pointing `S3Backend.endpoint_url` at it bypasses that path entirely and
exercises the actual aiobotocore call stack.
"""

import tempfile
from datetime import date, datetime

import boto3
import duckdb
import pandas as pd
import pytest
import pytest_asyncio
from moto.server import ThreadedMotoServer

from tsigma.database.cold_tier import ColdPartition, ColdTierQuery
from tsigma.storage.s3 import S3Backend

_BUCKET = "test-cold-tier"
_REGION = "us-east-1"


def _put_partition(s3_client, bucket: str, signal_id: str, d: str) -> None:
    """Upload one fixture parquet to s3://{bucket}/{signal_id}/{d}/events.parquet.

    Each partition holds three rows at 08:00, 10:00, 14:00 with
    event_code/event_param values [(1, 0), (10, 1), (100, 2)] — matches
    the filesystem fixture so tests stay in sync.
    """
    df = pd.DataFrame(
        {
            "signal_id": [signal_id, signal_id, signal_id],
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
    with tempfile.NamedTemporaryFile(suffix=".parquet") as tf:
        duckdb.from_df(df).write_parquet(tf.name)
        body = open(tf.name, "rb").read()
    s3_client.put_object(
        Bucket=bucket,
        Key=f"{signal_id}/{d}/events.parquet",
        Body=body,
    )


@pytest.fixture
def moto_endpoint():
    """Start a ThreadedMotoServer and seed it with the cold-tree partitions."""
    server = ThreadedMotoServer(ip_address="127.0.0.1", port=0)
    server.start()
    _, port = server._server.server_address
    endpoint_url = f"http://127.0.0.1:{port}"

    client = boto3.client(
        "s3",
        region_name=_REGION,
        endpoint_url=endpoint_url,
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
    )
    client.create_bucket(Bucket=_BUCKET)
    structure = {
        "SIG_001": ["2025-01-01", "2025-01-02", "2025-06-15"],
        "SIG_002": ["2025-06-15"],
    }
    for sig, dates in structure.items():
        for d in dates:
            _put_partition(client, _BUCKET, sig, d)

    yield endpoint_url

    server.stop()


@pytest_asyncio.fixture
async def s3_cold(moto_endpoint: str):
    """Wrap the moto endpoint in an S3Backend + ColdTierQuery for tests."""
    backend = S3Backend(
        bucket=_BUCKET,
        region=_REGION,
        endpoint_url=moto_endpoint,
        access_key="testing",
        secret_key="testing",
    )
    try:
        yield ColdTierQuery(backend)
    finally:
        await backend.close()


@pytest.mark.asyncio
async def test_list_partitions_for_signal(s3_cold: ColdTierQuery) -> None:
    partitions = await s3_cold.list_partitions(signal_id="SIG_001")
    assert [p.event_date for p in partitions] == [
        date(2025, 1, 1),
        date(2025, 1, 2),
        date(2025, 6, 15),
    ]
    assert all(isinstance(p, ColdPartition) for p in partitions)
    assert all(p.signal_id == "SIG_001" for p in partitions)


@pytest.mark.asyncio
async def test_list_partitions_filters_by_date_range(
    s3_cold: ColdTierQuery,
) -> None:
    partitions = await s3_cold.list_partitions(
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
    s3_cold: ColdTierQuery,
) -> None:
    partitions = await s3_cold.list_partitions(signal_id="SIG_NOPE")
    assert partitions == []


# ---------------------------------------------------------------------------
# fetch_events
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_events_filters_by_code(s3_cold: ColdTierQuery) -> None:
    df = await s3_cold.fetch_events(
        signal_id="SIG_001",
        start=datetime(2025, 1, 1),
        end=datetime(2025, 12, 31, 23, 59, 59),
        event_codes=[1],
    )
    assert list(df.columns) == ["event_code", "event_param", "event_time"]
    assert len(df) == 3
    assert set(df["event_code"].tolist()) == {1}
    times = df["event_time"].tolist()
    assert times == sorted(times)


@pytest.mark.asyncio
async def test_fetch_events_filters_by_time_window(s3_cold: ColdTierQuery) -> None:
    df = await s3_cold.fetch_events(
        signal_id="SIG_001",
        start=datetime(2025, 1, 1, 9, 0, 0),
        end=datetime(2025, 1, 2, 23, 59, 59),
        event_codes=[1, 10, 100],
    )
    assert len(df) == 5
    times = df["event_time"].tolist()
    assert times == sorted(times)


@pytest.mark.asyncio
async def test_fetch_events_filters_by_event_param(s3_cold: ColdTierQuery) -> None:
    df = await s3_cold.fetch_events(
        signal_id="SIG_001",
        start=datetime(2025, 1, 1),
        end=datetime(2025, 12, 31, 23, 59, 59),
        event_codes=[1, 10, 100],
        event_param_in=[0, 1],
    )
    assert len(df) == 6
    assert set(df["event_param"].tolist()) == {0, 1}


@pytest.mark.asyncio
async def test_fetch_events_empty_when_no_partitions(s3_cold: ColdTierQuery) -> None:
    df = await s3_cold.fetch_events(
        signal_id="SIG_NOPE",
        start=datetime(2025, 1, 1),
        end=datetime(2025, 12, 31),
        event_codes=[1],
    )
    assert list(df.columns) == ["event_code", "event_param", "event_time"]
    assert len(df) == 0


@pytest.mark.asyncio
async def test_fetch_events_empty_when_no_rows_match(s3_cold: ColdTierQuery) -> None:
    df = await s3_cold.fetch_events(
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
async def test_aggregate_count_scalar(s3_cold: ColdTierQuery) -> None:
    df = await s3_cold.aggregate_events(
        signal_id_or_ids="SIG_001",
        start=datetime(2025, 1, 1),
        end=datetime(2025, 12, 31, 23, 59, 59),
        agg=[("count", "*")],
    )
    assert list(df.columns) == ["agg_0"]
    assert len(df) == 1
    assert df["agg_0"].iloc[0] == 9


@pytest.mark.asyncio
async def test_aggregate_count_if_with_expr(s3_cold: ColdTierQuery) -> None:
    df = await s3_cold.aggregate_events(
        signal_id_or_ids="SIG_001",
        start=datetime(2025, 1, 1),
        end=datetime(2025, 12, 31, 23, 59, 59),
        agg=[("count_if", "event_code = 1")],
    )
    assert list(df.columns) == ["agg_0"]
    assert df["agg_0"].iloc[0] == 3


@pytest.mark.asyncio
async def test_aggregate_max_grouped(s3_cold: ColdTierQuery) -> None:
    df = await s3_cold.aggregate_events(
        signal_id_or_ids="SIG_001",
        start=datetime(2025, 1, 1),
        end=datetime(2025, 12, 31, 23, 59, 59),
        agg=[("max", "event_param")],
        group_by=["event_code"],
    )
    assert list(df.columns) == ["event_code", "agg_0"]
    df_sorted = df.sort_values("event_code").reset_index(drop=True)
    assert df_sorted["event_code"].tolist() == [1, 10, 100]
    assert df_sorted["agg_0"].tolist() == [0, 1, 2]


@pytest.mark.asyncio
async def test_aggregate_multi_spec_in_one_call(s3_cold: ColdTierQuery) -> None:
    df = await s3_cold.aggregate_events(
        signal_id_or_ids="SIG_001",
        start=datetime(2025, 1, 1),
        end=datetime(2025, 12, 31, 23, 59, 59),
        agg=[("count", "*"), ("max", "event_param")],
    )
    assert list(df.columns) == ["agg_0", "agg_1"]
    assert df["agg_0"].iloc[0] == 9
    assert df["agg_1"].iloc[0] == 2


@pytest.mark.asyncio
async def test_aggregate_with_filters(s3_cold: ColdTierQuery) -> None:
    df = await s3_cold.aggregate_events(
        signal_id_or_ids="SIG_001",
        start=datetime(2025, 1, 1),
        end=datetime(2025, 12, 31, 23, 59, 59),
        agg=[("count", "*")],
        filters={"event_code": [1, 10]},
    )
    assert df["agg_0"].iloc[0] == 6


@pytest.mark.asyncio
async def test_aggregate_multi_signal(s3_cold: ColdTierQuery) -> None:
    df = await s3_cold.aggregate_events(
        signal_id_or_ids=["SIG_001", "SIG_002"],
        start=datetime(2025, 1, 1),
        end=datetime(2025, 12, 31, 23, 59, 59),
        agg=[("count", "*")],
    )
    assert df["agg_0"].iloc[0] == 12


@pytest.mark.asyncio
async def test_aggregate_empty(s3_cold: ColdTierQuery) -> None:
    df = await s3_cold.aggregate_events(
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
    s3_cold: ColdTierQuery,
) -> None:
    """signal_id_or_ids="All" → aggregate across every signal in cold storage.

    Fixture contains SIG_001 (3 partitions × 3 rows = 9) and SIG_002
    (1 partition × 3 rows = 3). Grouped by signal_id with COUNT(*) we expect
    one row per signal with the correct count.
    """
    df = await s3_cold.aggregate_events(
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
    moto_endpoint: str,
) -> None:
    """signal_id_or_ids="All" against an empty bucket → empty DataFrame with
    the canonical column shape."""
    # Use a separate empty bucket against the same moto server.
    empty_bucket = "test-cold-tier-empty"
    boto_client = boto3.client(
        "s3",
        region_name=_REGION,
        endpoint_url=moto_endpoint,
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
    )
    boto_client.create_bucket(Bucket=empty_bucket)

    backend = S3Backend(
        bucket=empty_bucket,
        region=_REGION,
        endpoint_url=moto_endpoint,
        access_key="testing",
        secret_key="testing",
    )
    try:
        cold = ColdTierQuery(backend)
        df = await cold.aggregate_events(
            signal_id_or_ids="All",
            start=datetime(2025, 1, 1),
            end=datetime(2025, 12, 31, 23, 59, 59),
            agg=[("count", "*")],
            group_by=["signal_id"],
        )
    finally:
        await backend.close()

    assert list(df.columns) == ["signal_id", "agg_0"]
    assert len(df) == 0


@pytest.mark.asyncio
async def test_aggregate_events_with_none_signal_id_returns_empty(
    s3_cold: ColdTierQuery,
) -> None:
    """signal_id_or_ids=None → empty DataFrame with the canonical column
    shape, with no partition enumeration. The non-empty S3 fixture
    proves the short-circuit beats any storage scanning — if I/O ran,
    we'd see non-zero rows."""
    df = await s3_cold.aggregate_events(
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
    s3_cold: ColdTierQuery,
) -> None:
    """signal_id_or_ids=[] → empty DataFrame with the canonical column
    shape, with no partition enumeration."""
    df = await s3_cold.aggregate_events(
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
    s3_cold: ColdTierQuery,
) -> None:
    """B8 fragment mode: OR predicate returns the union of two filters in one scan."""
    df = await s3_cold.fetch_events(
        signal_id="SIG_001",
        start=datetime(2025, 1, 1),
        end=datetime(2025, 12, 31, 23, 59, 59),
        where_sql_fragment=(
            "event_code IN (1) "
            "OR (event_code IN (10) AND event_param IN (1))"
        ),
    )
    assert len(df) == 6
    assert set(df["event_code"].tolist()) == {1, 10}
