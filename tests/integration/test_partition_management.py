"""End-to-end coverage of the ``manage_partitions`` scheduler job.

Scoped to MS-SQL / Oracle / MySQL: PostgreSQL deployments use
TimescaleDB chunks (managed by the Timescale background scheduler) and
the job short-circuits there.

For the partitioned dialects we verify:

    * a first invocation extends the rolling window to
      ``1 + partition_lookahead_days / event_log_partition_interval_days``
      partitions per managed table;
    * the job handles both ``controller_event_log`` and ``roadside_event``;
    * a second invocation is a no-op — no new partitions are created
      because the window is already full.

The Oracle branch uses INTERVAL partitioning so
``ensure_partition_sql`` returns an empty statement list; in that case
we assert only that ``manage_partitions`` does not raise and the
existing-partition listing does not shrink.
"""

from __future__ import annotations

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker

from tsigma.config import settings
from tsigma.database.db import DialectHelper
from tsigma.scheduler.jobs.manage_partitions import manage_partitions

pytestmark = pytest.mark.integration


_MANAGED_TABLES = ("controller_event_log", "roadside_event")


async def _count_partitions(session_factory, dialect: DialectHelper, table: str) -> int:
    """Count the dialect-reported partitions for ``table``.

    Returns ``0`` if the dialect does not support partition listing
    (PostgreSQL) so callers can tell "no partitions" from "not
    partitionable" via a separate ``None`` check upstream.
    """
    sql = dialect.list_partitions_sql(table)
    if sql is None:
        return 0
    async with session_factory() as session:
        result = await session.execute(text(sql))
        return len(list(result.all()))


@pytest.fixture
def _db_type_setting(dialect_name: str, monkeypatch):
    """Point ``settings.db_type`` at the dialect the fixture spun up.

    ``manage_partitions`` reads ``settings.db_type`` to decide whether
    to short-circuit (PostgreSQL) or which ``DialectHelper`` to use.
    """
    monkeypatch.setattr(settings, "db_type", dialect_name)


@pytest.mark.asyncio
async def test_manage_partitions_extends_future_window(
    dialect_engine, dialect_name: str, _db_type_setting,
) -> None:
    """One call creates at least ``lookahead / interval + 1`` partitions."""
    if dialect_name == "postgresql":
        pytest.skip("PostgreSQL uses TimescaleDB chunks, not partitions")

    dialect = DialectHelper(dialect_name)
    if dialect.list_partitions_sql("controller_event_log") is None:
        pytest.skip(f"{dialect_name}: no partition introspection available")

    session_factory = async_sessionmaker(bind=dialect_engine, expire_on_commit=False)

    async with session_factory() as session:
        await manage_partitions(session)
        await session.commit()

    count = await _count_partitions(
        session_factory, dialect, "controller_event_log",
    )

    if not dialect.ensure_partition_sql(
        "controller_event_log", __import__("datetime").date.today(), 1,
    ):
        # Oracle's INTERVAL partitioning: auto-creation is driven by
        # inserts, not by the scheduler.  We can only verify the job did
        # not destroy the existing partition set.
        assert count >= 1
        return

    interval = int(settings.event_log_partition_interval_days)
    lookahead = int(settings.partition_lookahead_days)
    expected_minimum = 1 + (lookahead // interval)
    assert count >= expected_minimum, (
        f"{dialect_name}: expected >= {expected_minimum} partitions on "
        f"controller_event_log, got {count}"
    )


@pytest.mark.asyncio
async def test_manage_partitions_handles_roadside_event_too(
    dialect_engine, dialect_name: str, _db_type_setting,
) -> None:
    """Both event tables share the same cadence — both get extended."""
    if dialect_name == "postgresql":
        pytest.skip("PostgreSQL uses TimescaleDB chunks, not partitions")

    dialect = DialectHelper(dialect_name)
    if dialect.list_partitions_sql("roadside_event") is None:
        pytest.skip(f"{dialect_name}: no partition introspection available")

    session_factory = async_sessionmaker(bind=dialect_engine, expire_on_commit=False)

    async with session_factory() as session:
        await manage_partitions(session)
        await session.commit()

    count = await _count_partitions(session_factory, dialect, "roadside_event")

    if not dialect.ensure_partition_sql(
        "roadside_event", __import__("datetime").date.today(), 1,
    ):
        assert count >= 1
        return

    interval = int(settings.event_log_partition_interval_days)
    lookahead = int(settings.partition_lookahead_days)
    expected_minimum = 1 + (lookahead // interval)
    assert count >= expected_minimum, (
        f"{dialect_name}: expected >= {expected_minimum} partitions on "
        f"roadside_event, got {count}"
    )


@pytest.mark.asyncio
async def test_manage_partitions_second_call_is_noop(
    dialect_engine, dialect_name: str, _db_type_setting,
) -> None:
    """Calling twice does not grow the partition set.

    Once the rolling window is full, a follow-up call has nothing to
    add — any growth would indicate a deduplication bug in
    ``_ensure_future_partitions``.
    """
    if dialect_name == "postgresql":
        pytest.skip("PostgreSQL uses TimescaleDB chunks, not partitions")

    dialect = DialectHelper(dialect_name)
    if dialect.list_partitions_sql("controller_event_log") is None:
        pytest.skip(f"{dialect_name}: no partition introspection available")

    session_factory = async_sessionmaker(bind=dialect_engine, expire_on_commit=False)

    async with session_factory() as session:
        await manage_partitions(session)
        await session.commit()
    first = await _count_partitions(
        session_factory, dialect, "controller_event_log",
    )

    async with session_factory() as session:
        await manage_partitions(session)
        await session.commit()
    second = await _count_partitions(
        session_factory, dialect, "controller_event_log",
    )

    assert second == first, (
        f"{dialect_name}: second manage_partitions call grew the window "
        f"from {first} to {second}"
    )
