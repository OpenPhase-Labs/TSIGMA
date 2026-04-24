"""
Unit tests for the ``manage_partitions`` scheduler job.

The job is a pure SQL orchestrator — it queries existing partitions,
decides what to add or drop, and dispatches the dialect-specific DDL.
These tests mock the session and assert on the SQL that would be
executed, not on any real database.
"""

from datetime import date, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


def _mock_rows(rows: list[tuple[str, str]]) -> MagicMock:
    """Build a MagicMock ``execute`` result whose ``.all()`` returns rows.

    Each row has attributes ``partition_name`` and ``boundary_iso``.
    """
    row_mocks = []
    for name, boundary in rows:
        r = MagicMock()
        r.partition_name = name
        r.boundary_iso = boundary
        row_mocks.append(r)
    result = MagicMock()
    result.all.return_value = row_mocks
    return result


@pytest.mark.asyncio
async def test_skips_postgresql():
    """PostgreSQL uses TimescaleDB — job returns early without touching DB."""
    from tsigma.scheduler.jobs import manage_partitions as mp

    session = AsyncMock()
    with patch.object(mp.settings, "db_type", "postgresql"):
        await mp.manage_partitions(session)
    session.execute.assert_not_called()


@pytest.mark.asyncio
async def test_skips_when_table_not_partitioned():
    """list_partitions returning no rows means the table isn't partitioned yet."""
    from tsigma.scheduler.jobs import manage_partitions as mp

    session = AsyncMock()
    session.execute = AsyncMock(return_value=_mock_rows([]))

    with patch.object(mp, "_MANAGED_TABLES", ("controller_event_log",)), \
         patch.object(mp.settings, "db_type", "mysql"), \
         patch.object(mp.settings, "event_log_partition_interval_days", 1), \
         patch.object(mp.settings, "partition_lookahead_days", 7), \
         patch.object(mp.settings, "partition_retention_days", None):
        await mp.manage_partitions(session)

    # One list query, no ALTER statements
    assert session.execute.call_count == 1


@pytest.mark.asyncio
async def test_creates_missing_future_partitions_mysql():
    """When lookahead extends past existing partitions, new ones are created."""
    from tsigma.scheduler.jobs import manage_partitions as mp

    today = date.today()
    # Existing: only today and tomorrow — lookahead of 3 requires today+2, today+3.
    existing_rows = [
        (mp.DialectHelper("mysql").partition_name(today, 1), "99999999"),
        (mp.DialectHelper("mysql").partition_name(today + timedelta(days=1), 1),
         "99999999"),
    ]

    session = AsyncMock()
    # First call = list_partitions; subsequent calls = ALTER TABLE ADD PARTITION
    session.execute = AsyncMock(side_effect=[
        _mock_rows(existing_rows),
        MagicMock(),  # ADD partition for today+2
        MagicMock(),  # ADD partition for today+3
    ])

    with patch.object(mp, "_MANAGED_TABLES", ("controller_event_log",)), \
         patch.object(mp.settings, "db_type", "mysql"), \
         patch.object(mp.settings, "event_log_partition_interval_days", 1), \
         patch.object(mp.settings, "partition_lookahead_days", 3), \
         patch.object(mp.settings, "partition_retention_days", None):
        await mp.manage_partitions(session)

    # 1 list + 2 creates (today and today+1 already exist)
    assert session.execute.call_count == 3
    # Verify the ALTER statements targeted the missing two days
    add_calls = [c for c in session.execute.call_args_list[1:]]
    sqls = [str(c.args[0]) for c in add_calls]
    assert any((today + timedelta(days=2)).strftime("%Y%m%d") in s for s in sqls)
    assert any((today + timedelta(days=3)).strftime("%Y%m%d") in s for s in sqls)


@pytest.mark.asyncio
async def test_does_not_create_when_all_future_partitions_exist():
    """When existing partitions already cover the lookahead window, no adds."""
    from tsigma.scheduler.jobs import manage_partitions as mp

    today = date.today()
    helper = mp.DialectHelper("mysql")
    existing_rows = [
        (helper.partition_name(today + timedelta(days=i), 1), "99999999")
        for i in range(4)
    ]

    session = AsyncMock()
    session.execute = AsyncMock(return_value=_mock_rows(existing_rows))

    with patch.object(mp, "_MANAGED_TABLES", ("controller_event_log",)), \
         patch.object(mp.settings, "db_type", "mysql"), \
         patch.object(mp.settings, "event_log_partition_interval_days", 1), \
         patch.object(mp.settings, "partition_lookahead_days", 3), \
         patch.object(mp.settings, "partition_retention_days", None):
        await mp.manage_partitions(session)

    # Only the list query; nothing to add
    assert session.execute.call_count == 1


@pytest.mark.asyncio
async def test_drops_old_partitions_when_retention_set_mysql():
    """With retention enabled, partitions older than the cutoff are dropped."""
    from tsigma.scheduler.jobs import manage_partitions as mp

    today = date.today()
    helper = mp.DialectHelper("mysql")

    # One very old partition (45 days past today) and one inside retention.
    old_boundary = today - timedelta(days=45)
    fresh_boundary = today + timedelta(days=1)
    existing_rows = [
        (helper.partition_name(today - timedelta(days=46), 1),
         _mysql_unix_ts(old_boundary)),
        (helper.partition_name(today, 1),
         _mysql_unix_ts(fresh_boundary)),
    ]

    session = AsyncMock()
    # list + 1 create (today+1 missing) + 1 drop (old partition)
    session.execute = AsyncMock(side_effect=[
        _mock_rows(existing_rows),
        MagicMock(),  # ADD for today+1
        MagicMock(),  # DROP old partition
    ])

    with patch.object(mp, "_MANAGED_TABLES", ("controller_event_log",)), \
         patch.object(mp.settings, "db_type", "mysql"), \
         patch.object(mp.settings, "event_log_partition_interval_days", 1), \
         patch.object(mp.settings, "partition_lookahead_days", 1), \
         patch.object(mp.settings, "partition_retention_days", 30):
        await mp.manage_partitions(session)

    sqls = [str(c.args[0]) for c in session.execute.call_args_list]
    assert any("DROP PARTITION" in s for s in sqls)


@pytest.mark.asyncio
async def test_oracle_is_inert_for_ensure_creates_but_can_drop():
    """Oracle INTERVAL partitioning auto-creates partitions — ensure is a no-op."""
    from tsigma.scheduler.jobs import manage_partitions as mp

    # Oracle returns HIGH_VALUE as a TO_DATE literal; keep one recent row so
    # the table is considered partitioned.
    existing_rows = [
        ("P_20260101", "TO_DATE(' 2026-04-20 00:00:00', 'SYYYY-MM-DD HH24:MI:SS')"),
    ]

    session = AsyncMock()
    session.execute = AsyncMock(return_value=_mock_rows(existing_rows))

    with patch.object(mp, "_MANAGED_TABLES", ("controller_event_log",)), \
         patch.object(mp.settings, "db_type", "oracle"), \
         patch.object(mp.settings, "event_log_partition_interval_days", 1), \
         patch.object(mp.settings, "partition_lookahead_days", 5), \
         patch.object(mp.settings, "partition_retention_days", None):
        await mp.manage_partitions(session)

    # Only the list query — Oracle's ensure_partition_sql returns [].
    assert session.execute.call_count == 1


@pytest.mark.asyncio
async def test_manages_both_event_tables_mysql():
    """Both controller_event_log and roadside_event share the same window."""
    from tsigma.scheduler.jobs import manage_partitions as mp

    today = date.today()
    helper = mp.DialectHelper("mysql")
    # Neither table has tomorrow's partition yet.
    existing_rows = [
        (helper.partition_name(today, 1), "99999999"),
    ]

    session = AsyncMock()
    # Sequence: list(cel), create(cel tomorrow), list(re), create(re tomorrow).
    session.execute = AsyncMock(side_effect=[
        _mock_rows(existing_rows),
        MagicMock(),
        _mock_rows(existing_rows),
        MagicMock(),
    ])

    with patch.object(mp.settings, "db_type", "mysql"), \
         patch.object(mp.settings, "event_log_partition_interval_days", 1), \
         patch.object(mp.settings, "partition_lookahead_days", 1), \
         patch.object(mp.settings, "partition_retention_days", None):
        await mp.manage_partitions(session)

    sqls = [str(c.args[0]) for c in session.execute.call_args_list]
    # Both list queries and both ALTERs targeted their own table.
    assert any("controller_event_log" in s and "PARTITIONS" in s for s in sqls)
    assert any("roadside_event" in s and "PARTITIONS" in s for s in sqls)
    assert any(
        "ALTER TABLE controller_event_log ADD PARTITION" in s for s in sqls
    )
    assert any("ALTER TABLE roadside_event ADD PARTITION" in s for s in sqls)


def test_parse_boundary_mysql_unix_timestamp():
    """MySQL boundaries come back as integer UNIX timestamps."""
    from tsigma.scheduler.jobs.manage_partitions import _parse_boundary

    parsed = _parse_boundary(_mysql_unix_ts(date(2026, 4, 23)))
    assert parsed == date(2026, 4, 23)


def test_parse_boundary_mssql_iso_datetime():
    """MS-SQL boundaries come back as ISO-8601 datetime strings."""
    from tsigma.scheduler.jobs.manage_partitions import _parse_boundary

    parsed = _parse_boundary("2026-04-23T00:00:00")
    assert parsed == date(2026, 4, 23)


def test_parse_boundary_oracle_to_date_literal():
    """Oracle high_value is a ``TO_DATE('...')`` literal."""
    from tsigma.scheduler.jobs.manage_partitions import _parse_boundary

    parsed = _parse_boundary(
        "TO_DATE(' 2026-04-23 00:00:00', 'SYYYY-MM-DD HH24:MI:SS')"
    )
    assert parsed == date(2026, 4, 23)


def test_parse_boundary_unparseable_returns_none():
    """Unparseable boundaries return None (caller skips the row)."""
    from tsigma.scheduler.jobs.manage_partitions import _parse_boundary

    assert _parse_boundary("MAXVALUE") is None
    assert _parse_boundary("???") is None


def _mysql_unix_ts(d: date) -> str:
    """Helper: a MySQL-shaped UNIX_TIMESTAMP boundary string."""
    from datetime import datetime, timezone
    dt = datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
    return str(int(dt.timestamp()))
