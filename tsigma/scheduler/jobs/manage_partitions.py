"""
Event-log partition management for MS-SQL / Oracle / MySQL.

PostgreSQL deployments use TimescaleDB (see ``compress_chunks`` + the
``event_log_partition_interval_days`` setting piped into
``create_hypertable``).  For every other dialect the partitioned event
tables must be managed at the schema level and kept fresh: this job
keeps a rolling window of future partitions ahead of ``today`` so
inserts never hit the default range, and optionally drops partitions
older than ``partition_retention_days``.

Two tables share the same cadence:

  - ``controller_event_log`` — Indiana Hi-Res records from the cabinet
    controller.
  - ``roadside_event``       — per-detection records from radar / LiDAR
    / video sensors at the roadway edge.

Both use ``event_log_partition_interval_days`` as their chunk cadence so
a single lookahead/retention policy covers both streams.

Runs once per day at 02:00.  The job is inert on a per-table basis: if
a given table is not partitioned at the schema level yet,
``list_partitions_sql`` returns no rows and that table is skipped while
the other continues.
"""

import logging
from datetime import date, datetime, timedelta, timezone

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from tsigma.config import settings
from tsigma.database.db import DialectHelper
from tsigma.scheduler.registry import JobRegistry

logger = logging.getLogger(__name__)

# Partitioned event-stream tables kept in sync with the rolling window.
# Both use the same chunk cadence (event_log_partition_interval_days).
_MANAGED_TABLES = ("controller_event_log", "roadside_event")


@JobRegistry.register(
    name="manage_partitions",
    trigger="cron",
    hour=2,
    minute=0,
)
async def manage_partitions(session: AsyncSession) -> None:
    """Keep a rolling window of event-log partitions on non-PG dialects."""
    if settings.db_type == "postgresql":
        logger.debug(
            "Skipping manage_partitions — PostgreSQL uses TimescaleDB chunks "
            "(db_type=%s)", settings.db_type,
        )
        return

    dialect = DialectHelper(settings.db_type)
    interval = int(settings.event_log_partition_interval_days)
    lookahead = int(settings.partition_lookahead_days)
    retention = settings.partition_retention_days
    retention_int = int(retention) if retention is not None else None

    total_created = 0
    total_dropped = 0
    for table in _MANAGED_TABLES:
        created, dropped = await _manage_table(
            session, dialect, table, interval, lookahead, retention_int,
        )
        total_created += created
        total_dropped += dropped

    logger.info(
        "manage_partitions complete: %d partition(s) created, %d dropped "
        "across %d table(s) (interval=%dd, lookahead=%dd, retention=%s)",
        total_created, total_dropped, len(_MANAGED_TABLES),
        interval, lookahead,
        f"{retention}d" if retention is not None else "none",
    )


async def _manage_table(
    session: AsyncSession,
    dialect: DialectHelper,
    table: str,
    interval_days: int,
    lookahead_days: int,
    retention_days: int | None,
) -> tuple[int, int]:
    """Extend / prune the rolling window for a single partitioned table.

    Returns ``(created, dropped)``.  Skips the table (returning
    ``(0, 0)``) if the dialect does not expose partition listing or the
    table has not been partitioned yet.
    """
    existing = await _existing_partitions(session, dialect, table)
    if existing is None:
        logger.info(
            "manage_partitions: %s does not support partition listing "
            "(db_type=%s)", table, settings.db_type,
        )
        return 0, 0
    if not existing:
        logger.info(
            "manage_partitions: %s is not partitioned — skipping (enable "
            "partitioning at table creation then re-run)",
            table,
        )
        return 0, 0

    created = await _ensure_future_partitions(
        session, dialect, table, existing, interval_days, lookahead_days,
    )
    dropped = 0
    if retention_days is not None:
        dropped = await _drop_old_partitions(
            session, dialect, table, existing, retention_days,
        )
    return created, dropped


async def _existing_partitions(
    session: AsyncSession, dialect: DialectHelper, table: str,
) -> list[tuple[str, str]] | None:
    """Return ``[(partition_name, boundary_iso), ...]`` or ``None`` if the
    dialect does not expose partition introspection.
    """
    sql = dialect.list_partitions_sql(table)
    if sql is None:
        return None
    result = await session.execute(text(sql))
    return [(str(r.partition_name), str(r.boundary_iso)) for r in result.all()]


async def _ensure_future_partitions(
    session: AsyncSession,
    dialect: DialectHelper,
    table: str,
    existing: list[tuple[str, str]],
    interval_days: int,
    lookahead_days: int,
) -> int:
    """Create partitions for today .. today+lookahead that don't exist yet.

    Returns the number of partitions created.  Oracle's INTERVAL
    partitioning is a no-op (auto-created by the DB on insert).
    """
    existing_names = {name for name, _ in existing}
    today = date.today()
    created = 0
    for offset in range(0, lookahead_days + 1, interval_days):
        start = today + timedelta(days=offset)
        name = dialect.partition_name(start, interval_days)
        if name in existing_names:
            continue
        statements = dialect.ensure_partition_sql(
            table, start, interval_days,
        )
        if not statements:
            # Oracle INTERVAL / PostgreSQL — no manual action needed.
            continue
        for stmt in statements:
            await session.execute(text(stmt))
        logger.info(
            "Created partition %s on %s for %s",
            name, table, start.isoformat(),
        )
        created += 1
    return created


async def _drop_old_partitions(
    session: AsyncSession,
    dialect: DialectHelper,
    table: str,
    existing: list[tuple[str, str]],
    retention_days: int,
) -> int:
    """Drop partitions whose upper boundary is older than ``retention_days``.

    Returns the count dropped.  Callers should SWITCH OUT or otherwise
    handle data egress before this runs — MS-SQL's MERGE RANGE absorbs
    data into the prior partition, Oracle/MySQL DROP discards it.
    """
    cutoff = date.today() - timedelta(days=retention_days)
    dropped = 0
    for name, boundary_iso in existing:
        boundary = _parse_boundary(boundary_iso)
        if boundary is None or boundary >= cutoff:
            continue
        # MS-SQL identifies the partition to drop by its boundary value;
        # Oracle/MySQL use the partition name.
        target = boundary_iso if dialect.db_type == "mssql" else name
        for stmt in dialect.drop_partition_sql(table, target):
            await session.execute(text(stmt))
        logger.info(
            "Dropped partition %s on %s (boundary %s, cutoff %s)",
            name, table, boundary.isoformat(), cutoff.isoformat(),
        )
        dropped += 1
    return dropped


def _parse_boundary(boundary_iso: str) -> date | None:
    """Best-effort ISO-8601 date parse for the various shapes dialects emit.

    MS-SQL returns ``2026-04-23T00:00:00``; MySQL returns a UNIX_TIMESTAMP
    integer as text; Oracle returns a ``TO_DATE(...)`` literal.  Returns
    ``None`` for anything unparseable so the caller skips the row rather
    than dropping a partition it does not understand.
    """
    cleaned = boundary_iso.strip().strip("'\"")
    # MySQL emits the raw UNIX_TIMESTAMP literal.  Parse in UTC to avoid a
    # date slip on hosts running in a non-UTC local zone.
    if cleaned.isdigit():
        try:
            return datetime.fromtimestamp(int(cleaned), tz=timezone.utc).date()
        except (ValueError, OSError, OverflowError):
            return None
    # MS-SQL-ish ISO datetime.
    try:
        return datetime.fromisoformat(cleaned.replace("T", " ")).date()
    except ValueError:
        pass
    # Oracle ``TO_DATE(' 2026-04-23 00:00:00', ...)`` — extract the literal.
    marker = "TO_DATE('"
    if marker in cleaned:
        remainder = cleaned.split(marker, 1)[1]
        literal = remainder.split("'", 1)[0].strip()
        try:
            return datetime.fromisoformat(literal.replace(" 00:00:00", "")).date()
        except ValueError:
            return None
    return None
