"""
Event-log partition management for MS-SQL / Oracle / MySQL.

PostgreSQL deployments use TimescaleDB (see ``compress_chunks`` + the
``event_log_partition_interval_days`` setting piped into
``create_hypertable``).  For every other dialect the
``controller_event_log`` table must be partitioned at the schema level and
kept fresh: this job keeps a rolling window of future partitions ahead of
``today`` so inserts never hit the default range, and optionally drops
partitions older than ``partition_retention_days``.

Runs once per day at 02:00.  The job is inert until the table is actually
partitioned at the schema level — if ``list_partitions_sql`` returns no
rows, the job logs a one-line skip and exits.
"""

import logging
from datetime import date, datetime, timedelta, timezone

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from tsigma.config import settings
from tsigma.database.db import DialectHelper
from tsigma.scheduler.registry import JobRegistry

logger = logging.getLogger(__name__)

_EVENT_LOG_TABLE = "controller_event_log"


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

    existing = await _existing_partitions(session, dialect)
    if existing is None:
        logger.info(
            "manage_partitions: %s does not support partition listing "
            "(db_type=%s)", _EVENT_LOG_TABLE, settings.db_type,
        )
        return
    if not existing:
        logger.info(
            "manage_partitions: %s is not partitioned — skipping (enable "
            "partitioning at table creation then re-run)",
            _EVENT_LOG_TABLE,
        )
        return

    created = await _ensure_future_partitions(
        session, dialect, existing, interval, lookahead,
    )
    dropped = 0
    if retention is not None:
        dropped = await _drop_old_partitions(
            session, dialect, existing, int(retention),
        )

    logger.info(
        "manage_partitions complete: %d partition(s) created, %d dropped "
        "(interval=%dd, lookahead=%dd, retention=%s)",
        created, dropped, interval, lookahead,
        f"{retention}d" if retention is not None else "none",
    )


async def _existing_partitions(
    session: AsyncSession, dialect: DialectHelper,
) -> list[tuple[str, str]] | None:
    """Return ``[(partition_name, boundary_iso), ...]`` or ``None`` if the
    dialect does not expose partition introspection.
    """
    sql = dialect.list_partitions_sql(_EVENT_LOG_TABLE)
    if sql is None:
        return None
    result = await session.execute(text(sql))
    return [(str(r.partition_name), str(r.boundary_iso)) for r in result.all()]


async def _ensure_future_partitions(
    session: AsyncSession,
    dialect: DialectHelper,
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
            _EVENT_LOG_TABLE, start, interval_days,
        )
        if not statements:
            # Oracle INTERVAL / PostgreSQL — no manual action needed.
            continue
        for stmt in statements:
            await session.execute(text(stmt))
        logger.info("Created partition %s for %s", name, start.isoformat())
        created += 1
    return created


async def _drop_old_partitions(
    session: AsyncSession,
    dialect: DialectHelper,
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
        for stmt in dialect.drop_partition_sql(_EVENT_LOG_TABLE, target):
            await session.execute(text(stmt))
        logger.info(
            "Dropped partition %s (boundary %s, cutoff %s)",
            name, boundary.isoformat(), cutoff.isoformat(),
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
