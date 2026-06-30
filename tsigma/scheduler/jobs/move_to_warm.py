"""
Warm-tier partition placement for MS-SQL / Oracle / MySQL.

PostgreSQL deployments use TimescaleDB compression (see ``compress_chunks``)
to demote aged hypertable chunks; the other three dialects cannot compress in
place, so instead this job relocates partitions older than
``storage_warm_after`` onto a cheaper "warm" tablespace / filegroup
(``warm_tablespace_target``).

The job is opt-in: it no-ops unless ``warm_placement_enabled`` is True and a
``warm_tablespace_target`` is configured.  It shares the managed-table set and
boundary parsing with ``manage_partitions`` so a single retention/placement
policy covers both event streams.

Runs every 30 minutes so newly eligible partitions are demoted promptly
without a single large batch.
"""

import logging
from datetime import date, timedelta

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from tsigma.config import settings
from tsigma.database.db import DialectHelper
from tsigma.scheduler.jobs.manage_partitions import (
    _MANAGED_TABLES,
    _parse_boundary,
)
from tsigma.scheduler.registry import JobRegistry

logger = logging.getLogger(__name__)


def _warm_after_days(value: str) -> int | None:
    """Extract the leading integer day count from ``storage_warm_after``.

    ``"7 days"`` -> ``7``.  Returns ``None`` for anything without a valid
    positive leading integer so a malformed window is treated as
    "unknown" and the job demotes NOTHING (rather than collapsing the
    cutoff to today and demoting every historical partition).
    """
    token = str(value).strip().split()[0] if str(value).strip() else ""
    try:
        days = int(token)
    except ValueError:
        return None
    return days if days >= 0 else None


@JobRegistry.register(name="move_to_warm", trigger="interval", minutes=30)
async def move_to_warm(session: AsyncSession) -> None:
    """Move partitions older than the warm window onto cheaper storage."""
    if not settings.warm_placement_enabled:
        logger.debug(
            "Skipping move_to_warm — warm_placement_enabled is False",
        )
        return
    if not settings.warm_tablespace_target:
        logger.debug(
            "Skipping move_to_warm — no warm_tablespace_target configured",
        )
        return

    warm_days = _warm_after_days(settings.storage_warm_after)
    if warm_days is None:
        logger.warning(
            "Skipping move_to_warm — storage_warm_after is unparseable "
            "(%r); demoting nothing", settings.storage_warm_after,
        )
        return

    dialect = DialectHelper(settings.db_type)
    cutoff = date.today() - timedelta(days=warm_days)

    total_moved = 0
    for table in _MANAGED_TABLES:
        try:
            total_moved += await _move_table(session, dialect, table, cutoff)
        except Exception:
            logger.exception(
                "move_to_warm: failed processing %s — continuing", table,
            )

    logger.info(
        "move_to_warm complete: %d partition(s) moved to %s across %d "
        "table(s) (warm_after=%s)",
        total_moved, settings.warm_tablespace_target, len(_MANAGED_TABLES),
        settings.storage_warm_after,
    )


async def _move_table(
    session: AsyncSession,
    dialect: DialectHelper,
    table: str,
    cutoff: date,
) -> int:
    """Relocate every partition of ``table`` older than ``cutoff``.

    Returns the count moved.  Skips the table when the dialect does not
    expose partition listing (PostgreSQL) or the table is not partitioned.
    """
    sql = dialect.list_partitions_sql(table)
    if sql is None:
        logger.debug(
            "move_to_warm: %s has no partition listing (db_type=%s)",
            table, dialect.db_type,
        )
        return 0

    result = await session.execute(text(sql))
    rows = [(str(r.partition_name), str(r.boundary_iso)) for r in result.all()]

    moved = 0
    for name, boundary_iso in rows:
        boundary = _parse_boundary(boundary_iso)
        if boundary is None or boundary >= cutoff:
            continue
        try:
            for stmt in dialect.move_partition_tablespace_sql(
                table, name, settings.warm_tablespace_target,
            ):
                await session.execute(text(stmt))
            logger.info(
                "Moved partition %s on %s to %s (boundary %s, cutoff %s)",
                name, table, settings.warm_tablespace_target,
                boundary.isoformat(), cutoff.isoformat(),
            )
            moved += 1
        except Exception:
            logger.exception(
                "Failed to move partition %s on %s to warm storage",
                name, table,
            )
    return moved
