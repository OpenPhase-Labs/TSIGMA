"""
Materialized view refresh job.

On PostgreSQL with TimescaleDB, continuous aggregates have their own
refresh policies managed by TimescaleDB itself.  This job handles any
*additional* standard PostgreSQL materialized views that may exist
(e.g. mv_phase_performance, mv_detector_health, mv_coordination).

On non-PostgreSQL databases this job is a no-op — aggregate tables
are populated by the jobs in aggregate.py instead.
"""

import logging

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from tsigma.config import settings
from tsigma.scheduler.registry import JobRegistry

logger = logging.getLogger(__name__)

# Standard materialized views (non-TimescaleDB).
# These are only created if the deployment chose plain PG mat-views
# instead of TimescaleDB continuous aggregates.
_MATERIALIZED_VIEWS = [
    "mv_phase_performance",
    "mv_detector_health",
    "mv_coordination",
]


@JobRegistry.register(name="refresh_views", trigger="cron", minute="*/15")
async def refresh_views(session: AsyncSession) -> None:
    """Refresh standard PostgreSQL materialized views if they exist."""
    if settings.db_type != "postgresql":
        return

    for view in _MATERIALIZED_VIEWS:
        try:
            # Check if the view exists before attempting refresh
            result = await session.execute(text(
                "SELECT 1 FROM pg_matviews WHERE matviewname = :name"
            ), {"name": view})
            if result.scalar() is None:
                continue

            await session.execute(
                text(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {view}")
            )
            logger.info("Refreshed materialized view: %s", view)
        except Exception:
            logger.exception("Failed to refresh materialized view: %s", view)
