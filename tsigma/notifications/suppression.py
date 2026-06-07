"""
Shared alert-suppression helper.

This is the public, reusable form of the suppression check that previously
lived privately as ``_is_suppressed`` inside
``tsigma.scheduler.jobs.watchdog``. It is consumed both by the daily
watchdog job and by the per-ingest path, so the query logic lives here in a
single place.

This module only reads the ``alert_suppression`` table; it imports nothing
from ``tsigma.notifications`` itself, so it introduces no import cycle.
"""

import logging
from datetime import datetime, timezone

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from tsigma.models.alert_suppression import AlertSuppression

logger = logging.getLogger(__name__)

# Check-name identifiers used by the alert-suppression table.
CHECK_CONTROLLER_REPLACEMENT = "controller_replacement"
CHECK_CONFIG_PHASE_DRIFT = "config_phase_drift"


async def is_suppressed(
    session: AsyncSession, signal_id: str | None, check_name: str,
) -> bool:
    """
    Return True if an unexpired suppression rule covers ``(signal_id, check_name)``.

    A rule with NULL ``signal_id`` suppresses every signal for that check.
    A rule with NULL ``expires_at`` never expires.

    Fails open — database errors are logged and treated as not-suppressed so
    a broken table never silences real alerts.
    """
    now = datetime.now(timezone.utc)
    stmt = (
        select(func.count())
        .select_from(AlertSuppression)
        .where(
            AlertSuppression.check_name == check_name,
            (AlertSuppression.signal_id.is_(None))
            | (AlertSuppression.signal_id == signal_id),
            (AlertSuppression.expires_at.is_(None))
            | (AlertSuppression.expires_at > now),
        )
    )
    try:
        result = await session.execute(stmt)
        count = result.scalar() or 0
    except Exception:
        logger.exception(
            "alert_suppression lookup failed for (%s, %s) — failing open",
            signal_id, check_name,
        )
        return False
    return count > 0
