"""Request-limit guards for public event-list / aggregation endpoints.

Each guard reads its bound from the Phase A runtime-settings registry and
raises ``HTTPException(400)`` with a ``detail`` naming the violated key —
so admins can correlate rejections with the audit log via
``GET /api/v1/settings/{key}/audit``.

Designed as guard functions (call from inside a route handler) rather than
Python ``@decorator`` syntax — matches FastAPI's natural composition and
the ``require_max_lookback`` pattern already established in
``pagination.py``.

See ``docs/users/API_LIMITS.md`` for the client-facing reference.
"""

from __future__ import annotations

from datetime import datetime

from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from tsigma import settings_service

# Re-export so callers can import all four guards from one module.
from tsigma.reports.sdk.pagination import require_max_lookback as require_max_lookback


async def require_max_aggregation_days(
    start: datetime,
    end: datetime,
    *,
    session: AsyncSession | None,
) -> None:
    """Reject requests whose window exceeds ``api.max_aggregation_days``.

    The check is integer-day-resolution: ``(end - start).days``. Sub-day
    fractions don't count toward the ceiling.
    """
    max_days = await settings_service.get_int(
        "api.max_aggregation_days", session
    )
    window_days = (end - start).days
    if window_days > max_days:
        raise HTTPException(
            status_code=400,
            detail=(
                f"requested window ({window_days} days) exceeds "
                f"api.max_aggregation_days ({max_days})"
            ),
        )


async def require_max_signals_per_request(
    signal_ids: str | list[str],
    *,
    session: AsyncSession | None,
) -> None:
    """Reject requests whose signal-list size exceeds ``api.max_signals_per_request``.

    Single-string ``signal_ids`` counts as 1.
    """
    count = 1 if isinstance(signal_ids, str) else len(signal_ids)
    max_signals = await settings_service.get_int(
        "api.max_signals_per_request", session
    )
    if count > max_signals:
        raise HTTPException(
            status_code=400,
            detail=(
                f"requested signal count ({count}) exceeds "
                f"api.max_signals_per_request ({max_signals})"
            ),
        )


__all__ = [
    "require_max_aggregation_days",
    "require_max_signals_per_request",
    "require_max_lookback",
]
