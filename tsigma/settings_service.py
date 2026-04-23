"""
Database-backed system settings with in-memory cache.

Provides runtime-configurable settings that can be updated via the
admin UI without requiring an application restart. A TTL-based
in-memory cache keeps reads fast while allowing changes to propagate
within seconds.
"""

import logging
import time
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from tsigma.models.system_setting import SystemSetting

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Access policy categories — must match the keys seeded into the database.
# ---------------------------------------------------------------------------
ACCESS_CATEGORIES = (
    "analytics",
    "reports",
    "signal_detail",
    "health",
    "management",
    "ui",
)

# Categories that are always authenticated and cannot be changed by admin.
LOCKED_CATEGORIES = frozenset({"management"})

# Valid values for access policy settings.
ACCESS_VALUES = frozenset({"public", "authenticated"})

# ---------------------------------------------------------------------------
# Default rows seeded on first run.
# ---------------------------------------------------------------------------
DEFAULT_ACCESS_POLICY: list[dict[str, Any]] = [
    {
        "key": "access_policy.analytics",
        "value": "authenticated",
        "category": "access_policy",
        "description": "Access level for analytics endpoints (public or authenticated)",
        "editable": True,
    },
    {
        "key": "access_policy.reports",
        "value": "authenticated",
        "category": "access_policy",
        "description": "Access level for report generation and export (public or authenticated)",
        "editable": True,
    },
    {
        "key": "access_policy.signal_detail",
        "value": "authenticated",
        "category": "access_policy",
        "description": (
            "Access level for signal/approach/detector read endpoints"
            " (public or authenticated)"
        ),
        "editable": True,
    },
    {
        "key": "access_policy.health",
        "value": "authenticated",
        "category": "access_policy",
        "description": "Access level for health dashboard endpoints (public or authenticated)",
        "editable": True,
    },
    {
        "key": "access_policy.management",
        "value": "authenticated",
        "category": "access_policy",
        "description": "Access level for management/configuration endpoints (always authenticated)",
        "editable": False,
    },
    {
        "key": "access_policy.ui",
        "value": "authenticated",
        "category": "access_policy",
        "description": "Access level for web UI pages (public or authenticated)",
        "editable": True,
    },
]


# ---------------------------------------------------------------------------
# In-memory cache
# ---------------------------------------------------------------------------
class SettingsCache:
    """
    TTL-based in-memory cache for system settings.

    Reads from the database on first access and whenever the TTL expires.
    Thread-safe for single-process async apps (GIL protects dict ops).
    """

    def __init__(self, ttl_seconds: float = 30.0) -> None:
        self._ttl = ttl_seconds
        self._cache: dict[str, str] = {}
        self._last_refresh: float = 0.0

    @property
    def _is_stale(self) -> bool:
        return (time.monotonic() - self._last_refresh) >= self._ttl

    async def get(self, key: str, session: AsyncSession) -> str | None:
        """Get a setting value, refreshing cache if stale."""
        if self._is_stale:
            await self._refresh(session)
        return self._cache.get(key)

    async def get_all(self, session: AsyncSession) -> dict[str, str]:
        """Get all cached settings, refreshing if stale."""
        if self._is_stale:
            await self._refresh(session)
        return dict(self._cache)

    async def get_by_category(
        self, category: str, session: AsyncSession
    ) -> dict[str, str]:
        """Get all settings in a category, refreshing if stale."""
        if self._is_stale:
            await self._refresh(session)
        prefix = f"{category}."
        return {k: v for k, v in self._cache.items() if k.startswith(prefix)}

    def invalidate(self) -> None:
        """Force next read to hit the database."""
        self._last_refresh = 0.0

    async def _refresh(self, session: AsyncSession) -> None:
        """Reload all settings from the database."""
        try:
            result = await session.execute(select(SystemSetting))
            rows = result.scalars().all()
            self._cache = {row.key: row.value for row in rows}
            self._last_refresh = time.monotonic()
            logger.debug("Settings cache refreshed (%d entries)", len(self._cache))
        except Exception:
            logger.exception("Failed to refresh settings cache")
            # Keep serving stale data rather than crashing


# Global cache instance
settings_cache = SettingsCache(ttl_seconds=30.0)


# ---------------------------------------------------------------------------
# Seed function — called once during startup
# ---------------------------------------------------------------------------
async def seed_system_settings(session: AsyncSession) -> None:
    """
    Seed default system settings if they don't already exist.

    Only inserts rows for keys that are missing. Existing values are
    never overwritten, so admin changes survive restarts.
    """
    result = await session.execute(
        select(SystemSetting.key)
    )
    existing_keys = {row for row in result.scalars().all()}

    inserted = 0
    for row_data in DEFAULT_ACCESS_POLICY:
        if row_data["key"] not in existing_keys:
            session.add(SystemSetting(**row_data))
            inserted += 1

    if inserted:
        logger.info("Seeded %d default system settings", inserted)
