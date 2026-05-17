"""
Database-backed system settings with in-memory cache.

Provides runtime-configurable settings that can be updated via the
admin UI without requiring an application restart. A TTL-based
in-memory cache keeps reads fast while allowing changes to propagate
within seconds.

This module also exposes a typed-getter registry layer
(`register`, `RegistryEntry`, `get_int`, `get_bool`, `get_str`,
`get_float`) for known tuning-knob keys (see Phase A of the
runtime-settings plan). The untyped `SettingsCache.get(key) -> str | None`
contract is preserved unchanged for the existing access-policy callers.
"""

import asyncio
import logging
import os
import time
from dataclasses import dataclass
from typing import Any, TYPE_CHECKING

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from tsigma.config import settings
from tsigma.models.system_setting import SystemSetting
from tsigma.models.system_setting_audit import SystemSettingAudit

if TYPE_CHECKING:
    import valkey.asyncio

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Pub/sub channel for cross-replica settings cache invalidation (Task A4).
# ---------------------------------------------------------------------------
SETTINGS_INVALIDATE_CHANNEL = "tsigma:system_setting:invalidate"

# Module-level singleton Valkey publisher client. Lazily constructed on the
# first publish so single-instance deployments (valkey_url == "") never pay
# the connection cost. Shared across the process — mirrors the
# ``settings_cache`` module-level singleton idiom.
_publisher_client: "valkey.asyncio.Valkey | None" = None


async def _get_publisher_client() -> "valkey.asyncio.Valkey":
    """Lazily construct the module-level Valkey publisher client.

    A single shared client per process, constructed on first publish.
    Uses the same connection options as ``ValkeySessionStore`` (the
    ``valkey.asyncio.from_url`` call in ``tsigma.app.lifespan``).
    """
    global _publisher_client
    if _publisher_client is None:
        import valkey.asyncio as _valkey  # local import — only when publishing
        _publisher_client = _valkey.from_url(
            settings.valkey_url, decode_responses=False
        )
    return _publisher_client


async def _handle_invalidate_message(message: dict) -> None:
    """Handle a pub/sub message from the invalidation channel.

    Messages of type ``"message"`` invalidate the local cache. Other types
    (``subscribe`` / ``psubscribe`` / ``unsubscribe`` acks, etc.) are
    ignored. Payload data may arrive as ``bytes`` (the valkey-py default
    with ``decode_responses=False``) and is decoded to ``str``.
    """
    if message.get("type") != "message":
        return
    key = message.get("data")
    if isinstance(key, bytes):
        key = key.decode("utf-8", errors="replace")
    # Best-effort: invalidate the whole local cache so the next read pulls
    # fresh DB state. SettingsCache.invalidate() takes no args.
    settings_cache.invalidate()
    logger.debug("Settings cache invalidated by pub/sub for key %r", key)

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

    Two independent passes, in order:

    1. ``DEFAULT_ACCESS_POLICY`` loop — seeds the 6 ``access_policy.*`` rows
       that drive the access-control middleware.
    2. Registry-iteration pass — seeds one row per ``_REGISTRY`` entry that
       lacks a DB row, at the entry's registered default. Existing rows are
       NEVER overwritten (an admin who set ``storage.cold_enabled = True``
       keeps that value across restarts). The registry pass bypasses ``set()``
       so first-boot seeding produces ZERO audit rows.
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

    # Registry-iteration pass — insert one row per registry entry that the
    # access-policy loop did not already seed. Direct ``session.add(...)`` —
    # NOT via ``set()`` — so first-boot defaults do not pollute the audit log
    # with "system seeded the default" rows.
    for registry_key, entry in _REGISTRY.items():
        if registry_key in existing_keys:
            continue
        # Mirror the serialization branch in ``set()``: bools become
        # ``"true"`` / ``"false"`` (lowercase, matching ``_coerce_bool``'s
        # accepted set); everything else is ``str(default)``.
        if entry.python_type is bool:
            serialized = "true" if entry.default else "false"
        else:
            serialized = str(entry.default)
        session.add(
            SystemSetting(
                key=entry.key,
                value=serialized,
                category=entry.category,
                description=entry.description,
                editable=True,
            )
        )
        inserted += 1

    if inserted:
        logger.info("Seeded %d default system settings", inserted)


# ---------------------------------------------------------------------------
# Typed-getter registry (Phase A, Task A2)
# ---------------------------------------------------------------------------


class UnknownSettingError(KeyError):
    """Raised when a typed getter is called with a key absent from the registry."""


@dataclass(frozen=True)
class RegistryEntry:
    """One row of the typed-getter registry.

    The registry is the source of truth for what keys exist; the DB carries
    values. A read or write naming an unknown key, or supplying a wrong-typed
    value, raises immediately.
    """

    key: str
    python_type: type
    default: Any
    min: Any = None
    max: Any = None
    description: str = ""
    category: str = ""


# Module-level registry. Mirrors the dataclass idiom already used in
# tsigma/storage/base.py. Mutated only by `register()` at module load and by
# the explicit cleanup paths in tests.
_REGISTRY: dict[str, RegistryEntry] = {}


def _coerce_bool(raw: str, *, source: str) -> bool:
    """Strict bool parser.

    ``bool("false")`` is ``True`` in Python, so we cannot rely on the builtin.
    Accept the documented set case-insensitively; reject everything else.
    """
    normalized = raw.strip().lower()
    if normalized in {"true", "1", "yes"}:
        return True
    if normalized in {"false", "0", "no"}:
        return False
    raise ValueError(
        f"Cannot coerce {source} value {raw!r} to bool; "
        "accepted: true/false/1/0/yes/no (case-insensitive)"
    )


def _coerce(python_type: type, raw: str, *, source: str) -> Any:
    """Type-coerce a raw string to the registered Python type."""
    if python_type is bool:
        return _coerce_bool(raw, source=source)
    if python_type is int:
        return int(raw)
    if python_type is float:
        return float(raw)
    if python_type is str:
        return str(raw)
    raise TypeError(
        f"Registry contains unsupported python_type {python_type!r} "
        f"for {source}; expected one of bool/int/float/str"
    )


def _validate_bounds(entry: RegistryEntry, value: Any) -> None:
    """Reject values outside the entry's declared min/max."""
    if entry.min is not None and value < entry.min:
        raise ValueError(
            f"Value {value!r} for {entry.key!r} is below min {entry.min!r}"
        )
    if entry.max is not None and value > entry.max:
        raise ValueError(
            f"Value {value!r} for {entry.key!r} is above max {entry.max!r}"
        )


def register(
    key: str,
    python_type: type,
    default: Any,
    *,
    min: Any = None,  # noqa: A002 — matches the field name on RegistryEntry
    max: Any = None,  # noqa: A002
    description: str = "",
    category: str = "",
) -> RegistryEntry:
    """Register a known runtime-settings key.

    Raises ``ValueError`` if the key is already registered (registry is the
    single source of truth — silent re-registration would mask bugs) or if
    the default itself violates the declared bounds.
    """
    if key in _REGISTRY:
        raise ValueError(f"Key {key!r} is already registered")
    entry = RegistryEntry(
        key=key,
        python_type=python_type,
        default=default,
        min=min,
        max=max,
        description=description,
        category=category,
    )
    # Validate the default sits inside the declared bounds. Reject early
    # rather than at first read.
    if default is not None and python_type in (int, float):
        _validate_bounds(entry, default)
    _REGISTRY[key] = entry
    return entry


def _env_var_name(key: str) -> str:
    """Derive the TSIGMA_<KEY> env-var name from a registry key.

    Per Locked Decision #3: dots → underscores, uppercased, ``TSIGMA_`` prefix.
    """
    return f"TSIGMA_{key.upper().replace('.', '_')}"


def _lookup(key: str, expected_type: type) -> RegistryEntry:
    """Resolve a typed-getter call to its registry entry.

    Raises ``UnknownSettingError`` if the key is not registered and
    ``TypeError`` if the registered type does not match the typed getter
    being invoked.
    """
    entry = _REGISTRY.get(key)
    if entry is None:
        raise UnknownSettingError(
            f"Setting {key!r} is not registered; "
            "call register() at module load before reading"
        )
    if entry.python_type is not expected_type:
        raise TypeError(
            f"Setting {key!r} is registered as {entry.python_type.__name__}, "
            f"not {expected_type.__name__}"
        )
    return entry


async def _resolve(
    entry: RegistryEntry, session: AsyncSession
) -> Any:
    """Resolve the effective value of a registered key.

    Order of precedence per Locked Decision #3:
    1. ``TSIGMA_<KEY>`` env var (if present);
    2. cached DB row (via ``settings_cache.get``);
    3. registered default.

    The cache continues to store raw text; this helper parses on read.
    """
    env_raw = os.environ.get(_env_var_name(entry.key))
    if env_raw is not None:
        return _coerce(entry.python_type, env_raw, source=f"env {entry.key}")

    db_raw = await settings_cache.get(entry.key, session)
    if db_raw is not None:
        return _coerce(entry.python_type, db_raw, source=f"db {entry.key}")

    return entry.default


async def get_int(key: str, session: AsyncSession) -> int:
    """Read a registered int-typed setting."""
    entry = _lookup(key, int)
    return await _resolve(entry, session)


async def get_bool(key: str, session: AsyncSession) -> bool:
    """Read a registered bool-typed setting."""
    entry = _lookup(key, bool)
    return await _resolve(entry, session)


async def get_str(key: str, session: AsyncSession) -> str:
    """Read a registered str-typed setting."""
    entry = _lookup(key, str)
    return await _resolve(entry, session)


async def get_float(key: str, session: AsyncSession) -> float:
    """Read a registered float-typed setting."""
    entry = _lookup(key, float)
    return await _resolve(entry, session)


async def set(  # noqa: A001 — matches the builtin name intentionally for symmetry with get_*
    key: str,
    value: Any,
    session: AsyncSession,
    *,
    changed_by: str,
    reason: str | None = None,
) -> None:
    """Validate-and-write helper for runtime settings.

    Two branches:

    - **Registry branch** (key present in ``_REGISTRY``): full type-check,
      coerce, bounds-validate, UPSERT via ``pg_insert.on_conflict_do_update``.
      Used by typed tuning-knob writes (e.g. ``api.max_page_size``).
    - **Non-registry permissive branch**: required so the existing
      ``access_policy.*`` PUT endpoints can route through ``set()`` without
      registering every legacy key. Reads the existing row, captures its
      prior ``value`` for the audit log, and mutates the row in place
      (the ORM emits an UPDATE on flush).

    Both branches:

    1. Capture ``old_value`` from the prior row (``None`` if no row exists).
    2. Perform the write.
    3. Append a ``SystemSettingAudit`` row in the SAME transaction —
       ``changed_at`` is left unset so the server default ``func.now()``
       populates it.
    4. Invalidate the local cache so the next read refreshes from DB.

    The caller (HTTP route) holds the transaction; this helper does NOT
    commit.
    """
    entry = _REGISTRY.get(key)

    # Capture the prior value (NULL for first-time writes) in the SAME
    # transaction as the UPSERT / mutation below.
    prior_result = await session.execute(
        select(SystemSetting).where(SystemSetting.key == key)
    )
    existing = prior_result.scalar_one_or_none()
    old_value = existing.value if existing is not None else None

    if entry is not None:
        # Registry branch — type-check, coerce, bounds-validate, UPSERT.
        if isinstance(value, str) and entry.python_type is not str:
            coerced = _coerce(entry.python_type, value, source=f"set {key}")
        else:
            if not isinstance(value, entry.python_type):
                raise TypeError(
                    f"Setting {key!r} expects {entry.python_type.__name__}, "
                    f"got {type(value).__name__}"
                )
            coerced = value

        _validate_bounds(entry, coerced)

        # Persist as text (matches the existing `value TEXT` column).
        serialized = "true" if (entry.python_type is bool and coerced) else (
            "false" if entry.python_type is bool else str(coerced)
        )

        stmt = (
            pg_insert(SystemSetting)
            .values(
                key=key,
                value=serialized,
                category=entry.category,
                description=entry.description,
                editable=True,
                updated_by=changed_by,
            )
            .on_conflict_do_update(
                index_elements=["key"],
                set_={"value": serialized, "updated_by": changed_by},
            )
        )
        await session.execute(stmt)
        new_value = serialized
    else:
        # Non-registry permissive branch — needed so legacy `access_policy.*`
        # keys (managed via the existing schema seed, not the registry) can
        # route writes through `set()` so they too produce audit rows.
        if existing is None:
            raise UnknownSettingError(
                f"Setting {key!r} is not registered and no existing row "
                "found; cannot create an untyped row implicitly"
            )
        new_value = str(value)
        existing.value = new_value
        existing.updated_by = changed_by

    # Append the audit row in the same transaction. `changed_at` is left
    # unset — the column carries server_default=func.now().
    session.add(
        SystemSettingAudit(
            key=key,
            old_value=old_value,
            new_value=new_value,
            changed_by=changed_by,
            reason=reason,
        )
    )

    settings_cache.invalidate()

    # Publish on the cross-replica invalidation channel so peer replicas
    # drop their local caches. Dual-gated per Locked Decision #7: both the
    # feature flag AND a non-empty ``valkey_url`` must be set. Short-circuit
    # BEFORE touching the publisher client so the gated-closed paths never
    # construct a connection.
    if settings.valkey_settings_invalidation_enabled and settings.valkey_url:
        client = await _get_publisher_client()
        # Fire-and-forget — the DB write is the source of truth; the publish
        # is best-effort and must not add latency to the PUT or roll back the
        # transaction on Valkey outage.
        asyncio.create_task(
            client.publish(SETTINGS_INVALIDATE_CHANNEL, key)
        )


# ---------------------------------------------------------------------------
# Module-load registration of the 9 Phase A tuning-knob entries
# (verbatim from the Runtime Settings Registry table in the plan)
# ---------------------------------------------------------------------------

register(
    "storage.cold_enabled",
    bool,
    default=False,
    description="Master switch for cold-tier export. Replaces TSIGMA_STORAGE_COLD_ENABLED env var.",
    category="cold_tier",
)
register(
    "storage.cold_after_days",
    int,
    default=180,
    min=1,
    max=36500,
    description="Age threshold for archival. Replaces TSIGMA_STORAGE_COLD_AFTER.",
    category="cold_tier",
)
register(
    "storage.cold_delete_after_export",
    bool,
    default=True,
    description="Delete archived rows from hot DB after verified Parquet write.",
    category="cold_tier",
)
register(
    "cold_tier.query_enabled",
    bool,
    default=True,
    description="Route queries past threshold to cold tier; admins can disable to force hot-only.",
    category="cold_tier",
)
register(
    "cold_tier.threshold_days",
    int,
    default=180,
    min=1,
    max=36500,
    description="Events older than this many days are read from cold tier.",
    category="cold_tier",
)
register(
    "api.max_page_size",
    int,
    default=1000,
    min=1,
    max=100000,
    description="Event-list endpoint per-page cap.",
    category="api",
)
register(
    "api.max_aggregation_days",
    int,
    default=92,
    min=1,
    max=36500,
    description="Aggregation endpoint date-range cap.",
    category="api",
)
register(
    "api.max_signals_per_request",
    int,
    default=100,
    min=1,
    max=100000,
    description="Aggregation endpoint per-request signal count cap.",
    category="api",
)
register(
    "api.max_lookback_days",
    int,
    default=92,
    min=1,
    max=36500,
    description="Absolute oldest data an API request can ask for.",
    category="api",
)
