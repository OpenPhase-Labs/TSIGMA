"""
Unit tests for settings_service.py.

Tests SettingsCache TTL/refresh/invalidation and seed_system_settings idempotency.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

from tests._helpers import make_mock_session
from tsigma.models.system_setting import SystemSetting
from tsigma.models.system_setting_audit import SystemSettingAudit
from tsigma.settings_service import (
    _REGISTRY,
    ACCESS_CATEGORIES,
    ACCESS_VALUES,
    DEFAULT_ACCESS_POLICY,
    LOCKED_CATEGORIES,
    SettingsCache,
    seed_system_settings,
)
from tsigma.settings_service import set as set_setting

# Phase A registry keys seeded by the A5 additive pass.
_PHASE_A_REGISTRY_KEYS: tuple[str, ...] = (
    "storage.cold_enabled",
    "storage.cold_after_days",
    "storage.cold_delete_after_export",
    "cold_tier.query_enabled",
    "cold_tier.threshold_days",
    "api.max_page_size",
    "api.max_aggregation_days",
    "api.max_signals_per_request",
    "api.max_lookback_days",
)


def _expected_seed_value(default, python_type) -> str:
    """Return the text serialization a registry default lands in the DB as.

    Mirrors the serialization branch in ``settings_service.set()``:
    bools become ``"true"`` / ``"false"`` (lowercase, per the typed-getter's
    ``_coerce_bool`` accepted set); everything else is ``str(default)``.
    """
    if python_type is bool:
        return "true" if default else "false"
    return str(default)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------


class TestConstants:
    """Tests for module-level constants."""

    def test_access_categories_contains_expected(self):
        """Test all six access categories are defined."""
        assert set(ACCESS_CATEGORIES) == {
            "analytics", "reports", "signal_detail", "health", "management", "ui",
        }

    def test_locked_categories(self):
        """Test only management is locked."""
        assert LOCKED_CATEGORIES == frozenset({"management"})

    def test_access_values(self):
        """Test valid access values."""
        assert ACCESS_VALUES == frozenset({"public", "authenticated"})

    def test_default_policy_count(self):
        """Test default seed data has one row per category."""
        assert len(DEFAULT_ACCESS_POLICY) == len(ACCESS_CATEGORIES)

    def test_default_policy_all_authenticated(self):
        """Test all defaults are 'authenticated'."""
        for row in DEFAULT_ACCESS_POLICY:
            assert row["value"] == "authenticated"

    def test_management_not_editable(self):
        """Test management default seed row is not editable."""
        mgmt = [r for r in DEFAULT_ACCESS_POLICY if "management" in r["key"]]
        assert len(mgmt) == 1
        assert mgmt[0]["editable"] is False

    def test_non_management_are_editable(self):
        """Test non-management seed rows are editable."""
        non_mgmt = [r for r in DEFAULT_ACCESS_POLICY if "management" not in r["key"]]
        for row in non_mgmt:
            assert row["editable"] is True


# ---------------------------------------------------------------------------
# SettingsCache
# ---------------------------------------------------------------------------

def _make_mock_session(rows: list | None = None):
    """Create a mock AsyncSession that returns given SystemSetting rows."""
    if rows is None:
        rows = []
    mock_session = make_mock_session()
    result = MagicMock()
    result.scalars.return_value.all.return_value = rows
    mock_session.execute = AsyncMock(return_value=result)
    return mock_session


def _make_setting_row(key: str, value: str):
    """Create a mock SystemSetting row."""
    row = MagicMock(spec=SystemSetting)
    row.key = key
    row.value = value
    return row


class TestSettingsCache:
    """Tests for SettingsCache."""

    @pytest.mark.asyncio
    async def test_get_refreshes_on_first_call(self):
        """Test cache refreshes from DB on first access."""
        row = _make_setting_row("access_policy.analytics", "public")
        session = _make_mock_session([row])

        cache = SettingsCache(ttl_seconds=60.0)
        result = await cache.get("access_policy.analytics", session)

        assert result == "public"
        session.execute.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_get_returns_none_for_missing_key(self):
        """Test get returns None when key not in cache."""
        session = _make_mock_session([])

        cache = SettingsCache(ttl_seconds=60.0)
        result = await cache.get("nonexistent.key", session)

        assert result is None

    @pytest.mark.asyncio
    async def test_get_uses_cache_within_ttl(self):
        """Test cache does not re-query DB within TTL window."""
        row = _make_setting_row("test.key", "val")
        session = _make_mock_session([row])

        cache = SettingsCache(ttl_seconds=60.0)
        await cache.get("test.key", session)
        await cache.get("test.key", session)

        # Should only refresh once
        assert session.execute.await_count == 1

    @pytest.mark.asyncio
    async def test_get_refreshes_after_ttl_expires(self):
        """Test cache re-queries DB after TTL expires."""
        row = _make_setting_row("test.key", "val")
        session = _make_mock_session([row])

        cache = SettingsCache(ttl_seconds=0.0)  # Immediately stale
        await cache.get("test.key", session)
        await cache.get("test.key", session)

        # Both calls should trigger refresh
        assert session.execute.await_count == 2

    @pytest.mark.asyncio
    async def test_get_all_returns_dict(self):
        """Test get_all returns all cached settings."""
        rows = [
            _make_setting_row("a", "1"),
            _make_setting_row("b", "2"),
        ]
        session = _make_mock_session(rows)

        cache = SettingsCache(ttl_seconds=60.0)
        result = await cache.get_all(session)

        assert result == {"a": "1", "b": "2"}

    @pytest.mark.asyncio
    async def test_get_all_returns_copy(self):
        """Test get_all returns a copy, not the internal dict."""
        session = _make_mock_session([])

        cache = SettingsCache(ttl_seconds=60.0)
        result = await cache.get_all(session)
        result["injected"] = "bad"

        assert "injected" not in (await cache.get_all(session))

    @pytest.mark.asyncio
    async def test_get_by_category_filters(self):
        """Test get_by_category returns only matching keys."""
        rows = [
            _make_setting_row("access_policy.analytics", "public"),
            _make_setting_row("access_policy.reports", "authenticated"),
            _make_setting_row("other.setting", "value"),
        ]
        session = _make_mock_session(rows)

        cache = SettingsCache(ttl_seconds=60.0)
        result = await cache.get_by_category("access_policy", session)

        assert "access_policy.analytics" in result
        assert "access_policy.reports" in result
        assert "other.setting" not in result

    @pytest.mark.asyncio
    async def test_invalidate_forces_refresh(self):
        """Test invalidate() causes next read to hit DB."""
        row = _make_setting_row("test.key", "val")
        session = _make_mock_session([row])

        cache = SettingsCache(ttl_seconds=60.0)
        await cache.get("test.key", session)
        assert session.execute.await_count == 1

        cache.invalidate()
        await cache.get("test.key", session)
        assert session.execute.await_count == 2

    @pytest.mark.asyncio
    async def test_refresh_failure_keeps_stale_data(self):
        """Test that a DB error during refresh preserves existing cache."""
        row = _make_setting_row("test.key", "original")
        session = _make_mock_session([row])

        cache = SettingsCache(ttl_seconds=0.0)  # Always stale
        await cache.get("test.key", session)

        # Make next refresh fail
        session.execute = AsyncMock(side_effect=Exception("DB down"))
        result = await cache.get("test.key", session)

        assert result == "original"


# ---------------------------------------------------------------------------
# seed_system_settings
# ---------------------------------------------------------------------------

class TestSeedSystemSettings:
    """Tests for seed_system_settings()."""

    @pytest.mark.asyncio
    async def test_seeds_all_when_empty(self):
        """Test all defaults are inserted when no settings exist.

        Plan A5.1 Patch 8: with the additive registry seed pass, the empty-DB
        case now produces ``len(DEFAULT_ACCESS_POLICY) + len(_PHASE_A_REGISTRY_KEYS)``
        rows = 6 + 9 = 15.
        """
        session = make_mock_session()
        result = MagicMock()
        result.scalars.return_value.all.return_value = []
        session.execute = AsyncMock(return_value=result)

        await seed_system_settings(session)

        assert session.add.call_count == (
            len(DEFAULT_ACCESS_POLICY) + len(_PHASE_A_REGISTRY_KEYS)
        )

    @pytest.mark.asyncio
    async def test_skips_existing_keys(self):
        """Test existing keys are not re-inserted.

        Plan A5.1 Patch 8: pre-seeds the first 3 access-policy keys; the
        additive registry pass still inserts all 9 registry keys (none
        pre-seeded). Expected inserts = (6 - 3) + 9 = 12.
        """
        session = make_mock_session()
        result = MagicMock()
        # Pretend 3 keys already exist
        existing = [row["key"] for row in DEFAULT_ACCESS_POLICY[:3]]
        result.scalars.return_value.all.return_value = existing
        session.execute = AsyncMock(return_value=result)

        await seed_system_settings(session)

        expected_inserts = (
            (len(DEFAULT_ACCESS_POLICY) - 3) + len(_PHASE_A_REGISTRY_KEYS)
        )
        assert session.add.call_count == expected_inserts

    @pytest.mark.asyncio
    async def test_no_inserts_when_all_exist(self):
        """Test no inserts when all keys already present (idempotent).

        Plan A5.1 Patch 8: idempotency now requires pre-seeding BOTH the
        access-policy keys AND the 9 registry keys; otherwise the additive
        registry pass would insert the missing registry rows.
        """
        session = make_mock_session()
        result = MagicMock()
        existing = (
            [row["key"] for row in DEFAULT_ACCESS_POLICY]
            + list(_PHASE_A_REGISTRY_KEYS)
        )
        result.scalars.return_value.all.return_value = existing
        session.execute = AsyncMock(return_value=result)

        await seed_system_settings(session)

        session.add.assert_not_called()


# ---------------------------------------------------------------------------
# Helpers for A3 audit-on-write tests
# ---------------------------------------------------------------------------

def _make_audit_mock_session(existing_row: SystemSetting | None = None):
    """Create a mock AsyncSession for A3 set() tests.

    If ``existing_row`` is provided, the first ``session.execute`` returns a
    result whose ``scalar_one_or_none`` yields the row (UPSERT path: prior
    row exists, set() captures old_value from it). If None, the result
    yields None (first-time write path: old_value should be NULL).
    """
    mock_session = make_mock_session()
    result = MagicMock()
    result.scalar_one_or_none.return_value = existing_row
    result.scalars.return_value.all.return_value = (
        [existing_row] if existing_row is not None else []
    )
    mock_session.execute = AsyncMock(return_value=result)
    return mock_session


def _added_audit_rows(mock_session) -> list[SystemSettingAudit]:
    """Return SystemSettingAudit instances passed to session.add()."""
    rows: list[SystemSettingAudit] = []
    for call in mock_session.add.call_args_list:
        args, _ = call
        if args and isinstance(args[0], SystemSettingAudit):
            rows.append(args[0])
    return rows


# ---------------------------------------------------------------------------
# A3: set() writes audit rows
# ---------------------------------------------------------------------------

class TestSetWritesAuditRow:
    """Tests for the A3 ``set()`` helper's audit-row insert behavior."""

    @pytest.mark.asyncio
    async def test_set_writes_audit_row_on_successful_upsert(self):
        """set() with no prior row writes a SystemSettingAudit with old_value=None."""
        session = _make_audit_mock_session(existing_row=None)

        await set_setting(
            "api.max_page_size",
            500,
            session,
            changed_by="alice",
            reason="tune for prod",
        )

        audit_rows = _added_audit_rows(session)
        assert len(audit_rows) == 1, (
            f"Expected exactly one SystemSettingAudit row added; "
            f"got {len(audit_rows)}"
        )
        row = audit_rows[0]
        assert row.key == "api.max_page_size"
        assert row.new_value == "500"
        assert row.changed_by == "alice"
        assert row.reason == "tune for prod"
        assert row.old_value is None

    @pytest.mark.asyncio
    async def test_set_captures_old_value_from_prior_row(self):
        """set() with a pre-existing row captures old_value from that row."""
        prior = MagicMock(spec=SystemSetting)
        prior.key = "api.max_page_size"
        prior.value = "1000"
        prior.category = "api"
        prior.editable = True
        session = _make_audit_mock_session(existing_row=prior)

        await set_setting(
            "api.max_page_size",
            2000,
            session,
            changed_by="bob",
            reason="bump",
        )

        audit_rows = _added_audit_rows(session)
        assert len(audit_rows) == 1
        row = audit_rows[0]
        assert row.old_value == "1000"
        assert row.new_value == "2000"

    @pytest.mark.asyncio
    async def test_set_audit_changed_at_uses_server_default(self):
        """set() does not explicitly assign changed_at — server default ``func.now()`` populates it."""
        session = _make_audit_mock_session(existing_row=None)

        await set_setting(
            "api.max_page_size",
            500,
            session,
            changed_by="alice",
            reason=None,
        )

        audit_rows = _added_audit_rows(session)
        assert len(audit_rows) == 1
        # The model declares server_default=func.now(); the application code
        # must NOT explicitly populate changed_at. On a newly-constructed
        # instance pre-flush, the attribute should be None (server fills it).
        assert audit_rows[0].changed_at is None

    @pytest.mark.asyncio
    async def test_set_reason_is_optional(self):
        """set() called without a reason produces audit row with reason=None."""
        session = _make_audit_mock_session(existing_row=None)

        # Call without passing reason — defaults to None per signature.
        await set_setting(
            "api.max_page_size",
            500,
            session,
            changed_by="alice",
        )

        audit_rows = _added_audit_rows(session)
        assert len(audit_rows) == 1
        assert audit_rows[0].reason is None


# ---------------------------------------------------------------------------
# A3 regression guard: seed_system_settings writes no audit rows
# ---------------------------------------------------------------------------

class TestSeedWritesNoAuditRows:
    """Regression guard from plan A3.1.

    ``seed_system_settings()`` must NOT route through ``set()``; an audit row
    for "system seeded the default" pollutes the history. The seed path keeps
    ``session.add(SystemSetting(**row_data))`` and the audit log stays clean.
    """

    @pytest.mark.asyncio
    async def test_seed_system_settings_writes_no_audit_rows_for_access_policy_seed(self):
        """seed_system_settings() does not write SystemSettingAudit for the 6 access-policy seed rows."""
        session = make_mock_session()
        result = MagicMock()
        result.scalars.return_value.all.return_value = []
        session.execute = AsyncMock(return_value=result)

        await seed_system_settings(session)

        # Every single thing added must be a SystemSetting, NEVER a
        # SystemSettingAudit. The seed path inserts defaults into the
        # registry; audit rows would falsely record "user changed default".
        for call in session.add.call_args_list:
            args, _ = call
            assert args, "session.add called with no positional argument"
            added = args[0]
            assert not isinstance(added, SystemSettingAudit), (
                f"seed_system_settings() must not write SystemSettingAudit rows; "
                f"got one for key={getattr(added, 'key', '?')!r}"
            )

        audit_rows = _added_audit_rows(session)
        assert audit_rows == [], (
            f"Expected ZERO audit rows from seed; got {len(audit_rows)}"
        )

    @pytest.mark.asyncio
    async def test_seed_system_settings_writes_no_audit_rows_for_registry_keys(self):
        """Forward-compatible regression: even when A5's registry seed pass exists,
        seed_system_settings() must still emit ZERO SystemSettingAudit rows.

        A5 has not shipped yet; this guard protects against an implementer
        accidentally routing the registry seed pass through ``set()``.
        """
        session = make_mock_session()
        result = MagicMock()
        # Pretend access-policy keys exist already so any future registry
        # seed pass is the only thing that could call add().
        existing = [row["key"] for row in DEFAULT_ACCESS_POLICY]
        result.scalars.return_value.all.return_value = existing
        session.execute = AsyncMock(return_value=result)

        await seed_system_settings(session)

        audit_rows = _added_audit_rows(session)
        assert audit_rows == [], (
            f"Future-proof guard: the registry seed pass must not write "
            f"audit rows; got {len(audit_rows)}"
        )


# ---------------------------------------------------------------------------
# A5: additive seed pass for the typed-getter registry
# ---------------------------------------------------------------------------


def _added_system_settings(mock_session) -> list[SystemSetting]:
    """Return the SystemSetting instances passed to session.add(), in order."""
    rows: list[SystemSetting] = []
    for call in mock_session.add.call_args_list:
        args, _ = call
        if args and isinstance(args[0], SystemSetting):
            rows.append(args[0])
    return rows


class TestSeedRegistryKeys:
    """Plan A5: ``seed_system_settings()`` runs a SECOND, independent pass that
    inserts a row for every key in ``_REGISTRY`` that lacks a DB row. The
    existing access-policy pass is unchanged; the registry pass runs AFTER it.
    """

    @pytest.mark.asyncio
    async def test_seeds_all_9_registry_keys_on_first_run(self):
        """Empty DB: 6 access-policy rows + 9 registry rows = 15 inserts."""
        session = make_mock_session()
        result = MagicMock()
        result.scalars.return_value.all.return_value = []
        session.execute = AsyncMock(return_value=result)

        await seed_system_settings(session)

        added = _added_system_settings(session)
        added_keys = [row.key for row in added]

        # 6 + 9 = 15 total SystemSetting inserts.
        assert len(added) == (
            len(DEFAULT_ACCESS_POLICY) + len(_PHASE_A_REGISTRY_KEYS)
        ), (
            f"Expected 15 inserts (6 access-policy + 9 registry); "
            f"got {len(added)} with keys={added_keys!r}"
        )

        # All 9 registry keys must appear in the added rows.
        for registry_key in _PHASE_A_REGISTRY_KEYS:
            assert registry_key in added_keys, (
                f"Registry key {registry_key!r} was not seeded on first run; "
                f"got {added_keys!r}"
            )

    @pytest.mark.asyncio
    async def test_each_registry_key_seeded_at_its_default_value(self):
        """Every registry key is seeded at its registry default, text-serialized
        the same way ``set()`` would serialize a write (``"true"``/``"false"``
        for bools; ``str(default)`` for ints/floats/strs).
        """
        session = make_mock_session()
        result = MagicMock()
        result.scalars.return_value.all.return_value = []
        session.execute = AsyncMock(return_value=result)

        await seed_system_settings(session)

        added_by_key = {
            row.key: row for row in _added_system_settings(session)
        }

        for registry_key in _PHASE_A_REGISTRY_KEYS:
            entry = _REGISTRY[registry_key]
            expected = _expected_seed_value(entry.default, entry.python_type)
            assert registry_key in added_by_key, (
                f"Registry key {registry_key!r} not seeded"
            )
            actual = added_by_key[registry_key].value
            assert actual == expected, (
                f"Registry key {registry_key!r} seeded with value {actual!r}, "
                f"expected {expected!r} (default={entry.default!r}, "
                f"type={entry.python_type.__name__})"
            )

    @pytest.mark.asyncio
    async def test_each_registry_key_seeded_with_correct_category(self):
        """Every registry key is seeded with the category recorded on its
        ``RegistryEntry`` (``cold_tier`` for the 5 storage/cold-tier keys,
        ``api`` for the 4 api.* keys).
        """
        session = make_mock_session()
        result = MagicMock()
        result.scalars.return_value.all.return_value = []
        session.execute = AsyncMock(return_value=result)

        await seed_system_settings(session)

        added_by_key = {
            row.key: row for row in _added_system_settings(session)
        }

        for registry_key in _PHASE_A_REGISTRY_KEYS:
            entry = _REGISTRY[registry_key]
            assert registry_key in added_by_key, (
                f"Registry key {registry_key!r} not seeded"
            )
            actual = added_by_key[registry_key].category
            assert actual == entry.category, (
                f"Registry key {registry_key!r} seeded with category "
                f"{actual!r}; registry says {entry.category!r}"
            )

    @pytest.mark.asyncio
    async def test_registry_seed_idempotent_when_all_keys_present(self):
        """Pre-populate the DB with all 9 registry keys holding NON-DEFAULT
        values; seed_system_settings() must add ZERO new SystemSetting rows
        for the registry pass. Existing rows are untouched, values not
        overwritten.
        """
        session = make_mock_session()
        result = MagicMock()
        # Pre-seed: all access-policy keys AND all 9 registry keys present.
        existing = (
            [row["key"] for row in DEFAULT_ACCESS_POLICY]
            + list(_PHASE_A_REGISTRY_KEYS)
        )
        result.scalars.return_value.all.return_value = existing
        session.execute = AsyncMock(return_value=result)

        await seed_system_settings(session)

        added = _added_system_settings(session)
        added_registry_keys = [
            row.key for row in added if row.key in _PHASE_A_REGISTRY_KEYS
        ]
        assert added_registry_keys == [], (
            f"Idempotent seed must NOT re-insert pre-existing registry keys; "
            f"got {added_registry_keys!r}"
        )

    @pytest.mark.asyncio
    async def test_registry_seed_skips_existing_overwrite(self):
        """Pre-populate ``storage.cold_enabled = True`` (DIFFERENT from
        registry default ``False``). After seed, no new row for
        ``storage.cold_enabled`` was added — the existing row's value of
        ``True`` is preserved, NOT overwritten with the default.
        """
        session = make_mock_session()
        result = MagicMock()
        # Pretend storage.cold_enabled already exists in the DB.
        result.scalars.return_value.all.return_value = ["storage.cold_enabled"]
        session.execute = AsyncMock(return_value=result)

        await seed_system_settings(session)

        added_keys = [row.key for row in _added_system_settings(session)]
        assert "storage.cold_enabled" not in added_keys, (
            f"storage.cold_enabled must not be re-inserted (overwriting True "
            f"with False); got added_keys={added_keys!r}"
        )

    @pytest.mark.asyncio
    async def test_registry_seed_writes_no_audit_rows(self):
        """Regression guard from plan A3.1: the registry seed pass must NOT
        emit SystemSettingAudit rows. ``seed_system_settings()`` must bypass
        ``set()``, which would emit one audit row per insert.
        """
        session = make_mock_session()
        result = MagicMock()
        result.scalars.return_value.all.return_value = []
        session.execute = AsyncMock(return_value=result)

        await seed_system_settings(session)

        audit_rows = _added_audit_rows(session)
        assert audit_rows == [], (
            f"Registry seed pass must not write audit rows; "
            f"got {len(audit_rows)}"
        )

    @pytest.mark.asyncio
    async def test_registry_seed_runs_after_access_policy_seed(self):
        """Plan A5.2: the registry pass runs at the END of
        ``seed_system_settings()``, AFTER the existing ``DEFAULT_ACCESS_POLICY``
        loop. Verify the order in ``session.add.call_args_list``: the 6
        access-policy rows (in ``DEFAULT_ACCESS_POLICY`` order) appear first;
        the registry rows appear after.
        """
        session = make_mock_session()
        result = MagicMock()
        result.scalars.return_value.all.return_value = []
        session.execute = AsyncMock(return_value=result)

        await seed_system_settings(session)

        added = _added_system_settings(session)
        added_keys = [row.key for row in added]

        # The first len(DEFAULT_ACCESS_POLICY) inserts must be the
        # access-policy rows in their declared order.
        expected_access_order = [row["key"] for row in DEFAULT_ACCESS_POLICY]
        assert added_keys[: len(expected_access_order)] == expected_access_order, (
            f"Access-policy seed order changed or is interleaved; "
            f"got prefix={added_keys[: len(expected_access_order)]!r}, "
            f"expected {expected_access_order!r}"
        )

        # The remaining inserts must all be registry keys (no access-policy
        # keys interleaved into the tail).
        tail = added_keys[len(expected_access_order):]
        for key in tail:
            assert key in _PHASE_A_REGISTRY_KEYS, (
                f"Insert {key!r} appears in the tail (after access-policy) "
                f"but is not a registry key; tail={tail!r}"
            )
        assert set(tail) == set(_PHASE_A_REGISTRY_KEYS), (
            f"Registry seed pass did not insert exactly the 9 registry keys "
            f"in the tail; got {tail!r}"
        )
