"""
Unit tests for settings_service.py.

Tests SettingsCache TTL/refresh/invalidation and seed_system_settings idempotency.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

from tsigma.models.system_setting import SystemSetting
from tsigma.settings_service import (
    ACCESS_CATEGORIES,
    ACCESS_VALUES,
    DEFAULT_ACCESS_POLICY,
    LOCKED_CATEGORIES,
    SettingsCache,
    seed_system_settings,
)

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
    mock_session = AsyncMock()
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
        """Test all defaults are inserted when no settings exist."""
        session = AsyncMock()
        result = MagicMock()
        result.scalars.return_value.all.return_value = []
        session.execute = AsyncMock(return_value=result)

        await seed_system_settings(session)

        assert session.add.call_count == len(DEFAULT_ACCESS_POLICY)

    @pytest.mark.asyncio
    async def test_skips_existing_keys(self):
        """Test existing keys are not re-inserted."""
        session = AsyncMock()
        result = MagicMock()
        # Pretend 3 keys already exist
        existing = [row["key"] for row in DEFAULT_ACCESS_POLICY[:3]]
        result.scalars.return_value.all.return_value = existing
        session.execute = AsyncMock(return_value=result)

        await seed_system_settings(session)

        expected_inserts = len(DEFAULT_ACCESS_POLICY) - 3
        assert session.add.call_count == expected_inserts

    @pytest.mark.asyncio
    async def test_no_inserts_when_all_exist(self):
        """Test no inserts when all keys already present (idempotent)."""
        session = AsyncMock()
        result = MagicMock()
        existing = [row["key"] for row in DEFAULT_ACCESS_POLICY]
        result.scalars.return_value.all.return_value = existing
        session.execute = AsyncMock(return_value=result)

        await seed_system_settings(session)

        session.add.assert_not_called()
