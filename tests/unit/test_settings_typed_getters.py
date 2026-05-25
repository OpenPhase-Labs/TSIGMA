"""
Unit tests for the typed-getter registry layer in ``tsigma.settings_service``.

Covers Task A2 of the runtime-settings Phase A plan: a typed-getter registry
(`register`, `RegistryEntry`, module-level registry) plus four async typed
getters (`get_int`, `get_bool`, `get_str`, `get_float`) that consult an
in-process registry, an env-var override, the existing settings cache, and
fall through to the registered default.

These tests DO NOT exercise the untyped `SettingsCache.get` contract — its
`str | None` return type is a regression-guard for A2 and is verified by
the existing `test_settings_service.py`, `test_settings_api.py`,
`test_access_policy_dependency.py`, and the autouse fixture in
`tests/integration/test_api_signals.py`. One regression-guard assertion is
also included here to keep the contract explicit at the typed-getter layer.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

from tests._helpers import make_mock_session
from tsigma.models.system_setting import SystemSetting
from tsigma.settings_service import (
    RegistryEntry,
    SettingsCache,
    UnknownSettingError,
    _REGISTRY,
    get_bool,
    get_float,
    get_int,
    get_str,
    register,
    settings_cache,
)


# ---------------------------------------------------------------------------
# Helpers
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


@pytest.fixture
def fresh_cache(monkeypatch):
    """Replace the module-global settings_cache with a fresh instance.

    The shared module-global cache is mutated by the integration autouse
    fixture in ``tests/integration/test_api_signals.py``. We swap in a
    fresh, fully-isolated cache for typed-getter tests so we do not rely
    on or leak state into other suites.
    """
    new_cache = SettingsCache(ttl_seconds=60.0)
    monkeypatch.setattr("tsigma.settings_service.settings_cache", new_cache)
    return new_cache


@pytest.fixture
def clean_env(monkeypatch):
    """Ensure none of the registry-derived env vars are set for this test."""
    monkeypatch.delenv("TSIGMA_API_MAX_PAGE_SIZE", raising=False)
    monkeypatch.delenv("TSIGMA_STORAGE_COLD_ENABLED", raising=False)
    monkeypatch.delenv("TSIGMA_STORAGE_COLD_AFTER_DAYS", raising=False)
    monkeypatch.delenv("TSIGMA_API_MAX_AGGREGATION_DAYS", raising=False)
    monkeypatch.delenv("TSIGMA_COLD_TIER_QUERY_ENABLED", raising=False)


# ---------------------------------------------------------------------------
# Registry shape
# ---------------------------------------------------------------------------


class TestRegistry:
    """Tests for the module-level registry and `register()`."""

    def test_registry_is_dict_of_registry_entries(self):
        """_REGISTRY is a mapping of key -> RegistryEntry."""
        assert isinstance(_REGISTRY, dict)
        for key, entry in _REGISTRY.items():
            assert isinstance(key, str)
            assert isinstance(entry, RegistryEntry)

    def test_registry_contains_nine_known_entries(self):
        """All 9 Phase A registry keys are pre-registered at module load."""
        expected_keys = {
            "storage.cold_enabled",
            "storage.cold_after_days",
            "storage.cold_delete_after_export",
            "cold_tier.query_enabled",
            "cold_tier.threshold_days",
            "api.max_page_size",
            "api.max_aggregation_days",
            "api.max_signals_per_request",
            "api.max_lookback_days",
        }
        assert expected_keys.issubset(_REGISTRY.keys())

    def test_registry_entry_has_required_fields(self):
        """RegistryEntry exposes (key, python_type, default, min, max, ...)"""
        entry = _REGISTRY["api.max_page_size"]
        assert entry.key == "api.max_page_size"
        assert entry.python_type is int
        assert entry.default == 1000
        assert entry.min == 1
        assert entry.max == 100000
        assert entry.category == "api"

    def test_register_succeeds_for_new_key(self):
        """register() inserts a new entry for an unseen key."""
        key = "test.register.new_key"
        try:
            register(key, int, default=42, min=0, max=100)
            assert key in _REGISTRY
            assert _REGISTRY[key].default == 42
        finally:
            _REGISTRY.pop(key, None)

    def test_register_raises_on_duplicate_key(self):
        """Duplicate registration is rejected to keep the registry the source of truth."""
        key = "test.register.duplicate"
        try:
            register(key, int, default=1)
            with pytest.raises(ValueError, match="already registered"):
                register(key, int, default=2)
        finally:
            _REGISTRY.pop(key, None)

    def test_register_rejects_default_outside_min_max(self):
        """register() validates that the default fits within the declared bounds."""
        key = "test.register.out_of_range"
        try:
            with pytest.raises(ValueError, match="above max"):
                register(key, int, default=999, min=0, max=10)
        finally:
            _REGISTRY.pop(key, None)

    def test_register_rejects_default_below_min(self):
        """register() validates lower bound too."""
        key = "test.register.below_min"
        try:
            with pytest.raises(ValueError, match="below min"):
                register(key, int, default=-1, min=0, max=10)
        finally:
            _REGISTRY.pop(key, None)


# ---------------------------------------------------------------------------
# get_int
# ---------------------------------------------------------------------------


class TestGetInt:
    """Tests for the `get_int` typed getter."""

    @pytest.mark.asyncio
    async def test_returns_default_when_no_db_row(self, fresh_cache, clean_env):
        """`get_int` returns the registry default when no DB row exists."""
        session = _make_mock_session([])
        value = await get_int("api.max_page_size", session)
        assert value == 1000
        assert isinstance(value, int)

    @pytest.mark.asyncio
    async def test_returns_db_value_when_set(self, fresh_cache, clean_env):
        """`get_int` parses the DB row value when present."""
        row = _make_setting_row("api.max_page_size", "250")
        session = _make_mock_session([row])
        value = await get_int("api.max_page_size", session)
        assert value == 250
        assert isinstance(value, int)

    @pytest.mark.asyncio
    async def test_env_var_overrides_db(self, fresh_cache, clean_env, monkeypatch):
        """TSIGMA_API_MAX_PAGE_SIZE takes precedence over the DB row."""
        row = _make_setting_row("api.max_page_size", "250")
        session = _make_mock_session([row])
        monkeypatch.setenv("TSIGMA_API_MAX_PAGE_SIZE", "777")
        value = await get_int("api.max_page_size", session)
        assert value == 777

    @pytest.mark.asyncio
    async def test_raises_type_error_on_wrong_registered_type(
        self, fresh_cache, clean_env
    ):
        """`get_int` on a bool-typed key raises TypeError."""
        session = _make_mock_session([])
        with pytest.raises(TypeError, match="storage.cold_enabled"):
            await get_int("storage.cold_enabled", session)

    @pytest.mark.asyncio
    async def test_raises_unknown_setting_error_for_missing_key(
        self, fresh_cache, clean_env
    ):
        """`get_int` on an unregistered key raises UnknownSettingError."""
        session = _make_mock_session([])
        with pytest.raises(UnknownSettingError, match="not.registered.key"):
            await get_int("not.registered.key", session)


# ---------------------------------------------------------------------------
# get_bool
# ---------------------------------------------------------------------------


class TestGetBool:
    """Tests for the `get_bool` typed getter and bool-coercion edge cases."""

    @pytest.mark.asyncio
    async def test_returns_default_false(self, fresh_cache, clean_env):
        """`get_bool` returns the registry default when no DB row exists."""
        session = _make_mock_session([])
        value = await get_bool("storage.cold_enabled", session)
        assert value is False

    @pytest.mark.asyncio
    async def test_returns_default_true(self, fresh_cache, clean_env):
        """`cold_tier.query_enabled` defaults to True per the registry."""
        session = _make_mock_session([])
        value = await get_bool("cold_tier.query_enabled", session)
        assert value is True

    @pytest.mark.asyncio
    async def test_parses_db_value_true(self, fresh_cache, clean_env):
        """`get_bool` parses 'true' from the DB row."""
        row = _make_setting_row("storage.cold_enabled", "true")
        session = _make_mock_session([row])
        assert await get_bool("storage.cold_enabled", session) is True

    @pytest.mark.asyncio
    async def test_parses_db_value_false_string(self, fresh_cache, clean_env):
        """`get_bool` parses 'false' from the DB row (not Python truthiness!)."""
        row = _make_setting_row("cold_tier.query_enabled", "false")
        session = _make_mock_session([row])
        assert await get_bool("cold_tier.query_enabled", session) is False

    @pytest.mark.asyncio
    async def test_parses_db_value_one_zero(self, fresh_cache, clean_env):
        """`get_bool` parses '1' and '0' from the DB row."""
        row = _make_setting_row("storage.cold_enabled", "1")
        session = _make_mock_session([row])
        assert await get_bool("storage.cold_enabled", session) is True

        row = _make_setting_row("cold_tier.query_enabled", "0")
        # Fresh cache for the second call
        fresh_cache.invalidate()
        session2 = _make_mock_session([row])
        assert await get_bool("cold_tier.query_enabled", session2) is False

    @pytest.mark.asyncio
    async def test_rejects_garbage_string(self, fresh_cache, clean_env):
        """`get_bool` raises ValueError on unparseable strings."""
        row = _make_setting_row("storage.cold_enabled", "maybe")
        session = _make_mock_session([row])
        with pytest.raises(ValueError, match="bool"):
            await get_bool("storage.cold_enabled", session)

    @pytest.mark.asyncio
    async def test_env_var_overrides_db(self, fresh_cache, clean_env, monkeypatch):
        """TSIGMA_STORAGE_COLD_ENABLED takes precedence over the DB row."""
        row = _make_setting_row("storage.cold_enabled", "false")
        session = _make_mock_session([row])
        monkeypatch.setenv("TSIGMA_STORAGE_COLD_ENABLED", "true")
        assert await get_bool("storage.cold_enabled", session) is True

    @pytest.mark.asyncio
    async def test_raises_type_error_on_wrong_registered_type(
        self, fresh_cache, clean_env
    ):
        """`get_bool` on an int-typed key raises TypeError."""
        session = _make_mock_session([])
        with pytest.raises(TypeError, match="api.max_page_size"):
            await get_bool("api.max_page_size", session)


# ---------------------------------------------------------------------------
# get_str
# ---------------------------------------------------------------------------


class TestGetStr:
    """Tests for the `get_str` typed getter."""

    @pytest.mark.asyncio
    async def test_returns_default(self, fresh_cache, clean_env, monkeypatch):
        """`get_str` returns the registry default when no DB row exists."""
        # Register a temporary str-typed key.
        register("test.get_str.greeting", str, default="hello")
        try:
            session = _make_mock_session([])
            assert (
                await get_str("test.get_str.greeting", session) == "hello"
            )
        finally:
            _REGISTRY.pop("test.get_str.greeting", None)

    @pytest.mark.asyncio
    async def test_returns_db_value(self, fresh_cache, clean_env):
        """`get_str` returns the DB row value as-is."""
        register("test.get_str.db", str, default="default")
        try:
            row = _make_setting_row("test.get_str.db", "from-db")
            session = _make_mock_session([row])
            assert await get_str("test.get_str.db", session) == "from-db"
        finally:
            _REGISTRY.pop("test.get_str.db", None)

    @pytest.mark.asyncio
    async def test_env_var_overrides_db(
        self, fresh_cache, clean_env, monkeypatch
    ):
        """`TSIGMA_*` env var takes precedence over the DB row."""
        register("test.get_str.env", str, default="default")
        try:
            row = _make_setting_row("test.get_str.env", "from-db")
            session = _make_mock_session([row])
            monkeypatch.setenv("TSIGMA_TEST_GET_STR_ENV", "from-env")
            assert (
                await get_str("test.get_str.env", session) == "from-env"
            )
        finally:
            _REGISTRY.pop("test.get_str.env", None)

    @pytest.mark.asyncio
    async def test_raises_type_error_on_wrong_registered_type(
        self, fresh_cache, clean_env
    ):
        """`get_str` on an int-typed key raises TypeError."""
        session = _make_mock_session([])
        with pytest.raises(TypeError, match="api.max_page_size"):
            await get_str("api.max_page_size", session)


# ---------------------------------------------------------------------------
# get_float
# ---------------------------------------------------------------------------


class TestGetFloat:
    """Tests for the `get_float` typed getter."""

    @pytest.mark.asyncio
    async def test_returns_default(self, fresh_cache, clean_env):
        """`get_float` returns the registry default."""
        register("test.get_float.ratio", float, default=0.25)
        try:
            session = _make_mock_session([])
            value = await get_float("test.get_float.ratio", session)
            assert value == pytest.approx(0.25)
            assert isinstance(value, float)
        finally:
            _REGISTRY.pop("test.get_float.ratio", None)

    @pytest.mark.asyncio
    async def test_returns_db_value(self, fresh_cache, clean_env):
        """`get_float` parses the DB row to float."""
        register("test.get_float.db", float, default=1.0)
        try:
            row = _make_setting_row("test.get_float.db", "3.14")
            session = _make_mock_session([row])
            value = await get_float("test.get_float.db", session)
            assert value == pytest.approx(3.14)
        finally:
            _REGISTRY.pop("test.get_float.db", None)

    @pytest.mark.asyncio
    async def test_env_var_overrides_db(
        self, fresh_cache, clean_env, monkeypatch
    ):
        """env-var override beats DB value for float keys."""
        register("test.get_float.env", float, default=1.0)
        try:
            row = _make_setting_row("test.get_float.env", "2.0")
            session = _make_mock_session([row])
            monkeypatch.setenv("TSIGMA_TEST_GET_FLOAT_ENV", "9.5")
            assert (
                await get_float("test.get_float.env", session)
                == pytest.approx(9.5)
            )
        finally:
            _REGISTRY.pop("test.get_float.env", None)

    @pytest.mark.asyncio
    async def test_raises_type_error_on_wrong_registered_type(
        self, fresh_cache, clean_env
    ):
        """`get_float` on a bool-typed key raises TypeError."""
        session = _make_mock_session([])
        with pytest.raises(TypeError, match="storage.cold_enabled"):
            await get_float("storage.cold_enabled", session)


# ---------------------------------------------------------------------------
# Regression guard — SettingsCache.get still returns `str | None`
# ---------------------------------------------------------------------------


class TestSettingsCacheRegressionGuard:
    """Lock down the untyped contract so callers in auth/dependencies.py and
    api/v1/settings.py keep working unchanged.

    The full regression suite for this contract lives in
    `test_settings_service.py`, `test_settings_api.py`,
    `test_access_policy_dependency.py`, and the autouse fixture in
    `tests/integration/test_api_signals.py`. This one assertion makes the
    invariant explicit at the typed-getter test layer.
    """

    @pytest.mark.asyncio
    async def test_cache_get_returns_string_unchanged(self):
        """`settings_cache.get('access_policy.analytics', session)` returns a string."""
        row = _make_setting_row("access_policy.analytics", "public")
        session = _make_mock_session([row])
        cache = SettingsCache(ttl_seconds=60.0)
        result = await cache.get("access_policy.analytics", session)
        assert result == "public"
        assert isinstance(result, str)

    @pytest.mark.asyncio
    async def test_cache_get_returns_none_for_missing(self):
        """`settings_cache.get(...)` returns None for an unset key."""
        session = _make_mock_session([])
        cache = SettingsCache(ttl_seconds=60.0)
        result = await cache.get("access_policy.analytics", session)
        assert result is None

    def test_cache_private_attrs_present_for_integration_fixture(self):
        """The integration `_warm_settings_cache` fixture mutates these directly.

        If A2 renames either, every test in test_api_signals.py breaks
        silently. Keep them named exactly as-is.
        """
        cache = SettingsCache(ttl_seconds=60.0)
        assert isinstance(cache._cache, dict)
        assert isinstance(cache._last_refresh, float)


# ---------------------------------------------------------------------------
# Task A6: cold-tier env-var hard-cutover behavior
# ---------------------------------------------------------------------------


class TestColdTierEnvVarHardCutover:
    """Task A6: cold-tier env-var continuity post-removal from Pydantic.

    Two registry keys are involved:

    - ``storage.cold_enabled`` (bool) → ``TSIGMA_STORAGE_COLD_ENABLED``.
      Byte-identical to the legacy Pydantic env-var name (continuity case).
    - ``storage.cold_after_days`` (int) → ``TSIGMA_STORAGE_COLD_AFTER_DAYS``.
      NOT byte-identical to the legacy ``TSIGMA_STORAGE_COLD_AFTER``
      (hard-cutover decision 2026-05-16). The legacy name is silently
      inert post-A6 — no alias parser, no deprecation warning.
    """

    @pytest.mark.asyncio
    async def test_storage_cold_enabled_env_var_overrides_typed_getter(
        self, fresh_cache, clean_env, monkeypatch
    ):
        """``TSIGMA_STORAGE_COLD_ENABLED`` continues to override the runtime
        registry key ``storage.cold_enabled`` post-A6 (byte-identical mapping).

        This is the unchanged-continuity case — load-bearing negative-path
        guard for operators upgrading without env-var edits.
        """
        session = _make_mock_session([])
        monkeypatch.setenv("TSIGMA_STORAGE_COLD_ENABLED", "false")
        value = await get_bool("storage.cold_enabled", session)
        assert value is False

        # And the affirmative path — env var override to True.
        monkeypatch.setenv("TSIGMA_STORAGE_COLD_ENABLED", "true")
        value = await get_bool("storage.cold_enabled", session)
        assert value is True

    @pytest.mark.asyncio
    async def test_storage_cold_after_legacy_env_var_is_inert(
        self, fresh_cache, clean_env, monkeypatch
    ):
        """Hard cutover: the legacy ``TSIGMA_STORAGE_COLD_AFTER`` name
        (without ``_DAYS`` suffix) is silently ignored post-A6.

        The new key derives to ``TSIGMA_STORAGE_COLD_AFTER_DAYS``; the
        legacy name does NOT map to it. Operators must rename. No alias
        parser is added; no deprecation warning is emitted.

        When ONLY the legacy name is set, ``get_int`` falls through to
        the registry default (180), NOT a parse of ``"6 months"``.
        """
        session = _make_mock_session([])
        # Set ONLY the legacy name; the new name remains unset.
        monkeypatch.setenv("TSIGMA_STORAGE_COLD_AFTER", "6 months")
        # The new name must NOT be set for this test to be meaningful;
        # the clean_env fixture has already deleted it, but assert
        # explicitly for clarity.
        monkeypatch.delenv("TSIGMA_STORAGE_COLD_AFTER_DAYS", raising=False)

        value = await get_int("storage.cold_after_days", session)
        assert value == 180, (
            "Legacy ``TSIGMA_STORAGE_COLD_AFTER`` must be silently inert "
            "post-A6 (hard cutover, no alias parser). Expected the "
            "registry default 180, got %r — this indicates an unexpected "
            "alias parser was added." % value
        )

    @pytest.mark.asyncio
    async def test_storage_cold_after_days_new_env_var_works(
        self, fresh_cache, clean_env, monkeypatch
    ):
        """The NEW env-var name ``TSIGMA_STORAGE_COLD_AFTER_DAYS`` (int)
        overrides the registry default and DB value via the standard
        env-var-override mechanism (A2 Locked Decision #3).
        """
        session = _make_mock_session([])
        monkeypatch.setenv("TSIGMA_STORAGE_COLD_AFTER_DAYS", "90")
        value = await get_int("storage.cold_after_days", session)
        assert value == 90
        assert isinstance(value, int)


# ---------------------------------------------------------------------------
# Module-level: the imported `settings_cache` singleton still exists
# ---------------------------------------------------------------------------


def test_module_global_settings_cache_is_still_a_settings_cache():
    """A2 must not have renamed/removed the module-global `settings_cache`."""
    assert isinstance(settings_cache, SettingsCache)
