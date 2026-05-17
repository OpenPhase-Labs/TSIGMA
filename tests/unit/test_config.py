"""
Tests for configuration management.

Tests that settings load from environment variables correctly.
"""

from tsigma.config import Settings


class TestSettings:
    """Tests for Settings configuration."""

    def test_default_values(self):
        """Test default configuration values."""
        settings = Settings()
        assert settings.db_type == "postgresql"
        assert settings.pg_host == "localhost"
        assert settings.pg_port == 5432
        assert settings.pg_database == "tsigma"
        assert settings.enable_api is True
        assert settings.event_log_partition_interval_days == 1

    def test_env_override(self, monkeypatch):
        """Test environment variables override defaults."""
        monkeypatch.setenv("TSIGMA_PG_HOST", "testdb.example.com")
        monkeypatch.setenv("TSIGMA_PG_PORT", "5433")
        monkeypatch.setenv("TSIGMA_ENABLE_API", "false")

        settings = Settings()
        assert settings.pg_host == "testdb.example.com"
        assert settings.pg_port == 5433
        assert settings.enable_api is False

    def test_partition_and_storage_config(self):
        """Test event-log partition and storage tier settings.

        ``event_log_partition_interval_days`` is the unified chunk/partition
        size used by TimescaleDB on PostgreSQL and by the partition-management
        job for MS-SQL / Oracle / MySQL — default 1 (daily).
        """
        settings = Settings()
        assert settings.event_log_partition_interval_days == 1
        assert settings.storage_warm_after == "7 days"
        assert settings.storage_retention == "2 years"

    def test_auth_defaults(self):
        """Test default authentication settings."""
        settings = Settings()
        assert settings.auth_admin_user == "admin"
        assert settings.auth_admin_password == "changeme"
        assert settings.auth_session_ttl_minutes == 480
        assert settings.auth_cookie_name == "tsigma_session"
        assert settings.auth_cookie_secure is True

    def test_auth_env_override(self, monkeypatch):
        """Test auth settings can be overridden via environment."""
        monkeypatch.setenv("TSIGMA_AUTH_ADMIN_USER", "superadmin")
        monkeypatch.setenv("TSIGMA_AUTH_ADMIN_PASSWORD", "strongpass")
        monkeypatch.setenv("TSIGMA_AUTH_SESSION_TTL_MINUTES", "120")
        monkeypatch.setenv("TSIGMA_AUTH_COOKIE_NAME", "my_session")
        monkeypatch.setenv("TSIGMA_AUTH_COOKIE_SECURE", "true")

        settings = Settings()
        assert settings.auth_admin_user == "superadmin"
        assert settings.auth_admin_password == "strongpass"
        assert settings.auth_session_ttl_minutes == 120
        assert settings.auth_cookie_name == "my_session"
        assert settings.auth_cookie_secure is True

    def test_collector_defaults(self):
        """Test default collector settings."""
        settings = Settings()
        assert settings.collector_max_concurrent == 50
        assert settings.collector_poll_interval == 900
        assert settings.sensor_poll_interval == 900

    def test_collector_env_override(self, monkeypatch):
        """Test collector settings can be overridden via environment."""
        monkeypatch.setenv("TSIGMA_COLLECTOR_MAX_CONCURRENT", "100")
        monkeypatch.setenv("TSIGMA_COLLECTOR_POLL_INTERVAL", "60")

        settings = Settings()
        assert settings.collector_max_concurrent == 100
        assert settings.collector_poll_interval == 60


class TestColdTierPydanticSettingsRemoved:
    """Task A6: ``storage_cold_enabled`` and ``storage_cold_after`` are removed
    from the Pydantic ``Settings`` class (they move to the runtime settings
    registry as ``storage.cold_enabled`` / ``storage.cold_after_days``).

    ``storage_cold_path`` is the only cold-tier Pydantic attribute that
    survives A6 (Locked Decision #1 — deployment-fixed). That positive
    invariant is covered by other tests; this class covers the two
    negative invariants only.
    """

    def test_storage_cold_enabled_not_in_pydantic_settings(self):
        """``storage_cold_enabled`` is no longer a Pydantic ``Settings`` attribute.

        Post-A6 it lives only in the runtime registry as ``storage.cold_enabled``.
        """
        settings = Settings()
        assert hasattr(settings, "storage_cold_enabled") is False, (
            "Pydantic ``Settings.storage_cold_enabled`` must be removed under "
            "Task A6; the value moved to runtime registry key "
            "``storage.cold_enabled`` and is read via "
            "``await get_bool('storage.cold_enabled', session)``."
        )

    def test_storage_cold_after_not_in_pydantic_settings(self):
        """``storage_cold_after`` is no longer a Pydantic ``Settings`` attribute.

        Post-A6 it lives only in the runtime registry as
        ``storage.cold_after_days`` (note: type changed from str to int days).
        """
        settings = Settings()
        assert hasattr(settings, "storage_cold_after") is False, (
            "Pydantic ``Settings.storage_cold_after`` must be removed under "
            "Task A6; the value moved to runtime registry key "
            "``storage.cold_after_days`` (int days) and is read via "
            "``await get_int('storage.cold_after_days', session)``."
        )


class TestValkeySettingsInvalidationFlag:
    """Task A4: ``valkey_settings_invalidation_enabled`` config flag.

    The flag controls the FIRST half of the A4 dual gate (the second half
    is ``valkey_url != ""``). It is a deployment-fixed boolean: production
    default True, forced to False in the test suite via the root
    conftest's ``os.environ.setdefault(...)``.
    """

    def test_invalidation_flag_present(self, monkeypatch):
        """The ``valkey_settings_invalidation_enabled`` attribute exists on Settings."""
        # Ensure the env-var bootstrap (set in tests/conftest.py) does not
        # mask the attribute-existence check: we want to confirm the field
        # is part of the Pydantic Settings class, not just an env-var read.
        monkeypatch.delenv(
            "TSIGMA_VALKEY_SETTINGS_INVALIDATION_ENABLED", raising=False
        )
        settings = Settings()
        assert hasattr(settings, "valkey_settings_invalidation_enabled"), (
            "Settings class is missing the A4 flag "
            "``valkey_settings_invalidation_enabled``."
        )

    def test_invalidation_flag_default_is_true(self, monkeypatch):
        """Production default for the flag is True."""
        # Clear the test-suite override so we observe the production default.
        monkeypatch.delenv(
            "TSIGMA_VALKEY_SETTINGS_INVALIDATION_ENABLED", raising=False
        )
        settings = Settings()
        assert settings.valkey_settings_invalidation_enabled is True, (
            "Production default for valkey_settings_invalidation_enabled "
            "must be True (clusters opt in by default; single-instance "
            "deployments are short-circuited by the second gate "
            "``valkey_url == ''``)."
        )

    def test_invalidation_flag_env_override_false(self, monkeypatch):
        """``TSIGMA_VALKEY_SETTINGS_INVALIDATION_ENABLED=false`` overrides to False."""
        monkeypatch.setenv(
            "TSIGMA_VALKEY_SETTINGS_INVALIDATION_ENABLED", "false"
        )
        settings = Settings()
        assert settings.valkey_settings_invalidation_enabled is False

    def test_invalidation_flag_env_override_true(self, monkeypatch):
        """``TSIGMA_VALKEY_SETTINGS_INVALIDATION_ENABLED=true`` overrides to True."""
        monkeypatch.setenv(
            "TSIGMA_VALKEY_SETTINGS_INVALIDATION_ENABLED", "true"
        )
        settings = Settings()
        assert settings.valkey_settings_invalidation_enabled is True
