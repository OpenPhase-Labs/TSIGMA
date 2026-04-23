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
        assert settings.timescale_chunk_interval == "1 day"

    def test_env_override(self, monkeypatch):
        """Test environment variables override defaults."""
        monkeypatch.setenv("TSIGMA_PG_HOST", "testdb.example.com")
        monkeypatch.setenv("TSIGMA_PG_PORT", "5433")
        monkeypatch.setenv("TSIGMA_ENABLE_API", "false")

        settings = Settings()
        assert settings.pg_host == "testdb.example.com"
        assert settings.pg_port == 5433
        assert settings.enable_api is False

    def test_timescale_config(self):
        """Test TimescaleDB-specific settings."""
        settings = Settings()
        assert settings.timescale_chunk_interval == "1 day"
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
        assert settings.collector_poll_interval == 300

    def test_collector_env_override(self, monkeypatch):
        """Test collector settings can be overridden via environment."""
        monkeypatch.setenv("TSIGMA_COLLECTOR_MAX_CONCURRENT", "100")
        monkeypatch.setenv("TSIGMA_COLLECTOR_POLL_INTERVAL", "60")

        settings = Settings()
        assert settings.collector_max_concurrent == 100
        assert settings.collector_poll_interval == 60
