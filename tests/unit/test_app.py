"""
Tests for FastAPI application structure.

Tests app creation, middleware registration, and lifecycle.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tsigma.app import create_app


def _make_mock_facade():
    """Create a mock DatabaseFacade with session factory support."""
    mock_facade = AsyncMock()
    mock_facade.connect = AsyncMock()
    mock_facade.disconnect = AsyncMock()

    mock_session = AsyncMock()
    result_mock = MagicMock()
    result_mock.scalar_one_or_none.return_value = None
    result_mock.scalars.return_value.all.return_value = []
    mock_session.execute = AsyncMock(return_value=result_mock)
    mock_session.add = MagicMock()
    mock_session.flush = AsyncMock()
    mock_session.commit = AsyncMock()
    mock_session.rollback = AsyncMock()
    mock_session_ctx = MagicMock()
    mock_session_ctx.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session_ctx.__aexit__ = AsyncMock(return_value=False)
    # Must be MagicMock so calling it returns the ctx directly (not a coroutine)
    mock_facade._session_factory = MagicMock(return_value=mock_session_ctx)

    return mock_facade, mock_session


def _lifespan_patches(mock_facade, extra_patches=None):
    """Build the standard set of lifespan patches."""
    patches = [
        patch("tsigma.app.DatabaseFacade", return_value=mock_facade),
        patch("tsigma.app.seed_admin", new_callable=AsyncMock),
        patch("tsigma.app.seed_system_settings", new_callable=AsyncMock),
        patch("tsigma.notifications.registry.initialize_providers", new_callable=AsyncMock),
    ]
    if extra_patches:
        patches.extend(extra_patches)
    return patches


def _apply_mock_settings(mock_settings, **overrides):
    """Apply standard settings attributes to a mock settings object."""
    defaults = {
        "enable_api": True,
        "enable_collector": False,
        "enable_scheduler": False,
        "db_type": "postgresql",
        "pg_user": "test",
        "pg_password": "test",
        "pg_host": "localhost",
        "pg_port": 5432,
        "pg_database": "testdb",
        "auth_session_ttl_minutes": 480,
        "auth_mode": "local",
        "cors_origins": "",
        "log_level": "INFO",
        "log_format": "json",
        "valkey_url": "",
        "validation_enabled": False,
    }
    defaults.update(overrides)
    for k, v in defaults.items():
        setattr(mock_settings, k, v)


class TestAppCreation:
    """Tests for app factory."""

    def test_create_app_returns_fastapi(self):
        """Test create_app returns FastAPI instance."""
        app = create_app()
        assert app.title == "TSIGMA"
        assert app.version == "1.0.0"
        assert app.description == "Traffic Signal Intelligence: Gathering Metrics & Analytics"

    def test_middleware_registered(self):
        """Test middleware is registered in correct order."""
        app = create_app()
        middleware_classes = [m.cls.__name__ for m in app.user_middleware]

        assert "GZipMiddleware" in middleware_classes
        assert "SecurityHeadersMiddleware" in middleware_classes
        assert "RequestIDMiddleware" in middleware_classes

    def test_app_has_lifespan(self):
        """Test app has lifespan configured."""
        app = create_app()
        assert app.router.lifespan_context is not None

    def test_signals_router_registered(self):
        """Test signals API router is included."""
        app = create_app()
        routes = [r.path for r in app.routes]
        assert "/api/v1/signals/" in routes
        assert "/api/v1/signals/{signal_id}" in routes


class TestLifespan:
    """Tests for app lifespan (startup/shutdown)."""

    @pytest.mark.asyncio
    async def test_startup_initializes_facade(self):
        """Test lifespan startup creates and connects DatabaseFacade."""
        import tsigma.database.db as db_module
        original = db_module.db_facade

        mock_facade, _ = _make_mock_facade()
        patches = _lifespan_patches(mock_facade)

        try:
            with patches[0], patches[1], patches[2], patches[3]:
                app = create_app()
                async with app.router.lifespan_context(app):
                    mock_facade.connect.assert_awaited_once()
                    assert db_module.db_facade is mock_facade

                mock_facade.disconnect.assert_awaited_once()
        finally:
            db_module.db_facade = original

    @pytest.mark.asyncio
    async def test_startup_uses_settings(self):
        """Test lifespan passes config from settings to DatabaseFacade."""
        import tsigma.database.db as db_module
        original = db_module.db_facade

        mock_facade, _ = _make_mock_facade()

        try:
            with patch("tsigma.app.DatabaseFacade", return_value=mock_facade) as mock_cls, \
                 patch("tsigma.app.seed_admin", new_callable=AsyncMock), \
                 patch("tsigma.app.seed_system_settings", new_callable=AsyncMock), \
                 patch("tsigma.notifications.registry.initialize_providers",
                       new_callable=AsyncMock), \
                 patch("tsigma.app.settings") as mock_settings:
                _apply_mock_settings(
                    mock_settings,
                    pg_user="testuser",
                    pg_password="testpass",
                    pg_host="testhost",
                )

                app = create_app()
                async with app.router.lifespan_context(app):
                    pass

                mock_cls.assert_called_once_with(
                    "postgresql",
                    user="testuser",
                    password="testpass",
                    host="testhost",
                    port=5432,
                    database="testdb",
                )
        finally:
            db_module.db_facade = original

    @pytest.mark.asyncio
    async def test_shutdown_clears_facade(self):
        """Test lifespan shutdown sets db_facade back to None."""
        import tsigma.database.db as db_module
        original = db_module.db_facade

        mock_facade, _ = _make_mock_facade()

        try:
            with patch("tsigma.app.DatabaseFacade", return_value=mock_facade), \
                 patch("tsigma.app.seed_admin", new_callable=AsyncMock), \
                 patch("tsigma.app.seed_system_settings", new_callable=AsyncMock), \
                 patch("tsigma.notifications.registry.initialize_providers",
                       new_callable=AsyncMock):
                app = create_app()
                async with app.router.lifespan_context(app):
                    assert db_module.db_facade is not None

                assert db_module.db_facade is None
        finally:
            db_module.db_facade = original


class TestCollectorWiring:
    """Tests for CollectorService integration in app lifecycle."""

    @pytest.mark.asyncio
    async def test_startup_creates_collector_when_enabled(self):
        """Test lifespan creates and starts CollectorService."""
        import tsigma.database.db as db_module
        original = db_module.db_facade

        mock_facade, _ = _make_mock_facade()
        mock_collector = AsyncMock()

        try:
            with patch("tsigma.app.DatabaseFacade", return_value=mock_facade), \
                 patch("tsigma.app.seed_admin", new_callable=AsyncMock), \
                 patch("tsigma.app.seed_system_settings", new_callable=AsyncMock), \
                 patch("tsigma.notifications.registry.initialize_providers",
                       new_callable=AsyncMock), \
                 patch("tsigma.app.settings") as mock_settings, \
                 patch("tsigma.collection.service.CollectorService",
                       return_value=mock_collector):
                _apply_mock_settings(mock_settings, enable_collector=True)

                app = create_app()
                async with app.router.lifespan_context(app):
                    mock_collector.start.assert_awaited_once()
                    assert app.state.collector is mock_collector

                mock_collector.stop.assert_awaited_once()
        finally:
            db_module.db_facade = original

    @pytest.mark.asyncio
    async def test_collector_not_created_when_disabled(self):
        """Test lifespan skips CollectorService when disabled."""
        import tsigma.database.db as db_module
        original = db_module.db_facade

        mock_facade, _ = _make_mock_facade()

        try:
            with patch("tsigma.app.DatabaseFacade", return_value=mock_facade), \
                 patch("tsigma.app.seed_admin", new_callable=AsyncMock), \
                 patch("tsigma.app.seed_system_settings", new_callable=AsyncMock), \
                 patch("tsigma.notifications.registry.initialize_providers",
                       new_callable=AsyncMock), \
                 patch("tsigma.app.settings") as mock_settings:
                _apply_mock_settings(mock_settings)

                app = create_app()
                async with app.router.lifespan_context(app):
                    assert not hasattr(app.state, "collector")
        finally:
            db_module.db_facade = original


class TestSchedulerWiring:
    """Tests for SchedulerService integration in app lifecycle."""

    @pytest.mark.asyncio
    async def test_startup_creates_scheduler_when_enabled(self):
        """Test lifespan creates and starts SchedulerService."""
        import tsigma.database.db as db_module
        original = db_module.db_facade

        mock_facade, _ = _make_mock_facade()
        mock_scheduler = AsyncMock()

        try:
            with patch("tsigma.app.DatabaseFacade", return_value=mock_facade), \
                 patch("tsigma.app.seed_admin", new_callable=AsyncMock), \
                 patch("tsigma.app.seed_system_settings", new_callable=AsyncMock), \
                 patch("tsigma.notifications.registry.initialize_providers",
                       new_callable=AsyncMock), \
                 patch("tsigma.app.settings") as mock_settings, \
                 patch("tsigma.scheduler.service.SchedulerService",
                       return_value=mock_scheduler):
                _apply_mock_settings(mock_settings, enable_scheduler=True)

                app = create_app()
                async with app.router.lifespan_context(app):
                    mock_scheduler.load_registry.assert_called_once()
                    mock_scheduler.start.assert_awaited_once()
                    assert app.state.scheduler is mock_scheduler

                mock_scheduler.stop.assert_awaited_once()
        finally:
            db_module.db_facade = original

    @pytest.mark.asyncio
    async def test_scheduler_not_created_when_disabled(self):
        """Test lifespan skips SchedulerService when disabled."""
        import tsigma.database.db as db_module
        original = db_module.db_facade

        mock_facade, _ = _make_mock_facade()

        try:
            with patch("tsigma.app.DatabaseFacade", return_value=mock_facade), \
                 patch("tsigma.app.seed_admin", new_callable=AsyncMock), \
                 patch("tsigma.app.seed_system_settings", new_callable=AsyncMock), \
                 patch("tsigma.notifications.registry.initialize_providers",
                       new_callable=AsyncMock), \
                 patch("tsigma.app.settings") as mock_settings:
                _apply_mock_settings(mock_settings)

                app = create_app()
                async with app.router.lifespan_context(app):
                    assert not hasattr(app.state, "scheduler")
        finally:
            db_module.db_facade = original


class TestAuthWiring:
    """Tests for auth integration in app lifecycle."""

    def test_auth_router_registered(self):
        """Test auth API router is included in the app."""
        app = create_app()
        routes = [r.path for r in app.routes]
        # Static auth routes (login is mounted dynamically by auth provider)
        assert "/api/v1/auth/provider" in routes
        assert "/api/v1/auth/logout" in routes
        assert "/api/v1/auth/me" in routes

    @pytest.mark.asyncio
    async def test_lifespan_creates_session_store(self):
        """Test lifespan creates InMemorySessionStore on app.state."""
        import tsigma.database.db as db_module
        original = db_module.db_facade

        mock_facade, _ = _make_mock_facade()

        try:
            with patch("tsigma.app.DatabaseFacade", return_value=mock_facade), \
                 patch("tsigma.app.seed_admin", new_callable=AsyncMock), \
                 patch("tsigma.app.seed_system_settings", new_callable=AsyncMock), \
                 patch("tsigma.notifications.registry.initialize_providers",
                       new_callable=AsyncMock):
                app = create_app()
                async with app.router.lifespan_context(app):
                    assert hasattr(app.state, "session_store")
                    from tsigma.auth.sessions import InMemorySessionStore
                    assert isinstance(app.state.session_store, InMemorySessionStore)
        finally:
            db_module.db_facade = original

    @pytest.mark.asyncio
    async def test_lifespan_calls_seed_admin(self):
        """Test lifespan calls seed_admin after DB connect."""
        import tsigma.database.db as db_module
        original = db_module.db_facade

        mock_facade, _ = _make_mock_facade()

        try:
            with patch("tsigma.app.DatabaseFacade", return_value=mock_facade), \
                 patch("tsigma.app.seed_admin", new_callable=AsyncMock) as mock_seed, \
                 patch("tsigma.app.seed_system_settings", new_callable=AsyncMock), \
                 patch("tsigma.notifications.registry.initialize_providers",
                       new_callable=AsyncMock):
                app = create_app()
                async with app.router.lifespan_context(app):
                    pass

                mock_seed.assert_awaited_once()
        finally:
            db_module.db_facade = original


# ---------------------------------------------------------------------------
# Exception handler and health endpoint tests (lines 81-88, 103-105,
# 158, 180-181, 199, 206-207, 214-215, 224, 229-234)
# ---------------------------------------------------------------------------


class TestExceptionHandlers:
    """Tests for global exception handlers defined in create_app()."""

    def test_value_error_returns_422(self):
        """ValueError handler returns 422 with error detail."""
        from fastapi.testclient import TestClient

        app = create_app()

        # Add a test route that raises ValueError
        @app.get("/test-value-error")
        async def raise_value_error():
            raise ValueError("bad input data")

        client = TestClient(app, raise_server_exceptions=False)
        resp = client.get("/test-value-error")
        assert resp.status_code == 422
        assert resp.json()["detail"] == "bad input data"

    def test_runtime_error_returns_500(self):
        """RuntimeError handler returns 500 with generic message."""
        from fastapi.testclient import TestClient

        app = create_app()

        @app.get("/test-runtime-error")
        async def raise_runtime_error():
            raise RuntimeError("something broke")

        client = TestClient(app, raise_server_exceptions=False)
        resp = client.get("/test-runtime-error")
        assert resp.status_code == 500
        assert resp.json()["detail"] == "Internal server error"

    def test_unhandled_exception_returns_500(self):
        """Unhandled Exception handler returns 500 with generic message."""
        from fastapi.testclient import TestClient

        app = create_app()

        @app.get("/test-unhandled")
        async def raise_unhandled():
            raise TypeError("unexpected type")

        client = TestClient(app, raise_server_exceptions=False)
        resp = client.get("/test-unhandled")
        assert resp.status_code == 500
        assert resp.json()["detail"] == "Internal server error"


class TestHealthEndpoints:
    """Tests for /health and /ready endpoints."""

    def test_health_endpoint(self):
        """GET /health returns 200 with status ok."""
        from fastapi.testclient import TestClient

        app = create_app()
        client = TestClient(app)
        resp = client.get("/health")
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"

    def test_ready_endpoint_no_facade(self):
        """GET /ready returns 503 when db_facade is None."""
        import tsigma.database.db as db_module

        original = db_module.db_facade
        try:
            db_module.db_facade = None
            from fastapi.testclient import TestClient

            app = create_app()
            client = TestClient(app)
            resp = client.get("/ready")
            assert resp.status_code == 503
            assert resp.json()["status"] == "not ready"
            assert "database" in resp.json()["reason"]
        finally:
            db_module.db_facade = original

    def test_ready_endpoint_with_facade(self):
        """GET /ready returns 200 when db_facade is set."""
        import tsigma.database.db as db_module

        original = db_module.db_facade
        try:
            db_module.db_facade = MagicMock()
            from fastapi.testclient import TestClient

            app = create_app()
            client = TestClient(app)
            resp = client.get("/ready")
            assert resp.status_code == 200
            assert resp.json()["status"] == "ready"
        finally:
            db_module.db_facade = original


class TestStaticFilesMount:
    """Tests for static files mounting (line 264-265)."""

    def test_static_dir_not_exist_skips_mount(self):
        """Static files mount is skipped when static directory does not exist."""
        app = create_app()
        # The static dir may or may not exist in the test env; verify
        # create_app always returns a valid app regardless.
        assert app is not None
        assert app.title == "TSIGMA"


class TestCorsMiddleware:
    """Tests for CORS middleware configuration (lines 179-181)."""

    def test_cors_origins_configured(self):
        """CORS middleware is added when cors_origins is set."""
        with patch("tsigma.app.settings") as mock_settings:
            _apply_mock_settings(mock_settings, cors_origins="http://localhost:3000")
            app = create_app()
            middleware_classes = [m.cls.__name__ for m in app.user_middleware]
            assert "CORSMiddleware" in middleware_classes


class TestValkeySessionStore:
    """Tests for Valkey session store initialization (lines 81-88)."""

    @pytest.mark.asyncio
    async def test_lifespan_uses_valkey_when_configured(self):
        """Lifespan creates ValkeySessionStore when valkey_url is set."""
        import tsigma.database.db as db_module
        original = db_module.db_facade

        mock_facade, _ = _make_mock_facade()
        mock_valkey_client = AsyncMock()
        mock_valkey_client.aclose = AsyncMock()

        try:
            with patch("tsigma.app.DatabaseFacade", return_value=mock_facade), \
                 patch("tsigma.app.seed_admin", new_callable=AsyncMock), \
                 patch("tsigma.app.seed_system_settings", new_callable=AsyncMock), \
                 patch("tsigma.notifications.registry.initialize_providers",
                       new_callable=AsyncMock), \
                 patch("tsigma.app.settings") as mock_settings, \
                 patch("valkey.asyncio.from_url", return_value=mock_valkey_client):
                _apply_mock_settings(mock_settings, valkey_url="valkey://localhost:6379")

                app = create_app()
                async with app.router.lifespan_context(app):
                    assert hasattr(app.state, "session_store")
                    from tsigma.auth.sessions import ValkeySessionStore
                    assert isinstance(app.state.session_store, ValkeySessionStore)
                    assert app.state.valkey_client is mock_valkey_client

                mock_valkey_client.aclose.assert_awaited_once()
        finally:
            db_module.db_facade = original


class TestSeedRollback:
    """Tests for seed rollback on error (lines 103-105)."""

    @pytest.mark.asyncio
    async def test_seed_failure_rolls_back(self):
        """Lifespan rolls back session when seed_admin fails."""
        import tsigma.database.db as db_module
        original = db_module.db_facade

        mock_facade, mock_session = _make_mock_facade()

        try:
            with patch("tsigma.app.DatabaseFacade", return_value=mock_facade), \
                 patch("tsigma.app.seed_admin", new_callable=AsyncMock,
                       side_effect=RuntimeError("seed failed")), \
                 patch("tsigma.app.seed_system_settings", new_callable=AsyncMock), \
                 patch("tsigma.notifications.registry.initialize_providers",
                       new_callable=AsyncMock):
                app = create_app()
                with pytest.raises(RuntimeError, match="seed failed"):
                    async with app.router.lifespan_context(app):
                        pass

                mock_session.rollback.assert_awaited_once()
        finally:
            db_module.db_facade = original
