"""
Unit tests for the system settings API endpoints.

Tests GET/PUT /api/v1/settings/access-policy and related admin-only routes.
"""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

from fastapi import FastAPI
from fastapi.testclient import TestClient

from tsigma.api.v1.settings import router
from tsigma.auth.sessions import SessionData
from tsigma.models.system_setting import SystemSetting


def _create_test_app():
    """Create a minimal FastAPI app with the settings router."""
    app = FastAPI()
    app.include_router(router, prefix="/api/v1/settings")
    return app


def _admin_user():
    """Return a SessionData for an admin user."""
    return SessionData(
        user_id=uuid4(),
        username="admin",
        role="admin",
        created_at=datetime.now(timezone.utc),
        expires_at=datetime.now(timezone.utc),
    )


def _mock_setting(key: str, value: str, category: str = "access_policy",
                   editable: bool = True, **overrides):
    """Create a mock SystemSetting row."""
    mock = MagicMock(spec=SystemSetting)
    mock.key = key
    mock.value = value
    mock.category = category
    mock.description = overrides.get("description", f"Setting for {key}")
    mock.editable = editable
    mock.updated_at = overrides.get("updated_at", datetime.now(timezone.utc))
    mock.updated_by = overrides.get("updated_by", None)
    return mock


def _setup_app_with_admin(mock_session):
    """Create test app with session + admin overrides."""
    app = _create_test_app()

    from tsigma.auth.dependencies import require_admin
    from tsigma.dependencies import get_session

    async def override_session():
        yield mock_session

    app.dependency_overrides[get_session] = override_session
    app.dependency_overrides[require_admin] = lambda: _admin_user()

    return app


# ---------------------------------------------------------------------------
# GET /api/v1/settings/
# ---------------------------------------------------------------------------

class TestListSettings:
    """Tests for GET /api/v1/settings/."""

    def test_returns_all_settings(self):
        """Test listing all settings."""
        settings = [
            _mock_setting("access_policy.analytics", "authenticated"),
            _mock_setting("access_policy.reports", "public"),
        ]
        mock_session = AsyncMock()
        result = MagicMock()
        result.scalars.return_value.all.return_value = settings
        mock_session.execute = AsyncMock(return_value=result)

        app = _setup_app_with_admin(mock_session)
        client = TestClient(app)
        resp = client.get("/api/v1/settings/")

        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 2

    def test_filter_by_category(self):
        """Test filtering settings by category."""
        settings = [_mock_setting("access_policy.analytics", "authenticated")]
        mock_session = AsyncMock()
        result = MagicMock()
        result.scalars.return_value.all.return_value = settings
        mock_session.execute = AsyncMock(return_value=result)

        app = _setup_app_with_admin(mock_session)
        client = TestClient(app)
        resp = client.get("/api/v1/settings/?category=access_policy")

        assert resp.status_code == 200

    def test_requires_admin(self):
        """Test 401/403 when not admin."""
        mock_session = AsyncMock()
        app = _create_test_app()

        from tsigma.auth.dependencies import require_admin
        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session
        # No admin override — will trigger the real dependency chain error

        # Without any auth setup, the dependency will fail
        # We simulate by making require_admin raise 401
        from fastapi import HTTPException, status

        def raise_401():
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Not authenticated",
            )

        app.dependency_overrides[require_admin] = raise_401

        client = TestClient(app)
        resp = client.get("/api/v1/settings/")
        assert resp.status_code == 401


# ---------------------------------------------------------------------------
# GET /api/v1/settings/access-policy
# ---------------------------------------------------------------------------

class TestGetAccessPolicy:
    """Tests for GET /api/v1/settings/access-policy."""

    def test_returns_current_policy(self):
        """Test retrieving current access policy."""
        mock_session = AsyncMock()

        app = _setup_app_with_admin(mock_session)

        with patch("tsigma.api.v1.settings.settings_cache") as mock_cache:
            mock_cache.get_by_category = AsyncMock(return_value={
                "access_policy.analytics": "public",
                "access_policy.reports": "authenticated",
                "access_policy.signal_detail": "authenticated",
                "access_policy.health": "public",
                "access_policy.management": "authenticated",
            })

            client = TestClient(app)
            resp = client.get("/api/v1/settings/access-policy")

        assert resp.status_code == 200
        data = resp.json()
        assert data["analytics"] == "public"
        assert data["reports"] == "authenticated"
        assert data["health"] == "public"
        assert data["management"] == "authenticated"

    def test_defaults_when_cache_empty(self):
        """Test defaults to 'authenticated' when cache is empty."""
        mock_session = AsyncMock()

        app = _setup_app_with_admin(mock_session)

        with patch("tsigma.api.v1.settings.settings_cache") as mock_cache:
            mock_cache.get_by_category = AsyncMock(return_value={})

            client = TestClient(app)
            resp = client.get("/api/v1/settings/access-policy")

        assert resp.status_code == 200
        data = resp.json()
        for cat in ("analytics", "reports", "signal_detail", "health", "management"):
            assert data[cat] == "authenticated"


# ---------------------------------------------------------------------------
# PUT /api/v1/settings/access-policy
# ---------------------------------------------------------------------------

class TestUpdateAccessPolicy:
    """Tests for PUT /api/v1/settings/access-policy."""

    def test_updates_single_category(self):
        """Test updating analytics to public."""
        setting = _mock_setting("access_policy.analytics", "authenticated")
        mock_session = AsyncMock()
        result = MagicMock()
        result.scalar_one_or_none.return_value = setting
        mock_session.execute = AsyncMock(return_value=result)

        app = _setup_app_with_admin(mock_session)

        with patch("tsigma.api.v1.settings.settings_cache") as mock_cache:
            mock_cache.get_by_category = AsyncMock(return_value={
                "access_policy.analytics": "public",
                "access_policy.reports": "authenticated",
                "access_policy.signal_detail": "authenticated",
                "access_policy.health": "authenticated",
                "access_policy.management": "authenticated",
            })

            client = TestClient(app)
            resp = client.put(
                "/api/v1/settings/access-policy",
                json={"analytics": "public"},
            )

        assert resp.status_code == 200
        mock_cache.invalidate.assert_called_once()

    def test_rejects_invalid_value(self):
        """Test 422 for invalid access value."""
        mock_session = AsyncMock()
        app = _setup_app_with_admin(mock_session)

        client = TestClient(app)
        resp = client.put(
            "/api/v1/settings/access-policy",
            json={"analytics": "open"},
        )
        assert resp.status_code == 422

    def test_rejects_empty_update(self):
        """Test 422 when no fields provided."""
        mock_session = AsyncMock()
        app = _setup_app_with_admin(mock_session)

        client = TestClient(app)
        resp = client.put(
            "/api/v1/settings/access-policy",
            json={},
        )
        assert resp.status_code == 422

    def test_management_not_in_update_schema(self):
        """Test management field is not in AccessPolicyUpdate schema."""
        # The schema doesn't have management field, so it's simply ignored
        setting = _mock_setting("access_policy.analytics", "authenticated")
        mock_session = AsyncMock()
        result = MagicMock()
        result.scalar_one_or_none.return_value = setting
        mock_session.execute = AsyncMock(return_value=result)

        app = _setup_app_with_admin(mock_session)

        with patch("tsigma.api.v1.settings.settings_cache") as mock_cache:
            mock_cache.get_by_category = AsyncMock(return_value={
                "access_policy.analytics": "public",
                "access_policy.reports": "authenticated",
                "access_policy.signal_detail": "authenticated",
                "access_policy.health": "authenticated",
                "access_policy.management": "authenticated",
            })

            client = TestClient(app)
            # management is not in the schema, only analytics is valid here
            resp = client.put(
                "/api/v1/settings/access-policy",
                json={"analytics": "public", "management": "public"},
            )

        # management is silently ignored since it's not in the Pydantic model
        assert resp.status_code == 200

    def test_updates_multiple_categories(self):
        """Test updating multiple categories at once."""
        analytics_setting = _mock_setting("access_policy.analytics", "authenticated")
        reports_setting = _mock_setting("access_policy.reports", "authenticated")
        mock_session = AsyncMock()
        mock_session.execute = AsyncMock(side_effect=[
            MagicMock(scalar_one_or_none=MagicMock(return_value=analytics_setting)),
            MagicMock(scalar_one_or_none=MagicMock(return_value=reports_setting)),
        ])

        app = _setup_app_with_admin(mock_session)

        with patch("tsigma.api.v1.settings.settings_cache") as mock_cache:
            mock_cache.get_by_category = AsyncMock(return_value={
                "access_policy.analytics": "public",
                "access_policy.reports": "public",
                "access_policy.signal_detail": "authenticated",
                "access_policy.health": "authenticated",
                "access_policy.management": "authenticated",
            })

            client = TestClient(app)
            resp = client.put(
                "/api/v1/settings/access-policy",
                json={"analytics": "public", "reports": "public"},
            )

        assert resp.status_code == 200


# ---------------------------------------------------------------------------
# PUT /api/v1/settings/{setting_key}
# ---------------------------------------------------------------------------

class TestUpdateSetting:
    """Tests for PUT /api/v1/settings/{setting_key}."""

    def test_updates_editable_setting(self):
        """Test updating an editable setting."""
        setting = _mock_setting("access_policy.analytics", "authenticated")
        mock_session = AsyncMock()
        result = MagicMock()
        result.scalar_one_or_none.return_value = setting
        mock_session.execute = AsyncMock(return_value=result)

        app = _setup_app_with_admin(mock_session)

        with patch("tsigma.api.v1.settings.settings_cache"):
            client = TestClient(app)
            resp = client.put(
                "/api/v1/settings/access_policy.analytics",
                json={"value": "public"},
            )

        assert resp.status_code == 200
        assert setting.value == "public"

    def test_rejects_non_editable_setting(self):
        """Test 403 when trying to update a non-editable setting."""
        setting = _mock_setting(
            "access_policy.management", "authenticated", editable=False,
        )
        mock_session = AsyncMock()
        result = MagicMock()
        result.scalar_one_or_none.return_value = setting
        mock_session.execute = AsyncMock(return_value=result)

        app = _setup_app_with_admin(mock_session)

        client = TestClient(app)
        resp = client.put(
            "/api/v1/settings/access_policy.management",
            json={"value": "public"},
        )
        assert resp.status_code == 403

    def test_returns_404_for_unknown_key(self):
        """Test 404 when setting key doesn't exist."""
        mock_session = AsyncMock()
        result = MagicMock()
        result.scalar_one_or_none.return_value = None
        mock_session.execute = AsyncMock(return_value=result)

        app = _setup_app_with_admin(mock_session)

        client = TestClient(app)
        resp = client.put(
            "/api/v1/settings/nonexistent.key",
            json={"value": "anything"},
        )
        assert resp.status_code == 404

    def test_rejects_invalid_access_policy_value(self):
        """Test 422 for invalid value on access_policy category."""
        setting = _mock_setting("access_policy.analytics", "authenticated")
        mock_session = AsyncMock()
        result = MagicMock()
        result.scalar_one_or_none.return_value = setting
        mock_session.execute = AsyncMock(return_value=result)

        app = _setup_app_with_admin(mock_session)

        client = TestClient(app)
        resp = client.put(
            "/api/v1/settings/access_policy.analytics",
            json={"value": "open"},
        )
        assert resp.status_code == 422

    def test_invalidates_cache_on_update(self):
        """Test cache is invalidated after a setting update."""
        setting = _mock_setting("access_policy.analytics", "authenticated")
        mock_session = AsyncMock()
        result = MagicMock()
        result.scalar_one_or_none.return_value = setting
        mock_session.execute = AsyncMock(return_value=result)

        app = _setup_app_with_admin(mock_session)

        with patch("tsigma.api.v1.settings.settings_cache") as mock_cache:
            client = TestClient(app)
            resp = client.put(
                "/api/v1/settings/access_policy.analytics",
                json={"value": "public"},
            )

        assert resp.status_code == 200
        mock_cache.invalidate.assert_called_once()
