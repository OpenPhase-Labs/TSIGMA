"""
Unit tests for the system settings API endpoints.

Tests GET/PUT /api/v1/settings/access-policy and related admin-only routes.
"""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

from fastapi import FastAPI
from fastapi.testclient import TestClient

from tests._helpers import make_mock_session
from tsigma.api.v1.settings import router
from tsigma.auth.sessions import SessionData
from tsigma.models.system_setting import SystemSetting
from tsigma.models.system_setting_audit import SystemSettingAudit


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
        mock_session = make_mock_session()
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
        mock_session = make_mock_session()
        result = MagicMock()
        result.scalars.return_value.all.return_value = settings
        mock_session.execute = AsyncMock(return_value=result)

        app = _setup_app_with_admin(mock_session)
        client = TestClient(app)
        resp = client.get("/api/v1/settings/?category=access_policy")

        assert resp.status_code == 200

    def test_requires_admin(self):
        """Test 401/403 when not admin."""
        mock_session = make_mock_session()
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
        mock_session = make_mock_session()

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
        mock_session = make_mock_session()

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
        mock_session = make_mock_session()
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
        mock_session = make_mock_session()
        app = _setup_app_with_admin(mock_session)

        client = TestClient(app)
        resp = client.put(
            "/api/v1/settings/access-policy",
            json={"analytics": "open"},
        )
        assert resp.status_code == 422

    def test_rejects_empty_update(self):
        """Test 422 when no fields provided."""
        mock_session = make_mock_session()
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
        mock_session = make_mock_session()
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
        mock_session = make_mock_session()
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
        mock_session = make_mock_session()
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
        mock_session = make_mock_session()
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
        mock_session = make_mock_session()
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
        mock_session = make_mock_session()
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
        mock_session = make_mock_session()
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


# ---------------------------------------------------------------------------
# A3 helpers — audit-row inspection
# ---------------------------------------------------------------------------

def _added_audit_rows(mock_session) -> list[SystemSettingAudit]:
    """Return SystemSettingAudit instances passed to session.add()."""
    rows: list[SystemSettingAudit] = []
    for call in mock_session.add.call_args_list:
        args, _ = call
        if args and isinstance(args[0], SystemSettingAudit):
            rows.append(args[0])
    return rows


def _mock_audit_row(
    *,
    audit_id: int,
    key: str,
    old_value: str | None,
    new_value: str,
    changed_at: datetime,
    changed_by: str,
    reason: str | None = None,
) -> SystemSettingAudit:
    """Build a MagicMock(spec=SystemSettingAudit) with audit-row attributes."""
    row = MagicMock(spec=SystemSettingAudit)
    row.id = audit_id
    row.key = key
    row.old_value = old_value
    row.new_value = new_value
    row.changed_at = changed_at
    row.changed_by = changed_by
    row.reason = reason
    return row


def _make_audit_session(audit_rows: list[SystemSettingAudit]):
    """Mock session for GET /{key}/audit — single execute() returns the audit rows."""
    mock_session = make_mock_session()
    result = MagicMock()
    result.scalars.return_value.all.return_value = audit_rows
    mock_session.execute = AsyncMock(return_value=result)
    return mock_session


# ---------------------------------------------------------------------------
# A3: PUT /api/v1/settings/{key} writes audit
# ---------------------------------------------------------------------------

class TestUpdateSettingWritesAudit:
    """Tests for audit-row insert on PUT /api/v1/settings/{setting_key}."""

    def test_put_setting_writes_audit_row_with_admin_username(self):
        """Successful PUT writes a SystemSettingAudit with changed_by = admin.username."""
        setting = _mock_setting("access_policy.analytics", "authenticated")
        mock_session = make_mock_session()
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
        audit_rows = _added_audit_rows(mock_session)
        assert len(audit_rows) == 1
        assert audit_rows[0].key == "access_policy.analytics"
        # The admin fixture uses username="admin" (see _admin_user()).
        assert audit_rows[0].changed_by == "admin"

    def test_put_setting_captures_old_value(self):
        """PUT captures the prior row's value as the audit row's old_value."""
        setting = _mock_setting("access_policy.analytics", "authenticated")
        mock_session = make_mock_session()
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
        audit_rows = _added_audit_rows(mock_session)
        assert len(audit_rows) == 1
        assert audit_rows[0].old_value == "authenticated"
        assert audit_rows[0].new_value == "public"

    def test_put_setting_optional_reason_flows_to_audit(self):
        """Optional 'reason' body field flows through to the audit row."""
        setting = _mock_setting("access_policy.analytics", "authenticated")
        mock_session = make_mock_session()
        result = MagicMock()
        result.scalar_one_or_none.return_value = setting
        mock_session.execute = AsyncMock(return_value=result)

        app = _setup_app_with_admin(mock_session)

        with patch("tsigma.api.v1.settings.settings_cache"):
            client = TestClient(app)
            resp = client.put(
                "/api/v1/settings/access_policy.analytics",
                json={"value": "public", "reason": "audit demo"},
            )

        assert resp.status_code == 200
        audit_rows = _added_audit_rows(mock_session)
        assert len(audit_rows) == 1
        assert audit_rows[0].reason == "audit demo"

    def test_put_setting_404_writes_no_audit_row(self):
        """A 404 (unknown key) writes ZERO audit rows."""
        mock_session = make_mock_session()
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
        assert _added_audit_rows(mock_session) == []

    def test_put_setting_403_writes_no_audit_row(self):
        """A 403 (non-editable) writes ZERO audit rows."""
        setting = _mock_setting(
            "access_policy.management", "authenticated", editable=False,
        )
        mock_session = make_mock_session()
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
        assert _added_audit_rows(mock_session) == []

    def test_put_setting_422_writes_no_audit_row(self):
        """A 422 (invalid access_policy value) writes ZERO audit rows."""
        setting = _mock_setting("access_policy.analytics", "authenticated")
        mock_session = make_mock_session()
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
        assert _added_audit_rows(mock_session) == []

    def test_put_setting_401_writes_no_audit_row(self):
        """A 401 (unauthenticated) writes ZERO audit rows."""
        mock_session = make_mock_session()

        app = _create_test_app()
        from tsigma.auth.dependencies import require_admin
        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session
        from fastapi import HTTPException, status

        def raise_401():
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Not authenticated",
            )

        app.dependency_overrides[require_admin] = raise_401

        client = TestClient(app)
        resp = client.put(
            "/api/v1/settings/access_policy.analytics",
            json={"value": "public"},
        )

        assert resp.status_code == 401
        assert _added_audit_rows(mock_session) == []


# ---------------------------------------------------------------------------
# A3: PUT /api/v1/settings/access-policy writes audit
# ---------------------------------------------------------------------------

class TestUpdateAccessPolicyWritesAudit:
    """Tests for audit-row insert on PUT /api/v1/settings/access-policy."""

    def test_put_access_policy_writes_one_audit_row_per_modified_category(self):
        """PUT updating 3 categories writes 3 audit rows, one per category key."""
        analytics_setting = _mock_setting("access_policy.analytics", "authenticated")
        reports_setting = _mock_setting("access_policy.reports", "authenticated")
        health_setting = _mock_setting("access_policy.health", "authenticated")
        mock_session = make_mock_session()
        mock_session.execute = AsyncMock(side_effect=[
            MagicMock(scalar_one_or_none=MagicMock(return_value=analytics_setting)),
            MagicMock(scalar_one_or_none=MagicMock(return_value=reports_setting)),
            MagicMock(scalar_one_or_none=MagicMock(return_value=health_setting)),
        ])

        app = _setup_app_with_admin(mock_session)

        with patch("tsigma.api.v1.settings.settings_cache") as mock_cache:
            mock_cache.get_by_category = AsyncMock(return_value={
                "access_policy.analytics": "public",
                "access_policy.reports": "public",
                "access_policy.signal_detail": "authenticated",
                "access_policy.health": "public",
                "access_policy.management": "authenticated",
            })

            client = TestClient(app)
            resp = client.put(
                "/api/v1/settings/access-policy",
                json={
                    "analytics": "public",
                    "reports": "public",
                    "health": "public",
                },
            )

        assert resp.status_code == 200
        audit_rows = _added_audit_rows(mock_session)
        assert len(audit_rows) == 3
        keys_seen = {r.key for r in audit_rows}
        assert keys_seen == {
            "access_policy.analytics",
            "access_policy.reports",
            "access_policy.health",
        }

    def test_put_access_policy_shared_reason_flows_to_all_audit_rows(self):
        """Top-level 'reason' body field flows through to every audit row."""
        analytics_setting = _mock_setting("access_policy.analytics", "authenticated")
        reports_setting = _mock_setting("access_policy.reports", "authenticated")
        mock_session = make_mock_session()
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
                json={
                    "analytics": "public",
                    "reports": "public",
                    "reason": "opening telemetry for pilot",
                },
            )

        assert resp.status_code == 200
        audit_rows = _added_audit_rows(mock_session)
        assert len(audit_rows) == 2
        for row in audit_rows:
            assert row.reason == "opening telemetry for pilot"

    def test_put_access_policy_422_empty_update_writes_no_audit(self):
        """422 for empty update body writes ZERO audit rows."""
        mock_session = make_mock_session()

        app = _setup_app_with_admin(mock_session)

        client = TestClient(app)
        resp = client.put(
            "/api/v1/settings/access-policy",
            json={},
        )

        assert resp.status_code == 422
        assert _added_audit_rows(mock_session) == []

    def test_put_access_policy_422_invalid_value_writes_no_audit(self):
        """422 for invalid (non-public/non-authenticated) value writes ZERO audit rows."""
        mock_session = make_mock_session()

        app = _setup_app_with_admin(mock_session)

        client = TestClient(app)
        resp = client.put(
            "/api/v1/settings/access-policy",
            json={"analytics": "open"},
        )

        assert resp.status_code == 422
        assert _added_audit_rows(mock_session) == []

    def test_put_access_policy_management_category_writes_no_audit(self):
        """PUT cannot modify the locked 'management' category — no audit row for it.

        Per ``test_management_not_in_update_schema``, the existing
        AccessPolicyUpdate schema does not include 'management', so the field
        is silently dropped before any update loop runs. We assert that NO
        audit row carries key='access_policy.management'.
        """
        analytics_setting = _mock_setting("access_policy.analytics", "authenticated")
        mock_session = make_mock_session()
        result = MagicMock()
        result.scalar_one_or_none.return_value = analytics_setting
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
                json={"analytics": "public", "management": "public"},
            )

        assert resp.status_code == 200
        audit_rows = _added_audit_rows(mock_session)
        keys_seen = {r.key for r in audit_rows}
        assert "access_policy.management" not in keys_seen, (
            f"Locked 'management' category must never produce an audit row; "
            f"got keys: {keys_seen}"
        )

    def test_put_access_policy_403_writes_no_audit(self):
        """403 (non-admin) writes ZERO audit rows."""
        mock_session = make_mock_session()

        app = _create_test_app()
        from tsigma.auth.dependencies import require_admin
        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session
        from fastapi import HTTPException, status

        def raise_403():
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Admin role required",
            )

        app.dependency_overrides[require_admin] = raise_403

        client = TestClient(app)
        resp = client.put(
            "/api/v1/settings/access-policy",
            json={"analytics": "public"},
        )

        assert resp.status_code == 403
        assert _added_audit_rows(mock_session) == []

    def test_put_access_policy_401_writes_no_audit(self):
        """401 (unauthenticated) writes ZERO audit rows."""
        mock_session = make_mock_session()

        app = _create_test_app()
        from tsigma.auth.dependencies import require_admin
        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session
        from fastapi import HTTPException, status

        def raise_401():
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Not authenticated",
            )

        app.dependency_overrides[require_admin] = raise_401

        client = TestClient(app)
        resp = client.put(
            "/api/v1/settings/access-policy",
            json={"analytics": "public"},
        )

        assert resp.status_code == 401
        assert _added_audit_rows(mock_session) == []


# ---------------------------------------------------------------------------
# A3: GET /api/v1/settings/{key}/audit
# ---------------------------------------------------------------------------

class TestGetSettingAudit:
    """Tests for GET /api/v1/settings/{key}/audit.

    Mirrors the canonical audit-endpoint pattern at
    ``tsigma/api/v1/signals.py:261-302``: skip/limit pagination, ORDER BY
    changed_at DESC, list[dict] return shape, admin-only.
    """

    def test_get_audit_returns_rows_for_key(self):
        """GET returns a list of audit dicts for the requested key."""
        now = datetime.now(timezone.utc)
        rows = [
            _mock_audit_row(
                audit_id=3, key="api.max_page_size", old_value="500",
                new_value="1000", changed_at=now, changed_by="alice",
                reason="bump",
            ),
            _mock_audit_row(
                audit_id=2, key="api.max_page_size", old_value="250",
                new_value="500", changed_at=now, changed_by="bob",
            ),
            _mock_audit_row(
                audit_id=1, key="api.max_page_size", old_value=None,
                new_value="250", changed_at=now, changed_by="carol",
            ),
        ]
        mock_session = _make_audit_session(rows)
        app = _setup_app_with_admin(mock_session)

        client = TestClient(app)
        resp = client.get("/api/v1/settings/api.max_page_size/audit")

        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)
        assert len(data) == 3

    def test_get_audit_returns_empty_list_when_no_rows(self):
        """GET returns 200 with [] (not 404) when no audit rows exist."""
        mock_session = _make_audit_session([])
        app = _setup_app_with_admin(mock_session)

        client = TestClient(app)
        resp = client.get("/api/v1/settings/api.max_page_size/audit")

        assert resp.status_code == 200
        assert resp.json() == []

    def test_get_audit_orders_by_changed_at_desc(self):
        """GET returns rows newest-first (ORDER BY changed_at DESC)."""
        # Mock the rows in the DESC order the endpoint should emit;
        # the test confirms the endpoint preserves that order in the JSON.
        t_new = datetime(2026, 5, 15, 12, 0, 0, tzinfo=timezone.utc)
        t_mid = datetime(2026, 5, 15, 11, 0, 0, tzinfo=timezone.utc)
        t_old = datetime(2026, 5, 15, 10, 0, 0, tzinfo=timezone.utc)
        rows_desc = [
            _mock_audit_row(
                audit_id=30, key="api.max_page_size", old_value="500",
                new_value="1000", changed_at=t_new, changed_by="alice",
            ),
            _mock_audit_row(
                audit_id=20, key="api.max_page_size", old_value="250",
                new_value="500", changed_at=t_mid, changed_by="bob",
            ),
            _mock_audit_row(
                audit_id=10, key="api.max_page_size", old_value=None,
                new_value="250", changed_at=t_old, changed_by="carol",
            ),
        ]
        mock_session = _make_audit_session(rows_desc)
        app = _setup_app_with_admin(mock_session)

        client = TestClient(app)
        resp = client.get("/api/v1/settings/api.max_page_size/audit")

        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 3
        # Newest-first: changed_at values should descend through the list.
        timestamps = [item["changed_at"] for item in data]
        assert timestamps == sorted(timestamps, reverse=True), (
            f"Audit rows must be newest-first; got {timestamps}"
        )

        # Also verify the underlying SQL applied .order_by(...desc()).
        from tsigma.models.system_setting_audit import SystemSettingAudit as Model
        executed_stmt = mock_session.execute.await_args.args[0]
        compiled = str(executed_stmt.compile(
            compile_kwargs={"literal_binds": True}
        ))
        # The endpoint must order by changed_at DESC.
        assert "ORDER BY" in compiled.upper()
        assert "DESC" in compiled.upper()
        # And it must be selecting from system_setting_audit.
        assert Model.__tablename__ in compiled

    def test_get_audit_default_limit_is_50(self):
        """GET with no ?limit returns up to 50 rows (default per plan A3.3)."""
        # Build 75 rows. The endpoint must apply .limit(50) to the SQL
        # statement; we verify by inspecting the compiled statement, not by
        # relying on a real DB to truncate.
        now = datetime.now(timezone.utc)
        rows = [
            _mock_audit_row(
                audit_id=i, key="api.max_page_size", old_value="0",
                new_value=str(i), changed_at=now, changed_by="alice",
            )
            for i in range(75, 0, -1)
        ]
        mock_session = _make_audit_session(rows)
        app = _setup_app_with_admin(mock_session)

        client = TestClient(app)
        resp = client.get("/api/v1/settings/api.max_page_size/audit")

        assert resp.status_code == 200

        executed_stmt = mock_session.execute.await_args.args[0]
        # SQLAlchemy Select exposes ._limit_clause for the LIMIT expression.
        limit_clause = executed_stmt._limit_clause
        assert limit_clause is not None, (
            "Endpoint must apply a LIMIT to the audit query"
        )
        # The value attribute carries the literal int when limit() was
        # called with a positional int.
        assert getattr(limit_clause, "value", None) == 50, (
            f"Default LIMIT must be 50 per plan A3.3; "
            f"got {getattr(limit_clause, 'value', limit_clause)!r}"
        )

    def test_get_audit_skip_works(self):
        """GET with ?skip=N applies OFFSET N to the SQL."""
        now = datetime.now(timezone.utc)
        rows = [
            _mock_audit_row(
                audit_id=i, key="api.max_page_size", old_value="0",
                new_value=str(i), changed_at=now, changed_by="alice",
            )
            for i in range(10, 0, -1)
        ]
        mock_session = _make_audit_session(rows)
        app = _setup_app_with_admin(mock_session)

        client = TestClient(app)
        resp = client.get("/api/v1/settings/api.max_page_size/audit?skip=5")

        assert resp.status_code == 200

        executed_stmt = mock_session.execute.await_args.args[0]
        offset_clause = executed_stmt._offset_clause
        assert offset_clause is not None, (
            "Endpoint must apply an OFFSET when skip is supplied"
        )
        assert getattr(offset_clause, "value", None) == 5, (
            f"OFFSET must equal skip=5; "
            f"got {getattr(offset_clause, 'value', offset_clause)!r}"
        )

    def test_get_audit_limit_works(self):
        """GET with ?limit=N applies LIMIT N to the SQL."""
        now = datetime.now(timezone.utc)
        rows = [
            _mock_audit_row(
                audit_id=i, key="api.max_page_size", old_value="0",
                new_value=str(i), changed_at=now, changed_by="alice",
            )
            for i in range(100, 0, -1)
        ]
        mock_session = _make_audit_session(rows)
        app = _setup_app_with_admin(mock_session)

        client = TestClient(app)
        resp = client.get("/api/v1/settings/api.max_page_size/audit?limit=10")

        assert resp.status_code == 200

        executed_stmt = mock_session.execute.await_args.args[0]
        limit_clause = executed_stmt._limit_clause
        assert limit_clause is not None
        assert getattr(limit_clause, "value", None) == 10, (
            f"LIMIT must equal limit=10; "
            f"got {getattr(limit_clause, 'value', limit_clause)!r}"
        )

    def test_get_audit_returns_dict_shape(self):
        """GET response items are dicts with keys per plan A3.3 spec.

        Plan: ``list[dict]`` (mirror signals-audit; no new Pydantic response
        model). Required keys: ``id``, ``key``, ``old_value``, ``new_value``,
        ``changed_at``, ``changed_by``, ``reason``.
        """
        now = datetime(2026, 5, 15, 12, 0, 0, tzinfo=timezone.utc)
        rows = [
            _mock_audit_row(
                audit_id=42,
                key="api.max_page_size",
                old_value="500",
                new_value="1000",
                changed_at=now,
                changed_by="alice",
                reason="bump for prod",
            ),
        ]
        mock_session = _make_audit_session(rows)
        app = _setup_app_with_admin(mock_session)

        client = TestClient(app)
        resp = client.get("/api/v1/settings/api.max_page_size/audit")

        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        item = data[0]
        # Required keys from plan A3.3.
        required_keys = {
            "id", "key", "old_value", "new_value",
            "changed_at", "changed_by", "reason",
        }
        missing = required_keys - set(item.keys())
        assert not missing, (
            f"Audit dict missing required keys: {missing}; got {set(item.keys())}"
        )
        assert item["id"] == 42
        assert item["key"] == "api.max_page_size"
        assert item["old_value"] == "500"
        assert item["new_value"] == "1000"
        assert item["changed_by"] == "alice"
        assert item["reason"] == "bump for prod"

    def test_get_audit_requires_admin_403_for_non_admin(self):
        """Non-admin gets 403."""
        mock_session = _make_audit_session([])

        app = _create_test_app()
        from tsigma.auth.dependencies import require_admin
        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session
        from fastapi import HTTPException, status

        def raise_403():
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Admin role required",
            )

        app.dependency_overrides[require_admin] = raise_403

        client = TestClient(app)
        resp = client.get("/api/v1/settings/api.max_page_size/audit")
        assert resp.status_code == 403

    def test_get_audit_requires_admin_401_for_unauthenticated(self):
        """Unauthenticated gets 401."""
        mock_session = _make_audit_session([])

        app = _create_test_app()
        from tsigma.auth.dependencies import require_admin
        from tsigma.dependencies import get_session

        async def override_session():
            yield mock_session

        app.dependency_overrides[get_session] = override_session
        from fastapi import HTTPException, status

        def raise_401():
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Not authenticated",
            )

        app.dependency_overrides[require_admin] = raise_401

        client = TestClient(app)
        resp = client.get("/api/v1/settings/api.max_page_size/audit")
        assert resp.status_code == 401
