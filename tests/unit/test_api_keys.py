"""
Unit tests for API key authentication.

Tests model fields, key generation/validation, and API endpoints
for creating, listing, and revoking API keys.
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from tsigma.auth.dependencies import get_current_user, get_session_store
from tsigma.auth.models import ApiKey, UserRole
from tsigma.auth.router import router as auth_router
from tsigma.auth.sessions import InMemorySessionStore, SessionData
from tsigma.dependencies import get_session

# ---------------------------------------------------------------------------
# Model tests
# ---------------------------------------------------------------------------


class TestApiKeyModel:
    """Tests for ApiKey SQLAlchemy model."""

    def test_has_correct_tablename(self):
        """ApiKey model uses 'api_key' table."""
        assert ApiKey.__tablename__ == "api_key"

    def test_has_required_columns(self):
        """ApiKey model has all required columns."""
        col_names = {c.name for c in ApiKey.__table__.columns}
        expected = {
            "id", "user_id", "name", "key_hash", "key_prefix",
            "role", "created_at", "expires_at", "revoked_at",
            "last_used_at", "updated_at",
        }
        assert expected.issubset(col_names)

    def test_instantiate(self):
        """ApiKey can be instantiated with required fields."""
        user_id = uuid4()
        key = ApiKey(
            user_id=user_id,
            name="CI pipeline",
            key_hash="$2b$12$fakehash",
            key_prefix="tsgm_abc1",
            role=UserRole.VIEWER,
        )
        assert key.user_id == user_id
        assert key.name == "CI pipeline"
        assert key.role == UserRole.VIEWER

    def test_expires_at_nullable(self):
        """expires_at column is nullable (keys can be non-expiring)."""
        col = ApiKey.__table__.c.expires_at
        assert col.nullable is True

    def test_revoked_at_nullable(self):
        """revoked_at column is nullable (keys start un-revoked)."""
        col = ApiKey.__table__.c.revoked_at
        assert col.nullable is True

    def test_last_used_at_nullable(self):
        """last_used_at column is nullable (never used yet)."""
        col = ApiKey.__table__.c.last_used_at
        assert col.nullable is True

    def test_user_id_not_nullable(self):
        """user_id is required."""
        col = ApiKey.__table__.c.user_id
        assert col.nullable is False

    def test_key_hash_not_nullable(self):
        """key_hash is required."""
        col = ApiKey.__table__.c.key_hash
        assert col.nullable is False


# ---------------------------------------------------------------------------
# Service tests
# ---------------------------------------------------------------------------


class TestGenerateApiKey:
    """Tests for generate_api_key service function."""

    @pytest.mark.asyncio
    async def test_returns_id_and_plaintext(self):
        """generate_api_key returns (key_id, plaintext_key)."""
        from tsigma.auth.api_keys import generate_api_key

        mock_session = AsyncMock()
        user_id = uuid4()

        key_id, plaintext = await generate_api_key(
            user_id=user_id,
            name="test key",
            role="admin",
            session=mock_session,
        )

        assert key_id is not None
        assert isinstance(plaintext, str)
        assert plaintext.startswith("tsgm_")
        # Plaintext should be long enough for security
        assert len(plaintext) > 20

    @pytest.mark.asyncio
    async def test_stores_hash_not_plaintext(self):
        """The stored key_hash must differ from the plaintext."""
        from tsigma.auth.api_keys import generate_api_key

        mock_session = AsyncMock()
        user_id = uuid4()

        _key_id, plaintext = await generate_api_key(
            user_id=user_id,
            name="test key",
            role="admin",
            session=mock_session,
        )

        # The session.add call should have been made with an ApiKey
        added_obj = mock_session.add.call_args[0][0]
        assert isinstance(added_obj, ApiKey)
        assert added_obj.key_hash != plaintext
        assert added_obj.key_prefix == plaintext[:12]

    @pytest.mark.asyncio
    async def test_optional_expiration(self):
        """generate_api_key accepts optional expires_at."""
        from tsigma.auth.api_keys import generate_api_key

        mock_session = AsyncMock()
        user_id = uuid4()
        future = datetime.now(timezone.utc) + timedelta(days=30)

        key_id, _plaintext = await generate_api_key(
            user_id=user_id,
            name="expiring key",
            role="viewer",
            session=mock_session,
            expires_at=future,
        )

        added_obj = mock_session.add.call_args[0][0]
        assert added_obj.expires_at == future


class TestValidateApiKey:
    """Tests for validate_api_key service function."""

    @pytest.mark.asyncio
    async def test_valid_key_returns_session_data(self):
        """A valid, non-expired, non-revoked key returns SessionData."""
        from tsigma.auth.api_keys import generate_api_key, validate_api_key

        mock_session = AsyncMock()
        user_id = uuid4()

        _key_id, plaintext = await generate_api_key(
            user_id=user_id,
            name="valid key",
            role="admin",
            session=mock_session,
        )

        # Capture the stored ApiKey object
        stored_key = mock_session.add.call_args[0][0]
        stored_key.user_id = user_id
        stored_key.revoked_at = None
        stored_key.expires_at = None
        stored_key.last_used_at = None

        # Mock the query to return our stored key
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = [stored_key]
        mock_session.execute.return_value = mock_result

        # Also mock the username lookup
        mock_user = MagicMock()
        mock_user.username = "admin"

        with patch("tsigma.auth.api_keys._lookup_username", return_value="admin"):
            result = await validate_api_key(plaintext, mock_session)

        assert result is not None
        assert result.user_id == user_id
        assert result.role == "admin"

    @pytest.mark.asyncio
    async def test_expired_key_returns_none(self):
        """An expired key returns None."""
        from tsigma.auth.api_keys import generate_api_key, validate_api_key

        mock_session = AsyncMock()
        user_id = uuid4()
        past = datetime.now(timezone.utc) - timedelta(hours=1)

        _key_id, plaintext = await generate_api_key(
            user_id=user_id,
            name="expired key",
            role="admin",
            session=mock_session,
            expires_at=past,
        )

        stored_key = mock_session.add.call_args[0][0]
        stored_key.user_id = user_id
        stored_key.revoked_at = None
        stored_key.expires_at = past

        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = [stored_key]
        mock_session.execute.return_value = mock_result

        result = await validate_api_key(plaintext, mock_session)
        assert result is None

    @pytest.mark.asyncio
    async def test_revoked_key_returns_none(self):
        """A revoked key returns None."""
        from tsigma.auth.api_keys import generate_api_key, validate_api_key

        mock_session = AsyncMock()
        user_id = uuid4()

        _key_id, plaintext = await generate_api_key(
            user_id=user_id,
            name="revoked key",
            role="admin",
            session=mock_session,
        )

        stored_key = mock_session.add.call_args[0][0]
        stored_key.user_id = user_id
        stored_key.revoked_at = datetime.now(timezone.utc)
        stored_key.expires_at = None

        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = [stored_key]
        mock_session.execute.return_value = mock_result

        result = await validate_api_key(plaintext, mock_session)
        assert result is None

    @pytest.mark.asyncio
    async def test_wrong_key_returns_none(self):
        """A non-matching key returns None."""
        from tsigma.auth.api_keys import validate_api_key

        mock_session = AsyncMock()

        # No keys match the prefix
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = []
        mock_session.execute.return_value = mock_result

        result = await validate_api_key("tsgm_totallyinvalid", mock_session)
        assert result is None


# ---------------------------------------------------------------------------
# Dependency tests (header resolution)
# ---------------------------------------------------------------------------


class TestApiKeyHeaderResolution:
    """Tests for X-API-Key and Authorization: Bearer header resolution."""

    @pytest.mark.asyncio
    async def test_x_api_key_header(self):
        """X-API-Key header resolves to user session data."""
        from tsigma.auth.dependencies import get_current_user_optional

        user_id = uuid4()
        now = datetime.now(timezone.utc)
        expected = SessionData(
            user_id=user_id,
            username="apiuser",
            role="admin",
            created_at=now,
            expires_at=now + timedelta(hours=8),
        )

        mock_request = MagicMock()
        mock_request.headers = {"x-api-key": "tsgm_somevalidkey"}
        mock_request.cookies = {}
        mock_store = AsyncMock()

        with (
            patch("tsigma.auth.dependencies.settings") as mock_settings,
            patch("tsigma.auth.dependencies.validate_api_key", return_value=expected) as mock_validate,
            patch("tsigma.auth.dependencies._get_db_for_api_key") as mock_get_db,
        ):
            mock_settings.auth_cookie_name = "tsigma_session"
            mock_db = AsyncMock()
            mock_get_db.return_value = mock_db
            result = await get_current_user_optional(mock_request, mock_store)

        assert result is expected
        mock_validate.assert_called_once_with("tsgm_somevalidkey", mock_db)

    @pytest.mark.asyncio
    async def test_bearer_token(self):
        """Authorization: Bearer header resolves to user session data."""
        from tsigma.auth.dependencies import get_current_user_optional

        user_id = uuid4()
        now = datetime.now(timezone.utc)
        expected = SessionData(
            user_id=user_id,
            username="apiuser",
            role="viewer",
            created_at=now,
            expires_at=now + timedelta(hours=8),
        )

        mock_request = MagicMock()
        mock_request.headers = {"authorization": "Bearer tsgm_somevalidkey"}
        mock_request.cookies = {}
        mock_store = AsyncMock()

        with (
            patch("tsigma.auth.dependencies.settings") as mock_settings,
            patch("tsigma.auth.dependencies.validate_api_key", return_value=expected) as mock_validate,
            patch("tsigma.auth.dependencies._get_db_for_api_key") as mock_get_db,
        ):
            mock_settings.auth_cookie_name = "tsigma_session"
            mock_db = AsyncMock()
            mock_get_db.return_value = mock_db
            result = await get_current_user_optional(mock_request, mock_store)

        assert result is expected
        mock_validate.assert_called_once_with("tsgm_somevalidkey", mock_db)

    @pytest.mark.asyncio
    async def test_cookie_still_works(self):
        """Session cookie auth still works when no API key headers present."""
        from tsigma.auth.dependencies import get_current_user_optional

        user_id = uuid4()
        now = datetime.now(timezone.utc)
        expected = SessionData(
            user_id=user_id,
            username="webuser",
            role="admin",
            created_at=now,
            expires_at=now + timedelta(hours=8),
        )

        mock_request = MagicMock()
        mock_request.headers = {}
        mock_request.cookies = {"tsigma_session": "valid-session-id"}
        mock_store = AsyncMock()
        mock_store.get.return_value = expected

        with patch("tsigma.auth.dependencies.settings") as mock_settings:
            mock_settings.auth_cookie_name = "tsigma_session"
            result = await get_current_user_optional(mock_request, mock_store)

        assert result is expected


# ---------------------------------------------------------------------------
# Endpoint tests
# ---------------------------------------------------------------------------


def _make_session_data(**overrides):
    """Create a SessionData for test fixtures."""
    defaults = {
        "user_id": uuid4(),
        "username": "admin",
        "role": "admin",
        "created_at": datetime.now(timezone.utc),
        "expires_at": datetime.now(timezone.utc) + timedelta(hours=8),
    }
    defaults.update(overrides)
    return SessionData(**defaults)


def _create_api_key_app():
    """Create a minimal app with auth routes for testing."""
    app = FastAPI()
    app.include_router(auth_router, prefix="/api/v1/auth")
    return app


@pytest.fixture
def admin_session():
    """Admin session data fixture."""
    return _make_session_data(role="admin")


@pytest.fixture
def viewer_session():
    """Viewer session data fixture."""
    return _make_session_data(role="viewer", username="viewer")


@pytest.fixture
def api_key_client(admin_session):
    """TestClient with auth overrides for API key endpoints."""
    app = _create_api_key_app()
    mock_db = AsyncMock()
    mock_store = InMemorySessionStore()

    app.dependency_overrides[get_session] = lambda: mock_db
    app.dependency_overrides[get_session_store] = lambda: mock_store
    app.dependency_overrides[get_current_user] = lambda: admin_session

    return TestClient(app), mock_db


class TestCreateEndpoint:
    """Tests for POST /api/v1/auth/api-keys."""

    def test_create_api_key(self, api_key_client, admin_session):
        """POST /api/v1/auth/api-keys creates key and returns plaintext."""
        client, mock_db = api_key_client

        with patch("tsigma.auth.router.generate_api_key") as mock_gen:
            key_id = uuid4()
            mock_gen.return_value = (key_id, "tsgm_plaintext123456")
            response = client.post(
                "/api/v1/auth/api-keys",
                json={"name": "CI key"},
            )

        assert response.status_code == 201
        data = response.json()
        assert data["key_id"] == str(key_id)
        assert data["plaintext_key"] == "tsgm_plaintext123456"
        assert "key" in data["name"].lower() or data["name"] == "CI key"

    def test_create_with_expiration(self, api_key_client, admin_session):
        """POST /api/v1/auth/api-keys accepts optional expires_at."""
        client, mock_db = api_key_client

        with patch("tsigma.auth.router.generate_api_key") as mock_gen:
            key_id = uuid4()
            mock_gen.return_value = (key_id, "tsgm_plaintext123456")
            response = client.post(
                "/api/v1/auth/api-keys",
                json={
                    "name": "Temp key",
                    "expires_at": "2099-12-31T23:59:59Z",
                },
            )

        assert response.status_code == 201


class TestListEndpoint:
    """Tests for GET /api/v1/auth/api-keys."""

    def test_list_api_keys(self, api_key_client, admin_session):
        """GET /api/v1/auth/api-keys lists user's keys without plaintext."""
        client, mock_db = api_key_client

        with patch("tsigma.auth.router.list_user_keys") as mock_list:
            mock_list.return_value = [
                {
                    "id": str(uuid4()),
                    "name": "CI key",
                    "key_prefix": "tsgm_abc1...",
                    "role": "admin",
                    "created_at": datetime.now(timezone.utc).isoformat(),
                    "expires_at": None,
                    "revoked_at": None,
                    "last_used_at": None,
                },
            ]
            response = client.get("/api/v1/auth/api-keys")

        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        # Must not contain plaintext key or hash
        assert "key_hash" not in data[0]
        assert "plaintext_key" not in data[0]
        assert data[0]["key_prefix"] == "tsgm_abc1..."


class TestRevokeEndpoint:
    """Tests for DELETE /api/v1/auth/api-keys/{key_id}."""

    def test_revoke_api_key(self, api_key_client, admin_session):
        """DELETE /api/v1/auth/api-keys/{id} revokes the key."""
        client, mock_db = api_key_client
        key_id = uuid4()

        with patch("tsigma.auth.router.revoke_api_key") as mock_revoke:
            mock_revoke.return_value = True
            response = client.delete(f"/api/v1/auth/api-keys/{key_id}")

        assert response.status_code == 200
        assert response.json()["detail"] == "Key revoked"

    def test_revoke_nonexistent_returns_404(self, api_key_client, admin_session):
        """DELETE /api/v1/auth/api-keys/{id} returns 404 for unknown key."""
        client, mock_db = api_key_client
        key_id = uuid4()

        with patch("tsigma.auth.router.revoke_api_key") as mock_revoke:
            mock_revoke.return_value = False
            response = client.delete(f"/api/v1/auth/api-keys/{key_id}")

        assert response.status_code == 404
