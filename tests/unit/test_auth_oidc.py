"""
Unit tests for the OIDC (Azure AD / Entra ID) authentication provider.

Validates registration, router creation, state/nonce generation, state
validation, pending-state expiry, and callback token exchange without
making real HTTP requests.
"""

import time
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import httpx
import pytest
from fastapi import APIRouter, HTTPException

from tsigma.auth.providers.oidc import _STATE_TTL_SECONDS, OIDCAuthProvider
from tsigma.auth.registry import AuthProviderRegistry

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _find_route(router, path: str):
    """Return the route object for the given path."""
    for route in router.routes:
        if hasattr(route, "path") and route.path == path:
            return route
    raise AssertionError(f"Route {path} not found")


def _make_provider(**overrides):
    """Create an OIDCAuthProvider with endpoints pre-set."""
    p = OIDCAuthProvider()
    p._authorization_endpoint = overrides.get(
        "auth_ep",
        "https://login.microsoftonline.com/tenant/oauth2/v2.0/authorize",
    )
    p._token_endpoint = overrides.get(
        "token_ep",
        "https://login.microsoftonline.com/tenant/oauth2/v2.0/token",
    )
    p._userinfo_endpoint = overrides.get(
        "userinfo_ep", "https://graph.microsoft.com/oidc/userinfo"
    )
    return p


def _settings_defaults():
    """Return a MagicMock that behaves like the settings object."""
    s = MagicMock()
    s.oidc_tenant_id = "test-tenant"
    s.oidc_client_id = "test-client-id"
    s.oidc_client_secret = "test-client-secret"
    s.oidc_redirect_uri = "http://localhost/callback"
    s.oidc_scopes = "openid profile email"
    s.oidc_admin_groups = ""
    s.auth_cookie_name = "tsigma_session"
    s.auth_cookie_secure = False
    s.auth_session_ttl_minutes = 480
    return s


def _fake_httpx_response(json_data, status_code=200):
    """Build a minimal httpx.Response with .json() and .raise_for_status()."""
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code
    resp.json.return_value = json_data
    resp.raise_for_status.return_value = None
    if status_code >= 400:
        resp.raise_for_status.side_effect = httpx.HTTPStatusError(
            "error", request=MagicMock(), response=resp,
        )
    return resp


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------

class TestOIDCRegistration:
    """Tests for OIDC provider self-registration."""

    def test_registered(self):
        """AuthProviderRegistry.get('oidc') returns the OIDC provider class."""
        cls = AuthProviderRegistry.get("oidc")
        assert cls is OIDCAuthProvider


# ---------------------------------------------------------------------------
# Router basics
# ---------------------------------------------------------------------------

class TestOIDCRouter:
    """Tests for OIDC router and endpoint behavior."""

    def test_get_router_returns_router(self):
        """get_router() returns a FastAPI APIRouter instance."""
        provider = OIDCAuthProvider()
        router = provider.get_router()
        assert isinstance(router, APIRouter)

    @pytest.mark.asyncio
    async def test_authorize_generates_state_and_nonce(self):
        """The /oidc/login endpoint generates both state and nonce parameters."""
        provider = _make_provider()
        router = provider.get_router()
        login_route = _find_route(router, "/oidc/login")

        with patch("tsigma.auth.providers.oidc.settings", _settings_defaults()):
            result = await login_route.endpoint()

        assert "authorization_url" in result
        url = result["authorization_url"]
        assert "state=" in url
        assert "nonce=" in url

        # One pending state stored
        assert len(provider._pending_states) == 1

        # The stored value is a (nonce, timestamp) tuple
        state_key = next(iter(provider._pending_states))
        nonce, created_at = provider._pending_states[state_key]
        assert isinstance(nonce, str)
        assert len(nonce) > 0
        assert isinstance(created_at, float)

    @pytest.mark.asyncio
    async def test_callback_validates_state(self):
        """The /oidc/callback endpoint rejects invalid/missing state."""
        provider = _make_provider()
        router = provider.get_router()
        cb = _find_route(router, "/oidc/callback")

        with pytest.raises(HTTPException) as exc_info:
            await cb.endpoint(
                code="fake-code",
                state="bogus-state",
                response=MagicMock(),
                session=AsyncMock(),
                store=AsyncMock(),
            )

        assert exc_info.value.status_code == 401
        assert "state" in exc_info.value.detail.lower()

    def test_pending_states_expire(self):
        """Old pending states are cleaned up by _purge_expired_states."""
        provider = OIDCAuthProvider()

        # Insert a state that is already past the TTL
        old_time = time.monotonic() - _STATE_TTL_SECONDS - 1
        provider._pending_states["old-state"] = ("old-nonce", old_time)

        # Insert a fresh state
        provider._pending_states["fresh-state"] = ("fresh-nonce", time.monotonic())

        provider._purge_expired_states()

        assert "old-state" not in provider._pending_states
        assert "fresh-state" in provider._pending_states


# ---------------------------------------------------------------------------
# Callback handler
# ---------------------------------------------------------------------------

class TestOIDCCallback:
    """Tests for the /oidc/callback handler."""

    @pytest.mark.asyncio
    async def test_callback_rejects_invalid_state(self):
        """Callback with a state not in _pending_states returns 401."""
        provider = _make_provider()
        router = provider.get_router()
        cb = _find_route(router, "/oidc/callback")

        with pytest.raises(HTTPException) as exc_info:
            await cb.endpoint(
                code="auth-code",
                state="not-a-real-state",
                response=MagicMock(),
                session=AsyncMock(),
                store=AsyncMock(),
            )

        assert exc_info.value.status_code == 401
        assert "state" in exc_info.value.detail.lower()

    @pytest.mark.asyncio
    async def test_callback_exchanges_code(self):
        """Callback exchanges authorization code for tokens, creates session."""
        provider = _make_provider()
        valid_state = "valid-state-abc"
        provider._pending_states[valid_state] = ("nonce-123", time.monotonic())

        router = provider.get_router()
        cb = _find_route(router, "/oidc/callback")

        user_id = uuid4()
        mock_user = MagicMock()
        mock_user.id = user_id
        mock_user.username = "alice@example.com"
        mock_user.role.value = "viewer"

        token_resp = _fake_httpx_response({"access_token": "tok-123"})
        userinfo_resp = _fake_httpx_response({
            "sub": "ext-id-1",
            "email": "alice@example.com",
        })

        # OIDC callback opens two separate AsyncClient contexts (token + userinfo)
        mock_token_client = AsyncMock()
        mock_token_client.post.return_value = token_resp

        mock_userinfo_client = AsyncMock()
        mock_userinfo_client.get.return_value = userinfo_resp

        token_ctx = AsyncMock()
        token_ctx.__aenter__ = AsyncMock(return_value=mock_token_client)
        token_ctx.__aexit__ = AsyncMock(return_value=False)

        userinfo_ctx = AsyncMock()
        userinfo_ctx.__aenter__ = AsyncMock(return_value=mock_userinfo_client)
        userinfo_ctx.__aexit__ = AsyncMock(return_value=False)

        mock_store = AsyncMock()
        mock_store.create.return_value = "session-id-xyz"
        mock_response = MagicMock()
        mock_session = AsyncMock()

        with (
            patch("tsigma.auth.providers.oidc.settings", _settings_defaults()),
            patch(
                "tsigma.auth.providers.oidc.httpx.AsyncClient",
                side_effect=[token_ctx, userinfo_ctx],
            ),
            patch(
                "tsigma.auth.providers.oidc.provision_user",
                new_callable=AsyncMock,
                return_value=mock_user,
            ),
        ):
            result = await cb.endpoint(
                code="auth-code-123",
                state=valid_state,
                response=mock_response,
                session=mock_session,
                store=mock_store,
            )

        assert result["username"] == "alice@example.com"
        assert result["role"] == "viewer"
        # Token exchange was called
        mock_token_client.post.assert_called_once()
        # Userinfo was fetched
        mock_userinfo_client.get.assert_called_once()
        # Session was created
        mock_store.create.assert_awaited_once()
        # Cookie was set
        mock_response.set_cookie.assert_called_once()
        # State was consumed
        assert valid_state not in provider._pending_states

    def test_pending_states_cleanup(self):
        """Expired states are purged; fresh states are retained."""
        provider = OIDCAuthProvider()

        # Add three states: two expired, one fresh
        expired1 = time.monotonic() - _STATE_TTL_SECONDS - 100
        expired2 = time.monotonic() - _STATE_TTL_SECONDS - 1
        fresh = time.monotonic()

        provider._pending_states["s1"] = ("n1", expired1)
        provider._pending_states["s2"] = ("n2", expired2)
        provider._pending_states["s3"] = ("n3", fresh)

        provider._purge_expired_states()

        assert "s1" not in provider._pending_states
        assert "s2" not in provider._pending_states
        assert "s3" in provider._pending_states
        assert len(provider._pending_states) == 1


# ---------------------------------------------------------------------------
# Initialize / discovery
# ---------------------------------------------------------------------------

class TestOIDCInitialize:
    """Tests for OIDCAuthProvider.initialize() discovery."""

    @pytest.mark.asyncio
    async def test_initialize_discovers_endpoints(self):
        """initialize() fetches Azure AD discovery doc and sets endpoints."""
        provider = OIDCAuthProvider()
        mock_settings = _settings_defaults()

        discovery_doc = {
            "authorization_endpoint": "https://login.microsoftonline.com/tenant/oauth2/v2.0/authorize",
            "token_endpoint": "https://login.microsoftonline.com/tenant/oauth2/v2.0/token",
            "userinfo_endpoint": "https://graph.microsoft.com/oidc/userinfo",
            "jwks_uri": "https://login.microsoftonline.com/tenant/discovery/v2.0/keys",
        }
        discovery_resp = _fake_httpx_response(discovery_doc)

        mock_client = AsyncMock()
        mock_client.get.return_value = discovery_resp

        mock_client_ctx = AsyncMock()
        mock_client_ctx.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client_ctx.__aexit__ = AsyncMock(return_value=False)

        with (
            patch("tsigma.auth.providers.oidc.settings", mock_settings),
            patch("tsigma.auth.providers.oidc.httpx.AsyncClient", return_value=mock_client_ctx),
        ):
            await provider.initialize()

        assert provider._authorization_endpoint == discovery_doc["authorization_endpoint"]
        assert provider._token_endpoint == discovery_doc["token_endpoint"]
        assert provider._userinfo_endpoint == discovery_doc["userinfo_endpoint"]
        assert provider._jwks_uri == discovery_doc["jwks_uri"]

    @pytest.mark.asyncio
    async def test_initialize_raises_without_tenant_id(self):
        """initialize() raises ValueError when oidc_tenant_id is missing."""
        provider = OIDCAuthProvider()
        mock_settings = _settings_defaults()
        mock_settings.oidc_tenant_id = ""

        with (
            patch("tsigma.auth.providers.oidc.settings", mock_settings),
            pytest.raises(ValueError, match="oidc_tenant_id"),
        ):
            await provider.initialize()

    @pytest.mark.asyncio
    async def test_initialize_raises_without_client_id(self):
        """initialize() raises ValueError when oidc_client_id is missing."""
        provider = OIDCAuthProvider()
        mock_settings = _settings_defaults()
        mock_settings.oidc_client_id = ""

        with (
            patch("tsigma.auth.providers.oidc.settings", mock_settings),
            pytest.raises(ValueError, match="oidc_client_id"),
        ):
            await provider.initialize()

    @pytest.mark.asyncio
    async def test_initialize_raises_without_client_secret(self):
        """initialize() raises ValueError when oidc_client_secret is missing."""
        provider = OIDCAuthProvider()
        mock_settings = _settings_defaults()
        mock_settings.oidc_client_secret = ""

        with (
            patch("tsigma.auth.providers.oidc.settings", mock_settings),
            pytest.raises(ValueError, match="oidc_client_secret"),
        ):
            await provider.initialize()

    @pytest.mark.asyncio
    async def test_initialize_discovery_http_error(self):
        """initialize() raises when discovery HTTP request fails."""
        provider = OIDCAuthProvider()
        mock_settings = _settings_defaults()

        discovery_resp = _fake_httpx_response({}, status_code=500)

        mock_client = AsyncMock()
        mock_client.get.return_value = discovery_resp

        mock_client_ctx = AsyncMock()
        mock_client_ctx.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client_ctx.__aexit__ = AsyncMock(return_value=False)

        with (
            patch("tsigma.auth.providers.oidc.settings", mock_settings),
            patch("tsigma.auth.providers.oidc.httpx.AsyncClient", return_value=mock_client_ctx),
            pytest.raises(httpx.HTTPStatusError),
        ):
            await provider.initialize()


class TestOIDCCallbackErrors:
    """Tests for OIDC callback error paths."""

    @pytest.mark.asyncio
    async def test_callback_token_exchange_failure(self):
        """Callback returns 401 when token exchange fails."""
        provider = _make_provider()
        valid_state = "state-for-token-fail"
        provider._pending_states[valid_state] = ("nonce-1", time.monotonic())

        router = provider.get_router()
        cb = _find_route(router, "/oidc/callback")

        token_resp = _fake_httpx_response({}, status_code=400)

        mock_client = AsyncMock()
        mock_client.post.return_value = token_resp

        mock_client_ctx = AsyncMock()
        mock_client_ctx.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client_ctx.__aexit__ = AsyncMock(return_value=False)

        with (
            patch("tsigma.auth.providers.oidc.settings", _settings_defaults()),
            patch("tsigma.auth.providers.oidc.httpx.AsyncClient", return_value=mock_client_ctx),
            pytest.raises(HTTPException) as exc_info,
        ):
            await cb.endpoint(
                code="bad-code",
                state=valid_state,
                response=MagicMock(),
                session=AsyncMock(),
                store=AsyncMock(),
            )

        assert exc_info.value.status_code == 401
        assert "token" in exc_info.value.detail.lower()

    @pytest.mark.asyncio
    async def test_callback_missing_access_token(self):
        """Callback returns 401 when token response lacks access_token."""
        provider = _make_provider()
        valid_state = "state-no-token"
        provider._pending_states[valid_state] = ("nonce-2", time.monotonic())

        router = provider.get_router()
        cb = _find_route(router, "/oidc/callback")

        token_resp = _fake_httpx_response({"id_token": "id-tok-only"})

        mock_client = AsyncMock()
        mock_client.post.return_value = token_resp

        mock_client_ctx = AsyncMock()
        mock_client_ctx.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client_ctx.__aexit__ = AsyncMock(return_value=False)

        with (
            patch("tsigma.auth.providers.oidc.settings", _settings_defaults()),
            patch("tsigma.auth.providers.oidc.httpx.AsyncClient", return_value=mock_client_ctx),
            pytest.raises(HTTPException) as exc_info,
        ):
            await cb.endpoint(
                code="auth-code",
                state=valid_state,
                response=MagicMock(),
                session=AsyncMock(),
                store=AsyncMock(),
            )

        assert exc_info.value.status_code == 401
        assert "access token" in exc_info.value.detail.lower()

    @pytest.mark.asyncio
    async def test_callback_missing_claims(self):
        """Callback returns 401 when userinfo lacks required claims."""
        provider = _make_provider()
        valid_state = "state-missing-claims"
        provider._pending_states[valid_state] = ("nonce-3", time.monotonic())

        router = provider.get_router()
        cb = _find_route(router, "/oidc/callback")

        token_resp = _fake_httpx_response({"access_token": "tok-123"})
        # Missing both sub and email
        userinfo_resp = _fake_httpx_response({"name": "Alice"})

        mock_token_client = AsyncMock()
        mock_token_client.post.return_value = token_resp

        mock_userinfo_client = AsyncMock()
        mock_userinfo_client.get.return_value = userinfo_resp

        token_ctx = AsyncMock()
        token_ctx.__aenter__ = AsyncMock(return_value=mock_token_client)
        token_ctx.__aexit__ = AsyncMock(return_value=False)

        userinfo_ctx = AsyncMock()
        userinfo_ctx.__aenter__ = AsyncMock(return_value=mock_userinfo_client)
        userinfo_ctx.__aexit__ = AsyncMock(return_value=False)

        with (
            patch("tsigma.auth.providers.oidc.settings", _settings_defaults()),
            patch(
                "tsigma.auth.providers.oidc.httpx.AsyncClient",
                side_effect=[token_ctx, userinfo_ctx],
            ),
            pytest.raises(HTTPException) as exc_info,
        ):
            await cb.endpoint(
                code="auth-code",
                state=valid_state,
                response=MagicMock(),
                session=AsyncMock(),
                store=AsyncMock(),
            )

        assert exc_info.value.status_code == 401
        assert "claims" in exc_info.value.detail.lower()
