"""
Unit tests for the OAuth2 authentication provider.

Validates registration, router creation, state generation, state
validation, authorize redirect, and callback token exchange without
making real HTTP requests to identity providers.
"""

import time
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import httpx
import pytest
from fastapi import APIRouter, HTTPException

from tsigma.auth.providers.oauth2 import _STATE_TTL_SECONDS, OAuth2AuthProvider
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
    """Create an OAuth2AuthProvider with endpoints pre-set."""
    p = OAuth2AuthProvider()
    p._authorization_endpoint = overrides.get(
        "auth_ep", "https://idp.example.com/authorize"
    )
    p._token_endpoint = overrides.get(
        "token_ep", "https://idp.example.com/token"
    )
    p._userinfo_endpoint = overrides.get(
        "userinfo_ep", "https://idp.example.com/userinfo"
    )
    return p


def _settings_defaults():
    """Return a MagicMock that behaves like the settings object."""
    s = MagicMock()
    s.oauth2_client_id = "test-client-id"
    s.oauth2_client_secret = "test-client-secret"
    s.oauth2_redirect_uri = "http://localhost/callback"
    s.oauth2_scopes = "openid profile email"
    s.oauth2_admin_groups = ""
    s.oauth2_username_claim = "email"
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

class TestOAuth2Registration:
    """Tests for OAuth2 provider self-registration."""

    def test_registered(self):
        """AuthProviderRegistry.get('oauth2') returns the OAuth2 provider class."""
        cls = AuthProviderRegistry.get("oauth2")
        assert cls is OAuth2AuthProvider


# ---------------------------------------------------------------------------
# Router basics
# ---------------------------------------------------------------------------

class TestOAuth2Router:
    """Tests for OAuth2 router and endpoint behavior."""

    def test_get_router_returns_router(self):
        """get_router() returns a FastAPI APIRouter instance."""
        provider = OAuth2AuthProvider()
        router = provider.get_router()
        assert isinstance(router, APIRouter)

    @pytest.mark.asyncio
    async def test_authorize_generates_state(self):
        """The /oauth2/login endpoint generates a random state parameter."""
        provider = _make_provider()
        router = provider.get_router()
        login_route = _find_route(router, "/oauth2/login")

        with patch("tsigma.auth.providers.oauth2.settings", _settings_defaults()):
            result = await login_route.endpoint()

        assert "authorization_url" in result
        assert "state=" in result["authorization_url"]
        assert len(provider._pending_states) == 1

    @pytest.mark.asyncio
    async def test_callback_validates_state(self):
        """The /oauth2/callback endpoint rejects invalid/missing state."""
        provider = _make_provider()
        router = provider.get_router()
        callback_route = _find_route(router, "/oauth2/callback")

        with pytest.raises(HTTPException) as exc_info:
            await callback_route.endpoint(
                code="fake-code",
                state="invalid-state-value",
                response=MagicMock(),
                session=AsyncMock(),
                store=AsyncMock(),
            )

        assert exc_info.value.status_code == 401
        assert "state" in exc_info.value.detail.lower()


# ---------------------------------------------------------------------------
# Authorize redirect
# ---------------------------------------------------------------------------

class TestOAuth2Authorize:
    """Tests for the /oauth2/login authorize-redirect endpoint."""

    @pytest.mark.asyncio
    async def test_authorize_stores_state(self):
        """Calling /oauth2/login stores a state entry for later validation."""
        provider = _make_provider()
        router = provider.get_router()
        login_route = _find_route(router, "/oauth2/login")

        with patch("tsigma.auth.providers.oauth2.settings", _settings_defaults()):
            await login_route.endpoint()

        assert len(provider._pending_states) == 1
        state_key = next(iter(provider._pending_states))
        # Value is a monotonic timestamp
        assert isinstance(provider._pending_states[state_key], float)

    @pytest.mark.asyncio
    async def test_authorize_url_contains_required_params(self):
        """Authorization URL includes client_id, redirect_uri, scope, state."""
        provider = _make_provider()
        router = provider.get_router()
        login_route = _find_route(router, "/oauth2/login")

        with patch("tsigma.auth.providers.oauth2.settings", _settings_defaults()):
            result = await login_route.endpoint()

        url = result["authorization_url"]
        assert "client_id=test-client-id" in url
        assert "redirect_uri=" in url
        assert "scope=" in url
        assert "response_type=code" in url


# ---------------------------------------------------------------------------
# Callback handler
# ---------------------------------------------------------------------------

class TestOAuth2Callback:
    """Tests for the /oauth2/callback handler."""

    @pytest.mark.asyncio
    async def test_callback_rejects_invalid_state(self):
        """Callback with a state not in _pending_states returns 401."""
        provider = _make_provider()
        router = provider.get_router()
        cb = _find_route(router, "/oauth2/callback")

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
    async def test_callback_rejects_expired_state(self):
        """Callback with a state older than _STATE_TTL_SECONDS returns 401."""
        provider = _make_provider()
        # Insert an expired state
        expired_time = time.monotonic() - _STATE_TTL_SECONDS - 1
        provider._pending_states["expired-state"] = expired_time

        router = provider.get_router()
        cb = _find_route(router, "/oauth2/callback")

        with pytest.raises(HTTPException) as exc_info:
            await cb.endpoint(
                code="auth-code",
                state="expired-state",
                response=MagicMock(),
                session=AsyncMock(),
                store=AsyncMock(),
            )

        assert exc_info.value.status_code == 401
        assert "expired" in exc_info.value.detail.lower()

    @pytest.mark.asyncio
    async def test_callback_exchanges_code(self):
        """Callback exchanges authorization code for tokens, creates session."""
        provider = _make_provider()
        valid_state = "valid-state-abc"
        provider._pending_states[valid_state] = time.monotonic()

        router = provider.get_router()
        cb = _find_route(router, "/oauth2/callback")

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

        mock_client = AsyncMock()
        mock_client.post.return_value = token_resp
        mock_client.get.return_value = userinfo_resp

        mock_client_ctx = AsyncMock()
        mock_client_ctx.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client_ctx.__aexit__ = AsyncMock(return_value=False)

        mock_store = AsyncMock()
        mock_store.create.return_value = "session-id-xyz"
        mock_response = MagicMock()
        mock_session = AsyncMock()

        with (
            patch("tsigma.auth.providers.oauth2.settings", _settings_defaults()),
            patch("tsigma.auth.providers.oauth2.httpx.AsyncClient", return_value=mock_client_ctx),
            patch("tsigma.auth.providers.oauth2.provision_user", new_callable=AsyncMock, return_value=mock_user),
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
        mock_client.post.assert_called_once()
        # Userinfo was fetched
        mock_client.get.assert_called_once()
        # Session was created
        mock_store.create.assert_awaited_once()
        # Cookie was set
        mock_response.set_cookie.assert_called_once()
        # State was consumed
        assert valid_state not in provider._pending_states


# ---------------------------------------------------------------------------
# Initialize / discovery
# ---------------------------------------------------------------------------

class TestOAuth2Initialize:
    """Tests for OAuth2AuthProvider.initialize() endpoint discovery."""

    @pytest.mark.asyncio
    async def test_initialize_discovers_endpoints(self):
        """initialize() fetches OIDC discovery doc and sets endpoints."""
        provider = OAuth2AuthProvider()
        mock_settings = _settings_defaults()
        mock_settings.oauth2_issuer_url = "https://idp.example.com"
        mock_settings.oauth2_authorization_endpoint = ""
        mock_settings.oauth2_token_endpoint = ""
        mock_settings.oauth2_userinfo_endpoint = ""

        discovery_doc = {
            "authorization_endpoint": "https://idp.example.com/authorize",
            "token_endpoint": "https://idp.example.com/token",
            "userinfo_endpoint": "https://idp.example.com/userinfo",
        }
        discovery_resp = _fake_httpx_response(discovery_doc)

        mock_client = AsyncMock()
        mock_client.get.return_value = discovery_resp

        mock_client_ctx = AsyncMock()
        mock_client_ctx.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client_ctx.__aexit__ = AsyncMock(return_value=False)

        with (
            patch("tsigma.auth.providers.oauth2.settings", mock_settings),
            patch("tsigma.auth.providers.oauth2.httpx.AsyncClient", return_value=mock_client_ctx),
        ):
            await provider.initialize()

        assert provider._authorization_endpoint == "https://idp.example.com/authorize"
        assert provider._token_endpoint == "https://idp.example.com/token"
        assert provider._userinfo_endpoint == "https://idp.example.com/userinfo"

    @pytest.mark.asyncio
    async def test_initialize_uses_explicit_endpoints(self):
        """initialize() uses explicit endpoints when all three are set."""
        provider = OAuth2AuthProvider()
        mock_settings = _settings_defaults()
        mock_settings.oauth2_issuer_url = ""
        mock_settings.oauth2_authorization_endpoint = "https://custom/authorize"
        mock_settings.oauth2_token_endpoint = "https://custom/token"
        mock_settings.oauth2_userinfo_endpoint = "https://custom/userinfo"

        with patch("tsigma.auth.providers.oauth2.settings", mock_settings):
            await provider.initialize()

        assert provider._authorization_endpoint == "https://custom/authorize"
        assert provider._token_endpoint == "https://custom/token"
        assert provider._userinfo_endpoint == "https://custom/userinfo"

    @pytest.mark.asyncio
    async def test_initialize_raises_without_client_credentials(self):
        """initialize() raises ValueError when client_id/secret missing."""
        provider = OAuth2AuthProvider()
        mock_settings = _settings_defaults()
        mock_settings.oauth2_client_id = ""
        mock_settings.oauth2_client_secret = ""

        with (
            patch("tsigma.auth.providers.oauth2.settings", mock_settings),
            pytest.raises(ValueError, match="oauth2_client_id"),
        ):
            await provider.initialize()

    @pytest.mark.asyncio
    async def test_initialize_raises_without_endpoints_or_issuer(self):
        """initialize() raises ValueError when no issuer or explicit endpoints."""
        provider = OAuth2AuthProvider()
        mock_settings = _settings_defaults()
        mock_settings.oauth2_issuer_url = ""
        mock_settings.oauth2_authorization_endpoint = ""
        mock_settings.oauth2_token_endpoint = ""
        mock_settings.oauth2_userinfo_endpoint = ""

        with (
            patch("tsigma.auth.providers.oauth2.settings", mock_settings),
            pytest.raises(ValueError, match="oauth2_issuer_url"),
        ):
            await provider.initialize()

    @pytest.mark.asyncio
    async def test_initialize_discovery_http_error(self):
        """initialize() raises ValueError when discovery HTTP request fails."""
        provider = OAuth2AuthProvider()
        mock_settings = _settings_defaults()
        mock_settings.oauth2_issuer_url = "https://bad.example.com"
        mock_settings.oauth2_authorization_endpoint = ""
        mock_settings.oauth2_token_endpoint = ""
        mock_settings.oauth2_userinfo_endpoint = ""

        discovery_resp = _fake_httpx_response({}, status_code=404)

        mock_client = AsyncMock()
        mock_client.get.return_value = discovery_resp

        mock_client_ctx = AsyncMock()
        mock_client_ctx.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client_ctx.__aexit__ = AsyncMock(return_value=False)

        with (
            patch("tsigma.auth.providers.oauth2.settings", mock_settings),
            patch("tsigma.auth.providers.oauth2.httpx.AsyncClient", return_value=mock_client_ctx),
            pytest.raises(ValueError, match="discovery failed"),
        ):
            await provider.initialize()

    @pytest.mark.asyncio
    async def test_initialize_discovery_missing_userinfo(self):
        """initialize() raises ValueError when discovery doc lacks userinfo_endpoint."""
        provider = OAuth2AuthProvider()
        mock_settings = _settings_defaults()
        mock_settings.oauth2_issuer_url = "https://idp.example.com"
        mock_settings.oauth2_authorization_endpoint = ""
        mock_settings.oauth2_token_endpoint = ""
        mock_settings.oauth2_userinfo_endpoint = ""

        discovery_doc = {
            "authorization_endpoint": "https://idp.example.com/authorize",
            "token_endpoint": "https://idp.example.com/token",
            # userinfo_endpoint intentionally missing
        }
        discovery_resp = _fake_httpx_response(discovery_doc)

        mock_client = AsyncMock()
        mock_client.get.return_value = discovery_resp

        mock_client_ctx = AsyncMock()
        mock_client_ctx.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client_ctx.__aexit__ = AsyncMock(return_value=False)

        with (
            patch("tsigma.auth.providers.oauth2.settings", mock_settings),
            patch("tsigma.auth.providers.oauth2.httpx.AsyncClient", return_value=mock_client_ctx),
            pytest.raises(ValueError, match="userinfo_endpoint"),
        ):
            await provider.initialize()


class TestOAuth2CallbackErrors:
    """Tests for callback error paths (token exchange failure, missing claims)."""

    @pytest.mark.asyncio
    async def test_callback_token_exchange_failure(self):
        """Callback returns 401 when token exchange HTTP request fails."""
        provider = _make_provider()
        valid_state = "state-for-token-fail"
        provider._pending_states[valid_state] = time.monotonic()

        router = provider.get_router()
        cb = _find_route(router, "/oauth2/callback")

        token_resp = _fake_httpx_response({}, status_code=400)

        mock_client = AsyncMock()
        mock_client.post.return_value = token_resp

        mock_client_ctx = AsyncMock()
        mock_client_ctx.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client_ctx.__aexit__ = AsyncMock(return_value=False)

        with (
            patch("tsigma.auth.providers.oauth2.settings", _settings_defaults()),
            patch("tsigma.auth.providers.oauth2.httpx.AsyncClient", return_value=mock_client_ctx),
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
    async def test_callback_missing_sub_claim(self):
        """Callback returns 401 when userinfo lacks 'sub' claim."""
        provider = _make_provider()
        valid_state = "state-for-missing-sub"
        provider._pending_states[valid_state] = time.monotonic()

        router = provider.get_router()
        cb = _find_route(router, "/oauth2/callback")

        token_resp = _fake_httpx_response({"access_token": "tok-123"})
        userinfo_resp = _fake_httpx_response({
            "email": "alice@example.com",
            # "sub" intentionally missing
        })

        mock_client = AsyncMock()
        mock_client.post.return_value = token_resp
        mock_client.get.return_value = userinfo_resp

        mock_client_ctx = AsyncMock()
        mock_client_ctx.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client_ctx.__aexit__ = AsyncMock(return_value=False)

        with (
            patch("tsigma.auth.providers.oauth2.settings", _settings_defaults()),
            patch("tsigma.auth.providers.oauth2.httpx.AsyncClient", return_value=mock_client_ctx),
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
        assert "sub" in exc_info.value.detail.lower()
