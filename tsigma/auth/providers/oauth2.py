"""
Generic OAuth2 authentication provider.

Supports any OAuth2-compliant identity provider including Google, Okta,
Auth0, Keycloak, AWS Cognito, and others. Implements the Authorization
Code Flow with configurable endpoints and optional OpenID Connect
discovery.
"""

import logging
import secrets
import time
from urllib.parse import urlencode

import httpx
from fastapi import APIRouter, Depends, HTTPException, Response, status
from sqlalchemy.ext.asyncio import AsyncSession

from tsigma.auth.dependencies import get_session_store
from tsigma.auth.models import UserRole
from tsigma.auth.provisioning import provision_user
from tsigma.auth.registry import AuthProviderRegistry, BaseAuthProvider
from tsigma.auth.sessions import BaseSessionStore
from tsigma.auth.utils import set_auth_cookie
from tsigma.config import settings
from tsigma.dependencies import get_session

logger = logging.getLogger(__name__)

_STATE_TTL_SECONDS = 600  # 10 minutes


@AuthProviderRegistry.register("oauth2")
class OAuth2AuthProvider(BaseAuthProvider):
    """Generic OAuth2 authorization-code-flow authentication provider."""

    name = "oauth2"
    description = "Generic OAuth2 authentication (Google, Okta, Auth0, Keycloak, Cognito, etc.)"

    def __init__(self) -> None:
        self._authorization_endpoint: str = ""
        self._token_endpoint: str = ""
        self._userinfo_endpoint: str = ""
        self._pending_states: dict[str, float] = {}

    async def initialize(self) -> None:
        """
        Validate configuration and resolve OAuth2 endpoints.

        If ``oauth2_issuer_url`` is set and explicit endpoints are not,
        attempts OpenID Connect discovery to populate them automatically.

        Raises:
            ValueError: If required configuration is missing or discovery fails.
        """
        if not settings.oauth2_client_id or not settings.oauth2_client_secret:
            raise ValueError(
                "oauth2_client_id and oauth2_client_secret must be set"
            )

        has_explicit = (
            settings.oauth2_authorization_endpoint
            and settings.oauth2_token_endpoint
            and settings.oauth2_userinfo_endpoint
        )

        if settings.oauth2_issuer_url and not has_explicit:
            await self._discover_endpoints(settings.oauth2_issuer_url)
        elif has_explicit:
            self._authorization_endpoint = settings.oauth2_authorization_endpoint
            self._token_endpoint = settings.oauth2_token_endpoint
            self._userinfo_endpoint = settings.oauth2_userinfo_endpoint
        else:
            raise ValueError(
                "Either oauth2_issuer_url or all three explicit endpoints "
                "(oauth2_authorization_endpoint, oauth2_token_endpoint, "
                "oauth2_userinfo_endpoint) must be configured"
            )

        logger.info(
            "OAuth2 provider initialized (authorization=%s, token=%s, userinfo=%s)",
            self._authorization_endpoint,
            self._token_endpoint,
            self._userinfo_endpoint,
        )

    async def _discover_endpoints(self, issuer_url: str) -> None:
        """
        Fetch OpenID Connect discovery document and extract endpoints.

        Args:
            issuer_url: Base issuer URL (e.g. ``https://accounts.google.com``).

        Raises:
            ValueError: If discovery request fails or required fields are missing.
        """
        discovery_url = f"{issuer_url.rstrip('/')}/.well-known/openid-configuration"

        async with httpx.AsyncClient() as client:
            try:
                resp = await client.get(discovery_url, timeout=10.0)
                resp.raise_for_status()
            except httpx.HTTPError as exc:
                raise ValueError(
                    f"OpenID Connect discovery failed for {discovery_url}: {exc}"
                ) from exc

        doc = resp.json()

        self._authorization_endpoint = doc.get("authorization_endpoint", "")
        self._token_endpoint = doc.get("token_endpoint", "")
        self._userinfo_endpoint = doc.get("userinfo_endpoint", "")

        if not self._authorization_endpoint or not self._token_endpoint:
            raise ValueError(
                "Discovery document missing authorization_endpoint or "
                f"token_endpoint (source: {discovery_url})"
            )

        if not self._userinfo_endpoint:
            raise ValueError(
                f"Discovery document missing userinfo_endpoint (source: {discovery_url})"
            )

    def _cleanup_expired_states(self) -> None:
        """Remove pending OAuth2 states older than the TTL."""
        now = time.monotonic()
        expired = [
            state
            for state, created_at in self._pending_states.items()
            if now - created_at > _STATE_TTL_SECONDS
        ]
        for state in expired:
            del self._pending_states[state]

    def _validate_state(self, state: str) -> None:
        """Validate and consume the OAuth2 state parameter."""
        created_at = self._pending_states.pop(state, None)
        if created_at is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid or missing state parameter",
            )
        if time.monotonic() - created_at > _STATE_TTL_SECONDS:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="State parameter expired",
            )

    async def _exchange_code(self, code: str) -> str:
        """Exchange authorization code for access token."""
        async with httpx.AsyncClient() as client:
            try:
                token_resp = await client.post(
                    self._token_endpoint,
                    data={
                        "grant_type": "authorization_code",
                        "client_id": settings.oauth2_client_id,
                        "client_secret": settings.oauth2_client_secret,
                        "code": code,
                        "redirect_uri": settings.oauth2_redirect_uri,
                    },
                    timeout=10.0,
                )
                token_resp.raise_for_status()
            except httpx.HTTPError as exc:
                logger.error("OAuth2 token exchange failed: %s", exc)
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Token exchange failed",
                ) from exc

        token_data = token_resp.json()
        access_token = token_data.get("access_token")
        if not access_token:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="No access_token in token response",
            )
        return access_token

    async def _fetch_userinfo(self, access_token: str) -> dict:
        """Fetch user info from the OAuth2 provider."""
        async with httpx.AsyncClient() as client:
            try:
                resp = await client.get(
                    self._userinfo_endpoint,
                    headers={"Authorization": f"Bearer {access_token}"},
                    timeout=10.0,
                )
                resp.raise_for_status()
            except httpx.HTTPError as exc:
                logger.error("OAuth2 userinfo request failed: %s", exc)
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Failed to fetch user info",
                ) from exc
        return resp.json()

    def _extract_claims(self, userinfo: dict) -> tuple[str, str, "UserRole"]:
        """Extract external_id, username, and role from userinfo claims."""
        external_id = userinfo.get("sub")
        if not external_id:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Missing 'sub' claim in user info",
            )

        username = userinfo.get(settings.oauth2_username_claim)
        if not username:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail=f"Missing '{settings.oauth2_username_claim}' claim",
            )

        role = UserRole.VIEWER
        if settings.oauth2_admin_groups:
            admin_groups = {
                g.strip()
                for g in settings.oauth2_admin_groups.split(",")
                if g.strip()
            }
            user_groups = set(userinfo.get("groups", []))
            if admin_groups & user_groups:
                role = UserRole.ADMIN

        return external_id, username, role

    def get_router(self) -> APIRouter:
        """
        Return router with OAuth2 login and callback endpoints.

        Returns:
            APIRouter with GET /oauth2/login and GET /oauth2/callback routes.
        """
        router = APIRouter()
        provider = self

        @router.get("/oauth2/login")
        async def oauth2_login() -> dict:
            """
            Initiate OAuth2 authorization-code flow.

            Generates a random state parameter, stores it for CSRF
            validation, and returns the authorization URL to redirect
            the user to.

            Returns:
                Dict with ``authorization_url`` string.
            """
            provider._cleanup_expired_states()

            state = secrets.token_hex(32)
            provider._pending_states[state] = time.monotonic()

            params = {
                "response_type": "code",
                "client_id": settings.oauth2_client_id,
                "redirect_uri": settings.oauth2_redirect_uri,
                "scope": settings.oauth2_scopes,
                "state": state,
            }
            authorization_url = (
                f"{provider._authorization_endpoint}?{urlencode(params)}"
            )

            return {"authorization_url": authorization_url}

        @router.get("/oauth2/callback")
        async def oauth2_callback(
            code: str,
            state: str,
            response: Response,
            session: AsyncSession = Depends(get_session),
            store: BaseSessionStore = Depends(get_session_store),
        ) -> dict:
            """
            Handle the OAuth2 authorization callback.

            Validates the state parameter, exchanges the authorization
            code for tokens, fetches user info, provisions the user
            via JIT provisioning, and creates a session.

            Args:
                code: Authorization code from the IdP.
                state: State parameter for CSRF validation.
                response: FastAPI response (for setting cookies).
                session: Database session (injected).
                store: Session store (injected).

            Returns:
                Dict with username and role.

            Raises:
                HTTPException: 401 on invalid state, token exchange
                    failure, or missing user claims.
            """
            provider._validate_state(state)
            access_token = await provider._exchange_code(code)
            userinfo = await provider._fetch_userinfo(access_token)
            external_id, username, role = provider._extract_claims(userinfo)

            user = await provision_user(
                session,
                external_id=external_id,
                external_provider="oauth2",
                username=username,
                role=role,
            )

            session_id = await store.create(
                user_id=user.id,
                username=user.username,
                role=user.role.value,
            )

            set_auth_cookie(response, session_id)

            return {"username": user.username, "role": user.role.value}

        return router
