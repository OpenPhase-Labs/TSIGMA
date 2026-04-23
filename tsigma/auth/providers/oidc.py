"""
Azure AD (Entra ID) OIDC authentication provider.

Implements the Authorization Code Flow against Azure AD's v2.0 endpoints.
Uses JIT provisioning to create or update local AuthUser records on first
login via the external identity provider.
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


@AuthProviderRegistry.register("oidc")
class OIDCAuthProvider(BaseAuthProvider):
    """Azure AD OIDC authentication provider (Authorization Code Flow)."""

    name = "oidc"
    description = "Azure AD (Entra ID) OpenID Connect authentication"

    def __init__(self) -> None:
        self._authorization_endpoint: str = ""
        self._token_endpoint: str = ""
        self._userinfo_endpoint: str = ""
        self._jwks_uri: str = ""
        self._pending_states: dict[str, tuple[str, float]] = {}

    async def initialize(self) -> None:
        """
        Validate OIDC settings and fetch the OpenID Connect discovery document.

        Raises:
            ValueError: If required OIDC settings are missing.
            httpx.HTTPStatusError: If discovery document fetch fails.
        """
        if not settings.oidc_tenant_id:
            raise ValueError("oidc_tenant_id is required for OIDC provider")
        if not settings.oidc_client_id:
            raise ValueError("oidc_client_id is required for OIDC provider")
        if not settings.oidc_client_secret:
            raise ValueError("oidc_client_secret is required for OIDC provider")

        discovery_url = (
            f"https://login.microsoftonline.com/"
            f"{settings.oidc_tenant_id}/v2.0/.well-known/openid-configuration"
        )

        async with httpx.AsyncClient() as client:
            resp = await client.get(discovery_url)
            resp.raise_for_status()
            doc = resp.json()

        self._authorization_endpoint = doc["authorization_endpoint"]
        self._token_endpoint = doc["token_endpoint"]
        self._userinfo_endpoint = doc["userinfo_endpoint"]
        self._jwks_uri = doc["jwks_uri"]

        logger.info("OIDC discovery complete for tenant %s", settings.oidc_tenant_id)

    def _purge_expired_states(self) -> None:
        """Remove pending states older than the TTL."""
        now = time.monotonic()
        expired = [
            key
            for key, (_, created_at) in self._pending_states.items()
            if now - created_at > _STATE_TTL_SECONDS
        ]
        for key in expired:
            del self._pending_states[key]

    def _validate_state(self, state: str) -> None:
        """Validate and consume the OIDC state parameter."""
        self._purge_expired_states()
        if state not in self._pending_states:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid or expired state parameter",
            )
        self._pending_states.pop(state)

    async def _exchange_code(self, code: str) -> str:
        """Exchange authorization code for access token."""
        try:
            async with httpx.AsyncClient() as client:
                token_resp = await client.post(
                    self._token_endpoint,
                    data={
                        "grant_type": "authorization_code",
                        "client_id": settings.oidc_client_id,
                        "client_secret": settings.oidc_client_secret,
                        "code": code,
                        "redirect_uri": settings.oidc_redirect_uri,
                    },
                )
                token_resp.raise_for_status()
                token_data = token_resp.json()
        except httpx.HTTPStatusError:
            logger.exception("OIDC token exchange failed")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Token exchange failed",
            )

        access_token = token_data.get("access_token")
        if not access_token:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="No access token in token response",
            )
        return access_token

    async def _fetch_userinfo(self, access_token: str) -> dict:
        """Fetch user info from the OIDC provider."""
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.get(
                    self._userinfo_endpoint,
                    headers={"Authorization": f"Bearer {access_token}"},
                )
                resp.raise_for_status()
                return resp.json()
        except httpx.HTTPStatusError:
            logger.exception("OIDC userinfo request failed")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Failed to retrieve user info",
            )

    def _extract_claims(self, userinfo: dict) -> tuple[str, str, "UserRole"]:
        """Extract sub, email, and role from OIDC userinfo."""
        sub = userinfo.get("sub")
        email = userinfo.get("email") or userinfo.get("preferred_username")

        if not sub or not email:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Missing required claims (sub, email)",
            )

        role = UserRole.VIEWER
        if settings.oidc_admin_groups:
            admin_group_ids = {
                g.strip()
                for g in settings.oidc_admin_groups.split(",")
                if g.strip()
            }
            if admin_group_ids.intersection(userinfo.get("groups", [])):
                role = UserRole.ADMIN

        return sub, email, role

    def get_router(self) -> APIRouter:
        """
        Return router with OIDC login and callback endpoints.

        Returns:
            APIRouter with GET /oidc/login and GET /oidc/callback routes.
        """
        router = APIRouter()
        provider = self

        @router.get("/oidc/login")
        async def oidc_login() -> dict:
            """
            Start the OIDC Authorization Code Flow.

            Generates a random state and nonce, stores them for validation,
            and returns the Azure AD authorization URL.

            Returns:
                Dict with authorization_url.
            """
            provider._purge_expired_states()

            state = secrets.token_hex(32)
            nonce = secrets.token_hex(32)
            provider._pending_states[state] = (nonce, time.monotonic())

            params = {
                "response_type": "code",
                "client_id": settings.oidc_client_id,
                "redirect_uri": settings.oidc_redirect_uri,
                "scope": settings.oidc_scopes,
                "state": state,
                "nonce": nonce,
            }
            authorization_url = (
                f"{provider._authorization_endpoint}?{urlencode(params)}"
            )

            return {"authorization_url": authorization_url}

        @router.get("/oidc/callback")
        async def oidc_callback(
            code: str,
            state: str,
            response: Response,
            session: AsyncSession = Depends(get_session),
            store: BaseSessionStore = Depends(get_session_store),
        ) -> dict:
            """
            Handle the OIDC callback after Azure AD authorization.

            Validates the state parameter, exchanges the authorization code
            for tokens, fetches user info, provisions the user, and creates
            a local session.

            Args:
                code: Authorization code from Azure AD.
                state: State parameter for CSRF validation.
                response: FastAPI response (for setting cookies).
                session: Database session (injected).
                store: Session store (injected).

            Returns:
                Dict with username and role.

            Raises:
                HTTPException: 401 if state is invalid, expired, or token
                    exchange / userinfo retrieval fails.
            """
            provider._validate_state(state)
            access_token = await provider._exchange_code(code)
            userinfo = await provider._fetch_userinfo(access_token)
            sub, email, role = provider._extract_claims(userinfo)

            user = await provision_user(
                session,
                external_id=sub,
                external_provider="oidc",
                username=email,
                role=role,
            )

            # Create session
            session_id = await store.create(
                user_id=user.id,
                username=user.username,
                role=user.role.value,
            )

            set_auth_cookie(response, session_id)

            return {"username": user.username, "role": user.role.value}

        return router
