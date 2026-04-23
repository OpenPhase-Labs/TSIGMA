"""
Just-In-Time user provisioning for external auth providers.

Shared logic for OIDC and OAuth2 providers. Creates or updates
AuthUser records on first login via an external identity provider.
"""

import logging

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from tsigma.auth.models import AuthUser, UserRole

logger = logging.getLogger(__name__)

# Impossible bcrypt hash — blocks local login for externally-provisioned
# users. A valid bcrypt hash is 60 chars starting with "$2b$". This value
# will never match any password via verify_password().
_EXTERNAL_PASSWORD_HASH = "!external:no-local-login"


async def provision_user(
    session: AsyncSession,
    *,
    external_id: str,
    external_provider: str,
    username: str,
    role: UserRole,
) -> AuthUser:
    """
    Create or update an AuthUser from external identity claims.

    Lookup priority:
    1. By external_id + external_provider (stable identifier from IdP).
    2. By username (handles pre-created local users with same email).
    3. Create new user if neither match.

    Role policy: upgrades to admin if group-qualified, never downgrades.
    Password: set to "!external" sentinel (not valid bcrypt, blocks local login).

    Args:
        session: Active database session.
        external_id: IdP subject identifier (sub claim).
        external_provider: Provider name ("oidc" or "oauth2").
        username: Email or preferred_username from IdP.
        role: Resolved role based on group membership.

    Returns:
        AuthUser instance (attached to session).
    """
    # 1. Look up by external_id (stable)
    result = await session.execute(
        select(AuthUser).where(
            AuthUser.external_id == external_id,
            AuthUser.external_provider == external_provider,
        )
    )
    user = result.scalar_one_or_none()

    if user is not None:
        if user.username != username:
            user.username = username
        if role == UserRole.ADMIN and user.role != UserRole.ADMIN:
            user.role = UserRole.ADMIN
        await session.flush()
        return user

    # 2. Look up by username (link existing local user)
    result = await session.execute(
        select(AuthUser).where(AuthUser.username == username)
    )
    user = result.scalar_one_or_none()

    if user is not None:
        user.external_id = external_id
        user.external_provider = external_provider
        await session.flush()
        return user

    # 3. Create new user (JIT provisioning)
    user = AuthUser(
        username=username,
        password_hash=_EXTERNAL_PASSWORD_HASH,
        role=role,
        is_active=True,
        external_id=external_id,
        external_provider=external_provider,
    )
    session.add(user)
    await session.flush()
    logger.info("JIT provisioned user %s via %s", username, external_provider)
    return user
