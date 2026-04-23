"""
Admin user seeding.

Creates the default admin user on first startup if no admin exists.
Refuses to start if the admin password is the insecure default.
"""

import logging

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from tsigma.auth.models import AuthUser, UserRole
from tsigma.auth.passwords import hash_password
from tsigma.config import settings

logger = logging.getLogger(__name__)

FORBIDDEN_PASSWORDS = frozenset({
    "changeme",
    "password",
    "admin",
    "123456",
    "",
})


async def seed_admin(session: AsyncSession) -> None:
    """
    Create default admin user if no admin exists.

    Checks for any user with admin role. If none found,
    creates one using credentials from settings.

    Raises:
        SystemExit: If the admin password is an insecure default.

    Args:
        session: Active database session (caller manages commit).
    """
    if settings.auth_mode != "local":
        logger.info("Auth mode is %s, skipping local admin seed", settings.auth_mode)
        return

    result = await session.execute(
        select(AuthUser).where(AuthUser.role == UserRole.ADMIN).limit(1)
    )
    existing = result.scalar_one_or_none()

    if existing is not None:
        logger.info("Admin user already exists, skipping seed")
        return

    # Block startup if the admin password is an insecure default
    if settings.auth_admin_password in FORBIDDEN_PASSWORDS:
        logger.critical(
            "TSIGMA_AUTH_ADMIN_PASSWORD is set to an insecure default ('%s'). "
            "Set a strong password via environment variable or .env file before starting.",
            settings.auth_admin_password,
        )
        raise SystemExit(
            "Refusing to start: TSIGMA_AUTH_ADMIN_PASSWORD must be changed "
            "from the default value. Set it in your environment or .env file."
        )

    admin = AuthUser(
        username=settings.auth_admin_user,
        password_hash=hash_password(settings.auth_admin_password),
        role=UserRole.ADMIN,
        is_active=True,
    )
    session.add(admin)
    await session.flush()
    logger.info("Default admin user '%s' created", settings.auth_admin_user)
