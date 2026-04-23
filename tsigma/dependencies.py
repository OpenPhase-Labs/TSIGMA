"""
FastAPI dependency injection.

Provides reusable dependencies for database sessions, authentication, etc.
"""

from typing import AsyncGenerator

from fastapi import Depends, Request
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from .database.db import get_db_facade


async def get_session() -> AsyncGenerator[AsyncSession, None]:
    """
    Dependency for database sessions.

    Gets the DatabaseFacade, opens a session, and handles
    commit on success / rollback on error.

    Yields:
        AsyncSession for database operations.

    Raises:
        RuntimeError: If DatabaseFacade has not been initialized.
    """
    facade = get_db_facade()
    async with facade._session_factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


async def get_audited_session(
    request: Request,
    session: AsyncSession = Depends(get_session),
) -> AsyncSession:
    """
    Database session with the authenticated user set for audit triggers.

    Sets ``app.current_user`` via ``SET LOCAL`` so PostgreSQL triggers
    can attribute changes to the correct user. ``SET LOCAL`` is
    transaction-scoped — safe with connection pooling and async.

    Use this instead of ``get_session`` on any endpoint that modifies
    signal, approach, or detector tables.

    Args:
        request: FastAPI request (for reading session cookie).
        session: Base database session (injected).

    Returns:
        The same session, with ``app.current_user`` set.
    """
    # Import here to avoid circular dependency
    from .auth.sessions import BaseSessionStore
    from .config import settings

    store: BaseSessionStore | None = getattr(request.app.state, "session_store", None)
    username = None

    if store:
        session_id = request.cookies.get(settings.auth_cookie_name)
        if session_id:
            session_data = await store.get(session_id)
            if session_data:
                username = session_data.username

    if username:
        from .database.db import get_db_facade
        facade = get_db_facade()
        sql = facade.dialect.set_app_user_sql()
        await session.execute(text(sql), {"username": username})

    return session
