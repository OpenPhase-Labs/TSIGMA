"""Shared test helpers.

This module provides utilities used across the unit and integration test
suites. Kept underscore-prefixed so pytest does not collect it as a test
module.
"""

from unittest.mock import AsyncMock, MagicMock


def make_mock_session() -> AsyncMock:
    """Return an AsyncMock for AsyncSession with sync-method overrides.

    AsyncSession.add / add_all / expunge / expunge_all / expire / expire_all /
    begin / begin_nested / get_bind / get_transaction / get_nested_transaction /
    in_transaction / in_nested_transaction / is_modified are synchronous methods
    on the production class. Bare AsyncMock() treats every attribute as async by
    default, causing `RuntimeWarning: coroutine was never awaited` when production
    code calls these methods plainly. This factory overrides each sync method to
    a MagicMock so the mock's sync surface matches AsyncSession's real one.

    Do NOT override delete() - it IS async on SQLAlchemy 2.0.46+.
    """
    mock_session = AsyncMock()
    mock_session.add = MagicMock()
    mock_session.add_all = MagicMock()
    mock_session.expunge = MagicMock()
    mock_session.expunge_all = MagicMock()
    mock_session.expire = MagicMock()
    mock_session.expire_all = MagicMock()
    mock_session.begin = MagicMock()
    mock_session.begin_nested = MagicMock()
    mock_session.get_bind = MagicMock()
    mock_session.get_transaction = MagicMock()
    mock_session.get_nested_transaction = MagicMock()
    mock_session.in_transaction = MagicMock()
    mock_session.in_nested_transaction = MagicMock()
    mock_session.is_modified = MagicMock()
    return mock_session


def make_mock_session_factory():
    """Return ``(session_factory, session)`` for the ``async with`` pattern.

    Every call to the factory yields the SAME session, so a test can inspect
    every row a multi-session code path added - `ingest_raw` opens a fresh
    session per stage.

    Returns:
        A 2-tuple of the factory and the session it yields.
    """
    session = make_mock_session()
    context = MagicMock()
    context.__aenter__ = AsyncMock(return_value=session)
    context.__aexit__ = AsyncMock(return_value=None)
    return MagicMock(return_value=context), session
