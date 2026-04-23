"""
Unit tests for dependency injection.

Tests get_session yields a working async session from the DatabaseFacade
and properly handles commit/rollback lifecycle.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tsigma.dependencies import get_session


class TestGetSession:
    """Tests for get_session() dependency."""

    @pytest.mark.asyncio
    async def test_yields_session(self):
        """Test get_session yields an AsyncSession from the facade."""
        mock_session = AsyncMock()
        mock_facade = MagicMock()
        mock_facade._session_factory = MagicMock()
        mock_factory_ctx = MagicMock()
        mock_factory_ctx.__aenter__ = AsyncMock(return_value=mock_session)
        mock_factory_ctx.__aexit__ = AsyncMock(return_value=False)
        mock_facade._session_factory.return_value = mock_factory_ctx

        with patch("tsigma.dependencies.get_db_facade", return_value=mock_facade):
            session = None
            async for s in get_session():
                session = s
                break

            assert session is mock_session

    @pytest.mark.asyncio
    async def test_commits_on_success(self):
        """Test session is committed when no exception occurs."""
        mock_session = AsyncMock()
        mock_facade = MagicMock()
        mock_facade._session_factory = MagicMock()
        mock_factory_ctx = MagicMock()
        mock_factory_ctx.__aenter__ = AsyncMock(return_value=mock_session)
        mock_factory_ctx.__aexit__ = AsyncMock(return_value=False)
        mock_facade._session_factory.return_value = mock_factory_ctx

        with patch("tsigma.dependencies.get_db_facade", return_value=mock_facade):
            async for s in get_session():
                pass

        mock_session.commit.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_rollback_on_error(self):
        """Test session is rolled back when an exception occurs.

        Uses athrow() directly to match FastAPI's dependency injection
        behavior (async for calls aclose on exception, not athrow).
        """
        mock_session = AsyncMock()
        mock_facade = MagicMock()
        mock_facade._session_factory = MagicMock()
        mock_factory_ctx = MagicMock()
        mock_factory_ctx.__aenter__ = AsyncMock(return_value=mock_session)
        mock_factory_ctx.__aexit__ = AsyncMock(return_value=False)
        mock_facade._session_factory.return_value = mock_factory_ctx

        with patch("tsigma.dependencies.get_db_facade", return_value=mock_facade):
            gen = get_session()
            # Advance to yield
            session = await gen.__anext__()
            assert session is mock_session

            # Simulate FastAPI throwing an exception back into the generator
            with pytest.raises(ValueError):
                await gen.athrow(ValueError("test error"))

        mock_session.rollback.assert_awaited_once()
        mock_session.commit.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_raises_when_facade_not_initialized(self):
        """Test get_session raises RuntimeError when facade is None."""
        with patch(
            "tsigma.dependencies.get_db_facade",
            side_effect=RuntimeError("Database facade not initialized"),
        ):
            with pytest.raises(RuntimeError, match="not initialized"):
                async for _ in get_session():
                    pass


class TestGetAuditedSession:
    """Tests for get_audited_session() dependency."""

    @pytest.mark.asyncio
    async def test_sets_user_from_session_cookie(self):
        """When session cookie resolves to a user, SET LOCAL is executed."""
        from tsigma.dependencies import get_audited_session

        mock_session = AsyncMock()
        mock_request = MagicMock()

        # Session store returns a session with a username
        mock_session_data = MagicMock()
        mock_session_data.username = "jsloan"
        mock_store = AsyncMock()
        mock_store.get = AsyncMock(return_value=mock_session_data)
        mock_request.app.state.session_store = mock_store
        mock_request.cookies.get.return_value = "sess-abc123"

        mock_facade = MagicMock()
        mock_facade.dialect.set_app_user_sql.return_value = (
            "SET LOCAL app.current_user = :username"
        )

        with patch("tsigma.database.db.get_db_facade", return_value=mock_facade):
            with patch("tsigma.config.settings") as mock_settings:
                mock_settings.auth_cookie_name = "tsigma_session"
                result = await get_audited_session(mock_request, mock_session)

        assert result is mock_session
        mock_session.execute.assert_awaited_once()
        call_args = mock_session.execute.call_args
        assert "app.current_user" in str(call_args[0][0].text)

    @pytest.mark.asyncio
    async def test_no_cookie_skips_set_local(self):
        """When no session cookie is present, SET LOCAL is not executed."""
        from tsigma.dependencies import get_audited_session

        mock_session = AsyncMock()
        mock_request = MagicMock()

        mock_store = AsyncMock()
        mock_request.app.state.session_store = mock_store
        mock_request.cookies.get.return_value = None

        with patch("tsigma.config.settings") as mock_settings:
            mock_settings.auth_cookie_name = "tsigma_session"
            result = await get_audited_session(mock_request, mock_session)

        assert result is mock_session
        mock_session.execute.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_no_store_skips_set_local(self):
        """When no session store is on app state, SET LOCAL is not executed."""
        from tsigma.dependencies import get_audited_session

        mock_session = AsyncMock()
        mock_request = MagicMock()
        mock_request.app.state = MagicMock(spec=[])  # no session_store attr

        result = await get_audited_session(mock_request, mock_session)

        assert result is mock_session
        mock_session.execute.assert_not_awaited()
