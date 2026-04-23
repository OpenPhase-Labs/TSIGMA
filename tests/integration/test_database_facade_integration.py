"""
Integration tests for DatabaseFacade against a real PostgreSQL database.

These tests verify actual database connectivity, session management,
query execution, and DataFrame operations against a live database.

Requirements:
    - Running PostgreSQL instance
    - TSIGMA_TEST_DB_URL environment variable set, e.g.:
        export TSIGMA_TEST_DB_URL="postgresql+asyncpg://user:pass@localhost:5432/tsigma_test"

Skipped automatically when TSIGMA_TEST_DB_URL is not set.
"""

import pandas as pd
import pytest

pytestmark = pytest.mark.integration


class TestConnect:
    """Tests for real database connection lifecycle."""

    @pytest.mark.asyncio
    async def test_connect_and_disconnect(self, db_facade):
        """Test connecting and disconnecting from a real database."""
        assert db_facade._engine is not None
        assert db_facade._session_factory is not None

    @pytest.mark.asyncio
    async def test_session_yields_working_session(self, db_facade):
        """Test session context manager yields a usable session."""
        from sqlalchemy import literal_column, select

        async with db_facade.session() as session:
            result = await session.execute(select(literal_column("1").label("val")))
            row = result.first()
            assert row[0] == 1


class TestExecute:
    """Tests for execute() against a real database."""

    @pytest.mark.asyncio
    async def test_execute_sqlalchemy_statement(self, db_facade):
        """Test executing a SQLAlchemy Core statement."""
        from sqlalchemy import literal_column, select
        stmt = select(literal_column("42").label("answer"))

        result = await db_facade.execute(stmt)
        row = result.first()
        assert row[0] == 42

    @pytest.mark.asyncio
    async def test_execute_rejects_raw_string(self, db_facade):
        """Test execute raises TypeError for raw SQL strings."""
        with pytest.raises(TypeError, match="not a raw string"):
            await db_facade.execute("SELECT 1 AS val")

    @pytest.mark.asyncio
    async def test_execute_accepts_text_clause(self, db_facade):
        """Test execute accepts text() as a last resort."""
        from sqlalchemy import text

        result = await db_facade.execute(text("SELECT 1 AS val"))
        assert result is not None


class TestDataframeTosql:
    """Tests for dataframe_tosql() against a real database."""

    @pytest.mark.asyncio
    async def test_empty_dataframe_is_noop(self, db_facade):
        """Test empty DataFrame does not execute any queries."""
        result = await db_facade.dataframe_tosql(pd.DataFrame(), "nonexistent_table")
        assert result is None


class TestTimeBucket:
    """Tests for time_bucket() SQL generation (no execution — requires ORM)."""

    def test_time_bucket_generates_sql(self):
        """Test time_bucket returns valid SQL expression."""
        from tsigma.database.db import DatabaseFacade
        facade = DatabaseFacade("postgresql")
        bucket_expr = facade.time_bucket("event_time", "1 hour")
        assert "time_bucket" in bucket_expr
