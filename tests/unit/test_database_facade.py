"""
Unit tests for DatabaseFacade.

Tests connection string building, dialect-specific SQL generation,
session lifecycle, and query methods.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest

from tsigma.database.db import DatabaseFacade, get_db_facade


class TestBuildConnectionURL:
    """Tests for _build_connection_url across all dialects."""

    def test_postgresql(self):
        """Test PostgreSQL connection URL components."""
        facade = DatabaseFacade(
            "postgresql",
            user="tsigma",
            password="secret",
            host="localhost",
            port=5432,
            database="tsigma_db",
        )
        url = facade._build_connection_url()
        assert url.drivername == "postgresql+asyncpg"
        assert url.username == "tsigma"
        assert url.host == "localhost"
        assert url.port == 5432
        assert url.database == "tsigma_db"

    def test_mssql(self):
        """Test MS-SQL connection URL components."""
        facade = DatabaseFacade(
            "mssql",
            user="sa",
            password="pass",
            host="sqlserver",
            port=1433,
            database="tsigma_db",
            driver="ODBC+Driver+18+for+SQL+Server",
        )
        url = facade._build_connection_url()
        assert url.drivername == "mssql+aioodbc"
        assert url.host == "sqlserver"
        assert url.query.get("driver") == "ODBC+Driver+18+for+SQL+Server"

    def test_oracle(self):
        """Test Oracle connection URL components."""
        facade = DatabaseFacade(
            "oracle",
            user="tsigma",
            password="secret",
            host="oracledb",
            port=1521,
            service_name="TSIGMA",
        )
        url = facade._build_connection_url()
        assert url.drivername == "oracle+oracledb"
        assert url.database == "TSIGMA"

    def test_mysql(self):
        """Test MySQL connection URL components."""
        facade = DatabaseFacade(
            "mysql",
            user="root",
            password="pass",
            host="mysqlhost",
            port=3306,
            database="tsigma_db",
        )
        url = facade._build_connection_url()
        assert url.drivername == "mysql+aiomysql"
        assert url.database == "tsigma_db"

    def test_unsupported_dialect_raises(self):
        """Test unsupported database type raises ValueError."""
        facade = DatabaseFacade("sqlite", host="localhost")
        with pytest.raises(
            ValueError, match="Unsupported database type: sqlite"
        ):
            facade._build_connection_url()

    def test_password_not_in_repr(self):
        """Test password is not visible in string representation."""
        facade = DatabaseFacade(
            "postgresql",
            user="tsigma",
            password="super_secret",
            host="localhost",
            port=5432,
            database="tsigma_db",
        )
        url = facade._build_connection_url()
        assert "super_secret" not in repr(url)
        assert "super_secret" not in str(url)


class TestTimeBucket:
    """Tests for dialect-specific time_bucket SQL generation."""

    def test_postgresql_time_bucket(self):
        """Test PostgreSQL uses TimescaleDB time_bucket()."""
        facade = DatabaseFacade("postgresql")
        result = facade.time_bucket("event_time", "1 hour")
        assert result == "time_bucket('1 hour', event_time)"

    def test_mssql_time_bucket(self):
        """Test MS-SQL uses DATEADD/DATEDIFF pattern."""
        facade = DatabaseFacade("mssql")
        result = facade.time_bucket("event_time", "hour")
        assert result == "DATEADD(hour, DATEDIFF(hour, 0, event_time), 0)"

    def test_oracle_time_bucket(self):
        """Test Oracle uses TRUNC function."""
        facade = DatabaseFacade("oracle")
        result = facade.time_bucket("event_time", "HH")
        assert result == "TRUNC(event_time, 'HH')"

    def test_mysql_time_bucket(self):
        """Test MySQL uses DATE_FORMAT."""
        facade = DatabaseFacade("mysql")
        result = facade.time_bucket("event_time", "1 hour")
        assert result == "DATE_FORMAT(event_time, '%Y-%m-%d %H:00:00')"

    def test_unsupported_dialect_raises(self):
        """Test unsupported database type raises ValueError."""
        facade = DatabaseFacade("sqlite")
        with pytest.raises(ValueError, match="time_bucket not supported for sqlite"):
            facade.time_bucket("event_time", "1 hour")


class TestInit:
    """Tests for DatabaseFacade constructor."""

    def test_stores_db_type(self):
        """Test db_type is stored."""
        facade = DatabaseFacade("postgresql")
        assert facade.db_type == "postgresql"

    def test_stores_config(self):
        """Test config kwargs are stored."""
        facade = DatabaseFacade("postgresql", host="db.example.com", port=5432)
        assert facade.config["host"] == "db.example.com"
        assert facade.config["port"] == 5432

    def test_engine_starts_none(self):
        """Test engine is None before connect."""
        facade = DatabaseFacade("postgresql")
        assert facade._engine is None

    def test_session_factory_starts_none(self):
        """Test session factory is None before connect."""
        facade = DatabaseFacade("postgresql")
        assert facade._session_factory is None


class TestConnect:
    """Tests for connect() method."""

    @pytest.mark.asyncio
    async def test_creates_engine(self):
        """Test connect creates async engine."""
        facade = DatabaseFacade(
            "postgresql",
            user="u", password="p", host="h", port=5432, database="d",
        )

        with patch("tsigma.database.db.create_async_engine") as mock_engine, \
             patch("tsigma.database.db.async_sessionmaker"):
            mock_engine.return_value = MagicMock()
            await facade.connect()

            mock_engine.assert_called_once()
            assert facade._engine is not None

    @pytest.mark.asyncio
    async def test_creates_session_factory(self):
        """Test connect creates session factory."""
        facade = DatabaseFacade(
            "postgresql",
            user="u", password="p", host="h", port=5432, database="d",
        )

        with patch("tsigma.database.db.create_async_engine") as mock_engine, \
             patch("tsigma.database.db.async_sessionmaker") as mock_session:
            mock_engine.return_value = MagicMock()
            await facade.connect()

            mock_session.assert_called_once()
            assert facade._session_factory is not None

    @pytest.mark.asyncio
    async def test_idempotent_skips_if_connected(self):
        """Test calling connect twice doesn't recreate engine."""
        facade = DatabaseFacade(
            "postgresql",
            user="u", password="p", host="h", port=5432, database="d",
        )

        with patch("tsigma.database.db.create_async_engine") as mock_engine, \
             patch("tsigma.database.db.async_sessionmaker"):
            mock_engine.return_value = MagicMock()
            await facade.connect()
            await facade.connect()

            # Only called once - second connect is a no-op
            mock_engine.assert_called_once()

    @pytest.mark.asyncio
    async def test_pool_pre_ping_enabled(self):
        """Test connection pool uses pre_ping for health checks."""
        facade = DatabaseFacade(
            "postgresql",
            user="u", password="p", host="h", port=5432, database="d",
        )

        with patch("tsigma.database.db.create_async_engine") as mock_engine, \
             patch("tsigma.database.db.async_sessionmaker"):
            mock_engine.return_value = MagicMock()
            await facade.connect()

            call_kwargs = mock_engine.call_args[1]
            assert call_kwargs["pool_pre_ping"] is True

    @pytest.mark.asyncio
    async def test_custom_pool_size(self):
        """Test custom pool_size is passed to engine."""
        facade = DatabaseFacade(
            "postgresql",
            user="u", password="p", host="h", port=5432, database="d",
            pool_size=50,
        )

        with patch("tsigma.database.db.create_async_engine") as mock_engine, \
             patch("tsigma.database.db.async_sessionmaker"):
            mock_engine.return_value = MagicMock()
            await facade.connect()

            call_kwargs = mock_engine.call_args[1]
            assert call_kwargs["pool_size"] == 50


class TestDisconnect:
    """Tests for disconnect() method."""

    @pytest.mark.asyncio
    async def test_disposes_engine(self):
        """Test disconnect disposes the engine."""
        facade = DatabaseFacade("postgresql")
        mock_engine = AsyncMock()
        facade._engine = mock_engine
        facade._session_factory = MagicMock()

        await facade.disconnect()

        mock_engine.dispose.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_clears_engine_and_factory(self):
        """Test disconnect sets engine and factory to None."""
        facade = DatabaseFacade("postgresql")
        facade._engine = AsyncMock()
        facade._session_factory = MagicMock()

        await facade.disconnect()

        assert facade._engine is None
        assert facade._session_factory is None

    @pytest.mark.asyncio
    async def test_idempotent_when_not_connected(self):
        """Test disconnect is safe to call when not connected."""
        facade = DatabaseFacade("postgresql")
        # Should not raise
        await facade.disconnect()
        assert facade._engine is None


class TestSession:
    """Tests for session() context manager."""

    @pytest.mark.asyncio
    async def test_raises_when_not_initialized(self):
        """Test session raises RuntimeError before connect."""
        facade = DatabaseFacade("postgresql")
        with pytest.raises(RuntimeError, match="not initialized"):
            async with facade.session():
                pass

    @pytest.mark.asyncio
    async def test_commits_on_success(self):
        """Test session commits when block succeeds."""
        facade = DatabaseFacade("postgresql")
        mock_session = AsyncMock()
        mock_factory = MagicMock()
        mock_factory.return_value.__aenter__ = AsyncMock(return_value=mock_session)
        mock_factory.return_value.__aexit__ = AsyncMock(return_value=False)
        facade._session_factory = mock_factory

        async with facade.session():
            pass

        mock_session.commit.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_rollback_on_error(self):
        """Test session rolls back when block raises."""
        facade = DatabaseFacade("postgresql")
        mock_session = AsyncMock()
        mock_factory = MagicMock()
        mock_factory.return_value.__aenter__ = AsyncMock(return_value=mock_session)
        mock_factory.return_value.__aexit__ = AsyncMock(return_value=False)
        facade._session_factory = mock_factory

        with pytest.raises(ValueError):
            async with facade.session():
                raise ValueError("test error")

        mock_session.rollback.assert_awaited_once()


class TestExecute:
    """Tests for execute() method."""

    @pytest.mark.asyncio
    async def test_string_query_raises_type_error(self):
        """Test string queries raise TypeError."""
        facade = DatabaseFacade("postgresql")
        facade._session_factory = MagicMock()

        with pytest.raises(TypeError, match="not a raw string"):
            await facade.execute("SELECT 1")

    @pytest.mark.asyncio
    async def test_text_clause_is_accepted(self):
        """Test text() objects are accepted as a last resort."""
        from sqlalchemy import text

        facade = DatabaseFacade("postgresql")
        mock_session = AsyncMock()
        mock_session.commit = AsyncMock()
        mock_session.rollback = AsyncMock()
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)
        facade._session_factory = MagicMock(return_value=mock_session)

        stmt = text("SELECT 1")
        await facade.execute(stmt)
        mock_session.execute.assert_awaited_once_with(stmt)

    @pytest.mark.asyncio
    async def test_sqlalchemy_statement_passed_directly(self):
        """Test SQLAlchemy statements are passed without wrapping."""
        facade = DatabaseFacade("postgresql")
        mock_session = AsyncMock()
        mock_factory = MagicMock()
        mock_factory.return_value.__aenter__ = AsyncMock(return_value=mock_session)
        mock_factory.return_value.__aexit__ = AsyncMock(return_value=False)
        facade._session_factory = mock_factory

        from sqlalchemy import literal_column, select
        stmt = select(literal_column("1"))
        await facade.execute(stmt)

        call_args = mock_session.execute.call_args
        assert call_args[0][0] is stmt

    @pytest.mark.asyncio
    async def test_reraises_on_error(self):
        """Test execute logs and re-raises database errors."""
        facade = DatabaseFacade("postgresql")
        mock_session = AsyncMock()
        mock_session.execute.side_effect = RuntimeError("connection lost")
        mock_session.commit = AsyncMock()
        mock_session.rollback = AsyncMock()
        mock_factory = MagicMock()
        mock_factory.return_value.__aenter__ = AsyncMock(return_value=mock_session)
        mock_factory.return_value.__aexit__ = AsyncMock(return_value=False)
        facade._session_factory = mock_factory

        with pytest.raises(RuntimeError, match="connection lost"):
            await facade.execute(MagicMock())


class TestGetOne:
    """Tests for get_one() method."""

    @pytest.mark.asyncio
    async def test_returns_dict_for_single_row(self):
        """Test get_one returns dict when row exists."""
        facade = DatabaseFacade("postgresql")

        mock_row = MagicMock()
        mock_row._mapping = {"id": 1, "name": "test"}

        mock_result = MagicMock()
        mock_result.first.return_value = mock_row

        with patch.object(facade, "execute", new_callable=AsyncMock) as mock_exec:
            mock_exec.return_value = mock_result
            result = await facade.get_one(MagicMock())

        assert result == {"id": 1, "name": "test"}

    @pytest.mark.asyncio
    async def test_returns_none_for_no_rows(self):
        """Test get_one returns None when no rows match."""
        facade = DatabaseFacade("postgresql")

        mock_result = MagicMock()
        mock_result.first.return_value = None

        with patch.object(facade, "execute", new_callable=AsyncMock) as mock_exec:
            mock_exec.return_value = mock_result
            result = await facade.get_one(MagicMock())

        assert result is None


class TestGetMany:
    """Tests for get_many() method."""

    @pytest.mark.asyncio
    async def test_returns_list_of_dicts(self):
        """Test get_many returns list of dicts."""
        facade = DatabaseFacade("postgresql")

        row1 = MagicMock()
        row1._mapping = {"id": 1, "name": "a"}
        row2 = MagicMock()
        row2._mapping = {"id": 2, "name": "b"}

        mock_result = MagicMock()
        mock_result.all.return_value = [row1, row2]

        with patch.object(facade, "execute", new_callable=AsyncMock) as mock_exec:
            mock_exec.return_value = mock_result
            result = await facade.get_many(MagicMock())

        assert result == [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]

    @pytest.mark.asyncio
    async def test_returns_empty_list_for_no_rows(self):
        """Test get_many returns empty list when no rows match."""
        facade = DatabaseFacade("postgresql")

        mock_result = MagicMock()
        mock_result.all.return_value = []

        with patch.object(facade, "execute", new_callable=AsyncMock) as mock_exec:
            mock_exec.return_value = mock_result
            result = await facade.get_many(MagicMock())

        assert result == []


class TestGetDataframe:
    """Tests for get_dataframe() method."""

    @pytest.mark.asyncio
    async def test_returns_dataframe(self):
        """Test get_dataframe returns pandas DataFrame."""
        facade = DatabaseFacade("postgresql")

        row1 = MagicMock()
        row1._mapping = {"id": 1, "value": 100}
        row2 = MagicMock()
        row2._mapping = {"id": 2, "value": 200}

        mock_result = MagicMock()
        mock_result.all.return_value = [row1, row2]

        with patch.object(facade, "execute", new_callable=AsyncMock) as mock_exec:
            mock_exec.return_value = mock_result
            df = await facade.get_dataframe(MagicMock())

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        assert list(df.columns) == ["id", "value"]
        assert df.iloc[0]["value"] == 100

    @pytest.mark.asyncio
    async def test_empty_result_returns_empty_dataframe(self):
        """Test get_dataframe returns empty DataFrame with column names."""
        facade = DatabaseFacade("postgresql")

        mock_result = MagicMock()
        mock_result.all.return_value = []
        mock_result.keys.return_value = ["id", "value"]

        with patch.object(facade, "execute", new_callable=AsyncMock) as mock_exec:
            mock_exec.return_value = mock_result
            df = await facade.get_dataframe(MagicMock())

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0
        assert list(df.columns) == ["id", "value"]


class TestDataframeTosql:
    """Tests for dataframe_tosql() method."""

    @pytest.mark.asyncio
    async def test_empty_dataframe_returns_none(self):
        """Test empty DataFrame returns None without executing."""
        facade = DatabaseFacade("postgresql")

        with patch.object(facade, "execute", new_callable=AsyncMock) as mock_exec:
            result = await facade.dataframe_tosql(pd.DataFrame(), "test_table")

        assert result is None
        mock_exec.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_append_inserts_records(self):
        """Test append mode inserts records via SQLAlchemy Core."""
        facade = DatabaseFacade("postgresql")
        df = pd.DataFrame({"id": [1, 2], "value": [100, 200]})

        with patch.object(facade, "execute", new_callable=AsyncMock) as mock_exec:
            mock_exec.return_value = MagicMock()
            await facade.dataframe_tosql(df, "test_table", if_exists="append")

        # Should only have the insert call (no DELETE)
        assert mock_exec.await_count == 1

    @pytest.mark.asyncio
    async def test_replace_deletes_then_inserts(self):
        """Test replace mode deletes existing data before insert."""
        facade = DatabaseFacade("postgresql")
        df = pd.DataFrame({"id": [1], "value": [100]})

        with patch.object(facade, "execute", new_callable=AsyncMock) as mock_exec:
            mock_exec.return_value = MagicMock()
            await facade.dataframe_tosql(df, "test_table", if_exists="replace")

        # First call: DELETE, second call: INSERT
        assert mock_exec.await_count == 2
        first_arg = mock_exec.call_args_list[0][0][0]
        assert "DELETE FROM test_table" in str(first_arg)

    @pytest.mark.asyncio
    async def test_fail_raises_if_table_has_data(self):
        """Test fail mode raises ValueError when table has data."""
        facade = DatabaseFacade("postgresql")
        df = pd.DataFrame({"id": [1], "value": [100]})

        mock_result = MagicMock()
        mock_result.scalar.return_value = 5

        with patch.object(facade, "execute", new_callable=AsyncMock) as mock_exec:
            mock_exec.return_value = mock_result
            with pytest.raises(ValueError, match="already exists and contains data"):
                await facade.dataframe_tosql(df, "test_table", if_exists="fail")


class TestExecuteNotInitialized:
    """Tests for execute() when facade is not initialized."""

    @pytest.mark.asyncio
    async def test_raises_runtime_error_when_not_initialized(self):
        """execute() raises RuntimeError when _session_factory is None."""
        facade = DatabaseFacade("postgresql")
        # _session_factory is None (never connected)
        with pytest.raises(RuntimeError, match="not initialized"):
            await facade.execute(MagicMock())


class TestDataframeTosqlValidation:
    """Tests for dataframe_tosql identifier validation."""

    @pytest.mark.asyncio
    async def test_rejects_unsafe_table_name(self):
        """dataframe_tosql raises ValueError for SQL-injection table names."""
        facade = DatabaseFacade("postgresql")
        df = pd.DataFrame({"id": [1], "value": [100]})

        with pytest.raises(ValueError, match="Unsafe SQL table name"):
            await facade.dataframe_tosql(df, "DROP TABLE users; --")

    @pytest.mark.asyncio
    async def test_rejects_table_name_with_spaces(self):
        """dataframe_tosql raises ValueError for table names with spaces."""
        facade = DatabaseFacade("postgresql")
        df = pd.DataFrame({"id": [1], "value": [100]})

        with pytest.raises(ValueError, match="Unsafe SQL table name"):
            await facade.dataframe_tosql(df, "my table")


class TestGetDbFacade:
    """Tests for get_db_facade() dependency."""

    def test_raises_when_not_initialized(self):
        """Test get_db_facade raises RuntimeError when facade is None."""
        import tsigma.database.db as db_module
        original = db_module.db_facade
        try:
            db_module.db_facade = None
            with pytest.raises(RuntimeError, match="not initialized"):
                get_db_facade()
        finally:
            db_module.db_facade = original

    def test_returns_facade_when_initialized(self):
        """Test get_db_facade returns facade when set."""
        import tsigma.database.db as db_module
        original = db_module.db_facade
        try:
            mock_facade = MagicMock(spec=DatabaseFacade)
            db_module.db_facade = mock_facade
            result = get_db_facade()
            assert result is mock_facade
        finally:
            db_module.db_facade = original
