"""
Unit tests for DialectHelper audit methods and SQL generation.

Tests audit_trigger_sql, set_app_user_sql, time_bucket, and
identifier validation across all supported database dialects.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

from tsigma.database.db import DatabaseFacade, DialectHelper, _validate_identifier


class TestAuditTriggerSqlPostgresql:
    """audit_trigger_sql for PostgreSQL."""

    def test_returns_function_and_trigger(self):
        helper = DialectHelper("postgresql")
        stmts = helper.audit_trigger_sql("signal", "signal_audit", ["signal_id"])
        assert len(stmts) == 2
        assert "CREATE OR REPLACE FUNCTION" in stmts[0]
        assert "audit_signal_changes" in stmts[0]
        assert "CREATE TRIGGER" in stmts[1]
        assert "signal_audit_trigger" in stmts[1]

    def test_multi_column_pk(self):
        helper = DialectHelper("postgresql")
        stmts = helper.audit_trigger_sql(
            "approach", "approach_audit", ["approach_id", "signal_id"]
        )
        assert "OLD.approach_id, OLD.signal_id" in stmts[0]
        assert "NEW.approach_id, NEW.signal_id" in stmts[0]


class TestAuditTriggerSqlMssql:
    """audit_trigger_sql for MS-SQL."""

    def test_returns_single_trigger(self):
        helper = DialectHelper("mssql")
        stmts = helper.audit_trigger_sql("signal", "signal_audit", ["signal_id"])
        assert len(stmts) == 1
        assert "CREATE OR ALTER TRIGGER" in stmts[0]
        assert "SESSION_CONTEXT" in stmts[0]
        assert "INSERTED" in stmts[0]
        assert "DELETED" in stmts[0]


class TestAuditTriggerSqlOracle:
    """audit_trigger_sql for Oracle."""

    def test_returns_single_trigger(self):
        helper = DialectHelper("oracle")
        stmts = helper.audit_trigger_sql("signal", "signal_audit", ["signal_id"])
        assert len(stmts) == 1
        assert "CREATE OR REPLACE TRIGGER" in stmts[0]
        assert "SYS_CONTEXT" in stmts[0]
        assert "DELETING" in stmts[0]
        assert "UPDATING" in stmts[0]
        assert "INSERTING" in stmts[0]


class TestAuditTriggerSqlMysql:
    """audit_trigger_sql for MySQL."""

    def test_returns_three_triggers(self):
        helper = DialectHelper("mysql")
        stmts = helper.audit_trigger_sql("signal", "signal_audit", ["signal_id"])
        assert len(stmts) == 3

        # Each trigger targets a different operation
        ops = ["INSERT", "UPDATE", "DELETE"]
        for stmt, op in zip(stmts, ops):
            assert f"AFTER {op}" in stmt
            assert "signal_audit" in stmt

    def test_trigger_names(self):
        helper = DialectHelper("mysql")
        stmts = helper.audit_trigger_sql("signal", "signal_audit", ["signal_id"])
        assert "signal_audit_insert" in stmts[0]
        assert "signal_audit_update" in stmts[1]
        assert "signal_audit_delete" in stmts[2]


class TestSetAppUserSqlAllDialects:
    """set_app_user_sql returns correct SQL for each dialect."""

    def test_postgresql(self):
        sql = DialectHelper("postgresql").set_app_user_sql()
        assert "SET LOCAL" in sql
        assert "app.current_user" in sql
        assert ":username" in sql

    def test_mssql(self):
        sql = DialectHelper("mssql").set_app_user_sql()
        assert "sp_set_session_context" in sql
        assert ":username" in sql

    def test_oracle(self):
        sql = DialectHelper("oracle").set_app_user_sql()
        assert "DBMS_SESSION.SET_CONTEXT" in sql
        assert "CLIENTCONTEXT" in sql
        assert ":username" in sql

    def test_mysql(self):
        sql = DialectHelper("mysql").set_app_user_sql()
        assert "@app_user" in sql
        assert ":username" in sql


class TestTimeBucketAllDialects:
    """time_bucket returns correct SQL per dialect."""

    def test_postgresql(self):
        result = DialectHelper("postgresql").time_bucket("ts_col", "1 hour")
        assert "time_bucket" in result
        assert "ts_col" in result

    def test_mssql(self):
        result = DialectHelper("mssql").time_bucket("ts_col", "hour")
        assert "DATEADD" in result
        assert "DATEDIFF" in result

    def test_oracle(self):
        result = DialectHelper("oracle").time_bucket("ts_col", "HH")
        assert "TRUNC" in result
        assert "ts_col" in result

    def test_mysql(self):
        result = DialectHelper("mysql").time_bucket("ts_col", "1 hour")
        assert "DATE_FORMAT" in result

    def test_unsafe_interval_rejected(self):
        with pytest.raises(ValueError, match="Unsafe interval"):
            DialectHelper("postgresql").time_bucket("ts_col", "1; DROP TABLE x")


class TestDeleteWindowSqlAllDialects:
    """delete_window_sql returns correct SQL for each dialect."""

    def test_postgresql(self):
        sql = DialectHelper("postgresql").delete_window_sql("events", "ts", 24)
        assert "DELETE FROM events" in sql
        assert "NOW()" in sql
        assert "24 hours" in sql

    def test_mssql(self):
        sql = DialectHelper("mssql").delete_window_sql("events", "ts", 24)
        assert "DELETE FROM events" in sql
        assert "DATEADD" in sql
        assert "GETUTCDATE()" in sql

    def test_oracle(self):
        sql = DialectHelper("oracle").delete_window_sql("events", "ts", 24)
        assert "DELETE FROM events" in sql
        assert "SYSTIMESTAMP" in sql
        assert "INTERVAL" in sql

    def test_mysql(self):
        sql = DialectHelper("mysql").delete_window_sql("events", "ts", 24)
        assert "DELETE FROM events" in sql
        assert "DATE_SUB" in sql
        assert "UTC_TIMESTAMP()" in sql

    def test_unsupported_dialect_raises(self):
        with pytest.raises(ValueError, match="not supported"):
            DialectHelper("sqlite").delete_window_sql("events", "ts", 24)


class TestLookbackPredicateAllDialects:
    """lookback_predicate returns correct SQL for each dialect."""

    def test_postgresql(self):
        sql = DialectHelper("postgresql").lookback_predicate("ts", 12)
        assert "ts >=" in sql
        assert "NOW()" in sql
        assert "12 hours" in sql

    def test_mssql(self):
        sql = DialectHelper("mssql").lookback_predicate("ts", 12)
        assert "ts >=" in sql
        assert "DATEADD" in sql
        assert "GETUTCDATE()" in sql

    def test_oracle(self):
        sql = DialectHelper("oracle").lookback_predicate("ts", 12)
        assert "ts >=" in sql
        assert "SYSTIMESTAMP" in sql

    def test_mysql(self):
        sql = DialectHelper("mysql").lookback_predicate("ts", 12)
        assert "ts >=" in sql
        assert "DATE_SUB" in sql
        assert "UTC_TIMESTAMP()" in sql

    def test_unsupported_dialect_raises(self):
        with pytest.raises(ValueError, match="not supported"):
            DialectHelper("sqlite").lookback_predicate("ts", 12)


class TestDeleteWindowValidatesIdentifiers:
    """delete_window_sql rejects injection in table/column names."""

    def test_bad_table_name_raises(self):
        with pytest.raises(ValueError, match="Unsafe SQL"):
            DialectHelper("postgresql").delete_window_sql(
                "events; DROP TABLE x", "ts", 24
            )

    def test_bad_column_name_raises(self):
        with pytest.raises(ValueError, match="Unsafe SQL"):
            DialectHelper("postgresql").delete_window_sql(
                "events", "ts; DROP", 24
            )

    def test_lookback_bad_column_raises(self):
        with pytest.raises(ValueError, match="Unsafe SQL"):
            DialectHelper("postgresql").lookback_predicate("ts'--", 24)


class TestValidateIdentifierRejectsInjection:
    """_validate_identifier rejects SQL injection attempts."""

    def test_normal_identifier_accepted(self):
        assert _validate_identifier("signal_audit") == "signal_audit"

    def test_identifier_with_spaces_rejected(self):
        with pytest.raises(ValueError, match="Unsafe SQL"):
            _validate_identifier("signal audit")

    def test_identifier_with_semicolon_rejected(self):
        with pytest.raises(ValueError, match="Unsafe SQL"):
            _validate_identifier("signal; DROP TABLE x")

    def test_identifier_with_quotes_rejected(self):
        with pytest.raises(ValueError, match="Unsafe SQL"):
            _validate_identifier("signal'--")

    def test_identifier_with_dash_rejected(self):
        with pytest.raises(ValueError, match="Unsafe SQL"):
            _validate_identifier("signal-audit")

    def test_identifier_starting_with_digit_rejected(self):
        with pytest.raises(ValueError, match="Unsafe SQL"):
            _validate_identifier("1signal")

    def test_empty_string_rejected(self):
        with pytest.raises(ValueError, match="Unsafe SQL"):
            _validate_identifier("")

    def test_audit_trigger_rejects_bad_table(self):
        helper = DialectHelper("postgresql")
        with pytest.raises(ValueError, match="Unsafe SQL"):
            helper.audit_trigger_sql("signal; DROP", "signal_audit", ["signal_id"])

    def test_audit_trigger_rejects_bad_id_column(self):
        helper = DialectHelper("postgresql")
        with pytest.raises(ValueError, match="Unsafe SQL"):
            helper.audit_trigger_sql("signal", "signal_audit", ["id; DROP"])


# ---------------------------------------------------------------------------
# DatabaseFacade._build_connection_url tests (lines 626-661 / 676-727)
# ---------------------------------------------------------------------------


class TestBuildConnectionUrl:
    """Tests for DatabaseFacade._build_connection_url() across all dialects."""

    def test_postgresql_url(self):
        """PostgreSQL URL uses postgresql+asyncpg driver."""
        facade = DatabaseFacade(
            "postgresql",
            user="pguser",
            password="pgpass",
            host="db.example.com",
            port=5432,
            database="mydb",
        )
        url = facade._build_connection_url()
        assert str(url).startswith("postgresql+asyncpg://")
        assert "pguser" in str(url)
        assert "db.example.com" in str(url)
        assert "mydb" in str(url)

    def test_mssql_url(self):
        """MS-SQL URL uses mssql+aioodbc driver."""
        facade = DatabaseFacade(
            "mssql",
            user="sa",
            password="secret",
            host="sqlserver",
            port=1433,
            database="tsigma",
        )
        url = facade._build_connection_url()
        assert str(url).startswith("mssql+aioodbc://")
        assert "tsigma" in str(url)

    def test_mssql_url_with_driver(self):
        """MS-SQL URL includes ODBC driver query param when specified."""
        facade = DatabaseFacade(
            "mssql",
            user="sa",
            password="secret",
            host="sqlserver",
            port=1433,
            database="tsigma",
            driver="ODBC Driver 18 for SQL Server",
        )
        url = facade._build_connection_url()
        assert "driver" in str(url)

    def test_oracle_url(self):
        """Oracle URL uses oracle+oracledb driver and service_name."""
        facade = DatabaseFacade(
            "oracle",
            user="system",
            password="oracle",
            host="oradb",
            port=1521,
            service_name="ORCL",
        )
        url = facade._build_connection_url()
        assert str(url).startswith("oracle+oracledb://")
        assert "ORCL" in str(url)

    def test_mysql_url(self):
        """MySQL URL uses mysql+aiomysql driver."""
        facade = DatabaseFacade(
            "mysql",
            user="root",
            password="mysql",
            host="mysqlhost",
            port=3306,
            database="tsigma",
        )
        url = facade._build_connection_url()
        assert str(url).startswith("mysql+aiomysql://")
        assert "tsigma" in str(url)

    def test_unsupported_dialect_raises(self):
        """Unsupported db_type raises ValueError."""
        facade = DatabaseFacade(
            "sqlite",
            user="",
            password="",
            host="localhost",
            port=0,
            database="test.db",
        )
        with pytest.raises(ValueError, match="Unsupported database type"):
            facade._build_connection_url()

    def test_default_host_and_port(self):
        """URL uses default host/port when not specified."""
        facade = DatabaseFacade("postgresql")
        url = facade._build_connection_url()
        assert "localhost" in str(url)
        assert "5432" in str(url)


# ---------------------------------------------------------------------------
# DatabaseFacade helper methods (lines 500-502, 523-525, 559-561)
# ---------------------------------------------------------------------------


class TestDatabaseFacadeErrorPaths:
    """Tests for DatabaseFacade error logging paths."""

    @pytest.mark.asyncio
    async def test_get_one_reraises(self):
        """get_one re-raises exceptions after logging (lines 500-502)."""
        facade = DatabaseFacade("postgresql", user="u", password="p",
                                host="h", port=5432, database="d")
        facade._engine = MagicMock()
        facade._session_factory = MagicMock()

        # Make session().execute() raise
        mock_session = AsyncMock()
        mock_session.execute = AsyncMock(side_effect=RuntimeError("db error"))
        mock_session.commit = AsyncMock()
        mock_session.rollback = AsyncMock()

        mock_ctx = MagicMock()
        mock_ctx.__aenter__ = AsyncMock(return_value=mock_session)
        mock_ctx.__aexit__ = AsyncMock(return_value=False)
        facade._session_factory.return_value = mock_ctx

        with pytest.raises(RuntimeError, match="db error"):
            await facade.get_one(MagicMock())

    @pytest.mark.asyncio
    async def test_get_many_reraises(self):
        """get_many re-raises exceptions after logging (lines 523-525)."""
        facade = DatabaseFacade("postgresql", user="u", password="p",
                                host="h", port=5432, database="d")
        facade._engine = MagicMock()
        facade._session_factory = MagicMock()

        mock_session = AsyncMock()
        mock_session.execute = AsyncMock(side_effect=RuntimeError("timeout"))
        mock_session.commit = AsyncMock()
        mock_session.rollback = AsyncMock()

        mock_ctx = MagicMock()
        mock_ctx.__aenter__ = AsyncMock(return_value=mock_session)
        mock_ctx.__aexit__ = AsyncMock(return_value=False)
        facade._session_factory.return_value = mock_ctx

        with pytest.raises(RuntimeError, match="timeout"):
            await facade.get_many(MagicMock())

    @pytest.mark.asyncio
    async def test_get_dataframe_reraises(self):
        """get_dataframe re-raises exceptions after logging (lines 559-561)."""
        facade = DatabaseFacade("postgresql", user="u", password="p",
                                host="h", port=5432, database="d")
        facade._engine = MagicMock()
        facade._session_factory = MagicMock()

        mock_session = AsyncMock()
        mock_session.execute = AsyncMock(side_effect=RuntimeError("df error"))
        mock_session.commit = AsyncMock()
        mock_session.rollback = AsyncMock()

        mock_ctx = MagicMock()
        mock_ctx.__aenter__ = AsyncMock(return_value=mock_session)
        mock_ctx.__aexit__ = AsyncMock(return_value=False)
        facade._session_factory.return_value = mock_ctx

        with pytest.raises(RuntimeError, match="df error"):
            await facade.get_dataframe(MagicMock())


class TestDatabaseFacadeAuditTriggerUnsupported:
    """Tests for audit_trigger_sql with unsupported dialect (line 200)."""

    def test_unsupported_audit_trigger_raises(self):
        helper = DialectHelper("sqlite")
        with pytest.raises(ValueError, match="not supported"):
            helper.audit_trigger_sql("t", "t_audit", ["id"])


class TestTimeBucketUnsupported:
    """Tests for time_bucket with unsupported dialect."""

    def test_unsupported_time_bucket_raises(self):
        helper = DialectHelper("sqlite")
        with pytest.raises(ValueError, match="not supported"):
            helper.time_bucket("ts", "1 hour")


class TestSetAppUserUnsupported:
    """Tests for set_app_user_sql with unsupported dialect (line 353)."""

    def test_unsupported_set_app_user_raises(self):
        helper = DialectHelper("sqlite")
        with pytest.raises(ValueError, match="not supported"):
            helper.set_app_user_sql()
