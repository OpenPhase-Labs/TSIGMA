"""
Database Facade - Complete Abstraction

Handles all database concerns:
- Connection management
- Connection pooling
- Dialect abstraction (PostgreSQL, MS-SQL, Oracle, MySQL)
- Session lifecycle
- Transaction management

Routes never see connection details. Everything is abstracted here.

Multi-tenancy: Each tenant gets a separate app instance with its own database.
No schema switching needed - complete instance isolation.
"""

import logging
import re
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Literal

import pandas as pd
from sqlalchemy import URL, column, table, text
from sqlalchemy.engine import Result
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

logger = logging.getLogger(__name__)

# Strict SQL identifier pattern: letters, digits, underscores only.
_SAFE_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _validate_identifier(value: str, label: str = "identifier") -> str:
    """Validate that a value is a safe SQL identifier.

    Raises ValueError if the value contains anything other than
    letters, digits, and underscores.
    """
    if not _SAFE_IDENTIFIER_RE.match(value):
        raise ValueError(
            f"Unsafe SQL {label}: {value!r}. "
            "Only letters, digits, and underscores are allowed."
        )
    return value


class DialectHelper:
    """
    Dialect-specific SQL generation.

    Pure functions of db_type with no dependency on engine or session.
    Extracted from DatabaseFacade per SRP — connection management and
    SQL dialect logic are separate concerns.
    """

    # Schema mapping: logical schema → physical schema name.
    # MySQL doesn't support schemas (database = schema), so all resolve to None.
    SCHEMAS = {
        "config": "config",
        "events": "events",
        "aggregation": "aggregation",
        "identity": "identity",
    }

    def __init__(self, db_type: Literal["postgresql", "mssql", "oracle", "mysql"]):
        self.db_type = db_type

    def schema(self, logical_schema: str) -> str | None:
        """
        Resolve a logical schema name to a physical schema name.

        PostgreSQL, MS-SQL, and Oracle support schemas natively.
        MySQL does not — all tables go in the default database.

        Args:
            logical_schema: One of "config", "events", "aggregation", "identity".

        Returns:
            Physical schema name, or None for MySQL.
        """
        if self.db_type == "mysql":
            return None
        return self.SCHEMAS.get(logical_schema, logical_schema)

    def create_schemas_sql(self) -> list[str]:
        """
        Generate SQL statements to create all schemas.

        Returns empty list for MySQL (no schema support).

        Returns:
            List of CREATE SCHEMA IF NOT EXISTS statements.
        """
        if self.db_type == "mysql":
            return []

        stmts = []
        for schema_name in self.SCHEMAS.values():
            if self.db_type == "oracle":
                # Oracle uses CREATE USER for schemas
                stmts.append(
                    f"BEGIN EXECUTE IMMEDIATE 'CREATE USER {schema_name} "
                    f"IDENTIFIED BY {schema_name}'; "
                    f"EXCEPTION WHEN OTHERS THEN NULL; END;"
                )
            else:
                stmts.append(
                    f"CREATE SCHEMA IF NOT EXISTS {schema_name}"
                )
        return stmts

    # Allowed interval values for time_bucket across all dialects.
    _SAFE_INTERVALS = frozenset({
        "1 hour", "1 day", "15 minutes", "5 minutes", "30 minutes",
        "hour", "day", "minute", "HH", "DD", "MI",
    })

    def time_bucket(self, timestamp_column: str, interval: str) -> str:
        """
        Dialect-specific time bucketing.

        Returns SQL expression for time bucket aggregation.
        Abstracts differences between databases.
        """
        _validate_identifier(timestamp_column, "column name")
        if interval not in self._SAFE_INTERVALS:
            raise ValueError(
                f"Unsafe interval: {interval!r}. "
                f"Allowed: {sorted(self._SAFE_INTERVALS)}"
            )

        if self.db_type == "postgresql":
            return f"time_bucket('{interval}', {timestamp_column})"

        elif self.db_type == "mssql":
            return f"DATEADD({interval}, DATEDIFF({interval}, 0, {timestamp_column}), 0)"

        elif self.db_type == "oracle":
            return f"TRUNC({timestamp_column}, '{interval}')"

        elif self.db_type == "mysql":
            return f"DATE_FORMAT({timestamp_column}, '%Y-%m-%d %H:00:00')"

        else:
            raise ValueError(f"time_bucket not supported for {self.db_type}")

    def delete_window_sql(self, table: str, time_column: str, hours: int) -> str:
        """
        Dialect-specific SQL to delete rows within a lookback window.

        Returns a parameterised DELETE statement. The caller does NOT need
        to pass any bind parameters — the cutoff is computed server-side.
        """
        _validate_identifier(table, "table name")
        _validate_identifier(time_column, "column name")
        hours = int(hours)  # enforce integer — no string injection

        if self.db_type == "postgresql":
            return f"DELETE FROM {table} WHERE {time_column} >= NOW() - INTERVAL '{hours} hours'"
        elif self.db_type == "mssql":
            return (
                f"DELETE FROM {table}"
                f" WHERE {time_column} >= DATEADD(hour, -{hours}, GETUTCDATE())"
            )
        elif self.db_type == "oracle":
            return (
                f"DELETE FROM {table}"
                f" WHERE {time_column} >= SYSTIMESTAMP - INTERVAL '{hours}' HOUR"
            )
        elif self.db_type == "mysql":
            return (
                f"DELETE FROM {table}"
                f" WHERE {time_column} >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL {hours} HOUR)"
            )
        else:
            raise ValueError(f"delete_window_sql not supported for {self.db_type}")

    def lookback_predicate(self, time_column: str, hours: int) -> str:
        """
        Dialect-specific WHERE predicate for the lookback window.

        Used in INSERT ... SELECT statements to limit aggregation scope.
        """
        _validate_identifier(time_column, "column name")
        hours = int(hours)

        if self.db_type == "postgresql":
            return f"{time_column} >= NOW() - INTERVAL '{hours} hours'"
        elif self.db_type == "mssql":
            return f"{time_column} >= DATEADD(hour, -{hours}, GETUTCDATE())"
        elif self.db_type == "oracle":
            return f"{time_column} >= SYSTIMESTAMP - INTERVAL '{hours}' HOUR"
        elif self.db_type == "mysql":
            return f"{time_column} >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL {hours} HOUR)"
        else:
            raise ValueError(f"lookback_predicate not supported for {self.db_type}")

    def audit_trigger_sql(
        self,
        table: str,
        audit_table: str,
        id_columns: list[str],
    ) -> list[str]:
        """
        Dialect-specific SQL statements to create an audit trigger.

        Returns a list of SQL statements that create a trigger function
        and attach it to the table. The trigger captures INSERT, UPDATE,
        and DELETE operations, storing old/new values and the current
        application user.

        Args:
            table: Source table name (e.g., "signal").
            audit_table: Audit table name (e.g., "signal_audit").
            id_columns: PK columns to copy into the audit record
                        (e.g., ["signal_id"] or ["approach_id", "signal_id"]).

        Returns:
            List of SQL strings to execute in order.
        """
        _validate_identifier(table, "table name")
        _validate_identifier(audit_table, "audit table name")
        for col in id_columns:
            _validate_identifier(col, "id column")

        id_col_list = ", ".join(id_columns)
        old_id_list = ", ".join(f"OLD.{c}" for c in id_columns)
        new_id_list = ", ".join(f"NEW.{c}" for c in id_columns)

        if self.db_type == "postgresql":
            return self._pg_audit_trigger(
                table, audit_table, id_col_list, old_id_list, new_id_list
            )
        elif self.db_type == "mssql":
            return self._mssql_audit_trigger(
                table, audit_table, id_columns
            )
        elif self.db_type == "oracle":
            return self._oracle_audit_trigger(
                table, audit_table, id_col_list, old_id_list, new_id_list
            )
        elif self.db_type == "mysql":
            return self._mysql_audit_trigger(
                table, audit_table, id_col_list, old_id_list, new_id_list
            )
        else:
            raise ValueError(
                f"audit_trigger_sql not supported for {self.db_type}"
            )

    @staticmethod
    def _pg_audit_trigger(
        table, audit_table, id_col_list, old_id_list, new_id_list
    ) -> list[str]:
        func_sql = f"""
CREATE OR REPLACE FUNCTION audit_{table}_changes()
RETURNS TRIGGER AS $$
DECLARE
    v_user TEXT;
BEGIN
    v_user := current_setting('app.current_user', true);
    IF (TG_OP = 'DELETE') THEN
        INSERT INTO {audit_table} ({id_col_list}, changed_by, operation, old_values)
        VALUES ({old_id_list}, v_user, 'DELETE', to_jsonb(OLD));
        RETURN OLD;
    ELSIF (TG_OP = 'UPDATE') THEN
        INSERT INTO {audit_table} ({id_col_list}, changed_by, operation, old_values, new_values)
        VALUES ({new_id_list}, v_user, 'UPDATE', to_jsonb(OLD), to_jsonb(NEW));
        RETURN NEW;
    ELSIF (TG_OP = 'INSERT') THEN
        INSERT INTO {audit_table} ({id_col_list}, changed_by, operation, new_values)
        VALUES ({new_id_list}, v_user, 'INSERT', to_jsonb(NEW));
        RETURN NEW;
    END IF;
END;
$$ LANGUAGE plpgsql;
"""
        trigger_sql = f"""
CREATE TRIGGER {table}_audit_trigger
    AFTER INSERT OR UPDATE OR DELETE ON {table}
    FOR EACH ROW EXECUTE FUNCTION audit_{table}_changes();
"""
        return [func_sql, trigger_sql]

    @staticmethod
    def _mssql_audit_trigger(table, audit_table, id_columns) -> list[str]:
        id_col_list = ", ".join(id_columns)
        # MS-SQL uses INSERTED/DELETED pseudo-tables and SESSION_CONTEXT for user
        insert_cols = ", ".join(f"i.{c}" for c in id_columns)
        delete_cols = ", ".join(f"d.{c}" for c in id_columns)
        trigger_sql = f"""
CREATE OR ALTER TRIGGER {table}_audit_trigger
ON {table}
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @user NVARCHAR(256) = CAST(SESSION_CONTEXT(N'current_user') AS NVARCHAR(256));

    -- DELETE
    INSERT INTO {audit_table} ({id_col_list}, changed_by, operation, old_values)
    SELECT {delete_cols}, @user, 'DELETE',
           (SELECT d.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)
    FROM DELETED d
    WHERE NOT EXISTS (SELECT 1 FROM INSERTED);

    -- INSERT
    INSERT INTO {audit_table} ({id_col_list}, changed_by, operation, new_values)
    SELECT {insert_cols}, @user, 'INSERT',
           (SELECT i.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)
    FROM INSERTED i
    WHERE NOT EXISTS (SELECT 1 FROM DELETED);

    -- UPDATE
    INSERT INTO {audit_table} ({id_col_list}, changed_by, operation, old_values, new_values)
    SELECT {insert_cols}, @user, 'UPDATE',
           (SELECT d.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
           (SELECT i.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)
    FROM INSERTED i
    INNER JOIN DELETED d ON {' AND '.join(f'i.{c} = d.{c}' for c in id_columns)};
END;
"""
        return [trigger_sql]

    @staticmethod
    def _oracle_audit_trigger(
        table, audit_table, id_col_list, old_id_list, new_id_list
    ) -> list[str]:
        # Oracle uses :OLD/:NEW and SYS_CONTEXT for user
        trigger_sql = f"""
CREATE OR REPLACE TRIGGER {table}_audit_trigger
AFTER INSERT OR UPDATE OR DELETE ON {table}
FOR EACH ROW
DECLARE
    v_user VARCHAR2(256) := SYS_CONTEXT('CLIENTCONTEXT', 'current_user');
BEGIN
    IF DELETING THEN
        INSERT INTO {audit_table} ({id_col_list}, changed_by, operation, old_values)
        VALUES ({old_id_list}, v_user, 'DELETE', NULL);
    ELSIF UPDATING THEN
        INSERT INTO {audit_table} ({id_col_list}, changed_by, operation, old_values, new_values)
        VALUES ({new_id_list}, v_user, 'UPDATE', NULL, NULL);
    ELSIF INSERTING THEN
        INSERT INTO {audit_table} ({id_col_list}, changed_by, operation, new_values)
        VALUES ({new_id_list}, v_user, 'INSERT', NULL);
    END IF;
END;
"""
        return [trigger_sql]

    @staticmethod
    def _mysql_audit_trigger(
        table, audit_table, id_col_list, old_id_list, new_id_list
    ) -> list[str]:
        # MySQL needs separate triggers per operation, uses @app_user session variable
        stmts = []
        # INSERT trigger
        stmts.append(f"""
CREATE TRIGGER {table}_audit_insert
AFTER INSERT ON {table}
FOR EACH ROW
INSERT INTO {audit_table} ({id_col_list}, changed_by, operation, new_values)
VALUES ({new_id_list}, @app_user, 'INSERT', JSON_OBJECT());
""")
        # UPDATE trigger
        stmts.append(f"""
CREATE TRIGGER {table}_audit_update
AFTER UPDATE ON {table}
FOR EACH ROW
INSERT INTO {audit_table} ({id_col_list}, changed_by, operation, old_values, new_values)
VALUES ({new_id_list}, @app_user, 'UPDATE', JSON_OBJECT(), JSON_OBJECT());
""")
        # DELETE trigger
        stmts.append(f"""
CREATE TRIGGER {table}_audit_delete
AFTER DELETE ON {table}
FOR EACH ROW
INSERT INTO {audit_table} ({id_col_list}, changed_by, operation, old_values)
VALUES ({old_id_list}, @app_user, 'DELETE', JSON_OBJECT());
""")
        return stmts

    def set_app_user_sql(self) -> str:
        """
        Dialect-specific SQL to set the current application user
        on the database session/transaction for audit trigger attribution.

        The returned SQL should be executed with a 'username' bind parameter.
        Uses transaction-scoped settings to prevent connection pool leaks.
        """
        if self.db_type == "postgresql":
            return "SET LOCAL app.current_user = :username"
        elif self.db_type == "mssql":
            return "EXEC sp_set_session_context N'current_user', :username"
        elif self.db_type == "oracle":
            return "BEGIN DBMS_SESSION.SET_CONTEXT('CLIENTCONTEXT', 'current_user', :username); END;"
        elif self.db_type == "mysql":
            return "SET @app_user = :username"
        else:
            raise ValueError(
                f"set_app_user_sql not supported for {self.db_type}"
            )


class DatabaseFacade:
    """
    Complete database facade.

    Routes never see connections, pooling, or dialect details.
    Everything is abstracted here.

    Dialect-specific SQL generation is delegated to DialectHelper
    (self.dialect). Proxy methods are provided for backward compatibility.
    """

    def __init__(
        self,
        db_type: Literal["postgresql", "mssql", "oracle", "mysql"],
        **config
    ):
        self.db_type = db_type
        self.config = config
        self.dialect = DialectHelper(db_type)
        self._engine: AsyncEngine | None = None
        self._session_factory: async_sessionmaker | None = None

    async def connect(self) -> None:
        """
        Connect to database and create connection pool.

        Safe to call multiple times - will skip if already connected.

        Note: Async for API consistency with disconnect(), even though
        create_async_engine() is a sync factory function.
        """
        if self._engine is not None:
            logger.warning("Database already connected, skipping reconnection")
            return

        connection_url = self._build_connection_url()

        self._engine = create_async_engine(
            connection_url,
            pool_size=self.config.get("pool_size", 20),
            max_overflow=self.config.get("max_overflow", 10),
            pool_pre_ping=True,  # Verify connections before use
            echo=self.config.get("echo_sql", False)
        )

        self._session_factory = async_sessionmaker(
            self._engine,
            class_=AsyncSession,
            expire_on_commit=False
        )

        logger.info(f"Connected to {self.db_type} database")

    async def disconnect(self) -> None:
        """
        Close all connections and dispose of pool.

        Safe to call multiple times - will skip if already disconnected.
        """
        if self._engine:
            await self._engine.dispose()
            self._engine = None
            self._session_factory = None
            logger.info("Database disconnected")
        else:
            logger.warning("Database already disconnected")

    @asynccontextmanager
    async def session(self) -> AsyncIterator[AsyncSession]:
        """
        Get a database session.

        Handles:
        - Session lifecycle
        - Transaction commit/rollback
        - Connection cleanup

        Routes use this. They never see connection details.
        """
        if not self._session_factory:
            raise RuntimeError("DatabaseFacade not initialized")

        async with self._session_factory() as session:
            try:
                yield session
                await session.commit()

            except Exception:
                await session.rollback()
                raise

    async def execute(self, stmt: Any) -> Result:
        """
        Execute a SQLAlchemy ORM/Core statement and return the result.

        Accepts ORM constructs (select, insert, update, delete) and
        text() as a last resort when no ORM equivalent exists.
        Raw strings are rejected — use text() if you must write SQL.

        Args:
            stmt: SQLAlchemy statement (select, insert, update, delete, text())

        Returns:
            SQLAlchemy Result object

        Raises:
            RuntimeError: If database not initialized
            TypeError: If a raw string is passed
            Exception: Database errors (logged and re-raised)
        """
        if isinstance(stmt, str):
            raise TypeError(
                "execute() requires a SQLAlchemy statement, not a raw string. "
                "Use text() if no ORM construct is available."
            )
        try:
            async with self.session() as session:
                result = await session.execute(stmt)
                return result
        except Exception as e:
            logger.error(f"Query execution failed: {e}", exc_info=True)
            raise

    async def get_one(self, stmt: Any) -> dict[str, Any] | None:
        """
        Execute statement and return single row as dict.

        Args:
            stmt: SQLAlchemy ORM statement (select, insert, update, delete)

        Returns:
            Dict of column: value, or None if no results
        """
        try:
            result = await self.execute(stmt)
            row = result.first()
            return dict(row._mapping) if row else None
        except Exception as e:
            logger.error(f"get_one failed: {e}", exc_info=True)
            raise

    async def get_many(self, stmt: Any) -> list[dict[str, Any]]:
        """
        Execute statement and return all rows as list of dicts.

        Args:
            stmt: SQLAlchemy ORM statement (select, insert, update, delete)

        Returns:
            List of dicts (column: value)
        """
        try:
            result = await self.execute(stmt)
            rows = result.all()
            return [dict(row._mapping) for row in rows]
        except Exception as e:
            logger.error(f"get_many failed: {e}", exc_info=True)
            raise

    async def get_dataframe(self, stmt: Any) -> pd.DataFrame:
        """
        Execute statement and return results as pandas DataFrame.

        Args:
            stmt: SQLAlchemy ORM statement (select, insert, update, delete)

        Returns:
            DataFrame with query results
        """
        try:
            result = await self.execute(stmt)
            rows = result.all()

            if not rows:
                return pd.DataFrame(columns=result.keys())

            return pd.DataFrame([dict(row._mapping) for row in rows])

        except Exception as e:
            logger.error(f"get_dataframe failed: {e}", exc_info=True)
            raise

    async def dataframe_tosql(
        self,
        df: pd.DataFrame,
        table_name: str,
        if_exists: Literal["fail", "replace", "append"] = "append"
    ) -> Result:
        """
        Write DataFrame to database table.

        Centralized DataFrame writes:
        - Async execution through execute() method
        - Connection management in one place
        - Error handling centralized
        - Efficient bulk insert

        Args:
            df: DataFrame to write
            table_name: Target table name
            if_exists: How to behave if table exists ('fail', 'replace', 'append')

        Returns:
            SQLAlchemy Result object

        Raises:
            Exception: Database errors (logged and re-raised)
        """
        try:
            if df.empty:
                logger.warning(
                    "Empty DataFrame passed to dataframe_tosql"
                    f" for table {table_name}"
                )
                return None

            # Validate table name against injection
            _validate_identifier(table_name, "table name")

            # Convert DataFrame to list of dicts
            records = df.to_dict(orient='records')

            # Build dynamic table reference with columns from DataFrame
            cols = [column(c) for c in df.columns]
            tbl = table(table_name, *cols)

            # Handle if_exists parameter
            if if_exists == "replace":
                await self.execute(tbl.delete())
            elif if_exists == "fail":
                from sqlalchemy import func, select
                result = await self.execute(select(func.count()).select_from(tbl))
                count = result.scalar()
                if count and count > 0:
                    raise ValueError(f"Table {table_name} already exists and contains data")

            # Insert records using SQLAlchemy Core
            result = await self.execute(tbl.insert().values(records))
            return result

        except Exception as e:
            logger.error(f"dataframe_tosql failed: {e}", exc_info=True)
            raise

    def _build_connection_url(self) -> URL:
        """Build dialect-specific connection URL.

        Uses SQLAlchemy URL.create() so the password is never
        embedded as a plain string in logs or tracebacks.
        """
        _DRIVERS = {
            "postgresql": "postgresql+asyncpg",
            "mssql": "mssql+aioodbc",
            "oracle": "oracle+oracledb",
            "mysql": "mysql+aiomysql",
        }
        driver = _DRIVERS.get(self.db_type)
        if driver is None:
            raise ValueError(
                f"Unsupported database type: {self.db_type}"
            )

        database = self.config.get(
            "service_name" if self.db_type == "oracle" else "database",
            "",
        )

        query: dict[str, str] = {}
        if self.db_type == "mssql" and self.config.get("driver"):
            query["driver"] = self.config["driver"]

        return URL.create(
            drivername=driver,
            username=self.config.get("user", ""),
            password=self.config.get("password", ""),
            host=self.config.get("host", "localhost"),
            port=int(self.config.get("port", 5432)),
            database=database,
            query=query,
        )

    # -- Dialect proxy methods (delegate to self.dialect) ------------------

    def time_bucket(self, timestamp_column: str, interval: str) -> str:
        """Dialect-specific time bucketing. Delegates to DialectHelper."""
        return self.dialect.time_bucket(timestamp_column, interval)

    async def has_timescaledb(self, session: AsyncSession) -> bool:
        """
        Detect whether TimescaleDB extension is installed.

        Returns True only for PostgreSQL with TimescaleDB active.
        All other databases return False.
        """
        if self.db_type != "postgresql":
            return False

        try:
            result = await session.execute(
                text("SELECT 1 FROM pg_extension WHERE extname = 'timescaledb'")
            )
            return result.scalar() is not None
        except Exception:
            return False

    def delete_window_sql(self, table: str, time_column: str, hours: int) -> str:
        """Dialect-specific DELETE for lookback window. Delegates to DialectHelper."""
        return self.dialect.delete_window_sql(table, time_column, hours)

    def lookback_predicate(self, time_column: str, hours: int) -> str:
        """Dialect-specific WHERE predicate for lookback. Delegates to DialectHelper."""
        return self.dialect.lookback_predicate(time_column, hours)

    async def refresh_materialized_views(self, session: AsyncSession) -> None:
        """
        Refresh materialized views (dialect-specific).

        Abstracts the differences:
        - PostgreSQL: REFRESH MATERIALIZED VIEW CONCURRENTLY
        - MS-SQL: Indexed views (auto-refresh, no action needed)
        - Oracle: DBMS_MVIEW.REFRESH()
        - MySQL: Not supported (use tables instead)
        """
        if self.db_type == "postgresql":
            # Refresh all materialized views concurrently
            views = ["mv_phase_performance", "mv_detector_health", "mv_coordination"]
            for view in views:
                _validate_identifier(view, "view name")
                await session.execute(
                    text(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {view}")
                )

        elif self.db_type == "mssql":
            # Indexed views auto-refresh, nothing to do
            pass

        elif self.db_type == "oracle":
            # Use Oracle's DBMS_MVIEW package
            await session.execute(
                text("BEGIN DBMS_MVIEW.REFRESH('MV_PHASE_PERFORMANCE'); END;")
            )

        elif self.db_type == "mysql":
            # MySQL doesn't support materialized views
            # Alternative: Use regular tables updated via triggers or scheduler
            pass


# Global facade instance (initialized in app.py lifespan)
db_facade: DatabaseFacade | None = None


def get_db_facade() -> DatabaseFacade:
    """Dependency injection - get database facade."""
    if db_facade is None:
        raise RuntimeError("Database facade not initialized")
    return db_facade
