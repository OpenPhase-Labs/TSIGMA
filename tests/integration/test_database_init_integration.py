"""
Integration tests for database initialization against a real PostgreSQL database.

Tests that tables, indexes, and TimescaleDB hypertables are actually created
in a live database — not just that the SQL strings look right.

Requirements:
    - Running PostgreSQL instance (with TimescaleDB extension for hypertable tests)
    - TSIGMA_TEST_DB_URL environment variable set, e.g.:
        export TSIGMA_TEST_DB_URL="postgresql+asyncpg://user:pass@localhost:5432/tsigma_test"

Skipped automatically when TSIGMA_TEST_DB_URL is not set.
"""

import pytest
from sqlalchemy import column, func, select, table

from tsigma.database.init import (
    _create_indexes,
    create_backfill_progress_table,
    initialize_database,
)

pytestmark = pytest.mark.integration

# ── Reusable system-catalog table references ──────────────────────────
_info_tables = table(
    "tables", column("table_name"), column("table_schema"),
    schema="information_schema",
)
_info_columns = table(
    "columns", column("column_name"), column("table_name"), column("ordinal_position"),
    schema="information_schema",
)
_pg_indexes = table("pg_indexes", column("indexname"))

# TimescaleDB catalog references
_ts_hypertables = table(
    "hypertables", column("hypertable_name"),
    schema="timescaledb_information",
)
_ts_jobs = table(
    "jobs", column("hypertable_name"), column("proc_name"),
    schema="timescaledb_information",
)


class TestInitializeDatabase:
    """Tests for initialize_database() against a real database."""

    @pytest.mark.asyncio
    async def test_creates_all_model_tables(self, db_facade):
        """Test initialize_database creates all SQLAlchemy model tables."""
        await initialize_database(db_facade, enable_timescale=False)

        # Verify core tables exist by querying information_schema
        result = await db_facade.get_many(
            select(_info_tables.c.table_name)
            .where(_info_tables.c.table_schema == "public")
            .order_by(_info_tables.c.table_name)
        )
        table_names = [r["table_name"] for r in result]

        assert "signal" in table_names
        assert "controller_event_log" in table_names
        assert "approach" in table_names
        assert "detector" in table_names
        assert "direction_type" in table_names
        assert "controller_type" in table_names

    @pytest.mark.asyncio
    async def test_idempotent_runs_twice(self, db_facade):
        """Test initialize_database can be called twice without error."""
        await initialize_database(db_facade, enable_timescale=False)
        await initialize_database(db_facade, enable_timescale=False)

        # If we get here without error, idempotency works
        result = await db_facade.get_one(
            select(func.count().label("cnt"))
            .select_from(_info_tables)
            .where(
                (_info_tables.c.table_schema == "public")
                & (_info_tables.c.table_name == "signal")
            )
        )
        assert result["cnt"] == 1


class TestCreateIndexes:
    """Tests for _create_indexes() against a real database."""

    @pytest.mark.asyncio
    async def test_creates_signal_timestamp_index(self, db_facade):
        """Test signal+timestamp index exists after creation."""
        await initialize_database(db_facade, enable_timescale=False)

        result = await db_facade.get_one(
            select(func.count().label("cnt"))
            .select_from(_pg_indexes)
            .where(_pg_indexes.c.indexname == "idx_cel_signal_timestamp")
        )
        assert result["cnt"] == 1

    @pytest.mark.asyncio
    async def test_creates_event_timestamp_index(self, db_facade):
        """Test event_code+timestamp index exists after creation."""
        await initialize_database(db_facade, enable_timescale=False)

        result = await db_facade.get_one(
            select(func.count().label("cnt"))
            .select_from(_pg_indexes)
            .where(_pg_indexes.c.indexname == "idx_cel_event_timestamp")
        )
        assert result["cnt"] == 1

    @pytest.mark.asyncio
    async def test_indexes_are_idempotent(self, db_facade):
        """Test indexes can be created multiple times without error."""
        await initialize_database(db_facade, enable_timescale=False)

        # Call _create_indexes directly a second time
        async with db_facade.session() as session:
            await _create_indexes(session, "postgresql")

        # No error means IF NOT EXISTS works
        result = await db_facade.get_one(
            select(func.count().label("cnt"))
            .select_from(_pg_indexes)
            .where(_pg_indexes.c.indexname == "idx_cel_signal_timestamp")
        )
        assert result["cnt"] == 1


class TestBackfillProgressTable:
    """Tests for create_backfill_progress_table() against a real database."""

    @pytest.mark.asyncio
    async def test_creates_table(self, db_facade):
        """Test backfill_progress table is created."""
        async with db_facade.session() as session:
            await create_backfill_progress_table(session)

        result = await db_facade.get_one(
            select(func.count().label("cnt"))
            .select_from(_info_tables)
            .where(_info_tables.c.table_name == "backfill_progress")
        )
        assert result["cnt"] == 1

    @pytest.mark.asyncio
    async def test_has_correct_columns(self, db_facade):
        """Test backfill_progress has expected columns."""
        async with db_facade.session() as session:
            await create_backfill_progress_table(session)

        result = await db_facade.get_many(
            select(_info_columns.c.column_name)
            .where(_info_columns.c.table_name == "backfill_progress")
            .order_by(_info_columns.c.ordinal_position)
        )
        columns = [r["column_name"] for r in result]
        assert "hour_start" in columns
        assert "row_count" in columns
        assert "completed_at" in columns

    @pytest.mark.asyncio
    async def test_idempotent(self, db_facade):
        """Test table creation is idempotent."""
        async with db_facade.session() as session:
            await create_backfill_progress_table(session)
            await create_backfill_progress_table(session)

        # No error means IF NOT EXISTS works


class TestTimescaleDB:
    """Tests for TimescaleDB hypertable setup.

    These tests require the TimescaleDB extension to be installed.
    They will fail if TimescaleDB is not available on the test database.
    """

    @pytest.mark.asyncio
    async def test_creates_hypertable(self, db_facade):
        """Test controller_event_log is converted to a hypertable."""
        await initialize_database(
            db_facade, enable_timescale=True, chunk_time_interval_days=7
        )

        result = await db_facade.get_one(
            select(func.count().label("cnt"))
            .select_from(_ts_hypertables)
            .where(_ts_hypertables.c.hypertable_name == "controller_event_log")
        )
        assert result["cnt"] == 1

    @pytest.mark.asyncio
    async def test_compression_policy_set(self, db_facade):
        """Test compression policy is applied to hypertable."""
        await initialize_database(
            db_facade,
            enable_timescale=True,
            chunk_time_interval_days=7,
            compression_after_days=14,
        )

        result = await db_facade.get_one(
            select(func.count().label("cnt"))
            .select_from(_ts_jobs)
            .where(
                (_ts_jobs.c.hypertable_name == "controller_event_log")
                & (_ts_jobs.c.proc_name == "policy_compression")
            )
        )
        assert result["cnt"] >= 1
