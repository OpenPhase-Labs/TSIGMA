"""
Unit tests for database initialization.

Tests table creation, TimescaleDB setup, index creation,
and backfill progress table using mocked sessions.
"""

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tsigma.database.init import (
    _create_indexes,
    _setup_timescale,
    create_backfill_progress_table,
    initialize_database,
)


def _make_facade_with_mock_session(db_type: str = "postgresql"):
    """
    Build a mock DatabaseFacade whose session() yields an AsyncMock session.

    Returns:
        Tuple of (mock_facade, mock_session).
    """
    mock_session = AsyncMock()

    # session.begin() is used as `async with session.begin():` so it must
    # return an async context manager directly (not a coroutine).
    # Override begin as a plain MagicMock so calling it returns immediately.
    mock_begin_ctx = MagicMock()
    mock_begin_ctx.__aenter__ = AsyncMock(return_value=None)
    mock_begin_ctx.__aexit__ = AsyncMock(return_value=False)
    mock_session.begin = MagicMock(return_value=mock_begin_ctx)

    # facade.session() is an @asynccontextmanager — build a real one
    @asynccontextmanager
    async def fake_session():
        yield mock_session

    mock_facade = MagicMock()
    mock_facade.db_type = db_type
    mock_facade.session = fake_session

    return mock_facade, mock_session


class TestInitializeDatabase:
    """Tests for initialize_database()."""

    @pytest.mark.asyncio
    async def test_creates_all_tables(self):
        """Test initialize_database calls Base.metadata.create_all."""
        mock_facade, mock_session = _make_facade_with_mock_session("postgresql")

        with patch("tsigma.database.init._setup_timescale", new_callable=AsyncMock), \
             patch("tsigma.database.init._create_indexes", new_callable=AsyncMock):
            await initialize_database(mock_facade)

        mock_session.run_sync.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_calls_timescale_setup_for_postgresql(self):
        """Test TimescaleDB setup is called for PostgreSQL.

        Default chunk interval reads from
        ``settings.event_log_partition_interval_days`` (default: 1 day).
        """
        mock_facade, mock_session = _make_facade_with_mock_session("postgresql")

        with patch("tsigma.database.init._setup_timescale", new_callable=AsyncMock) as mock_ts, \
             patch("tsigma.database.init._create_indexes", new_callable=AsyncMock):
            await initialize_database(mock_facade, enable_timescale=True)

        mock_ts.assert_awaited_once_with(mock_session, 1, 7)

    @pytest.mark.asyncio
    async def test_skips_timescale_for_mssql(self):
        """Test TimescaleDB setup is skipped for non-PostgreSQL."""
        mock_facade, _ = _make_facade_with_mock_session("mssql")

        with patch("tsigma.database.init._setup_timescale", new_callable=AsyncMock) as mock_ts, \
             patch("tsigma.database.init._create_indexes", new_callable=AsyncMock):
            await initialize_database(mock_facade, enable_timescale=True)

        mock_ts.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_skips_timescale_when_disabled(self):
        """Test TimescaleDB setup is skipped when enable_timescale=False."""
        mock_facade, _ = _make_facade_with_mock_session("postgresql")

        with patch("tsigma.database.init._setup_timescale", new_callable=AsyncMock) as mock_ts, \
             patch("tsigma.database.init._create_indexes", new_callable=AsyncMock):
            await initialize_database(mock_facade, enable_timescale=False)

        mock_ts.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_custom_chunk_and_compression_intervals(self):
        """Test custom chunk_time_interval and compression_after are passed."""
        mock_facade, mock_session = _make_facade_with_mock_session("postgresql")

        with patch("tsigma.database.init._setup_timescale", new_callable=AsyncMock) as mock_ts, \
             patch("tsigma.database.init._create_indexes", new_callable=AsyncMock):
            await initialize_database(
                mock_facade,
                chunk_time_interval_days=14,
                compression_after_days=30,
            )

        mock_ts.assert_awaited_once_with(mock_session, 14, 30)

    @pytest.mark.asyncio
    async def test_always_creates_indexes(self):
        """Test indexes are created regardless of database type."""
        mock_facade, mock_session = _make_facade_with_mock_session("mssql")

        with patch("tsigma.database.init._setup_timescale", new_callable=AsyncMock), \
             patch("tsigma.database.init._create_indexes", new_callable=AsyncMock) as mock_idx:
            await initialize_database(mock_facade)

        mock_idx.assert_awaited_once_with(mock_session, "mssql")


class TestSetupTimescale:
    """Tests for _setup_timescale()."""

    @pytest.mark.asyncio
    async def test_creates_extension(self):
        """Test TimescaleDB extension is created."""
        mock_session = AsyncMock()

        await _setup_timescale(mock_session, 7, 7)

        # First call should be CREATE EXTENSION
        first_call = mock_session.execute.call_args_list[0]
        sql = str(first_call[0][0].text)
        assert "CREATE EXTENSION IF NOT EXISTS timescaledb" in sql

    @pytest.mark.asyncio
    async def test_creates_hypertable(self):
        """Test hypertable is created with correct chunk interval."""
        mock_session = AsyncMock()

        await _setup_timescale(mock_session, 14, 7)

        # Second call should be create_hypertable with bind param
        second_call = mock_session.execute.call_args_list[1]
        sql = str(second_call[0][0].text)
        assert "create_hypertable" in sql
        assert "controller_event_log" in sql
        params = second_call[0][1]
        assert params["chunk_interval"] == "14 days"

    @pytest.mark.asyncio
    async def test_configures_compression(self):
        """Test compression policy is set up."""
        mock_session = AsyncMock()

        await _setup_timescale(mock_session, 7, 30)

        # Check compression calls exist
        all_sql = [str(c[0][0].text) for c in mock_session.execute.call_args_list]
        assert any("timescaledb.compress" in sql for sql in all_sql)
        assert any("add_compression_policy" in sql for sql in all_sql)
        # Compression interval is now a bind parameter
        all_params = [c[0][1] for c in mock_session.execute.call_args_list if len(c[0]) > 1]
        assert any(p.get("compress_interval") == "30 days" for p in all_params)

    @pytest.mark.asyncio
    async def test_hypertable_already_exists_is_handled(self):
        """Test existing hypertable doesn't raise."""
        mock_session = AsyncMock()
        # First call (CREATE EXTENSION) succeeds, second (create_hypertable) fails
        mock_session.execute.side_effect = [
            None,  # CREATE EXTENSION
            Exception("already a hypertable"),  # create_hypertable
            None,  # ALTER TABLE compression
            None,  # add_compression_policy
        ]

        # Should not raise
        await _setup_timescale(mock_session, 7, 7)

    @pytest.mark.asyncio
    async def test_hypertable_non_already_error_reraises(self):
        """Test non-'already' hypertable error is re-raised (line 106)."""
        mock_session = AsyncMock()
        mock_session.execute.side_effect = [
            None,  # CREATE EXTENSION
            Exception("permission denied for table controller_event_log"),
        ]

        with pytest.raises(Exception, match="permission denied"):
            await _setup_timescale(mock_session, 7, 7)

    @pytest.mark.asyncio
    async def test_compression_already_configured(self):
        """Test compression 'already' error is logged as info (line 129-130)."""
        mock_session = AsyncMock()
        mock_session.execute.side_effect = [
            None,  # CREATE EXTENSION
            None,  # create_hypertable
            None,  # ALTER TABLE compression
            Exception("policy already exists for this table"),  # add_compression_policy
        ]

        # Should not raise
        await _setup_timescale(mock_session, 7, 7)

    @pytest.mark.asyncio
    async def test_compression_other_error_warns(self):
        """Test non-'already' compression error is logged as warning (lines 131-132)."""
        mock_session = AsyncMock()
        mock_session.execute.side_effect = [
            None,  # CREATE EXTENSION
            None,  # create_hypertable
            Exception("connection lost during compression setup"),
        ]

        # Should not raise — warning is logged instead
        await _setup_timescale(mock_session, 7, 7)


class TestCreateIndexes:
    """Tests for _create_indexes()."""

    @pytest.mark.asyncio
    async def test_creates_signal_event_time_index(self):
        """Test signal+event_time composite index is created."""
        mock_session = AsyncMock()

        await _create_indexes(mock_session, "postgresql")

        all_sql = [str(c[0][0].text) for c in mock_session.execute.call_args_list]
        assert any("idx_cel_signal_event_time" in sql for sql in all_sql)
        assert any("signal_id" in sql and "event_time" in sql for sql in all_sql)

    @pytest.mark.asyncio
    async def test_creates_event_timestamp_index(self):
        """Test event_code+timestamp composite index is created."""
        mock_session = AsyncMock()

        await _create_indexes(mock_session, "postgresql")

        all_sql = [str(c[0][0].text) for c in mock_session.execute.call_args_list]
        assert any("idx_cel_event_code_time" in sql for sql in all_sql)
        assert any("event_code" in sql for sql in all_sql)

    @pytest.mark.asyncio
    async def test_uses_if_not_exists(self):
        """Test indexes use IF NOT EXISTS for idempotency."""
        mock_session = AsyncMock()

        await _create_indexes(mock_session, "postgresql")

        all_sql = [str(c[0][0].text) for c in mock_session.execute.call_args_list]
        for sql in all_sql:
            assert "IF NOT EXISTS" in sql


class TestCreateBackfillProgressTable:
    """Tests for create_backfill_progress_table()."""

    @pytest.mark.asyncio
    async def test_creates_table(self):
        """Test backfill_progress table is created."""
        mock_session = AsyncMock()

        await create_backfill_progress_table(mock_session)

        call_sql = str(mock_session.execute.call_args[0][0].text)
        assert "CREATE TABLE IF NOT EXISTS backfill_progress" in call_sql

    @pytest.mark.asyncio
    async def test_has_required_columns(self):
        """Test table has hour_start, row_count, and completed_at."""
        mock_session = AsyncMock()

        await create_backfill_progress_table(mock_session)

        call_sql = str(mock_session.execute.call_args[0][0].text)
        assert "hour_start" in call_sql
        assert "row_count" in call_sql
        assert "completed_at" in call_sql

    @pytest.mark.asyncio
    async def test_idempotent(self):
        """Test uses IF NOT EXISTS for safe re-runs."""
        mock_session = AsyncMock()

        await create_backfill_progress_table(mock_session)

        call_sql = str(mock_session.execute.call_args[0][0].text)
        assert "IF NOT EXISTS" in call_sql
