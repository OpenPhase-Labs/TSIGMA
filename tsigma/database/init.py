"""
Database Initialization

Handles first-run setup:
- Table creation from models
- TimescaleDB hypertable configuration
- Compression policies
- Indexes

Run once on application startup or via CLI command.
"""

import logging

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from tsigma.config import settings
from tsigma.database.db import DatabaseFacade
from tsigma.models.base import Base

logger = logging.getLogger(__name__)


async def initialize_database(
    db_facade: DatabaseFacade,
    enable_timescale: bool = True,
    chunk_time_interval_days: int | None = None,
    compression_after_days: int = 7,
) -> None:
    """
    Initialize database on first run.

    Creates:
    - All tables from SQLAlchemy models
    - TimescaleDB hypertables (if PostgreSQL)
    - Compression policies
    - Indexes

    Args:
        enable_timescale: Enable TimescaleDB hypertables (PostgreSQL only).
        chunk_time_interval_days: Hypertable chunk size in days. When None
            (default), reads from ``settings.event_log_partition_interval_days``
            (default: 1) so the TimescaleDB baseline matches the partition
            interval used by MS-SQL / Oracle / MySQL.
        compression_after_days: Compress chunks older than N days (default: 7).
    """
    if chunk_time_interval_days is None:
        chunk_time_interval_days = settings.event_log_partition_interval_days

    async with db_facade.session() as session:
        # Create all tables from models
        async with session.begin():
            await session.run_sync(Base.metadata.create_all)

        # PostgreSQL-specific: TimescaleDB setup
        if db_facade.db_type == "postgresql" and enable_timescale:
            await _setup_timescale(
                session,
                chunk_time_interval_days,
                compression_after_days,
            )

        # Create indexes for query performance
        await _create_indexes(session, db_facade.db_type)


async def _setup_timescale(
    session: AsyncSession,
    chunk_time_interval_days: int,
    compression_after_days: int
) -> None:
    """
    Configure TimescaleDB for controller_event_log.

    Creates:
    - Hypertable with configurable chunk size
    - Compression policy with configurable threshold

    Args:
        chunk_time_interval_days: Chunk size in days (default: 7)
        compression_after_days: Compress chunks older than N days (default: 7)
    """
    # Enforce integer types — prevent injection via string values
    chunk_time_interval_days = int(chunk_time_interval_days)
    compression_after_days = int(compression_after_days)

    # Enable TimescaleDB extension
    await session.execute(text("CREATE EXTENSION IF NOT EXISTS timescaledb"))

    # Convert controller_event_log to hypertable
    try:
        await session.execute(
            text("""
                SELECT create_hypertable(
                    'controller_event_log',
                    'event_time',
                    chunk_time_interval => INTERVAL :chunk_interval,
                    if_not_exists => TRUE,
                    migrate_data => TRUE
                )
            """),
            {"chunk_interval": f"{chunk_time_interval_days} days"},
        )
        logger.info(
            "Created TimescaleDB hypertable: controller_event_log (chunk: %dd)",
            chunk_time_interval_days,
        )
    except Exception as e:
        if "already a hypertable" in str(e).lower():
            logger.info("Hypertable already exists: controller_event_log")
        else:
            raise

    # Enable compression for chunks older than specified days
    try:
        await session.execute(text("""
            ALTER TABLE controller_event_log SET (
                timescaledb.compress,
                timescaledb.compress_segmentby = 'signal_id',
                timescaledb.compress_orderby = 'event_time DESC'
            )
        """))
        await session.execute(
            text("""
                SELECT add_compression_policy(
                    'controller_event_log',
                    INTERVAL :compress_interval,
                    if_not_exists => TRUE
                )
            """),
            {"compress_interval": f"{compression_after_days} days"},
        )
        logger.info("Compression policy enabled (%dd threshold)", compression_after_days)
    except Exception as e:
        if "already" in str(e).lower():
            logger.info("Compression policy already configured")
        else:
            logger.warning("Could not configure compression: %s", e)


async def _create_indexes(
    session: AsyncSession,
    db_type: str
) -> None:
    """
    Create indexes for query performance.

    Database-neutral strategy:
    - PRIMARY: signal_id + event_time (single-signal queries)
    - SECONDARY: event_code + event_time (cross-signal queries)
    """
    # Index 1: Signal + Time (covers PCD, split monitor, etc.)
    await session.execute(text("""
        CREATE INDEX IF NOT EXISTS idx_cel_signal_event_time
        ON controller_event_log (signal_id, event_time DESC)
    """))

    # Index 2: Event code + Time (covers flash status, daily reports)
    await session.execute(text("""
        CREATE INDEX IF NOT EXISTS idx_cel_event_code_time
        ON controller_event_log (event_code, event_time DESC)
    """))

    logger.info("Indexes created")


async def create_backfill_progress_table(session: AsyncSession) -> None:
    """
    Create backfill progress tracking table.

    Used by backfill script to track completed hours.
    Separate from main schema, only needed for backfills.
    """
    await session.execute(text("""
        CREATE TABLE IF NOT EXISTS backfill_progress (
            hour_start TIMESTAMPTZ PRIMARY KEY,
            row_count BIGINT NOT NULL,
            completed_at TIMESTAMPTZ DEFAULT NOW()
        )
    """))
    logger.info("Backfill progress tracking table created")
