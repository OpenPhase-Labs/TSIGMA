#!/usr/bin/env python3
"""
TSIGMA - ATSPM Backfill Script (Async Version)

Migrates historical data from ATSPM MS-SQL to PostgreSQL.
Async implementation with asyncpg and aioodbc for parallel processing.

PREREQUISITES:
    Run `python -m tsigma.cli init-db` first to create tables and configure TimescaleDB.
    This script only inserts data - it does NOT create tables or set up compression.

Usage:
    python ATSPM_backfill_pgsql.py --start-date 2025-01-23 --end-date 2025-02-14
    python ATSPM_backfill_pgsql.py --start-date 2025-01-23 --workers 4

Environment Variables:
    MSSQL_SERVER      - MS-SQL server hostname
    MSSQL_DATABASE    - MS-SQL database name
    MSSQL_USER        - MS-SQL username
    MSSQL_PASSWORD    - MS-SQL password
    PG_HOST           - PostgreSQL hostname
    PG_DATABASE       - PostgreSQL database name
    PG_USER           - PostgreSQL username
    PG_PASSWORD       - PostgreSQL password
    PG_PORT           - PostgreSQL port (default: 5432)

Developed by OpenPhase Labs
"""

import argparse
import asyncio
import logging
import os
import signal
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import AsyncIterator, Optional
from zoneinfo import ZoneInfo

import asyncpg
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('backfill.log')
    ]
)
logger = logging.getLogger(__name__)

# Global flag for graceful shutdown
shutdown_event = asyncio.Event()


@dataclass
class MSSQLConfig:
    server: str
    database: str
    user: str
    password: str
    port: int = 1433
    driver: str = 'ODBC Driver 18 for SQL Server'
    timeout: int = 300
    trust_server_certificate: bool = False

    @classmethod
    def from_env(cls) -> 'MSSQLConfig':
        return cls(
            server=os.environ.get('MSSQL_SERVER', ''),
            database=os.environ.get('MSSQL_DATABASE', ''),
            user=os.environ.get('MSSQL_USER', ''),
            password=os.environ.get('MSSQL_PASSWORD', ''),
            port=int(os.environ.get('MSSQL_PORT', '1433')),
            trust_server_certificate=os.environ.get(
                'MSSQL_TRUST_CERT', ''
            ).lower() in ('1', 'true', 'yes'),
        )

    def connection_string(self) -> str:
        trust = "yes" if self.trust_server_certificate else "no"
        return (
            f"DRIVER={{{self.driver}}};"
            f"SERVER={self.server},{self.port};"
            f"DATABASE={self.database};"
            f"UID={self.user};"
            f"PWD={self.password};"
            f"Connection Timeout={self.timeout};"
            f"TrustServerCertificate={trust};"
        )

    def __repr__(self) -> str:
        return (
            f"MSSQLConfig(server={self.server!r}, database={self.database!r}, "
            f"user={self.user!r}, password='***', port={self.port})"
        )


@dataclass
class PostgresConfig:
    host: str
    database: str
    user: str
    password: str
    port: int = 5432
    connect_timeout: int = 10

    @classmethod
    def from_env(cls) -> 'PostgresConfig':
        return cls(
            host=os.environ.get('PG_HOST', 'localhost'),
            database=os.environ.get('PG_DATABASE', 'tsigma'),
            user=os.environ.get('PG_USER', 'tsigma'),
            password=os.environ.get('PG_PASSWORD', ''),
            port=int(os.environ.get('PG_PORT', '5432')),
        )

    def __repr__(self) -> str:
        return (
            f"PostgresConfig(host={self.host!r}, database={self.database!r}, "
            f"user={self.user!r}, password='***', port={self.port})"
        )


async def get_mssql_connection(config: MSSQLConfig):
    """Create async MS-SQL connection."""
    import aioodbc
    conn = await aioodbc.connect(
        dsn=config.connection_string(),
        timeout=config.timeout,
        autocommit=False
    )
    return conn


async def get_postgres_pool(config: PostgresConfig, min_size: int = 2, max_size: int = 10):
    """Create asyncpg connection pool."""
    import asyncpg
    return await asyncpg.create_pool(
        host=config.host,
        port=config.port,
        database=config.database,
        user=config.user,
        password=config.password,
        min_size=min_size,
        max_size=max_size,
        command_timeout=60
    )


async def ensure_progress_table_exists(pg_config: PostgresConfig) -> None:
    """
    Create the backfill_progress tracking table.

    NOTE: This script does NOT create controller_event_log table.
    Run `python -m tsigma.cli init-db` first to set up tables and TimescaleDB.
    """
    conn = await asyncpg.connect(
        host=pg_config.host,
        port=pg_config.port,
        database=pg_config.database,
        user=pg_config.user,
        password=pg_config.password
    )

    try:
        # Create progress tracking table (tracks completed hours)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS backfill_progress (
                hour_start TIMESTAMPTZ PRIMARY KEY,
                row_count BIGINT NOT NULL,
                completed_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        logger.info("Progress tracking table ready")

        # Verify controller_event_log exists
        exists = await conn.fetchval("""
            SELECT EXISTS(
                SELECT 1 FROM information_schema.tables
                WHERE table_name = 'controller_event_log'
            )
        """)

        if not exists:
            raise RuntimeError(
                "Table 'controller_event_log' does not exist!\n"
                "Run `python -m tsigma.cli init-db` first to create tables."
            )

    finally:
        await conn.close()


# NOTE: Partition/chunk creation is handled automatically by TimescaleDB
# No need for manual partition management


async def stream_hour_from_mssql(
    config: MSSQLConfig,
    start_time: datetime,
    end_time: datetime,
    batch_size: int = 50000,
    source_timezone: str = 'America/New_York'
) -> AsyncIterator[list]:
    """
    Async stream events for a time range from MS-SQL.
    Yields batches of rows with timestamps converted to UTC.

    Args:
        source_timezone: IANA timezone name for the source data (e.g., 'America/New_York').
                        MS-SQL ATSPM stores timestamps in local time without timezone info.
                        This parameter specifies what timezone those timestamps represent.
    """
    query = """
        SELECT SignalID, Timestamp, EventCode, EventParam
        FROM dbo.Controller_Event_Log WITH (NOLOCK)
        WHERE Timestamp >= ? AND Timestamp < ?
    """

    # Timezone for converting naive local timestamps to UTC
    local_tz = ZoneInfo(source_timezone)
    utc_tz = ZoneInfo('UTC')

    conn = await get_mssql_connection(config)
    try:
        cursor = await conn.cursor()
        await cursor.execute(query, (start_time, end_time))

        while not shutdown_event.is_set():
            rows = await cursor.fetchmany(batch_size)
            if not rows:
                break
            # Convert timestamps: interpret as local time, convert to UTC
            converted = []
            for row in rows:
                signal_id, ts, event_code, event_param = row
                # ts is naive datetime from MS-SQL - interpret as local timezone
                if ts is not None and ts.tzinfo is None:
                    ts = ts.replace(tzinfo=local_tz).astimezone(utc_tz)
                converted.append((signal_id, ts, event_code, event_param))
            yield converted

    finally:
        await cursor.close()
        await conn.close()


async def insert_to_postgres(
    pool,
    rows: list
) -> int:
    """
    Bulk insert rows into PostgreSQL using asyncpg's copy_records_to_table.
    """
    if not rows:
        return 0

    async with pool.acquire() as conn:
        await conn.copy_records_to_table(
            'controller_event_log',
            records=rows,
            columns=['signal_id', 'timestamp', 'event_code', 'event_param']
        )

    return len(rows)


async def mark_hour_complete(pool, hour_start: datetime, row_count: int) -> None:
    """Record that an hour has been fully ingested."""
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO backfill_progress (hour_start, row_count)
            VALUES ($1, $2)
            ON CONFLICT (hour_start) DO UPDATE SET row_count = $2, completed_at = NOW()
            """,
            hour_start, row_count
        )


async def is_hour_complete(pool, hour_start: datetime) -> bool:
    """Check if an hour has been fully ingested."""
    async with pool.acquire() as conn:
        result = await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM backfill_progress WHERE hour_start = $1)",
            hour_start
        )
        return result or False


async def delete_hour_data(pool, hour_start: datetime, hour_end: datetime) -> int:
    """Delete all data for an hour (for re-processing incomplete hours)."""
    async with pool.acquire() as conn:
        result = await conn.execute(
            "DELETE FROM controller_event_log WHERE timestamp >= $1 AND timestamp < $2",
            hour_start, hour_end,
            timeout=300  # 5 minutes for large deletes on hypertables
        )
        # Returns "DELETE N" - extract the count
        return int(result.split()[-1])


async def process_hour_chunk(
    start_time: datetime,
    end_time: datetime,
    mssql_config: MSSQLConfig,
    pg_pool,
    batch_size: int = 50000,
    semaphore: Optional[asyncio.Semaphore] = None,
    source_timezone: str = 'America/New_York'
) -> dict:
    """
    Process a single hour's data asynchronously.

    Uses a progress tracking table to record completed hours.
    On restart, incomplete hours are detected and cleaned up before re-processing.
    """
    chunk_str = start_time.strftime('%Y-%m-%d %H:%M')
    result = {
        'chunk': chunk_str,
        'rows_fetched': 0,
        'rows_inserted': 0,
        'success': False,
        'error': None
    }

    # Use semaphore to limit concurrent workers
    async with semaphore if semaphore else asyncio.nullcontext():
        if shutdown_event.is_set():
            result['error'] = 'Shutdown requested'
            return result

        try:
            # TimescaleDB handles chunk creation automatically

            chunk_start = datetime.now()
            last_log_time = chunk_start
            last_log_rows = 0

            async for batch in stream_hour_from_mssql(
                mssql_config, start_time, end_time,
                batch_size, source_timezone,
            ):
                if shutdown_event.is_set():
                    result['error'] = 'Shutdown requested'
                    return result

                batch_len = len(batch)
                result['rows_fetched'] += batch_len

                if batch:
                    inserted = await insert_to_postgres(pg_pool, batch)
                    result['rows_inserted'] += inserted

                # Log progress every 1 million rows
                now = datetime.now()
                if result['rows_fetched'] // 1_000_000 > last_log_rows // 1_000_000:
                    elapsed_since_log = (now - last_log_time).total_seconds()
                    rows_since_log = result['rows_fetched'] - last_log_rows
                    rate = rows_since_log / elapsed_since_log if elapsed_since_log > 0 else 0
                    elapsed_total = (now - chunk_start).total_seconds()

                    logger.info(
                        f"{chunk_str}: {result['rows_fetched']:,} rows | "
                        f"{rate:,.0f} rows/sec | "
                        f"elapsed {elapsed_total:.0f}s"
                    )
                    last_log_time = now
                    last_log_rows = result['rows_fetched']

            # Mark hour as complete ONLY after all batches succeed
            await mark_hour_complete(pg_pool, start_time, result['rows_inserted'])

            total_elapsed = (datetime.now() - chunk_start).total_seconds()
            avg_rate = result['rows_fetched'] / total_elapsed if total_elapsed > 0 else 0

            result['success'] = True
            if result['rows_fetched'] > 0:
                logger.info(
                    f"{chunk_str}: DONE - {result['rows_fetched']:,} rows | "
                    f"{total_elapsed:.1f}s | "
                    f"{avg_rate:,.0f} rows/sec"
                )

        except Exception as e:
            result['error'] = str(e)
            logger.error(f"{chunk_str}: ERROR after {result['rows_fetched']:,} rows - {e}")

    return result


def generate_hourly_chunks(start_date: datetime, end_date: datetime) -> list:
    """Generate hourly time chunks for parallel processing."""
    chunks = []
    current = start_date
    while current < end_date:
        chunk_end = min(current + timedelta(hours=1), end_date)
        chunks.append((current, chunk_end))
        current = chunk_end
    return chunks


async def get_table_size(pg_pool) -> str:
    """Get the current size of the controller_event_log table."""
    try:
        async with pg_pool.acquire() as conn:
            result = await conn.fetchval("""
                SELECT pg_size_pretty(pg_total_relation_size('controller_event_log'))
            """)
            return result if result else "unknown"
    except Exception as e:
        logger.warning(f"Could not get table size: {e}")
        return "unknown"


async def check_hour_has_data(pg_pool, start_time: datetime, end_time: datetime) -> bool:
    """Check if an hour already has data in PostgreSQL."""
    try:
        async with pg_pool.acquire() as conn:
            result = await conn.fetchval(
                "SELECT EXISTS(SELECT 1 FROM controller_event_log"
                " WHERE timestamp >= $1 AND timestamp < $2 LIMIT 1)",
                start_time, end_time
            )
            return result or False
    except Exception as e:
        logger.warning(f"Could not check hour {start_time}: {e}")
        return False


async def get_row_count_estimate(pg_pool) -> int:
    """Get estimated row count from PostgreSQL."""
    try:
        async with pg_pool.acquire() as conn:
            result = await conn.fetchval("""
                SELECT reltuples::bigint
                FROM pg_class
                WHERE relname = 'controller_event_log'
            """)
            return result or 0
    except Exception as e:
        logger.warning(f"Could not get row count: {e}")
        return 0


async def run_backfill(
    start_date: datetime,
    end_date: datetime,
    mssql_config: MSSQLConfig,
    pg_config: PostgresConfig,
    workers: int = 4,
    skip_existing: bool = False,
    batch_size: int = 50000,
    source_timezone: str = 'America/New_York'
) -> dict:
    """
    Run the backfill process using async tasks with controlled concurrency.

    Args:
        skip_existing: If True, skip hours that already have data (safe resume).
        source_timezone: IANA timezone for source MS-SQL data.
    """
    # Ensure progress tracking table exists (and verify controller_event_log exists)
    await ensure_progress_table_exists(pg_config)

    # Create connection pool for PostgreSQL
    pg_pool = await get_postgres_pool(pg_config, min_size=workers, max_size=workers * 2)

    try:
        # Generate hourly chunks
        all_chunks = generate_hourly_chunks(start_date, end_date)
        total_days = (end_date - start_date).days or 1

        # Filter out completed chunks and clean up incomplete ones
        if skip_existing:
            logger.info("Checking progress tracking table...")

            chunks = []
            skipped = 0
            cleaned = 0

            for start, end in all_chunks:
                # Check progress table (authoritative source)
                is_complete = await is_hour_complete(pg_pool, start)

                if is_complete:
                    skipped += 1
                else:
                    # Check if there's partial data (hour started but not completed)
                    has_data = await check_hour_has_data(pg_pool, start, end)
                    if has_data:
                        # Clean up partial data before re-processing
                        deleted = await delete_hour_data(pg_pool, start, end)
                        logger.warning(
                            f"Cleaned up {deleted:,} partial rows"
                            f" for {start.strftime('%Y-%m-%d %H:%M')}"
                        )
                        cleaned += 1

                    chunks.append((start, end))

            if skipped > 0:
                logger.info(f"Skipping {skipped} completed hours")
            if cleaned > 0:
                logger.warning(f"Cleaned up {cleaned} incomplete hours (will re-process)")
        else:
            chunks = all_chunks

        total_chunks = len(chunks)
        if total_chunks == 0:
            logger.info("No chunks to process - all data already exists")
            return {
                'total_chunks': 0,
                'total_days': total_days,
                'successful_chunks': 0,
                'failed_chunks': 0,
                'total_rows_fetched': 0,
                'total_rows_inserted': 0,
                'errors': []
            }

        logger.info(
            f"Processing {total_chunks} hourly chunks"
            f" ({total_days} days) with {workers} concurrent tasks"
        )

        # Statistics
        stats = {
            'total_chunks': total_chunks,
            'total_days': total_days,
            'successful_chunks': 0,
            'failed_chunks': 0,
            'total_rows_fetched': 0,
            'total_rows_inserted': 0,
            'errors': []
        }

        backfill_start = datetime.now()

        # Semaphore to limit concurrent workers
        semaphore = asyncio.Semaphore(workers)

        # Create all tasks
        tasks = [
            asyncio.create_task(
                process_hour_chunk(
                    start, end, mssql_config, pg_pool,
                    batch_size, semaphore, source_timezone,
                )
            )
            for start, end in chunks
        ]

        # Process results as they complete
        completed = 0
        for coro in asyncio.as_completed(tasks):
            if shutdown_event.is_set():
                logger.info("Shutdown requested, cancelling remaining tasks...")
                for task in tasks:
                    task.cancel()
                break

            try:
                result = await coro
                completed += 1

                if result['success']:
                    stats['successful_chunks'] += 1
                    stats['total_rows_fetched'] += result['rows_fetched']
                    stats['total_rows_inserted'] += result['rows_inserted']
                else:
                    stats['failed_chunks'] += 1
                    if result['error'] and result['error'] != 'Shutdown requested':
                        stats['errors'].append({'chunk': result['chunk'], 'error': result['error']})

                # Progress update every 24 completions
                if completed % 24 == 0:
                    elapsed = (datetime.now() - backfill_start).total_seconds()
                    rate = stats['total_rows_fetched'] / elapsed if elapsed > 0 else 0
                    pct = completed / total_chunks * 100
                    table_size = await get_table_size(pg_pool)
                    logger.info(
                        f"Progress: {completed}/{total_chunks} chunks ({pct:.1f}%) | "
                        f"{stats['total_rows_fetched']:,} rows | "
                        f"{rate:,.0f} rows/sec | "
                        f"size: {table_size}"
                    )

            except asyncio.CancelledError:
                pass

        return stats

    finally:
        await pg_pool.close()


def setup_signal_handlers():
    """Set up signal handlers for graceful shutdown."""
    def handle_shutdown(signum, frame):
        signame = signal.Signals(signum).name
        logger.info(f"Received {signame}, initiating graceful shutdown...")
        shutdown_event.set()

    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)


async def async_main(args):
    """Async main entry point."""
    # Parse dates
    try:
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
        end_date = datetime.strptime(args.end_date, '%Y-%m-%d')
    except ValueError as e:
        logger.error(f"Invalid date format: {e}")
        return 1

    # Load configs from environment
    mssql_config = MSSQLConfig.from_env()
    pg_config = PostgresConfig.from_env()

    # Validate configs
    if not mssql_config.server or not mssql_config.database:
        logger.error(
            "MS-SQL configuration missing."
            " Set MSSQL_SERVER, MSSQL_DATABASE, MSSQL_USER, MSSQL_PASSWORD"
        )
        return 1

    if not pg_config.host or not pg_config.database:
        logger.error(
            "PostgreSQL configuration missing."
            " Set PG_HOST, PG_DATABASE, PG_USER, PG_PASSWORD"
        )
        return 1

    logger.info("=" * 60)
    logger.info("TSIGMA - ATSPM Backfill (Async)")
    logger.info("=" * 60)
    logger.info("Source: MS-SQL (see MSSQL_SERVER env var)")
    logger.info("Target: PostgreSQL (see PG_HOST env var)")
    logger.info(f"Date range: {args.start_date} to {args.end_date}")
    logger.info(f"Workers: {args.workers}")
    logger.info(f"Batch size: {args.batch_size:,}")
    logger.info(f"Skip existing: {args.skip_existing}")
    logger.info(f"Source timezone: {args.source_timezone}")
    logger.info("=" * 60)
    logger.info("NOTE: Tables must be created first with: python -m tsigma.cli init-db")
    logger.info("=" * 60)

    if args.skip_existing:
        logger.info("Using progress tracking table for resume support")

    # Run backfill
    start_time = datetime.now()
    stats = await run_backfill(
        start_date=start_date,
        end_date=end_date,
        mssql_config=mssql_config,
        pg_config=pg_config,
        workers=args.workers,
        skip_existing=args.skip_existing,
        batch_size=args.batch_size,
        source_timezone=args.source_timezone
    )
    elapsed = datetime.now() - start_time

    # Print summary
    logger.info("=" * 60)
    logger.info("BACKFILL COMPLETE" if not shutdown_event.is_set() else "BACKFILL INTERRUPTED")
    logger.info("=" * 60)
    logger.info(f"Total chunks: {stats['total_chunks']} ({stats['total_days']} days)")
    logger.info(f"Successful: {stats['successful_chunks']}")
    logger.info(f"Failed: {stats['failed_chunks']}")
    logger.info(f"Total rows fetched: {stats['total_rows_fetched']:,}")
    logger.info(f"Total rows inserted: {stats['total_rows_inserted']:,}")
    logger.info(f"Elapsed time: {elapsed}")

    if stats['errors']:
        logger.warning(f"Errors encountered on {len(stats['errors'])} chunks:")
        for err in stats['errors'][:10]:
            logger.warning(f"  {err['chunk']}: {err['error']}")
        if len(stats['errors']) > 10:
            logger.warning(f"  ... and {len(stats['errors']) - 10} more")

    return 0 if stats['failed_chunks'] == 0 and not shutdown_event.is_set() else 1


def main():
    parser = argparse.ArgumentParser(
        description='TSIGMA - Backfill ATSPM data from MS-SQL to PostgreSQL (Async)'
    )
    parser.add_argument(
        '--start-date',
        type=str,
        required=True,
        help='Start date (YYYY-MM-DD)'
    )
    parser.add_argument(
        '--end-date',
        type=str,
        default=datetime.now().strftime('%Y-%m-%d'),
        help='End date (YYYY-MM-DD), default: today'
    )
    parser.add_argument(
        '--workers',
        type=int,
        default=4,
        help='Number of concurrent tasks (default: 4)'
    )
    parser.add_argument(
        '--skip-existing',
        action='store_true',
        help='Skip hours that already have data (safe resume). '
             'Uses progress tracking table to determine completed hours.'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=50000,
        help='Rows per batch (default: 50000). Lower if running out of memory.'
    )
    parser.add_argument(
        '--source-timezone',
        type=str,
        default='America/New_York',
        help='IANA timezone of source data (default: America/New_York). '
             'ATSPM stores local time without timezone - this specifies what timezone that is.'
    )

    args = parser.parse_args()

    # Set up signal handlers
    setup_signal_handlers()

    # Run async main
    exit_code = asyncio.run(async_main(args))
    sys.exit(exit_code)


if __name__ == '__main__':
    main()
