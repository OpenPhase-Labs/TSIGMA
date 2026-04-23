# TSIGMA Backfill Guide

## Overview

The TSIGMA backfill script migrates historical ATSPM data from MS-SQL Server to PostgreSQL/TimescaleDB.

**Key Design Principle:** The backfill script only **inserts data**. It does NOT create tables, set up hypertables, or configure compression. This separation ensures proper initialization and prevents configuration drift.

---

## Workflow

### Step 1: Initialize Database (One-Time Setup)

Run database initialization **BEFORE** backfilling data:

```bash
# (Planned — not yet implemented. Initialize the database using Alembic migrations: `alembic upgrade head`)
# Initialize database with TimescaleDB (PostgreSQL only)
python -m tsigma.cli init-db

# Or skip TimescaleDB (use native PostgreSQL partitioning)
python -m tsigma.cli init-db --no-timescale

# Custom chunk and compression intervals (default: 7 days for both)
python -m tsigma.cli init-db --chunk-time-days 7 --compression-days 7

# Example: Weekly chunks, compress after 30 days (keep hot data longer)
python -m tsigma.cli init-db --chunk-time-days 7 --compression-days 30

# Example: Daily chunks, compress after 3 days (aggressive compression)
python -m tsigma.cli init-db --chunk-time-days 1 --compression-days 3
```

This creates:
- ✅ All tables from SQLAlchemy models
- ✅ TimescaleDB hypertables (if PostgreSQL)
- ✅ Compression policies (configurable threshold, default: 7 days)
- ✅ Indexes for query performance
- ✅ Backfill progress tracking table

**TimescaleDB Configuration:**
- `--chunk-time-days`: Hypertable chunk size in days (default: 7)
  - Smaller chunks = faster queries on recent data, more overhead
  - Larger chunks = less overhead, slower recent queries
- `--compression-days`: Compress chunks older than N days (default: 7)
  - Lower value = more storage savings, slower writes to compressed chunks
  - Higher value = faster writes, less compression

### Step 2: Run Backfill

After database is initialized, run the backfill script:

```bash
# Basic usage
python scripts/ATSPM_backfill_pgsql.py --start-date 2025-01-01 --end-date 2025-02-01

# With custom workers and batch size
python scripts/ATSPM_backfill_pgsql.py \
    --start-date 2025-01-01 \
    --end-date 2025-02-01 \
    --workers 8 \
    --batch-size 100000

# Safe resume (skip completed hours)
python scripts/ATSPM_backfill_pgsql.py \
    --start-date 2025-01-01 \
    --end-date 2025-02-01 \
    --skip-existing

# Custom source timezone (default: America/New_York)
python scripts/ATSPM_backfill_pgsql.py \
    --start-date 2025-01-01 \
    --end-date 2025-02-01 \
    --source-timezone America/Los_Angeles
```

---

## Environment Variables

Create a `.env` file with database credentials:

```bash
# MS-SQL Source (ATSPM database)
MSSQL_SERVER=atspm-db.example.com
MSSQL_PORT=1433
MSSQL_DATABASE=ATSPM
MSSQL_USER=atspm_reader
MSSQL_PASSWORD=your_password

# PostgreSQL Target (TSIGMA database)
PG_HOST=tsigma-db.example.com
PG_PORT=5432
PG_DATABASE=tsigma
PG_USER=tsigma
PG_PASSWORD=your_password
```

---

## What Changed

### Before (Old Script)

❌ Backfill script created tables
❌ Backfill script set up hypertables
❌ Backfill script configured compression
❌ Configuration mixed with data migration

**Problem:** Table setup embedded in backfill script meant DOTs couldn't customize database schema without modifying the backfill script.

### After (New Workflow)

✅ **Separation of concerns:**
- `init-db` command: Create tables, set up TimescaleDB, configure compression (ONE TIME)
- `ATSPM_backfill_pgsql.py`: Insert data only (REPEATABLE)

✅ **Benefits:**
- DOTs can run `init-db` once during TSIGMA setup
- Backfill script can be run multiple times for different date ranges
- Clear separation between schema management and data migration
- Schema is defined in SQLAlchemy models (single source of truth)

---

## For DOT System Administrators

### First-Time Setup

1. **Install TSIGMA:**
   ```bash
   cd TSIGMA
   pip install -e .
   ```

2. **Configure database:**
   ```bash
   cp .env.example .env
   # Edit .env with your database credentials
   ```

3. **Initialize database:**
   ```bash
   # (Planned — not yet implemented. Initialize the database using Alembic migrations: `alembic upgrade head`)
   python -m tsigma.cli init-db
   ```

4. **Verify initialization:**
   ```bash
   # Check that controller_event_log table exists
   psql -h localhost -U tsigma -d tsigma -c "\dt controller_event_log"
   ```

### Backfilling Historical Data

1. **Choose date range:**
   - Start with recent data (last 30 days) to verify
   - Then backfill older data in chunks

2. **Run backfill:**
   ```bash
   python scripts/ATSPM_backfill_pgsql.py \
       --start-date 2025-01-01 \
       --end-date 2025-02-01 \
       --workers 4 \
       --skip-existing
   ```

3. **Monitor progress:**
   - Check `backfill.log` for detailed progress
   - Use `--skip-existing` for safe resume on interruption

### Performance Tuning

**Backfill Performance:**
- **Workers:** Default 4, increase for faster backfill (max: CPU cores)
- **Batch size:** Default 50,000 rows, decrease if memory issues

**TimescaleDB Configuration:**
- **Chunk size:** Default 7 days (weekly chunks)
  - **Smaller chunks (1-3 days):** Faster queries on recent data, more metadata overhead
  - **Larger chunks (7-14 days):** Less overhead, better for historical queries
  - **Recommendation:** 7 days aligns with weekly reporting cycles

- **Compression threshold:** Default 7 days
  - **Aggressive (1-3 days):** Maximum storage savings, slower writes to compressed data
  - **Balanced (7-14 days):** Good compression, minimal impact on active data
  - **Conservative (30+ days):** Keep more hot data uncompressed for faster writes
  - **Recommendation:** Match your active data retention window

**Example Configurations:**

```bash
# (All init-db commands below are planned — not yet implemented.
#  Initialize the database using Alembic migrations: `alembic upgrade head`)

# High-volume agency (lots of signals, need storage savings)
python -m tsigma.cli init-db --chunk-time-days 7 --compression-days 3

# Medium-volume agency (balanced performance)
python -m tsigma.cli init-db --chunk-time-days 7 --compression-days 7

# Low-volume agency (prioritize query speed over compression)
python -m tsigma.cli init-db --chunk-time-days 7 --compression-days 30
```

---

## Troubleshooting

### Error: "Table 'controller_event_log' does not exist"

**Solution:** Run database initialization first:
```bash
# (Planned — not yet implemented. Initialize the database using Alembic migrations: `alembic upgrade head`)
python -m tsigma.cli init-db
```

### Backfill is slow

**Solutions:**
- Increase workers: `--workers 8`
- Increase batch size: `--batch-size 100000`
- Check network latency between MS-SQL and PostgreSQL servers

### Out of memory errors

**Solution:** Decrease batch size:
```bash
python scripts/ATSPM_backfill_pgsql.py ... --batch-size 25000
```

### Resume after interruption

**Solution:** Use `--skip-existing` flag:
```bash
python scripts/ATSPM_backfill_pgsql.py ... --skip-existing
```

This uses the progress tracking table to skip completed hours.

---

## Architecture Notes

### Why Separate init-db from backfill?

1. **Single Source of Truth:** Tables are defined in SQLAlchemy models, not embedded in scripts
2. **Flexibility:** DOTs can customize schema without modifying backfill script
3. **Repeatability:** Backfill script can run multiple times without schema conflicts
4. **Simplicity:** Each tool does one thing well

### TimescaleDB Automatic Features

- **Chunk creation:** Automatic (weekly chunks)
- **Compression:** Automatic after 3 days (configured in init-db)
- **Retention:** Manual (configure compression policy for long-term storage)

No manual partition management needed!
