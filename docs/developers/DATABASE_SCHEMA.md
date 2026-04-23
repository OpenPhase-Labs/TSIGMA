# TSIGMA Database Schema Design

## Overview

This document defines the optimized database schema for TSIGMA, designed for PostgreSQL with TimescaleDB. The schema addresses the limitations of ATSPM 4.x (flat tables, no partitioning, inefficient types) and improves upon ATSPM 5.x (over-compressed data, complex decompression).

## Design Principles

1. **Time-series optimization** - Leverage TimescaleDB hypertables for automatic partitioning
2. **Efficient data types** - Use appropriate sizes (SMALLINT vs INT where applicable)
3. **Query-first design** - Indexes optimized for common ATSPM query patterns
4. **Separation of concerns** - Raw events, aggregations, and configuration in separate tables
5. **Backward compatibility** - Support migration from ATSPM MS-SQL
6. **International character support** - UTF-8 encoding for universal language compatibility
7. **Application-level validation** - Use TEXT for strings (no DB length checks), validate in Pydantic models for performance

---

## Validation Strategy

### Application-Level vs Database-Level

TSIGMA uses **TEXT** for all variable-length string fields and validates lengths in the application layer (Pydantic models), not the database.

**Why?**
- ✅ **Faster writes** - No length checking overhead in PostgreSQL on every INSERT/UPDATE
- ✅ **Better error messages** - Pydantic validation provides clear, structured errors
- ✅ **Same data integrity** - Validation happens before reaching database
- ✅ **Flexibility** - Change validation rules without database migrations

**Pattern:**
```python
# Application layer (Pydantic validation)
class SignalCreate(BaseModel):
    signal_id: str = Field(max_length=32)
    primary_street: str = Field(max_length=200)
    secondary_street: str | None = Field(None, max_length=200)

# Database layer (TEXT, no constraint overhead)
CREATE TABLE signal (
    signal_id           TEXT PRIMARY KEY,
    primary_street      TEXT NOT NULL,
    secondary_street    TEXT
);
```

**Database constraints are ONLY used for:**
- NOT NULL (data presence)
- UNIQUE (uniqueness)
- FOREIGN KEY (referential integrity)
- CHECK (enum validation - e.g., operation IN ('INSERT', 'UPDATE', 'DELETE'))

**Length validation happens in Pydantic**, not the database.

---

## Character Encoding

### Why Encoding Matters

ATSPM 4.x used `NVARCHAR` (UTF-16) which wastes 2 bytes per ASCII character. TSIGMA uses PostgreSQL's native `VARCHAR` with configurable encoding to balance efficiency and international support.

### Encoding Comparison

| Character | ASCII | UTF-8 | LATIN1 | NVARCHAR (UTF-16) |
|-----------|-------|-------|--------|-------------------|
| A | 1 byte | 1 byte | 1 byte | 2 bytes |
| ñ | N/A | 2 bytes | 1 byte | 2 bytes |
| ł (Polish) | N/A | 2 bytes | N/A | 2 bytes |
| 中 (Chinese) | N/A | 3 bytes | N/A | 2 bytes |

### Recommended: UTF-8 (PostgreSQL Default)

UTF-8 is the recommended encoding for all TSIGMA deployments:

```sql
-- Default PostgreSQL database creation (UTF-8)
CREATE DATABASE tsigma;

-- Explicit UTF-8 (equivalent to default)
CREATE DATABASE tsigma
    ENCODING 'UTF8'
    LC_COLLATE 'en_US.UTF-8'
    LC_CTYPE 'en_US.UTF-8';
```

**Supported character sets**:
- Western European: Spanish (ñ, á), French (é, ç), German (ü, ß), Portuguese (ã, õ)
- Central/Eastern European: Polish (ł, ż), Czech (ř, ů), Hungarian (ő, ű)
- Other: Turkish (ş, ğ), Greek (α, β), Cyrillic (а, б), Arabic, Hebrew, CJK

### Alternative: LATIN1 (ISO-8859-1)

For deployments with strict byte-size requirements or legacy system constraints:

```sql
CREATE DATABASE tsigma
    ENCODING 'LATIN1'
    LC_COLLATE 'en_US.ISO-8859-1'
    LC_CTYPE 'en_US.ISO-8859-1'
    TEMPLATE template0;
```

> **Warning**: LATIN1 only supports Western European languages. Characters from Polish, Turkish, Greek, Cyrillic, and Asian languages will cause errors. Encoding cannot be changed after database creation.

### Byte Impact on Storage

For a typical US DOT using ASCII-only signal IDs (e.g., `GA-1234`, `SIGNAL_001`):
- UTF-8 and LATIN1 are identical: 1 byte per character
- No storage penalty for choosing UTF-8

For international DOTs with accented characters (e.g., `SEÑAL_001`):
- UTF-8: 9 bytes (SE + ñ[2] + AL_001)
- LATIN1: 8 bytes (all 1-byte characters)
- Difference: 1 byte per accented character

**Conclusion**: The ~12% overhead for accented characters in UTF-8 is negligible compared to the flexibility gained. Event data (billions of rows) uses the same `signal_id` value repeatedly, and PostgreSQL's TOAST compression handles repeated strings efficiently.

---

## ATSPM Schema Analysis

### ATSPM 4.x (MS-SQL)

```sql
-- Simple but inefficient
CREATE TABLE Controller_Event_Log (
    SignalID    NVARCHAR(10) NULL,  -- Wasteful Unicode, nullable
    Timestamp   DATETIME2(7) NULL,   -- 7 decimal precision overkill
    EventCode   INT NULL,            -- INT for values 0-255
    EventParam  INT NULL             -- INT for values 0-255
);
-- No primary key, no indexes by default, no partitioning
```

**Problems:**
- No primary key or unique constraint
- NVARCHAR uses 2 bytes per character (Unicode) vs 1 byte for VARCHAR (ASCII)
- INT (4 bytes) for event codes that max at 255
- No partitioning strategy
- Table grows unbounded (~28B rows for 3 weeks across 9,000 signals at GDOT)

### ATSPM 5.x (PostgreSQL/EF Core)

```sql
-- Over-engineered compression
CREATE TABLE CompressedEvents (
    LocationIdentifier VARCHAR(10),
    DeviceId           INT,
    DataType           VARCHAR(32),
    Start              TIMESTAMP,
    End                TIMESTAMP,
    Data               BYTEA,        -- Compressed JSON blob
    PRIMARY KEY (LocationIdentifier, DeviceId, DataType, Start, End)
);
```

**Problems:**
- Requires decompression for every query
- Cannot use SQL for filtering within compressed blocks
- Complex discriminator pattern for different event types
- Makes ad-hoc analysis difficult
- Loses query parallelism benefits

---

## TSIGMA Schema Design

> **Note — Schema Qualification:** In PostgreSQL deployments, TSIGMA routes tables to
> dedicated schemas via `tsigma_schema()` (e.g., `config.signal`, `events.controller_event_log`,
> `aggregation.detector_volume_hourly`, `identity.*`). The DDLs below omit schema prefixes
> for readability. MySQL deployments use a single schema. See `tsigma/database/init.py` for
> the schema routing logic.

### Core Event Table

```sql
-- =============================================================================
-- CONTROLLER EVENT LOG - Primary time-series table
-- =============================================================================
-- Uses Indiana Traffic Signal Hi Resolution Data Logger Enumerations
-- Reference: https://docs.lib.purdue.edu/jtrpdata/4/

CREATE TABLE controller_event_log (
    -- Signal identifier (validated in Pydantic, not DB constraint)
    signal_id       TEXT NOT NULL,

    -- Event timestamp with timezone (microsecond precision sufficient)
    event_time      TIMESTAMPTZ NOT NULL,

    -- Event code: Indiana spec says 0-255, but real-world ATSPM data contains
    -- values > 32767 (SMALLINT max). Using INTEGER for compatibility.
    event_code      INTEGER NOT NULL,

    -- Event parameter (phase number, detector channel, preempt number, etc.)
    -- Also INTEGER for ATSPM compatibility - some vendors use larger values.
    event_param     INTEGER NOT NULL,

    -- Optional: Device ID for multi-controller signals
    device_id       SMALLINT DEFAULT 1,

    -- Validation results (populated by validation pipeline, NULL if not validated)
    validation_metadata JSONB,

    PRIMARY KEY (signal_id, event_time, event_code, event_param)
);

-- Convert to TimescaleDB hypertable
-- Chunk interval is configured at app initialization via TSIGMA_TIMESCALE_CHUNK_INTERVAL
--
-- Recommended intervals based on real-world testing (9,000 signals):
--   Small agency (< 500 signals):  INTERVAL '1 week'  (lower overhead)
--   Large agency (2000+):          INTERVAL '1 day'   (RECOMMENDED - faster compression)
--
-- Performance data (9,000 signals):
--   Daily chunks:  1.2B rows/chunk, faster compression, less CPU intensive
--   Weekly chunks: 8.4B rows/chunk, same compression ratio, slower + more CPU
--
-- Conclusion: Daily chunks compress 7x faster with same compression ratio

-- Executed during app initialization (tsigma/database/init.py)
-- Chunk interval from environment variable: TSIGMA_TIMESCALE_CHUNK_INTERVAL
-- Valid values: '1 day', '1 week'
-- Default: '1 day' (recommended for production based on 9K signal testing)
--
-- Python example (in database initialization):
--   chunk_interval = os.getenv('TSIGMA_TIMESCALE_CHUNK_INTERVAL', '1 day')
--   await session.execute(text(f"""
--       SELECT create_hypertable(
--           'controller_event_log',
--           'event_time',
--           chunk_time_interval => INTERVAL '{chunk_interval}',
--           if_not_exists => TRUE
--       )
--   """))
--
-- For manual creation (development/testing):
SELECT create_hypertable(
    'controller_event_log',
    'event_time',
    chunk_time_interval => INTERVAL '1 day',  -- Configurable via TSIGMA_TIMESCALE_CHUNK_INTERVAL
    if_not_exists => TRUE
);

-- =============================================================================
-- TUNING CHUNK INTERVAL (AFTER DEPLOYMENT)
-- =============================================================================
-- Chunk interval can be changed dynamically without recreating the table.
-- Existing chunks are NOT affected, but new chunks will use the new interval.
--
-- Use case: Start with daily chunks, switch to weekly if compression overhead is acceptable
--
-- Change to weekly chunks:
-- SELECT set_chunk_time_interval('controller_event_log', INTERVAL '1 week');
--
-- Change to daily chunks:
-- SELECT set_chunk_time_interval('controller_event_log', INTERVAL '1 day');
--
-- Verify current setting:
-- SELECT * FROM timescaledb_information.dimensions
-- WHERE hypertable_name = 'controller_event_log';

-- Enable compression (warm tier)
ALTER TABLE controller_event_log SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'signal_id',
    timescaledb.compress_orderby = 'event_time DESC'
);

-- Compression policy configured at app initialization
-- Environment variable: TSIGMA_STORAGE_WARM_AFTER
-- Default: '7 days'
--
-- Python example (in database initialization):
--   warm_after = os.getenv('TSIGMA_STORAGE_WARM_AFTER', '7 days')
--   await session.execute(text(f"""
--       SELECT add_compression_policy(
--           'controller_event_log',
--           INTERVAL '{warm_after}',
--           if_not_exists => TRUE
--       )
--   """))
--
-- For manual creation (development/testing):
SELECT add_compression_policy('controller_event_log', INTERVAL '7 days', if_not_exists => TRUE);

-- =============================================================================
-- INDEX STRATEGY - Database Neutral
-- =============================================================================
-- TSIGMA supports multiple databases. Index recommendations are designed to work
-- across PostgreSQL, MS-SQL, Oracle, and ClickHouse with equivalent performance.
--
-- ATSPM Query Patterns (from codebase analysis):
--   1. Signal + Time range (90%+): PCD, split monitor, approach volume
--   2. Signal + Time + Event code: Detector events, phase events
--   3. Cross-signal + Time + Event code: Flash status, system health
--
-- Storage vs Performance Trade-off:
--   - Minimal (smaller storage): idx_cel_signal_time only
--   - Standard (balanced): idx_cel_signal_time + idx_event_time
--   - Full (maximum query speed): All indexes including partial indexes
--
-- Database-specific optimizations are handled by backend adapters:
--   - PostgreSQL/TimescaleDB: Automatic chunk exclusion on time column
--   - MS-SQL: Clustered index on (signal_id, event_time) recommended
--   - Oracle: Partitioned indexes aligned with table partitions
--   - ClickHouse: ORDER BY clause in MergeTree engine (no separate index needed)

-- PRIMARY INDEX: Signal + Time (required for all deployments)
-- Covers: Single-signal queries, PCD, split monitor, approach analysis
CREATE INDEX idx_cel_signal_time
    ON controller_event_log (signal_id, event_time DESC);

-- SECONDARY INDEX: Event code + Time (recommended for cross-signal queries)
-- Covers: Flash status (173), system-wide detector health, daily reports
-- This index enables efficient filtering when querying across all signals
CREATE INDEX idx_cel_event_time
    ON controller_event_log (event_code, event_time DESC);
```

### Event Code Reference Table

```sql
-- =============================================================================
-- EVENT CODE DEFINITIONS - Indiana Hi-Res Enumerations
-- =============================================================================

CREATE TABLE event_code_definition (
    event_code      SMALLINT PRIMARY KEY,
    name            TEXT NOT NULL,
    description     TEXT,
    category        TEXT NOT NULL CHECK (category IN (
        'ActivePhase', 'ActivePedestrian', 'BarrierRing',
        'PhaseControl', 'Overlap', 'Detector', 'Preemption',
        'Coordination', 'Cabinet', 'System'
    )),
    param_type      TEXT NOT NULL CHECK (param_type IN (
        'None', 'PhaseNumber', 'DetectorChannel', 'PedDetectorChannel',
        'Barrier', 'FYA', 'Overlap', 'TSP', 'Preemption', 'Pattern'
    ))
);

-- Insert Indiana specification event codes
INSERT INTO event_code_definition (event_code, name, category, param_type, description) VALUES
-- Active Phase Events (0-20)
(0, 'Phase On', 'ActivePhase', 'PhaseNumber', 'Phase becomes active upon start of green or walk'),
(1, 'Phase Begin Green', 'ActivePhase', 'PhaseNumber', 'Solid or flashing green indication begins'),
(2, 'Phase Check', 'ActivePhase', 'PhaseNumber', 'Conflicting call registered, MAX timing begins'),
(3, 'Phase Min Complete', 'ActivePhase', 'PhaseNumber', 'Phase minimum timer expires'),
(4, 'Phase Gap Out', 'ActivePhase', 'PhaseNumber', 'Phase terminates due to gap out'),
(5, 'Phase Max Out', 'ActivePhase', 'PhaseNumber', 'Phase MAX timer expires'),
(6, 'Phase Force Off', 'ActivePhase', 'PhaseNumber', 'Coordinator force off applied'),
(7, 'Phase Green Termination', 'ActivePhase', 'PhaseNumber', 'Green ends, yellow or FYA begins'),
(8, 'Phase Begin Yellow', 'ActivePhase', 'PhaseNumber', 'Yellow indication active'),
(9, 'Phase End Yellow', 'ActivePhase', 'PhaseNumber', 'Yellow indication ends'),
(10, 'Phase Begin Red Clearance', 'ActivePhase', 'PhaseNumber', 'Red clearance timing begins'),
(11, 'Phase End Red Clearance', 'ActivePhase', 'PhaseNumber', 'Red clearance timing ends'),
(12, 'Phase Inactive', 'ActivePhase', 'PhaseNumber', 'Phase no longer active in ring'),
(13, 'Extension Timer Gap Out', 'ActivePhase', 'PhaseNumber', 'Extension timer gaps out'),
(14, 'Phase Skipped', 'ActivePhase', 'PhaseNumber', 'Phase skipped in sequence'),

-- Pedestrian Events (21-30)
(21, 'Pedestrian Begin Walk', 'ActivePedestrian', 'PhaseNumber', 'Walk indication active'),
(22, 'Pedestrian Begin Change', 'ActivePedestrian', 'PhaseNumber', 'Flashing dont walk begins'),
(23, 'Pedestrian Begin Solid DW', 'ActivePedestrian', 'PhaseNumber', 'Solid dont walk begins'),
(24, 'Pedestrian Dark', 'ActivePedestrian', 'PhaseNumber', 'Pedestrian outputs off'),

-- Barrier Events (31-40)
(31, 'Barrier Termination', 'BarrierRing', 'Barrier', 'All phases inactive, cross barrier next'),
(32, 'FYA Begin Permissive', 'BarrierRing', 'FYA', 'Flashing yellow arrow active'),
(33, 'FYA End Permissive', 'BarrierRing', 'FYA', 'FYA inactive'),

-- Phase Control Events (41-60)
(41, 'Phase Hold Active', 'PhaseControl', 'PhaseNumber', 'Phase hold applied'),
(42, 'Phase Hold Released', 'PhaseControl', 'PhaseNumber', 'Phase hold released'),
(43, 'Phase Call Registered', 'PhaseControl', 'PhaseNumber', 'Vehicle call registered'),
(44, 'Phase Call Dropped', 'PhaseControl', 'PhaseNumber', 'Call cleared'),
(45, 'Pedestrian Call Registered', 'PhaseControl', 'PhaseNumber', 'Ped call registered'),
(46, 'Phase Omit On', 'PhaseControl', 'PhaseNumber', 'Phase omit applied'),
(47, 'Phase Omit Off', 'PhaseControl', 'PhaseNumber', 'Phase omit released'),

-- Overlap Events (61-80)
(61, 'Overlap Begin Green', 'Overlap', 'Overlap', 'Overlap green begins'),
(62, 'Overlap Begin Trailing Green', 'Overlap', 'Overlap', 'Overlap extension timing'),
(63, 'Overlap Begin Yellow', 'Overlap', 'Overlap', 'Overlap yellow begins'),
(64, 'Overlap Begin Red Clearance', 'Overlap', 'Overlap', 'Overlap red clearance'),
(65, 'Overlap Off', 'Overlap', 'Overlap', 'Overlap inactive'),

-- Detector Events (81-100)
(81, 'Vehicle Detector Off', 'Detector', 'DetectorChannel', 'Detector deactivates'),
(82, 'Vehicle Detector On', 'Detector', 'DetectorChannel', 'Detector activates'),
(83, 'Detector Restored', 'Detector', 'DetectorChannel', 'Detector fault cleared'),
(84, 'Detector Fault Other', 'Detector', 'DetectorChannel', 'General detector fault'),
(85, 'Detector Fault Watchdog', 'Detector', 'DetectorChannel', 'Watchdog fault'),
(86, 'Detector Fault Open Loop', 'Detector', 'DetectorChannel', 'Open loop fault'),
(87, 'Detector Fault Shorted Loop', 'Detector', 'DetectorChannel', 'Shorted loop fault'),
(89, 'Ped Detector Off', 'Detector', 'PedDetectorChannel', 'Ped detector deactivates'),
(90, 'Ped Detector On', 'Detector', 'PedDetectorChannel', 'Ped detector activates'),

-- Preemption Events (101-130)
(101, 'Preempt Advance Warning', 'Preemption', 'Preemption', 'Advance warning input'),
(102, 'Preempt Call Input On', 'Preemption', 'Preemption', 'Preemption input activated'),
(103, 'Preempt Gate Down', 'Preemption', 'Preemption', 'Gate down input received'),
(104, 'Preempt Call Input Off', 'Preemption', 'Preemption', 'Preemption input deactivated'),
(105, 'Preempt Entry Started', 'Preemption', 'Preemption', 'Preemption transition begins'),
(106, 'Preempt Begin Track Clearance', 'Preemption', 'Preemption', 'Track clearance timing'),
(107, 'Preempt Begin Dwell', 'Preemption', 'Preemption', 'Dwell service begins'),
(111, 'Preempt Begin Exit', 'Preemption', 'Preemption', 'Exit interval timing'),
(112, 'TSP Check In', 'Preemption', 'TSP', 'Priority request received'),
(113, 'TSP Adjustment Early Green', 'Preemption', 'TSP', 'Early green adjustment'),
(114, 'TSP Adjustment Extend Green', 'Preemption', 'TSP', 'Green extension'),
(115, 'TSP Check Out', 'Preemption', 'TSP', 'Priority request ended'),

-- Coordination Events (131-170)
(131, 'Coord Pattern Change', 'Coordination', 'Pattern', 'Active pattern changed'),
(132, 'Cycle Length Change', 'Coordination', 'Pattern', 'Cycle length changed'),
(133, 'Offset Length Change', 'Coordination', 'Pattern', 'Offset changed'),
(150, 'Coord Cycle State Change', 'Coordination', 'None', 'Cycle state changed'),
(151, 'Coord Phase Yield Point', 'Coordination', 'PhaseNumber', 'Coordinated phase suspended'),
(152, 'Coord Phase Begin', 'Coordination', 'PhaseNumber', 'Coordinated phase begins'),

-- Cabinet Events (171-180)
(171, 'Test Input On', 'Cabinet', 'None', 'Test input activated'),
(172, 'Test Input Off', 'Cabinet', 'None', 'Test input deactivated'),
(173, 'Unit Flash Status Change', 'Cabinet', 'None', 'Flash status changed'),
(174, 'Unit Alarm Status Change', 'Cabinet', 'None', 'Alarm status changed');
```

### Aggregate Tables (Core Schema)

Aggregate tables are part of the core schema, defined in `tsigma/models/aggregates.py`.
They are populated by one of two mechanisms depending on the database:

- **PostgreSQL + TimescaleDB**: Continuous aggregates (automatic, incremental)
- **All other databases**: APScheduler jobs in `tsigma/scheduler/jobs/aggregate.py`
  (delete-and-reinsert sliding window every 15 minutes)

The API layer reads from these tables identically regardless of how they are populated.

### Aggregation Strategy by Database

| Database | Strategy |
|----------|----------|
| PostgreSQL + TimescaleDB | Continuous aggregates (auto-refresh) |
| MS-SQL / Oracle / MySQL | APScheduler cron jobs (delete + reinsert) |

See [MULTI_DATABASE_AGGREGATES.md](MULTI_DATABASE_AGGREGATES.md) for full details.

```sql
-- =============================================================================
-- AGGREGATE TABLES - Pre-computed metrics from controller_event_log
-- =============================================================================
-- Models: tsigma/models/aggregates.py
-- Jobs:   tsigma/scheduler/jobs/aggregate.py

CREATE TABLE detector_volume_hourly (
    signal_id           TEXT NOT NULL,
    detector_channel    INTEGER NOT NULL,
    hour_start          TIMESTAMPTZ NOT NULL,
    volume              INTEGER NOT NULL DEFAULT 0,
    activations         INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (signal_id, detector_channel, hour_start)
);

CREATE TABLE detector_occupancy_hourly (
    signal_id           TEXT NOT NULL,
    detector_channel    INTEGER NOT NULL,
    hour_start          TIMESTAMPTZ NOT NULL,
    occupancy_pct       FLOAT NOT NULL DEFAULT 0.0,
    total_on_seconds    FLOAT NOT NULL DEFAULT 0.0,
    activation_count    INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (signal_id, detector_channel, hour_start)
);

CREATE TABLE split_failure_hourly (
    signal_id           TEXT NOT NULL,
    phase               INTEGER NOT NULL,
    hour_start          TIMESTAMPTZ NOT NULL,
    total_cycles        INTEGER NOT NULL DEFAULT 0,
    failed_cycles       INTEGER NOT NULL DEFAULT 0,
    failure_rate_pct    FLOAT NOT NULL DEFAULT 0.0,
    PRIMARY KEY (signal_id, phase, hour_start)
);

CREATE TABLE approach_delay_15min (
    signal_id           TEXT NOT NULL,
    phase               INTEGER NOT NULL,
    bin_start           TIMESTAMPTZ NOT NULL,
    avg_delay_seconds   FLOAT NOT NULL DEFAULT 0.0,
    max_delay_seconds   FLOAT NOT NULL DEFAULT 0.0,
    total_arrivals      INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (signal_id, phase, bin_start)
);

CREATE TABLE arrival_on_red_hourly (
    signal_id           TEXT NOT NULL,
    phase               INTEGER NOT NULL,
    hour_start          TIMESTAMPTZ NOT NULL,
    total_arrivals      INTEGER NOT NULL DEFAULT 0,
    arrivals_on_red     INTEGER NOT NULL DEFAULT 0,
    arrivals_on_green   INTEGER NOT NULL DEFAULT 0,
    red_pct             FLOAT NOT NULL DEFAULT 0.0,
    green_pct           FLOAT NOT NULL DEFAULT 0.0,
    PRIMARY KEY (signal_id, phase, hour_start)
);

CREATE TABLE coordination_quality_hourly (
    signal_id               TEXT NOT NULL,
    hour_start              TIMESTAMPTZ NOT NULL,
    total_cycles            INTEGER NOT NULL DEFAULT 0,
    cycles_within_tolerance INTEGER NOT NULL DEFAULT 0,
    quality_pct             FLOAT NOT NULL DEFAULT 0.0,
    avg_cycle_length_seconds FLOAT NOT NULL DEFAULT 0.0,
    avg_offset_error_seconds FLOAT NOT NULL DEFAULT 0.0,
    PRIMARY KEY (signal_id, hour_start)
);

CREATE TABLE phase_termination_hourly (
    signal_id           TEXT NOT NULL,
    phase               INTEGER NOT NULL,
    hour_start          TIMESTAMPTZ NOT NULL,
    total_cycles        INTEGER NOT NULL DEFAULT 0,
    gap_outs            INTEGER NOT NULL DEFAULT 0,
    max_outs            INTEGER NOT NULL DEFAULT 0,
    force_offs          INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (signal_id, phase, hour_start)
);

-- PCD Continuous Aggregate Views (TimescaleDB only)
-- These models map to TimescaleDB continuous aggregate views, not regular tables.
-- For non-TimescaleDB databases, these don't exist and the PCD report falls
-- back to querying raw events.

CREATE TABLE cycle_boundary (
    signal_id               TEXT NOT NULL,
    phase                   INTEGER NOT NULL,
    green_start             TIMESTAMPTZ NOT NULL,
    yellow_start            TIMESTAMPTZ,
    red_start               TIMESTAMPTZ,
    cycle_end               TIMESTAMPTZ,
    green_duration_seconds  FLOAT,
    yellow_duration_seconds FLOAT,
    red_duration_seconds    FLOAT,
    cycle_duration_seconds  FLOAT,
    termination_type        TEXT,
    PRIMARY KEY (signal_id, phase, green_start)
);

CREATE TABLE cycle_detector_arrival (
    signal_id               TEXT NOT NULL,
    phase                   INTEGER NOT NULL,
    detector_channel        INTEGER NOT NULL,
    arrival_time            TIMESTAMPTZ NOT NULL,
    green_start             TIMESTAMPTZ NOT NULL,
    time_in_cycle_seconds   FLOAT NOT NULL,
    phase_state             TEXT NOT NULL,
    PRIMARY KEY (signal_id, phase, detector_channel, arrival_time)
);

CREATE TABLE cycle_summary_15min (
    signal_id               TEXT NOT NULL,
    phase                   INTEGER NOT NULL,
    bin_start               TIMESTAMPTZ NOT NULL,
    total_cycles            INTEGER NOT NULL DEFAULT 0,
    avg_cycle_length_seconds FLOAT NOT NULL DEFAULT 0.0,
    avg_green_seconds       FLOAT NOT NULL DEFAULT 0.0,
    total_arrivals          INTEGER NOT NULL DEFAULT 0,
    arrivals_on_green       INTEGER NOT NULL DEFAULT 0,
    arrivals_on_yellow      INTEGER NOT NULL DEFAULT 0,
    arrivals_on_red         INTEGER NOT NULL DEFAULT 0,
    arrival_on_green_pct    FLOAT NOT NULL DEFAULT 0.0,
    PRIMARY KEY (signal_id, phase, bin_start)
);
```

### Example: TimescaleDB Continuous Aggregates

When TimescaleDB is available, continuous aggregates can be created for
automatic incremental refresh:

```sql
-- Detector counts (15-minute buckets)
CREATE MATERIALIZED VIEW cagg_detector_count
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('15 minutes', event_time) AS bucket,
    signal_id,
    event_param AS detector_channel,
    COUNT(*) AS event_count
FROM controller_event_log
WHERE event_code = 82  -- Vehicle Detector On
GROUP BY bucket, signal_id, event_param
WITH NO DATA;

SELECT add_continuous_aggregate_policy('cagg_detector_count',
    start_offset => INTERVAL '1 hour',
    end_offset => INTERVAL '15 minutes',
    schedule_interval => INTERVAL '15 minutes');

-- Phase terminations (15-minute buckets)
CREATE MATERIALIZED VIEW cagg_phase_termination
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('15 minutes', event_time) AS bucket,
    signal_id,
    event_param AS phase_number,
    COUNT(*) FILTER (WHERE event_code = 4) AS gap_outs,
    COUNT(*) FILTER (WHERE event_code = 5) AS max_outs,
    COUNT(*) FILTER (WHERE event_code = 6) AS force_offs
FROM controller_event_log
WHERE event_code IN (4, 5, 6)  -- Gap Out, Max Out, Force Off
GROUP BY bucket, signal_id, event_param
WITH NO DATA;

SELECT add_continuous_aggregate_policy('cagg_phase_termination',
    start_offset => INTERVAL '1 hour',
    end_offset => INTERVAL '15 minutes',
    schedule_interval => INTERVAL '15 minutes');
```

### Configuration Tables

```sql
-- =============================================================================
-- CONFIGURATION TABLES - Signal metadata
-- =============================================================================

-- Signals
CREATE TABLE signal (
    signal_id           TEXT PRIMARY KEY,
    primary_street      TEXT NOT NULL,
    secondary_street    TEXT,
    latitude            DECIMAL(10, 7),
    longitude           DECIMAL(10, 7),
    jurisdiction_id     UUID,
    region_id           UUID,
    corridor_id         UUID,
    controller_type_id  UUID,
    ip_address          INET,
    note                TEXT,
    metadata            JSONB,  -- Flexible key-value pairs (location_type, custom fields, etc.)
    enabled             BOOLEAN DEFAULT TRUE,
    start_date          DATE,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_signal_region ON signal (region_id);
CREATE INDEX idx_signal_corridor ON signal (corridor_id);
CREATE INDEX idx_signal_controller_type ON signal (controller_type_id);
CREATE INDEX idx_signal_metadata ON signal USING GIN (metadata);

-- Signal Audit History
-- Tracks all changes to signal configuration over time
CREATE TABLE signal_audit (
    audit_id            BIGSERIAL PRIMARY KEY,
    signal_id           TEXT NOT NULL,
    changed_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    changed_by          TEXT,  -- User ID or system identifier
    operation           TEXT NOT NULL CHECK (operation IN ('INSERT', 'UPDATE', 'DELETE')),
    -- Snapshot of changed fields (JSONB for flexibility)
    old_values          JSONB,
    new_values          JSONB
);

CREATE INDEX idx_signal_audit_signal ON signal_audit (signal_id, changed_at DESC);
CREATE INDEX idx_signal_audit_time ON signal_audit (changed_at DESC);

-- Trigger to automatically populate audit table
CREATE OR REPLACE FUNCTION audit_signal_changes()
RETURNS TRIGGER AS $$
BEGIN
    IF (TG_OP = 'DELETE') THEN
        INSERT INTO signal_audit (signal_id, operation, old_values)
        VALUES (OLD.signal_id, 'DELETE', to_jsonb(OLD));
        RETURN OLD;
    ELSIF (TG_OP = 'UPDATE') THEN
        INSERT INTO signal_audit (signal_id, operation, old_values, new_values)
        VALUES (NEW.signal_id, 'UPDATE', to_jsonb(OLD), to_jsonb(NEW));
        RETURN NEW;
    ELSIF (TG_OP = 'INSERT') THEN
        INSERT INTO signal_audit (signal_id, operation, new_values)
        VALUES (NEW.signal_id, 'INSERT', to_jsonb(NEW));
        RETURN NEW;
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER signal_audit_trigger
    AFTER INSERT OR UPDATE OR DELETE ON signal
    FOR EACH ROW EXECUTE FUNCTION audit_signal_changes();

-- Approaches
CREATE TABLE approach (
    approach_id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    signal_id               TEXT NOT NULL REFERENCES signal(signal_id),
    direction_type_id       SMALLINT NOT NULL,
    description             TEXT,
    mph                     SMALLINT,
    protected_phase_number  SMALLINT,
    is_protected_phase_overlap BOOLEAN DEFAULT FALSE,
    permissive_phase_number SMALLINT,
    is_permissive_phase_overlap BOOLEAN DEFAULT FALSE,
    ped_phase_number        SMALLINT,
    created_at              TIMESTAMPTZ DEFAULT NOW(),
    updated_at              TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_approach_signal ON approach (signal_id);

-- Direction Types
CREATE TABLE direction_type (
    direction_type_id   SMALLINT PRIMARY KEY,
    abbreviation        TEXT NOT NULL,
    description         TEXT NOT NULL
);

INSERT INTO direction_type VALUES
(1, 'NB', 'Northbound'),
(2, 'SB', 'Southbound'),
(3, 'EB', 'Eastbound'),
(4, 'WB', 'Westbound'),
(5, 'NE', 'Northeast'),
(6, 'NW', 'Northwest'),
(7, 'SE', 'Southeast'),
(8, 'SW', 'Southwest');

-- Detectors
CREATE TABLE detector (
    detector_id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    approach_id             UUID NOT NULL REFERENCES approach(approach_id),
    detector_channel        SMALLINT NOT NULL,
    distance_from_stop_bar  INTEGER,
    min_speed_filter        SMALLINT,
    decision_point          INTEGER,
    movement_delay          SMALLINT,
    lane_number             SMALLINT,
    lane_type_id            UUID REFERENCES lane_type(lane_type_id),
    movement_type_id        UUID REFERENCES movement_type(movement_type_id),
    detection_hardware_id   UUID REFERENCES detection_hardware(detection_hardware_id),
    lat_lon_distance        INTEGER,
    created_at              TIMESTAMPTZ DEFAULT NOW(),
    updated_at              TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_detector_approach ON detector (approach_id);

-- Controller Types
CREATE TABLE controller_type (
    controller_type_id  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    description         TEXT NOT NULL,
    snmp_port           INTEGER DEFAULT 161,
    ftp_directory       TEXT,
    active_ftp          BOOLEAN DEFAULT FALSE,
    username            TEXT,
    password            TEXT
);

-- Seed data with fixed UUIDs (deterministic for all deployments)
INSERT INTO controller_type (controller_type_id, description) VALUES
('00000000-0000-0000-0000-000000000001'::UUID, 'Econolite Cobalt'),
('00000000-0000-0000-0000-000000000002'::UUID, 'Econolite ASC/3'),
('00000000-0000-0000-0000-000000000003'::UUID, 'Siemens SEPAC'),
('00000000-0000-0000-0000-000000000004'::UUID, 'McCain ATC'),
('00000000-0000-0000-0000-000000000005'::UUID, 'Intelight MaxTime'),
('00000000-0000-0000-0000-000000000006'::UUID, 'Q-Free MaxView'),
('00000000-0000-0000-0000-000000000007'::UUID, 'Trafficware'),
('00000000-0000-0000-0000-000000000008'::UUID, 'Peek');

-- Jurisdictions
CREATE TABLE jurisdiction (
    jurisdiction_id     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name                TEXT NOT NULL,
    mpo_name            TEXT,
    county_name         TEXT
);

-- Regions (hierarchical - allows unlimited nesting)
CREATE TABLE region (
    region_id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    parent_region_id    UUID REFERENCES region(region_id) ON DELETE CASCADE,
    description         TEXT NOT NULL
);

CREATE INDEX idx_region_parent ON region (parent_region_id);

-- Corridors (simple organizational grouping)
CREATE TABLE corridor (
    corridor_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name                TEXT NOT NULL,
    description         TEXT,
    jurisdiction_id     UUID
);

-- Routes (progression/coordination plans)
CREATE TABLE route (
    route_id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name                TEXT NOT NULL
);

-- Route Signals (ordered sequence of signals in progression)
CREATE TABLE route_signal (
    route_signal_id     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    route_id            UUID NOT NULL REFERENCES route(route_id),
    signal_id           TEXT NOT NULL REFERENCES signal(signal_id),
    sequence_order      SMALLINT NOT NULL,
    UNIQUE (route_id, sequence_order),
    UNIQUE (route_id, signal_id)
);

CREATE INDEX idx_route_signal_route ON route_signal (route_id, sequence_order);

-- Route Phase Configuration (which phases participate in progression)
CREATE TABLE route_phase (
    route_phase_id      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    route_signal_id     UUID NOT NULL REFERENCES route_signal(route_signal_id),
    phase_number        SMALLINT NOT NULL,
    direction_type_id   SMALLINT NOT NULL,
    is_overlap          BOOLEAN DEFAULT FALSE,
    is_primary_approach BOOLEAN DEFAULT FALSE
);

CREATE INDEX idx_route_phase_signal ON route_phase (route_signal_id);

-- Route Distance (travel time between consecutive signals)
CREATE TABLE route_distance (
    route_distance_id   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    from_route_signal_id UUID NOT NULL REFERENCES route_signal(route_signal_id),
    to_route_signal_id  UUID NOT NULL REFERENCES route_signal(route_signal_id),
    distance_feet       INTEGER NOT NULL,
    travel_time_seconds SMALLINT,
    UNIQUE (from_route_signal_id, to_route_signal_id)
);

CREATE INDEX idx_route_distance_from ON route_distance (from_route_signal_id);

-- Lane Types (vehicle, bike, pedestrian, etc.)
CREATE TABLE lane_type (
    lane_type_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    description         TEXT NOT NULL,
    abbreviation        TEXT
);

-- Seed data with fixed UUIDs
INSERT INTO lane_type (lane_type_id, description, abbreviation) VALUES
('00000000-0000-0000-0001-000000000001'::UUID, 'Vehicle', 'V'),
('00000000-0000-0000-0001-000000000002'::UUID, 'Bicycle', 'B'),
('00000000-0000-0000-0001-000000000003'::UUID, 'Pedestrian', 'P'),
('00000000-0000-0000-0001-000000000004'::UUID, 'HOV', 'HOV'),
('00000000-0000-0000-0001-000000000005'::UUID, 'Exit', 'EX');

-- Movement Types (left, through, right, U-turn)
CREATE TABLE movement_type (
    movement_type_id    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    description         TEXT NOT NULL,
    abbreviation        TEXT,
    display_order       SMALLINT
);

-- Seed data with fixed UUIDs
INSERT INTO movement_type (movement_type_id, description, abbreviation, display_order) VALUES
('00000000-0000-0000-0002-000000000001'::UUID, 'Left Turn', 'L', 1),
('00000000-0000-0000-0002-000000000002'::UUID, 'Through', 'T', 2),
('00000000-0000-0000-0002-000000000003'::UUID, 'Right Turn', 'R', 3),
('00000000-0000-0000-0002-000000000004'::UUID, 'U-Turn', 'U', 4),
('00000000-0000-0000-0002-000000000005'::UUID, 'Pedestrian', 'P', 5);

-- Detection Hardware (loop, video, radar, etc.)
CREATE TABLE detection_hardware (
    detection_hardware_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name                TEXT NOT NULL
);

-- Seed data with fixed UUIDs
INSERT INTO detection_hardware (detection_hardware_id, name) VALUES
('00000000-0000-0000-0003-000000000001'::UUID, 'Inductive Loop'),
('00000000-0000-0000-0003-000000000002'::UUID, 'Video Detection'),
('00000000-0000-0000-0003-000000000003'::UUID, 'Radar'),
('00000000-0000-0000-0003-000000000004'::UUID, 'Microwave'),
('00000000-0000-0000-0003-000000000005'::UUID, 'Magnetic'),
('00000000-0000-0000-0003-000000000006'::UUID, 'Acoustic');
```

### Polling Checkpoint Table

```sql
-- =============================================================================
-- POLLING CHECKPOINT - Tracks last successful poll per signal per method
-- =============================================================================
-- Provides persistent, non-destructive polling state so TSIGMA:
--   1. Never re-downloads files it has already ingested
--   2. Survives service restarts without data loss or reprocessing
--   3. Allows multiple independent consumers (no destructive reads)
--   4. Enables crash recovery from the exact point of interruption
--
-- ATSPM 4.x/5.x Comparison:
--   ATSPM 4.x: Deletes files from controller after FTP download (destructive)
--   ATSPM 5.x: Excludes newest file by modification time (in-memory only)
--   TSIGMA:    Persistent checkpoint per signal — non-destructive, restartable
--
-- Design Rationale:
--   - Composite PK (signal_id, method) allows the same signal to be polled
--     by different methods (e.g., FTP primary, HTTP fallback) independently
--   - last_filename + last_file_mtime enable file-level deduplication for
--     FTP/SFTP where files accumulate on the controller
--   - last_event_timestamp enables event-level deduplication for HTTP APIs
--     that support incremental queries (e.g., ?since=<timestamp>)
--   - last_successful_poll tracks when the poll cycle itself last completed,
--     independent of what data was in it (useful for health monitoring)
--   - error tracking (consecutive_errors, last_error, last_error_time) enables
--     automatic backoff and alerting without a separate monitoring table
--   - files_hash stores a hash of the set of filenames seen on last poll,
--     enabling fast "anything changed?" checks without re-listing every file

CREATE TABLE polling_checkpoint (
    -- Which signal and which ingestion method
    signal_id               TEXT NOT NULL,
    method                  TEXT NOT NULL,       -- 'ftp_pull', 'http_pull', etc.

    -- File-based checkpoint (FTP/SFTP polling)
    last_filename           TEXT,                -- Last file successfully ingested
    last_file_mtime         TIMESTAMPTZ,         -- Modification time of that file on the remote server
    files_hash              TEXT,                -- SHA-256 of sorted filenames from last listing
                                                 -- (quick change detection without full re-comparison)

    -- Event-based checkpoint (HTTP polling)
    last_event_timestamp    TIMESTAMPTZ,         -- Timestamp of newest event in last successful ingest
                                                 -- Used as ?since= parameter on next poll

    -- Poll cycle metadata
    last_successful_poll    TIMESTAMPTZ,         -- When the last poll cycle completed successfully
    events_ingested         BIGINT DEFAULT 0,    -- Cumulative events ingested for this signal+method
    files_ingested          BIGINT DEFAULT 0,    -- Cumulative files ingested for this signal+method

    -- Error tracking (for backoff and alerting)
    consecutive_errors      INTEGER DEFAULT 0,   -- Reset to 0 on success, increment on failure
    last_error              TEXT,                -- Most recent error message
    last_error_time         TIMESTAMPTZ,         -- When the last error occurred

    -- Audit
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (signal_id, method)
);

-- Index for health monitoring queries ("which signals haven't polled recently?")
CREATE INDEX idx_checkpoint_last_poll
    ON polling_checkpoint (method, last_successful_poll);

-- Index for error monitoring ("which signals are failing?")
CREATE INDEX idx_checkpoint_errors
    ON polling_checkpoint (consecutive_errors DESC)
    WHERE consecutive_errors > 0;

-- Foreign key to signal table (ensures checkpoint cleanup when signal is deleted)
ALTER TABLE polling_checkpoint
    ADD CONSTRAINT fk_checkpoint_signal
    FOREIGN KEY (signal_id) REFERENCES signal(signal_id)
    ON DELETE CASCADE;
```

#### Checkpoint Update Flow

```
Poll Cycle Start
    │
    ├─ Query: SELECT * FROM polling_checkpoint
    │         WHERE signal_id = ? AND method = ?
    │
    ├─ FTP: List remote files
    │   ├─ Compare files_hash → if unchanged, skip (no new files)
    │   ├─ Filter: file.mtime > last_file_mtime
    │   ├─ Download + decode + ingest new files only
    │   └─ Update: last_filename, last_file_mtime, files_hash,
    │              last_successful_poll, events_ingested, files_ingested,
    │              consecutive_errors = 0
    │
    ├─ HTTP: Query with ?since=last_event_timestamp
    │   ├─ Decode + ingest returned events
    │   └─ Update: last_event_timestamp, last_successful_poll,
    │              events_ingested, consecutive_errors = 0
    │
    └─ On Error:
        └─ Update: consecutive_errors += 1, last_error, last_error_time
           (do NOT update last_successful_poll or any checkpoint fields)
```

#### Checkpoint Lifecycle

| Scenario | Behavior |
|----------|----------|
| **First poll ever** | No checkpoint row exists → poll all files → INSERT checkpoint |
| **Normal poll** | Checkpoint exists → filter by mtime/event_timestamp → UPDATE on success |
| **Service restart** | Checkpoint persists → resumes exactly where it left off |
| **Failed poll** | Only error fields updated → checkpoint stays at last success |
| **Signal deleted** | `ON DELETE CASCADE` removes checkpoint automatically |
| **Method change** | New (signal_id, method) pair → fresh checkpoint, old one remains |
| **Multiple consumers** | Each consumer uses a different `method` value → independent checkpoints |
| **Controller replaced** | Admin resets checkpoint via API or SQL → re-polls from scratch |

### Data Storage Tiers & Retention

TSIGMA uses a tiered storage model. All intervals are configurable per deployment.

```
HOT (uncompressed) ──► WARM (compressed) ──► COLD (Parquet) ──► DROP
     0 – warm_after    warm_after – cold_after   cold_after – retention
```

#### Tier Transitions

| Transition | Mechanism | Default Interval | Config Variable |
|------------|-----------|------------------|-----------------|
| Hot → Warm | TimescaleDB compression policy | 7 days | `TSIGMA_STORAGE_WARM_AFTER` |
| Warm → Cold | Parquet export scheduler job | 6 months | `TSIGMA_STORAGE_COLD_AFTER` |
| Cold → Drop | TimescaleDB retention policy | 2 years | `TSIGMA_STORAGE_RETENTION` |

**Note:** SaaS deployments skip the Cold tier. Warm data is retained until `TSIGMA_STORAGE_RETENTION`, then dropped.

```sql
-- =============================================================================
-- WARM TIER — TimescaleDB compression
-- =============================================================================

-- Compression policy (interval from TSIGMA_STORAGE_WARM_AFTER, default 7 days)
ALTER TABLE controller_event_log SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'signal_id',
    timescaledb.compress_orderby = 'event_time DESC'
);
SELECT add_compression_policy('controller_event_log', INTERVAL '7 days');

-- =============================================================================
-- COLD TIER — Parquet export (On-Prem only)
-- =============================================================================
-- Managed by APScheduler job: exports aged-out chunks to Parquet files,
-- drops the TimescaleDB chunk, and creates a foreign table via parquet_fdw.
--
-- Requires: CREATE EXTENSION parquet_fdw;  (or duckdb_fdw)
--
-- Foreign server setup:
CREATE SERVER parquet_srv FOREIGN DATA WRAPPER parquet_fdw;

-- Foreign table (auto-created per exported chunk by the cold export job):
-- CREATE FOREIGN TABLE controller_event_log_cold_2026w01 (
--     signal_id  VARCHAR(32),
--     event_time   TIMESTAMPTZ,
--     event_code   INTEGER,
--     event_param  INTEGER
-- ) SERVER parquet_srv
-- OPTIONS (filename '/var/lib/tsigma/cold/cel_2026w01.parquet');

-- Unified view (all tiers queryable via single table):
-- CREATE VIEW controller_event_log_all AS
-- SELECT * FROM controller_event_log           -- hot + warm
-- UNION ALL
-- SELECT * FROM controller_event_log_cold;     -- cold (Parquet)

-- =============================================================================
-- RETENTION — drop data entirely
-- =============================================================================

-- Retention policy configured at app initialization
-- Environment variable: TSIGMA_STORAGE_RETENTION
-- Default: '2 years'
--
-- Python example (in database initialization):
--   retention = os.getenv('TSIGMA_STORAGE_RETENTION', '2 years')
--   await session.execute(text(f"""
--       SELECT add_retention_policy(
--           'controller_event_log',
--           INTERVAL '{retention}',
--           if_not_exists => TRUE
--       )
--   """))
--
-- For manual creation (development/testing):
SELECT add_retention_policy('controller_event_log', INTERVAL '2 years', if_not_exists => TRUE);

-- NOTE: Aggregation retention policies are managed by the report plugins that create them
-- Example from plugin:
-- SELECT add_retention_policy('cagg_detector_count', INTERVAL '7 years', if_not_exists => TRUE);
```

#### Continuous Aggregates

```sql
-- Continuous aggregation for real-time dashboards
CREATE MATERIALIZED VIEW mv_hourly_event_counts
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', event_time) AS bucket,
    signal_id,
    COUNT(*) as event_count,
    COUNT(DISTINCT event_code) as unique_codes
FROM controller_event_log
GROUP BY bucket, signal_id;

SELECT add_continuous_aggregate_policy('mv_hourly_event_counts',
    start_offset => INTERVAL '3 hours',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour');
```

---

## Storage Estimates

| Data Type | Per Signal/Day | 9000 Signals | Per Year |
|-----------|---------------|--------------|----------|
| Raw Events | ~1.2M rows | ~11B rows | ~4 TB compressed |
| Aggregations | ~1000 rows | ~9M rows | ~50 GB |
| Configuration | Static | ~50K rows | < 100 MB |

**TimescaleDB Compression ratios:**
- Raw events: 10-15x compression
- Aggregations: 5-8x compression

---

## Migration from ATSPM

```sql
-- Example: Insert from ATSPM MS-SQL export
INSERT INTO controller_event_log (signal_id, event_time, event_code, event_param)
SELECT
    TRIM(SignalID),
    Timestamp AT TIME ZONE 'America/New_York',
    EventCode::INTEGER,
    EventParam::INTEGER
FROM atspm_import
ON CONFLICT DO NOTHING;
```

---

## Database-Specific Implementations

TSIGMA is database-neutral with a preference for PostgreSQL + TimescaleDB. Each supported database has optimal configurations for time-series event data.

### PostgreSQL + TimescaleDB (Preferred)

```sql
-- Hypertable with weekly chunks (automatic partitioning)
SELECT create_hypertable('controller_event_log', 'event_time',
    chunk_time_interval => INTERVAL '1 week',
    origin => '2026-02-07'::timestamptz);  -- Saturday for GDOT

-- Warm tier compression (10-15x ratio, interval from TSIGMA_STORAGE_WARM_AFTER)
ALTER TABLE controller_event_log SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'signal_id',
    timescaledb.compress_orderby = 'event_time DESC'
);
SELECT add_compression_policy('controller_event_log', INTERVAL '7 days');

-- Indexes (chunk exclusion handles time filtering automatically)
CREATE INDEX idx_cel_signal_time ON controller_event_log (signal_id, event_time DESC);
CREATE INDEX idx_cel_event_time ON controller_event_log (event_code, event_time DESC);
```

### MS-SQL Server

```sql
-- Clustered index (physically orders data for optimal range scans)
CREATE CLUSTERED INDEX IX_CEL_Location_Time
    ON controller_event_log (signal_id, event_time DESC);

-- Non-clustered index for cross-signal queries
CREATE NONCLUSTERED INDEX IX_CEL_Event_Time
    ON controller_event_log (event_code, event_time DESC)
    INCLUDE (signal_id, event_param);

-- Partitioning (requires Enterprise Edition)
CREATE PARTITION FUNCTION pf_cel_weekly (DATETIME2)
    AS RANGE RIGHT FOR VALUES ('2026-02-07', '2026-02-14', ...);
```

### Oracle

```sql
-- Partitioned table with weekly intervals
CREATE TABLE controller_event_log (
    signal_id       VARCHAR2(4000) NOT NULL,  -- Oracle max VARCHAR2 size
    event_time      TIMESTAMP WITH TIME ZONE NOT NULL,
    event_code      NUMBER(10) NOT NULL,
    event_param     NUMBER(10) NOT NULL
)
PARTITION BY RANGE (event_time)
INTERVAL (NUMTODSINTERVAL(7, 'DAY'))
(PARTITION p_initial VALUES LESS THAN (TIMESTAMP '2026-02-07 00:00:00'));

-- Local indexes (one per partition)
CREATE INDEX idx_cel_signal_time ON controller_event_log (signal_id, event_time DESC) LOCAL;
CREATE INDEX idx_cel_event_time ON controller_event_log (event_code, event_time DESC) LOCAL;
```

### ClickHouse

```sql
-- MergeTree engine (columnar storage, no indexes needed for filtering)
CREATE TABLE controller_event_log (
    signal_id     String,
    event_time      DateTime64(6, 'UTC'),
    event_code      UInt16,
    event_param     UInt32
)
ENGINE = MergeTree()
PARTITION BY toMonday(event_time)  -- Weekly partitions
ORDER BY (signal_id, event_time, event_code)
SETTINGS index_granularity = 8192;

-- ClickHouse's columnar storage makes event_code filtering efficient without indexes
-- ORDER BY clause provides implicit indexing for signal_id + event_time queries
```

### Index Size Comparison (Estimated for 3 weeks, ~9k signals)

| Database | Table Size | Index Size | Total | Notes |
|----------|-----------|------------|-------|-------|
| MS-SQL (ATSPM) | ~2.5 TB | ~1 TB | ~3.5 TB | Clustered + non-clustered |
| PostgreSQL + TimescaleDB | ~800 GB | ~200 GB | ~1 TB | Before compression |
| PostgreSQL + TimescaleDB | ~80 GB | ~50 GB | ~130 GB | After compression (7+ days) |
| ClickHouse | ~50 GB | N/A | ~50 GB | Columnar compression |

---

## Key Improvements Over ATSPM

| Feature | ATSPM 4.x | ATSPM 5.x | TSIGMA |
|---------|-----------|-----------|--------|
| Database Support | MS-SQL only | Multi-DB (unoptimized) | Multi-DB (optimized per platform) |
| Partitioning | None | None | Native (TimescaleDB/Oracle/ClickHouse) |
| Compression | None | JSON blobs | Native (10-25x) |
| Data Types | Oversized INT | Correct | INTEGER (ATSPM-compatible) |
| Query Speed | Slow (full scan) | Slow (decompress) | Fast (partition pruning) |
| Retention | Manual | Manual | Automatic policies |
| Real-time | No | No | Continuous aggregates |

---

Developed by OpenPhase Labs
