# Database Design

> Part of [TSIGMA Architecture](../ARCHITECTURE.md)

---

## Schema Organization

TSIGMA uses four schemas for logical separation on PostgreSQL, MS-SQL,
and Oracle. MySQL uses a single database (no schema prefix). Models
declare their schema via `tsigma_schema()` in `__table_args__`.

| Schema | Tables | Purpose |
|--------|--------|---------|
| **config** | `signal`, `approach`, `detector`, `controller_type`, `jurisdiction`, `region`, `corridor`, `direction_type`, `lane_type`, `movement_type`, `detection_hardware`, `event_code_definition`, `route`, `route_signal`, `route_phase`, `route_distance`, `signal_audit`, `approach_audit`, `detector_audit` | Configuration, reference data, routes, audit |
| **events** | `controller_event_log`, `polling_checkpoint`, `signal_plan` | Time-series events, operational state |
| **aggregation** | `detector_volume_hourly`, `detector_occupancy_hourly`, `split_failure_hourly`, `approach_delay_15min`, `arrival_on_red_hourly`, `coordination_quality_hourly`, `phase_termination_hourly`, `cycle_boundary`, `cycle_detector_arrival`, `cycle_summary_15min` | Pre-computed aggregations |
| **identity** | `auth_audit_log`, `auth_user`, `api_key`, `system_setting` | Authentication, system config |

## Configuration Tables

```sql
-- Signals (traffic intersections)
CREATE TABLE signal (
    signal_id           TEXT PRIMARY KEY,
    primary_street      TEXT NOT NULL,
    secondary_street    TEXT,
    latitude            DECIMAL(10, 7),
    longitude           DECIMAL(10, 7),
    jurisdiction_id     UUID REFERENCES jurisdiction(jurisdiction_id),
    region_id           UUID REFERENCES region(region_id),
    corridor_id         UUID REFERENCES corridor(corridor_id),
    controller_type_id  UUID REFERENCES controller_type(controller_type_id),
    ip_address          INET,
    note                TEXT,
    metadata            JSONB,
    enabled             BOOLEAN DEFAULT TRUE,
    start_date          DATE,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Approaches
CREATE TABLE approach (
    approach_id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    signal_id               TEXT NOT NULL REFERENCES signal(signal_id),
    direction_type_id       SMALLINT NOT NULL REFERENCES direction_type(direction_type_id),
    description             TEXT,
    mph                     SMALLINT,
    protected_phase_number  SMALLINT,
    is_protected_phase_overlap BOOLEAN DEFAULT FALSE,
    permissive_phase_number SMALLINT,
    is_permissive_phase_overlap BOOLEAN DEFAULT FALSE,
    ped_phase_number        SMALLINT,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

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
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
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

-- Route Phase Configuration
CREATE TABLE route_phase (
    route_phase_id      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    route_signal_id     UUID NOT NULL REFERENCES route_signal(route_signal_id),
    phase_number        SMALLINT NOT NULL,
    direction_type_id   SMALLINT NOT NULL REFERENCES direction_type(direction_type_id),
    is_overlap          BOOLEAN DEFAULT FALSE,
    is_primary_approach BOOLEAN DEFAULT FALSE
);

-- Route Distance (travel time between consecutive signals)
CREATE TABLE route_distance (
    route_distance_id   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    from_route_signal_id UUID NOT NULL REFERENCES route_signal(route_signal_id),
    to_route_signal_id  UUID NOT NULL REFERENCES route_signal(route_signal_id),
    distance_feet       INTEGER NOT NULL,
    travel_time_seconds SMALLINT,
    UNIQUE (from_route_signal_id, to_route_signal_id)
);
```

## Event Table

```sql
-- Controller events (TimescaleDB hypertable)
-- Note: INTEGER for event_code/event_param - ATSPM vendors use values > SMALLINT max
CREATE TABLE controller_event_log (
    signal_id       TEXT NOT NULL,
    event_time      TIMESTAMPTZ NOT NULL,
    event_code      INTEGER NOT NULL,
    event_param     INTEGER NOT NULL,
    device_id       SMALLINT DEFAULT 1,
    validation_metadata JSONB,
    PRIMARY KEY (signal_id, event_time, event_code, event_param)
);

-- Indexes
CREATE INDEX idx_cel_signal_time
    ON controller_event_log (signal_id, event_time DESC);
CREATE INDEX idx_cel_event_time
    ON controller_event_log (event_code, event_time DESC);
```

## Aggregate Tables

Pre-computed aggregates populated by one of two mechanisms:

- **PostgreSQL + TimescaleDB**: Continuous aggregates (automatic, incremental)
- **All other databases**: APScheduler jobs that delete-and-reinsert a sliding window every 15 minutes (see `tsigma/scheduler/jobs/aggregate.py`)

The API layer reads from these tables identically regardless of how they are populated.

```sql
-- Hourly detector volume
CREATE TABLE detector_volume_hourly (
    signal_id           TEXT NOT NULL,
    detector_channel    INTEGER NOT NULL,
    hour_start          TIMESTAMPTZ NOT NULL,
    volume              INTEGER NOT NULL DEFAULT 0,
    activations         INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (signal_id, detector_channel, hour_start)
);

-- Hourly detector occupancy
CREATE TABLE detector_occupancy_hourly (
    signal_id           TEXT NOT NULL,
    detector_channel    INTEGER NOT NULL,
    hour_start          TIMESTAMPTZ NOT NULL,
    occupancy_pct       FLOAT NOT NULL DEFAULT 0.0,
    total_on_seconds    FLOAT NOT NULL DEFAULT 0.0,
    activation_count    INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (signal_id, detector_channel, hour_start)
);

-- Hourly split failure rate
CREATE TABLE split_failure_hourly (
    signal_id           TEXT NOT NULL,
    phase               INTEGER NOT NULL,
    hour_start          TIMESTAMPTZ NOT NULL,
    total_cycles        INTEGER NOT NULL DEFAULT 0,
    failed_cycles       INTEGER NOT NULL DEFAULT 0,
    failure_rate_pct    FLOAT NOT NULL DEFAULT 0.0,
    PRIMARY KEY (signal_id, phase, hour_start)
);

-- 15-minute approach delay
CREATE TABLE approach_delay_15min (
    signal_id           TEXT NOT NULL,
    phase               INTEGER NOT NULL,
    bin_start           TIMESTAMPTZ NOT NULL,
    avg_delay_seconds   FLOAT NOT NULL DEFAULT 0.0,
    max_delay_seconds   FLOAT NOT NULL DEFAULT 0.0,
    total_arrivals      INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (signal_id, phase, bin_start)
);

-- Hourly arrivals on red
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

-- Hourly coordination quality
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

-- Hourly phase termination breakdown
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
```

**Refresh Strategy:**

| Table | Refresh Interval | Mechanism |
|-------|-----------------|-----------|
| All aggregate tables | Every 15 min | APScheduler cron jobs (`tsigma/scheduler/jobs/aggregate.py`) |

APScheduler jobs auto-disable when TimescaleDB continuous aggregates are detected. See [MULTI_DATABASE_AGGREGATES.md](MULTI_DATABASE_AGGREGATES.md).


## Audit Tables

```sql
-- Signal configuration change history
CREATE TABLE signal_audit (
    audit_id            BIGSERIAL PRIMARY KEY,
    signal_id           TEXT NOT NULL,
    changed_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    changed_by          TEXT,
    operation           TEXT NOT NULL,
    old_values          JSONB,
    new_values          JSONB
);

-- Approach configuration change history
CREATE TABLE approach_audit (
    audit_id            BIGSERIAL PRIMARY KEY,
    approach_id         UUID NOT NULL,
    signal_id           TEXT NOT NULL,
    changed_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    changed_by          TEXT,
    operation           TEXT NOT NULL,
    old_values          JSONB,
    new_values          JSONB
);

-- Detector configuration change history
CREATE TABLE detector_audit (
    audit_id            BIGSERIAL PRIMARY KEY,
    detector_id         UUID NOT NULL,
    approach_id         UUID NOT NULL,
    changed_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    changed_by          TEXT,
    operation           TEXT NOT NULL,
    old_values          JSONB,
    new_values          JSONB
);

-- Authentication event log
CREATE TABLE auth_audit_log (
    id                  BIGSERIAL PRIMARY KEY,
    event_type          TEXT NOT NULL,
    user_id             UUID,
    username            TEXT NOT NULL,
    ip_address          TEXT,
    user_agent          TEXT,
    timestamp           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

Audit triggers are generated by `DialectHelper.audit_trigger_sql()` in `tsigma/database/db.py`, which produces dialect-specific trigger DDL for PostgreSQL, MS-SQL, Oracle, and MySQL.


## Data Storage Tiers

TSIGMA uses a three-tier data storage model. See [Architecture Data Storage Tiers](../ARCHITECTURE.md#8-data-storage-tiers) for full details.

```
HOT (uncompressed) --> WARM (compressed) --> COLD (Parquet) --> DROP
     0 - warm_after    warm_after - cold_after   cold_after - retention
```

| Tier | Storage | Query Speed | Config |
|------|---------|-------------|--------|
| **Hot** | TimescaleDB (uncompressed) | Fastest | `TSIGMA_STORAGE_WARM_AFTER` |
| **Warm** | TimescaleDB (compressed, 10-15x) | Fast | `TSIGMA_STORAGE_COLD_AFTER` |
| **Cold** | Parquet via `parquet_fdw` (On-Prem only) | Seconds | `TSIGMA_STORAGE_COLD_*` |

### Warm Tier (TimescaleDB Compression)

```sql
ALTER TABLE controller_event_log SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'signal_id',
    timescaledb.compress_orderby = 'event_time DESC'
);

-- Interval configured via TSIGMA_STORAGE_WARM_AFTER (default: 7 days)
SELECT add_compression_policy('controller_event_log', INTERVAL '7 days');
```

Compression setup is handled in `tsigma/database/init.py` (`_setup_timescale()`).

### Cold Tier (Parquet -- On-Prem Only)

On-Prem deployments can export aged-out warm chunks to Parquet files. A unified view keeps cold data queryable alongside hot/warm data:

```sql
CREATE VIEW controller_event_log_all AS
SELECT * FROM controller_event_log           -- hot + warm (TimescaleDB)
UNION ALL
SELECT * FROM controller_event_log_cold;     -- cold (Parquet FDW)
```

Cold storage endpoint is configurable: local filesystem, NAS mount, or S3-compatible object store.

### Data Retention

```sql
-- TimescaleDB retention policy (from TSIGMA_STORAGE_RETENTION)
SELECT add_retention_policy('controller_event_log', INTERVAL '2 years');
```

### Non-TimescaleDB Deployments

For non-TimescaleDB databases, aggregation is handled by APScheduler jobs in `tsigma/scheduler/jobs/aggregate.py`. See [MULTI_DATABASE_AGGREGATES.md](MULTI_DATABASE_AGGREGATES.md) for details.

## Database Encoding

TSIGMA supports international character sets in signal identifiers and other text fields. The database encoding determines which characters can be stored and how efficiently.

### Encoding Options

| Encoding | Bytes/Char | Character Support | Use Case |
|----------|------------|-------------------|----------|
| **UTF-8** (Default) | 1-4 | Universal (all languages) | Recommended for all deployments |
| **LATIN1** (ISO-8859-1) | 1 | Western European only | Legacy system compatibility |

### UTF-8 (Recommended)

UTF-8 is the default and recommended encoding for all TSIGMA deployments:

- **Universal support**: All languages including Spanish, French, German, Polish, Turkish, Greek, Cyrillic, CJK, and more
- **ASCII compatibility**: Standard ASCII characters (A-Z, 0-9) use exactly 1 byte
- **Future-proof**: No migration needed if requirements change
- **PostgreSQL default**: No special configuration required

### LATIN1 (ISO-8859-1)

Latin1 encoding provides fixed 1-byte characters but with limited language support:

**Supported languages**: Spanish, Portuguese, French, German, Italian, Dutch, Swedish, Norwegian, Danish, Finnish, Icelandic

**NOT supported**: Polish, Czech, Hungarian, Turkish, Greek, Russian, Arabic, Hebrew, CJK languages

> **Warning**: Choosing LATIN1 encoding cannot be changed after database creation. If you later need characters outside Western European languages, you must dump the database, recreate it with UTF-8, and restore the data.

### Configuration

**Docker Compose (LATIN1 - only if required)**:

```yaml
services:
  db:
    image: postgres:18
    environment:
      - POSTGRES_USER=atspm
      - POSTGRES_PASSWORD=${DB_PASS}
      - POSTGRES_DB=atspm
      - POSTGRES_INITDB_ARGS=--encoding=LATIN1 --lc-collate=en_US.ISO-8859-1 --lc-ctype=en_US.ISO-8859-1
```

**Verify encoding**:

```sql
SELECT pg_encoding_to_char(encoding) FROM pg_database WHERE datname = 'tsigma';
-- Expected: UTF8 (default) or LATIN1 (if configured)
```

---

## Migrations (Alembic)

Migrations live in `alembic/versions/`. Alembic runs in async mode
via the custom `env.py`.

### Core Rules

1. **Additive only.** Never drop tables, columns, or data. To undo a change,
   write a new forward migration. `downgrade()` raises `NotImplementedError`.

   TSIGMA must support blue/green deployments: the new version runs migrations
   **before** traffic cuts over, while the old version is still serving
   requests against the same database. A migration that drops a column or
   table will crash the old (green) version that still depends on it. Additive
   changes (new tables, new columns with defaults, new indexes) are safe
   because the old code simply ignores what it doesn't know about.

2. **Idempotent.** Running `alembic upgrade head` twice against the same
   database must be a safe no-op on the second run. This protects against
   corrupted `alembic_version`, manual re-runs, and CI/CD pipelines that
   always run `upgrade head` on deploy.

### How to Write Idempotent Migrations

**Schema changes (CREATE TABLE, CREATE INDEX):**

```python
from alembic import op
import sqlalchemy as sa


def _table_exists(name: str) -> bool:
    conn = op.get_bind()
    result = conn.execute(
        sa.text(
            "SELECT EXISTS ("
            "  SELECT 1 FROM information_schema.tables"
            "  WHERE table_schema = 'public' AND table_name = :name"
            ")"
        ),
        {"name": name},
    )
    return result.scalar() or False


def _index_exists(name: str) -> bool:
    conn = op.get_bind()
    result = conn.execute(
        sa.text(
            "SELECT EXISTS ("
            "  SELECT 1 FROM pg_indexes"
            "  WHERE schemaname = 'public' AND indexname = :name"
            ")"
        ),
        {"name": name},
    )
    return result.scalar() or False


def upgrade() -> None:
    if not _table_exists("my_table"):
        op.create_table("my_table", ...)

    if not _index_exists("ix_my_table_col"):
        op.create_index("ix_my_table_col", "my_table", ["col"])


def downgrade() -> None:
    raise NotImplementedError(
        "Destructive downgrades are not supported. "
        "Write a new forward migration instead."
    )
```

**Data changes (INSERT seed rows):**

```python
def upgrade() -> None:
    conn = op.get_bind()
    conn.execute(
        sa.text(
            "INSERT INTO my_table (id, name) VALUES (:id, :name) "
            "ON CONFLICT DO NOTHING"
        ),
        [{"id": 1, "name": "default"}],
    )
```

**Column additions:**

```python
def _column_exists(table: str, column: str) -> bool:
    conn = op.get_bind()
    result = conn.execute(
        sa.text(
            "SELECT EXISTS ("
            "  SELECT 1 FROM information_schema.columns"
            "  WHERE table_schema = 'public'"
            "    AND table_name = :table AND column_name = :column"
            ")"
        ),
        {"table": table, "column": column},
    )
    return result.scalar() or False


def upgrade() -> None:
    if not _column_exists("my_table", "new_col"):
        op.add_column("my_table", sa.Column("new_col", sa.Text()))
```

### Checklist Before Merging a Migration

- [ ] `upgrade()` is idempotent -- safe to run twice
- [ ] `downgrade()` raises `NotImplementedError` (no destructive rollbacks)
- [ ] Additive only -- no `DROP TABLE`, `DROP COLUMN`, or `DELETE` of user data
- [ ] TimescaleDB calls guarded with `IF EXISTS (SELECT 1 FROM pg_extension ...)`
- [ ] Seed data uses `ON CONFLICT DO NOTHING`
