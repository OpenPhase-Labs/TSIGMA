# Multi-Database Aggregate Strategy

**Purpose**: Document how TSIGMA aggregates work across different database engines.

---

## Overview

TSIGMA supports 4 database engines with **different aggregate capabilities**:

| Database | Continuous Aggregates | Materialized Views | Solution |
|----------|----------------------|-------------------|----------|
| **PostgreSQL + TimescaleDB** | Yes (incremental) | Yes | **Use continuous aggregates (automatic)** |
| **MS-SQL** | No | Indexed views (limited) | **Use APScheduler jobs** |
| **Oracle** | No | Yes (manual refresh) | **Use APScheduler jobs** |
| **MySQL** | No | No | **Use APScheduler jobs (required)** |

---

## Architecture: Dual-Mode Aggregates

### Same Tables, Different Population

```
Aggregate Tables (created by Alembic migration)
    |- detector_volume_hourly
    |- detector_occupancy_hourly
    |- split_failure_hourly
    |- approach_delay_15min
    |- arrival_on_red_hourly
    |- coordination_quality_hourly
    |- phase_termination_hourly
    |- cycle_boundary
    |- cycle_detector_arrival
    |- cycle_summary_15min

Population Method (database-dependent):
    |- PostgreSQL + TimescaleDB -> Continuous aggregates (automatic, incremental)
    |- MS-SQL/Oracle/MySQL -> APScheduler jobs (delete + reinsert sliding window)

API Endpoints (database-agnostic):
    |- Read from tables (same code for all databases)
```

**Key Insight**: API layer doesn't know or care how tables are populated!

---

## PostgreSQL + TimescaleDB (Automatic)

### Continuous Aggregates

TimescaleDB automatically maintains aggregates:

```sql
-- Migration creates regular table first
CREATE TABLE detector_volume_hourly (...);

-- Then converts to continuous aggregate (PostgreSQL + TimescaleDB only)
CREATE MATERIALIZED VIEW detector_volume_hourly_cagg
WITH (timescaledb.continuous) AS
SELECT
    signal_id,
    event_param AS detector_channel,
    time_bucket('1 hour', event_time) AS hour_start,
    COUNT(*) FILTER (WHERE event_code = 82) AS volume,
    COUNT(*) FILTER (WHERE event_code = 81) AS activations
FROM controller_event_log
WHERE event_code IN (81, 82)
GROUP BY 1, 2, 3;

-- Auto-refresh policy (every 15 minutes)
SELECT add_continuous_aggregate_policy('detector_volume_hourly_cagg',
    start_offset => INTERVAL '2 hours',
    end_offset => INTERVAL '0 minutes',
    schedule_interval => INTERVAL '15 minutes'
);
```

**Advantages**:
- Incremental updates (only processes new data)
- Automatic (no APScheduler job needed)
- Real-time (15 min lag configurable to 1 min)
- Concurrent refresh (no locking)

**Designed continuous aggregates (not yet implemented):**

```sql
-- Signal event counts (hourly rollup)
CREATE MATERIALIZED VIEW signal_event_count_hourly
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', event_time) AS hour_start,
    signal_id,
    COUNT(*) AS event_count,
    COUNT(DISTINCT event_code) AS unique_event_codes
FROM controller_event_log
GROUP BY time_bucket('1 hour', event_time), signal_id
WITH NO DATA;

SELECT add_continuous_aggregate_policy('signal_event_count_hourly',
    start_offset => INTERVAL '2 hours',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour');

-- Preemption events (15-minute buckets)
CREATE MATERIALIZED VIEW preemption_15min
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('15 minutes', event_time) AS bin_start,
    signal_id,
    event_param AS preempt_number,
    COUNT(*) FILTER (WHERE event_code = 102) AS preempt_requests,
    COUNT(*) FILTER (WHERE event_code = 105) AS preempt_services
FROM controller_event_log
WHERE event_code IN (102, 105)
GROUP BY time_bucket('15 minutes', event_time), signal_id, event_param
WITH NO DATA;

SELECT add_continuous_aggregate_policy('preemption_15min',
    start_offset => INTERVAL '1 hour',
    end_offset => INTERVAL '15 minutes',
    schedule_interval => INTERVAL '15 minutes');
```

```sql
-- Approach speed (15-minute bins, 15th/85th percentile)
-- Maps to ATSPM 5.x: ApproachSpeedAggregation
CREATE MATERIALIZED VIEW approach_speed_15min
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('15 minutes', event_time) AS bin_start,
    signal_id,
    event_param AS detector_channel,
    COUNT(*) AS speed_volume,
    SUM(event_param) AS summed_speed,
    PERCENTILE_CONT(0.15) WITHIN GROUP (ORDER BY event_param) AS speed_15th,
    PERCENTILE_CONT(0.85) WITHIN GROUP (ORDER BY event_param) AS speed_85th
FROM controller_event_log
WHERE event_code = 82  -- Speed events via detector ON
GROUP BY time_bucket('15 minutes', event_time), signal_id, event_param
WITH NO DATA;

SELECT add_continuous_aggregate_policy('approach_speed_15min',
    start_offset => INTERVAL '1 hour',
    end_offset => INTERVAL '15 minutes',
    schedule_interval => INTERVAL '15 minutes');

-- Phase cycle (15-minute bins, green/yellow/red time)
-- Maps to ATSPM 5.x: PhaseCycleAggregation
CREATE MATERIALIZED VIEW phase_cycle_15min
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('15 minutes', event_time) AS bin_start,
    signal_id,
    event_param AS phase_number,
    COUNT(*) FILTER (WHERE event_code = 1) AS total_green_to_green_cycles,
    COUNT(*) FILTER (WHERE event_code = 7) AS phase_begin_count,
    SUM(CASE WHEN event_code = 1 THEN 1 ELSE 0 END) AS green_count,
    SUM(CASE WHEN event_code = 8 THEN 1 ELSE 0 END) AS yellow_count,
    SUM(CASE WHEN event_code = 9 THEN 1 ELSE 0 END) AS red_count
FROM controller_event_log
WHERE event_code IN (1, 7, 8, 9, 10, 11)
GROUP BY time_bucket('15 minutes', event_time), signal_id, event_param
WITH NO DATA;

SELECT add_continuous_aggregate_policy('phase_cycle_15min',
    start_offset => INTERVAL '1 hour',
    end_offset => INTERVAL '15 minutes',
    schedule_interval => INTERVAL '15 minutes');

-- Left turn gap (15-minute bins, 11 gap duration bins)
-- Maps to ATSPM 5.x: PhaseLeftTurnGapAggregation
-- Gap bins: 1=0-1s, 2=1-2s, 3=2-3s, 4=3-4s, 5=4-5s, 6=5-6s,
--           7=6-7s, 8=7-8s, 9=8-9s, 10=9-10s, 11=10s+
CREATE MATERIALIZED VIEW phase_left_turn_gap_15min
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('15 minutes', event_time) AS bin_start,
    signal_id,
    event_param AS phase_number,
    COUNT(*) AS total_gaps,
    COUNT(*) FILTER (WHERE event_code = 4) AS gap_outs
FROM controller_event_log
WHERE event_code IN (1, 4, 5, 6, 82)
GROUP BY time_bucket('15 minutes', event_time), signal_id, event_param
WITH NO DATA;

SELECT add_continuous_aggregate_policy('phase_left_turn_gap_15min',
    start_offset => INTERVAL '1 hour',
    end_offset => INTERVAL '15 minutes',
    schedule_interval => INTERVAL '15 minutes');

-- Phase pedestrian (15-minute bins, walks/calls/delay)
-- Maps to ATSPM 5.x: AggregateSignalPedDelay
CREATE MATERIALIZED VIEW phase_pedestrian_15min
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('15 minutes', event_time) AS bin_start,
    signal_id,
    event_param AS phase_number,
    COUNT(*) FILTER (WHERE event_code = 45) AS ped_calls,
    COUNT(*) FILTER (WHERE event_code = 21) AS ped_walks,
    COUNT(*) FILTER (WHERE event_code = 22) AS ped_clearances,
    COUNT(*) FILTER (WHERE event_code = 24) AS ped_dark
FROM controller_event_log
WHERE event_code IN (21, 22, 24, 45)
GROUP BY time_bucket('15 minutes', event_time), signal_id, event_param
WITH NO DATA;

SELECT add_continuous_aggregate_policy('phase_pedestrian_15min',
    start_offset => INTERVAL '1 hour',
    end_offset => INTERVAL '15 minutes',
    schedule_interval => INTERVAL '15 minutes');

-- Priority (15-minute bins, requests/early green/extended green)
-- Maps to ATSPM 5.x: PriorityAggregation
CREATE MATERIALIZED VIEW priority_15min
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('15 minutes', event_time) AS bin_start,
    signal_id,
    event_param AS priority_number,
    COUNT(*) FILTER (WHERE event_code = 112) AS priority_requests,
    COUNT(*) FILTER (WHERE event_code = 113) AS priority_service_early_green,
    COUNT(*) FILTER (WHERE event_code = 114) AS priority_service_extended_green
FROM controller_event_log
WHERE event_code IN (112, 113, 114)
GROUP BY time_bucket('15 minutes', event_time), signal_id, event_param
WITH NO DATA;

SELECT add_continuous_aggregate_policy('priority_15min',
    start_offset => INTERVAL '1 hour',
    end_offset => INTERVAL '15 minutes',
    schedule_interval => INTERVAL '15 minutes');

-- Yellow/Red activations (15-minute bins)
-- Maps to ATSPM 5.x: ApproachYellowRedActivationAggregation
CREATE MATERIALIZED VIEW yellow_red_activation_15min
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('15 minutes', event_time) AS bin_start,
    signal_id,
    event_param AS phase_number,
    COUNT(*) FILTER (WHERE event_code = 1) AS cycles,
    COUNT(*) FILTER (WHERE event_code = 8) AS yellow_activations,
    COUNT(*) FILTER (WHERE event_code = 9) AS red_activations
FROM controller_event_log
WHERE event_code IN (1, 8, 9, 10, 82)
GROUP BY time_bucket('15 minutes', event_time), signal_id, event_param
WITH NO DATA;

SELECT add_continuous_aggregate_policy('yellow_red_activation_15min',
    start_offset => INTERVAL '1 hour',
    end_offset => INTERVAL '15 minutes',
    schedule_interval => INTERVAL '15 minutes');
```

**No APScheduler jobs needed** - TimescaleDB handles everything

---

## MS-SQL / Oracle / MySQL (Manual via APScheduler)

### APScheduler Jobs

For databases without continuous aggregates, registered scheduler jobs
delete stale rows within a sliding lookback window and re-aggregate from
raw `controller_event_log` events. All SQL is generated via `DatabaseFacade`
helpers so that it works across PostgreSQL, MS-SQL, Oracle, and MySQL.

Volume/occupancy/split-failure/delay/coordination/phase-termination jobs
live in `tsigma/scheduler/jobs/aggregate.py`. Cycle aggregate jobs
(`agg_cycle_boundary`, `agg_cycle_detector_arrival`, `agg_cycle_summary_15min`)
live in `tsigma/scheduler/jobs/cycle_aggregate.py`.

```python
# tsigma/scheduler/jobs/aggregate.py

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from tsigma.config import settings
from tsigma.database.db import db_facade
from tsigma.scheduler.registry import JobRegistry


@JobRegistry.register(name="agg_detector_volume", trigger="cron", minute="*/15")
async def agg_detector_volume(session: AsyncSession) -> None:
    """Aggregate hourly detector ON counts per signal/channel."""
    if await _should_skip(session):
        return

    hours = settings.aggregation_lookback_hours
    bucket = db_facade.time_bucket("event_time", "hour")
    predicate = db_facade.lookback_predicate("event_time", hours)

    # PostgreSQL uses FILTER (WHERE ...); other dialects use CASE
    if db_facade.db_type == "postgresql":
        sql = f"""
            INSERT INTO detector_volume_hourly
                (signal_id, detector_channel, hour_start, volume, activations)
            SELECT
                signal_id,
                event_param AS detector_channel,
                {bucket} AS hour_start,
                COUNT(*) FILTER (WHERE event_code = 82) AS volume,
                COUNT(*) FILTER (WHERE event_code = 81) AS activations
            FROM controller_event_log
            WHERE event_code IN (81, 82)
              AND {predicate}
            GROUP BY signal_id, event_param, {bucket}
        """
    else:
        sql = f"""
            INSERT INTO detector_volume_hourly
                (signal_id, detector_channel, hour_start, volume, activations)
            SELECT
                signal_id,
                event_param AS detector_channel,
                {bucket} AS hour_start,
                SUM(CASE WHEN event_code = 82 THEN 1 ELSE 0 END) AS volume,
                SUM(CASE WHEN event_code = 81 THEN 1 ELSE 0 END) AS activations
            FROM controller_event_log
            WHERE event_code IN (81, 82)
              AND {predicate}
            GROUP BY signal_id, event_param, {bucket}
        """

    await _refresh_aggregate(
        session,
        table="detector_volume_hourly",
        time_column="hour_start",
        insert_sql=sql,
    )
```

The `_refresh_aggregate()` helper performs the delete-and-reinsert pattern:

```python
async def _refresh_aggregate(
    session: AsyncSession,
    *,
    table: str,
    time_column: str,
    insert_sql: str,
) -> None:
    """Delete-and-reinsert within the lookback window."""
    hours = settings.aggregation_lookback_hours
    delete_sql = db_facade.delete_window_sql(table, time_column, hours)
    await session.execute(text(delete_sql))
    await session.execute(text(insert_sql))
```

### Registered Aggregate Jobs

All jobs use `@JobRegistry.register` with `trigger="cron", minute="*/15"`:

| Job Name | Target Table | Source Events |
|----------|-------------|---------------|
| `agg_detector_volume` | `detector_volume_hourly` | event_code 81 (OFF), 82 (ON) |
| `agg_detector_occupancy` | `detector_occupancy_hourly` | event_code 81, 82 |
| `agg_split_failure` | `split_failure_hourly` | event_code 1 (green), 5 (max-out) |
| `agg_approach_delay` | `approach_delay_15min` | event_code 1, 82 |
| `agg_arrival_on_red` | `arrival_on_red_hourly` | event_code 1, 82 |
| `agg_coordination_quality` | `coordination_quality_hourly` | event_code 1 (phase 2 green) |
| `agg_phase_termination` | `phase_termination_hourly` | event_code 1, 4, 5, 6 |
| `agg_cycle_boundary` | `cycle_boundary` | event_code 1, 4, 5, 6, 8, 9, 10 |
| `agg_cycle_detector_arrival` | `cycle_detector_arrival` | event_code 82 + cycle_boundary |
| `agg_cycle_summary_15min` | `cycle_summary_15min` | cycle_boundary + cycle_detector_arrival |

**Designed (not yet implemented):**

| Job Name | Target Table | Source Events |
|----------|-------------|---------------|
| `agg_signal_event_count` | `signal_event_count_hourly` | all event codes |
| `agg_preemption` | `preemption_15min` | event_code 102 (request), 105 (entry) |
| `agg_approach_speed` | `approach_speed_15min` | event_code 82 (speed via detector) |
| `agg_phase_cycle` | `phase_cycle_15min` | event_code 1, 7, 8, 9, 10, 11 |
| `agg_left_turn_gap` | `phase_left_turn_gap_15min` | event_code 1, 4, 5, 6, 82 |
| `agg_phase_pedestrian` | `phase_pedestrian_15min` | event_code 21, 22, 24, 45 |
| `agg_priority` | `priority_15min` | event_code 112, 113, 114 |
| `agg_yellow_red_activation` | `yellow_red_activation_15min` | event_code 1, 8, 9, 10, 82 |

### Auto-Detection Logic

At first run, each job checks for TimescaleDB:

```python
# tsigma/scheduler/jobs/aggregate.py

_timescaledb_checked: bool = False
_timescaledb_active: bool = False

async def _should_skip(session: AsyncSession) -> bool:
    """Return True if TimescaleDB continuous aggregates handle this."""
    global _timescaledb_checked, _timescaledb_active

    if not settings.aggregation_enabled:
        return True

    if not _timescaledb_checked:
        _timescaledb_active = await db_facade.has_timescaledb(session)
        _timescaledb_checked = True
        if _timescaledb_active:
            logger.info(
                "TimescaleDB detected -- aggregation jobs disabled "
                "(continuous aggregates handle this)"
            )

    return _timescaledb_active
```

---

## Database-Specific SQL Syntax

### Time Bucketing

The `DialectHelper.time_bucket()` method generates dialect-specific SQL.
DatabaseFacade proxies these methods to DialectHelper — use `db_facade.time_bucket()` etc.

| Database | Hourly Bucket | 15-Minute Bucket |
|----------|---------------|-----------------|
| **PostgreSQL + TimescaleDB** | `time_bucket('1 hour', event_time)` | `time_bucket('15 minutes', event_time)` |
| **MS-SQL** | `DATEADD(hour, DATEDIFF(hour, 0, event_time), 0)` | `DATEADD(minute, (DATEDIFF(minute, 0, event_time) / 15) * 15, 0)` |
| **Oracle** | `TRUNC(event_time, 'hour')` | `TRUNC(event_time, 'MI')` |
| **MySQL** | `DATE_FORMAT(event_time, '%Y-%m-%d %H:00:00')` | Custom expression |

### Lookback Window

The `DialectHelper.lookback_predicate()` and `DialectHelper.delete_window_sql()` methods
generate dialect-specific SQL for the sliding window:

| Database | WHERE Predicate |
|----------|----------------|
| **PostgreSQL** | `event_time >= NOW() - INTERVAL 'N hours'` |
| **MS-SQL** | `event_time >= DATEADD(hour, -N, GETUTCDATE())` |
| **Oracle** | `event_time >= SYSTIMESTAMP - INTERVAL 'N' HOUR` |
| **MySQL** | `event_time >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL N HOUR)` |

---

## Performance Comparison

### PostgreSQL + TimescaleDB (Continuous Aggregates)

Continuous aggregates provide significantly faster reads than raw event queries. TimescaleDB supports incremental refresh, meaning only new data is processed on each refresh cycle. Query performance scales with the volume of aggregated data, not raw events.

**Advantage**: Incremental refresh (only new data processed)

---

### MS-SQL / Oracle / MySQL (APScheduler Jobs)

APScheduler-based aggregation performs a full re-aggregation within the lookback window on each cycle. Read performance is fast since results are pre-computed, but refresh cost is higher than TimescaleDB's incremental approach.

**Disadvantage**: Full re-aggregation every 15 minutes (not incremental)

**Mitigation**: Only refresh within lookback window (configurable via `settings.aggregation_lookback_hours`)

---

## Configuration

### Enable/Disable Aggregation Jobs

```env
# .env

# Aggregation scheduler
TSIGMA_AGGREGATION_ENABLED=true
TSIGMA_AGGREGATION_LOOKBACK_HOURS=2

# Database type (auto-detects TimescaleDB)
DB_TYPE=postgresql  # or mssql, oracle, mysql
```

---

## Deployment Scenarios

### Scenario 1: PostgreSQL + TimescaleDB (Recommended)

```yaml
# docker-compose.yml
services:
  postgres:
    image: timescale/timescaledb:latest-pg16
    environment:
      POSTGRES_DB: tsigma
```

**Aggregation**: Automatic (continuous aggregates)
**APScheduler**: Jobs auto-disable via `_should_skip()`
**Performance**: Best (incremental updates)

---

### Scenario 2: MS-SQL (Enterprise)

```env
DB_TYPE=mssql
MSSQL_HOST=sql-server.example.com
TSIGMA_AGGREGATION_ENABLED=true
```

**Aggregation**: APScheduler jobs (delete + reinsert)
**APScheduler**: Required
**Performance**: Good (full refresh every 15 min)

---

### Scenario 3: Oracle (Government/Legacy)

```env
DB_TYPE=oracle
ORACLE_SERVICE_NAME=tsigma
TSIGMA_AGGREGATION_ENABLED=true
```

**Aggregation**: APScheduler jobs
**APScheduler**: Required
**Performance**: Good

---

### Scenario 4: MySQL (Small Agencies)

```env
DB_TYPE=mysql
MYSQL_HOST=localhost
TSIGMA_AGGREGATION_ENABLED=true
```

**Aggregation**: APScheduler jobs (only option for MySQL)
**APScheduler**: Required
**Performance**: Acceptable for <1,000 signals

---

## Recommendation

**For new deployments**: Use **PostgreSQL + TimescaleDB**
- Best performance (incremental updates)
- No APScheduler overhead
- Continuous aggregates maintained automatically

**For existing infrastructure**:
- **MS-SQL**: Use APScheduler jobs (works fine for <5,000 signals)
- **Oracle**: Use APScheduler jobs
- **MySQL**: Use PostgreSQL instead if possible (MySQL lacks features)
