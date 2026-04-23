# Analytics Implementation Guide

**Purpose**: Decision guide for implementing new analytics metrics in TSIGMA.

**Last Updated**: 2026-03-03

---

## Quick Decision Tree

```
Is this a time-series rollup?
(hourly/daily aggregation of event counts/averages)
    ↓ YES
Use TimescaleDB Continuous Aggregate
    Examples: hourly volume, daily occupancy, hourly split failures

    ↓ NO

Does it need custom parameters?
(user selects detector, time range, thresholds)
    ↓ YES
Use API Endpoint (on-demand query)
    Examples: PCD chart, gap analysis, custom date ranges

    ↓ NO

Does it involve external systems or complex logic?
(email alerts, business rules, multi-step workflow)
    ↓ YES
Use APScheduler Job
    Examples: watchdog scans with email alerts, health scoring with caching

    ↓ NO

Use API Endpoint (simple query)
    Examples: preemption lookup, real-time status check
```

---

## When to Use Each Approach

### ✅ Use Continuous Aggregates When:

- Metric is **time-based rollup** (hourly, daily, 15-min intervals)
- Same aggregation **used repeatedly** (dashboard displays, multiple reports)
- Data **doesn't need to be real-time** (15 min lag acceptable)
- Query involves **heavy SUM/COUNT/AVG** across millions of events
- Results are **always the same** for a given time window (deterministic)

**Examples**:
- ✅ Hourly detector volumes (sum of detector hits per hour)
- ✅ Daily split failure counts (count split failures per day)
- ✅ 15-minute approach delay (average delay every 15 min)
- ❌ PCD chart (user picks custom time window) - Use API instead

---

### ✅ Use API Endpoints When:

- User provides **custom parameters** (date range, detector selection, thresholds)
- Query is **fast** (TimescaleDB partitioning and indexes keep these efficient)
- Results are **not reused** (different every request)
- Need **real-time data** (no caching delay)
- Simple **SELECT query** with WHERE clause

**Examples**:
- ✅ PCD chart data (user selects specific 15-minute window)
- ✅ Gap analysis for specific detector (user picks detector + date)
- ✅ Preemption lookup (fast query, infrequent events)
- ❌ Hourly volumes for dashboard (same query every page load) - Use CA instead

---

### ✅ Use APScheduler When:

- Involves **external systems** (email, SMTP, webhooks, SNMP traps)
- Requires **complex business logic** (if/then rules, scoring algorithms)
- Result needs **caching** (compute once per 15 min, serve many times)
- **Multi-step workflow** (query → compute → cache → alert)
- **Not SQL-expressible** (Python logic required)

**Examples**:
- ✅ Watchdog scan (check thresholds → send email alerts)
- ✅ Signal health score (multi-factor algorithm → cache result)
- ✅ Daily report generation (PDF/Excel export)
- ❌ Hourly volume aggregation (pure SQL) - Use Continuous Aggregate instead

---

## Implementation Patterns

### Pattern 1: Continuous Aggregate + API Endpoint (Most Common)

**Best for**: Dashboard metrics (volume, occupancy, split failures)

```sql
-- 1. Create continuous aggregate (database level)
CREATE MATERIALIZED VIEW detector_volume_hourly
WITH (timescaledb.continuous) AS
SELECT
    signal_id,
    event_param AS detector_channel,
    time_bucket('1 hour', event_time) AS hour_start,
    COUNT(*) FILTER (WHERE event_code = 82) AS volume,
    COUNT(*) FILTER (WHERE event_code = 81) AS activations
FROM controller_event_log
WHERE event_code IN (81, 82)
GROUP BY signal_id, event_param, time_bucket('1 hour', event_time);

SELECT add_continuous_aggregate_policy('detector_volume_hourly',
    start_offset => INTERVAL '2 hours',
    end_offset => INTERVAL '1 minute',
    schedule_interval => INTERVAL '15 minutes'
);
```

```python
# 2. Create API endpoint (reads from aggregate)
@router.get("/volume/hourly")
async def get_hourly_volume(signal_id: str, start: datetime, end: datetime,
                            session: AsyncSession = Depends(get_session)):
    """Fast read from continuous aggregate."""
    result = await session.execute(
        select(DetectorVolumeHourly).where(
            DetectorVolumeHourly.signal_id == signal_id,
            DetectorVolumeHourly.hour_start.between(start, end)
        )
    )
    return result.scalars().all()
```

**Performance**: Continuous aggregates are significantly faster than querying raw events

---

### Pattern 2: API Endpoint Only (Custom Queries)

**Best for**: User-driven analysis (PCD, custom gap analysis)

```python
@router.get("/pcd")
async def get_pcd_data(
    signal_id: str, phase: int, start: datetime, end: datetime,
    session: AsyncSession = Depends(get_session),
):
    """
    Query raw events for PCD chart.

    Fast with TimescaleDB partitioning:
    - Query only relevant partition
    - Index on (signal_id, event_time DESC)
    - Returns in 1-2 seconds
    """
    result = await session.execute(
        select(ControllerEventLog).where(
            ControllerEventLog.signal_id == signal_id,
            ControllerEventLog.event_param == phase,
            ControllerEventLog.event_code.in_([1, 8, 9, 10]),
            ControllerEventLog.event_time.between(start, end),
        ).order_by(ControllerEventLog.event_time)
    )
    return result.scalars().all()
```

**Performance**: 1-2 seconds (acceptable for user-driven request)

---

### Pattern 3: APScheduler + API Endpoint (Cached Results)

**Best for**: Complex analytics with caching (health scores, watchdog)

```python
# 1. APScheduler computes and caches
@scheduler.scheduled_job('interval', minutes=30)
async def compute_signal_health():
    """Compute health scores for all intersections."""
    async with get_session() as session:
        for intersection_id in all_intersections:
            # Run complex scoring algorithm
            health = await score_signal_health(
                session, intersection_id,
                datetime.now() - timedelta(hours=1),
                datetime.now()
            )

            # Cache result
            await session.merge(SignalHealthCache(
                intersection_id=intersection_id,
                health_score=health['health_score'],
                last_computed=datetime.now(),
                ...
            ))
        await session.commit()

# 2. API reads cached results
@router.get("/health/{signal_id}")
async def get_signal_health(signal_id: str,
                            session: AsyncSession = Depends(get_session)):
    """Read cached health score."""
    return await session.execute(
        select(SignalHealthCache).where(
            SignalHealthCache.signal_id == signal_id
        )
    ).scalar_one()
```

**Performance**: Pre-computed and cached; reads are near-instant compared to on-demand calculation

---

## Custom Analytics Jobs - Database Guidelines

### Plugin-Based Job Architecture

Custom analytics jobs use the **JobRegistry pattern** (see [ARCHITECTURE.md § 9](ARCHITECTURE.md#9-background-jobs--scheduling)). Each job is a self-contained module that:

1. Registers itself via `@JobRegistry.register()` decorator
2. Auto-discovered on import (no core code changes)
3. Can be included or excluded from deployment

**Example:**
```python
# tsigma/scheduler/jobs/custom_corridor_metrics.py

from tsigma.scheduler.registry import JobRegistry

@JobRegistry.register(name="corridor_metrics", trigger="cron", hour="4")
async def compute_corridor_metrics():
    """Custom corridor analysis - runs daily at 4 AM."""
    async with get_session() as session:
        # Your custom logic here
        ...
```

### Database Access Rules

**CRITICAL:** Custom jobs can create their own tables but **MUST NOT modify base TSIGMA tables**.

#### ✅ Allowed:
- Create custom tables (use `custom_` prefix to avoid conflicts)
- Insert/update/delete in your own tables
- **Read** from TSIGMA base tables via repositories
- Create indexes on your own tables
- Create materialized views that read from TSIGMA tables

#### ❌ Prohibited:
- Modify TSIGMA core tables (`controller_event_log`, `signal`, `detector`, etc.)
- Delete from TSIGMA core tables
- Alter TSIGMA table schemas
- Drop TSIGMA indexes or constraints

#### Example - Custom Analytics Cache Table:

**CRITICAL:** Migrations MUST be **idempotent** - safe to run multiple times. See [DATABASE.md § Migrations](DATABASE.md#migrations-alembic) for complete guide.

```python
# migrations/versions/20260305_custom_analytics.py

def _table_exists(name: str) -> bool:
    """Check if table exists (idempotency guard)."""
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
    """Check if index exists (idempotency guard)."""
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

def upgrade():
    """Create custom analytics cache table."""
    # ✅ IDEMPOTENT: Check before creating
    if not _table_exists("custom_corridor_quality"):
        op.create_table(
            "custom_corridor_quality",
            sa.Column("id", sa.UUID(), primary_key=True, default=uuid4),
            sa.Column("corridor_id", sa.UUID(), nullable=False),
            sa.Column("date", sa.Date(), nullable=False),
            sa.Column("quality_score", sa.Float()),
            sa.Column("total_signals", sa.Integer()),
            sa.Column("signals_coordinated", sa.Integer()),
            sa.Column("avg_offset_error_ms", sa.Float()),
            sa.Column("computed_at", sa.DateTime(timezone=True)),
            sa.UniqueConstraint("corridor_id", "date", name="uq_corridor_quality")
        )

    # ✅ IDEMPOTENT: Check before creating index
    if not _index_exists("ix_custom_corridor_quality_corridor_date"):
        op.create_index(
            "ix_custom_corridor_quality_corridor_date",
            "custom_corridor_quality",
            ["corridor_id", "date"]
        )

def downgrade():
    # ✅ NEVER drop tables - write forward migration instead
    raise NotImplementedError("Additive only - write new migration to remove")
```

**Key rules:**
1. **IDEMPOTENT** - Use `if not _table_exists()` and `if not _index_exists()` guards
2. **Additive only** - Never drop tables, columns, or user data
3. **No destructive rollbacks** - `downgrade()` must raise `NotImplementedError`
4. **Safe re-runs** - Running `alembic upgrade head` twice is a no-op

#### Custom Job Using Its Own Table:

```python
# tsigma/scheduler/jobs/corridor_quality.py

from tsigma.scheduler.registry import JobRegistry
from tsigma.dependencies import get_session
from sqlalchemy import select, insert

@JobRegistry.register(name="corridor_quality", trigger="cron", hour="2")
async def compute_corridor_quality():
    """
    Analyze corridor coordination quality.
    Reads from TSIGMA tables, writes to custom_corridor_quality.
    """
    async with get_session() as session:
        # ✅ ALLOWED: Read from TSIGMA base tables
        corridors = await session.execute(
            select(Route).where(Route.enabled == True)
        )

        for corridor in corridors.scalars():
            # ✅ ALLOWED: Read event data via query
            events = await session.execute(
                select(ControllerEventLog).where(
                    ControllerEventLog.signal_id.in_(corridor.signal_ids),
                    ControllerEventLog.event_time >= yesterday,
                )
            )

            # Compute custom metrics
            quality = analyze_corridor_coordination(events.scalars().all())

            # ✅ ALLOWED: Write to your custom table
            await session.execute(
                insert(CustomCorridorQuality).values(
                    corridor_id=corridor.id,
                    date=date.today(),
                    quality_score=quality["score"],
                    total_signals=quality["total_signals"],
                    signals_coordinated=quality["coordinated"],
                    avg_offset_error_ms=quality["avg_error"],
                    computed_at=datetime.now(UTC)
                )
            )

        await session.commit()
```

### Why These Rules?

| Reason | Impact |
|--------|--------|
| **Data integrity** | Core TSIGMA tables remain consistent and validated |
| **Upgrade safety** | TSIGMA upgrades won't conflict with custom schema changes |
| **Multi-tenancy** | Custom tables can be tenant-scoped independently |
| **Debugging** | Clear separation between core and custom data |
| **Rollback safety** | Custom job failures don't corrupt base system |

---

## Specific Metrics Breakdown

### PCD (Purdue Coordination Diagram)

**Method**: ✅ API Endpoint (on-demand query)

**Rationale**:
- User selects specific time window (15 min to 24 hours)
- Results not reused (different every request)
- Query is fast (1-2 seconds with partitioning)
- No aggregation needed (raw phase events)

**Implementation**: `GET /api/v1/analytics/pcd`

---

### Approach Volume

**Method**: ✅ Continuous Aggregate + API Endpoint

**Rationale**:
- Heavy aggregation (sum all detector hits per hour)
- Reused in multiple dashboards
- TimescaleDB incrementally updates hourly

**Implementation**:
```sql
CREATE MATERIALIZED VIEW approach_volume_hourly
WITH (timescaledb.continuous) AS
SELECT
    signal_id,
    event_param AS detector_channel,
    time_bucket('1 hour', event_time) AS hour_start,
    COUNT(*) AS volume
FROM controller_event_log
WHERE event_code = 82  -- DETECTOR_ON
GROUP BY signal_id, event_param, time_bucket('1 hour', event_time);
```

---

### Split Failures

**Method**: ✅ Continuous Aggregate + APScheduler + API Endpoint (All Three!)

**Rationale**:
- Continuous Aggregate: Hourly split failure counts (auto-updated)
- APScheduler: Daily summary + alert if >20% failures
- API Endpoint: Read from aggregate for dashboard, custom queries for details

**Implementation**:
```sql
-- Continuous aggregate
CREATE MATERIALIZED VIEW split_failure_hourly ...

-- APScheduler job
@scheduler.scheduled_job('cron', hour='2')
async def daily_split_failure_summary():
    # Read from split_failure_hourly aggregate
    # Compute daily percentages
    # Send alert if >20% failure rate
    # Cache in split_failure_daily table

-- API endpoint
@app.get("/api/v1/analytics/split_failures")
async def get_split_failures(...):
    # Read from split_failure_hourly (fast)
```

---

### Stuck Detectors

**Method**: ✅ APScheduler + API Endpoint

**Rationale**:
- APScheduler: Scan every 15 min, cache results, send alerts
- API Endpoint: Read cached results (dashboard) or run on-demand (custom)
- Continuous Aggregate: Not suitable (not a time-series rollup)

**Implementation**:
```python
# APScheduler scan
@scheduler.scheduled_job('interval', minutes=15)
async def scan_stuck_detectors():
    stuck = await find_stuck_detectors(...)  # Analytics function
    await cache_results(stuck)  # Store in detector_health_status
    if critical_stuck:
        await send_email_alert(...)  # Email integration

# API endpoint
@router.get("/detectors/stuck/{signal_id}")
async def get_stuck_detectors_cached(signal_id: str):
    # Read from detector_health_status table (cached by APScheduler)
    # Fast cached read
```

---

## Summary Recommendation

**For TSIGMA analytics implementation**:

| Component | Count | Purpose |
|-----------|-------|---------|
| **Continuous Aggregates** | 7 views | Hourly/daily rollups (volume, occupancy, splits, delay, speed, arrival on red, coordination) |
| **API Endpoints** | 15 endpoints | Read aggregates + custom queries (PCD, gaps, preemption) |
| **APScheduler Jobs** | 5 jobs | Health scans, watchdog, daily summaries, alerts |

**Total**: 27 components for full ATSPM analytics parity.

See [ARCHITECTURE.md § Analytics Architecture](ARCHITECTURE.md#9-analytics-architecture) for the full three-tier design.