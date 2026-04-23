# Large-Scale Polling (9,000+ Signals)

**Purpose**: Guide for deploying TSIGMA polling at scale (e.g., GDOT: 9,000 signals).

**Last Updated**: April 2026

---

## Challenge

**GDOT Deployment**: 9,000 signalized intersections

**Legacy Controllers**:
- Econolite ASC/3 (majority)
- Intelight MaxTime (some)
- Siemens SEPAC (some)
- Peek/McCain ATC (few)

**Event Volume**: ~18 billion events/day (2M events/day x 9,000 signals)

**Polling Frequency**: 5-minute intervals (configurable)

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│              SchedulerService (APScheduler)                  │
│  ┌─────────────────────────────────────────────────────┐    │
│  │  poll_cycle_ftp_pull (interval job)                 │    │
│  │  poll_cycle_http_pull (interval job)                │    │
│  └────────────┬────────────────────────────────────────┘    │
│               │                                              │
│  ┌────────────▼────────────────────────────────────────┐    │
│  │  CollectorService._run_poll_cycle()                 │    │
│  │  Query enabled signals → fan out with semaphore     │    │
│  └────────────┬────────────────────────────────────────┘    │
│               │                                              │
│  ┌────────────▼────────────────────────────────────────┐    │
│  │  asyncio.Semaphore (collector_max_concurrent)       │    │
│  │  Bounds total simultaneous connections              │    │
│  └────────────┬────────────────────────────────────────┘    │
└───────────────┼──────────────────────────────────────────────┘
                │
    ┌───────────┼───────────┐
    ▼           ▼           ▼
┌────────┐ ┌────────┐ ┌────────┐
│ FTP/   │ │ HTTP   │ │ FTP/   │
│ SFTP   │ │ XML    │ │ SFTP   │  (9,000 controllers)
│ #1     │ │ #2     │ │ #N     │
└────────┘ └────────┘ └────────┘
    │           │           │
    ▼           ▼           ▼
┌────────────────────────────────┐
│  Decoders (ASC3/Siemens/etc.) │
└────────────┬───────────────────┘
             │
┌────────────▼───────────────────┐
│  PostgreSQL + TimescaleDB      │
│  - Partitioned by week         │
│  - Idempotent INSERT ON        │
│    CONFLICT DO NOTHING         │
└────────────────────────────────┘
```

---

## Configuration

### Environment Variables

```env
# .env

# Enable the collector
TSIGMA_ENABLE_COLLECTOR=true

# Concurrency — max simultaneous connections across all methods
TSIGMA_COLLECTOR_MAX_CONCURRENT=200

# Poll interval — seconds between poll cycles
TSIGMA_COLLECTOR_POLL_INTERVAL=300

# Checkpoint resilience
TSIGMA_CHECKPOINT_FUTURE_TOLERANCE_SECONDS=300
TSIGMA_CHECKPOINT_SILENT_CYCLES_THRESHOLD=3
```

### Per-Signal Configuration

Each signal stores its own collection config in the `signal_metadata` JSONB column. This means:
- Different signals can use different protocols (FTP, SFTP, HTTP)
- Different decoders per signal (ASC3, MaxTime, Siemens, etc.)
- Add/remove signals without restart (database-driven)
- Bulk operations via SQL

**Example signal configuration:**

```sql
UPDATE signal
SET metadata = jsonb_set(
    COALESCE(metadata, '{}'),
    '{collection}',
    '{
        "method": "ftp_pull",
        "protocol": "ftps",
        "username": "atspm",
        "password": "secret",
        "remote_dir": "/data/logs",
        "decoder": "asc3"
    }'
)
WHERE signal_id = 'GDOT-0001';
```

**Bulk configuration for 9,000 signals:**

```sql
-- Configure all signals with FTP collection
UPDATE signal
SET metadata = jsonb_set(
    COALESCE(metadata, '{}'),
    '{collection}',
    jsonb_build_object(
        'method', 'ftp_pull',
        'protocol', 'ftps',
        'username', 'atspm',
        'password', 'secret',
        'remote_dir', '/data/logs',
        'decoder', 'asc3'
    )
)
WHERE signal_id LIKE 'GDOT-%'
  AND enabled = true;
```

**HTTP/XML signals (Intelight MaxTime):**

```sql
UPDATE signal
SET metadata = jsonb_set(
    COALESCE(metadata, '{}'),
    '{collection}',
    '{
        "method": "http_pull",
        "port": 80,
        "path": "/v1/asclog/xml/full",
        "decoder": "maxtime"
    }'
)
WHERE controller_type_id = (
    SELECT controller_type_id FROM controller_type
    WHERE description = 'Intelight MaxTime'
);
```

---

## Scaling Profiles

| Max Concurrent | CPU Cores | RAM | Use Case |
|----------------|-----------|-----|----------|
| **50** (default) | 8 cores | 4 GB | Small agencies (<1K signals) |
| **100** | 12 cores | 8 GB | Medium agencies (1K-3K signals) |
| **200** | 16 cores | 12 GB | Large agencies (3K-9K signals) |
| **500** | 32 cores | 24 GB | Ultra-fast burst (9K+ signals) |

**Python 3.14+ required** for 200+ concurrent connections. Free-threaded execution (no GIL) enables true parallel I/O.

---

## Horizontal Scaling (Multiple Instances)

**Partition by Region**:
```
TSIGMA Instance 1 (Metro Atlanta: 3,000 signals)
TSIGMA Instance 2 (North Georgia: 3,000 signals)
TSIGMA Instance 3 (South Georgia: 3,000 signals)
    ↓
Shared PostgreSQL Database (TimescaleDB)
```

Each instance only polls signals in its region. Configure by filtering on signal metadata:

```sql
-- Each instance queries only its signals based on region
SELECT signal_id, ip_address, metadata
FROM signal
WHERE enabled = true
  AND metadata->'collection'->>'method' = 'ftp_pull'
  AND region_id = '<region-uuid>';
```

---

## Monitoring

### Health Checks

```bash
# Liveness
curl http://localhost:8080/health

# Readiness (database connected)
curl http://localhost:8080/ready
```

### Polling Status

Check `polling_checkpoint` table for per-signal collection status:

```sql
-- Signals with recent errors
SELECT signal_id, method, consecutive_errors, last_error, last_error_time
FROM polling_checkpoint
WHERE consecutive_errors > 0
ORDER BY consecutive_errors DESC;

-- Silent signals (no events for N cycles)
SELECT signal_id, method, consecutive_silent_cycles, last_successful_poll
FROM polling_checkpoint
WHERE consecutive_silent_cycles >= 3
ORDER BY consecutive_silent_cycles DESC;

-- Overall collection health
SELECT
    method,
    COUNT(*) AS total_signals,
    COUNT(*) FILTER (WHERE consecutive_errors = 0) AS healthy,
    COUNT(*) FILTER (WHERE consecutive_errors > 0) AS erroring,
    COUNT(*) FILTER (WHERE consecutive_silent_cycles >= 3) AS silent,
    AVG(events_ingested) AS avg_events
FROM polling_checkpoint
GROUP BY method;
```

### Automatic Recovery

TSIGMA automatically detects and recovers from **poisoned checkpoints** (controller clock drift that advances the checkpoint past real time). When detected:

1. Checkpoint is rolled back to server time
2. `consecutive_silent_cycles` is reset
3. CRITICAL notification sent to configured providers (email, Slack, Teams)
4. Next poll cycle resumes normal collection

No operator intervention required.

---

## Database Scaling

### TimescaleDB Compression

```sql
-- Enable compression (7-day policy)
ALTER TABLE controller_event_log SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'signal_id',
    timescaledb.compress_orderby = 'event_time DESC'
);

SELECT add_compression_policy('controller_event_log', INTERVAL '7 days');
```

**Result**: 90% storage reduction after 7 days.

### Storage Estimates (9,000 Signals)

**Hot Storage** (3 weeks in PostgreSQL):
```
9,000 signals x 2M events/day x 21 days = 378 billion events
~2.5 TB uncompressed
```

**Warm Storage** (TimescaleDB compressed):
```
~50 GB/week after compression (10:1 ratio)
```

**Cold Archive** (Parquet export):
```
~50 GB/week in Parquet files
Query with DuckDB or Polars when needed
```

---

## Cost Analysis (9,000 Signals)

| Component | Specification | Estimated Cost |
|-----------|--------------|----------------|
| **Collector** | 16 cores, 16 GB RAM | $500/month |
| **Database** | 32 vCPU, 2 TB NVMe SSD | $1,500/month |
| **Cold Storage** | 20 TB S3/object storage | $200/month |
| **Total** | | **~$2,200/month** |

---

## Failure Handling

### Checkpoint Resilience

- **Idempotent ingestion**: `INSERT ON CONFLICT DO NOTHING` — re-ingesting the same events is a safe no-op
- **Non-destructive collection**: TSIGMA never deletes files from controllers
- **File-based checkpoints**: FTP uses file identity (name + size + hash), not event timestamps — a controller with a bad clock cannot poison the checkpoint
- **Event-based checkpoints**: HTTP uses `?since=<timestamp>` with future-date capping

### Error Tracking

Per-signal error state in `polling_checkpoint`:
- `consecutive_errors` — incremented on each failed poll, reset on success
- `last_error` — error message (truncated to 1000 chars)
- `last_error_time` — when the last error occurred

### Notifications

Configure notification providers in `.env`:

```env
TSIGMA_NOTIFICATION_PROVIDERS=email,slack

# Email
TSIGMA_SMTP_HOST=smtp.example.com
TSIGMA_NOTIFICATION_TO_EMAILS=ops@example.com

# Slack
TSIGMA_SLACK_WEBHOOK_URL=https://hooks.slack.com/services/...
```

Alerts are sent for:
- Silent signals (WARNING after N consecutive silent cycles)
- Clock drift detected (WARNING with drift details)
- Poisoned checkpoint auto-recovery (CRITICAL)

---

**Last Updated**: April 2026
