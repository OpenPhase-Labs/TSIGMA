# High-Concurrency Polling (200+ Simultaneous)

**Purpose**: Internal architecture for TSIGMA's high-concurrency polling design.

**Use Case**: Large agencies with burst polling requirements (e.g., GDOT 9,000 signals with staggered schedules)

---

## Architecture

### How It Works

The `CollectorService` orchestrates polling via the scheduler. Each registered polling method (FTP, HTTP, etc.) gets an interval job. On each cycle, the service queries all enabled signals configured for that method, then fans out to semaphore-bounded concurrent polls.

```
SchedulerService (APScheduler)
    ↓
CollectorService._run_poll_cycle(method_name)
    ↓
Query signal table → filter by method
    ↓
asyncio.Semaphore (collector_max_concurrent)
    ↓
200+ Simultaneous FTP/SFTP/HTTP connections
    ↓
9,000 Legacy Controllers
```

**Key**: Python 3.14 free-threaded execution (no GIL) enables true parallel connections.

### Code Path

```python
# tsigma/collection/service.py

class CollectorService:
    def __init__(self, session_factory, settings):
        self._session_factory = session_factory
        self._settings = settings
        self._semaphore = asyncio.Semaphore(settings.collector_max_concurrent)

    async def _run_poll_cycle(self, method_name):
        # Query enabled signals configured for this method
        async with self._session_factory() as session:
            rows = await session.execute(
                select(Signal.signal_id, Signal.ip_address, Signal.signal_metadata)
                .where(Signal.enabled == true())
            )

        # Fan out with semaphore-bounded concurrency
        tasks = []
        for row in rows:
            if row.signal_metadata.get("collection", {}).get("method") == method_name:
                tasks.append(self._process_signal(method, row.signal_id, config))
        await asyncio.gather(*tasks)

    async def _process_signal(self, method, signal_id, config):
        async with self._semaphore:  # Bounds concurrent connections
            await method.poll_once(signal_id, config, self._session_factory)
```

### Per-Signal Configuration

Each signal stores its own collection config in `signal_metadata` JSONB:

```json
{
  "collection": {
    "method": "ftp_pull",
    "protocol": "ftps",
    "username": "admin",
    "password": "***",
    "remote_dir": "/logs",
    "decoder": "asc3"
  }
}
```

Different signals can use different methods, protocols, and decoders — all polled concurrently within the same cycle.

---

## Configuration

### Environment Variables

```env
# Component toggle
TSIGMA_ENABLE_COLLECTOR=true

# Concurrency control
TSIGMA_COLLECTOR_MAX_CONCURRENT=200   # Max simultaneous connections
TSIGMA_COLLECTOR_POLL_INTERVAL=300    # Seconds between poll cycles
```

### Scaling Profiles

| Max Concurrent | CPU Cores | RAM | Use Case |
|----------------|-----------|-----|----------|
| **50** (default) | 8 cores | 4 GB | Small agencies (<1K signals) |
| **100** | 12 cores | 8 GB | Medium agencies (1K-3K signals) |
| **200** | 16 cores | 12 GB | Large agencies (3K-9K signals) |
| **500** | 32 cores | 24 GB | Ultra-fast burst (9K+ signals) |

---

## Python 3.14 Free-Threaded Impact

**Without GIL (Python 3.14+)**:
- 200 workers execute in true parallel
- Effective parallelism: 200 FTP connections
- CPU utilization: ~50-70% across all cores

**With GIL (Python 3.13)**:
- 200 workers blocked by GIL
- Effective parallelism: ~10-20 workers (limited by GIL contention)
- Performance degradation: **10x slower**

**Critical**: Python 3.14+ required for 200+ worker performance.

---

## System Requirements

### For 200 Concurrent

**CPU**: 16-24 cores (Ryzen 9 7950X or Xeon Gold)

**RAM**: 12-16 GB
- Worker tasks: 200 x 10 MB = 2 GB
- File buffers: 200 x 5 MB = 1 GB
- Database connection pool: 100 connections x 10 MB = 1 GB
- OS + Python runtime: 2 GB
- **Total**: ~6-8 GB typical, 12-16 GB peak

**Network**: 1 Gbps Ethernet
- 200 controllers x 2 MB/file = 400 MB/burst
- Sustained: ~50 Mbps average

**Disk**: 500 GB SSD (NVMe recommended)

### For 500 Concurrent

**CPU**: 32-64 cores (Dual Xeon or EPYC)

**RAM**: 24-32 GB

**Network**: 10 Gbps Ethernet (recommended)

---

## Rate Limiting

### Why Rate Limit Per Host?

**Problem**: Multiple controllers behind the same FTP server IP.

Example: 20 controllers at 192.168.1.100

**Without rate limiting**:
- 20 simultaneous FTP connections to single server
- Server overload, timeouts, failed polls

**With semaphore bounding** (`collector_max_concurrent`):
- The global semaphore limits total concurrent connections
- Per-host limiting is handled by the FTP server's own connection limits
- If a server rejects connections, the error is recorded and retried next cycle

---

## Staggered Scheduling

### Problem: Burst vs Sustained

With `collector_poll_interval=3600` (1 hour):
- 9,000 jobs burst every hour
- All jobs burst at once
- Idle for most of the interval

### Solution: Match the Controller's Rotation Cadence

With `collector_poll_interval=900` (15 minutes — default):
- 9,000 signals polled every 15 minutes
- Matches the typical controller `.dat` file rotation cadence:
  polling any faster returns zero new data on most deployments
- Load distributed across each 15-minute window via the concurrency
  semaphore and per-host backoff
- Controller checkpoint ensures only new data is downloaded each
  cycle regardless of interval

Poll more frequently only when the controller rotates files faster
than 15 minutes (rare) or when an HTTP XML endpoint returns
incremental events continuously.  The `sensor_poll_interval` setting
controls the same cadence for roadside-sensor polling (legacy
radar / LiDAR trace-file pulls); push-driven sensors ignore this
setting entirely.

---

## Monitoring

### Health Check

```bash
curl http://localhost:8080/health
curl http://localhost:8080/ready
```

### Collector Health (programmatic)

```python
# From app.state
health = await app.state.collector.health_check()
# Returns: {"ftp_pull": True, "http_pull": True, ...}
```

### Silent Signal Detection

The `CollectorService` tracks signals that return zero events for N consecutive cycles via `PollingCheckpoint.consecutive_silent_cycles`. After the threshold (`checkpoint_silent_cycles_threshold`, default 3), it investigates and notifies:

- **Poisoned checkpoint**: Auto-recovers (rolls back to server time) + CRITICAL notification
- **Not poisoned**: WARNING notification for operator investigation

---

## Tuning Guide

### Optimal Concurrency

Start with the default concurrency for your agency size (see Scaling Profiles table) and adjust based on observed CPU utilization and poll completion times. Actual throughput depends on deployment hardware, network conditions, and FTP server capacity.

### FTP Server Capacity Test

```bash
# Test max concurrent connections to a controller
for i in {1..20}; do
    (ftp -n <<EOF
open 192.168.1.100
user atspm secret
quit
EOF
) &
done
wait

# If server rejects connections at >10, that's the per-host ceiling
```

---

**Last Updated**: April 2026
