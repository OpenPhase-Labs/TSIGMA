# Deployment

> Part of [TSIGMA Architecture](../ARCHITECTURE.md)

---

## Docker Compose (Standard — Single Container)

All components in one container. Suitable for most DOTs.

```yaml
# docker/docker-compose.yml

services:
  tsigma:
    build:
      context: ..
      dockerfile: Dockerfile
    ports:
      - "8080:8080"
    environment:
      - TSIGMA_PG_HOST=db
      - TSIGMA_PG_PORT=5432
      - TSIGMA_PG_DATABASE=tsigma
      - TSIGMA_PG_USER=tsigma
      - TSIGMA_PG_PASSWORD=${DB_PASSWORD}
      - TSIGMA_ENABLE_API=true
      - TSIGMA_ENABLE_COLLECTOR=true
      - TSIGMA_ENABLE_SCHEDULER=true
      - TSIGMA_LOG_FORMAT=json
    depends_on:
      db:
        condition: service_healthy

  db:
    image: timescale/timescaledb:latest-pg18
    volumes:
      - pgdata:/var/lib/postgresql/data
    environment:
      - POSTGRES_USER=tsigma
      - POSTGRES_PASSWORD=${DB_PASSWORD}
      - POSTGRES_DB=tsigma
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U tsigma"]
      interval: 5s
      timeout: 5s
      retries: 5

volumes:
  pgdata:
```

## Docker Compose (Large Deployment — Split Components)

Same image, different env vars per service.

```yaml
# docker/docker-compose.large.yml

services:
  api:
    image: tsigma:latest
    ports:
      - "8080:8080"
    environment:
      - TSIGMA_PG_HOST=db
      - TSIGMA_ENABLE_API=true
      - TSIGMA_ENABLE_COLLECTOR=false
      - TSIGMA_ENABLE_SCHEDULER=true   # Only one instance runs scheduler
    deploy:
      replicas: 3

  collector-0:
    image: tsigma:latest
    environment:
      - TSIGMA_PG_HOST=db
      - TSIGMA_ENABLE_API=false
      - TSIGMA_ENABLE_COLLECTOR=true
      - TSIGMA_ENABLE_SCHEDULER=false
      - TSIGMA_WORKER_ID=0  # (Planned)
      - TSIGMA_WORKER_COUNT=2  # (Planned)

  collector-1:
    image: tsigma:latest
    environment:
      - TSIGMA_PG_HOST=db
      - TSIGMA_ENABLE_API=false
      - TSIGMA_ENABLE_COLLECTOR=true
      - TSIGMA_ENABLE_SCHEDULER=false
      - TSIGMA_WORKER_ID=1  # (Planned)
      - TSIGMA_WORKER_COUNT=2  # (Planned)

  db:
    image: timescale/timescaledb:latest-pg18
    volumes:
      - pgdata:/var/lib/postgresql/data
    environment:
      - POSTGRES_USER=tsigma
      - POSTGRES_PASSWORD=${DB_PASSWORD}
      - POSTGRES_DB=tsigma

volumes:
  pgdata:
```

## Deployment Modes

TSIGMA supports two deployment modes that differ in data storage tier availability.

### On-Prem (Hot → Warm → Cold)

On-Prem deployments have full access to all three storage tiers, including Parquet cold storage with configurable endpoints.

```yaml
# docker/docker-compose.onprem.yml (additions to standard compose)

services:
  tsigma:
    environment:
      # Storage tiers
      - TSIGMA_STORAGE_WARM_AFTER=7 days
      - TSIGMA_STORAGE_COLD_ENABLED=true
      - TSIGMA_STORAGE_COLD_AFTER=6 months
      - TSIGMA_STORAGE_COLD_FORMAT=parquet
      - TSIGMA_STORAGE_RETENTION=2 years
      # Cold endpoint — filesystem
      - TSIGMA_STORAGE_BACKEND=filesystem
      - TSIGMA_STORAGE_COLD_PATH=/var/lib/tsigma/cold
    volumes:
      - cold_storage:/var/lib/tsigma/cold

volumes:
  cold_storage:
```

For S3-compatible cold storage:

```yaml
services:
  tsigma:
    environment:
      - TSIGMA_STORAGE_BACKEND=s3
      - TSIGMA_STORAGE_S3_BUCKET=tsigma-cold
      - TSIGMA_STORAGE_S3_REGION=us-east-1
      - TSIGMA_STORAGE_S3_ENDPOINT=  # set for MinIO/Ceph
```

### SaaS (Hot → Warm)

SaaS deployments disable cold storage. All event data stays in TimescaleDB (hot or compressed).

```yaml
# docker/docker-compose.saas.yml (additions to standard compose)

services:
  tsigma:
    environment:
      - TSIGMA_STORAGE_WARM_AFTER=7 days
      - TSIGMA_STORAGE_COLD_ENABLED=false
      - TSIGMA_STORAGE_RETENTION=1 year
```

---

## Environment Variables Reference

### Core

| Variable | Default | Description |
|----------|---------|-------------|
| `TSIGMA_ENABLE_API` | `true` | Enable REST API, GraphQL, Web UI |
| `TSIGMA_ENABLE_COLLECTOR` | `true` | Enable controller polling and ingestion |
| `TSIGMA_ENABLE_SCHEDULER` | `true` | Enable APScheduler (view refresh, watchdog) |
| `TSIGMA_WORKER_ID` | `0` | Worker index for signal partitioning (Planned) |
| `TSIGMA_WORKER_COUNT` | `1` | Total number of collector workers (Planned) |
| `TSIGMA_PG_HOST` | `localhost` | PostgreSQL host |
| `TSIGMA_PG_PORT` | `5432` | PostgreSQL port |
| `TSIGMA_PG_DATABASE` | `tsigma` | Database name |
| `TSIGMA_PG_USER` | `tsigma` | Database user |
| `TSIGMA_PG_PASSWORD` | _(required)_ | Database password |
| `TSIGMA_COLLECTOR_POLL_INTERVAL` | `300` | Controller poll interval (seconds) |
| `TSIGMA_REFRESH_SCHEDULE` | `*/15 * * * *` | Materialized view refresh cron (Planned — not yet implemented) |
| `TSIGMA_WATCHDOG_SCHEDULE` | `0 6 * * *` | Watchdog cron (Planned — not yet implemented) |
| `TSIGMA_LOG_LEVEL` | `INFO` | Log level |
| `TSIGMA_LOG_FORMAT` | `json` | Log format (json or console) |
| `TSIGMA_DEBUG` | `false` | Debug mode |

### Storage Tiers

| Variable | Default | Description |
|----------|---------|-------------|
| `TSIGMA_STORAGE_WARM_AFTER` | `7 days` | Compress chunks older than this interval |
| `TSIGMA_STORAGE_WARM_MAX_DISK` | — | Compress early if hot tier exceeds this size (TimescaleDB only, e.g., `500 GB`) |
| `TSIGMA_STORAGE_WARM_CHECK_INTERVAL` | `5m` | How often to check disk usage for `WARM_MAX_DISK` |
| `TSIGMA_STORAGE_COLD_ENABLED` | `false` | Enable Parquet cold tier (On-Prem only) |
| `TSIGMA_STORAGE_COLD_AFTER` | `6 months` | Export to Parquet after this age |
| `TSIGMA_STORAGE_COLD_FORMAT` | `parquet` | Cold export format |
| `TSIGMA_STORAGE_BACKEND` | `filesystem` | Cold storage backend (`filesystem` or `s3`) |
| `TSIGMA_STORAGE_COLD_PATH` | `/var/lib/tsigma/cold` | Filesystem path for cold storage |
| `TSIGMA_STORAGE_S3_BUCKET` | — | S3 bucket for cold storage |
| `TSIGMA_STORAGE_S3_REGION` | `us-east-1` | S3 region |
| `TSIGMA_STORAGE_S3_ENDPOINT` | — | Custom S3 endpoint (MinIO/Ceph) |
| `TSIGMA_STORAGE_RETENTION` | `2 years` | Drop data entirely after this age |
