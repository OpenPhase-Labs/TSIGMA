# Deployment

> Part of [TSIGMA Architecture](../ARCHITECTURE.md)

---

## Database Backends & Setup

TSIGMA runs on PostgreSQL (preferred), MS-SQL, Oracle, or MySQL/MariaDB. Time-series
partitioning and aggregate refresh differ by backend:

| Backend | Partitioning + aggregate refresh | Status |
|---------|----------------------------------|--------|
| PostgreSQL **+ TimescaleDB** | Hypertables + continuous aggregates; TimescaleDB policies own refresh | Supported |
| PostgreSQL (no TimescaleDB) | Native declarative range partitioning + `manage_partitions` scheduler job; APScheduler aggregation jobs | Supported |
| MS-SQL / Oracle / MySQL | Native range partitioning + APScheduler aggregation jobs | Supported |

### Schema layout

On PostgreSQL / MS-SQL / Oracle the tables live in four logical schemas —
`config`, `events`, `aggregation`, `identity` — created automatically by the
initial migration. MySQL/MariaDB has no schemas, so everything lands in the
single database. Models are schema-qualified, but the audit-trigger functions
and the `user_role` enum resolve unqualified at runtime, so set the role's
`search_path` once:

```sql
ALTER ROLE tsigma SET search_path = config, events, aggregation, identity, public;
```

### Role, database, and init (all backends)

```sql
CREATE ROLE tsigma LOGIN PASSWORD '<strong-password>';
CREATE DATABASE tsigma OWNER tsigma;
-- then set search_path as above
```

```bash
# .env: TSIGMA_PG_* connection + a non-default TSIGMA_AUTH_ADMIN_PASSWORD
alembic upgrade head
```

### PostgreSQL + TimescaleDB

TimescaleDB is an explicit, installer-declared mode (set `TSIGMA_ENABLE_TIMESCALEDB=true`).
Install the **TSL/Community** package — **not** the Apache/OSS build, which lacks
continuous aggregates and compression:

```bash
# ✅ Community (TSL) — has continuous aggregates + compression
dnf install timescaledb-2-postgresql-18   # pulls timescaledb-2-loader-postgresql-18
# ❌ avoid: timescaledb_18 / timescaledb-2-oss-postgresql-18  (Apache/OSS only)
```

```ini
# postgresql.conf, then restart the server
shared_preload_libraries = 'timescaledb'
```

```sql
CREATE EXTENSION IF NOT EXISTS timescaledb;   -- in the tsigma database
```

With `TSIGMA_ENABLE_TIMESCALEDB=true`, the migration builds the `events.*`
hypertables and `aggregation.*` continuous aggregates, and the APScheduler
aggregation jobs defer to TimescaleDB's refresh policies.

### Plain PostgreSQL (no TimescaleDB)

Set `TSIGMA_ENABLE_TIMESCALEDB=false`. The event-log tables (`events.controller_event_log`,
`events.roadside_event`) are created with **native declarative range partitioning**
on `event_time` — a `DEFAULT` partition plus the current day's partition — and the
**`manage_partitions` scheduler job** keeps a rolling window (creates
`partition_lookahead_days` ahead, drops past `partition_retention_days`) using the
same dialect-agnostic framework as MS-SQL/Oracle/MySQL. Aggregate refresh runs via
the APScheduler aggregation jobs. No `pg_partman` or `pg_cron` required.

### Related

- Backfilling history from a legacy ATSPM (MS-SQL) via `tds_fdw` → [BACKFILL_GUIDE.md](BACKFILL_GUIDE.md)
- Production hardening, assets, systemd/Kubernetes → [PRODUCTION_DEPLOYMENT.md](PRODUCTION_DEPLOYMENT.md)

---

## Docker Compose (Standard — Single Container)

All components in one container. Suitable for most DOTs.

The `TSIGMA_ENABLE_LISTENERS=true` umbrella below boots every listener type that has at least one signal/sensor configured for it (TCP, UDP, gRPC, MQTT, NATS, directory_watch). Add Layer-2 server config (broker URLs, bind ports, credentials) only for the listener types you actually use — see [Listener Deployment](#listener-deployment) below and [LISTENERS.md](../developers/LISTENERS.md) for the per-method matrix.

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
      - TSIGMA_ENABLE_LISTENERS=true
      - TSIGMA_ENABLE_SCHEDULER=true
      - TSIGMA_LOG_FORMAT=json
      # Layer-2 listener config — only the types this DOT actually uses:
      # - TSIGMA_NATS_URL=nats://nats.dot.gov:4222
      # - TSIGMA_MQTT_BROKER_URL=mqtts://mqtt.dot.gov:8883
      # - TSIGMA_TCP_BIND_PORT=10088
    ports:
      - "8080:8080"
      # Expose listener ports if push-mode signals/sensors will reach this container directly:
      # - "10088:10088"   # TCP / UDP listener
      # - "50051:50051"   # gRPC listener
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

Same image, different env vars per service. API workers, the polling collector, each listener type, and the scheduler all run as their own containers. Add or remove listener services based on which protocols this DOT actually uses — types with no configured signals don't need a container at all.

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
      - TSIGMA_ENABLE_LISTENERS=false
      - TSIGMA_ENABLE_SCHEDULER=false
    deploy:
      replicas: 3

  scheduler:
    image: tsigma:latest
    environment:
      - TSIGMA_PG_HOST=db
      - TSIGMA_ENABLE_API=false
      - TSIGMA_ENABLE_COLLECTOR=false
      - TSIGMA_ENABLE_LISTENERS=false
      - TSIGMA_ENABLE_SCHEDULER=true   # Singleton — only one instance
    # Exactly one replica.

  collector-0:
    image: tsigma:latest
    environment:
      - TSIGMA_PG_HOST=db
      - TSIGMA_ENABLE_API=false
      - TSIGMA_ENABLE_COLLECTOR=true   # FTP + HTTP polling
      - TSIGMA_ENABLE_LISTENERS=false
      - TSIGMA_ENABLE_SCHEDULER=false
      - TSIGMA_WORKER_ID=0  # (Planned)
      - TSIGMA_WORKER_COUNT=2  # (Planned)

  collector-1:
    image: tsigma:latest
    environment:
      - TSIGMA_PG_HOST=db
      - TSIGMA_ENABLE_API=false
      - TSIGMA_ENABLE_COLLECTOR=true
      - TSIGMA_ENABLE_LISTENERS=false
      - TSIGMA_ENABLE_SCHEDULER=false
      - TSIGMA_WORKER_ID=1  # (Planned)
      - TSIGMA_WORKER_COUNT=2  # (Planned)

  # Listener: NATS — only needed if any signals use method=nats_listener
  listener-nats:
    image: tsigma:latest
    environment:
      - TSIGMA_PG_HOST=db
      - TSIGMA_ENABLE_API=false
      - TSIGMA_ENABLE_COLLECTOR=false
      - TSIGMA_ENABLE_NATS_LISTENER=true
      - TSIGMA_ENABLE_SCHEDULER=false
      - TSIGMA_NATS_URL=nats://nats.dot.gov:4222
      - TSIGMA_NATS_CREDENTIALS_FILE=/run/secrets/nats.creds
    secrets:
      - nats.creds

  # Listener: MQTT — internal broker
  listener-mqtt-internal:
    image: tsigma:latest
    environment:
      - TSIGMA_PG_HOST=db
      - TSIGMA_ENABLE_API=false
      - TSIGMA_ENABLE_COLLECTOR=false
      - TSIGMA_ENABLE_MQTT_LISTENER=true
      - TSIGMA_ENABLE_SCHEDULER=false
      - TSIGMA_MQTT_INSTANCE=internal
      - TSIGMA_MQTT_BROKER_URL=mqtt://internal.dot.local:1883

  # Listener: MQTT — vendor cloud broker (separate container, separate instance)
  listener-mqtt-cloud:
    image: tsigma:latest
    environment:
      - TSIGMA_PG_HOST=db
      - TSIGMA_ENABLE_MQTT_LISTENER=true
      - TSIGMA_MQTT_INSTANCE=cloud
      - TSIGMA_MQTT_BROKER_URL=mqtts://broker.vendor.com:8883
      - TSIGMA_MQTT_USERNAME_FILE=/run/secrets/mqtt-cloud.user
      - TSIGMA_MQTT_PASSWORD_FILE=/run/secrets/mqtt-cloud.pw
    secrets:
      - mqtt-cloud.user
      - mqtt-cloud.pw

  # Listener: TCP — for Wavetronics speed sensors and similar
  listener-tcp:
    image: tsigma:latest
    ports:
      - "10088:10088"
    environment:
      - TSIGMA_PG_HOST=db
      - TSIGMA_ENABLE_TCP_LISTENER=true
      - TSIGMA_TCP_BIND_HOST=0.0.0.0
      - TSIGMA_TCP_BIND_PORT=10088

  db:
    image: timescale/timescaledb:latest-pg18
    volumes:
      - pgdata:/var/lib/postgresql/data
    environment:
      - POSTGRES_USER=tsigma
      - POSTGRES_PASSWORD=${DB_PASSWORD}
      - POSTGRES_DB=tsigma

secrets:
  nats.creds:
    file: ./secrets/nats.creds
  mqtt-cloud.user:
    file: ./secrets/mqtt-cloud.user
  mqtt-cloud.pw:
    file: ./secrets/mqtt-cloud.pw

volumes:
  pgdata:
```

## Listener Deployment

Listeners are gated by `TSIGMA_ENABLE_LISTENERS` (umbrella) or per-method `TSIGMA_ENABLE_*_LISTENER` flags. Pick one shape:

| Shape | When | Flags |
|-------|------|-------|
| **Single container, all listeners** | Small DOT (< 2,000 signals), low operational overhead | `TSIGMA_ENABLE_LISTENERS=true` on the main container; set Layer-2 env vars only for the protocols actually used |
| **One listener container per type** | Large DOT, independent failure domains, listener-type-level scaling | `TSIGMA_ENABLE_TCP_LISTENER=true` (etc.) on a dedicated container; set Layer-2 env vars for that one type |
| **Multi-broker** (e.g. internal MQTT + vendor MQTT) | DOT has multiple servers of the same listener type | One container per broker, each with `TSIGMA_MQTT_INSTANCE=<name>` matching the `instance` field on per-signal JSONB |

A listener type with zero matching signals/sensors is a no-op even when its env flag is set — no orphan broker connections, no wasted container if you forgot to remove it.

**Adding a new listener-type DOT-side:**

1. Configure signals/sensors via API or DB to use `method=<listener_name>` (and optionally `instance=<name>` for multi-broker).
2. Set the corresponding `TSIGMA_ENABLE_*_LISTENER=true` env var on a container.
3. Set Layer-2 env vars (`TSIGMA_NATS_URL`, etc.) on the same container.
4. Restart the container. The listener boots, queries matching signals, and starts accepting/subscribing.

For the complete per-method matrix (env vars, JSONB fields, decoder pairing, source-IP routing for TCP/UDP), see [LISTENERS.md](../developers/LISTENERS.md).

## Deployment Modes

TSIGMA supports two deployment modes that differ in data storage tier availability.

### Storage Tier Model

The three storage tiers describe where event-log rows physically live as they age. Tier boundaries are operator-tunable; the SDK layer handles routing transparently so REST and GraphQL clients see one logical event stream.

| Tier | Where the data lives | Tunable by |
|------|----------------------|------------|
| **Hot** | TimescaleDB uncompressed chunks (or plain Postgres / MS-SQL / Oracle / MySQL hot tables) | n/a — newest data, always present |
| **Warm** | TimescaleDB compressed chunks. Columnar, still queryable through the same SQL surface — Timescale handles transparent decompression. | `TSIGMA_STORAGE_WARM_AFTER` (default `7 days`) |
| **Cold** | Parquet files (filesystem or S3/MinIO/Ceph), written by the `export_cold` scheduler job, read via DuckDB or a Postgres FDW. | `TSIGMA_STORAGE_COLD_AFTER_DAYS` (default `180`) |

Notes:

- **Warm tier is TimescaleDB-specific.** Plain Postgres / MS-SQL / Oracle / MySQL deployments have only hot and cold; rows stay uncompressed in the live database until export to Parquet. Setting `TSIGMA_STORAGE_WARM_AFTER` on those backends has no effect — there is no compression policy to install.
- See [Cold-Tier Read Paths](#cold-tier-read-paths) below for the operational choice of where the Parquet reader actually runs (application process vs Postgres backend process).

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
      - TSIGMA_STORAGE_COLD_AFTER_DAYS=180
      - TSIGMA_STORAGE_COLD_FORMAT=parquet
      # Retention is off by default. Native-PostgreSQL deployments can drop old
      # partitions; TimescaleDB needs a manual retention policy (see DATABASE.md):
      # - TSIGMA_PARTITION_RETENTION_DAYS=730
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
      # Retention: add a TimescaleDB retention policy to drop old data
      # (not driven by a TSIGMA env var on TimescaleDB deployments).
```

### Cold-Tier Read Paths

When `TSIGMA_STORAGE_COLD_ENABLED=true`, queries that reach past `cold_tier.threshold_days` need a way to read the Parquet partitions back. TSIGMA supports two read paths; the choice is per-deployment and depends on the database family and where the cold files live.

#### Application-layer DuckDB (universal fallback)

The TSIGMA application process reads Parquet via DuckDB and unions the result with hot/warm rows from the database, inside the SDK layer (`tsigma.reports.sdk.queries.fetch_events` and friends). Works against every supported database family. Requires `TSIGMA_COLD_TIER_QUERY_ENABLED=true` (the default) and the DuckDB `httpfs` extension if cold storage is S3 / MinIO / Ceph (pre-installed in the official image; see `scripts/install_duckdb_extensions.py`).

#### In-database via Postgres extension (preferred for PostgreSQL)

PostgreSQL deployments can read cold Parquet directly inside the database using one of three extensions. The deployment exposes a unified view:

```sql
CREATE VIEW controller_event_log_all AS
SELECT * FROM controller_event_log         -- hot + warm (TimescaleDB)
UNION ALL
SELECT * FROM controller_event_log_cold;   -- cold (Parquet)
```

When this view is in place, set `TSIGMA_COLD_TIER_QUERY_ENABLED=false` so the SDK short-circuits to a single SQL query against the unified view — the in-DB extension does the work and no application-layer DuckDB runs.

#### Choosing the Postgres extension

- **`pg_duckdb`** (MotherDuck + DuckDB Labs, MIT, actively maintained) — embeds DuckDB inside the Postgres backend process. Supports local filesystem and S3 / MinIO / Ceph via DuckDB's `httpfs` (`read_parquet('s3://...')` with secrets via `duckdb.create_simple_secret()`). PG 14–18. **Recommended choice** for new deployments.
- **`duckdb_fdw`** (community, alitrack) — wraps DuckDB as a foreign data wrapper. Functionally similar to `pg_duckdb`; less active upstream development. Acceptable when a deployment already standardized on it.
- **`parquet_fdw`** (Adjust) — native Parquet FDW. Primarily targets local filesystem; S3/MinIO support exists in some forks but is fragile. Acceptable for local-filesystem-only cold storage.

TimescaleDB compatibility for `pg_duckdb` is not explicitly tested upstream — verify with a smoke query in the target deployment before declaring it production-blessed. The two extensions hook into different Postgres planner layers and are expected to coexist, but verification is cheap insurance.

#### Read-path matrix

| Database family | Cold storage | Read paths available |
|-----------------|--------------|----------------------|
| PostgreSQL + TimescaleDB | Local filesystem | App-layer DuckDB, `pg_duckdb` (preferred), `duckdb_fdw`, `parquet_fdw` |
| PostgreSQL + TimescaleDB | S3 / MinIO / Ceph | App-layer DuckDB, `pg_duckdb` (preferred), `duckdb_fdw` |
| PostgreSQL (plain, no Timescale) | Local filesystem | App-layer DuckDB, `pg_duckdb` (preferred), `duckdb_fdw`, `parquet_fdw` |
| PostgreSQL (plain, no Timescale) | S3 / MinIO / Ceph | App-layer DuckDB, `pg_duckdb` (preferred), `duckdb_fdw` |
| MS-SQL / Oracle / MySQL | Any | App-layer DuckDB only (no Parquet-reading extension available) |

---

## Environment Variables Reference

### Core

| Variable | Default | Description |
|----------|---------|-------------|
| `TSIGMA_ENABLE_API` | `true` | Enable REST API, GraphQL, Web UI |
| `TSIGMA_ENABLE_COLLECTOR` | `true` | Enable polling ingestion (ftp_pull, http_pull) |
| `TSIGMA_ENABLE_LISTENERS` | `false` | Umbrella: enable every listener type that has at least one configured signal/sensor |
| `TSIGMA_ENABLE_TCP_LISTENER` | `false` | Enable TCP listener only (overrides umbrella) |
| `TSIGMA_ENABLE_UDP_LISTENER` | `false` | Enable UDP listener only |
| `TSIGMA_ENABLE_GRPC_LISTENER` | `false` | Enable gRPC listener only |
| `TSIGMA_ENABLE_MQTT_LISTENER` | `false` | Enable MQTT listener only |
| `TSIGMA_ENABLE_NATS_LISTENER` | `false` | Enable NATS listener only |
| `TSIGMA_ENABLE_DIRECTORY_WATCH` | `false` | Enable filesystem directory watcher only |
| `TSIGMA_ENABLE_SCHEDULER` | `true` | Enable APScheduler (view refresh, watchdog) |
| `TSIGMA_WORKER_ID` | `0` | Worker index for signal partitioning (Planned) |
| `TSIGMA_WORKER_COUNT` | `1` | Total number of collector workers (Planned) |
| `TSIGMA_PG_HOST` | `localhost` | PostgreSQL host |
| `TSIGMA_PG_PORT` | `5432` | PostgreSQL port |
| `TSIGMA_PG_DATABASE` | `tsigma` | Database name |
| `TSIGMA_PG_USER` | `tsigma` | Database user |
| `TSIGMA_PG_PASSWORD` | _(required)_ | Database password |
| `TSIGMA_ENABLE_TIMESCALEDB` | `false` | PostgreSQL-only: build hypertables + continuous aggregates and let TimescaleDB own refresh. Must be `false` for MS-SQL/Oracle/MySQL |
| `TSIGMA_COLLECTOR_POLL_INTERVAL` | `300` | Controller poll interval (seconds) |
| `TSIGMA_REFRESH_SCHEDULE` | `*/15 * * * *` | Materialized view refresh cron (Planned — not yet implemented) |
| `TSIGMA_WATCHDOG_SCHEDULE` | `0 6 * * *` | Watchdog cron (Planned — not yet implemented) |
| `TSIGMA_LOG_LEVEL` | `INFO` | Log level |
| `TSIGMA_LOG_FORMAT` | `json` | Log format (json or console) |
| `TSIGMA_DEBUG` | `false` | Debug mode |

### Listener Server Config (Layer 2)

Set these only on processes that have the matching `TSIGMA_ENABLE_*_LISTENER` (or umbrella) flag set. See [LISTENERS.md](../developers/LISTENERS.md) for the full per-method matrix.

| Variable | Default | Description |
|----------|---------|-------------|
| `TSIGMA_TCP_BIND_HOST` | `0.0.0.0` | TCP listener bind address |
| `TSIGMA_TCP_BIND_PORT` | `10088` | TCP listener bind port |
| `TSIGMA_TCP_MAX_CONNECTIONS` | `2000` | Max concurrent TCP connections |
| `TSIGMA_TCP_IDLE_TIMEOUT` | `300` | Drop idle TCP connection after N seconds |
| `TSIGMA_UDP_BIND_HOST` | `0.0.0.0` | UDP listener bind address |
| `TSIGMA_UDP_BIND_PORT` | `10088` | UDP listener bind port |
| `TSIGMA_UDP_MAX_PACKET_SIZE` | `4096` | Max UDP datagram size in bytes |
| `TSIGMA_GRPC_BIND_HOST` | `0.0.0.0` | gRPC listener bind address |
| `TSIGMA_GRPC_BIND_PORT` | `50051` | gRPC listener bind port |
| `TSIGMA_GRPC_TLS_CERT_FILE` | — | Path to gRPC server TLS cert (mounted secret) |
| `TSIGMA_GRPC_TLS_KEY_FILE` | — | Path to gRPC server TLS key (mounted secret) |
| `TSIGMA_GRPC_MAX_MESSAGE_SIZE` | `4194304` | Max gRPC message size (bytes) |
| `TSIGMA_MQTT_BROKER_URL` | _(required)_ | MQTT broker URL (e.g., `mqtts://host:8883`) |
| `TSIGMA_MQTT_CLIENT_ID` | `tsigma-listener` | MQTT client ID |
| `TSIGMA_MQTT_USERNAME` | — | MQTT username (or use `*_FILE` variant) |
| `TSIGMA_MQTT_USERNAME_FILE` | — | Path to file containing MQTT username (mounted secret) |
| `TSIGMA_MQTT_PASSWORD_FILE` | — | Path to file containing MQTT password (mounted secret) |
| `TSIGMA_MQTT_KEEPALIVE` | `60` | MQTT keepalive interval (seconds) |
| `TSIGMA_MQTT_INSTANCE` | `default` | Discriminator name; matches `collection.instance` on per-signal JSONB |
| `TSIGMA_NATS_URL` | _(required)_ | NATS server URL (`nats://host:4222`) |
| `TSIGMA_NATS_CREDENTIALS_FILE` | — | Path to NATS credentials file (mounted secret) |
| `TSIGMA_NATS_TLS` | `false` | Enable TLS for NATS connection |
| `TSIGMA_NATS_MAX_RECONNECTS` | `-1` | Max reconnect attempts (`-1` = infinite) |
| `TSIGMA_NATS_INSTANCE` | `default` | Discriminator name; matches `collection.instance` |
| `TSIGMA_DIRECTORY_WATCH_PATHS` | — | Comma-separated paths to watch |
| `TSIGMA_DIRECTORY_WATCH_PATTERNS` | `*` | Comma-separated glob patterns to match |

### Storage Tiers

| Variable | Default | Description |
|----------|---------|-------------|
| `TSIGMA_STORAGE_WARM_AFTER` | `7 days` | Compress chunks older than this interval |
| `TSIGMA_STORAGE_WARM_MAX_DISK` | — | Compress early if hot tier exceeds this size (TimescaleDB only, e.g., `500 GB`) |
| `TSIGMA_STORAGE_WARM_CHECK_INTERVAL` | `5m` | How often to check disk usage for `WARM_MAX_DISK` |
| `TSIGMA_STORAGE_COLD_ENABLED` | `false` | Enable Parquet cold tier (On-Prem only). Runtime-registry override for `storage.cold_enabled`. |
| `TSIGMA_STORAGE_COLD_AFTER_DAYS` | `180` | Export to Parquet after this many days. Runtime-registry override for `storage.cold_after_days`. **Renamed — see migration note below.** |
| `TSIGMA_STORAGE_COLD_FORMAT` | `parquet` | Cold export format |
| `TSIGMA_STORAGE_BACKEND` | `filesystem` | Cold storage backend (`filesystem` or `s3`) |
| `TSIGMA_STORAGE_COLD_PATH` | `/var/lib/tsigma/cold` | Filesystem path for cold storage |
| `TSIGMA_STORAGE_S3_BUCKET` | — | S3 bucket for cold storage |
| `TSIGMA_STORAGE_S3_REGION` | `us-east-1` | S3 region |
| `TSIGMA_STORAGE_S3_ENDPOINT` | — | Custom S3 endpoint (MinIO/Ceph) |
| `TSIGMA_PARTITION_RETENTION_DAYS` | — (unset) | Drop event partitions older than N days (native PostgreSQL only; unset keeps all data) |

### Runtime Settings (registry-derived overrides)

TSIGMA exposes a runtime-settings registry — nine admin-tunable keys
stored in `identity.system_setting` and exposed via the admin API. Each
registered key carries a `TSIGMA_<KEY>` environment-variable override
(dots → underscores, uppercased) that takes precedence over the
database row. See
[docs/operations/runtime-settings.md](../operations/runtime-settings.md)
for the full registry, admin API reference, and audit log details.

| Variable | Default | Description |
|----------|---------|-------------|
| `TSIGMA_VALKEY_SETTINGS_INVALIDATION_ENABLED` | `true` | Publish runtime-settings invalidations on the Valkey pub/sub channel `tsigma:system_setting:invalidate` so peer replicas drop their local caches. Dual-gated with `TSIGMA_VALKEY_URL` — both must be set; setting this to `false` disables publication without unsetting the shared Valkey URL. Single-replica deployments may leave this at default. |
| `TSIGMA_COLD_TIER_QUERY_ENABLED` | `true` | Enable application-layer cold-tier reads via DuckDB. Override for `cold_tier.query_enabled`. Set to `false` for PG + FDW deployments that expose a unified hot/warm/cold view — the SDK then short-circuits to a single SQL query against the view (see [Cold-Tier Read Paths](#cold-tier-read-paths)). |
| `TSIGMA_COLD_TIER_THRESHOLD_DAYS` | `180` | Events older than this many days are read via the application-layer DuckDB cold path. Override for `cold_tier.threshold_days`. Has no effect on the FDW unified-view path (that path always sees all tiers). |
| `TSIGMA_STORAGE_COLD_DELETE_AFTER_EXPORT` | `true` | Delete archived rows from the hot DB after verified Parquet write. Override for `storage.cold_delete_after_export`. |
| `TSIGMA_API_MAX_PAGE_SIZE` | `1000` | Event-list endpoint per-page cap. Override for `api.max_page_size`. |
| `TSIGMA_API_MAX_AGGREGATION_DAYS` | `92` | Aggregation endpoint date-range cap. Override for `api.max_aggregation_days`. |
| `TSIGMA_API_MAX_SIGNALS_PER_REQUEST` | `100` | Aggregation endpoint per-request signal count cap. Override for `api.max_signals_per_request`. |
| `TSIGMA_API_MAX_LOOKBACK_DAYS` | `92` | Absolute oldest data an API request can ask for. Override for `api.max_lookback_days`. |

> **Note on precedence.** When an env-var override is set, the matching
> DB row is ignored at read time. Admin-UI writes still succeed and
> still produce audit rows, but the running process keeps observing the
> env-var value until the env var is removed and the process is
> restarted (or the cache TTL passes after env removal). Document
> deployed env-var overrides in your runbook so operators do not mistake
> them for unsaved admin changes.

### Migration Notes (hard cutover)

One storage env var was renamed with a **hard cutover** — the old name
is silently ignored and no deprecation warning is emitted. Audit your
deployment artifacts (`.env`, docker-compose, Helm values, Kubernetes
ConfigMaps, secret stores) before deploying:

| Old (silently inert)                     | New                                       |
|------------------------------------------|-------------------------------------------|
| `TSIGMA_STORAGE_COLD_AFTER="6 months"`   | `TSIGMA_STORAGE_COLD_AFTER_DAYS=180`      |

`TSIGMA_STORAGE_COLD_ENABLED` is **unchanged**.
`TSIGMA_STORAGE_COLD_PATH` is **unchanged** (remains a deployment-fixed
Pydantic config attribute, not a runtime-registry key).

If you previously set `TSIGMA_STORAGE_COLD_AFTER="6 months"`, rename it
to `TSIGMA_STORAGE_COLD_AFTER_DAYS=180` (or your preferred integer day
count) before the next deploy. The cold-export scheduler job
(`tsigma.scheduler.jobs.export_cold`) now reads the threshold from the
runtime registry; the previous string-with-units format is no longer
parsed.
