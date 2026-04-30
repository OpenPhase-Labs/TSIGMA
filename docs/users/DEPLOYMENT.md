# Deployment

> Part of [TSIGMA Architecture](../ARCHITECTURE.md)

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
| `TSIGMA_STORAGE_COLD_ENABLED` | `false` | Enable Parquet cold tier (On-Prem only) |
| `TSIGMA_STORAGE_COLD_AFTER` | `6 months` | Export to Parquet after this age |
| `TSIGMA_STORAGE_COLD_FORMAT` | `parquet` | Cold export format |
| `TSIGMA_STORAGE_BACKEND` | `filesystem` | Cold storage backend (`filesystem` or `s3`) |
| `TSIGMA_STORAGE_COLD_PATH` | `/var/lib/tsigma/cold` | Filesystem path for cold storage |
| `TSIGMA_STORAGE_S3_BUCKET` | — | S3 bucket for cold storage |
| `TSIGMA_STORAGE_S3_REGION` | `us-east-1` | S3 region |
| `TSIGMA_STORAGE_S3_ENDPOINT` | — | Custom S3 endpoint (MinIO/Ceph) |
| `TSIGMA_STORAGE_RETENTION` | `2 years` | Drop data entirely after this age |
