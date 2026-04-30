# Listener Ingestion

> Part of [TSIGMA Architecture](../ARCHITECTURE.md). See also [INGESTION.md](INGESTION.md) for the polling and decoder pipelines.

---

## Overview

Listener ingestion is push-mode data collection. External devices — controllers, roadside sensors, edge brokers — open a connection to TSIGMA and stream events. TSIGMA does not initiate the connection; it binds a port (TCP/UDP/gRPC) or subscribes to a broker (MQTT/NATS) and accepts whatever arrives.

Listener plugins live alongside polling plugins under `tsigma/collection/methods/`, register with the same `IngestionMethodRegistry`, decode through the same `DecoderRegistry`, and write to the same event tables (`controller_event_log` / `roadside_event`). What differs is lifecycle: a listener is a long-lived async server, started once at boot and stopped on shutdown.

`ListenerService` (`tsigma/collection/listener_service.py`) is the orchestrator. It is parallel to `CollectorService` and is enabled separately so that listener-heavy and polling-heavy deployments can scale and fail independently.

---

## Configuration Layers

Three layers configure a listener. Each layer answers a different question.

| Layer | Question | Where it lives | Scope |
|-------|----------|----------------|-------|
| 1 — Lifecycle gate | "Should this listener type boot in this process?" | Env vars (`config.py`) | Process |
| 2 — Server connection | "Where does this listener bind / connect / authenticate?" | Env vars (`config.py`) | Listener instance (one per type per process) |
| 3 — Device routing | "Which devices does this listener handle, and how does each one route/decode?" | `signal.metadata.collection` / `roadside_sensor.metadata.collection` JSONB | Per device |

### Layer 1 — Lifecycle gate

```env
# Umbrella: boot every listener-type that has at least one configured signal/sensor.
# Suitable for small DOTs running everything in one container.
TSIGMA_ENABLE_LISTENERS=true

# Per-method: boot exactly one listener type. Suitable for multi-process
# deployments where each container runs one listener.
TSIGMA_ENABLE_TCP_LISTENER=true
TSIGMA_ENABLE_UDP_LISTENER=true
TSIGMA_ENABLE_GRPC_LISTENER=true
TSIGMA_ENABLE_MQTT_LISTENER=true
TSIGMA_ENABLE_NATS_LISTENER=true
TSIGMA_ENABLE_DIRECTORY_WATCH=true
```

Per-method flags imply the umbrella; setting any of them enables exactly that one listener and skips the rest. If the umbrella alone is set, every registered listener type boots.

### Layer 2 — Server connection

Server-level config (broker URL, bind, credentials, TLS) is environment-driven. One set of variables per listener type per process. Credentials use the `*_FILE` convention so K8s/Docker secrets can be mounted without baking values into the env.

```env
# TCP listener
TSIGMA_TCP_BIND_HOST=0.0.0.0
TSIGMA_TCP_BIND_PORT=10088
TSIGMA_TCP_MAX_CONNECTIONS=2000
TSIGMA_TCP_IDLE_TIMEOUT=300

# UDP listener
TSIGMA_UDP_BIND_HOST=0.0.0.0
TSIGMA_UDP_BIND_PORT=10088
TSIGMA_UDP_MAX_PACKET_SIZE=4096

# gRPC listener
TSIGMA_GRPC_BIND_HOST=0.0.0.0
TSIGMA_GRPC_BIND_PORT=50051
TSIGMA_GRPC_TLS_CERT_FILE=/run/secrets/grpc.crt
TSIGMA_GRPC_TLS_KEY_FILE=/run/secrets/grpc.key
TSIGMA_GRPC_MAX_MESSAGE_SIZE=4194304

# MQTT listener
TSIGMA_MQTT_BROKER_URL=mqtts://mqtt.dot.gov:8883
TSIGMA_MQTT_CLIENT_ID=tsigma-listener-1
TSIGMA_MQTT_USERNAME=tsigma
TSIGMA_MQTT_PASSWORD_FILE=/run/secrets/mqtt.pw
TSIGMA_MQTT_KEEPALIVE=60
TSIGMA_MQTT_INSTANCE=default        # See "Multi-instance" below

# NATS listener
TSIGMA_NATS_URL=nats://nats.dot.gov:4222
TSIGMA_NATS_CREDENTIALS_FILE=/run/secrets/nats.creds
TSIGMA_NATS_TLS=true
TSIGMA_NATS_MAX_RECONNECTS=-1
TSIGMA_NATS_INSTANCE=default

# Directory watch
TSIGMA_DIRECTORY_WATCH_PATHS=/var/lib/tsigma/incoming
TSIGMA_DIRECTORY_WATCH_PATTERNS=*.dat,*.csv
```

### Layer 3 — Device routing/decoder

Per-device routing is per-signal or per-sensor JSONB on `metadata.collection`. The first-class network triple (`ip_address`, `port`, `protocol`) lives in dedicated table columns on both `signal` and `roadside_sensor` — not in JSONB — so source-IP lookups for inbound TCP/UDP packets resolve through B-tree indexes.

```json
// TCP — source-IP lookup goes through signal.ip_address (B-tree)
{
  "collection": {
    "method": "tcp_server",
    "decoder": "wavetronix_advance"
  }
}

// MQTT — broker URL is env, JSONB carries the topic pattern + decoder
{
  "collection": {
    "method": "mqtt_listener",
    "topic": "atspm/gdot-0142/+",
    "qos": 1,
    "decoder": "openphase"
  }
}

// NATS — broker URL is env, JSONB carries the subject + decoder
{
  "collection": {
    "method": "nats_listener",
    "subject": "signals.gdot-0142.events",
    "decoder": "openphase"
  }
}

// gRPC — JSONB carries the device's gRPC client identity + decoder
{
  "collection": {
    "method": "grpc_server",
    "client_id": "openphase-gdot-0142",
    "decoder": "openphase"
  }
}

// Multi-broker MQTT — `instance` selects which container owns this signal
{
  "collection": {
    "method": "mqtt_listener",
    "instance": "cloud",
    "topic": "tenant/gdot/0143/events",
    "decoder": "openphase"
  }
}
```

---

## Per-Method Config Matrix

What each listener type expects at each layer:

| Method | Layer 2 (env) | Layer 3 (per-device JSONB) | Device-class lookup |
|--------|---------------|----------------------------|----------------------|
| `tcp_server` | `TSIGMA_TCP_BIND_HOST`, `TSIGMA_TCP_BIND_PORT`, `TSIGMA_TCP_MAX_CONNECTIONS`, `TSIGMA_TCP_IDLE_TIMEOUT` | `decoder` | Source IP → `signal.ip_address` / `roadside_sensor.ip_address` (B-tree) |
| `udp_server` | `TSIGMA_UDP_BIND_HOST`, `TSIGMA_UDP_BIND_PORT`, `TSIGMA_UDP_MAX_PACKET_SIZE` | `decoder` | Source IP → `signal.ip_address` / `roadside_sensor.ip_address` (B-tree) |
| `grpc_server` | `TSIGMA_GRPC_BIND_*`, `TSIGMA_GRPC_TLS_*`, `TSIGMA_GRPC_MAX_MESSAGE_SIZE` | `client_id`, `decoder` | gRPC client identity → device |
| `mqtt_listener` | `TSIGMA_MQTT_BROKER_URL`, `TSIGMA_MQTT_CLIENT_ID`, `TSIGMA_MQTT_USERNAME`, `TSIGMA_MQTT_PASSWORD_FILE`, `TSIGMA_MQTT_KEEPALIVE`, `TSIGMA_MQTT_INSTANCE` | `topic`, `qos`, `decoder`, optional `instance` | Topic match against device's configured `topic` |
| `nats_listener` | `TSIGMA_NATS_URL`, `TSIGMA_NATS_CREDENTIALS_FILE`, `TSIGMA_NATS_TLS`, `TSIGMA_NATS_MAX_RECONNECTS`, `TSIGMA_NATS_INSTANCE` | `subject`, `decoder`, optional `instance` | Subject match against device's configured `subject` |
| `directory_watch` | `TSIGMA_DIRECTORY_WATCH_PATHS`, `TSIGMA_DIRECTORY_WATCH_PATTERNS` | `decoder` (optional; `auto` if omitted) | Filename → device via filename convention or per-watcher mapping |

---

## Source-IP Routing (TCP / UDP)

When a TCP/UDP packet arrives, the listener needs to resolve the source IP to a specific signal or sensor before it can decode and persist events.

The lookup uses the `DeviceSource` the listener was instantiated with:

- `SignalDeviceSource` → query `signal` where `ip_address = source` and `metadata->'collection'->>'method' = 'tcp_server'` (or `udp_server`)
- `RoadsideSensorDeviceSource` → query `roadside_sensor` where `ip_address = source` and `metadata->'collection'->>'method' = 'tcp_server'` (or `udp_server`)

Both `signal.ip_address` and `roadside_sensor.ip_address` carry B-tree indexes; the `metadata->>'method'` predicate is the GIN-indexed filter. A listener container that serves both controllers and sensors instantiates with both sources; one that serves only one device class instantiates with just that source.

A connection from an unrecognized IP is logged at WARNING and dropped. No event is persisted without a resolved device.

---

## Multi-Instance Pattern

A DOT may have multiple servers of the same listener type — for example, an internal MQTT broker for legacy devices and a vendor-cloud MQTT broker for managed signals. Each broker needs its own listener container, and each signal needs to declare which broker it belongs to.

The `instance` discriminator on `metadata.collection` solves this. Each listener container sets `TSIGMA_MQTT_INSTANCE` (or `TSIGMA_NATS_INSTANCE`) to its own name. On boot, the listener queries:

```sql
SELECT signal_id
FROM signal
WHERE metadata->'collection'->>'method' = 'mqtt_listener'
  AND COALESCE(metadata->'collection'->>'instance', 'default') = :env_instance
```

Single-broker DOTs omit `instance` from per-signal config and don't set `TSIGMA_MQTT_INSTANCE` — both default to `'default'` and one container picks up everything.

```env
# Container A — internal broker
TSIGMA_ENABLE_MQTT_LISTENER=true
TSIGMA_MQTT_INSTANCE=internal
TSIGMA_MQTT_BROKER_URL=mqtt://internal.dot.local:1883

# Container B — cloud broker
TSIGMA_ENABLE_MQTT_LISTENER=true
TSIGMA_MQTT_INSTANCE=cloud
TSIGMA_MQTT_BROKER_URL=mqtts://broker.vendor.com:8883
TSIGMA_MQTT_USERNAME_FILE=/run/secrets/mqtt-cloud.user
TSIGMA_MQTT_PASSWORD_FILE=/run/secrets/mqtt-cloud.pw
```

```json
// Internal-broker signal
{ "collection": { "method": "mqtt_listener", "instance": "internal",
                  "topic": "atspm/gdot-0142/+", "decoder": "openphase" } }

// Cloud-broker signal
{ "collection": { "method": "mqtt_listener", "instance": "cloud",
                  "topic": "tenant/gdot/0143/events", "decoder": "openphase" } }
```

No orphan subscriptions: each container only subscribes to topics for signals tagged with its own instance name.

---

## Lifecycle

`ListenerService.start()` is called from the lifespan manager in `app.py`. It runs once at startup:

1. **Discover.** Walk `IngestionMethodRegistry.get_listener_methods()` and `get_event_driven_methods()`.
2. **Filter by env.** Skip any method whose `TSIGMA_ENABLE_*_LISTENER` flag is unset (and whose umbrella `TSIGMA_ENABLE_LISTENERS` is also unset).
3. **Build server config.** Read Layer 2 env vars into a Pydantic settings object for that method.
4. **Match devices.** For each enabled `(method × DeviceSource)` pair, query the source for devices using this method (and matching `instance`, if applicable). Skip the method entirely if zero devices match.
5. **Start.** Call `method.start(server_config, session_factory)`. The plugin opens its connection / binds its port and subscribes / accepts on behalf of all matched devices.

`ListenerService.stop()` runs in reverse on shutdown — calls `method.stop()` for every method that was started, gathering exceptions and logging them without blocking other stops.

### Health checks

Each listener implements `health_check() -> bool`. `ListenerService.health_check()` aggregates results into a `dict[str, bool]` keyed by method name, exposed through the same `/api/v1/collection/health` endpoint that polling uses.

### Restart semantics

Listener config changes (broker URL, bind port, credentials) require a process restart — the env-driven config layer is loaded once at startup. Per-device JSONB changes (new signal added, topic edited) are picked up by the plugin's own re-query cadence; for MQTT/NATS, this typically means re-subscribing on the next reconnect or on an explicit `/api/v1/collection/listeners/{name}/refresh` admin call.

---

## Decoder Pairing

Listener decoders work the same way as polling decoders. Each device's `metadata.collection.decoder` names a registered decoder; the listener calls `DecoderRegistry.get(name)().decode_bytes(...)` (or the streaming equivalent) on inbound payloads.

For listener-specific binary protocols (Wavetronix Advance over TCP, OpenPhase protobuf over NATS/MQTT), decoders are registered alongside the polling decoders in `tsigma/collection/decoders/`. See [DECODERS.md](DECODERS.md).

If `decoder` is omitted on a per-device config, listeners default to `auto` (probes registered decoders by `can_decode()`). Listener-specific protocols where auto-detect doesn't make sense (binary structs without magic bytes) **must** set `decoder` explicitly — `auto` will raise.

---

## Failure Modes and Notifications

Listeners reuse the same notification channels as polling for operational alerts:

| Condition | Severity | Notification |
|-----------|----------|--------------|
| Listener fails to `start()` (port in use, broker unreachable, bad credentials) | CRITICAL | Method name, error class, retry policy |
| Broker reconnect storm (MQTT/NATS) | WARNING | Disconnect count over the last N minutes |
| Inbound packet from unrecognized source IP (TCP/UDP) | WARNING (rate-limited) | Source IP, listener name, count |
| Decoder failure on inbound payload | WARNING (rate-limited) | Device ID, decoder, payload size, exception |
| Listener has been silent for N minutes (no events from any device) | WARNING | Method name, last event timestamp |

Silent-device detection (the per-device variant) reuses the same `consecutive_silent_cycles` mechanism as polling, with the cycle clock driven by the listener's own heartbeat instead of a polling interval. See [INGESTION.md — Silent Signal Detection](INGESTION.md#4-silent-signal-detection-and-auto-recovery).

---

## On-Demand Trigger

Listeners do not currently support an on-demand "poll now" trigger — they're inherently demand-driven from the device side. The closest equivalent is `POST /api/v1/collection/listeners/{name}/refresh`, which forces the listener to re-query its `DeviceSource` and re-subscribe (used after editing per-device JSONB without a process restart).

For MQTT/NATS replay of historical messages, use the broker's own retained-messages or stream-replay mechanisms — this is outside the listener's scope.

---

## Related Documents

- [ARCHITECTURE.md](ARCHITECTURE.md) — overall system design, deployment model, lifespan
- [INGESTION.md](INGESTION.md) — polling pipeline, decoder pipeline, on-demand poll API, base classes
- [DECODERS.md](DECODERS.md) — decoder plugin authoring
- [DATABASE_SCHEMA.md](DATABASE_SCHEMA.md) — `signal`, `roadside_sensor`, network-triple columns and indexes
- [DEPLOYMENT.md](../users/DEPLOYMENT.md) — operator-facing deployment recipes
