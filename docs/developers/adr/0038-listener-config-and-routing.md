# Listener configuration and routing: three layers, multi-instance, source-IP

- **Status**: Accepted
- **Date**: 2026-06-28
- **Deciders**: jsloan@celldara.com

## Context and Problem Statement

Listener ingestion needs to know: whether to boot at all, where to connect
(broker/bind), and how to route each inbound message to the right device. These
concerns have different scopes (process / server / per-device). How is listener
configuration layered, and how are inbound messages routed to devices?

## Decision Drivers

- "Whether to boot" is process-wide; "where to connect" is server-wide; "how to route" is per-device.
- Multi-broker deployments (e.g. internal + cloud broker) must route each device to the right container.
- Inbound TCP/UDP has no app-level device id — only a source IP — so routing must resolve IP→device fast.
- gRPC/MQTT carry their own identifiers (`client_id`, topic).

## Considered Options

- Three config layers + per-device instance discriminator + indexed source-IP routing
- One flat config blob
- Hardcoded single-broker config

## Decision Outcome

**Three configuration layers:**

1. **Lifecycle gate (env)** — whether listeners boot: an umbrella `TSIGMA_ENABLE_LISTENERS`, or per-method flags (`TSIGMA_ENABLE_TCP_LISTENER`, etc.).
2. **Server connection (env)** — broker URL, bind address/port, credentials, TLS.
3. **Per-device routing (JSONB)** — subject/topic/decoder/instance/qos per device in the `collection` metadata.

**Multi-instance:** a per-device `instance` discriminator (default `"default"`)
selects which listener container owns each device, so multi-broker deployments
route cleanly without orphan subscriptions.

**Routing inbound messages to devices:** TCP/UDP listeners resolve the **source
IP** to a device via a B-tree index on the first-class `ip_address` column
(ADR-0010); MQTT routes by topic, gRPC by `client_id`. Unrecognized sources are
logged (rate-limited) and dropped.

### Consequences

- Concerns separate cleanly: boot (process), connect (server), route (per-device).
- Multi-broker, multi-container deployments work via the instance discriminator.
- Source-IP routing is index-backed (O(log n)), not a JSONB scan.
- Server-level config changes need a restart; per-device JSONB changes are picked up by re-query/refresh.

### Confirmation

Env gates control boot; broker/bind/credentials are env; per-device routing is
JSONB with an `instance` discriminator; TCP/UDP resolve source IP via the indexed
column; unknown sources are rate-limited-logged and dropped.

## Pros and Cons of the Options

### Three layers + instance + source-IP (chosen)

- Good, because scoping is clean, multi-broker is supported, and IP routing is fast.
- Bad, because operators must understand three layers (mitigated by clear gates).

### One flat config blob

- Bad, because it conflates process/server/device scopes and makes multi-broker awkward.

### Hardcoded single-broker

- Bad, because there's no multi-broker support and no per-device routing.

## More Information

- ADR-0036 (ListenerService), ADR-0037 (DeviceSource), ADR-0010 (network-triple typed columns), ADR-0035 (listener methods)
