# Cluster coordination via shared state (DB + Valkey)

- **Status**: Accepted
- **Date**: 2026-06-28
- **Deciders**: jsloan@celldara.com

## Context and Problem Statement

TSIGMA scales by running the same image as role-specialized replicas (ADR-0003):
API, collector, listener, and scheduler tiers. Replicas must coordinate on
exactly-once work (scheduled jobs / aggregate maintenance), partitioned work
(which collector polls which signals), and cross-replica fan-out (cache / settings
invalidation; live updates). What connects them — without inventing a consensus
plane?

## Decision Drivers

- The database is already required and authoritative; reusing it adds no infrastructure.
- Coordinated operations aren't latency-sensitive (a job firing 100 ms later is fine).
- Multi-dialect support (PG/MS-SQL/Oracle/MySQL) — coordination shouldn't depend on a PG-only primitive across all tiers.
- Replicas sit behind a load balancer; correctness must not require sticky sessions.
- In-memory caches must be reconstructable from durable state (cold-start safe).

## Considered Options

- Shared state: DB authoritative + singleton scheduler + work sharding + Valkey pub/sub
- Raft / etcd / Consul consensus
- Gossip protocol
- Direct node-to-node gRPC

## Decision Outcome

**Coordinate through shared state.** The database is the source of truth for all
persistent state. Exactly-once background work runs in a **singleton scheduler**
(ADR-0003: exactly one scheduler) — not lock-contended across nodes. Partitioned
ingestion is **sharded** by `WORKER_ID`/`WORKER_COUNT` so each collector/listener
owns a disjoint slice. Cross-replica fan-out — runtime-settings/cache invalidation
and live-update delivery — uses **Valkey pub/sub**. (The future NATS push plane
carries event ingestion, not core coordination.) No Raft, gossip, etcd, or Consul.

In-memory caches tolerate cold start; all node state is reconstructable from the
database (+ Valkey).

### Consequences

- No new infrastructure — the DB is required anyway, and Valkey is the optional multi-replica add-on already used for sessions.
- Operators reason about familiar pieces (DB, Valkey), not a consensus plane.
- Any replica serves any request via the load balancer; no sticky sessions.
- The DB sits in the coordination path — already true for persistence, so not a new failure mode.
- Exactly-once relies on deploying a single scheduler; running two is an operator error (guarded).

### Confirmation

Exactly-once work runs only in the scheduler tier; collectors/listeners shard by
worker id; cross-replica invalidation goes through Valkey; no Raft/etcd/Consul in
the build; caches cold-start clean.

## Pros and Cons of the Options

### Shared state: DB + singleton scheduler + sharding + Valkey (chosen)

- Good, because zero new infrastructure, familiar ops, stateless replicas, cold-start safe.
- Bad, because DB latency floors coordination (fine for this workload) and the single-scheduler rule must be enforced.

### Raft / etcd / Consul

- Bad, because it adds a third stateful plane and specialized ops for no workload-justified gain.

### Gossip protocol

- Bad, because it is eventually consistent — wrong for exactly-once work.

### Direct node-to-node gRPC

- Bad, because it needs service discovery and has no natural exactly-once / leader primitive.

## More Information

- ADR-0003 (role-specialized deployment, singleton scheduler, DB as shared state)
- Forthcoming: real-time transport (SSE via Valkey fan-out); the future NATS push plane
