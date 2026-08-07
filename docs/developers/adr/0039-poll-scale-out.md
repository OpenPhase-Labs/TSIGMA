# Poll-plane scale-out: shard the device inventory + bounded concurrency

- **Status**: Accepted
- **Date**: 2026-06-28
- **Deciders**: jsloan@celldara.com

## Context and Problem Statement

The poll plane must scale from a handful of signals on one box to a large fleet
(~9,000+). The poller works from a device inventory — a list of connection targets —
and must divide it across workers evenly, bound concurrent connections within a
worker, and let operators tune all of it at runtime. How does the poll plane scale,
and by what key?

## Decision Drivers

- Thousands of devices must be polled within cadence without one process doing all of it.
- Identifiers are not dense/sequential (ADR-0078), so raw `id % N` distributes unevenly — it cannot be the shard function.
- Real deployments need locality/isolation: a region, or an externally-owned network (a signal whose network and jurisdiction belong to another agency), should be groupable so its connection limits and failures stay contained.
- Operators (traffic operations, not sysadmins) must tune worker count, per-worker load, and concurrency at runtime — not via env vars or restarts (ADR-0077).
- Unbounded concurrency exhausts sockets/handles and saturates the network.
- Scale-out must need no central runtime coordinator (ADR-0012).

## Considered Options

- Shard the device inventory: even distribution by stable hash / ordinal, optional attribute partitioning, bounded async concurrency
- Raw `signal_id % WORKER_COUNT` modulo sharding
- An external job queue distributing each poll

## Decision Outcome

**Shard the enabled device inventory** (`config.signal` / the DeviceSource,
ADR-0037); the host stamps identity onto decoded events afterward (ADR-0034/ADR-0078)
— identity is never a shard input from the payload.

- **Distribution (even).** Within a shard, devices are assigned by a **stable
  cross-process hash** of the device's inventory key (`stable_hash(key) %
  WORKER_COUNT`) or an ordinal split of the enumerated inventory — never raw-value
  modulo of a sparse identifier. A worker self-selects its slice with no central
  coordinator (ADR-0012); a worker-count change reshuffles slices safely via
  checkpoints (ADR-0043).

- **Partitioning (locality/isolation), optional.** An operator may partition before
  distributing: `partition_by ∈ { none (default) | region_id | (region_id,
  jurisdiction_id) }`, with `network` deferred until a comms-endpoint/range concept
  exists. Partitioned mode uses an operator-declared **allocation map** (bucket →
  worker-set) that allows carve-outs — e.g. an externally-owned agency's signals
  fenced to their own worker so their outages and connection limits stay contained.
  It is declared static config, not a runtime coordinator. `region_id` breaks up a
  dominant owner that spans regions; `jurisdiction` is the ownership axis for authz
  and sharing (ADR-0013/ADR-0073), used for carve-outs — not as a primary load key,
  since one owner can span every region.

- **Bounded concurrency.** Within a worker, `asyncio.Semaphore(collector_max_concurrent)`
  caps simultaneous connections (async fits the I/O-bound workload; default in the
  tens, tunable higher). Partitioned by network/endpoint, this bounds per-endpoint
  connections coherently, because one worker owns the endpoint.

- **Tuning.** Worker count, devices-per-worker, concurrency, `partition_by` /
  `distribute_by`, and the allocation map are runtime-registry knobs
  (ADR-0077/ADR-0051) — UI-tunable, no restart; env is only a debug override.

### Consequences

- Good, because scale-out is even and coordinator-free even with sparse identifiers; add workers to scale.
- Good, because operators tune the whole poll fleet live; regions and externally-owned networks can be isolated; per-endpoint limits become enforceable.
- Good, because connection counts stay bounded — no socket exhaustion or network saturation.
- Bad, because a worker now needs the shard spec (from the registry), not just its identity, to compute its slice.
- Bad, because a coarse partition can hot-spot a worker at scale (e.g. a large metro region); the escalation is an operator-weighted allocation, and — only if that proves insufficient — a computed weighted assignment map, introduced when a real hot bucket appears.

### Confirmation

The poller shards the enabled inventory; distribution uses a stable hash/ordinal,
never raw id modulo; partitioning and the allocation map are operator-declared
registry knobs; a semaphore bounds concurrency; adding a worker needs no code change;
identity is host-attached (ADR-0078).

## Pros and Cons of the Options

### Shard the inventory: hash/ordinal + optional partitioning (chosen)

- Good, because it's even regardless of id density, coordinator-free, operator-tunable, and supports region/owner isolation.
- Bad, because coarse partitions can hot-spot and then need a weighted allocation.

### Raw `signal_id % WORKER_COUNT`

- Bad, because identifiers aren't sequential (ADR-0078), so modulo distributes unevenly, and it offers no locality/isolation control.

### External job queue per poll

- Bad, because it's heavy infrastructure and coordination for a division a hash/ordinal already solves.

## More Information

- ADR-0078 (TSIGMA-owned identity; host-attached, not from the payload), ADR-0077 (operator-managed tuning knobs)
- ADR-0034 (host attaches identity), ADR-0037 (DeviceSource inventory), ADR-0012 (sharded work, no central coordinator)
- ADR-0043 (checkpoint-safe reshuffle), ADR-0051 (runtime registry), ADR-0013/ADR-0073 (jurisdiction as authz/sharing axis)
