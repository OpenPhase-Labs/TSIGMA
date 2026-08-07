# Decoding scales as a format-keyed pool, not by shard

- **Status**: Accepted
- **Date**: 2026-07-16
- **Deciders**: jsloan@celldara.com

## Context and Problem Statement

Polling shards its device inventory across workers (ADR-0039). Decoding is a
separate stage (ADR-0034/ADR-0040). Should decoding shard the same way, or scale
differently?

## Decision Drivers

- Polling is I/O-bound with per-device durable state (checkpoints, ADR-0042); decoding is CPU-bound and stateless (ADR-0034: a pure bytes → events transform).
- Decoders are format-specific and selected by content/extension (ADR-0048).
- Decode throughput must scale to the raw-payload stream independently of poll-worker count.
- Decoders are already separate processes (ADR-0018).

## Considered Options

- A format-keyed, load-balanced pool of decoder instances, scaled by backlog
- Shard decoding by the same key as polling (device/region)
- One decoder instance per format

## Decision Outcome

**Decoding scales as a load-balanced pool of format-specific decoder instances**,
not by shard. The pipeline (ADR-0040) dispatches each raw payload to any free
instance of the correct decoder (routed by format / controller type, ADR-0048); the
pool sizes to decode backlog/CPU, decoupled from poll-worker count. In `direct` mode
decode runs inline in the collector task; in `database`/`valkey` modes a separate
decoder fleet drains the queue and scales on its own curve (ADR-0040). Instances are
a **warm pool per active format** — a format with no signals runs zero instances;
the pool grows on backlog and shrinks when idle, without cold-starting a process per
payload.

### Consequences

- Good, because utilization is even (load-balanced dispatch self-levels); no hot shard from an uneven key.
- Good, because decode throughput scales to the payload stream independently of poll workers.
- Good, because a format nobody uses costs nothing, and bursts add instances.
- Bad, because queued modes require the checkpoint to advance on persist-completion, not poll-completion, so a crash between poll and a still-queued decode doesn't look "done" (never-lose-data, ADR-0034; detail in ADR-0043).

### Confirmation

Decode work is dispatched to a format-keyed pool, not assigned by a shard key; pool
size tracks backlog, not poll-worker count; unused formats run no instances;
queued-mode checkpoints advance on persist.

## Pros and Cons of the Options

### Format-keyed load-balanced pool (chosen)

- Good, because it matches the CPU-bound, stateless, format-specific nature of decode and scales independently.
- Bad, because queued modes move the checkpoint-advance point downstream.

### Shard decoding like polling

- Bad, because a stateless CPU stage gains nothing from device/region ownership and inherits hot-shard imbalance and the wrong (non-format) routing key.

### One instance per format

- Bad, because a single instance can't absorb burst or use multiple cores; it bottlenecks high-volume formats.

## More Information

- ADR-0040 (event pipeline modes / independent decoder scaling), ADR-0034 (pure decoder transform; host-owned spine)
- ADR-0048 (format selection), ADR-0018 (decoders are separate processes), ADR-0043 (checkpoint advance on persist), ADR-0039 (poll sharding — the contrasting model)
