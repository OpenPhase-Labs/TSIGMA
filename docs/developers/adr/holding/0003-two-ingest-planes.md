# ADR-0003: Two ingest planes (legacy poll + next-gen NATS)

- Status: Accepted
- Date: 2026-06-27
- Deciders: Jim Sloan

## Context

Legacy controllers (Intelight/MaxTime XML, ASC/3, D4, SEPAC, ...) are polled
and emit verbose vendor formats. Next-gen OpenPhase controllers (SOMA/APEX)
emit native Indiana-HiRes Protobuf over NATS/JetStream and buffer days locally.

## Decision

Two ingest planes feed **one** HiRes store:

1. **Legacy poll plane.** A host-driven rolling-queue poll (cadence/rate owned
   by the host) -> method (poller) plugin fetches raw bytes -> decoder plugin
   turns bytes into HiRes -> host validates and persists.
2. **Next-gen push plane.** SOMA/APEX publish native Protobuf over
   NATS/JetStream; a durable host consumer ingests them. **No decoder** - the
   payload is already canonical.

Reports and analytics are **transport-agnostic** on the shared store.

## Rationale

- Decode + the verbose->compact (~15-30x) collapse only exist on the legacy
  edge; everything downstream is compact HiRes.
- Different transports need different integrity mechanisms (see ADR-0004): poll
  self-heals by re-pull; NATS needs JetStream's acked/durable/replay.
- The firehose never crosses a plugin boundary - in-process scheduler jobs /
  TimescaleDB continuous aggregates consume the store. At the GDOT envelope
  (~9000 signals, ~1.25B rows/day) the rolling queue is a steady ~14.5k
  rows/sec, comfortably in budget.

## Consequences

- Decoder plugins are a **legacy-bridge** class only; next-gen needs none.
- The "method" subsystem covers both poll and listen.
- never-lose-data is realized differently per plane (ADR-0004).

## Related

- `INGESTION.md`, `LISTENERS.md`, `HIGH_CONCURRENCY_POLLING.md`
- ADR-0002 (decoder/method are plugins), ADR-0004 (integrity)
