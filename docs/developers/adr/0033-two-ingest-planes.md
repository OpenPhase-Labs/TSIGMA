# Two ingest planes feeding one event store (legacy poll; future push)

- **Status**: Accepted
- **Date**: 2026-06-28
- **Deciders**: jsloan@celldara.com

## Context and Problem Statement

TSIGMA ingests from two very different sources: legacy controllers
(Intelight/MaxTime XML, ASC/3, D4, SEPAC, …) that are polled and emit verbose
vendor formats, and — in the future — next-gen controllers that emit native
canonical Protobuf over a message bus. How is ingestion structured across both?

## Decision Drivers

- Legacy controllers must be polled and their verbose vendor formats decoded.
- Future next-gen controllers will emit the canonical HiRes shape natively (no decode).
- Downstream (storage, analytics, reports) should not care how data arrived.
- Different transports need different integrity mechanisms (ADR-0034).

## Considered Options

- Two planes (poll + push) feeding one shared event store
- One poll-only plane
- Force everything through one transport

## Decision Outcome

**Two ingest planes feed one shared HiRes event store:**

1. **Legacy poll plane (built today):** a host-driven rolling-queue poll — a
   method (poller) plugin fetches raw bytes → a decoder plugin turns bytes into
   canonical HiRes events → the host validates and persists. Decode and the
   verbose→compact (~15–30×) collapse exist only on this legacy edge.
2. **Next-gen push plane (designed-for, future):** next-gen controllers publish
   the canonical Protobuf (OPENPHASE, ADR-0009) over NATS/JetStream; a durable
   host consumer ingests it. **No decoder** — the payload is already canonical.

Reports and analytics are **transport-agnostic** on the shared store. The
firehose never crosses a plugin boundary downstream — in-process scheduler jobs /
continuous aggregates consume the store directly.

### Consequences

- Decode + the verbose→compact collapse live only at the legacy edge; everything downstream is compact canonical HiRes.
- Decoder plugins are a legacy-bridge class only; the future push plane needs none.
- The "method" subsystem covers both poll and listen.
- never-lose-data is realized differently per plane (ADR-0034): poll self-heals by re-pull; the push plane needs JetStream's acked/durable/replay.
- Only the poll plane exists today; the push plane is future-gated until next-gen hardware ships.

### Confirmation

Poll plane: method → decoder → validate → persist. Push plane (when built):
durable JetStream consumer → validate → persist, no decoder. Reports/analytics read
the shared store without knowing the plane.

## Pros and Cons of the Options

### Two planes, one store (chosen)

- Good, because each transport is handled optimally, downstream is uniform, and decode is isolated to the legacy edge.
- Bad, because there are two ingest paths to build/test (poll now, push later).

### Poll-only

- Bad, because it can't natively consume future push controllers and forces polling semantics on a streaming source.

### One forced transport

- Bad, because it either forces a broker on simple polled devices or forces streaming sources into polling — a wrong fit both ways.

## More Information

- ADR-0034 (per-plane integrity), ADR-0009 (canonical event model), ADR-0018 (method/decoder plugins)
- The push plane (NATS) is future, not present.
