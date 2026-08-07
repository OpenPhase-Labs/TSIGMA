# Validation: plugin-based, three-layer, flag-never-block

- **Status**: Accepted
- **Date**: 2026-06-28
- **Deciders**: jsloan@celldara.com

## Context and Problem Statement

Ingested events vary in trustworthiness; some checks are cheap and deterministic,
others need models or cross-signal context. Per the integrity spine (ADR-0034),
validation flags but never blocks. How is validation structured?

## Decision Drivers

- Validation must never withhold data (ADR-0034 never-lose-data) — it flags.
- Checks range from cheap/deterministic to expensive (ML / cross-signal).
- Agencies should adopt deeper validation incrementally.
- Validators should be extensible like other subsystems (ADR-0018).

## Considered Options

- Plugin-based validators in three layers, per-layer toggle; deterministic inline, heavier async
- One monolithic validator
- No validation (trust the data)

## Decision Outcome

**Validation is a plugin subsystem (ADR-0018) stratified into three independently
toggleable layers:**

- **Layer 1 — deterministic** (schema / range, e.g. NTCIP 1202 bounds). Cheap, no
  dependencies. Runs **inline** in the spine between decode and persist, attaching
  metadata and flagging — never blocking (ADR-0034). Implemented now.
- **Layer 2 — temporal / anomaly** (per-signal models). Runs **asynchronously**
  after persist. Deferred (needs signal-level models).
- **Layer 3 — cross-signal / corridor** (relationships across signals). Async
  after persist. Deferred (needs corridor configuration).

Validators flag with a status; they never reject. Layers 2/3 stay gated off until
their prerequisites exist.

### Consequences

- A deterministic quality signal is attached at ingest; deeper validation enriches later, off the hot path.
- Agencies enable layers incrementally as prerequisites (models, corridor config) land.
- New validators are added as plugins without core changes.
- A given event's validation completeness varies by deployment (recorded in metadata, ADR-0047).

### Confirmation

Layer 1 runs inline and flags (never blocks); Layers 2/3 run async and are
toggle-gated; validators register as plugins; rejection never happens — only flagging.

## Pros and Cons of the Options

### Plugin-based, three-layer, flag-never-block (chosen)

- Good, because there's a cheap signal now and deeper validation later, with incremental adoption, extensibility, and no data loss.
- Bad, because there are multiple layers/timings to coordinate and completeness varies by deployment.

### One monolithic validator

- Bad, because it couples cheap and expensive checks and blocks incremental adoption.

### No validation

- Bad, because there's no quality signal — it defeats the integrity purpose.

## More Information

- ADR-0034 (flag-never-block spine), ADR-0018 (validators are plugins), ADR-0047 (validation_metadata), ADR-0008 (status vocabulary)
