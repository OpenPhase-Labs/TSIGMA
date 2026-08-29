# Validation: in-process, three-layer, flag-never-block

- **Status**: Accepted
- **Date**: 2026-06-28
- **Amended**: 2026-08-29 — validation is core, not a gRPC plugin subsystem
- **Deciders**: jsloan@celldara.com

## Context and Problem Statement

Ingested events vary in trustworthiness; some checks are cheap and deterministic,
others need models or cross-signal context. Per the integrity spine (ADR-0034),
validation flags but never blocks. How is validation structured, and does it sit
inside the host or across the plugin boundary (ADR-0018)?

## Decision Drivers

- Validation must never withhold data (ADR-0034 never-lose-data) — it flags.
- Checks range from cheap/deterministic to expensive (ML / cross-signal).
- Agencies should adopt deeper validation incrementally.
- Validators should be extensible without core changes.
- The host attaches `validation_metadata` (ADR-0047) and is accountable for it. A
  judgment the host did not compute is one it cannot stand behind.
- A validator that silently passes everything is indistinguishable from clean
  data — the failure is undetectable from outside.

## Considered Options

- In-process validator registry in three layers, per-layer toggle; deterministic inline, heavier async
- A gRPC validator plugin subsystem, like decoders and methods (ADR-0018)
- One monolithic validator
- No validation (trust the data)

## Decision Outcome

**Validation is core**, stratified into three independently toggleable layers:

- **Layer 1 — deterministic** (schema / range, e.g. NTCIP 1202 bounds). Cheap, no
  dependencies. Runs **inline** in the spine between decode and persist, attaching
  metadata and flagging — never blocking (ADR-0034). Implemented now.
- **Layer 2 — temporal / anomaly** (per-signal models). Runs **asynchronously**
  after persist. Deferred (needs signal-level models).
- **Layer 3 — cross-signal / corridor** (relationships across signals). Async
  after persist. Deferred (needs corridor configuration).

Validators flag with a status; they never reject. Layers 2/3 stay gated off until
their prerequisites exist.

Validators register in the in-process `ValidationRegistry` and are first-party or
host-supervised code. Validation is **not** a wire subsystem: it has no `.proto`
in the contract, loads no third-party binary, and is never delegated across the
gRPC plugin boundary. It shares the registry *shape* of the wire subsystems,
which is why earlier prose — including this ADR before its amendment — listed it
among them.

### Consequences

- A deterministic quality signal is attached at ingest; deeper validation enriches later, off the hot path.
- Agencies enable layers incrementally as prerequisites (models, corridor config) land.
- New validators are added to the registry without changes to the ingest spine.
- A given event's validation completeness varies by deployment (recorded in metadata, ADR-0047).
- An **untrusted** vendor binary cannot supply, replace, or disable a validator. Custom validators are first-party or host-supervised code; today, with no validation `.proto`, that means adding one is a change to the host rather than an install.

### Confirmation

Layer 1 runs inline and flags (never blocks); Layers 2/3 run async and are
toggle-gated; rejection never happens — only flagging. No validation service
appears in `tsigma/plugins/`, and no validation `.proto` exists in the contract.

## Pros and Cons of the Options

### In-process, three-layer, flag-never-block (chosen)

- Good, because there's a cheap signal now and deeper validation later, with incremental adoption, extensibility, and no data loss.
- Good, because the host computes every judgment it attests to in `validation_metadata`.
- Bad, because there are multiple layers/timings to coordinate and completeness varies by deployment.
- Bad, because a vendor with a genuinely better detector has no way to ship it today — there is no validation `.proto` for it to implement.

### A gRPC validator plugin subsystem

- Bad, because it hands an untrusted binary the decision about whether data is trustworthy, and a pass-everything validator cannot be detected by the host.
- Bad, because the host would attach `validation_metadata` (ADR-0047) asserting a judgment it did not make.
- Bad, because Layer 1 runs inline on the hot path between decode and persist; a per-event RPC there is the wrong shape.

### One monolithic validator

- Bad, because it couples cheap and expensive checks and blocks incremental adoption.

### No validation

- Bad, because there's no quality signal — it defeats the integrity purpose.

## More Information

- ADR-0034 (flag-never-block spine, host owns persistence), ADR-0047 (validation_metadata), ADR-0008 (status vocabulary)
- ADR-0079 (the database access layer is likewise in-process, not a gRPC plugin) — the same reasoning applied to a different subsystem
- ADR-0082 (the host-owned integrity spine stays in TSIGMA when plugins leave)
- TSIGMA Contract ADR-0011 and `PROTOCOL.md` section 6: validation is core, with no wire contract
- **Open, not decided here:** whether a *privileged-tier* validator contract should exist. Contract ADR-0011 bars untrusted vendor binaries, not host-supervised ones, and contract ADR-0007 already defines a privileged tier (auth, storage) whose registration is gated by the **deploying operator's** trust anchor — not by OpenPhase Labs. Whoever runs the deployment decides what it trusts; a third party running their own build sets their own policy. That route needs a validation `.proto` and depends on contract ADR-0016, the privileged-tier enforcement mechanism, which is still Open. This ADR rules only that validation is not an untrusted-vendor wire subsystem in TSIGMA's own build.
- Amendment note: as first written this ADR cited ADR-0018 and called validation "a plugin subsystem". The implementation never did that — `ValidationRegistry` has always been in-process — and the contract rules it out. The three-layer decision is unchanged; only the plugin classification is corrected.
