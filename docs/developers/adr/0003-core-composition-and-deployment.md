# Core composition and deployment: one environment-toggled deployable

- **Status**: Accepted
- **Date**: 2026-06-28
- **Deciders**: jsloan@celldara.com

## Context and Problem Statement

ADR-0002 set the principle (a host-owned core, extended only by plugins). This
records what the core is made of and how it is deployed — not the extension
boundary or the database schema, which are the core's two faces and come later.

## Decision Drivers

- One product must run from a single small-DOT box up to a high-volume agency.
- Operational simplicity for the common single-site case.
- Ingestion is largely I/O-bound, so asyncio gives ample in-process concurrency; scale out for volume.

## Considered Options

- One codebase / one image, roles activated by environment
- Separate build artifacts per role
- Microservices from the start

## Decision Outcome

**One codebase and image; roles activated by environment.**

- **Core subsystems (the host):** data-plane owner · ingestion orchestration ·
  validation/integrity spine · config & audit · API host · scheduler.
- **Runtime:** Python 3.13, asyncio, ordered lifespan (start in dependency
  order, shut down in reverse).
- **Deployment:** `TSIGMA_ENABLE_*` flags select roles — all-in-one for small
  sites, or role-specialized replicas (API / collector / listener / scheduler) at
  scale. **Exactly one scheduler.** The database is the authoritative shared state
  (Valkey optional for sessions / cache / invalidation).

### Consequences

- One artifact for every scale; small sites pay no distributed-systems tax.
- Shared mutable state lives in the DB/Valkey, not process memory.
- Correct role wiring — especially the single scheduler — depends on env-flag discipline.

### Confirmation

One build artifact exists; role selection is configuration only; lifespan ordering
is covered by tests; a guard prevents a second scheduler from running.

## Pros and Cons of the Options

### One codebase, env-toggled roles

- Good, because one artifact scales from a single box to multi-container.
- Good, because identical code across roles — no drift.
- Bad, because the image carries code a given role won't run; needs env-flag discipline.

### Separate artifacts per role

- Good, because each artifact is minimal.
- Bad, because N build pipelines and version-skew risk across roles.

### Microservices from the start

- Good, because maximal independent scaling and isolation.
- Bad, because operationally unjustified for the common single-site DOT.

## More Information

- ADR-0002 (the core this composes)
- Later: gRPC plugin contract; database schema/abstraction
- `ARCHITECTURE.md` §3–§5, §11 — predates the gRPC change; reconcile from the ADRs
