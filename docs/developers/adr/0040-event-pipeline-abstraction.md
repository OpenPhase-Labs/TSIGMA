# Event pipeline abstraction: direct, database, valkey

- **Status**: Accepted
- **Date**: 2026-06-28
- **Deciders**: jsloan@celldara.com

## Context and Problem Statement

After decode, events flow to validation and persistence. Small deployments want
this in-process; larger ones want decode/validate to retry failures and scale
independently. How is the post-decode event flow structured?

## Decision Drivers

- Most deployments are fine with in-process flow (simplest).
- Larger deployments want failed-decode retries and independent decoder scaling.
- The mechanism should be swappable without changing method/decoder code.

## Considered Options

- An EventPipeline abstraction with three modes (direct / database / valkey)
- In-process only
- Always use an external queue

## Decision Outcome

**An `EventPipeline` abstraction with three modes:**

- **direct** (default) — in-process: decode → validate → persist in the same task. Simplest; right for most deployments.
- **database** — a database-backed work queue between stages, using the configured dialect via the facade (ADR-0023) — works on any supported database, not just PostgreSQL; enables retries and decoupling without extra infrastructure.
- **valkey** — Valkey streams between stages; higher-throughput decoupling and independent decoder scaling.

The mode is configured; methods and decoders don't change across modes.

### Consequences

- Small deployments stay simple (direct, no extra infrastructure).
- Larger deployments get failed-decode retries and independent scaling (database/valkey) without rewriting plugins.
- Three modes to maintain and test.

### Confirmation

The pipeline mode is a config switch; method/decoder code is mode-agnostic;
database/valkey modes support retry of failed decodes; the database mode uses the
facade/dialect, not PostgreSQL-specific features.

## Pros and Cons of the Options

### EventPipeline, three modes (chosen)

- Good, because the default is simplest and scale/retry is available when needed, transparently to plugins.
- Bad, because there are three modes to maintain.

### In-process only

- Bad, because there's no retry/decoupling for large deployments.

### Always external queue

- Bad, because it forces infrastructure on small deployments that don't need it.

## More Information

- ADR-0034 (decode→validate→persist spine), ADR-0012 (DB/Valkey shared state), ADR-0035/0036 (methods/orchestrators)
