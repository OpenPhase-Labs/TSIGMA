# Typed columns + namespaced JSONB metadata

- **Status**: Accepted
- **Date**: 2026-06-28
- **Deciders**: jsloan@celldara.com

## Context and Problem Statement

TSIGMA models signals, approaches, detectors, and roadside sensors, each with
attributes the core reasons about (UI, validation, indexing, routing, scoping).
Plugins — often closed vendor decoders/methods — want to attach per-device state
(firmware, health flags, last-poll time) without core schema migrations.

## Decision Drivers

- The core needs typed columns for anything it reasons about: queries, joins, indexing, jurisdiction scoping, source-IP routing.
- Closed plugins must ship per-device state without core schema migrations.
- A zero-plugin install must still have first-class device records.
- New device classes appear over the system's lifetime.

## Considered Options

- Typed table per family + JSONB `metadata` on every row (hybrid)
- Pure JSONB / EAV
- Pure typed schema (no extensibility)
- Per-plugin sidecar tables

## Decision Outcome

**Hybrid: typed tables per family + a JSONB `metadata` column on every device row,
namespaced by plugin id.** Typed columns cover everything the core reasons about
(e.g. the signal network triple `ip/port/protocol`, jurisdiction); `metadata`
holds plugin/type-specific extension data.

Boundary rule: **the core never reads `metadata` to make decisions** — it stores
and returns it; plugins own their namespaced slice. If the core needs to reason
about a value, promote it to a typed column. (TSIGMA already does this: signal
typed columns + a `collection` JSONB namespace.)

### Consequences

- Well-known families get indexed queries, joins, validated writes, typed UI bindings.
- Vendors ship closed plugins carrying per-device state without touching the core schema; namespacing prevents collisions.
- Discipline required: once core logic branches on `metadata`, the boundary is violated — promote to a column.

### Confirmation

Each family is a typed table; every device row has a JSONB `metadata`; review
rejects core logic branching on `metadata`; plugin docs specify the namespacing.

## Pros and Cons of the Options

### Hybrid (chosen)

- Good, because it gives type safety + queryability where the core cares, plugin extensibility without migrations, and a clean ownership boundary.
- Bad, because per-family tables need up-front design and the no-read-metadata discipline must be enforced.

### Pure JSONB / EAV

- Bad, because it loses type safety, indexing, and clean scoping, and makes joins/aggregates painful.

### Pure typed schema (no extensibility)

- Bad, because every vendor plugin needs a core migration — defeating the plugin model.

### Per-plugin sidecar tables

- Bad, because the core must provision/migrate plugin tables, and install/uninstall and cross-plugin joins get messy.

## More Information

- ADR-0006 (jurisdiction scoping within the install); forthcoming: gRPC plugin contract
