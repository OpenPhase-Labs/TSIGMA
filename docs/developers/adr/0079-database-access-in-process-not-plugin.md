# The database access layer is in-process, not a gRPC plugin

- **Status**: Accepted
- **Date**: 2026-07-16
- **Deciders**: jsloan@celldara.com

## Context and Problem Statement

Every extensible subsystem is a separate-process gRPC plugin (ADR-0018), and the
database layout is one of the two portable surfaces (ADR-0023). Should database
access itself also be a gRPC service/plugin, or stay an in-process facade?

## Decision Drivers

- Database access is on the hottest path (bulk event ingest, analytical reads at billions of rows).
- Models are the schema and API contract with no DTO layer (ADR-0024); routes use the ORM session directly (ADR-0027).
- Persistence spans multiple statements per transaction (event + audit together, ADR-0034/ADR-0015).
- gRPC plugins exist for independent lifecycle, fault isolation, language independence, and third-party extensibility — none of which the DB layer needs.

## Considered Options

- In-process `DatabaseFacade` + per-dialect `DialectHelper` (ADR-0023), behind the plugin boundary
- A gRPC data-access service the core and plugins call
- Plugins hold their own DB drivers/connections

## Decision Outcome

**Database access stays in-process** via the `DatabaseFacade` (ADR-0023), living
inside the host behind the gRPC plugin boundary (ADR-0018). The gRPC seam is the
plugin edge; the core owns persistence (ADR-0034). Plugins reach data by handing
events to, or requesting data from, the core over gRPC — they never open their own
DB connections, and there is no core → DB gRPC hop.

### Consequences

- Good, because there is no serialization tax on the hot path; PostgreSQL keeps ADR-0023's "queries pass through" property.
- Good, because ORM session semantics (unit-of-work, multi-statement transactions, relationships) are preserved with no proto/DTO layer (ADR-0024).
- Good, because one process owns the transaction that writes an event and its audit (ADR-0015/ADR-0034).
- Bad, because plugins cannot query the DB directly; rich data needs must be served by core RPCs (report/query surfaces).

### Confirmation

No direct driver/dialect imports outside the facade — including in plugins
(ADR-0023); no gRPC service fronts the database; plugins obtain data only via core
gRPC surfaces.

## Pros and Cons of the Options

### In-process facade behind the plugin boundary (chosen)

- Good, because it's fast, preserves ORM/transaction semantics, and adds no redundant boundary.
- Bad, because plugins must go through the core for data rather than querying directly.

### gRPC data-access service

- Bad, because it serializes every row on the hot path, forces a DTO layer (contra ADR-0024), and breaks multi-statement transactions across an RPC boundary.

### Plugins hold their own DB connections

- Bad, because it scatters credentials/dialect logic into plugins (contra ADR-0023/ADR-0034) and lets plugins bypass the host-owned integrity spine.

## More Information

- ADR-0018 (gRPC plugin boundary), ADR-0023 (facade + per-dialect), ADR-0024 (no DTO), ADR-0027 (no repository layer)
- ADR-0034 (host owns persistence), ADR-0032 (the pluggable *file* storage backend is a plugin; the relational store is not)
