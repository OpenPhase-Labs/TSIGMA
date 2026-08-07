# Database schema layout: four logical schemas

- **Status**: Accepted
- **Date**: 2026-06-28
- **Deciders**: jsloan@celldara.com

## Context and Problem Statement

TSIGMA's tables span distinct concerns — reference/config data, time-series events,
pre-computed aggregates, and identity/auth. How are they organized in the database?

## Decision Drivers

- Logical isolation of concerns (reference vs time-series vs pre-computed vs auth) eases evolution and permissions.
- Multi-dialect: not every database has the same schema/namespace concept.
- Clear boundaries help backups, retention, and access scoping.

## Considered Options

- Four logical schemas (`config`, `events`, `aggregation`, `identity`)
- One flat schema/namespace
- A schema per table group beyond four

## Decision Outcome

**Four logical schemas on databases that support them (PostgreSQL, MS-SQL,
Oracle): `config`, `events`, `aggregation`, `identity`.** On MySQL (no equivalent
cross-schema concept), the same separation is achieved by **table-name prefixing**
within a single database. Schema qualification is handled through the
facade/dialect (ADR-0023), not hardcoded in business logic.

### Consequences

- Concerns are isolated — high-volume time-series (`events`) separate from low-volume reference (`config`) and auth (`identity`).
- Retention, backup, and permissions can target a schema.
- MySQL uses prefixes for the same logical separation — the dialect hides the difference.
- Cross-schema queries must qualify names (via the facade).

### Confirmation

Tables live in their logical schema (or prefixed group on MySQL); schema
qualification goes through the dialect; no hardcoded schema names in business logic.

## Pros and Cons of the Options

### Four logical schemas (chosen)

- Good, because of clean isolation, targetable retention/permissions, and a mapping that mirrors the domains.
- Bad, because cross-schema queries need qualification and MySQL needs the prefix fallback.

### One flat namespace

- Bad, because there's no isolation and permissions/retention scoping is harder.

### Many schemas

- Bad, because it over-fragments for little gain and adds cross-schema friction.

## More Information

- ADR-0023 (facade/dialect handles qualification), ADR-0015 (per-domain audit tables)
