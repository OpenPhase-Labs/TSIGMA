# Database-agnostic core via a facade and per-dialect packages

- **Status**: Accepted
- **Date**: 2026-06-28
- **Deciders**: jsloan@celldara.com

## Context and Problem Statement

TSIGMA needs persistent storage, but agencies have different database standards —
many run PostgreSQL, some are mandated to MS-SQL or Oracle, some prefer
MySQL/MariaDB. Locking to one database would fork the deployment story or exclude
agencies. How does the core support multiple backends? (This is one of the two
surfaces deliberately kept stable and portable.)

## Decision Drivers

- Broad DOT adoption requires multiple backends without forks.
- PostgreSQL is the reference target (richest features, simplest path).
- Per-DB feature checks must not leak into business logic — code writes against a uniform facade.
- Contributors should add or improve a dialect without touching business logic.
- Some features (e.g. TimescaleDB continuous aggregates) lack equivalents everywhere — degrade explicitly, never break silently.

## Considered Options

- DB-agnostic via facade + per-dialect packages (PostgreSQL reference)
- PostgreSQL only
- A full ORM that abstracts everything by configuration
- Two hand-maintained implementations

## Decision Outcome

**Database-agnostic core via a `DatabaseFacade` + per-dialect `DialectHelper`.**
The facade owns connection pooling, sessions, transactions, and dialect selection;
`DialectHelper` is a pure (engine/session-free) generator of dialect-specific SQL
(time-bucketing, lookback predicates, audit triggers, user-context). Supported
dialects: **PostgreSQL (reference), MS-SQL, Oracle, MySQL.** Rules:

- **ORM-first** — `execute()` rejects raw SQL strings; use `select()/insert()/
  update()/delete()`; `text()` only as a last resort for DDL/extension queries.
- **No dialect-specific imports outside the facade** — anything dialect-specific
  lives in `DialectHelper`.
- Capability differences are explicit; features degrade gracefully where a dialect
  can't natively support them, with operators warned at config time.

### Consequences

- Agencies adopt TSIGMA regardless of DB standard; PostgreSQL users pay no abstraction tax (thin facade, queries pass through).
- Adding a dialect is a contained change — business logic doesn't move.
- The facade is a real up-front design cost; non-PG dialects start less battle-tested.
- Some advanced features (Timescale aggregates, full-text) can't be reproduced everywhere — operators accept the tradeoff on those DBs.

### Confirmation

All data-layer code goes through the facade — no direct driver/dialect imports in
routes/reports/plugins; each dialect is a distinct implementation; `text()` usage
is rare and reviewed; PostgreSQL is the most-tested dialect.

## Pros and Cons of the Options

### Facade + per-dialect (chosen)

- Good, because it gives broad adoption without forks, a no-tax PostgreSQL reference path, and contained dialect additions.
- Bad, because of the up-front facade design cost and less-proven non-PG dialects.

### PostgreSQL only

- Bad, because it excludes non-PG agencies and forces forks.

### Full ORM by configuration

- Bad, because it hides query semantics (painful for performance, aggregates, triggers) and is an opinionated mismatch for the data shapes.

### Two hand-maintained implementations

- Bad, because only two DBs are covered and every storage change is a dual-write burden.

## More Information

- ADR-0003 (the DB is authoritative shared state), ADR-0016/0017 (dialect-generated audit triggers, append-only)
- One of the two stable/portable surfaces (the other is the gRPC contract, ADR-0018)
