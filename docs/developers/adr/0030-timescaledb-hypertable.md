# TimescaleDB hypertable for the event log (PostgreSQL + TimescaleDB; daily chunks)

- **Status**: Accepted
- **Date**: 2026-06-28
- **Deciders**: jsloan@celldara.com

## Context and Problem Statement

The event log is the highest-volume table. TimescaleDB — an *optional* PostgreSQL
extension, not part of core PostgreSQL — provides time partitioning, compression,
and incremental aggregates. How is the event table physically organized, and what
happens when TimescaleDB isn't present (plain PostgreSQL or another dialect)?

## Decision Drivers

- Time-series partitioning + compression are essential at agency volume.
- TimescaleDB is an optional PostgreSQL extension (it runs only on PostgreSQL, and only when installed) — not core PostgreSQL.
- Chunk width affects compression speed and query performance.
- The schema must also work on plain PostgreSQL and on MS-SQL / Oracle / MySQL.

## Considered Options

- Hypertable where TimescaleDB is present (daily chunks); plain/partitioned table otherwise (incl. plain PostgreSQL)
- Plain table everywhere (never use Timescale)
- Manual application-level partitioning

## Decision Outcome

**With the TimescaleDB extension (on PostgreSQL), the event log is a hypertable**
partitioned by `event_time`, default **1-day chunk interval** (tunable via a
runtime setting), with the warm tier driven by Timescale compression policies
(ADR-0029). Daily chunks compress faster and use less CPU than weekly at the
agency envelope. **Without TimescaleDB — plain PostgreSQL or any other dialect —
the same logical table is a regular table** (with native partitioning where
available). Timescale is PostgreSQL's warm-tier mechanism; other dialects realize
the warm tier with their own native columnar compression (ADR-0076), and
continuous-aggregate maintenance falls back to scheduled refresh (ADR-0053). The
hypertable/dialect difference is handled by the facade/dialect (ADR-0023), not
business logic.

### Consequences

- Deployments with TimescaleDB get automatic partitioning, compression, and a path to continuous aggregates.
- The chunk interval is tunable post-deploy (affects only future chunks).
- Deployments without TimescaleDB (plain PostgreSQL or other dialects) still function with a plain table; they forgo Timescale optimizations.
- Reports and queries target the same logical table regardless of dialect.

### Confirmation

With TimescaleDB the event log is a hypertable with the configured chunk interval;
without it (plain PG or other dialects) a plain/partitioned table is used; no
Timescale-specific SQL appears outside the dialect layer.

## Pros and Cons of the Options

### Hypertable with TimescaleDB, plain table otherwise (chosen)

- Good, because it gives best-in-class time-series where TimescaleDB is installed while still working without it.
- Bad, because of feature asymmetry between TimescaleDB and non-TimescaleDB deployments (explicit, not silent).

### Plain table everywhere

- Bad, because it forgoes compression/partitioning/continuous-aggregates even where TimescaleDB is available (the reference deployment).

### Manual application-level partitioning

- Bad, because it reinvents what Timescale / native partitioning already do, and it's error-prone.

## More Information

- ADR-0029 (tier lifecycle / warm), ADR-0076 (per-dialect warm-tier columnar compression), ADR-0023 (facade/dialect), ADR-0009 (event model)
