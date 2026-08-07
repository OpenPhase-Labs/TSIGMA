# Dual-mode aggregate maintenance: continuous aggregates or scheduled refresh

- **Status**: Accepted
- **Date**: 2026-06-28
- **Deciders**: jsloan@celldara.com

## Context and Problem Statement

Pre-computed aggregates (ADR-0052) must be kept fresh as events arrive. TimescaleDB
offers continuous aggregates (automatic, incremental); other databases don't. How
are aggregates maintained across dialects without forking the schema?

## Decision Drivers

- TimescaleDB provides incremental continuous aggregates; plain PostgreSQL and other dialects don't.
- The aggregate tables and the API reading them should be identical regardless of how they're maintained.
- Multi-dialect support (ADR-0023) must not fragment the analytics schema.

## Considered Options

- Dual-mode: continuous aggregates with TimescaleDB; scheduled delete-reinsert otherwise; one shared schema
- Continuous aggregates only (TimescaleDB required)
- Scheduled refresh only (never use continuous aggregates)

## Decision Outcome

**One aggregate schema, two maintenance modes:**

- **With TimescaleDB** — TimescaleDB **continuous aggregates** maintain the rollups automatically and incrementally (refresh policy).
- **Without TimescaleDB** (plain PostgreSQL or MS-SQL/Oracle/MySQL) — a **scheduled delete-reinsert job** recomputes a sliding window (e.g. the last N minutes) on the scheduler (ADR-0003).

The aggregate **table shape is identical** in both modes, so reports and the API
read the same tables regardless of maintenance mechanism; the mechanism is chosen by
TimescaleDB presence / dialect via the facade (ADR-0023).

### Consequences

- TimescaleDB deployments get automatic incremental rollups; others get correct (if heavier) scheduled refresh.
- Reports/API are agnostic to the maintenance mode — one query path.
- The scheduled mode recomputes a window (more work than incremental); window size trades freshness vs cost.
- Two maintenance code paths to maintain.

### Confirmation

Aggregate tables have one shape; TimescaleDB uses continuous aggregates;
non-Timescale uses a scheduled delete-reinsert window job; API/report reads don't
branch on mode.

## Pros and Cons of the Options

### Dual-mode, one schema (chosen)

- Good, because it works on every dialect, keeps an identical read path, and uses the best mechanism per deployment.
- Bad, because there are two maintenance paths and the scheduled mode is heavier.

### Continuous aggregates only

- Bad, because it requires TimescaleDB everywhere — excluding other dialects and plain PostgreSQL.

### Scheduled refresh only

- Bad, because it forgoes TimescaleDB's efficient incremental aggregates where available.

## More Information

- ADR-0052 (which analytics are pre-computed), ADR-0030 (TimescaleDB hypertable), ADR-0023 (dialect selection), ADR-0003 (scheduler runs the refresh jobs)
