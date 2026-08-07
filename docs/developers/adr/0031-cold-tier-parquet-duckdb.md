# Cold tier: partitioned Parquet with DuckDB unified query

- **Status**: Accepted
- **Date**: 2026-06-28
- **Deciders**: jsloan@celldara.com

## Context and Problem Statement

Cold (aged-out) events are exported out of the live database to cheap storage, but
must still be queryable for historical reports. How are cold events stored and
queried, across dialects?

## Decision Drivers

- Cold storage must be cheap and portable (object store / filesystem).
- Cold data must remain queryable with predicate pushdown, not full scans.
- PostgreSQL can query Parquet in-database (FDW); other dialects can't.
- Reports shouldn't have to know which tier holds the data.

## Considered Options

- Partitioned Parquet + DuckDB (PG via FDW; app-layer DuckDB elsewhere); tier-aware SDK
- Keep cold in a second relational database
- Cold is offline-only (export, no query)

## Decision Outcome

**Cold events are written as partitioned Parquet** (`{signal_id}/{date}/events.parquet`)
via the storage backend (ADR-0032), and queried with **DuckDB**. On PostgreSQL,
cold Parquet is also exposed in-database via `pg_duckdb` (preferred) /
`duckdb_fdw` / `parquet_fdw` and stitched into a unified view; on all other
dialects (MS-SQL/Oracle/MySQL) an **application-layer DuckDB reader**
(`ColdTierQuery`) reads the Parquet directly. A **tier-aware SDK** routes reads
between hot and cold (gated by `cold_tier.query_enabled`, threshold
`cold_tier.threshold_days`) so report authors and API endpoints don't think about
tiers; hot and cold return the same canonical columns so results concatenate.

### Consequences

- Cold storage is cheap, portable (filesystem/S3), and externally consumable (plain Parquet).
- DuckDB gives predicate pushdown (column / event_code / time) over Parquet — no full scans.
- PG gets a unified in-DB view; other dialects use the app-layer reader — same SDK surface.
- Cold reads can be globally disabled at runtime without a redeploy.
- Trust note: SDK-internal DuckDB SQL fragments are trusted code, never built from API input (column allowlist + int-cast inlining for anything user-derived).

### Confirmation

The cold-export job writes the partition layout; `ColdTierQuery` reads filesystem
and S3; the SDK routes hot/cold by the threshold and `query_enabled`; hot/cold
column shapes match for concatenation.

## Pros and Cons of the Options

### Parquet + DuckDB unified query (chosen)

- Good, because cold storage is cheap, portable, and still queryable with pushdown, behind one SDK surface — with a PG bonus in-DB view.
- Bad, because it adds a second query engine (DuckDB) and per-backend dispatch.

### Second relational database for cold

- Bad, because of more infrastructure that isn't cheap and isn't externally consumable.

### Offline-only cold

- Bad, because historical reports can't span the cold threshold.

## More Information

- ADR-0029 (lifecycle), ADR-0030 (hot/warm), ADR-0032 (storage backend), report SDK (forthcoming)
