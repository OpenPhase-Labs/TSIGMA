# Warm tier via native per-dialect columnar compression

- **Status**: Accepted
- **Date**: 2026-06-28
- **Deciders**: jsloan@celldara.com

## Context and Problem Statement

The three-tier lifecycle (ADR-0029) has a warm tier — older data kept compressed but
still queryable in the database. It was initially framed as TimescaleDB-only
(PostgreSQL). But each supported dialect has a native columnar/compression mechanism
well suited to high-resolution event data. How is the warm tier realized across
dialects?

## Decision Drivers

- Hi-Res event data is highly compressible (repetitive event codes/params — ideal for columnar / run-length encoding).
- Each supported dialect has a native columnar mechanism; using it beats a lowest-common-denominator or a hot→cold-only approach.
- The warm tier should exist on **all** supported dialects, not just PostgreSQL.
- The mechanism is a dialect concern (facade / DialectHelper, ADR-0023), invisible to queries.

## Considered Options

- Native per-dialect columnar compression for the warm tier (Timescale / CCI / HCC / HeatWave), app-level RLE fallback
- TimescaleDB-only warm tier (other dialects hot → cold)
- App-level compression everywhere

## Decision Outcome

**The warm tier uses each dialect's native columnar/compression mechanism:**

- **PostgreSQL** — TimescaleDB hypertable compression (ADR-0030).
- **MS-SQL** — Clustered Columnstore Index (CCI).
- **Oracle** — Hybrid Columnar Compression (HCC).
- **MySQL** — HeatWave columnar acceleration where available; **app-level RLE** otherwise.

Where a deployment lacks a native mechanism (e.g. plain PostgreSQL without
TimescaleDB, or MySQL without HeatWave), an **app-level RLE fallback** provides the
warm tier — so no deployment is stuck hot→cold. The event-log schema and queries are
identical across dialects (ADR-0023/0024); the warm-tier mechanism is selected and
managed by the dialect layer. Hi-Res events compress heavily (repetitive
codes/params), so columnar/RLE yields large space savings while keeping mid-age data
queryable in the DB.

### Consequences

- Every supported dialect gets an in-DB warm tier — no dialect is stuck hot→cold.
- Each engine's best-in-class columnar path is used (no lowest-common-denominator).
- The cold tier (Parquet/DuckDB, ADR-0031) still handles aged-out data uniformly.
- Each dialect's warm mechanism must be implemented/tested in its DialectHelper; the app-level RLE fallback is less efficient than the native paths.

### Confirmation

Each dialect implements its warm-tier columnar mechanism in the dialect layer; the
event schema/queries don't change per dialect; deployments without a native mechanism
use the app-level RLE fallback; aged data still flows to the cold tier.

## Pros and Cons of the Options

### Native per-dialect columnar + RLE fallback (chosen)

- Good, because there's a warm tier everywhere, using each engine's best columnar path, with a schema that doesn't vary per dialect.
- Bad, because there are four native mechanisms to implement/test and the MySQL/plain-PG RLE fallback is weaker.

### TimescaleDB-only warm tier

- Bad, because non-PG dialects lose the warm tier (hot→cold), wasting hot storage.

### App-level compression everywhere

- Bad, because it reinvents what the engines do natively — worse performance/space than CCI/HCC/Timescale.

## More Information

- ADR-0029 (three-tier lifecycle — the warm tier this realizes), ADR-0030 (Timescale is PostgreSQL's mechanism), ADR-0023 (dialect layer selects the mechanism), ADR-0031 (cold tier)
