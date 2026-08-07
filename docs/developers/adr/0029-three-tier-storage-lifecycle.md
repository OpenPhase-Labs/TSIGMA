# Three-tier storage lifecycle: hot → warm → cold

- **Status**: Accepted
- **Date**: 2026-06-28
- **Deciders**: jsloan@celldara.com

## Context and Problem Statement

TSIGMA stores high-resolution events at high volume (the GDOT envelope is ~1.25B
rows/day). Keeping everything in the live database is fast but expensive; archiving
everything is cheap but slow to query. How is event data aged across
cost/performance tiers?

## Decision Drivers

- Recent data must be query-fast (dashboards, real-time analytics).
- Old data must be cheap to retain (compliance, historical reports) without bloating the hot store.
- Multi-dialect: each supported dialect has a native columnar mechanism for the warm tier (ADR-0076), so a warm tier is available on all of them.
- Retention is agency-policy-driven and must be configurable.

## Considered Options

- Three-tier lifecycle: hot (live) → warm (compressed) → cold (Parquet) → optional drop
- Single hot store only
- Hot + cold only (no warm)

## Decision Outcome

**A three-tier lifecycle:** **HOT** (recent, uncompressed, in the live DB) →
**WARM** (older, compressed in place via each dialect's **native columnar
mechanism** — ADR-0076) → **COLD** (archived as partitioned Parquet via the storage
backend, ADR-0032) → optional **drop** past retention. Tier ages are
runtime-configurable (e.g. cold after ~180 days). The **warm tier is available on
every supported dialect** (native columnar where present, app-level RLE fallback
otherwise), so the lifecycle is uniform. Raw ingested device files are retained
~90 days for reprocessing, separate from event retention.

### Consequences

- Recent data stays fast; old data is cheap; the hot store doesn't grow unbounded.
- Every dialect gets an in-DB warm tier via its native columnar mechanism (or an app-level RLE fallback) — no deployment is stuck hot → cold.
- Retention is explicit, auditable config; cold export + optional delete runs as a scheduled job.
- Cross-tier reads need tier-aware routing (ADR-0031).

### Confirmation

Tier transitions run as scheduled jobs; tier ages are runtime settings; each
dialect realizes the warm tier per ADR-0076; raw-file retention is independent of
event retention.

## Pros and Cons of the Options

### Three-tier (chosen)

- Good, because it balances speed/cost/retention and degrades cleanly without Timescale.
- Bad, because of more moving parts (transitions, routing).

### Single hot store

- Bad, because cost and scale blow up at agency volume.

### Hot + cold only

- Good, because it's simpler.
- Bad, because it loses in-place warm compression that keeps mid-age data both compact and queryable in the DB (with TimescaleDB).

## More Information

- ADR-0030 (Timescale hypertable / warm), ADR-0031 (cold Parquet + query routing), ADR-0023 (multi-dialect)
