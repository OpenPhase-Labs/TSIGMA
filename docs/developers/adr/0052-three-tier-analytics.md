# Three-tier analytics: pre-computed aggregates, on-demand API, scheduled jobs

- **Status**: Accepted
- **Date**: 2026-06-28
- **Deciders**: jsloan@celldara.com

## Context and Problem Statement

Analytics range from fixed dashboard rollups queried constantly, to ad-hoc queries
with custom parameters, to business logic that runs on a schedule and emits alerts.
Computing everything on demand is slow; pre-computing everything is rigid. Where
should a given analytic live?

## Decision Drivers

- Dashboard rollups are queried repeatedly with stable shapes — pre-compute pays off.
- Some queries need arbitrary parameters (date range, signal set) — pre-computing every combination is impossible.
- Some analytics are business logic with side effects (thresholds → alerts, caching, external pushes) — they fit a scheduled job.
- Report computation itself lives in report plugins (ADR-0004 not-kitchen-sink), not the core.

## Considered Options

- Three tiers: pre-computed aggregates / on-demand API / scheduled jobs (choose per analytic)
- Compute everything on demand
- Pre-compute everything

## Decision Outcome

**Analytics are placed in one of three tiers by fit:**

- **Pre-computed aggregates** — heavy, repeated rollups with stable shapes (volume, occupancy, splits) kept materialized and read fast (maintained per ADR-0053).
- **On-demand API queries** — custom-parameter queries computed at request time against hot/cold tiers (the tier-aware SDK, ADR-0031), guarded by lookback/aggregation limits.
- **Scheduled jobs** — business logic with side effects (anomaly thresholds → alerts, caching, external integration) on the scheduler (ADR-0003, one scheduler).

Decision lens: repeated stable rollup → aggregate; arbitrary parameters →
on-demand; logic-with-side-effects/alerting → job. Heavy report computation is
owned by report plugins, not the core.

### Consequences

- Dashboards read pre-aggregated data fast; ad-hoc queries stay flexible; alerting logic has a home.
- Each analytic gets the cheapest mechanism that fits.
- Three mechanisms to understand; choosing the wrong tier is a (correctable) design call.

### Confirmation

Repeated rollups are materialized aggregates; custom-parameter analytics are
on-demand SDK queries with guards; side-effect/alerting analytics are scheduled
jobs; report math lives in report plugins.

## Pros and Cons of the Options

### Three tiers (chosen)

- Good, because each analytic gets the right cost/flexibility — fast dashboards, flexible ad-hoc, a clear home for alerting.
- Bad, because there are three mechanisms and a per-analytic placement decision.

### Everything on demand

- Bad, because heavy repeated rollups are slow and expensive every time.

### Pre-compute everything

- Bad, because it can't cover arbitrary parameters and is rigid and wasteful.

## More Information

- ADR-0053 (how aggregates are maintained), ADR-0031 (tier-aware on-demand SDK), ADR-0003 (scheduler), ADR-0004 (report computation in plugins)
