# Per-event validation metadata (JSONB, worst-status merge)

- **Status**: Accepted
- **Date**: 2026-06-28
- **Deciders**: jsloan@celldara.com

## Context and Problem Statement

Multiple validators (across layers, ADR-0046) each produce a result for an event.
Where do those results live, and how is an overall status derived?

## Decision Drivers

- Per-validator results must attach to the event without exploding the schema.
- An at-a-glance overall status is needed for queries and dashboards.
- Validation completeness varies (layers toggle; async layers arrive later).

## Considered Options

- One JSONB `validation_metadata` column per event + worst-status merge
- A column per validator
- A separate validation-results table

## Decision Outcome

**A single `validation_metadata` JSONB column on the event** holds per-validator
results (validator → status + detail). An overall status is the **worst** across
validators, ordered `unvalidated < clean < suspect < invalid`, so anomalies and
hard failures surface. As async layers (ADR-0046) complete, they merge their
results in and the overall status is recomputed.

### Consequences

- Per-validator detail travels with the event; no schema change per new validator.
- The worst-status rollup gives a single queryable quality flag.
- Async layers update the metadata after persist — an event's status can sharpen over time.
- JSONB queries for a specific validator's result are less efficient than typed columns (acceptable; the rollup covers the common query).

### Confirmation

Events carry a `validation_metadata` JSONB; overall status is the worst of the
present validators by the defined ordering; async-layer results merge in and
recompute.

## Pros and Cons of the Options

### JSONB + worst-status (chosen)

- Good, because there's no schema explosion, a single rollup flag, and room for variable/late validators.
- Bad, because per-validator JSONB queries are less efficient than typed columns.

### Column per validator

- Bad, because of schema churn on every new validator and sparse columns.

### Separate results table

- Bad, because it adds a join per event for a small, event-local payload.

## More Information

- ADR-0046 (validators/layers that write metadata), ADR-0034 (flag-not-block), ADR-0008 (status vocabulary), ADR-0010 (typed + JSONB pattern)
