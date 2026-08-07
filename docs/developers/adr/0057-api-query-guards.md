# API query guards: max-lookback and max-aggregation limits

- **Status**: Accepted
- **Date**: 2026-06-28
- **Deciders**: jsloan@celldara.com

## Context and Problem Statement

Event/analytics endpoints can be asked for enormous ranges (years of
high-resolution events across many signals), which can exhaust resources or span
the cold tier expensively. How are expensive queries bounded?

## Decision Drivers

- Unbounded lookback/aggregation can exhaust memory/CPU or hammer the cold tier.
- Limits must be operator-tunable (different agencies, different capacity).
- Guards must apply before any read (fail fast), on both REST and GraphQL.

## Considered Options

- Configurable max-lookback + max-aggregation guards enforced pre-read
- No limits (trust callers)
- Hard-coded limits

## Decision Outcome

**Configurable query guards enforced before any read:** `api.max_lookback_days`
(how far back a query may reach) and `api.max_aggregation_days` (the widest span a
single aggregation may cover), plus a clamped max page size. They are runtime
settings (ADR-0051), applied at the API layer before touching hot or cold storage,
on **both REST and GraphQL**. A request exceeding a guard is rejected with a clear
RFC-7807 error (ADR-0056).

### Consequences

- A single query can't exhaust resources or run away across the cold tier.
- Operators tune the limits to their capacity at runtime (no redeploy).
- Guards apply uniformly to both API surfaces.
- Legitimate large exports must page or use a dedicated export path, not one giant query.

### Confirmation

`max_lookback_days` / `max_aggregation_days` are runtime settings checked before any
read on REST and GraphQL; over-limit requests get an RFC-7807 error; page size is
clamped.

## Pros and Cons of the Options

### Configurable pre-read guards (chosen)

- Good, because it protects resources, is tunable, applies uniformly, and fails fast.
- Bad, because legitimate big pulls must page or use an export path.

### No limits

- Bad, because one query can take down the service.

### Hard-coded limits

- Bad, because they can't match agency capacity and need a redeploy to change.

## More Information

- ADR-0051 (runtime settings), ADR-0056 (error shape), ADR-0031 (tier-aware reads being guarded), ADR-0055 (surfaces)
