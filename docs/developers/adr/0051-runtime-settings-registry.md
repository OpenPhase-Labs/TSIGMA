# Runtime-settings registry: typed source of truth + cross-replica invalidation

- **Status**: Accepted
- **Date**: 2026-06-28
- **Deciders**: jsloan@celldara.com

## Context and Problem Statement

Runtime-tunable settings (ADR-0050) live in the database and are edited at runtime.
What defines which keys exist and their types, and how do many replicas stay
consistent when a value changes?

## Decision Drivers

- Settings must be type- and bounds-checked (a bad value shouldn't break a subsystem).
- Operators tune values without code changes; typos must be caught.
- Many replicas (ADR-0003) must see a change quickly without a restart.
- A per-request DB read for every setting is too costly.

## Considered Options

- A typed registry (source of truth for keys) + DB-stored values + TTL cache + Valkey pub/sub invalidation
- The DB table is the only source of truth (free-form keys)
- In-memory only (no persistence)

## Decision Outcome

**A typed settings registry is the source of truth for which keys exist** (name,
type, bounds, default, category) — not the database. Values are stored in a settings
table (via the facade, ADR-0023) and read/written through the registry, which
enforces the registered type/bounds; unknown keys are rejected. Reads use a short
**in-process TTL cache** (~30 s); on a change, **Valkey pub/sub** broadcasts an
invalidation so all replicas refresh promptly (ADR-0012). Every change is audited
(ADR-0015) in the same transaction as the write.

### Consequences

- Type/bounds enforcement prevents bad values and typos; the registry documents every knob.
- Many replicas converge within the TTL, or immediately on a Valkey invalidation.
- Without Valkey, replicas still converge within the TTL (Valkey is the optional fast path).
- The registry must be kept in sync with the code that consumes each key.

### Confirmation

The registry (not the DB) defines valid keys/types/bounds; writes validate against
it and audit in the same transaction; reads are TTL-cached; changes publish a Valkey
invalidation; unknown keys are rejected.

## Pros and Cons of the Options

### Typed registry + cache + invalidation (chosen)

- Good, because config is safe and typed, reads are fast, replicas converge promptly, and changes are audited.
- Bad, because the registry must track the code's keys, and there's a small cache-staleness window without Valkey.

### DB-only free-form

- Bad, because there's no type safety, typos slip through, and valid keys aren't documented.

### In-memory only

- Bad, because it isn't persistent and isn't shareable across replicas.

## More Information

- ADR-0050 (config layering/precedence), ADR-0012 (DB + Valkey shared state / invalidation), ADR-0015 (settings change audited), ADR-0023 (stored via facade)
