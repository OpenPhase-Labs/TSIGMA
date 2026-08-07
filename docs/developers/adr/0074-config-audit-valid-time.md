# Config audit: bitemporal valid-time via effective_at

- **Status**: Accepted
- **Date**: 2026-06-28
- **Deciders**: jsloan@celldara.com

## Context and Problem Statement

The config audit (ADR-0015–0017) records *when an edit was committed* (`changed_at`
= system time), and reports reconstruct a signal's config as-of the report period
using that. But config edits lag reality — a controller is swapped or re-timed
Tuesday, the edit lands Friday — so reports for the gap reconstruct against the
wrong config, and the only way to say "true since Tuesday" today is to falsify
`changed_at`. How is "when it was actually true" recorded distinctly from "when we
recorded it"?

## Decision Drivers

- Config changes lag the real-world change they describe; reconstruction must use real-world (valid) time, not commit time.
- Falsifying `changed_at` corrupts the audit/system-time record.
- Greenfield now — adding the column is cheap; retrofitting after there's audit history is expensive.
- Aggregates are config-independent (config is resolved at query time on raw phase/channel), so no aggregate recompute is needed.

## Considered Options

- Add `effective_at` (valid time) defaulting to `changed_at`; reconstruct by `effective_at` — bitemporal-capable
- Transaction-time only (status quo; falsify `changed_at` when needed)
- Full bitemporal now (build both valid-time and system-time "as-known-at" queries)

## Decision Outcome

**Add a nullable `effective_at` (valid time) to the config-audit tables, defaulting
to `changed_at`** — an override, never a required field. `config_resolver`
filters/orders by `effective_at`, with `changed_at` / `audit_id` as tiebreaker.
`effective_at` rides the **same transaction-scoped session-variable channel** that
already carries `changed_by` (the audit triggers write it, COALESCE session-var →
`now()`).

Storing both axes (valid time `effective_at` + system time `changed_at`) makes the
history **bitemporal-capable**, but only **valid-time** reconstruction is built now;
system-time "as-known-at" queries are deferred (no schema change needed later).
Un-overridden edits behave exactly as today (`effective_at` = `changed_at`).

### Consequences

- "True since Tuesday, recorded Friday" is representable without falsifying the commit record.
- Reconstruction for the lag gap uses the correct config.
- No aggregate recompute — config is resolved at query time, keyed on raw phase/channel.
- A nullable `effective_at` column + `_eff` indexes on the config-audit tables; triggers populate it.
- `system_setting` audit stays system-time only for now (revisit if needed).

### Confirmation

Config-audit tables carry `effective_at` (default `changed_at`); `config_resolver`
orders by `effective_at` with `changed_at`/`audit_id` tiebreaker; triggers seed
`effective_at` from the session var; un-overridden behavior matches `changed_at`.

## Pros and Cons of the Options

### `effective_at`, valid-time now, bitemporal-capable (chosen)

- Good, because reconstruction is correct, the commit record isn't falsified, it's cheap greenfield, needs no aggregate recompute, and future system-time queries need no schema change.
- Bad, because of a column + indexes + resolver/trigger logic to carry, and operators must understand override-vs-default.

### Transaction-time only

- Bad, because it forces falsifying `changed_at` to fix gap reconstruction — corrupting the audit record.

### Full bitemporal now

- Bad, because it builds system-time "as-known-at" queries with no present demand — more work than valid-time needs.

## More Information

- ADR-0015–0017 (config audit tables / shape / immutability this extends), ADR-0050/0051 (config & settings)
- Open: naive-vs-UTC timestamp handling is a related but separate decoder/contract concern.
