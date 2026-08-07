# ADR-0005: Config effective-date (valid-time) audit axis

- Status: Accepted (design direction); implementation pending
- Date: 2026-06-27
- Deciders: Jim Sloan

## Context

The config audit history keys only on `changed_at` (system time = when the edit
committed). Reports resolve a signal's config as-of the report start. But config
edits lag reality - a controller is swapped Tuesday, the edit lands Friday - so
reports for the gap reconstruct against the wrong config, and the only way to
record "true since Tuesday" today is to falsify `changed_at`.

## Decision

Add **`effective_at`** (valid time) to the config audit tables, **defaulting to
`changed_at`** (an override, never a required field). Reconstruction
(`config_resolver`) filters/orders by `effective_at` with `changed_at` /
`audit_id` as tiebreaker. `effective_at` rides the **same transaction-scoped
session-variable channel** that already carries `changed_by` (the audit
triggers). Storing both axes makes the history **bitemporal-capable**; only
valid-time queries are built now (system-time "as known at" queries deferred,
no schema change needed later).

## Rationale

- Greenfield (no users yet) - the column is cheap now and expensive to retrofit
  after there is audit history.
- Aggregates are **config-independent** (config is resolved at query time, keyed
  on raw phase/channel), so effective-dating needs **no aggregate recompute**.
- Default-to-`changed_at` means un-overridden edits behave exactly as today.

## Consequences

- A nullable-defaulted `effective_at` column + `_eff` indexes on the
  config-audit tables; triggers write it (COALESCE session-var, `now()`).
- `system_setting_audit` left system-time only for now (revisit later).
- Open: naive-vs-UTC handling is a related but separate decoder/contract concern.

## Related

- `AUDITING.md`
- Design detail: `specs/2026-06-22-config-effective-date-design.md` (ephemeral;
  this ADR is the durable record)
