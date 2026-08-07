# Audit tables share a common base shape

- **Status**: Accepted
- **Date**: 2026-06-28
- **Deciders**: jsloan@celldara.com

## Context and Problem Statement

With per-domain audit tables (ADR-0015), each domain could invent its own column
names and conventions, making cross-domain queries, export, and onboarding
painful. How is the shape constrained without losing per-domain flexibility?

## Decision Drivers

- A common shape keeps cross-domain `UNION ALL` (ADR-0015) cheap.
- Generic export tooling (CSV / JSON / SIEM) needs predictable columns.
- A clear template makes the audit-is-a-requirement discipline (ADR-0005) enforceable.
- Domains genuinely need extensions (auth: `source_ip`, `auth_method`).

## Considered Options

- Common base columns + per-table typed extensions
- Common base only; all extensions in JSON
- No common shape

## Decision Outcome

**Common base column set + per-table additive extensions.** Every audit table
carries the base columns: `audit_id`, `changed_at`, `changed_by` (actor),
`actor_source` (operator/plugin/system), `action`, `target_type`, `target_id`,
`before_state`/`after_state` (JSON), and `payload` (JSON for domain extras).
Domains promote frequently-queried fields to typed columns; the rest goes in
`payload`. (Config-audit tables also carry `effective_at` for valid time — its own
ADR.)

### Consequences

- Cross-domain `UNION ALL` over the base columns is straightforward.
- Generic export works across all audit tables without per-table code.
- New audit tables follow one template — less drift.
- Frequently-queried domain fields stay typed.
- The base must fit the general case — `before_state`/`after_state` may sit empty for non-state-transition events; base-shape changes are a fleet-wide migration.

### Confirmation

Schema review confirms the base columns are present with standard names/types; a
template/helper captures the base shape; documented cross-domain query examples
exercise the base columns.

## Pros and Cons of the Options

### Common base + typed extensions (chosen)

- Good, because it gives a predictable export surface, keeps typed domain fields, and provides a clear template.
- Bad, because some events leave the state columns empty and base changes touch every table.

### Common base + all-JSON extensions

- Good, because no migrations on domain evolution.
- Bad, because it loses typed query/indexing and the schema isn't self-describing.

### No common shape

- Bad, because of bespoke query/export per table, weak discipline, and compounding drift.

## More Information

- ADR-0015 (per-domain tables), ADR-0017 (append-only), ADR-0005 (audit requirement)
- Forthcoming: config-audit valid-time (`effective_at`)
