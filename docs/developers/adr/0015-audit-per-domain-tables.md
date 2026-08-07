# Audit: per-domain tables, not a unified table

- **Status**: Accepted
- **Date**: 2026-06-28
- **Deciders**: jsloan@celldara.com

## Context and Problem Statement

ADR-0005 makes auditing a core requirement. Coverage spans several cross-cutting
concerns — config changes (signal / approach / detector), runtime settings,
authentication, ingest review/corrections. How is audit storage shaped: one
unified table with a domain discriminator, or a separate table per domain?

## Decision Drivers

- Audit tables should mirror TSIGMA's existing per-domain config tables — consistent modeling.
- Domain-specific typed columns (e.g. auth: `source_ip`, `auth_method`) are worth keeping queryable, not flattened into JSON.
- Table-level access control is simpler and safer than row-level filtering on a shared table.
- Forensic queries occasionally span domains.

## Considered Options

- Per-domain audit tables (`signal_audit`, `approach_audit`, `detector_audit`, `system_setting_audit`, `auth_audit_log`, …)
- Unified audit table with a `domain` discriminator (row-level filtering)
- Per-domain tables + a denormalized unified view

## Decision Outcome

**Per-domain audit tables.** Each audited domain has its own table; new audited
concerns add their own table rather than a row in a shared one. Cross-domain
forensic queries use `UNION ALL` over the shared base columns (ADR-0016).
Domain-specific typed columns live on each table; the rest goes in a JSON payload.

### Consequences

- Mirrors the per-domain config tables — consistent, familiar modeling.
- Access can be granted/denied at table granularity (no per-row leakage risk).
- Domain-specific fields stay typed and queryable.
- Cross-domain reporting needs `UNION ALL` (made tractable by the common base shape, ADR-0016).
- The audit-table catalog grows as new audited concerns land.

### Confirmation

Schema review confirms each new audited concern gets a dedicated table; documented
cross-domain query examples use the base columns.

## Pros and Cons of the Options

### Per-domain tables (chosen)

- Good, because it mirrors the config tables, keeps typed domain columns, allows table-level access, and makes segregation structural.
- Bad, because the catalog grows and cross-domain queries need `UNION ALL`.

### Unified table + discriminator

- Good, because cross-domain queries are trivial and there are fewer migrations.
- Bad, because access shifts to error-prone row-level filtering and domain columns go sparse/JSON.

### Per-domain + unified view

- Good, because both shapes are available.
- Bad, because of extra complexity/maintenance not needed on day one (can add later).

## More Information

- ADR-0005 (auditing is a requirement), ADR-0016 (common base shape), ADR-0017 (append-only)
- Forthcoming: config-audit valid-time (`effective_at`)
