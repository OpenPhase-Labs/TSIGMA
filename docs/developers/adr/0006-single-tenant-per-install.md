# Single-tenant per install

- **Status**: Accepted
- **Date**: 2026-06-28
- **Deciders**: jsloan@celldara.com

## Context and Problem Statement

DOTs do not want their data co-mingled with other agencies. Data sovereignty,
security boundaries, IT governance, procurement/liability, and compliance all
push the same direction. What is TSIGMA's tenancy model?

## Decision Drivers

- Data sovereignty: agencies want their data in their own infrastructure.
- Security: cross-agency leakage is unsurvivable; isolation that relies on always filtering by `tenant_id` is one missed `WHERE` clause from a breach.
- Each agency has its own IT governance, compliance posture, contracts, and liability.
- Inter-agency coordination is still needed — but via an explicit method, not co-mingling.

## Considered Options

- Strict single-tenant per install
- Multi-tenant with a `tenant_id` discriminator
- Hybrid (multi-tenant for small agencies, single-tenant for large)

## Decision Outcome

**Strict single-tenant per install.** One install = one agency; no `tenant_id` on
shared rows; isolation is structural at the install boundary. Within an install,
access is scoped by role (admin/viewer) and jurisdiction. Cross-agency needs are
met by the inter-agency method for shared **Method-B** signals (forthcoming ADR) —
an explicit, audited path between installs — not by granting a foreign user a role
here. "Hosted" offerings run a dedicated instance per customer (still single-tenant).

### Consequences

- A whole class of cross-tenant security bugs cannot exist; the data model is simpler (no `tenant_id`, no row-level security).
- Each agency's compliance posture is satisfied by its own install.
- Hosted offerings cost scales per customer (N dedicated instances).

### Confirmation

No `tenant_id` on shared rows; RBAC scopes are within one install; cross-agency
access only via inter-agency agreements.

## Pros and Cons of the Options

### Strict single-tenant (chosen)

- Good, because isolation is structural and the model is simpler; matches how DOTs operate.
- Bad, because per-customer hosting cost is higher than shared SaaS.

### Multi-tenant `tenant_id`

- Bad, because isolation depends on app code, compliance must satisfy every tenant at once, and DOTs reject co-mingling.

### Hybrid

- Bad, because two data models, and the small-agency variant carries the same isolation risks.

## More Information

- ADR-0003 (the same deployable serves on-prem and dedicated hosted instances)
- Forthcoming: inter-agency method (Method-B shared signals)
