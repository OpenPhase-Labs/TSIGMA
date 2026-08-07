# Audit immutability: append-only, enforced at the database

- **Status**: Accepted
- **Date**: 2026-06-28
- **Deciders**: jsloan@celldara.com

## Context and Problem Statement

Audit is only credible if it cannot be modified after the fact. "We promise not to
UPDATE audit tables" is bypassable by bugs, ad-hoc admin scripts, or compromised
code. Compliance frameworks expect tamper-resistant audit. Where is immutability
enforced?

## Decision Drivers

- Credibility needs "can't modify by accident," not just convention.
- Compliance (SOC 2, FedRAMP, NIST 800-53) expects tamper-resistant audit by default.
- App-layer discipline is bypassable by any code path with a DB connection (plugins, scripts, migrations).
- Stronger tamper-evidence (hash chains / signing) is wanted by some agencies but belongs in an optional layer.
- Multi-dialect (PG/MS-SQL/Oracle/MySQL) — enforcement must be expressible per dialect.

## Considered Options

- DB-layer triggers/constraints preventing UPDATE and DELETE on audit tables
- Application-layer discipline only
- Cryptographic tamper-evident seal in the default

## Decision Outcome

**Database-layer enforcement.** Every audit table gets dialect-appropriate
triggers/constraints that reject UPDATE and DELETE; the core ships them as part of
the schema so every deployment (production, training, lab) gets the same
guarantee. Legitimate corrections are modeled as new audit rows that supersede
prior ones — never in-place edits. Cryptographic tamper-evidence (hash chain /
signing) remains an optional layer on top, not the default.

### Consequences

- Application bugs can't corrupt audit — a stray UPDATE/DELETE errors at the DB.
- Compliance-friendly by default; the guarantee is observable in the schema.
- Corrections must be new rows (aligns with audit norms anyway).
- Schema migrations on audit tables must manage the triggers (drop/alter/recreate).

### Confirmation

Schema review confirms UPDATE/DELETE-prevention on each audit table; an integration
test asserts UPDATE and DELETE fail on every audit table; migration review catches
accidental trigger removal.

## Pros and Cons of the Options

### DB-layer triggers (chosen)

- Good, because immutability is enforced at the lowest layer, survives app bugs/compromise, is consistent across deployments, and is observable in the schema.
- Bad, because corrections must be new rows and migrations must manage the triggers.

### Application-layer discipline only

- Bad, because it's bypassable by any DB connection and compliance won't accept "we promised."

### Cryptographic seal in the default

- Good, because it's the strongest guarantee.
- Bad, because of key management, performance, and complexity not every deployment needs — keep it optional.

## More Information

- ADR-0015 (per-domain tables), ADR-0016 (common base shape), ADR-0005 (audit requirement)
- Deferred: retention policy, export formats, optional cryptographic seal
