# Migrations are additive-only and idempotent (no destructive downgrades)

- **Status**: Accepted
- **Date**: 2026-06-28
- **Deciders**: jsloan@celldara.com

## Context and Problem Statement

Schema changes must be safe to apply during rolling / blue-green deploys where old
and new code run simultaneously, safe to re-run in CI, and recoverable from a
partial apply. How are migrations constrained?

## Decision Drivers

- Blue/green and rolling deploys mean old code may still run during/after a migration — a dropped table/column it depends on crashes it.
- CI and recovery re-run migrations — they must be idempotent.
- Audit append-only (ADR-0017) already forbids destructive edits to audit data.

## Considered Options

- Additive-only + idempotent, no downgrades
- Reversible up/down migrations (classic)
- Ad-hoc / manual schema changes

## Decision Outcome

**Migrations are additive-only and idempotent, with no destructive downgrades.**
`upgrade()` is safe to run twice (guards on CREATE/ALTER); migrations never DROP or
DELETE schema/data that live code may depend on; there is no downgrade path.
Deprecating a column/table is a staged, additive process (add new → migrate → stop
using old), not an in-place drop. Managed with Alembic.

### Consequences

- Blue/green and rolling deploys are safe — old code never loses a column/table mid-deploy.
- Migrations are safe to re-run (CI, recovery from a partial apply).
- Removing schema is deliberate and staged, never a single destructive migration.
- The schema accretes deprecated artifacts until a separate, deliberate cleanup (with all code off them).

### Confirmation

Migration review rejects DROP/DELETE of in-use schema/data and non-idempotent
steps; there are no downgrade functions; deprecations are staged additively.

## Pros and Cons of the Options

### Additive-only + idempotent (chosen)

- Good, because it's deploy-safe, re-runnable, and recoverable.
- Bad, because deprecated schema lingers until a deliberate cleanup.

### Reversible up/down migrations

- Good, because of clean rollback in theory.
- Bad, because down-migrations are rarely tested and dangerous with live old code — false safety.

### Ad-hoc / manual

- Bad, because it's unrepeatable, error-prone, and leaves no audit trail.

## More Information

- ADR-0003 (role-specialized / rolling deploys), ADR-0017 (audit append-only)
