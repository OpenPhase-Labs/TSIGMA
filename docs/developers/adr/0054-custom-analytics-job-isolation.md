# Custom analytics job isolation

- **Status**: Accepted
- **Date**: 2026-06-28
- **Deciders**: jsloan@celldara.com

## Context and Problem Statement

Agencies and plugins add custom analytics / background jobs. These must extend the
system without endangering the core data. What can a custom job touch?

## Decision Drivers

- Custom jobs must not corrupt or break core ingestion/analytics data.
- Upgrades must stay safe regardless of installed custom jobs.
- Custom jobs need their own storage for results.
- Clear isolation aids debugging and trust.

## Considered Options

- Isolation: custom jobs own prefixed tables, read-only on core, idempotent
- Custom jobs may write core tables
- No custom jobs

## Decision Outcome

**Custom analytics jobs are isolated.** They may **create and write their own
tables** (using a reserved prefix/namespace to avoid collisions) and **read** core
tables, but are **forbidden to modify, delete, or alter core tables/schemas**.
Their migrations are idempotent and additive (ADR-0026). They run on the scheduler
(ADR-0003) like core jobs but within this boundary.

### Consequences

- Core ingestion/analytics data can't be corrupted by a custom job.
- Upgrades stay safe regardless of installed custom jobs.
- Custom jobs have a clear place for results (their own tables).
- The read-only-core boundary must be enforced (review, and DB grants where available).

### Confirmation

Custom jobs create only prefixed tables, read core tables, never modify/delete core;
their migrations are idempotent; the boundary is enforced in review (and DB grants
where supported).

## Pros and Cons of the Options

### Isolation (chosen)

- Good, because it protects core data, keeps upgrades safe, and gives custom jobs clear result storage.
- Bad, because the boundary needs enforcement discipline.

### Write core tables

- Bad, because a custom job can corrupt ingestion/analytics and make upgrades unsafe.

### No custom jobs

- Bad, because it forgoes a key extensibility surface for agency-specific analytics.

## More Information

- ADR-0003 (scheduler), ADR-0026 (additive idempotent migrations), ADR-0052 (analytics tiers), ADR-0002 (core/plugin boundary)
