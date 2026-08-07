# Persistent, non-destructive polling checkpoint

- **Status**: Accepted
- **Date**: 2026-06-28
- **Deciders**: jsloan@celldara.com

## Context and Problem Statement

The poll plane must avoid reprocessing and survive restarts without deleting source
files at the edge. How does TSIGMA track polling progress?

## Decision Drivers

- Don't delete source files (reprocessing for decoder improvements; never destroy the source of truth at the edge).
- Survive process restarts and crashes.
- Support multiple consumers / sharded workers safely.
- Avoid re-ingesting already-seen data (dedup).

## Considered Options

- A persistent checkpoint table (per device + method) with non-destructive cursors
- Delete source files after pull
- In-memory progress only

## Decision Outcome

**A persistent `polling_checkpoint` table, keyed per `(device, method)`**,
advancing its cursor only after a successful ingest. Cursors are non-destructive:
file methods use **file identity** (name + size + mtime); event-stream methods use
a **last-event-timestamp** (capped — ADR-0043). Source files are never deleted at
the edge. The checkpoint survives restarts and is safe for multiple / sharded
consumers (each owns its slice, ADR-0039). A **dialect-appropriate idempotent
insert** on the event primary key (via the facade, ADR-0023) absorbs overlap; the
checkpoint tracks `duplicates_absorbed` and `consecutive_errors` for
health/alerting.

### Consequences

- No reprocessing of seen data; safe restart/crash recovery.
- Source files remain for reprocessing (decoder improvements) — the poll plane never deletes them at the edge.
- Sharded / multi-consumer polling is safe (per-slice checkpoints).
- Health signals (duplicates absorbed, consecutive errors) are queryable per device.

### Confirmation

A persistent checkpoint table exists per (device, method); cursors advance only
after successful ingest; source files are not deleted at the edge; the idempotent
insert absorbs overlap; checkpoint health columns are populated.

## Pros and Cons of the Options

### Persistent non-destructive checkpoint (chosen)

- Good, because there's no loss, it's restart- and multi-consumer-safe, and it avoids reprocessing.
- Bad, because there's a checkpoint table to maintain and reason about.

### Delete source files after pull

- Bad, because it destroys the source and prevents reprocessing.

### In-memory progress only

- Bad, because it's lost on restart, causing re-ingest or gaps.

## More Information

- ADR-0043 (resilience), ADR-0039 (sharded polling), ADR-0034 (never-lose-data spine), ADR-0023 (dialect-appropriate idempotent insert)
