# Checkpoint resilience: immunity, future-cap, drift detection, auto-recovery

- **Status**: Accepted
- **Date**: 2026-06-28
- **Deciders**: jsloan@celldara.com

## Context and Problem Statement

A controller with a wrong clock can poison a timestamp-based checkpoint: a
future-dated event advances the watermark past real time, so the next cycle
requests events after that future point and silently drops all real data. This
silent-data-loss failure mode is what TSIGMA must prevent. How are checkpoints
hardened?

## Decision Drivers

- Controllers routinely have wrong clocks (ADR-0034).
- A poisoned checkpoint silently stops data flow — the worst kind of failure.
- Detection must be operator-visible and, where safe, self-healing.
- File-based polling shouldn't be exposed to in-file timestamp lies at all.

## Considered Options

- Four-part resilience (file-immunity, future-cap, drift detection, auto-recovery)
- Trust event timestamps unconditionally
- Manual operator intervention only

## Decision Outcome

**Four independent resilience layers:**

1. **File-based immunity** — FTP/SFTP checkpoints use file identity (name + size + mtime) only, never event timestamps inside files; future-dated in-file events have zero effect on the cursor.
2. **Future-tolerance cap** — event-timestamp checkpoints (HTTP/push) are capped to `min(latest_event_timestamp, server_time + tolerance)`, so a future-dated event can't push the watermark past real time.
3. **Clock-drift detection + notification** — each cycle, the controller's clock offset is checked; drift beyond tolerance flags and notifies (never blocks ingest).
4. **Auto-recovery** — after N consecutive silent cycles, a poisoned checkpoint (`checkpoint > server_time + tolerance`) is rolled back to server time so polling resumes.

### Consequences

- The classic "wrong clock kills polling" failure mode is structurally prevented.
- File-based polling is immune to in-file timestamp lies by construction.
- Operators are alerted to drift; poisoned checkpoints self-heal without intervention.
- Each layer is independent — they compose, and any one can be reasoned about alone.

### Confirmation

File checkpoints use file identity only; event-timestamp checkpoints apply the
future cap; per-cycle drift is detected and notified; silent-signal auto-recovery
rolls back poisoned checkpoints.

## Pros and Cons of the Options

### Four-part resilience (chosen)

- Good, because it structurally prevents silent data loss, makes the file path immune, and self-heals.
- Bad, because there are four mechanisms to implement and test.

### Trust event timestamps unconditionally

- Bad, because it's the exact checkpoint-poisoning failure TSIGMA exists to prevent.

### Manual intervention only

- Bad, because silent failures persist until a human notices — often long after.

## More Information

- ADR-0042 (checkpoint), ADR-0034 (untrusted controller / never-lose-data), ADR-0045 (watchdog surfaces drift and trends offsets)
