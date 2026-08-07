# Two-tier watchdog: inline per-cycle + scheduled background

- **Status**: Accepted
- **Date**: 2026-06-28
- **Deciders**: jsloan@celldara.com

## Context and Problem Statement

Data-quality problems (clock drift, silent signals, stuck detectors, missing data
windows) must be caught both immediately at ingest and via deeper periodic
analysis. How is monitoring structured?

## Decision Drivers

- Some issues must be caught at ingest time (clock drift, a signal going silent, a poisoned checkpoint).
- Deeper analytics (stuck detectors, missing windows, offset trends) need to scan across time and signals, off the ingest path.
- Alerts must be best-effort and never block ingestion (ADR-0034).

## Considered Options

- Two tiers: inline per-cycle checks + a scheduled background job
- Inline-only
- Scheduled-only

## Decision Outcome

**Two tiers:**

- **Inline per-cycle (in CollectorService)** — immediate checks at ingest:
  clock-drift detection, silent-signal detection, and poisoned-checkpoint
  auto-recovery (ADR-0043). Catch-and-act at the moment of ingest.
- **Scheduled background job** — periodic (e.g. daily) deeper analysis across
  time/signals: stuck-detector detection, missing-data-window scans, low-event-count
  anomalies, and **controller clock-offset trending** (per-ingest signed offsets
  are recorded; the job trends each controller's mean |offset| and alerts
  proactively before the poison threshold).

All alerts are best-effort, severity-tiered (INFO / WARNING / CRITICAL), and never
block ingestion.

### Consequences

- Immediate issues are caught at ingest; deeper patterns are caught periodically.
- Clock problems are flagged proactively (trending) before they poison checkpoints.
- Notification failures never affect ingestion.
- Two monitoring paths to maintain; some scheduled checks are planned/iterative.

### Confirmation

Inline checks run per cycle (drift, silent, auto-recovery); a scheduled job runs the
deeper checks incl. offset trending; alerts are severity-tiered and best-effort.

## Pros and Cons of the Options

### Two-tier watchdog (chosen)

- Good, because it gives immediate + deep coverage, proactive clock trending, and stays non-blocking.
- Bad, because there are two paths to build.

### Inline-only

- Bad, because it can't do cross-time/cross-signal analysis on the ingest path.

### Scheduled-only

- Bad, because immediate issues (drift, silent) are caught late.

## More Information

- ADR-0043 (resilience checks surfaced here), ADR-0044 (provenance findings), ADR-0034 (best-effort, never-block); notification severity/providers in the forthcoming notifications ADR
