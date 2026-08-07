# File-ingest provenance and integrity detection → review queue

- **Status**: Accepted
- **Date**: 2026-06-28
- **Deciders**: jsloan@celldara.com

## Context and Problem Statement

Controllers get swapped, re-IP'd, and reconfigured without notice; files arrive
with headers that may disagree with the configured signal. TSIGMA must detect these
without losing data. How are file ingests recorded and checked?

## Decision Drivers

- Hardware replacement (MAC change at the same IP) and config drift must be detectable.
- Header timestamps may disagree with reality (clock issues).
- Never-lose-data (ADR-0034): always ingest; flag operator-actionable findings.
- Operators need an actionable worklist, not log noise.

## Considered Options

- Per-file provenance row + integrity detections feeding a review queue
- Trust the file/controller; no provenance
- Reject mismatched files

## Decision Outcome

**Every ingested file writes one provenance row** (source IP, device MAC, log
version, phases-in-use, anchor times, decoder, ingested-at). Three integrity checks
run at ingest and, on a finding, **always ingest and write an operator-actionable
item to the review queue**:

- **Controller-replacement detection** — a new MAC at the same IP vs the configured controller MAC flags a possible unit replacement.
- **Config-phase drift** — header `phases_in_use` vs the signal's configured phases flags stale config.
- **Temporal integrity** — the file's newest event timestamp vs server time; beyond tolerance, a suggested correction goes to the review queue.

Non-actionable vendor/diagnostic artifacts stay log-only (ADR-0034).

### Consequences

- Replacements, config drift, and clock issues are caught with a full provenance trail.
- Data is never withheld — findings are flagged and corrected later.
- The review queue stays actionable; corrections apply via bulk/anchor endpoints.
- A provenance table and a review-queue workflow are required.

### Confirmation

Each file ingest writes a provenance row; MAC-replacement, config-drift, and
temporal checks run and enqueue operator-actionable review items; data is always
ingested; non-actionable findings are log-only.

## Pros and Cons of the Options

### Provenance + detection → review queue (chosen)

- Good, because it detects real-world controller churn with a trail, never loses data, and keeps the queue actionable.
- Bad, because it needs provenance storage and a review/correction workflow.

### Trust the file; no provenance

- Bad, because replacement/drift go silent and there's no audit trail.

### Reject mismatched files

- Bad, because it loses data (violates never-lose-data) and takes the decision away from the agency.

## More Information

- ADR-0034 (never-lose-data / review queue), ADR-0011 (decoder reads headers; host owns identity), ADR-0045 (clock-offset trending), ADR-0005 (audit)
