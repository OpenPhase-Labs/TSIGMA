# Ingestion integrity: never-lose-data, poison-aware, host-owned spine

- **Status**: Accepted
- **Date**: 2026-06-28
- **Amended**: 2026-08-30 - never-lose-data scopes to data the host holds; a plugin's declared outcome is evidence
- **Deciders**: jsloan@celldara.com

## Context and Problem Statement

Controllers routinely have wrong clocks, stale configs, and get replaced / re-IP'd
without ceremony. Conventional practice treats the controller's clock/config as
ground truth; TSIGMA treats the controller as **untrusted**. How does TSIGMA keep
ingestion correct without losing data?

Since ADR-0018 and ADR-0082 there is a second untrusted party: the plugin author.
Decoders ship from third-party repositories, so an integrity claim can now arrive
from someone OpenPhase Labs does not vet and cannot bind (contract ADR-0007,
contract ADR-0016). Never-lose-data is a stance TSIGMA writes its own code to; it is not
enforceable on a vendor. It therefore scopes to what the host controls: **once
bytes or rows have reached the host, the host does not discard them.** What a
vendor's plugin chooses to emit or drop is outside that boundary.

This scoping also settles what a terminal decode status is. "A decoder never
supplies identity or integrity" (below) and the contract's SUCCESS / PARTIAL /
FAILURE `DecodeStatus` are not in conflict: a status is **evidence the host
records and flags**, never an instruction to destroy data already in hand. A
FAILURE arriving with rows persists those rows and flags; the declared status and
the author's own emitted / dropped counts go on the review row as their claim, and
a mismatch between that claim and what arrived is itself an operator-actionable
finding.

## Decision Drivers

- The controller is untrusted (wrong clocks, stale config, swapped hardware).
- Agencies want every byte retained and corrected later, not withheld — this is the product's moat.
- The decoder must be reusable (the same decoder runs on an uploaded file or a different poller), so identity/integrity can't live in the decoder.
- Validation must sit between decode and persist.

## Considered Options

- Never-lose-data + poison-aware, host-owned spine
- Reject/withhold suspect data (a programming-correctness stance)
- Trust the controller's clock/config (conventional practice)

## Decision Outcome

**Two linked stances:**

1. **Never-lose-data.** Any integrity/poison failure → **ingest + flag +
   needs-review + correct-later.** Never withhold, drop, or hold data. This
   overrides programming-correctness objections.
2. **Poison-aware, host-owned integrity spine.** The host owns `fetch → decode →
   validate → persist` and orchestrates it. The **decoder is a pure transform**
   (bytes → events); the **host** attaches `signal_id` / `device_id` /
   `validation_metadata`. A decoder never supplies identity or integrity.

`validate-and-flag` sits between decode and persist; it **flags, never blocks**
(consistent with never-lose-data). Deterministic checks run in the spine; deeper /
ML validation runs asynchronously after persist (layering is the validation ADR).
The review queue carries only **operator-actionable** findings; non-actionable
vendor/diagnostic artifacts stay log-only. Per-plane delivery (ADR-0033): the poll
plane self-heals by re-pull (bounded by the controller buffer); the push plane
requires **JetStream** (acked + durable + sequence replay) — at-most-once would
silently drop.

### Consequences

- Suspect data is ingested and flagged, never lost; corrections happen later.
- The host as orchestrator preserves decoder reuse and keeps `validate` between decode and persist — a combined vendor binary must keep its contracts separate (no merged poll-and-decode black box that routes around `validate`).
- The review queue stays actionable; diagnostics are log-only.
- A flagging step and a review-queue / correction workflow are required.

### Confirmation

The host attaches identity/metadata (never the decoder); validation runs between
decode and persist and never withholds; the review queue holds only
operator-actionable items; the push plane uses JetStream durable/acked/replay.

## Pros and Cons of the Options

### Never-lose-data + host-owned spine (chosen)

- Good, because no data is lost, it's a differentiator no incumbent offers, decoder reuse is preserved, and validation can't be routed around.
- Bad, because it requires a flag/review/correction workflow and disciplined host orchestration.

### Reject/withhold suspect data

- Bad, because it loses data the agency may need — the agency, not the system, should decide.

### Trust the controller

- Bad, because wrong clocks/config poison the data silently — the exact failure mode TSIGMA exists to prevent.

## More Information

- ADR-0033 (two planes / per-plane delivery), ADR-0009 (canonical events), ADR-0018 (decoder is a pure-transform plugin), ADR-0005 (audit); validation-layer timing in the forthcoming validation ADR
- ADR-0082 (plugins live in third-party repositories); contract ADR-0007 (trust tiers) and contract ADR-0016 (the trust anchor is the deploying operator's, never OpenPhase Labs)
- Amendment note (2026-08-30): the decision is unchanged. As first written, this ADR's only untrusted party was the controller, because decoders were in-process TSIGMA code. Plugins now ship from third-party repositories, so the amendment names who never-lose-data binds - the host - and classifies a plugin's declared outcome as evidence rather than integrity. Both stances, the flag-never-block rule, and the Confirmation criteria stand as written.
