# ADR-0004: Ingestion integrity (never-lose-data + poison-aware)

- Status: Accepted
- Date: 2026-06-27
- Deciders: Jim Sloan

## Context

Controllers routinely have wrong clocks, stale configs, and get
replaced/re-IP'd without ceremony. The ATSPM field assumes the controller's
clock/config is ground truth; TSIGMA inverts that axiom. This is a domain call
(the agency's), and it is the product's moat.

## Decision

Two linked stances:

1. **Never-lose-data.** Any integrity/poison failure -> **ingest + flag +
   needs-review + correct-later**. Never withhold, drop, or hold data. This
   overrides programming-correctness objections.
2. **Poison-aware, host-owned integrity spine.** The controller is untrusted.
   The **host** owns the spine and orchestrates `fetch -> decode -> validate ->
   persist`. The decoder is a **pure transform** (bytes -> events); the host
   attaches `signal_id` / `device_id` / `validation_metadata` - a decoder never
   supplies identity or integrity.

Per-plane delivery (see ADR-0003): poll self-heals via re-pull (bounded by
controller buffer); the NATS plane requires **JetStream** (acked + durable +
sequence replay) - core NATS at-most-once would silently drop.

## Rationale

- It is the agency's call and overrides correctness objections; it is also the
  differentiator no incumbent ATSPM offers.
- Keeping the host as the orchestrator preserves decoder reuse (a decoder must
  also run on an uploaded file or a file pulled by a different poller) and keeps
  `validate` between decode and persist. A combined vendor binary therefore
  must keep its contracts separate - never a merged poll-and-decode black box
  that routes around `validate`.

## Consequences

- `validate-and-flag` sits between decode and persist.
- The review queue carries only **operator-actionable** findings; non-actionable
  vendor/diagnostic artifacts stay log-only.
- Transport-specific delivery guarantees are part of the ingest design, not an
  ingest flag.

## Related

- `VALIDATION.md`, `DECODERS.md`, `AUDITING.md`, `INGESTION.md`
- ADR-0002 (host orchestration, combined-binary rule), ADR-0003 (per-plane)
