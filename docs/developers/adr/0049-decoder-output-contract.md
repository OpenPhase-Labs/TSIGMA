# Decoder output: pure canonical events + optional provenance envelope

- **Status**: Accepted
- **Date**: 2026-06-28
- **Deciders**: jsloan@celldara.com

## Context and Problem Statement

A decoder turns vendor bytes into canonical events. Some callers want only the
events; ingest also wants file/source provenance (device IP, MAC, log version,
anchor times) for its integrity checks. What does a decoder return, and what stays
the host's job?

## Decision Drivers

- The host attaches identity/integrity, not the decoder (ADR-0034); the decoder is a pure transform.
- Ingest needs provenance (for replacement/drift/temporal detection, ADR-0044); lightweight callers (a quick re-decode) don't.
- A decoder must be reusable on an uploaded file or any poller's bytes (ADR-0034).

## Considered Options

- Two surfaces: pure events + an optional provenance envelope
- Always return events only
- Always return a heavy envelope

## Decision Outcome

**Two decoder output surfaces.** A **pure events** result (canonical HiRes events
only) for callers that just need the data, and an **optional provenance envelope**
(events + file/source metadata: device IP, MAC, log version, phases-in-use, anchor
times) for ingest's integrity checks (ADR-0044). The decoder remains a **pure
transform** — it reports what it parsed (including header provenance) but **never
attaches identity or integrity** (`signal_id` / `device_id` / validation are the
host's, ADR-0034). The same decoder runs on an uploaded file or any poller's bytes.

### Consequences

- Ingest gets the provenance its detections need; lightweight callers avoid the overhead.
- Decoder reuse is preserved (file upload, any poller) — identity stays with the host.
- Decoders surface header facts but never decide identity/integrity.
- Two output shapes for decoder authors (the envelope wraps the pure result).

### Confirmation

Decoders expose a pure-events output and an optional provenance envelope; they
never set signal/device identity or validation status; the same decoder works on
uploads and polled bytes.

## Pros and Cons of the Options

### Pure events + optional envelope (chosen)

- Good, because it serves both ingest and lightweight callers and preserves the pure-transform/reuse boundary.
- Bad, because there are two output shapes to support.

### Events only

- Bad, because ingest loses the header provenance it needs for integrity detection.

### Always heavy envelope

- Bad, because it forces provenance overhead on callers that don't need it.

## More Information

- ADR-0034 (host owns identity/integrity; decoder is a pure transform), ADR-0044 (provenance/detection consumes the envelope), ADR-0011 (semantics in core), ADR-0009 (canonical events)
