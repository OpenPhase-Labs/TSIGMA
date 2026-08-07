# Decoder selection: declared extensions + content probing, priority order

- **Status**: Accepted
- **Date**: 2026-06-28
- **Deciders**: jsloan@celldara.com

## Context and Problem Statement

A file or byte stream arrives (a poll result, or an operator upload) and the host
must pick the decoder to parse it. Extensions are ambiguous (e.g. `.dat` could be
several formats) and uploads may have no useful name. How does the host choose a
decoder?

## Decision Drivers

- A poll result or upload must be routed to the right decoder.
- File extensions are ambiguous or absent.
- Decoder plugins declare the formats/extensions they handle (manifest, ADR-0020).
- An explicit per-device decoder override should win when configured.

## Considered Options

- Explicit override → declared-extension match → content probing in priority order
- Extension only
- Content probing only

## Decision Outcome

**A layered selection.** If a device configures an explicit decoder, use it.
Otherwise, match the **declared extensions** of registered decoder plugins
(capabilities from the manifest, ADR-0020); when the extension is ambiguous or
absent, **probe content** by asking candidate decoders, in a defined **priority
order** — binary/structured formats before permissive text formats, so a
permissive format (e.g. CSV) doesn't greedily claim binary data. The first decoder
that confidently recognizes the content wins; unrecognized input is flagged, never
silently mis-decoded.

### Consequences

- Uploads and polls are routed without relying on names alone.
- Ambiguous extensions resolve by content, deterministically (priority order).
- A per-device override gives operators an escape hatch.
- The probing priority order matters and must be maintained as formats are added.

### Confirmation

Explicit override wins; otherwise extension match then content probe in priority
order; permissive text formats are probed last; unrecognized input is flagged.

## Pros and Cons of the Options

### Override → extension → content probe (chosen)

- Good, because it's robust to ambiguous/absent names, deterministic, and offers an operator override.
- Bad, because there's a probing priority order to maintain.

### Extension only

- Bad, because it fails on ambiguous/absent extensions and mis-routes.

### Content probing only

- Bad, because it probes every decoder every time (slower) and still needs a tiebreak order.

## More Information

- ADR-0020 (manifest declares decoder formats/extensions), ADR-0011 (decoders normalize to canonical semantics), ADR-0034 (host orchestrates; flag unrecognized)
