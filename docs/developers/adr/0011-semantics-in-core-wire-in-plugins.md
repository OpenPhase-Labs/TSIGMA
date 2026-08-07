# Event/protocol semantics in the core; wire formats in decoder plugins

- **Status**: Accepted
- **Date**: 2026-06-28
- **Deciders**: jsloan@celldara.com

## Context and Problem Statement

"Everything is a plugin" does not mean all protocol work lives in plugins. If the
core knows nothing of NTCIP 1202 phase concepts or Indiana Hi-Res event codes, it
cannot validate, query, analyze, or share events. Where does the core-vs-plugin
line fall for protocols?

## Decision Drivers

- Analytics and operator workflows require the core to understand event vocabulary (event codes, phase/movement concepts).
- Inter-agency exchange (Method-B) requires a shared semantic vocabulary.
- A zero-plugin install must be meaningful for any protocol the core claims to support.
- Decoders are essential for vendor wire formats but don't give the core semantic grounding.
- The rule must apply to any new protocol, not be a one-off.

## Considered Options

- Semantics in core, wire in decoder plugins
- Everything protocol-related in plugins
- Everything in the core

## Decision Outcome

**Semantics in the core; wire in decoder plugins.** The core ships the canonical
event model (ADR-0009), the event-code vocabulary, validators, and renderers — the
semantic layer. Decoder plugins handle vendor wire formats (ASC/3, MaxTime,
Siemens, D4, SEPAC, CSV) and transport, normalizing into the canonical shape.

The line: a decoder tells the core **WHEN** something happened, not **WHAT IT
MEANS** — meaning is defined by the core's canonical codes. A decoder that defines
new meaning is a smell and triggers re-review.

### Consequences

- Operators and reports can validate, query, and federate events with zero decoders installed for a given format.
- Standards-conformance (NTCIP / Indiana Hi-Res) is in-tree and testable.
- The core grows as it adds semantic models for supported protocols; decoders stay focused on wire + vendor quirks.

### Confirmation

The core ships testable semantic parsers/validators independent of any decoder;
review confirms decoders carry wire/transport only; "the decoder tells us what it
means" triggers re-review.

## Pros and Cons of the Options

### Semantics in core, wire in plugins (chosen)

- Good, because events are meaningful with zero decoders, conformance is verifiable, and decoders stay focused.
- Bad, because the core grows per supported protocol and the line is judgment-driven.

### Everything protocol-related in plugins

- Bad, because nothing is validatable/federatable without the right decoder, and the core can make no conformance claims.

### Everything in the core

- Bad, because vendor quirks pollute the core, every new vendor means a core change, and it conflicts with the plugin model.

## More Information

- ADR-0009 (canonical event model), ADR-0004 (not-kitchen-sink); forthcoming: decoders, gRPC plugin contract
