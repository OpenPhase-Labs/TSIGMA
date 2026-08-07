# Signal identity is TSIGMA-owned, not vendor-provided

- **Status**: Accepted
- **Date**: 2026-07-16
- **Deciders**: jsloan@celldara.com

## Context and Problem Statement

High-resolution records carry only `(timestamp, event_code, event_parameter)` — no
signal identifier. Whatever identity a controller or file exposes (a name in a
header or filename, a device number, or nothing) is inconsistent and unguaranteed
across vendors. How does TSIGMA identify a signal?

## Decision Drivers

- Records contain no identity; a vendor identifier, when present, is inconsistent and cannot be relied on.
- The host already owns the fetch → decode → validate → persist spine and knows which device it polled (ADR-0034).
- Identity must be stable across vendor firmware, formats, and renames.
- The event log needs a compact, fixed-width key at billions-of-rows scale.

## Considered Options

- TSIGMA assigns its own identity, bound to the polled device at configuration
- Derive identity from a vendor-provided identifier in the payload/header/filename
- A composite of vendor identifier + source

## Decision Outcome

**TSIGMA owns signal identity.** `signal_id` is a TSIGMA-assigned `BIGINT`, the
primary key of `config.signal`, bound to a device/connection at onboarding. The host
stamps it onto decoded events (ADR-0034); it is never read from the record. A
signal's human-readable name/label is a separate `TEXT` attribute — never the
identity.

A vendor-provided identifier, where one exists (filename, header, or an XML name
line), is a **provenance cross-check** (ADR-0044): a mismatch against the expected
device flags to the review queue — never a silent relabel, and never a source of
identity.

### Consequences

- Good, because identity is robust whether a vendor emits a name, a number, or nothing.
- Good, because `BIGINT` is a compact fixed-width key for the event log, its `compress_segmentby` (ADR-0030), and every aggregate.
- Good, because the poller shards TSIGMA's own device inventory and stamps our id (ADR-0039); it never parses vendor identifiers.
- Bad, because onboarding must bind each device to its TSIGMA `signal_id`; a mis-binding is a config error (mitigated by ADR-0044 provenance checks).

### Confirmation

`config.signal.signal_id` is `BIGINT`; the host attaches it from the polled device,
not the payload; vendor identifiers feed only ADR-0044 integrity flags; the name is a
separate `TEXT` column.

## Pros and Cons of the Options

### TSIGMA-owned identity (chosen)

- Good, because identity is stable, compact, and independent of vendor behavior.
- Bad, because onboarding must bind devices to ids.

### Derive from a vendor identifier

- Bad, because it's inconsistent, sometimes absent, and unstable across firmware/format — identity would break on a vendor change.

### Composite vendor id + source

- Bad, because it inherits the vendor identifier's unreliability and complicates the key for no gain.

## More Information

- ADR-0034 (host-owned spine attaches identity), ADR-0044 (provenance/integrity cross-checks → review queue)
- ADR-0009 (canonical event model), ADR-0030 (segment key), ADR-0039 (poller shards the device inventory)
- ADR-0028 (identifiers such as `signal_id` are integer device ids; the "strings are TEXT" rule governs names/free-text, not identity keys)
