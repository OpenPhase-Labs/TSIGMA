# Plugin declaration: manifest file + runtime gRPC Register

- **Status**: Accepted
- **Date**: 2026-06-28
- **Deciders**: jsloan@celldara.com

## Context and Problem Statement

The core needs to know what each installed plugin provides — capabilities, config
schema, version, required core API level, process-model preference — before and
during operation. Operators must audit a plugin before installing; registries must
list capabilities without running it; the core needs ground truth at runtime even
if an on-disk manifest is stale.

## Decision Drivers

- Operators (air-gapped / review boards) must audit claimed capabilities and config before install.
- Registries must list capabilities without executing plugin code.
- A manifest on disk can drift or lie — runtime state must be authoritative.
- Plugin authors need a clear declarative surface.
- Capability-based compatibility (ADR-0022) needs a structured declaration.

## Considered Options

- Manifest file only
- Runtime registration only
- Both: manifest + runtime `Register` RPC

## Decision Outcome

**Both.** Every plugin ships a manifest (format defers to implementation) AND
re-declares via a gRPC `Register` RPC at startup. The manifest declares: id,
version, name, description; capabilities (decoder formats, method types, report
ids, notify channels, etc.); required core API version; config schema (validates
plugin config); process-model preference (ADR-0019); resource hints. At startup
the plugin re-declares the same surface via `Register`; the core reconciles —
**runtime wins** if it diverges from the manifest, and the divergence is logged as
a plugin audit event. The manifest is the static contract for pre-install audit
and registries; runtime `Register` is the truth the core acts on.

### Consequences

- Operators audit before install; registries list capabilities without running plugins.
- The core never operates on stale or fabricated declarations.
- Authors keep two surfaces in sync (usually generated from one source in the SDK).

### Confirmation

The SDK provides a manifest schema + a `Register` stub; the core logs
manifest↔runtime divergence to the plugin audit table; registry tooling lists
capabilities from manifests without executing binaries.

## Pros and Cons of the Options

### Both (chosen)

- Good, because both audiences get what they need (pre-install audit + registry listing vs. runtime ground truth) and divergence is detectable.
- Neutral, because there are two surfaces (often generated from one).

### Manifest only

- Bad, because a stale/fabricated manifest misleads the core and there's no runtime feature detection.

### Runtime only

- Bad, because there's no pre-install audit and registries can't list without running plugins.

## More Information

- ADR-0018 (gRPC contract), ADR-0019 (process-model preference), ADR-0021 (registry surfaces manifests), ADR-0022 (capability flags), ADR-0015 (plugin audit events)
