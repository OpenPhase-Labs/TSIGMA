# Plugin versioning: semver + capability flags + required core API version

- **Status**: Accepted
- **Date**: 2026-06-28
- **Deciders**: jsloan@celldara.com

## Context and Problem Statement

Plugins and the core evolve independently. The core must decide whether a given
plugin is compatible before running it, and plugins must degrade gracefully across
core versions. How is plugin/core compatibility expressed?

## Decision Drivers

- Plugins are upgraded independently of the core (ADR-0018/0019).
- The core must reject or warn on incompatible plugins before operating.
- Feature detection should be explicit, not inferred from version numbers alone.
- Vendors need a stable compatibility contract across core releases.

## Considered Options

- Semver + declared required core API version + capability flags
- Strict version pinning (exact core/plugin pairs)
- Best-effort (no declared compatibility)

## Decision Outcome

**Semver + a required core API version + capability flags.** Each plugin declares
its own semver, the **core API version** it requires, and the **capabilities** it
provides/consumes (in the manifest + `Register`, ADR-0020). The core checks the
required API version at registration and gates behavior on declared capabilities
rather than version-sniffing. Backward-compatible core changes bump the minor;
breaking changes bump the core API major, and plugins declare which majors they
support.

### Consequences

- The core can accept / reject / limit a plugin deterministically at registration.
- Plugins degrade gracefully — a capability absent on an older core is simply not used.
- Vendors get a stable compatibility contract; the core can evolve without breaking every plugin.
- The core maintains a capability registry and an API-version policy.

### Confirmation

Registration rejects/flags plugins whose required core API version is unmet;
behavior is gated on capability flags, not raw versions; compatibility events go to
the plugin audit table.

## Pros and Cons of the Options

### Semver + required API version + capabilities (chosen)

- Good, because compatibility is deterministic, feature detection is explicit, and the core and plugins evolve independently.
- Bad, because the core maintains a capability registry and an API-version policy.

### Strict pinning

- Bad, because every core upgrade forces a matching plugin rebuild — brittle.

### Best-effort

- Bad, because incompatible plugins fail unpredictably at runtime.

## More Information

- ADR-0018 (contract), ADR-0020 (manifest/Register declare version + capabilities), ADR-0015 (compatibility events audited)
