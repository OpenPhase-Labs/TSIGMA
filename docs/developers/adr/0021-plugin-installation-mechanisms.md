# Plugin installation: filesystem-local + OCI artifacts + HTTP registry

- **Status**: Accepted
- **Date**: 2026-06-28
- **Deciders**: jsloan@celldara.com

## Context and Problem Statement

Plugins need a path from "published" to "running." Agencies differ wildly:
air-gapped networks with no internet, container-native shops on OCI registries,
and agencies wanting a browse-and-install marketplace. How are plugins distributed
and installed?

## Decision Drivers

- Air-gapped / classified agencies must install without external network access.
- Container / k8s shops want OCI-native distribution.
- A browseable registry is an ecosystem play (no COTS competitor offers one).
- Closed vendor plugins (ADR-0007) need credible distribution channels.
- Agencies should mix mechanisms; the manifest (ADR-0020) is the unit of audit regardless of source.

## Considered Options

- Filesystem-local only
- OCI artifacts only
- HTTP plugin registry only
- All three

## Decision Outcome

**Support all three; agencies choose per plugin:**

1. **Filesystem-local** — binary + manifest dropped into a plugin directory; the
   core scans/loads. Required for air-gapped / custom / classified deployments.
2. **OCI artifacts** — plugins as OCI artifacts in a container registry; the core
   pulls. Fits container/k8s natively (incl. pods-per-family, ADR-0019).
3. **HTTP plugin registry** — project-maintained at first; vendors publish,
   agencies browse/install/update. A strategic ecosystem win.

### Consequences

- Every operating environment has a fitting mechanism; container shops reuse OCI supply-chain tooling; the registry builds a discoverable ecosystem; filesystem works with zero infrastructure.
- The core implements three loaders (they differ only in *acquisition*; manifest validation + `Register` are identical).
- Three supply-chain attack surfaces (each needs signing/provenance); running a registry is ongoing work — **defer registry standup until demand warrants**.

### Confirmation

The plugin host loads from each source; reference plugins are published per
mechanism; docs describe the trade-offs.

## Pros and Cons of the Options

### All three (chosen)

- Good, because it fits every environment, agencies mix per plugin, and there's ecosystem upside.
- Bad, because of three loaders + three supply-chain stories, and the registry is ongoing work (deferred).

### Filesystem only

- Bad, because there's no discovery/update and it doesn't fit container-native deployments.

### OCI only

- Bad, because air-gapped agencies need a local mirror and OCI fluency is required even for trivial plugins.

### Registry only

- Bad, because it's useless for air-gapped agencies and is a central dependency.

## More Information

- ADR-0018 (contract), ADR-0019 (process model / pods-per-family), ADR-0020 (manifest is the audit unit), ADR-0007 (closed plugins need distribution)
