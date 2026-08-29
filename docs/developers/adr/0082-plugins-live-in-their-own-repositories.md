# gRPC plugins live in their own repositories, not in the core

- **Status**: Accepted
- **Date**: 2026-08-29
- **Deciders**: jsloan@celldara.com

## Context and Problem Statement

ADR-0018 makes every extensible subsystem a separate-process gRPC plugin, and
ADR-0021 defines three ways to *install* one (filesystem-local, OCI artifact,
HTTP registry) — all of which assume a plugin is a separately built artifact.
Neither says where plugin **source** lives. Today every decoder, method, report,
and notification provider sits inside this repository as an in-process class, so
the unstated answer is "in the core". Where should plugin source live once the
gRPC migration lands?

## Decision Drivers

- Anyone can write a plugin. A plugin is **used by** the system, not **part of** it.
- ADR-0018 already puts the technical seam at a separate process, and ADR-0007 puts the legal seam there too: MPL-2.0 file-level copyleft plus "use in larger works", so a closed vendor plugin against an MPL core is viable. A vendor cannot ship closed source from inside this repository.
- ADR-0021's three installation mechanisms are all artifact-based; none of them implies, or benefits from, the plugin's source living in the core tree.
- Every plugin in the core is a core change: a new vendor format means a PR against TSIGMA, its release cadence, and its test suite.
- Plugin tests belong with plugin code. While `ftp_pull` lives here, `tests/unit/test_ftp_pull.py` is a core test that breaks when core internals move — 14 of them broke during this plan's P7b for exactly that reason, with no behaviour change at all.

## Considered Options

- Plugins in their own repositories; the core keeps only the plugin host
- Plugins in the core repository, built and shipped with it
- A monorepo: plugins in subdirectories of the core, released independently

## Decision Outcome

**A gRPC plugin lives in its own repository.** The core keeps only the machinery
for *hosting* plugins, never a concrete plugin.

Stays in TSIGMA:
- the plugin host runtime — `tsigma/plugins/` (protocol shim, connection seam, supervisor, broker, coexistence dispatch)
- generated contract stubs (`tsigma/plugins/gen/`), regenerated from the external contract
- the host-owned integrity spine — `ingest_raw`, decode/validate/persist orchestration (ADR-0034)
- registry dispatch, so a name resolves to whichever plugin serves it

Leaves TSIGMA, one repository each:
- decoders (asc3, maxtime, peek, siemens, d4, csv, openphase, wavetronix)
- ingestion methods (ftp_pull, http_pull, directory_watch, the tcp/udp/mqtt/nats/grpc listeners)
- reports, notification providers, and — when the privileged tier unblocks — auth providers and storage backends

The contract is already external (`/opt/webpages/TSIGMA-Contract`), so a plugin
author depends on the contract and never on this repository.

### Consequences

- Good, because a new vendor format is a new repository, not a PR against the core; the core stops growing with the device population (ADR-0018's stated goal, now true of the source tree and not only the process model).
- Good, because closed vendor plugins become straightforward: separate repo, separate build, separate licence, no MPL file-level copyleft question about core files (ADR-0007).
- Good, because plugin tests live with plugin code and stop breaking on core refactors.
- Good, because plugins version independently of the core; contract compatibility is negotiated per subsystem at registration (ADR-0004), not by shipping together.
- Bad, because a cross-cutting change now spans repositories, and the core cannot refactor a plugin in the same commit that changes what plugins must do.
- Bad, because first-party plugins need their own CI, release, and publishing setup — real overhead per repository.
- Bad, because "clone one repo and run it" stops being true; a working install needs the core plus whichever plugins the deployment uses.

### Confirmation

No concrete decoder, method, report, or notification provider under `tsigma/`
once the migration completes. `tsigma/plugins/` contains host machinery only. Any
plugin still in-tree is transition scaffolding under ADR-0018, marked as such and
scheduled for extraction.

## Pros and Cons of the Options

### Own repository per plugin (chosen)

- Good, because it matches where ADR-0018 and ADR-0007 already put the seam, and where ADR-0021 already expects the artifact to come from.
- Bad, because cross-repository changes and per-repository release overhead are real costs.

### Plugins in the core repository

- Good, because one clone, one CI, atomic cross-cutting changes.
- Bad, because it contradicts "anyone can write a plugin": a third party cannot add one without a PR, and a closed vendor cannot add one at all.

### Monorepo with independently released subdirectories

- Good, because it keeps atomic refactors while allowing separate release cadence.
- Bad, because it still requires a third party to land code in this repository, which is the thing the plugin boundary exists to avoid; it buys core convenience at the ecosystem's expense.

## More Information

- ADR-0018 (subsystems are gRPC plugins; the process/binary/copyright seam), ADR-0021 (installation: filesystem / OCI / HTTP registry), ADR-0007 (MPL-2.0; the boundary is the legal seam)
- ADR-0019 (process models — an externally orchestrated plugin is already assumed to be deployed separately), ADR-0004 (per-subsystem contract versioning)
- The contract repository is external by design and is what a plugin author depends on.
- Extraction order is the migration's business: `plans/2026-06-27-grpc-plugin-migration.md`. P7c (methods become transport-only) is the precondition for extracting a method at all — a method that decodes and persists needs the database; a transport-only one needs a socket and a gRPC connection.
