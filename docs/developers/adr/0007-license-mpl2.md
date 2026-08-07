# Open-source license: MPL-2.0

- **Status**: Accepted
- **Date**: 2026-06-28
- **Deciders**: jsloan@celldara.com

## Context and Problem Statement

TSIGMA is open-source, but vendors must be able to ship proprietary plugins
(separate gRPC processes) without forking the core or releasing their plugin
source. The license must keep the core open and protected from fork-and-close,
allow closed plugins, and include a patent grant (ITS standards like NTCIP carry
patent considerations).

## Decision Drivers

- Keep the core open, protected against fork-and-close.
- A closed-source plugin process must not be forced to release its source.
- A contributor patent grant matters for ITS work.
- Compose cleanly with the Python dependency ecosystem (MIT/BSD/Apache/MPL); GPL/AGPL deps in core would poison the closed-plugin story.

## Considered Options

- MPL-2.0
- Apache-2.0
- GPL / AGPL v3
- BSD-3-Clause / MIT

## Decision Outcome

**MPL-2.0.** File-level (weak) copyleft: modifications to MPL files return to the
project, but "use in larger works" is free — a vendor running a closed plugin
process against the core, or importing a core library, need not release its own
code. MPL-2.0 carries an explicit patent grant. The gRPC plugin boundary is the
legal and technical seam that makes "closed plugin against MPL core" viable.

Open sub-question: SDK / contract files (`.proto` + generated
stubs) may be licensed Apache-2.0 to remove copyleft ambiguity for downstream
consumers — resolve before publishing SDK packages.

### Consequences

- The core is protected from fork-and-close; plugins stay commercially flexible.
- The explicit patent grant covers contributed code.
- GPL/AGPL dependencies must be avoided in the core.

### Confirmation

Repo root has the full MPL-2.0 `LICENSE`; new core files carry the `MPL-2.0` SPDX
header; dependency manifests are reviewed for GPL/AGPL; SDK-file licensing is
resolved before the first SDK publish.

## Pros and Cons of the Options

### MPL-2.0 (chosen)

- Good, because file-level copyleft protects the core while closed plugins are allowed; explicit patent grant.
- Bad, because SDK-file licensing needs explicit resolution and file-level copyleft is subtler than permissive.

### Apache-2.0

- Good, because patent grant and permissive.
- Bad, because no copyleft — fork-and-close is allowed.

### GPL / AGPL

- Bad, because viral — it kills the closed-plugin story and draws adoption hostility.

### BSD-3-Clause / MIT

- Bad, because no copyleft and no patent grant.

## More Information

- Forthcoming: gRPC plugin contract (the legal/technical seam)
- MPL-2.0: https://www.mozilla.org/en-US/MPL/2.0/ — SPDX: `MPL-2.0`
