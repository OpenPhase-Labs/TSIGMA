# Vendored frontend libraries: no npm/CDN at runtime

- **Status**: Accepted
- **Date**: 2026-06-28
- **Deciders**: jsloan@celldara.com

## Context and Problem Statement

The frontend uses Alpine, ECharts, MapLibre, Tailwind, etc. How are these shipped —
built from npm, loaded from a CDN, or vendored into the repo?

## Decision Drivers

- Air-gapped/restricted networks can't reach npm or a CDN.
- Reproducible, auditable deployments (no surprise CDN content / version drift).
- No build pipeline (ADR-0069) means no npm install/bundle step.
- Supply-chain: pinned, reviewed assets beat live CDN fetches.

## Considered Options

- Vendor (commit) the built library assets into the repo; serve them locally
- Build from npm at deploy time
- Load from a public CDN at runtime

## Decision Outcome

**Frontend library assets are vendored — committed into the repo and served
locally** by the app. No npm install/build at deploy time, and no runtime CDN
dependency. Upgrading a library is a deliberate, reviewed commit of the new asset.
(Tailwind, used for styling, is applied via its vendored build output; its
content/safelist contract is an implementation detail of that build.)

### Consequences

- Works fully offline/air-gapped — no external fetch needed to run the UI.
- Deployments are reproducible and auditable; no CDN version drift or outage risk.
- Library upgrades are explicit commits (reviewable), not transitive resolutions.
- The repo carries built JS/CSS assets (size); upgrades are manual.

### Confirmation

Library assets are committed and served locally; no npm/build step at deploy; no
runtime CDN references; upgrades land as reviewed asset commits.

## Pros and Cons of the Options

### Vendored assets (chosen)

- Good, because it's offline-capable, reproducible/auditable, free of CDN risk, and needs no build step.
- Bad, because the repo carries assets and upgrades are manual.

### Build from npm

- Bad, because it needs a build pipeline + registry access (breaks air-gapped, contradicts ADR-0069).

### Runtime CDN

- Bad, because it breaks offline use and adds version/outage/supply-chain risk.

## More Information

- ADR-0069 (no SPA build), ADR-0014 (works in restricted deployments), ADR-0064 (a strict CSP is easy with local assets)
