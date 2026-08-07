# Auditing is a core requirement, not an afterthought

- **Status**: Accepted
- **Date**: 2026-06-28
- **Deciders**: jsloan@celldara.com

## Context and Problem Statement

DOT deployments are publicly accountable — to legislators, the public, federal
funders, and auditors. Audit bolted on late is invariably incomplete. TSIGMA
ingests from untrusted sources, runs closed plugins, and may share data
inter-agency. How should auditing be positioned in the core design?

## Decision Drivers

- Accountability: know who/what changed what, regardless of source (operator, plugin, inter-agency import, system).
- Closed plugins modify state — operators and auditors need a core-mediated record, not plugin-chosen logs.
- Audit bolted on late is invariably incomplete.
- User (emphatic): "auditing is a REQUIREMENT, not an afterthought."

## Considered Options

- Core-wide audit requirement (auditable by construction; missing audit = defect)
- Best-effort per-module audit
- Bolt-on audit module

## Decision Outcome

**Core-wide audit requirement.** Every meaningful state change — config edits
(signal/approach/detector), runtime settings, auth events, ingest corrections and
review-queue actions, inter-agency imports, plugin activity — is auditable by
construction. Missing audit on a meaningful change is a DEFECT. Audit is a
first-class core concern with its own durability, retention, query, and export,
using the same substrate regardless of source.

Every module design answers "what's the audit record for this change?" — no audit
story means the design isn't finished. Records capture WHAT/WHO/WHEN/BEFORE-AFTER.
Details (retention, immutability/append-only, export formats, unified-vs-per-domain)
defer to implementation; the requirement is locked now.

### Consequences

- Every module ships audit coverage from day one — no retrofit gaps.
- A uniform, queryable substrate across all sources; plugins can't evade core-mediated audit.
- Per-module design and runtime/storage cost; retention becomes an operational concern.

### Confirmation

Every module ADR/design has an explicit audit section; review flags state-change
paths that emit no audit record.

## Pros and Cons of the Options

### Core-wide requirement (chosen)

- Good, because coverage is uniform and plugin/inter-agency activity is audited through one substrate.
- Bad, because of per-module design cost and audit-log growth.

### Best-effort per-module

- Bad, because coverage is inconsistent and gaps surface during review.

### Bolt-on audit module

- Bad, because late audit is invariably incomplete and the retrofit is costly.

## More Information

- Related (forthcoming): config audit + effective-date; auth audit log; ingest review queue
