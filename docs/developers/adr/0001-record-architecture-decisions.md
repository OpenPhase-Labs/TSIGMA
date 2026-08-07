# Record architecture decisions

- **Status**: Accepted
- **Date**: 2026-06-28
- **Deciders**: jsloan@celldara.com

## Context and Problem Statement

TSIGMA is an open-source traffic-signal event ingestion and analytics platform,
designed for adoption by multiple DOTs and by controller / sidecar vendors
writing plugins. Architectural decisions made during design and development need
to be visible to all contributors, reviewable by humans before adoption, linkable
from code and other docs, durable across many years of personnel and tool
changes, and discoverable by vendors who need to understand WHY decisions were
made (not just WHAT).

How should we record significant architectural decisions?

## Decision Drivers

- The project is open-source; contributors include vendors writing closed-source plugins who depend on a stable, well-reasoned core
- Long project lifetime expected; decisions need to survive personnel and tool changes
- Decisions that affect the plugin boundary must be legible to third-party plugin authors
- Many decisions are being made in design conversations; without a durable repository they get lost
- We do not want to invent a new format

## Considered Options

- **MADR (Markdown Any Decision Records)** — https://adr.github.io/madr/
- **Nygard's classic ADR format** — short: Title / Status / Context / Decision / Consequences
- **No formal practice** — decisions captured ad-hoc in commit messages, design docs, issues
- **IEEE 1471-style architecture description** — heavier, formal documentation framework

## Decision Outcome

**Chosen option**: MADR, because it is a well-maintained standard with a public
template repository, Markdown-based (plays well with code review), structured
enough to capture nuance (Context / Drivers / Options / Decision / Consequences /
Pros and Cons), lighter than IEEE 1471 but richer than Nygard's original, and
widely adopted in open-source projects (so contributors arrive familiar with it).

ADRs live in `docs/developers/adr/`.

### Consequences

- Good, because every significant architectural decision becomes a reviewable, version-controlled, citable artifact
- Good, because future contributors and plugin authors can read the reasoning behind a decision, not just the result
- Good, because superseded decisions remain in the record with explicit links to what replaced them — design archaeology is possible
- Good, because plugin authors can audit the choices that affect the plugin boundary
- Bad, because writing an ADR adds friction to making decisions; we accept this for *significant* decisions and skip it for trivial ones (see `README.md` § "When NOT to write an ADR")
- Bad, because the initial backfill of decisions already embedded in the developer docs is a non-trivial chunk of work (see `README.md` § "Backfill queue")

### Confirmation

- `docs/developers/adr/` exists, populated with this ADR and the `template.md`
- New architectural decisions land as ADRs through normal review
- Code, docs, and other ADRs cite ADR numbers when referencing decisions

## Pros and Cons of the Options

### MADR

Markdown Any Decision Records — https://adr.github.io/madr/

- Good, because structured and consistent across ADRs
- Good, because actively maintained, public template repository
- Good, because Markdown is the lingua franca of project docs and code reviews
- Neutral, because slightly more verbose than Nygard's original
- Bad, because requires copy-pasting the template each time

### Nygard's classic ADR

From Michael Nygard's 2011 post — five short sections: Title, Status, Context, Decision, Consequences.

- Good, because very lightweight
- Good, because Nygard's seminal post remains influential and widely cited
- Bad, because lacks structured "Considered Options" and "Pros/Cons" — nuance is harder to capture
- Bad, because no maintained public template repository

### No formal practice

Decisions captured ad-hoc in commit messages, design docs, issues.

- Good, because zero overhead
- Bad, because architectural reasoning gets lost over time
- Bad, because no canonical place for vendors and contributors to learn the WHY behind decisions
- Bad, because PR discussions and design conversations are ephemeral

### IEEE 1471 architecture description

Formal architecture documentation framework.

- Good, because most thorough and formal
- Bad, because excessive overhead for most decisions
- Bad, because primarily a documentation framework, not a decision-record format

## More Information

- MADR specification: https://adr.github.io/madr/
- Michael Nygard's original ADR post: https://www.cognitect.com/blog/2011/11/15/documenting-architecture-decisions
- ADR community resources: https://adr.github.io/
- Inherited (abandoned-repo) ADRs kept for reference: [holding/](holding/)
- Backfill queue (decisions embedded in the dev docs, awaiting ADRs): see [README.md](README.md#backfill-queue)
