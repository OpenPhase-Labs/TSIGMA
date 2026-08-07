# Vocabulary / controlled-lookup pattern

- **Status**: Accepted
- **Date**: 2026-06-28
- **Deciders**: jsloan@celldara.com

## Context and Problem Statement

Classifications, codes, and labels recur across the system (event codes, event
categories, detector types, validation statuses, alert classifications).
Operators across agencies use different labels for the same concept, while
inter-agency exchange (Method-B) needs a shared canonical vocabulary. How should
the core handle codes/labels so local terms are honored without breaking exchange?

## Decision Drivers

- Different agencies use different labels for the same concept.
- Inter-agency exchange needs a canonical vocabulary peers can interpret.
- The core must not hardcode any agency's terminology.
- Plugins must work against canonical codes so they port across agencies.
- Apply the pattern uniformly, not reinvented per feature.

## Considered Options

- Canonical core vocabularies + agency-configurable local labels + required mapping (the pattern)
- One hardcoded vocabulary
- Pure agency-defined vocabularies (no canonical layer)
- Per-feature ad-hoc designs

## Decision Outcome

**A uniform vocabulary pattern.** The core ships canonical reference vocabularies
(NTCIP / Indiana-HiRes-aligned where covered, core-defined otherwise). Each agency
may configure local labels for display/ops, with a mapping back to canonical for
inter-agency exchange. Applies to anything with codes/enums/labels: event codes,
event categories, detector types, validation statuses, alert classifications, etc.

Each configurable vocabulary gets: a canonical enum (core-shipped), an optional
agency label table, a mapping to canonical, and a seeded starter set.

### Consequences

- Operators keep familiar labels; inter-agency exchange uses the canonical mapping.
- Plugins written against canonical codes port across agencies.
- Schema overhead per classification (canonical + local + mapping); the core maintains canonical enums where no standard covers a concept.

### Confirmation

Core logic references canonical codes/roles, not hardcoded label strings; plugin
contracts use canonical codes; agency labels appear only in UI / operator APIs;
inter-agency wire carries the canonical mapping.

## Pros and Cons of the Options

### Canonical + local + mapping (chosen)

- Good, because it honors local vocabulary, enables exchange, keeps plugins portable, and is uniform.
- Bad, because of per-classification schema overhead.

### One hardcoded vocabulary

- Bad, because it alienates operators or collapses real distinctions.

### Pure agency-defined

- Bad, because exchange becomes ad-hoc per-peer and plugins go per-agency.

### Per-feature ad-hoc

- Bad, because it guarantees inconsistency and breaks exchange unpredictably.

## More Information

- Forthcoming: inter-agency method (needs canonical mapping); canonical event model
