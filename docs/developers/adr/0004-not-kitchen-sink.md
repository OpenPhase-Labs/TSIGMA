# Core provides data; specialized tools own workflows (not kitchen-sink)

- **Status**: Accepted
- **Date**: 2026-06-28
- **Deciders**: jsloan@celldara.com

## Context and Problem Statement

Platforms that try to own every traffic-analytics workflow end up shallow at all
of them. TSIGMA's value is being the open, plugin-extensible backbone for signal
event data — not another walled garden. What decides whether the core owns a
workflow or just exposes data for a specialized tool?

## Decision Drivers

- Focused tools beat kitchen-sink platforms in their domain.
- TSIGMA's moat is the open plugin SDK + canonical event store, not feature breadth.
- A zero-plugin install must still be usable (core-complete baseline).
- Canonical data models (events, signals, devices, config, audit) must live centrally — in the core.

## Considered Options

- Core provides DATA; specialized tools / plugins own WORKFLOWS (with carve-outs)
- Kitchen sink — the core ships every workflow
- Pure data layer — no workflows at all
- Workflow plugins only — nothing ships in the core

## Decision Outcome

**The core provides data; specialized tools own workflows.** The core owns:
canonical data models (events, signals/approaches/detectors, devices, config,
audit); workflows nobody else does (e.g. inter-agency Method-B sharing); and a
minimum-viable surface so a no-plugin install is usable. Everything else —
analytics/report computation (even canonical ATSPM metrics), dashboards,
optimization — is exposed as data and owned by report plugins or external tools.

Decision lens for any feature: does the core need to OWN this workflow, or just
expose the DATA? Default to exposing data when a report/plugin can own it.

### Consequences

- The core stays focused and shippable.
- Report/analytics plugins own computation; the core owns the event store + API.
- Requires discipline to refuse "the core should just do X" when X is a report/plugin concern.

### Confirmation

Feature proposals state "core owns workflow, or exposes data?"; review rejects
core workflow that duplicates a report/plugin without invoking the minimum-viable bar.

## Pros and Cons of the Options

### Core = data, tools = workflow (chosen)

- Good, because it plays to the plugin-SDK moat and keeps the core small.
- Bad, because it ships less out of the box than kitchen-sink products; needs discipline.

### Kitchen sink

- Good, because it demos broad.
- Bad, because each workflow ends up shallow and the core scope explodes.

### Pure data layer

- Bad, because a no-plugin install isn't usable.

### Workflow plugins only

- Bad, because baseline workflows get reinvented per plugin.

## More Information

- ADR-0002 (core principle), ADR-0003 (core composition)
- Forthcoming: gRPC plugin contract; report plugin SDK; inter-agency Method-B sharing
