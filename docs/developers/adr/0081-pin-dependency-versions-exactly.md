# Dependency versions are pinned exactly

- **Status**: Accepted
- **Date**: 2026-08-22
- **Deciders**: jsloan@celldara.com

## Context and Problem Statement

Dependencies were declared with open floors (`fastapi>=0.115`, `starlette>=1.1.0`).
Building a fresh environment on Linux resolved FastAPI to 0.141.1 and Starlette to
1.6.0 — far newer than what the code was written against — and two tests broke on
a changed `app.routes` shape. Should the declared versions be pinned, or keep
floating?

## Decision Drivers

- A floor-only declaration means the install differs by machine and by date; the same commit is not the same build.
- Breakage surfaces as unrelated test failures, so the cost lands on whoever builds next, not on whoever chose to upgrade.
- Development moved Windows → Linux (2026-08-07), which discarded the environment that had been holding versions steady by accident.
- Upstreams do not honor semantic versioning. Starlette behaved identically at 0.45 and 0.46, then removed the `TemplateResponse(name, {"request": request})` signature at 0.47 — a breaking change in a minor bump. A version range cannot be written against a scheme nobody follows.
- Agents and contributors cannot tell "my change broke this" from "the resolver moved" without a fixed baseline.
- TSIGMA is a deployed application (ADR-0003), not a library other projects resolve against, so tight pins cost nothing downstream.

## Considered Options

- Exact pins in `pyproject.toml` (`==`)
- Ranges with upper bounds plus a committed lockfile pinning transitives
- Upper bounds only (`>=x,<y`), no lockfile

## Decision Outcome

**Every direct dependency is pinned exactly** with `==` in `pyproject.toml`,
across `dependencies` and every `optional-dependencies` group. Upgrades are an
explicit edit: change the pin, run the suite, commit. There is no separate
lockfile and no resolver freedom for direct dependencies.

### Consequences

- Good, because the same commit installs the same versions on every machine and every date.
- Good, because an upgrade is a reviewable one-line diff attached to whatever it broke.
- Good, because it needs no tooling beyond pip — one file, no lock step to forget.
- Bad, because transitive dependencies still float; a break can still arrive from a dependency-of-a-dependency.
- Bad, because security patches no longer arrive by rebuilding — each one is a deliberate bump.

### Confirmation

No `>=`, `~=`, or unbounded specifier in `pyproject.toml`; every requirement
carries `==`. Upgrades appear in history as pin changes, not as environment
drift.

## Pros and Cons of the Options

### Exact pins in pyproject (chosen)

- Good, because it is reproducible with no tooling and no second source of truth.
- Bad, because transitives are uncontrolled and security bumps are manual.

### Ranges plus a committed lockfile

- Good, because it pins transitives too, which is the only way to make the build fully reproducible.
- Bad, because it splits versions across two files and adds a regen step that goes stale when skipped.

### Upper bounds only

- Bad, because a ceiling has to be guessed from a version scheme upstreams do not follow; Starlette 0.47 broke templating in a minor bump, so any bound short of exact admits the same break.
- Bad, because installs still vary between machines within the allowed range — the failure mode that prompted this ADR stays open.

## More Information

- ADR-0003 (one deployable — TSIGMA is deployed, not consumed as a library)
- Prompted by FastAPI 0.141.1 changing `app.routes` to leave `_IncludedRouter` wrappers in place of flattened routes, and by Starlette 0.47 having previously removed the `TemplateResponse(name, {"request": request})` signature.
