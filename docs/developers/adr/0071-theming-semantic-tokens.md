# Theming: semantic design tokens (CSS custom properties), layered resolution

- **Status**: Accepted
- **Date**: 2026-06-28
- **Deciders**: jsloan@celldara.com

## Context and Problem Statement

TSIGMA needs theming — light/dark, and agency branding. How are themes defined and
applied so components don't hardcode colors and agencies can customize?

## Decision Drivers

- Components must not hardcode colors/spacing — they reference semantic roles.
- Light/dark and per-agency branding must be supported.
- Theming should work with server-rendered pages + vendored CSS (ADR-0069/0070), with no build per theme.

## Considered Options

- Semantic design tokens as CSS custom properties, resolved in layers (base → theme → agency override)
- Hardcoded per-component styles
- A separate compiled stylesheet per theme

## Decision Outcome

**Theming uses semantic design tokens implemented as CSS custom properties**,
resolved in **layers**: a base token set, a theme layer (e.g. light/dark) that
overrides tokens, and an optional agency-branding layer on top. Components reference
**semantic tokens** (e.g. `--color-surface`, `--color-accent`), never raw colors.
Switching theme/branding swaps token values at runtime — no per-theme stylesheet
build.

### Consequences

- A new theme or agency brand is a set of token overrides, not a component rewrite or a new build.
- Light/dark and branding are runtime token swaps (works with server-rendered pages).
- Components stay theme-agnostic (they reference semantic roles).
- Designers/operators must work within the defined token vocabulary.

### Confirmation

Components reference semantic CSS-custom-property tokens (no raw colors);
themes/branding are layered token overrides; theme switching is a runtime token swap
with no per-theme build.

## Pros and Cons of the Options

### Semantic tokens + layered resolution (chosen)

- Good, because it's themeable/brandable without rewrites or per-theme builds, with a clean component contract.
- Bad, because it requires discipline (a defined token vocabulary).

### Hardcoded per-component styles

- Bad, because there's no theming without editing every component.

### Per-theme compiled stylesheet

- Bad, because it needs a build per theme — contradicting the no-build stack (ADR-0069/0070).

## More Information

- ADR-0069 (frontend stack), ADR-0070 (vendored CSS); THEMING.md token-vocabulary detail to be reconciled
