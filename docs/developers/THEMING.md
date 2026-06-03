# Theming & White-Label (Developer Reference)

> Part of [TSIGMA Architecture](../ARCHITECTURE.md) · see also [UI.md](UI.md)
> **Creating a theme as a deployer/agency?** See the task guide:
> [CREATING_A_THEME.md](../users/CREATING_A_THEME.md). This document is the
> *developer* reference — how the system works and how to develop for it.

How TSIGMA is branded per agency. Every deployment can carry its own colors,
logo, fonts, login page, **and layout** — without forking. An agency supplies
*token values + assets + optional template/CSS overrides*; never a code change
and never a Tailwind build.

---

## 1. Model

- **Single-agency per deployment.** The active theme is named by the
  `TSIGMA_THEME` setting and resolved **once at startup**.
- **`default` is the built-in theme** — `tsigma/templates/` + `tsigma/static/`
  are the fallback layer. Any agency theme *shadows* it file-by-file; anything
  not overridden falls through to the default.
- **A theme is a directory** under `themes/<name>/`:

  ```
  themes/<name>/
    theme.toml          # brand token values (light + dark), fonts
    templates/          # optional Jinja overrides — shadow ANY default template
    static/             # optional logo, favicon, fonts, custom.css
  ```

- **Open-core vs deployment:** `themes/` is gitignored **except**
  `themes/example/` (the committed template agencies copy). Real agency themes
  (logos, brand colors) are deployment artifacts, not committed to core.

## 2. Token system

Colors live in **CSS custom properties**, not hardcoded in templates. Two layers
plus phase colors:

- **Semantic tokens** — the only colors templates/components reference:
  `--color-surface`, `--color-surface-raised`, `--color-foreground`,
  `--color-muted-foreground`, `--color-border`, `--color-ring`, `--color-brand`,
  `--color-brand-foreground`, and `--color-success` / `-warning` / `-error`
  (each with a `-foreground`).
- **Phase/domain tokens** — kept *separate from brand* so a green phase always
  reads green: `--chart-phase-green` / `-yellow` / `-red`. Themeable and
  mode-adjusted, but never derived from `--color-brand`.
- **Light/dark** — semantic + phase tokens have two value sets: light in
  `:root`, dark under `[data-mode="dark"]`.

Tailwind utilities are bound to these vars (`bg-surface`, `text-foreground`,
`border-border`, `bg-brand`, `text-brand-foreground`, `bg-success/15`, …), so
changing a token value recolors the **entire UI and all charts** at once.
Templates must use these semantic utilities — **never a literal gray/color**.

## 3. `theme.toml`

```toml
[meta]
name = "acme"                 # must match the folder name and TSIGMA_THEME
display_name = "Acme DOT"

[fonts]
display = "Bricolage Grotesque"   # headings
body = "IBM Plex Sans"            # body
mono = "IBM Plex Mono"            # metrics

[semantic.light]                  # every token below is REQUIRED
surface = "#f9fafb"
surface-raised = "#ffffff"
foreground = "#111827"
muted-foreground = "#6b7280"
border = "#e5e7eb"
ring = "#6366f1"
brand = "#6366f1"                 # ← agency brand color
brand-foreground = "#ffffff"
success = "#16a34a"
success-foreground = "#ffffff"
warning = "#d97706"
warning-foreground = "#ffffff"
error = "#dc2626"
error-foreground = "#ffffff"

[semantic.dark]                   # same keys, dark values (brand usually lighter)
# …

[phase.light]                     # green = "#16a34a", yellow = "#f59e0b", red = "#dc2626"
[phase.dark]                      # green = "#4ade80", yellow = "#fbbf24", red = "#f87171"
```

`tsigma/theming/tokens.py` renders this into a `:root { … }` /
`[data-mode="dark"] { … }` `<style>` block injected inline in `base.html`
`<head>` (after `tailwind.css`, so theme values win). **Every required token
must be present in both light and dark** — a missing one raises
`TokenValidationError` at startup. The complete working reference is the
committed `tsigma/theming/default_theme.toml`; the annotated, fill-in-your-brand
copy is `themes/example/theme.toml`.

## 4. Creating a theme (deployers)

Agencies create themes without touching code — the full step-by-step (with a
GDOT worked example) is in [CREATING_A_THEME.md](../users/CREATING_A_THEME.md). In
short: copy `themes/example/`, edit `theme.toml`, optionally override templates
(`templates/…`) and static assets (`static/…`), set `TSIGMA_THEME`, restart.
Anything not overridden inherits the default via the ChoiceLoader (templates)
and the static resolver (assets).

## 4a. Developing for theming

- **Add a semantic token:** add it to `[semantic.light]` + `[semantic.dark]` in
  `default_theme.toml` *and* to `REQUIRED_SEMANTIC_TOKENS` in `tokens.py`; add
  the matching `--color-<name>` to the `@theme` block in `tailwind.src.css`;
  rebuild `tailwind.css`. Templates can then use `bg-<name>` / `text-<name>` /
  `border-<name>`.
- **Templates reference only semantic utilities** (`bg-surface`,
  `text-foreground`, `bg-brand`, `border-border`, …) — never a literal
  gray/color — so themes and light/dark resolve automatically. Reusable markup
  lives in `components/ui.html` (button/badge/field/card/page_header macros).
- **Make a chart/map theme-aware:** read colors from `tsigma.theme.tokens()`
  and register for re-theming (`tsigma.theme.register(chart, render)` or
  `tsigma.theme.onChange(cb)`) instead of hardcoding hex (§7).
- **Rebuild CSS** (`scripts/build_css.ps1`) only when utility *class usage* in
  templates changes — not when token *values* change (those inject at render
  time).

## 5. Resolution at startup

- **Templates** — `tsigma/theming/resolver.py` builds a Jinja `ChoiceLoader`
  of `[<theme>/templates, <default>/templates]` (theme first). A named theme
  whose directory is absent logs a WARNING and falls back to default.
- **Static** — `tsigma/theming/static.py` resolves `/static/<path>` across
  `[<theme>/static, <default>/static]` (theme first, path-traversal guarded),
  wired in `tsigma/app.py`.
- **Tokens** — `tokens.token_style_for()` loads the active `theme.toml` (or the
  default) and renders the inline `<style>`; exposed to templates as the
  `theme_token_style` Jinja global.

## 6. Light / dark mode

- `data-mode` on `<html>`: light = `:root` token set, `[data-mode="dark"]`
  overrides.
- **First visit** follows OS `prefers-color-scheme`; the **nav toggle**
  (`data-mode-toggle`) persists the choice to `localStorage`.
- **No FOUC** — a tiny inline script in `<head>` sets `data-mode` before paint.
- Flipping mode dispatches `tsigma:themechange`; charts/maps re-theme (§7).

## 7. Charts & maps (`theme.js`)

Canvas/WebGL widgets can't read CSS variables, so `tsigma/static/js/theme.js`
bridges them:

- `tsigma.theme.tokens()` — current resolved token values.
- `tsigma.theme.echartsBase()` / `.axis()` — ECharts option fragments from tokens.
- `tsigma.theme.register(chart, render)` — re-renders the chart on
  `tsigma:themechange`; `tsigma.theme.onChange(cb)` for MapLibre paint updates.

Chart modules use phase tokens for signal colors and `brand` for series, so they
recolor with the theme and the mode toggle.

## 8. Compiled CSS (Tailwind v4)

- **Source:** `tsigma/static/css/tailwind.src.css` — `@import "tailwindcss"`,
  the self-hosted `@font-face` rules, the `@custom-variant dark` (keyed to
  `[data-mode="dark"]`), and the `@theme` block binding utilities to the token
  vars.
- **Build:** the vendored Tailwind v4 standalone CLI compiles it to the
  committed `tsigma/static/css/tailwind.css` via `scripts/build_css.ps1`
  (developer-side only; the binary is fetched per-dev, not committed).
- **Agency-agnostic:** the compiled CSS is the same for every agency — utilities
  reference the vars; per-theme/per-mode *values* are injected at render time
  (§3). Agencies never run a Tailwind build.
- **Rebuild when** utility class usage changes in templates (not when only token
  *values* change). Deployers serve the committed `tailwind.css` as-is.

## 9. File map

| Path | Role |
|------|------|
| `tsigma/theming/resolver.py` | template ChoiceLoader + theme dir resolution |
| `tsigma/theming/static.py` | theme→default static file resolution |
| `tsigma/theming/tokens.py` | `theme.toml` → validated CSS-var `<style>` |
| `tsigma/theming/default_theme.toml` | built-in Modern Airy token values |
| `tsigma/static/css/tailwind.src.css` | Tailwind v4 source (`@theme`, fonts) |
| `tsigma/static/css/tailwind.css` | committed compiled output |
| `tsigma/static/js/theme.js` | token→ECharts/MapLibre bridge |
| `tsigma/templates/components/ui.html` | reusable partials (button/badge/field/card/page_header) |
| `themes/example/` | committed agency theme template |
| `scripts/build_css.ps1` | developer CSS build |
