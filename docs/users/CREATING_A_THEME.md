# Creating an Agency Theme

TSIGMA is **white-label**: your deployment can carry your agency's colors, logo,
fonts, login page, and layout — with **no code changes and no build step**. You
supply a theme folder; TSIGMA does the rest.

This guide is for the person setting up a TSIGMA deployment. Developers
extending the theming system should read
[../developers/THEMING.md](../developers/THEMING.md).

---

## What you can customize

| You can change… | by editing… |
|---|---|
| All UI **colors** (light + dark) and chart colors | `theme.toml` |
| **Logo**, favicon, fonts, extra CSS | files in your theme's `static/` |
| The **navigation menu** (add / remove / reorder items) | a template override |
| The **layout** of any page or the whole shell | template overrides |

You never edit TSIGMA's code, and you only create the pieces you want to change
— everything else uses the built-in defaults.

## Step 1 — Copy the example

A complete, annotated starter theme ships at `themes/example/`. Copy it to a new
folder named for your agency (lowercase, no spaces):

```
themes/example/   →   themes/acme/
```

Your theme is just that folder:

```
themes/acme/
  theme.toml      # colors + fonts   (required)
  static/         # logo, favicon, fonts, custom.css   (optional)
  templates/      # nav / layout overrides   (optional)
```

## Step 2 — Set your colors (`theme.toml`)

Open `themes/acme/theme.toml`. Set `name` to your folder name, then fill in your
brand colors. The single most important line is **`brand`** — it drives buttons,
links, the nav bar, focus rings, and chart series.

```toml
[meta]
name = "acme"                 # MUST match the folder name
display_name = "Acme DOT"

[semantic.light]
brand = "#0a5fb4"             # ← your brand color
brand-foreground = "#ffffff"  # text/icons on top of the brand color
# … surface, foreground, border, success/warning/error, etc.

[semantic.dark]
brand = "#5aa9ec"             # usually a lighter shade for dark mode
# …
```

Every color is provided for **both** light and dark mode. The file is fully
commented — each token says what it controls. You don't have to invent values:
start from the example's working defaults and change what you want.

> Tip: phase colors (`[phase.*]` — the signal green/yellow/red used in charts)
> are kept separate from your brand on purpose, so a green phase always reads
> green. Leave them unless you have a reason to adjust.

## Step 3 — Add your logo and assets (optional)

Drop files under `themes/acme/static/` and reference them at `/static/...`:

```
themes/acme/static/
  img/logo.png        →  served at /static/img/logo.png
  favicon.ico
  fonts/MyFont.woff2
  css/custom.css       →  loaded last, for any final tweaks
```

Any file you don't provide falls back to the TSIGMA default.

## Step 4 — Change the nav or layout (optional)

To change templates, put a file at the **same path** as a default template,
under your theme's `templates/`. It replaces the default; everything else is
inherited.

**Example — swap the wordmark for your logo** in the nav: create
`themes/acme/templates/components/nav.html`, copy the default nav, and replace
the brand text with:

```html
<img src="/static/img/logo.png" alt="Acme DOT" class="h-7 w-auto">
```

You can override:

- `components/nav.html` — the top navigation: **add, remove, or reorder menu items**.
- `base.html` — the whole page shell: header layout, sidebar (or no sidebar), grid.
- `pages/…` — any individual page; `login.html` — the sign-in screen.
- any component, plus brand-new templates you add and reference yourself.

Use the same color utility classes the defaults use (`bg-surface`,
`text-foreground`, `bg-brand`, `text-brand-foreground`, `border-border`, …) so
your overrides automatically follow your `theme.toml` colors and light/dark.

## Available utility classes

Your template overrides can use a broad, standard set of Tailwind utility
classes **out of the box — no build step**. The shipped stylesheet already
includes:

- **Layout & spacing** — flexbox and grid (`flex`, `grid`, `grid-cols-3`,
  `gap-4`, `items-center`, `justify-between`), padding/margin (`p-4`, `mt-8`,
  `-mx-2`), sizing (`w-full`, `max-w-3xl`, `h-12`), positioning (`absolute`,
  `top-0`, `z-10`).
- **Typography** — sizes (`text-sm` … `text-4xl`), weights (`font-semibold`),
  alignment, `leading-*`, `tracking-*`, `truncate`, `line-clamp-2`.
- **Borders & effects** — `rounded-lg`, `border`, `ring-2`, `shadow-md`,
  `opacity-75`, transitions/transforms (`transition`, `scale-105`).
- **Responsive & state variants** — `sm: md: lg: xl: 2xl:` breakpoints and
  `hover: focus: focus-visible: disabled: dark:` on the classes above.

**Colors are intentionally limited** so your UI stays on-theme:

- **Semantic token utilities** — `bg-brand`, `text-foreground`, `bg-surface`,
  `border-border`, `text-brand-foreground`, `bg-success`, … follow your
  `theme.toml` and switch with light/dark automatically. Use these for anything
  that should look like *your brand*.
- **Neutral grays** — `bg-gray-100`, `text-gray-600`, `border-gray-200`, … are
  available for non-brand surfaces (subtle dividers, muted text).
- **Other literal colors are not available.** `bg-blue-500`, `text-red-600`,
  etc. are deliberately absent so the UI can't drift off-theme. If you truly
  need a specific color, or a utility outside the set above, add it in your
  theme's `static/css/custom.css` (loaded last) — the explicit, intentional
  escape hatch.

## Step 5 — Activate

Set the environment variable and restart TSIGMA:

```
TSIGMA_THEME=acme
```

The theme is applied at startup. Reload the page — the UI is now your brand, in
both light and dark, including the charts.

## Light / dark

Every theme ships both modes. Visitors get their operating-system preference on
first visit and can flip it with the sun/moon toggle in the nav; the choice is
remembered. You set the colors for both modes in `theme.toml` (Step 2).

## Worked example: GDOT

Georgia DOT's theme sets `brand` to GDOT blue in `theme.toml`, adds
`static/img/gdot-logo.png`, and overrides `templates/components/nav.html` to
show that logo. The result is a GDOT-blue nav bar with the GDOT logo and
blue charts, in light and dark — with zero changes to TSIGMA itself.

## Troubleshooting

| Symptom | Cause / fix |
|---|---|
| Log: `Theme 'acme' has no templates dir …; falling back to default theme` | Harmless if you only set colors — it just means you have no template overrides. |
| Colors didn't change | Did you set `TSIGMA_THEME=acme` and **restart**? Themes load at startup. |
| Startup error about a missing token | `theme.toml` must include every token in **both** `[semantic.light]` and `[semantic.dark]` (and `[phase.*]`). Start from `themes/example/` so none are missing. |
| Logo / custom.css not showing | Confirm the file is under your theme's `static/` and the path you reference matches (`/static/<path>`). |
| Brand color looks wrong on the nav bar | The nav uses `brand` as its background and `brand-foreground` as its text — make sure those contrast (e.g. white text on a dark brand). |
