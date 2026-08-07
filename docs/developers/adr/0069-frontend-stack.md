# Frontend stack: server-rendered Jinja2 + Alpine.js + ECharts/MapLibre (no SPA build)

- **Status**: Accepted
- **Date**: 2026-06-28
- **Deciders**: jsloan@celldara.com

## Context and Problem Statement

TSIGMA's UI shows dashboards, signal-performance charts (reports, PCD), and maps. It
must be maintainable by a small team and deployable in restricted/air-gapped
environments. What frontend architecture?

## Decision Drivers

- A small team shouldn't maintain a heavy SPA build/toolchain.
- Air-gapped/restricted deployments complicate npm-based build pipelines.
- Charts (ECharts) and maps (MapLibre) are needed, with modest interactivity (Alpine).
- Server-rendered pages are simpler to secure and serve.

## Considered Options

- Server-rendered Jinja2 templates + Alpine.js + ECharts + MapLibre (no SPA build)
- A full SPA (React/Vue/etc.)
- Server-rendered only (no client JS)

## Decision Outcome

**Server-rendered HTML (Jinja2) enhanced with Alpine.js for interactivity, ECharts
for charts, and MapLibre GL for maps — no SPA build step.** Pages render on the
server; Alpine adds client-side behavior; ECharts/MapLibre fetch metric data from
the API (public GET endpoints, ADR-0058) to draw charts/maps. There is no React/Vue
SPA and no JS bundling/build pipeline.

### Consequences

- No SPA build toolchain to maintain; pages are server-rendered and simple to serve/secure.
- Rich charts/maps via ECharts/MapLibre without a framework.
- Air-gapped deployments avoid an npm build pipeline (see vendored libs, ADR-0070).
- Highly-interactive, app-like UX is harder than with a full SPA (acceptable — TSIGMA is dashboards + forms).

### Confirmation

Pages are Jinja2 server-rendered; interactivity is Alpine.js; charts ECharts; maps
MapLibre; there is no SPA framework or JS build step.

## Pros and Cons of the Options

### Server-rendered + Alpine + ECharts/MapLibre (chosen)

- Good, because there's no build toolchain, it's simple to serve/secure, charts/maps are capable, and it's small-team-friendly.
- Bad, because it's less suited to highly-interactive app UX than a full SPA.

### Full SPA

- Bad, because of a heavy build/toolchain, harder air-gapped deployment, and more to secure/maintain for a dashboard app.

### Server-rendered, no client JS

- Bad, because it can't do the interactive charts/maps consumers expect.

## More Information

- ADR-0070 (vendored frontend libs, no npm/CDN), ADR-0071 (theming), ADR-0058 (UI fetches public metric GETs), ADR-0055 (REST API)
