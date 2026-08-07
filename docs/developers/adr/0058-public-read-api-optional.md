# Public read access to metrics; authentication for writes and admin

- **Status**: Accepted
- **Date**: 2026-06-28
- **Deciders**: jsloan@celldara.com

## Context and Problem Statement

TSIGMA's UI (server-rendered pages with client-side Alpine + ECharts) renders
signal-performance visualizations (reports, Purdue Coordination Diagrams, etc.) by
querying metric GET endpoints. As with typical
public ATSPM dashboards, that metric/analytics data is meant to be viewable
without login. But writes and administrative/configuration functions must be
protected. Where is the public-vs-authenticated line?

## Decision Drivers

- The UI draws charts from metric/report GET endpoints; forcing login to view public signal-performance data is undesirable (public-dashboard model).
- Pre-computed aggregates can't answer everything — some reports/visualizations (e.g. PCD) need custom on-demand queries against the raw IHR event table.
- Writes and admin/config (signal/detector config, users, keys, settings, audit) must be authenticated.
- Some agencies may need to lock the whole instance behind auth (internal-only deployments).

## Considered Options

- Public anonymous GET for metric/analytics reads; auth for writes + admin/config; optional full lock-down
- Everything authenticated (opt-in public)
- Everything public

## Decision Outcome

**Metric/analytics read endpoints are public (anonymous GET)** — reports, PCD,
signal-performance, the signal/detector inventory the UI renders, and **custom
on-demand queries that hit the raw IHR event table** (pre-computed aggregates can't
answer everything). The protection for the public read surface — including the
raw-IHR-backed queries — is the **query guards (ADR-0057: max-lookback /
max-aggregation, clamped page size) plus rate limiting**, not authentication;
there is no separate auth wall for raw-vs-aggregate reads.

**Writes (POST/PUT/PATCH/DELETE) and administrative/configuration endpoints**
(signal/detector config, user/key management, jurisdictions, settings, audit logs)
**require authentication** (ADR-0059); so do other sensitive reads (user/key lists,
audit). An agency may optionally **require authentication for the metric reads too**
(internal-only lock-down) via configuration.

### Consequences

- The UI renders public dashboards with no login, matching the public-ATSPM model.
- Sensitive operations (writes, config, admin, audit) stay protected.
- Public read endpoints need guards + rate limiting to resist abuse (already required, ADR-0057).
- Jurisdiction scoping (ADR-0013) applies to authenticated users; the public read surface exposes the agency's public metric data (operators decide its scope) — see open item.

### Confirmation

Metric/report GETs work anonymously; writes and admin/config require auth; public
reads are guarded + rate-limited; a config flag can require auth for reads
(lock-down).

## Pros and Cons of the Options

### Public metric reads + authenticated writes/admin (chosen)

- Good, because it gives the public-dashboard UX with no login while keeping sensitive operations protected, plus a lock-down option.
- Bad, because the public read surface is exposed (guarded) and the public-vs-sensitive read boundary must be drawn carefully.

### Everything authenticated (opt-in public)

- Bad, because there are no public dashboards — the UI would force login to view metrics.

### Everything public

- Bad, because it exposes writes/config/admin — unacceptable.

## More Information

- ADR-0055 (API surfaces), ADR-0057 (guards/rate limits on the public surface), ADR-0059 (auth for writes/admin), ADR-0013 (jurisdiction scoping for authed users)
- Public reads may run guarded on-demand queries against the raw IHR event table (not just pre-computed aggregates); the guards + rate limiting bound cost/scraping. Sensitive reads (user/key management, audit, settings) stay authenticated. A dedicated high-volume bulk export, if ever needed, can be a separate authenticated path.
