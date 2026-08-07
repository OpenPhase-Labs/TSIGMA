# API surfaces: versioned REST (OpenAPI) + GraphQL read surface

- **Status**: Accepted
- **Date**: 2026-06-28
- **Deciders**: jsloan@celldara.com

## Context and Problem Statement

TSIGMA exposes signal config and event/analytics data to operators, dashboards,
and third-party / analytics consumers. Consumers vary: some want simple resource
endpoints, some want flexible shaped queries, some want machine-readable API docs.
What HTTP API surfaces does TSIGMA offer?

## Decision Drivers

- Operators, dashboards, and integrations need straightforward resource access (REST).
- Analytics consumers benefit from flexible, shaped queries (GraphQL).
- Machine-readable, always-current API docs ease integration and tooling.
- API evolution must not silently break consumers (versioning).

## Considered Options

- REST (versioned, OpenAPI) + GraphQL read surface
- REST only
- GraphQL only

## Decision Outcome

**Two surfaces:**

- **REST `/api/v1`** — the primary surface. URL-versioned (`/api/v1`), with
  **content negotiation** (JSON default; CSV / XML for exports). API docs are
  **OpenAPI / Swagger, generated code-first** from the route/model definitions
  (always in sync; Swagger UI served).
- **GraphQL** — a **read-only** query surface for flexible/shaped reads (e.g.
  events) by analytics consumers. Writes/mutations go through REST.

Both read through the same core / tier-aware SDK (ADR-0031); GraphQL is additive,
not a replacement.

### Consequences

- Simple consumers use REST; analytics consumers get flexible GraphQL reads; integrators get accurate OpenAPI.
- URL versioning lets the API evolve without silently breaking `/api/v1` consumers.
- Two read surfaces to secure and keep behavior-consistent (the shared SDK underneath helps).
- Code-first OpenAPI means the docs can't drift from the implementation.

### Confirmation

REST is `/api/v1` with content negotiation; OpenAPI/Swagger is generated from code;
GraphQL is read-only (no mutations); both go through the same query/SDK layer.

## Pros and Cons of the Options

### REST + OpenAPI + GraphQL read (chosen)

- Good, because it serves both simple and flexible consumers, gives accurate auto-docs, and versions cleanly.
- Bad, because there are two read surfaces to secure and maintain.

### REST only

- Bad, because there's no flexible shaped-query surface for analytics consumers.

### GraphQL only

- Bad, because it's heavier for simple resource access, has weaker caching/tooling for the common case, and lacks simple export endpoints.

## More Information

- ADR-0056 (REST conventions), ADR-0057 (query guards), ADR-0031 (tier-aware SDK both surfaces use), ADR-0013 (auth)
