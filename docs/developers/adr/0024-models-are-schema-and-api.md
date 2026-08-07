# Models are the schema and the API contract (no DTO layer)

- **Status**: Accepted
- **Date**: 2026-06-28
- **Deciders**: jsloan@celldara.com

## Context and Problem Statement

A data layer can keep separate types for database rows (ORM models) and API
payloads (DTOs/schemas), with mapping between them. That mapping is boilerplate and
a source of drift. How does TSIGMA shape its models?

## Decision Drivers

- A single source of truth avoids model/DTO drift and mapping boilerplate.
- SQLAlchemy 2.0 + Pydantic validators can serve both persistence and API validation.
- Validation should run on write regardless of entry path.

## Considered Options

- Models are the schema AND the API contract (no DTO)
- Separate ORM models + API DTOs with explicit mapping

## Decision Outcome

**SQLAlchemy 2.0 models (with Pydantic validators) are the single definition** —
they are the database schema, the API request/response shape, and the type-safe
dataclass. No separate DTO layer; routes use models directly; validators run on
write. Where API exposure must differ from storage (e.g. sensitive fields), it is
handled explicitly (response redaction), not via a parallel DTO hierarchy.

### Consequences

- One source of truth; model changes propagate to schema and API automatically; no mapping boilerplate.
- Validation is consistent across entry paths.
- Divergent API-vs-storage shapes need explicit handling (redaction), not a separate schema layer.

### Confirmation

No DTO/mapping layer exists; routes accept/return models; Pydantic validators
enforce constraints on write; sensitive-field exposure is handled by explicit
redaction.

## Pros and Cons of the Options

### Models = schema + API (chosen)

- Good, because it gives a single source of truth, no mapping boilerplate, and consistent validation.
- Bad, because divergent API-vs-storage shapes need explicit handling (redaction).

### Separate models + DTOs

- Good, because of clean separation when API and storage diverge heavily.
- Bad, because of constant mapping boilerplate and drift risk for a schema that mostly mirrors storage.

## More Information

- ADR-0023 (the facade these models flow through), ADR-0010 (typed columns + JSONB)
- Sensitive-field redaction handled in the API layer (forthcoming security ADRs)
