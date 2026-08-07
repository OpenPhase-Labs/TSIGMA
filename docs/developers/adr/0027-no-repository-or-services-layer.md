# No repository or generic services layer

- **Status**: Accepted
- **Date**: 2026-06-28
- **Deciders**: jsloan@celldara.com

## Context and Problem Statement

A data layer can interpose a repository layer (data-access objects) and a generic
services layer between routes and the database. With the database facade (ADR-0023)
and SQLAlchemy 2.0 already providing query abstraction, do we add those layers?

## Decision Drivers

- The facade (ADR-0023) + ORM already abstract connection/dialect/queries.
- Extra layers add indirection without decoupling benefit when logic varies by subsystem.
- Reports have a dedicated SDK for data access (forthcoming ADR).
- Keep the call path short and legible for a small team.

## Considered Options

- No repository/services layer: routes use the facade/ORM directly; reports use the SDK; complex logic lives in domain-specific modules
- A generic repository (DAO) layer between routes and the DB
- A generic services layer wrapping business logic

## Decision Outcome

**No generic repository layer and no generic services layer.** Routes execute
queries directly via the injected facade/ORM session; the report SDK is the
data-access path for reports; complex or shared logic lives in dedicated,
domain-specific modules (collector, validation, scheduler, …), not a one-size
repository or services tier.

### Consequences

- The call path is short: route → facade/ORM, or route → domain module → facade.
- No mapping boilerplate or premature abstraction; logic sits where it's used.
- Reports never touch the facade directly — the SDK is their boundary.
- Genuinely shared query logic must be deliberately extracted to a domain module / SDK helper (not a generic repository), which takes discipline.

### Confirmation

No generic repository/DAO or services package exists; routes use the facade/ORM or
a domain module; reports use only the SDK; review rejects a new generic
data-access tier.

## Pros and Cons of the Options

### No repository/services layer (chosen)

- Good, because the path is short and legible, there's no premature abstraction, and logic is co-located with its use.
- Bad, because shared logic must be deliberately extracted to domain modules/SDK.

### Generic repository (DAO)

- Good, because it's familiar and gives a single data-access seam.
- Bad, because it's indirection without decoupling when logic varies per subsystem — the ORM already abstracts queries.

### Generic services layer

- Bad, because it's a catch-all tier that blurs responsibility; domain-specific modules are clearer.

## More Information

- ADR-0023 (facade), ADR-0024 (models as schema/API); report SDK (forthcoming)
- Note: an older "repository pattern for complex queries" line in `CODING_GUIDELINES.md` is outdated and will be reconciled to this decision.
