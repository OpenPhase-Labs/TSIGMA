# Column and encoding conventions: TEXT + app validation, INTEGER event codes, UTF-8

- **Status**: Accepted
- **Date**: 2026-06-28
- **Deciders**: jsloan@celldara.com

## Context and Problem Statement

A handful of low-level data-shape conventions recur across the schema. Recording
them once keeps the schema consistent and the rationale durable.

## Decision Drivers

- String length/format rules change more often than schema migrations are convenient.
- Application-layer (Pydantic) validation gives clearer errors than database length errors.
- Real-world ATSPM controllers emit event codes/params beyond the Indiana spec's small range.
- One universal text encoding avoids mojibake.

## Considered Options

- The conventions below (TEXT + app validation; INTEGER codes; UTF-8 default)
- `VARCHAR(n)` + DB constraints; `SMALLINT` codes; per-deployment encoding

## Decision Outcome

Three schema conventions:

- **Strings are `TEXT`, validated in the application** (Pydantic), not `VARCHAR(n)`.
  Length/format rules live in models, not DB constraints — faster writes, better
  errors, changeable without a migration.
- **`INTEGER` for `event_code` / `event_param`**, not `SMALLINT` — real deployed
  ATSPM controllers emit values beyond the Indiana spec's small range; matching
  reality beats matching the spec.
- **UTF-8 is the default database encoding** (recommended; LATIN1 optional for
  strict byte-size needs, but immutable once set).

### Consequences

- String rules evolve without migrations; validation errors are clearer.
- The event table tolerates real-world out-of-spec codes without data loss.
- UTF-8 handles all text with no penalty for ASCII; encoding is fixed at create time.
- Application-layer validation must be applied consistently — it's the only length guard.

### Confirmation

String columns are `TEXT` with Pydantic validators; `event_code`/`event_param` are
`INTEGER`; databases are created UTF-8 unless an operator deliberately chooses
otherwise.

## Pros and Cons of the Options

### Chosen conventions

- Good, because string rules stay flexible, codes tolerate real-world data, and the encoding is universal.
- Bad, because there's no DB-level string-length guard (it relies on app validation).

### VARCHAR(n) + SMALLINT + per-deployment encoding

- Bad, because of migration churn on rule changes, data loss on out-of-spec codes, and mojibake risk.

## More Information

- ADR-0024 (models / Pydantic validation), ADR-0009 (canonical event model), ADR-0023 (facade/dialect)
- ADR-0078: identity keys such as `signal_id` are integer device ids (`BIGINT`); the "strings are TEXT" rule here governs names and free-text, not identifiers.
