# REST conventions: RFC-7807 errors, cursor pagination, filter/sort/search

- **Status**: Accepted
- **Date**: 2026-06-28
- **Deciders**: jsloan@celldara.com

## Context and Problem Statement

REST endpoints (ADR-0055) need consistent error reporting, pagination over large
result sets (events), and query shaping. What conventions apply across the REST API?

## Decision Drivers

- Errors must be machine-parseable and consistent across endpoints.
- Event result sets are large/unbounded — offset pagination is fragile under concurrent writes.
- Consumers need to filter, sort, and search list endpoints predictably and safely.

## Considered Options

- RFC-7807 problem+json errors; opaque cursor pagination (+ Link headers); allowlisted filter/sort/search
- Ad-hoc per-endpoint conventions
- Offset/limit pagination

## Decision Outcome

**Three conventions across the REST API:**

- **Errors** — RFC-7807 `application/problem+json` (`type`, `title`, `status`,
  `detail`), consistent across endpoints.
- **Pagination** — **opaque cursor** pagination (`{items, next_cursor}`; the cursor
  encodes the last row's stable key), with **Link headers**, and page size clamped
  to a max. Cursors are stable under concurrent inserts (unlike offset).
- **Filter / sort / search** — standard query parameters on list endpoints,
  validated against an **allowlist** of fields.

### Consequences

- Clients parse one error shape everywhere.
- Pagination is correct under concurrent writes and bounded in size.
- List querying is predictable and injection-safe (allowlisted fields).
- Cursors are opaque — clients must treat them as tokens, not parse them.

### Confirmation

Errors are `problem+json`; list endpoints return `{items, next_cursor}` + Link
headers with a clamped page size; filter/sort/search fields are allowlisted.

## Pros and Cons of the Options

### RFC-7807 + cursor + allowlisted filter/sort (chosen)

- Good, because it's consistent, correct under concurrent writes, and injection-safe.
- Bad, because opaque cursors are less human-pokeable than offsets.

### Ad-hoc per-endpoint conventions

- Bad, because they're inconsistent and force per-endpoint client special-casing.

### Offset/limit pagination

- Bad, because it skips/dupes rows under concurrent inserts and is slow on deep pages.

## More Information

- ADR-0055 (REST surface), ADR-0057 (guards), ADR-0031 (cursor over hot/cold event rows)
