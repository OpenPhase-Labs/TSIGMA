# Rate limiting: login, read, and write categories

- **Status**: Accepted
- **Date**: 2026-06-28
- **Deciders**: jsloan@celldara.com

## Context and Problem Statement

The API (public reads + authenticated writes) must resist brute-force login,
scraping of the public read surface, and write abuse. One global limit fits none of
these well. How is rate limiting applied?

## Decision Drivers

- Login endpoints need strict limits (brute-force defense).
- Public reads need lenient-but-bounded limits (the UI makes many; protect against scraping).
- Writes need moderate limits.
- Limits should be tunable per deployment.

## Considered Options

- Three categories (login / read / write) with category-specific limits
- One global limit
- No rate limiting

## Decision Outcome

**Rate limiting in three categories, each independently tunable:**

- **Login** — strict (per-IP / per-account), to blunt brute-force and credential-stuffing.
- **Read** — lenient but bounded (the UI and public consumers make many reads; this caps scraping of the public surface).
- **Write** — moderate.

Limits are runtime settings (ADR-0051). They apply **alongside** the query guards
(ADR-0057): guards bound a single query's cost; rate limits bound request frequency.

### Consequences

- Brute-force, scraping, and write-abuse are each bounded by an appropriate limit.
- Operators tune limits to their capacity/exposure at runtime.
- The public read surface (ADR-0058) is protected by read limits + query guards together.
- Legitimate high-frequency clients may need their limits raised (tunable).

### Confirmation

Login/read/write each have their own limit; limits are runtime-tunable; login is the
strictest; limits apply on top of query guards.

## Pros and Cons of the Options

### Three categories (chosen)

- Good, because each risk gets the right limit, the limits are tunable, and they complement the guards.
- Bad, because there are a few categories to configure.

### One global limit

- Bad, because it's too strict for reads or too loose for login.

### No rate limiting

- Bad, because brute-force, scraping, and abuse go unbounded.

## More Information

- ADR-0058 (public read surface protected by these), ADR-0057 (per-query guards), ADR-0051 (runtime settings), ADR-0059 (login)
