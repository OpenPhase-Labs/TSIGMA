# CSRF protection: one-time nonce for cookie/form auth

- **Status**: Accepted
- **Date**: 2026-06-28
- **Deciders**: jsloan@celldara.com

## Context and Problem Statement

Cookie-based browser sessions (ADR-0059) are vulnerable to cross-site request
forgery on state-changing requests. API-key and Bearer clients are not (they don't
ride on ambient cookies). How is CSRF handled?

## Decision Drivers

- Cookie-authenticated state-changing requests need CSRF protection.
- API-key / Bearer requests don't (no ambient credential to forge).
- The mechanism should be simple for the client.

## Considered Options

- One-time CSRF nonce (short TTL) for cookie/form auth
- SameSite cookies only
- No CSRF protection

## Decision Outcome

**A one-time CSRF nonce with a short TTL (~5 min) guards state-changing requests
made with cookie/form auth.** The server issues a nonce; the client returns it on
the next mutating request; it's single-use and expires. Requests authenticated by
API key or Bearer token are exempt (no ambient cookie to forge). SameSite cookies
are used as defense-in-depth, not the sole control.

### Consequences

- Cookie-session mutations are protected against CSRF.
- Machine / external clients (API key / Bearer) aren't burdened with CSRF.
- The client must fetch/echo a nonce per mutation (short-lived).
- One-time + TTL limits replay.

### Confirmation

Cookie/form mutations require a valid one-time, unexpired CSRF nonce; API-key/Bearer
requests are exempt; SameSite cookies are set as defense-in-depth.

## Pros and Cons of the Options

### One-time nonce (chosen)

- Good, because it gives strong CSRF protection scoped to cookie auth and limits replay.
- Bad, because clients must handle a nonce round-trip.

### SameSite cookies only

- Bad, because it's browser-dependent and weaker on its own for a security-sensitive app.

### No CSRF protection

- Bad, because cookie sessions are forgeable on mutations.

## More Information

- ADR-0059 (cookie sessions this protects), ADR-0064 (security headers incl. SameSite/CORS)
