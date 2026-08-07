# Request authentication: server-side sessions (no JWT) + credential precedence

- **Status**: Accepted
- **Date**: 2026-06-28
- **Deciders**: jsloan@celldara.com

## Context and Problem Statement

A request can carry credentials three ways: an API key (machine clients), a Bearer
token (external IdP), or a session cookie (browser). How does a user authenticate
to TSIGMA, and which credential wins when more than one is present?

## Decision Drivers

- Browser sessions must be revocable instantly (logout, compromise) — server-side state, not self-contained tokens.
- Machine clients need long-lived credentials (API keys).
- External-IdP-issued Bearer tokens must be accepted.
- A deterministic order is needed when multiple credentials are present.

## Considered Options

- Server-side opaque sessions (no JWT) + precedence API key > Bearer > cookie
- Stateless JWT sessions
- A single credential type only

## Decision Outcome

**Server-side opaque sessions; JWT is not used for sessions.** A login creates a
server-side session (record in DB/Valkey) and sets an **httpOnly** cookie holding
an opaque token; the server can revoke it instantly. A self-contained JWT can't be
revoked before expiry, so it's rejected for sessions.

**Credential precedence per request: API key > Bearer token > session cookie.** The
first present wins; authorization (two-role + jurisdiction, ADR-0013) is identical
regardless of which credential authenticated.

### Consequences

- Sessions are instantly revocable (logout/compromise) — the server holds the state.
- Machine (API key), external-IdP (Bearer), and browser (cookie) clients all work, with a deterministic resolution order.
- Session lookups hit the session store (DB/Valkey) — mitigated by caching/Valkey.
- httpOnly cookies resist XSS token theft.

### Confirmation

Sessions are server-side opaque tokens in httpOnly cookies (no JWT); a request
resolves credentials API key > Bearer > cookie; logout/revocation invalidates the
session immediately.

## Pros and Cons of the Options

### Server-side sessions + precedence (chosen)

- Good, because sessions revoke instantly, all three client types are supported, and resolution is deterministic.
- Bad, because it needs server-side session-store lookups (cacheable).

### Stateless JWT sessions

- Bad, because they can't be revoked before expiry, logout is awkward, and key rotation is painful.

### Single credential type

- Bad, because it can't serve browsers, machines, and external IdPs together.

## More Information

- ADR-0013 (authn plugins / two-role authz), ADR-0060 (API keys), ADR-0061 (CSRF for cookie auth), ADR-0012 (session store in DB/Valkey)
