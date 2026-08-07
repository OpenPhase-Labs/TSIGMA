# API keys: prefixed, hashed at rest, optional expiry

- **Status**: Accepted
- **Date**: 2026-06-28
- **Deciders**: jsloan@celldara.com

## Context and Problem Statement

Machine clients (integrations, scripts, plugins calling the API) need long-lived
credentials that are safe to store and easy to identify and revoke. How are API
keys issued and stored?

## Decision Drivers

- A leaked key store must not expose usable keys (hash at rest).
- Keys should be identifiable in logs/leaks (prefix) for fast recognition/revocation.
- Some keys should expire; others are long-lived.
- Keys are shown once, at creation.

## Considered Options

- Prefixed, bcrypt-hashed, optional-expiry keys
- Plaintext keys in the database
- Reuse session tokens for machines

## Decision Outcome

**API keys are issued with a recognizable prefix (`tsgm_`), stored only as a bcrypt
hash** (the plaintext is shown once at creation and never stored), and carry an
**optional expiry**. A key authenticates at the top of the precedence order
(ADR-0059). Revoking deletes/disables the row. The prefix makes keys greppable in
logs and leak scanners.

### Consequences

- A leaked database yields no usable keys (only bcrypt hashes).
- Keys are recognizable (prefix) for leak-scanning and operator identification.
- Expiry supports rotation; long-lived keys remain possible.
- The plaintext is unrecoverable after creation (lost key ⇒ reissue).

### Confirmation

Keys carry the `tsgm_` prefix; only bcrypt hashes are stored; plaintext is shown
once; optional expiry is enforced; revocation is immediate.

## Pros and Cons of the Options

### Prefixed + bcrypt + optional expiry (chosen)

- Good, because keys are safe at rest, greppable, and rotatable.
- Bad, because a lost plaintext means reissue.

### Plaintext keys in the database

- Bad, because a DB leak hands out live credentials.

### Reuse session tokens

- Bad, because sessions are revocable/short-lived by design — the wrong fit for machines.

## More Information

- ADR-0059 (credential precedence), ADR-0062 (secrets encrypted at rest), ADR-0013 (authz applies equally)
