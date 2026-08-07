# External-IdP just-in-time provisioning, never auto-downgrade

- **Status**: Accepted
- **Date**: 2026-06-28
- **Deciders**: jsloan@celldara.com

## Context and Problem Statement

With external IdPs (OIDC/OAuth2) as auth plugins (ADR-0013), a user may authenticate
who has no local account yet. Do we pre-create accounts, or create them on first
login — and what happens to roles on subsequent logins?

## Decision Drivers

- Pre-creating accounts for every IdP user is impractical.
- First-login users need an account and a sensible default role.
- IdP-driven role mapping must not accidentally strip a deliberately-elevated admin.

## Considered Options

- Just-in-time provisioning on first login + never auto-downgrade
- Pre-provision all accounts
- No provisioning (manual account creation only)

## Decision Outcome

**Users from an external IdP are provisioned just-in-time on first successful
login** — a local `user` + `user_identity` (ADR-0013) is created, with a default
role (viewer) or a configured IdP-group → role mapping. On subsequent logins, role
mapping may **raise** privileges but **never automatically downgrades an existing
admin** (a deliberate elevation isn't silently revoked by an IdP claim change);
de-elevation is an explicit admin action.

### Consequences

- IdP users get accounts without manual pre-provisioning.
- New users land as viewers unless mapped higher (safe default).
- A flaky or changed IdP claim can't silently strip an admin's access.
- Removing an admin is a deliberate action, not an IdP side effect (operators must de-elevate explicitly).

### Confirmation

First IdP login creates `user` + `user_identity` with a default/mapped role;
subsequent logins may elevate but never auto-downgrade an admin; de-elevation is
explicit.

## Pros and Cons of the Options

### JIT + never-downgrade (chosen)

- Good, because it avoids manual pre-provisioning, uses safe defaults, and keeps admin access stable.
- Bad, because revoking admin needs an explicit de-elevation step.

### Pre-provision all accounts

- Bad, because it's impractical and leaves stale accounts.

### Manual only

- Bad, because every IdP user needs hand-creation — defeating SSO convenience.

## More Information

- ADR-0013 (authn plugins, user/user_identity, two-role authz), ADR-0059 (sessions after login)
