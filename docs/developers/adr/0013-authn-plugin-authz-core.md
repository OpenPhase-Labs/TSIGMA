# Auth: authentication is a plugin; authorization (two-role + jurisdiction) is in the core

- **Status**: Accepted
- **Date**: 2026-06-28
- **Deciders**: jsloan@celldara.com

## Context and Problem Statement

Agencies authenticate differently — local accounts, OIDC, OAuth2, agency SSO — and
one install may need more than one at once (e.g. SSO plus a local break-glass
account). Authorization — what an authenticated user may do against TSIGMA
resources — must be consistent regardless of how the user logged in.

## Decision Drivers

- Identity providers are agency-specific; the core can't ship the one right IDP.
- An install may need more than one provider at once (SSO + local break-glass).
- Access decisions about core resources must be made by the core, never by a plugin.
- The core needs a stable user entity that survives IDP changes.
- Authorization for a single-tenant DOT install (ADR-0006) is simple — small ops teams.

## Considered Options

- Authn = plugin, authz = core (stable `user` + `user_identity` mapping)
- Both in the core (fixed built-in IDP set)
- Both as plugins (plugins decide access)
- Both external (Keycloak / Authelia)

## Decision Outcome

**Authentication is a plugin domain; authorization is in the core.** The core keeps
a stable `user` entity; auth plugins (local, OIDC, OAuth2, agency SSO) map IDP
claims to core users via a `user_identity` join — one user, possibly several
identities — and more than one provider may be enabled at once.

Authorization lives wholly in the core and is identical regardless of which plugin
authenticated the user. TSIGMA's authz model is deliberately simple (not
NetBox-style object permissions): **two roles — admin and viewer — plus
jurisdiction scoping** (viewers see their assigned jurisdiction(s); admins see
all), enforced at the query layer. No plugin ever makes an access decision about
core resources.

### Consequences

- Agencies pick IDPs without forking; one user can log in via SSO or local break-glass.
- A permission means the same thing regardless of IDP.
- The core never delegates access decisions to outside code (a security property).
- Auth plugins handle IDP-specific concerns (claim/group parsing) without core coupling.
- The core must define a stable claims→user mapping contract for auth plugins.

### Confirmation

The `user` table has no IDP-specific fields; a `user_identity` table joins claims
to users; auth plugins load via the plugin host; no plugin contract exposes an
"isAllowed" hook; authz is admin/viewer + jurisdiction enforced in core queries.

## Pros and Cons of the Options

### Authn = plugin, authz = core (chosen)

- Good, because it matches the org-boundary heuristic, makes multi-IDP and multi-identity natural, and keeps access decisions inside the trust boundary.
- Bad, because it needs a well-defined claims-mapping contract up front.

### Both in the core

- Bad, because a new IDP means a core release, the core balloons, and closed agency SSO can't be added without a fork.

### Both as plugins

- Bad, because access decisions move outside the core — a security regression.

### Both external

- Bad, because it is operationally heavy and external authz can't scope core resources (jurisdiction).

## More Information

- ADR-0006 (single-tenant; jurisdiction scoping within the install)
- Forthcoming: gRPC plugin contract (the auth-plugin host)
