# Layered configuration: bootstrap settings (env/file) + runtime registry (DB)

- **Status**: Accepted
- **Date**: 2026-06-28
- **Deciders**: jsloan@celldara.com

## Context and Problem Statement

TSIGMA config has two natures: static infrastructure that must be known before the
database is even reachable (DB connection, broker URLs, secret-key source, ports),
and operational knobs operators want to tune at runtime without a restart
(thresholds, retention ages, feature toggles). Where does each live, and how do
they layer?

## Decision Drivers

- Some config is needed before the DB is available (bootstrap) — it can't live in the DB.
- Operators want to tune runtime knobs from the admin UI without shell access, file edits, SQL, or restarts.
- Sensitive bootstrap values (secret-key source, DB credentials) belong in env/secret stores, not a UI.
- Deploy-time overrides must be possible for debugging.

## Considered Options

- Two layers: bootstrap settings (env/file) + a DB-backed runtime registry
- All config in env/files
- All config in the DB

## Decision Outcome

**Two configuration layers:**

- **Bootstrap settings** — typed (Pydantic) settings loaded once at startup from OS
  env (and an optional `.env` file): static infrastructure and secrets (DB
  connection, broker URLs, secret-key source, ports, storage backend). Not
  runtime-tunable; changing them is a restart.
- **Runtime registry** — a DB-backed, typed settings registry (ADR-0051),
  UI-editable at runtime without a restart: operational knobs (thresholds,
  retention ages, tier toggles, rate limits).

For a **runtime** key, precedence is **`TSIGMA_<KEY>` OS env var > DB row >
registered default** — the env override exists for deploy-time/debug control, the
DB row is the operator-tuned value, the default is the fallback. Bootstrap settings
come only from env/file.

### Consequences

- Pre-DB infrastructure config is always available; runtime knobs are tunable from the UI without a restart.
- Secrets stay in env/secret stores, never the UI.
- A debug/deploy env override can pin a runtime key above the DB value.
- Operators must know which knobs are bootstrap (restart) vs runtime (live) — surfaced in the admin UI/docs.

### Confirmation

Bootstrap settings load from env/file at startup; runtime keys resolve env > DB >
default; sensitive infra/secrets are bootstrap-only; runtime knobs are UI-editable
without restart.

## Pros and Cons of the Options

### Two layers (chosen)

- Good, because pre-DB config works, operators get live tuning, and secrets stay out of the UI.
- Bad, because there are two systems and the bootstrap-vs-runtime split must be clear to operators.

### All in env/files

- Bad, because every tuning change is a redeploy/restart and there's no admin-UI tuning.

### All in the DB

- Bad, because you can't configure how to reach the DB before the DB is reachable.

## More Information

- ADR-0051 (the runtime registry + cache invalidation), ADR-0023 (registry stored via the facade); secret-key sourcing in the forthcoming security ADRs
