# Runtime Settings

> Operator reference for TSIGMA's runtime-settings registry.
> Architectural details live in [ARCHITECTURE.md §13](../developers/ARCHITECTURE.md#13-configuration-management).

---

## Overview

TSIGMA exposes a typed key-value runtime-settings registry backed by the
`identity.system_setting` table. Nine admin-tunable "tuning knob" keys
cover cold-tier control, cold-tier query routing, and API limits. Settings persist across restarts (rows live in the
database), can be changed at runtime through the admin API without a
redeploy, and can be overridden per-deployment with environment
variables.

Behind the scenes:

- Reads go through type-coerced async getters (`get_int`, `get_bool`,
  `get_str`, `get_float`) on the application's database session.
- Writes go through `tsigma.settings_service.set()`, which validates
  type and bounds, UPSERTs the row, and appends a `system_setting_audit`
  row in the same transaction.
- A 30-second in-process TTL cache fronts the table.
- When Valkey is configured, every write publishes the changed key on
  the `tsigma:system_setting:invalidate` channel so peer replicas drop
  their local caches immediately.

---

## Registry of known keys

The following nine keys are registered at module load in
`tsigma.settings_service`. The registry is the source of truth for
which keys exist; the database carries values. Unknown keys raise
immediately.

| Key                                 | Type | Default | Min | Max   | Category   | Description                                                                          |
|-------------------------------------|------|---------|-----|-------|------------|--------------------------------------------------------------------------------------|
| `storage.cold_enabled`              | bool | `false` | —   | —     | cold_tier  | Master switch for cold-tier export. Replaces `TSIGMA_STORAGE_COLD_ENABLED` env var.  |
| `storage.cold_after_days`           | int  | `180`   | 1   | 36500 | cold_tier  | Age threshold (days) for archival. Replaces `TSIGMA_STORAGE_COLD_AFTER`.             |
| `storage.cold_delete_after_export`  | bool | `true`  | —   | —     | cold_tier  | Delete archived rows from hot DB after verified Parquet write.                       |
| `cold_tier.query_enabled`           | bool | `true`  | —   | —     | cold_tier  | Route queries past threshold to cold tier; admins can disable to force hot-only.     |
| `cold_tier.threshold_days`          | int  | `180`   | 1   | 36500 | cold_tier  | Events older than this many days are read from the cold tier.                        |
| `api.max_page_size`                 | int  | `1000`  | 1   | 100000| api        | Event-list endpoint per-page cap.                                                    |
| `api.max_aggregation_days`          | int  | `92`    | 1   | 36500 | api        | Aggregation endpoint date-range cap.                                                 |
| `api.max_signals_per_request`       | int  | `100`   | 1   | 100000| api        | Aggregation endpoint per-request signal count cap.                                   |
| `api.max_lookback_days`             | int  | `92`    | 1   | 36500 | api        | Absolute oldest data an API request can ask for.                                     |

The `cold_tier.*` keys (query routing) are not yet wired to a query
layer; the registry rows ship now so operators can pre-stage values.

---

## Reading values

All settings endpoints require an authenticated admin session.

### List all settings

```
GET /api/v1/settings/
```

Returns every row in `system_setting`, including category, description,
editable flag, and `updated_at` / `updated_by`. Optional
`?category=<name>` filters by category (e.g. `cold_tier`, `api`,
`access_policy`).

There is no per-key GET endpoint at this time — use the list endpoint
and filter client-side, or use the audit endpoint to inspect a single
key's history.

### Get current access-policy snapshot

```
GET /api/v1/settings/access-policy
```

Returns a hand-enumerated subset of the `access_policy.*` fields as a
single object — not every seeded row. This is a convenience endpoint for
the access-control middleware UI and does not cover the typed tuning
knobs listed above. For the complete set, use
`GET /api/v1/settings/?category=access_policy`.

### Get the audit trail for a single key

```
GET /api/v1/settings/{key}/audit?skip=0&limit=50
```

Returns the most recent N audit rows for `key`, newest first. See
[Audit log](#audit-log) below.

---

## Changing values

```
PUT /api/v1/settings/{key}
Content-Type: application/json

{
  "value": "1500",
  "reason": "raising page size for bulk-export team"
}
```

- `value` is always sent as a string. The server coerces to the
  registered type (bool, int, float, or str). Bools accept
  `true`/`false`/`1`/`0`/`yes`/`no` (case-insensitive).
- `reason` is optional free text. When provided, it is stored on the
  audit row and is visible in `GET /api/v1/settings/{key}/audit`.
- Successful response: `200 OK` with the updated `SettingResponse`.
- A type-coercion failure or bounds violation returns `422`.
- A request against a key with `editable=false` returns `403`.
- A request against a key that does not exist in `system_setting`
  returns `404`. (Note: the nine registry keys are seeded on first
  boot — `404` here means an unknown key, not a default-valued key.)

---

## Env-var overrides

Every registered key derives an environment-variable name by the rule:

```
TSIGMA_<KEY>            # dots → underscores, uppercased
```

Examples:

| Registry key                      | Env var                                |
|-----------------------------------|----------------------------------------|
| `storage.cold_enabled`            | `TSIGMA_STORAGE_COLD_ENABLED`          |
| `storage.cold_after_days`         | `TSIGMA_STORAGE_COLD_AFTER_DAYS`       |
| `storage.cold_delete_after_export`| `TSIGMA_STORAGE_COLD_DELETE_AFTER_EXPORT` |
| `cold_tier.query_enabled`         | `TSIGMA_COLD_TIER_QUERY_ENABLED`       |
| `cold_tier.threshold_days`        | `TSIGMA_COLD_TIER_THRESHOLD_DAYS`      |
| `api.max_page_size`               | `TSIGMA_API_MAX_PAGE_SIZE`             |
| `api.max_aggregation_days`        | `TSIGMA_API_MAX_AGGREGATION_DAYS`      |
| `api.max_signals_per_request`     | `TSIGMA_API_MAX_SIGNALS_PER_REQUEST`   |
| `api.max_lookback_days`           | `TSIGMA_API_MAX_LOOKBACK_DAYS`         |

Resolution order, highest precedence first:

1. The matching `TSIGMA_<KEY>` env var, if set;
2. The cached DB row;
3. The registered default.

When an env var is set, the DB row is **ignored** for reads — admin UI
changes will succeed and write audit rows, but the running process will
still observe the env-var value. Document this in operator runbooks so
config drift is not mistaken for a bug.

Env-var values are parsed with the same type coercion as DB values, so
`TSIGMA_STORAGE_COLD_ENABLED=true` works, `TSIGMA_API_MAX_PAGE_SIZE=500`
works, and a malformed value (e.g. `TSIGMA_API_MAX_PAGE_SIZE=many`)
raises at read time.

---

## Audit log

Every successful `PUT` writes one row to `identity.system_setting_audit`
in the same transaction as the UPSERT. The audit row carries:

- `id` (bigserial primary key);
- `key` — the registry key written;
- `old_value` — prior value as text, or `NULL` for a first-time write;
- `new_value` — new value as text;
- `changed_at` — server-side `now()`, timezone-aware;
- `changed_by` — the admin username from the session;
- `reason` — the operator-supplied reason, or `NULL`.

A composite index `idx_system_setting_audit_key (key, changed_at DESC)`
keeps per-key history queries fast.

Retrieve recent changes with:

```
GET /api/v1/settings/{key}/audit?limit=50
```

### Retention

No automatic retention. The audit table grows indefinitely by design —
runtime-setting changes are rare and the rows are small. Operators who
want to trim old history may run a manual `DELETE FROM
identity.system_setting_audit WHERE changed_at < now() - INTERVAL '1
year'` (or equivalent on non-PostgreSQL backends). Document any local
trim policy alongside the deployment runbook.

---

## Cross-replica propagation

Multi-replica deployments need invalidation across processes — an admin
PUT against one replica must not leave the other replicas serving stale
cached values for up to 30 seconds. TSIGMA uses Valkey pub/sub on the
channel `tsigma:system_setting:invalidate` to broadcast invalidations.

Publication is **dual-gated**:

1. The `valkey_settings_invalidation_enabled` config flag must be
   `true` (default: `true`); AND
2. `valkey_url` must be non-empty.

If either gate is closed, the publish path is short-circuited before any
Valkey client is constructed — single-instance deployments and test
suites pay no connection cost. With the gate closed, the 30-second TTL
cache is the only invalidation mechanism — acceptable for single-replica
deployments, not for multi-replica.

To opt out without unsetting `valkey_url` (useful in test harnesses or
isolated single-replica deployments that share a Valkey instance for
session storage), set:

```
TSIGMA_VALKEY_SETTINGS_INVALIDATION_ENABLED=false
```

Subscribers run as a lifespan-scoped background task in `tsigma.app`
that reuses the existing process-wide Valkey client; the task is
cancelled before `valkey_client.aclose()` on shutdown.

---

## Operator migration notes (hard cutover)

One env var was renamed with a **hard cutover** — no alias parser, no
deprecation warning. Audit your deployment artifacts:

| Old (silently inert)                    | New                                       |
|-----------------------------------------|-------------------------------------------|
| `TSIGMA_STORAGE_COLD_AFTER="6 months"`  | `TSIGMA_STORAGE_COLD_AFTER_DAYS=180`      |

`TSIGMA_STORAGE_COLD_ENABLED` is **unchanged** — its byte representation
maps directly onto the new registry key `storage.cold_enabled`.

`TSIGMA_STORAGE_COLD_PATH` remains a `tsigma/config.py` Pydantic
setting, not a runtime-registry key — the cold-tier filesystem path is
fixed at deployment time.

If `TSIGMA_STORAGE_COLD_AFTER` is still set in your environment, it is
silently ignored. The cold-export job uses the registry value
(`storage.cold_after_days`, default 180) instead. Rename the env var
to `TSIGMA_STORAGE_COLD_AFTER_DAYS=180` (or your preferred integer
day count) before the next deploy.

---

## Related documents

- [ARCHITECTURE.md §13 Configuration Management](../developers/ARCHITECTURE.md#13-configuration-management)
- [DATABASE.md](../developers/DATABASE.md) — `identity.system_setting`
  and `identity.system_setting_audit` table definitions
- [STORAGE.md](../developers/STORAGE.md) — cold-tier configuration
- [DEPLOYMENT.md](../users/DEPLOYMENT.md) — env-var reference and
  docker-compose examples
