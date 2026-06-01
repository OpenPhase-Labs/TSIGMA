# Configuration Layering — Spec & Plan

> Status: **Draft / proposed.** Phase 0 done; Phases 1–3 not started.
> Owner: TBD. Tracks the work labelled #2 and #3 in the config-layering discussion.

## 0. Guiding goal

Operators must never need a systems engineer — no shell, no `.env` edits, no manual SQL, no
restarts — to adjust a *tunable* setting. Anything an agency would reasonably retune over the
life of a deployment is an **admin-UI** action, and the app performs whatever DB-side work that
change requires (e.g. issuing `set_chunk_time_interval`) on the operator's behalf. The app *is*
the systems engineer for these settings. `.env`/OS-env exists for bootstrap and for
developer/debug overrides — not as the day-to-day tuning surface.

## 1. Problem

TSIGMA has **two** configuration systems that overlap awkwardly:

- **System 1 — `Settings` (pydantic-settings, `tsigma/config.py`).** Read from the
  `.env` *file* + OS environment. Precedence: **OS env > `.env` file > field default**.
  `extra='forbid'`. No DB, no UI.
- **System 2 — runtime-settings registry (`tsigma/settings_service.py`).** DB-backed,
  admin-UI/API editable. Precedence (`_resolve`): **`TSIGMA_<KEY>` env > DB row > default**.

System 2 already implements the desired model (UI/DB is source of truth, env overrides
for debugging). Two problems remain:

1. **Misplaced settings.** Several values live in System 1 but are *re-read by scheduler
   jobs on every run* — i.e. they are already runtime values, just not tunable without a
   restart and not exposed in the UI.
2. **The `.env` file can't carry System 2 overrides.** pydantic parses the `.env` file and
   `extra='forbid'` rejects any key it doesn't own (boot crash); and pydantic never copies
   `.env` contents into `os.environ`, so System 2's `os.environ.get()` never sees them.
   Net: registry overrides work **only as real OS env vars** (compose/k8s/systemd), never
   from a `.env` file. The natural "edit `.env` to override a setting" workflow silently
   does not work for half the settings.

## 2. Governing principle

A setting belongs in **System 1 (`.env`/OS-env, bootstrap)** *only* if it is one of:

1. **Required before the DB is reachable** — needed for the app to start at all
   (DB connection, the secrets to make it), **or**
2. **Fixed at startup, not tunable live** — set once during boot; changing it requires a
   restart regardless.

Secrets are a standing exception: they stay in System 1 even if conceptually "tunable",
because they should never be persisted in the settings DB.

**Everything else — anything re-read / tunable at runtime — belongs in System 2 (registry,
UI-editable), with env as an override.**

A note on **schema parameters.** Truly-immutable structure — the partition key, primary key,
the partitioning column — is fixed at creation, but that is schema, not a config knob. The
partition/chunk *width* is **not** immutable: TimescaleDB's `set_chunk_time_interval()` and
the native-PG partition job both honor a new width for *future* chunks/partitions (existing
ones keep their width — data is never silently re-chunked). Because it is changeable at
runtime it is a **System 2** setting — but with a twist: tuning it is an **applied action**
(it must call the DB to take effect), and the effective value's source of truth is the
database, not a passively-read config field.

Motivation (real case): an agency starts at 7-day chunks and later moves to 1 day as volume
grows — or starts at 1 day and widens to 3–5 days. The change must apply to *subsequent*
chunks/partitions while existing ones keep their width. That is exactly the supported
behavior, and it's why this is a tunable setting rather than a creation-time constant.

## 3. Current-state inventory

| Field | Read where | Verdict |
|---|---|---|
| DB connection, auth/OIDC/S3 secrets, `valkey_url` | boot, pre-DB | ✅ System 1 |
| `enable_api/collector/scheduler`, `api_host/port`, `debug`, `log_format`, `enable_timescaledb`, storage paths/backend | boot, set once | ✅ System 1 |
| `collector_poll_interval`, `collector_max_concurrent`, `sensor_poll_interval` | startup — job registration / semaphore | ✅ System 1 (restart to change) |
| `checkpoint_silent_cycles_threshold`, `checkpoint_future_tolerance_seconds` | each poll cycle (`collection/service.py`) | ⚠️ currently start-fixed; candidate for System 2 |
| **`storage_warm_after`** | compress_chunks job, every 5 min | ❌ **misplaced** → System 2 |
| **`partition_retention_days`**, **`partition_lookahead_days`** | manage_partitions job, each run | ❌ **misplaced** → System 2 |
| `event_log_partition_interval_days` | migration create; native-PG job re-reads it each run *(fragile)* | ✅ **System 2** — tunable; change = applied DB action, future chunks only (see D1) |

## 4. Goals / non-goals

**Goals**
- Operators tune everything tunable from the admin UI alone — no shell, SQL, `.env` edit, or
  restart. The app encapsulates every DB-side side effect a change requires.
- `.env` becomes a usable override surface for *both* systems (env overrides DB, from the file).
- The misplaced runtime knobs become UI-tunable via the registry, with no behavior change
  (the jobs already re-read each run).
- Typo protection for bootstrap keys is preserved.
- A copied `.env.example` always boots — guarded by a regression test.

**Non-goals (for this spec)**
- Reworking the registry's storage/caching/invalidation.
- Moving secrets into the DB.
- Promoting the collector cadences to runtime (they are start-time by design; revisit later).

## 5. Design

### 5.1 #3 — Make `.env` a real override surface (do this first)

Removing a Settings field (#2) would break any deployment that sets the corresponding
`TSIGMA_*` key in a `.env` *file*, so the override surface must be unified **before** #2.

Two paths:

- **3a (simple):** `load_dotenv()` at startup (before settings_service resolves) + switch
  `Settings` to `extra='ignore'`. Cost: lose typo protection on bootstrap keys (a misspelled
  `TSIGMA_PG_HOST` is silently ignored → connects to localhost).
- **3c (recommended):** `load_dotenv()` at startup **+** a `model_validator(mode='before')`
  on `Settings` that drops keys recognized as registered registry keys before validation,
  while genuinely-unknown keys still trip `extra='forbid'`. Result: `.env` can override DB
  settings, *and* bootstrap-key typos are still caught.

  Implementation risk to resolve: `config.py` is imported very early; it must learn the set
  of registry key → env-var names without a circular import on `settings_service`. Options:
  a small constants module listing registry env names, or lazy import inside the validator.

### 5.2 #2 — Migrate misplaced runtime knobs to the registry

Move these from `Settings` to registry keys (env-var names are **preserved** so existing
OS-env overrides keep working — `_env_var_name("storage.warm_after") == "TSIGMA_STORAGE_WARM_AFTER"`):

| Settings field | New registry key | Type | Default | Reader to update |
|---|---|---|---|---|
| `storage_warm_after` | `storage.warm_after` | str (interval) | `"7 days"` | `scheduler/jobs/compress_chunks.py` |
| `partition_retention_days` | `partitioning.retention_days` | int? (nullable) | `null` (off) | `scheduler/jobs/manage_partitions.py` |
| `partition_lookahead_days` | `partitioning.lookahead_days` | int | `7` | `scheduler/jobs/manage_partitions.py` |
| `event_log_partition_interval_days` † | `partitioning.chunk_interval_days` | int | `1` | `manage_partitions.py` + **apply-action** (D1) |

- The jobs already receive an `AsyncSession`; switch `settings.X` → `await get_*("key", session)`.
- `partition_retention_days` is `int | None` (None = off). The registry getters return
  non-optional types today; add `get_int_or_none` (or a nullable entry) — see Decision D4.
- Remove the migrated fields from `Settings`. With #3 in place, leftover `TSIGMA_*` lines in a
  `.env` file resolve through the registry instead of crashing.
- **†** `chunk_interval_days` is special (see D1): a change is an **applied action**, not just
  a stored value. On TimescaleDB the setter must issue `set_chunk_time_interval`; on native PG
  the job picks it up next run. It also seeds the *initial* width at DB creation (migration) —
  the create-time-source sub-decision is open in D1.

### 5.3 Backward compatibility

- OS-env overrides: **unchanged** (same env-var names, now read by the registry).
- `.env`-file overrides: **newly work** after #3 (previously crashed).
- Admins who set these via UI/API: now possible for the first time.
- Blue-green / migration safety: no destructive DB migration needed — registry keys are
  registered in code with defaults; existing DB rows (if any) are respected by `_resolve`.

## 6. Open decisions (need sign-off)

- **D1 — chunk/partition width (`event_log_partition_interval_days`): DECIDED → System 2
  (tunable), as an applied action.** It is changeable at runtime: TimescaleDB
  `set_chunk_time_interval()` and the native-PG partition job both honor a new width for
  *future* chunks/partitions; existing data is never re-chunked. So expose it as a registry
  key (UI-editable, env-overridable). No app restart is involved either
  way. The apply path differs by backend:
  - **TimescaleDB:** the admin action must *issue* `SELECT set_chunk_time_interval(...)` on the
    live connection — storing the registry value is not enough, TimescaleDB won't know about it
    otherwise. Takes effect on the next chunk immediately.
  - **Native PG:** updating the registry value is sufficient; `manage_partitions` already
    re-reads each run and cuts new partitions at the new width.

  Replace the passive `settings.event_log_partition_interval_days` read in `manage_partitions`
  with the registry value (DB = source of truth). UI must warn that a change affects only new
  chunks/partitions. Remaining sub-decision: where the initial width at DB-creation comes from
  (migration default vs. seeded registry value).
- **D2 — #3 approach:** 3a vs 3c. **Recommend 3c** (keeps typo protection).
- **D3 — checkpoint thresholds / collector cadences:** leave as System 1 for now
  **[recommended]**, or also promote to runtime. Out of scope unless desired.
- **D4 — nullable registry ints:** add `get_int_or_none`, or model "off" as `0`/sentinel.
  **Recommend** explicit nullable support.
- **D5 — registry key namespacing:** `partitioning.*` vs `storage.*` for the partition knobs.

## 7. Phased plan

Each phase = atomic commit, `ruff check .` clean, `pytest` green.

- **Phase 0 — Unbreak `.env.example` (DONE).** Comment the registry-only cold keys; replace
  phantom keys with real ones. Already committed + a follow-up for the cold keys.
- **Phase 1 — #3 override surface.** `load_dotenv()` at startup; `Settings` before-validator
  (3c). Tests: `.env`-file registry override beats DB; bootstrap-key typo still raises.
- **Phase 2 — #2 migrate knobs.** Add registry keys + nullable getter; repoint
  compress_chunks / manage_partitions; remove the three Settings fields; update
  `test_config` + scheduler-job tests.
- **Phase 3 — Docs + guard.** Update `.env.example`, STORAGE.md, runtime-settings docs;
  add a regression test that copies `.env.example` → a temp `.env` and asserts `Settings()`
  loads (permanently guards the install-breaker class).

## 8. Testing strategy

- **Install-breaker guard:** copy `.env.example` → tmp `.env`, `Settings()` must load.
- **Precedence:** for each migrated key, assert env > DB > default via `_resolve`.
- **Typo protection (3c):** an unknown bootstrap-style key in `.env` still raises
  `extra_forbidden`; a recognized registry key does not.
- **No behavior drift:** compress_chunks / manage_partitions produce identical SQL when the
  registry value equals the old Settings default.
