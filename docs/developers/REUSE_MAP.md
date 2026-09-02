# REUSE_MAP - what exists, and where

**Start here before adding code.** This is the orientation map for the codebase: "to do X, read doc
Y, mirror exemplar Z, reuse helper W." Use it so you reuse what is already here instead of rebuilding
it (DRY) and follow the established pattern for each subsystem (SRP / consistency). New to an area?
This is your first read.

**How to use:** find your subsystem below, read the linked doc(s), open the exemplar file and mirror
its shape, reuse the listed helpers. This is a pointer, not the source of truth - confirm specifics
against the code (the docs are the design frame; in a few places the code has hardened past the doc,
see "Known doc/code divergences" at the bottom).

**Keep this current.** When you add a reusable surface (a new SDK helper, a new shared pattern),
update this map in the same commit. A stale map sends the next developer down the wrong path,
duplicating something that already exists.

---

## Subsystem map

| To work on... | Read | Mirror (exemplar) | Reuse (do not reinvent) |
|---|---|---|---|
| **A report / metric** | `REPORTS.md`, `ANALYTICS_IMPLEMENTATION_GUIDE.md` | `reports/approach_speed.py`, `reports/ped_delay.py` | `reports/registry.py` (`Report`, `ReportMetadata`, `@ReportRegistry.register`); the reports SDK in `reports/sdk/`: `aggregates.py` (`aggregate_events` - multi-signal, group_by, hot/cold tier-aware), `events.py` (`fetch_events`), `time_bins.py`, `cycles.py`, `occupancy.py`, `plans.py`, `queries.py`, `limits.py`, `pagination.py`; `config_resolver.get_config_at` |
| **A decoder (controller or sensor)** | `DECODERS.md` | `asc3.py` / `siemens.py` / `d4.py` (file), `wavetronix_advance.py` (sensor) | `collection/decoders/base.py`: `BaseDecoder` / `BaseSensorDecoder`, `DecodedEvent` / `SensorDetection`, `FileMetadata` / `DecodeResult` (the envelope + `decode()` override), `DecoderRegistry` |
| **An ingestion method / listener** | `INGESTION.md`, `LISTENERS.md`, `HIGH_CONCURRENCY_POLLING.md` | `collection/methods/ftp_pull.py`, `http_pull.py`, `directory_watch.py` (plus tcp/udp/nats/mqtt/grpc servers) | `collection/registry.py`; the host-owned spine in `collection/ingest.py`: `ingest_raw` (decode -> normalize -> integrity -> persist, returns the 3-state `IngestOutcome`; nothing raises out of it) and `_queue_review` (the one writer for every review row); the seam in `collection/targets/` (`IngestionTarget.ingest` - a method hands over bytes plus `device_type` and `last_successful_poll`, and never decodes, validates, or persists itself); the ingest SDK in `collection/sdk/__init__.py`: `persist_events`, `persist_events_with_drift_check`, `_upsert_events`, `load_checkpoint`/`save_checkpoint`, `decode_and_persist_message`, `resolve_decoder_by_*`, `is_backward_poisoned` |
| **DB dialects / partitioning / tiering** | `DATABASE.md`, `DATABASE_FACADE_PATTERN.md`, `MULTI_DATABASE_AGGREGATES.md` | `scheduler/jobs/compress_chunks.py`, `manage_partitions.py` | `database/db.py` `DialectHelper`; `database/cold_tier.py`; `scheduler/jobs/` (`aggregate.py`, `export_cold.py`) |
| **A model / schema / migration** | `DATABASE_SCHEMA.md` | `models/signal.py`, `models/event.py`, `models/reference.py`; migration idiom = the latest `alembic/versions/*` (DialectHelper schema + has_table/get_indexes guards) | `models/base.py`: `Base`, `schema_fk`, `tsigma_schema`, `TimestampMixin` |
| **A REST / GraphQL endpoint** | `API.md` | `api/v1/jurisdictions.py` (CRUD), `api/v1/routes.py` (nested membership), `api/v1/reports.py`; author-scoped writes = `api/v1/metric_comments.py`; a side-fetch overlay on a parent resource = `api/v1/signals.py::list_signal_metric_comments` | `api/v1/crud_factory.py` (`crud_router`); `api/v1/schemas.py`; `api/v1/helpers.py` (`get_or_404`) |
| **Auth / access control** | `SECURITY.md` | usage in any `api/v1/*` router | `auth/dependencies.py`: `get_current_user`, `get_current_user_optional`, `require_admin`, `require_access(category)`; `AuthUser`, `SessionData`; `auth/api_keys.py`; categories are seeded rows in `settings_service.DEFAULT_ACCESS_POLICY` - add one there rather than reusing a category with different defaults |
| **Notifications / watchdog** | `NOTIFICATIONS.md`, `WATCHDOG.md` | `scheduler/jobs/watchdog.py` | `notifications/registry.py` (`notify`, severities INFO/WARNING/CRITICAL); `notifications/suppression.py` (`is_suppressed`); `models/alert_suppression.py` |
| **Storage (files / tiles / cold)** | `STORAGE.md` | cold-export usage | `storage/` `StorageBackend` ABC (`put`/`get`/`exists`/`delete`/`list_files`/`get_url`, `StoredFile.last_modified`); `storage/factory.py` (`get_storage_backend`, `get_cold_storage_backend`) |
| **Config / settings** | `CONFIG_LAYERING.md` | `config.py` | `config.py` `Settings` (`TSIGMA_*`); `settings_service.py`; `models/system_setting.py` |
| **Timestamp / poison correction** | `VALIDATION.md`, `DECODERS.md` | the ASC/3 integrity code (inc-1/2/3) | `sdk/__init__.py` drift pipeline (`_warn_on_drift`, checkpoint cap); `api/v1/collection.py` `/corrections/bulk` + `/corrections/anchor`; the `ingest_review` worklist + review endpoints |
| **Auditing** | `AUDITING.md` | `models/audit.py`, `models/signal.py` `SignalAudit` | the append-only audit-row pattern |
| **A plugin / the plugin host** | `adr/0018`-`0020`, TSIGMA-Contract `PROTOCOL.md` | `plugins/connection.py` (the three ADR-0019 modes), `plugins/supervisor.py` | `plugins/protocol.py`: `PluginProcess` (launch/health/shutdown - the only kill+close path), `validate_handshake`, `check_health`; `plugins/connection.py` `PluginConnection` seam (`host_owns_lifecycle`, `idle`); `plugins/coexistence.py` `GrpcCoexistenceMixin` - registration (`register_grpc` / `unregister_grpc`), resolution (`origin` / `is_remote` / `get_connection` / `list_grpc` / `list_names`), the two guards a registry calls from its own decorator and `get` (`_guard_in_process` -> `RegistryConflictError`, `_guard_remote_lookup` -> `RemoteRegistrationError`, both `ValueError` subclasses), and the `_in_process_names` hook each registry overrides; `plugins/supervisor.py` `default_registries`; `plugins/audit.py` (`PluginAuditEvent`, `PluginAuditRecord`, `log_plugin_event`, the injectable sink) + `models/plugin_audit.py` |
| **Tests** | `TESTING.md` | `tests/unit/test_report_with_data.py`, `test_api_reports.py`, `test_model_*` | run via `.venv/bin/python -m pytest tests/`, or `node ~/.claude/tdd/tdd-bridge.mjs run-test --repo-root .` which reads the authoritative command from `.claude/tdd/project-config.json` and is what the TDD chain's gates use. Shared report-test helpers live in `tests/_helpers.py`: `make_event`, `events_to_df`, `make_events_mock_session`, `make_mock_session`, `make_mock_session_factory` (the `async with` factory, yielding the SAME session on every call so a multi-session path like `ingest_raw` can be inspected as a whole) - import these, do not redefine local `_event` / `_events_to_df` / `_mock_session` copies. A double for a Protocol implementation is built with `unittest.mock.create_autospec(<concrete class>, spec_set=True, instance=True)` - a `@runtime_checkable` `isinstance` check proves only that the methods are present, not that they take the arguments the caller passes |
| **Standards (always)** | `STYLE_GUIDE.md`, `CODING_GUIDELINES.md` | -- | PEP 8 + line-length 120 + isort `I001`; UTF-8 no BOM; ASCII punctuation (`->` not arrow chars, no em-dashes); SRP; keep files under 1000 lines |
| **UI / charts / theming** | `UI.md`, `UI_ARCHITECTURE.md`, `THEMING.md` | `static/js/dashboard.js`, `static/js/charts/*` | self-vendored MapLibre + ECharts; the client renders JSON (no server-side chart rendering) |

## Commonly re-invented - reuse these instead
- **Aggregation across signals/time:** `reports/sdk/aggregates.py::aggregate_events` (multi-signal, `group_by`, `"All"`, hot/cold tier-aware). Powers the reports and the aggregate-data builder.
- **Event fetch + time-binning:** `reports/sdk/events.py::fetch_events`, `sdk/time_bins.py`, `sdk/cycles.py`.
- **Suppressible alerts:** `notifications/suppression.py::is_suppressed` + `notifications/registry.py::notify` (best-effort - a notify failure must never block ingest).
- **Decode envelope:** `decoders/base.py` `DecodeResult` / `FileMetadata` - return the envelope (events + file header provenance), not a bare event list, where a header exists.
- **Timestamp correction:** the existing `/corrections/*` endpoints - do not write a new correction engine.
- **CRUD endpoints:** `crud_factory.crud_router` - do not hand-roll CRUD routers. The one standing exception is author-scoped writes (`api/v1/metric_comments.py`): `crud_router` hard-wires `Depends(require_admin)` on its write endpoints and cannot express author-or-admin.
- **Time-anchored annotations (chart comments, and the detector-comment / action-log siblings when they land):** `models/metric_comment.py` + `api/v1/metric_comments.py`, overlay query in `api/v1/signals.py::list_signal_metric_comments`. Mirror the established shape instead of inventing a second one: three anchor states only (unanchored / point / range, enforced by a CheckConstraint AND the create/update validators); authorship is a denormalised `author_uuid` + `author_username` snapshot with NO FK to `auth_user` (matches `system_setting.updated_by`, `alert_suppression.created_by`); reads gated on `require_access("comments")`; the overlay is a side fetch, never embedded in a report response.
- **ANY-of filtering across a many-to-many:** an `IN` over a `scalar_subquery()` of the association table (`list_signal_metric_comments`) - a JOIN duplicates the parent row once per matching child.
- **Registry name resolution (in-process + gRPC):** `plugins/coexistence.py` - `list_names()` for every registered name and where it resolves, `origin()` / `is_remote()` for one name, `_guard_in_process` / `_guard_remote_lookup` inside a registry's decorator and `get`. Do not union a registry's private dict with `list_grpc()` at the call site, and do not re-derive "is this name remote" from a caught error.
- **Report-test fixtures:** `tests/_helpers.py::make_event` / `events_to_df` / `make_events_mock_session` (and `make_mock_session`) - import these in report test modules instead of declaring local `_event` / `_events_to_df` / `_mock_session`. Several legacy report test files still carry their own copies (pre-dating the shared module); new test files must not add to that drift.

## Where the code has hardened past the design
Most of `docs/` was written before the code it describes. In a few places the implementation moved on
and the document did not. That makes a doc/code disagreement a per-item reconciliation, not an
automatic "the doc is right" - and worth fixing the doc when you find one. Known cases:
- **WebSocket `/ws/events`** appears in `UI.md` / `UI_ARCHITECTURE.md` / `README` / `ARCHITECTURE.md`, but was never built - real-time streaming is a not-yet-implemented design. There is no `/ws/events`; do not build against one.
- General: when a doc and the code disagree, the code is frequently the hardened truth - confirm per case before relying on the doc.
