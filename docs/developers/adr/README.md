# Architecture Decision Records (ADRs)

Durable, version-controlled records of significant developer / architecture
decisions for the **TSIGMA (Python, open) edition**. One file per decision. The
*why and rationale* live here; the *how it works* lives in the topical docs
(`ARCHITECTURE.md`, `INGESTION.md`, `DECODERS.md`, …).

> **Fresh start (2026-06-28).** The five ADRs originally numbered 0001–0005 were
> inherited from a now-abandoned repo. They are **not authoritative** and have
> been moved to [`holding/`](holding/). We are authoring the ADR set fresh,
> numbering from `0001`, by walking the architecture. Each holding ADR is pulled
> back in — reconsidered and rewritten in the MADR template — when we reach its
> topic. See [`holding/README.md`](holding/README.md) for the topic map.

## Why ADRs

TSIGMA is an open platform meant for adoption by multiple DOTs and controller /
sidecar vendors. Decisions made during design need to survive personnel changes,
tool changes, and years of contributor turnover. ADRs make the *reasoning*
visible — not just the result — so vendors writing plugins, agencies evaluating
adoption, and future contributors can read why a path was chosen.

## Format

We use **MADR** (Markdown Any Decision Records) — see
[adr.github.io/madr](https://adr.github.io/madr/). Each ADR follows
[template.md](template.md): Title · Status · Date · Deciders · Context and
Problem Statement · Decision Drivers · Considered Options · Decision Outcome
(chosen option + consequences + confirmation) · Pros and Cons of the Options ·
More Information.

## Numbering

Four-digit sequential from `0001`. Filenames are `NNNN-short-kebab-title.md`.
Numbers are assigned **when an ADR is authored** (not pre-reserved from the
backlog), and are never reused. Holding-set numbers do not reserve anything.

## Status values

- **Proposed** — under discussion; not yet adopted
- **Accepted** — current decision
- **Deprecated** — no longer recommended, but not replaced
- **Superseded by [NNNN]** — replaced by a later ADR (which links back here)

## Process

1. Copy `template.md` to `NNNN-short-kebab-title.md` with the next number.
2. Fill in Context, Drivers, Options, Decision, Consequences, Pros/Cons.
3. Status = Proposed while under discussion; flip to Accepted when adopted.
4. When superseding later, write a new ADR and set the old one's Status to
   "Superseded by [NNNN]" with a link.

## Scope boundaries

These ADRs record TSIGMA's own (open-source) architecture and reasoning. They do
not describe or depend on any other implementation.

Two surfaces are designed to be **stable and language-neutral** so they can be
reused beyond this codebase (e.g. by third-party plugin authors); their detailed
specs may be governed as their own artifacts rather than as ADRs here:

- the **gRPC plugin contract** — the boundary plugins build against, and
- the **database schema / abstraction layout**.

## Index by topic

Files stay flat and numbered (the number is the ADR's permanent identity and how
ADRs cross-reference each other); this is a navigation view layered on top. A few
ADRs that genuinely span areas are listed under more than one heading. The
[chronological index](#full-index-chronological) below is the canonical append-log.

**Foundations & principles**
- [0001](0001-record-architecture-decisions.md) — Record architecture decisions
- [0002](0002-core-architecture.md) — Core architecture: single host-owned center, extended only via plugins
- [0003](0003-core-composition-and-deployment.md) — Core composition: one environment-toggled deployable
- [0004](0004-not-kitchen-sink.md) — Core provides data; specialized tools own workflows
- [0006](0006-single-tenant-per-install.md) — Single-tenant per install
- [0007](0007-license-mpl2.md) — License: MPL-2.0
- [0008](0008-vocabulary-pattern.md) — Vocabulary / controlled-lookup pattern
- [0014](0014-core-complete-as-is.md) — Core is complete as-is (usable without plugins, except live ingestion)
- [0081](0081-pin-dependency-versions-exactly.md) — Dependency versions are pinned exactly

**Event model & data typing**
- [0009](0009-canonical-event-model-openphase.md) — Canonical HiRes event model (OPENPHASE / NTCIP / Indiana Hi-Res)
- [0010](0010-typed-columns-jsonb-metadata.md) — Typed columns + namespaced JSONB metadata
- [0011](0011-semantics-in-core-wire-in-plugins.md) — Event semantics in the core; wire formats in decoder plugins
- [0078](0078-signal-identity-tsigma-owned.md) — Signal identity is TSIGMA-owned (assigned BIGINT), not vendor-provided

**Plugins (gRPC subsystems)**
- [0018](0018-subsystems-are-grpc-plugins.md) — Subsystems are gRPC plugins (separate processes; optional NATS)
- [0019](0019-plugin-process-model.md) — Plugin process model: core-managed + cron + external
- [0020](0020-plugin-manifest-and-registration.md) — Plugin declaration: manifest + runtime gRPC Register
- [0021](0021-plugin-installation-mechanisms.md) — Plugin installation: filesystem + OCI + HTTP registry
- [0082](0082-plugins-live-in-their-own-repositories.md) — gRPC plugins live in their own repositories, not in the core
- [0022](0022-plugin-versioning-semver-capabilities.md) — Plugin versioning: semver + capability flags
- [0032](0032-pluggable-storage-backend.md) — Pluggable file storage backend (filesystem + S3) *(also: Storage)*

**Database, schema & code structure**
- [0012](0012-coordination-shared-state.md) — Cluster coordination via shared state (DB + Valkey)
- [0023](0023-database-agnostic-facade.md) — Database-agnostic core via a facade + per-dialect packages
- [0024](0024-models-are-schema-and-api.md) — Models are the schema and API contract (no DTO layer)
- [0025](0025-schema-four-logical-schemas.md) — Four logical schemas
- [0026](0026-additive-idempotent-migrations.md) — Additive-only, idempotent migrations
- [0027](0027-no-repository-or-services-layer.md) — No repository or generic services layer
- [0028](0028-column-and-encoding-conventions.md) — Column & encoding conventions (TEXT + app validation, INTEGER codes, UTF-8)
- [0079](0079-database-access-in-process-not-plugin.md) — Database access is in-process, not a gRPC plugin

**Storage tiers**
- [0029](0029-three-tier-storage-lifecycle.md) — Three-tier storage lifecycle: hot → warm → cold
- [0030](0030-timescaledb-hypertable.md) — TimescaleDB hypertable for the event log (PG + TimescaleDB)
- [0031](0031-cold-tier-parquet-duckdb.md) — Cold tier: partitioned Parquet + DuckDB query
- [0032](0032-pluggable-storage-backend.md) — Pluggable file storage backend *(also: Plugins)*
- [0076](0076-warm-tier-columnar-per-dialect.md) — Warm tier via native per-dialect columnar compression

**Ingestion, polling & listeners**
- [0033](0033-two-ingest-planes.md) — Two ingest planes feeding one event store (legacy poll; future push)
- [0034](0034-ingestion-integrity-spine.md) — Ingestion integrity: never-lose-data, poison-aware spine
- [0035](0035-three-ingestion-execution-modes.md) — Three ingestion execution modes, one registry
- [0036](0036-collector-listener-orchestrators.md) — Separate Collector and Listener orchestrators
- [0037](0037-devicesource-abstraction.md) — DeviceSource abstraction (signals + roadside sensors)
- [0038](0038-listener-config-and-routing.md) — Listener config & routing: three layers, multi-instance, source-IP
- [0039](0039-poll-scale-out.md) — Poll-plane scale-out: shard the device inventory + bounded concurrency
- [0040](0040-event-pipeline-abstraction.md) — Event pipeline abstraction: direct, database, valkey
- [0041](0041-on-demand-poll-api.md) — On-demand poll API: legacy SOAP compat + REST trigger
- [0042](0042-persistent-polling-checkpoint.md) — Persistent, non-destructive polling checkpoint
- [0043](0043-checkpoint-resilience.md) — Checkpoint resilience: immunity, future-cap, drift, auto-recovery
- [0044](0044-file-provenance-and-integrity-detection.md) — File-ingest provenance & integrity detection → review queue
- [0045](0045-two-tier-watchdog.md) — Two-tier watchdog: inline per-cycle + scheduled background

**Decoders & validation**
- [0046](0046-validation-plugin-layers.md) — Validation: plugin-based, three-layer, flag-never-block
- [0047](0047-validation-metadata.md) — Per-event validation metadata (JSONB, worst-status merge)
- [0048](0048-decoder-auto-detection.md) — Decoder selection: extensions + content probing, priority order
- [0049](0049-decoder-output-contract.md) — Decoder output: canonical events + optional provenance envelope
- [0080](0080-decode-scales-as-format-pool.md) — Decoding scales as a format-keyed pool, not by shard

**Audit**
- [0005](0005-auditing-is-a-requirement.md) — Auditing is a core requirement *(also: Foundations)*
- [0015](0015-audit-per-domain-tables.md) — Per-domain audit tables, not a unified table
- [0016](0016-audit-common-base-shape.md) — Audit tables share a common base shape
- [0017](0017-audit-append-only-db-enforced.md) — Audit immutability: append-only, enforced at the database
- [0074](0074-config-audit-valid-time.md) — Config audit: bitemporal valid-time via effective_at

**Config & tuning**
- [0050](0050-layered-configuration.md) — Layered configuration: bootstrap (env/file) + runtime registry (DB)
- [0051](0051-runtime-settings-registry.md) — Runtime-settings registry: typed source of truth + invalidation
- [0077](0077-operator-managed-tuning.md) — Fine-tuning is operator-managed, never hardcoded or env-bound

**Analytics**
- [0052](0052-three-tier-analytics.md) — Three-tier analytics: aggregates, on-demand API, scheduled jobs
- [0053](0053-dual-mode-aggregates.md) — Dual-mode aggregate maintenance: continuous or scheduled
- [0054](0054-custom-analytics-job-isolation.md) — Custom analytics job isolation

**API**
- [0055](0055-api-surfaces-rest-graphql.md) — API surfaces: versioned REST (OpenAPI) + GraphQL read surface
- [0056](0056-rest-conventions.md) — REST conventions: RFC-7807 errors, cursor pagination, filter/sort
- [0057](0057-api-query-guards.md) — API query guards: max-lookback + max-aggregation limits
- [0058](0058-public-read-api-optional.md) — Public read access to metrics; auth for writes/admin

**Security & auth**
- [0013](0013-authn-plugin-authz-core.md) — Authn is a plugin, authz (two-role + jurisdiction) is in the core
- [0059](0059-request-authentication.md) — Request auth: server-side sessions (no JWT) + credential precedence
- [0060](0060-api-keys.md) — API keys: prefixed, hashed at rest, optional expiry
- [0061](0061-csrf-nonce.md) — CSRF protection: one-time nonce for cookie/form auth
- [0062](0062-encryption-at-rest.md) — Encryption at rest: Fernet, decrypt at use, redact in responses
- [0063](0063-rate-limiting.md) — Rate limiting: login, read, and write categories
- [0064](0064-deployment-security-posture.md) — Deployment security: reverse-proxy TLS, headers, explicit CORS
- [0065](0065-jit-provisioning.md) — External-IdP just-in-time provisioning, never auto-downgrade

**Notifications & observability**
- [0066](0066-notification-system.md) — Notifications: plugin-based, fire-and-forget, severity-gated
- [0067](0067-notification-channels.md) — Notification channels: built-in core + plugin channels
- [0068](0068-observability.md) — Observability: structured JSON logs, request-id correlation, OpenTelemetry

**UI & theming**
- [0069](0069-frontend-stack.md) — Frontend stack: server-rendered Jinja2 + Alpine.js + ECharts/MapLibre
- [0070](0070-vendored-frontend-libraries.md) — Vendored frontend libraries: no npm/CDN at runtime
- [0071](0071-theming-semantic-tokens.md) — Theming: semantic design tokens, layered resolution
- [0072](0072-real-time-transport-sse.md) — Real-time transport: SSE for charts, polling for the rest

**Scope & inter-agency**
- [0073](0073-inter-agency-signal-sharing.md) — Inter-agency signal sharing via the OpenPhase/NATS push plane
- [0075](0075-cv-v2x-scope.md) — CV/V2X scope: out of the time-critical path; SPaT ingestion deferred

---

## Full index (chronological)

The canonical append-log — every ADR in the order authored. New ADRs are added
here and filed into an [Index by topic](#index-by-topic) heading.

| # | Title | Status | Date |
|---|-------|--------|------|
| [0001](0001-record-architecture-decisions.md) | Record architecture decisions | Accepted | 2026-06-28 |
| [0002](0002-core-architecture.md) | Core architecture: a single host-owned center, extended only through plugins | Accepted | 2026-06-28 |
| [0003](0003-core-composition-and-deployment.md) | Core composition and deployment: one environment-toggled deployable | Accepted | 2026-06-28 |
| [0004](0004-not-kitchen-sink.md) | Core provides data; specialized tools own workflows (not kitchen-sink) | Accepted | 2026-06-28 |
| [0005](0005-auditing-is-a-requirement.md) | Auditing is a core requirement, not an afterthought | Accepted | 2026-06-28 |
| [0006](0006-single-tenant-per-install.md) | Single-tenant per install | Accepted | 2026-06-28 |
| [0007](0007-license-mpl2.md) | Open-source license: MPL-2.0 | Accepted | 2026-06-28 |
| [0008](0008-vocabulary-pattern.md) | Vocabulary / controlled-lookup pattern | Accepted | 2026-06-28 |
| [0009](0009-canonical-event-model-openphase.md) | Canonical HiRes event model: OPENPHASE / NTCIP / Indiana Hi-Res basis | Accepted | 2026-06-28 |
| [0010](0010-typed-columns-jsonb-metadata.md) | Typed columns + namespaced JSONB metadata | Accepted | 2026-06-28 |
| [0011](0011-semantics-in-core-wire-in-plugins.md) | Event/protocol semantics in the core; wire formats in decoder plugins | Accepted | 2026-06-28 |
| [0012](0012-coordination-shared-state.md) | Cluster coordination via shared state (DB + Valkey) | Accepted | 2026-06-28 |
| [0013](0013-authn-plugin-authz-core.md) | Auth: authn is a plugin, authz (two-role + jurisdiction) is in the core | Accepted | 2026-06-28 |
| [0014](0014-core-complete-as-is.md) | Core is complete as-is (usable without plugins, except live ingestion) | Accepted | 2026-06-28 |
| [0015](0015-audit-per-domain-tables.md) | Audit: per-domain tables, not a unified table | Accepted | 2026-06-28 |
| [0016](0016-audit-common-base-shape.md) | Audit tables share a common base shape | Accepted | 2026-06-28 |
| [0017](0017-audit-append-only-db-enforced.md) | Audit immutability: append-only, enforced at the database | Accepted | 2026-06-28 |
| [0018](0018-subsystems-are-grpc-plugins.md) | Subsystems are gRPC plugins (separate processes; optional NATS) | Accepted | 2026-06-28 |
| [0019](0019-plugin-process-model.md) | Plugin process model: core-managed children + cron + external | Accepted | 2026-06-28 |
| [0020](0020-plugin-manifest-and-registration.md) | Plugin declaration: manifest file + runtime gRPC Register | Accepted | 2026-06-28 |
| [0021](0021-plugin-installation-mechanisms.md) | Plugin installation: filesystem + OCI + HTTP registry | Accepted | 2026-06-28 |
| [0022](0022-plugin-versioning-semver-capabilities.md) | Plugin versioning: semver + capability flags + required core API version | Accepted | 2026-06-28 |
| [0023](0023-database-agnostic-facade.md) | Database-agnostic core via a facade and per-dialect packages | Accepted | 2026-06-28 |
| [0024](0024-models-are-schema-and-api.md) | Models are the schema and the API contract (no DTO layer) | Accepted | 2026-06-28 |
| [0025](0025-schema-four-logical-schemas.md) | Database schema layout: four logical schemas | Accepted | 2026-06-28 |
| [0026](0026-additive-idempotent-migrations.md) | Migrations are additive-only and idempotent (no destructive downgrades) | Accepted | 2026-06-28 |
| [0027](0027-no-repository-or-services-layer.md) | No repository or generic services layer | Accepted | 2026-06-28 |
| [0028](0028-column-and-encoding-conventions.md) | Column and encoding conventions: TEXT + app validation, INTEGER event codes, UTF-8 | Accepted | 2026-06-28 |
| [0029](0029-three-tier-storage-lifecycle.md) | Three-tier storage lifecycle: hot → warm → cold | Accepted | 2026-06-28 |
| [0030](0030-timescaledb-hypertable.md) | TimescaleDB hypertable for the event log (PostgreSQL + TimescaleDB; daily chunks) | Accepted | 2026-06-28 |
| [0031](0031-cold-tier-parquet-duckdb.md) | Cold tier: partitioned Parquet with DuckDB unified query | Accepted | 2026-06-28 |
| [0032](0032-pluggable-storage-backend.md) | Pluggable file storage backend (filesystem + S3) | Accepted | 2026-06-28 |
| [0033](0033-two-ingest-planes.md) | Two ingest planes feeding one event store (legacy poll; future push) | Accepted | 2026-06-28 |
| [0034](0034-ingestion-integrity-spine.md) | Ingestion integrity: never-lose-data, poison-aware, host-owned spine | Accepted | 2026-06-28 |
| [0035](0035-three-ingestion-execution-modes.md) | Three ingestion execution modes, one registry | Accepted | 2026-06-28 |
| [0036](0036-collector-listener-orchestrators.md) | Separate Collector and Listener orchestrators | Accepted | 2026-06-28 |
| [0037](0037-devicesource-abstraction.md) | DeviceSource abstraction (signals and roadside sensors) | Accepted | 2026-06-28 |
| [0038](0038-listener-config-and-routing.md) | Listener configuration and routing: three layers, multi-instance, source-IP | Accepted | 2026-06-28 |
| [0039](0039-poll-scale-out.md) | Poll-plane scale-out: shard the device inventory + bounded concurrency | Accepted | 2026-06-28 |
| [0040](0040-event-pipeline-abstraction.md) | Event pipeline abstraction: direct, database, valkey | Accepted | 2026-06-28 |
| [0041](0041-on-demand-poll-api.md) | On-demand poll API: legacy SOAP compatibility + REST trigger | Accepted | 2026-06-28 |
| [0042](0042-persistent-polling-checkpoint.md) | Persistent, non-destructive polling checkpoint | Accepted | 2026-06-28 |
| [0043](0043-checkpoint-resilience.md) | Checkpoint resilience: immunity, future-cap, drift detection, auto-recovery | Accepted | 2026-06-28 |
| [0044](0044-file-provenance-and-integrity-detection.md) | File-ingest provenance and integrity detection → review queue | Accepted | 2026-06-28 |
| [0045](0045-two-tier-watchdog.md) | Two-tier watchdog: inline per-cycle + scheduled background | Accepted | 2026-06-28 |
| [0046](0046-validation-plugin-layers.md) | Validation: plugin-based, three-layer, flag-never-block | Accepted | 2026-06-28 |
| [0047](0047-validation-metadata.md) | Per-event validation metadata (JSONB, worst-status merge) | Accepted | 2026-06-28 |
| [0048](0048-decoder-auto-detection.md) | Decoder selection: declared extensions + content probing, priority order | Accepted | 2026-06-28 |
| [0049](0049-decoder-output-contract.md) | Decoder output: pure canonical events + optional provenance envelope | Accepted | 2026-06-28 |
| [0050](0050-layered-configuration.md) | Layered configuration: bootstrap settings (env/file) + runtime registry (DB) | Accepted | 2026-06-28 |
| [0051](0051-runtime-settings-registry.md) | Runtime-settings registry: typed source of truth + cross-replica invalidation | Accepted | 2026-06-28 |
| [0052](0052-three-tier-analytics.md) | Three-tier analytics: pre-computed aggregates, on-demand API, scheduled jobs | Accepted | 2026-06-28 |
| [0053](0053-dual-mode-aggregates.md) | Dual-mode aggregate maintenance: continuous aggregates or scheduled refresh | Accepted | 2026-06-28 |
| [0054](0054-custom-analytics-job-isolation.md) | Custom analytics job isolation | Accepted | 2026-06-28 |
| [0055](0055-api-surfaces-rest-graphql.md) | API surfaces: versioned REST (OpenAPI) + GraphQL read surface | Accepted | 2026-06-28 |
| [0056](0056-rest-conventions.md) | REST conventions: RFC-7807 errors, cursor pagination, filter/sort/search | Accepted | 2026-06-28 |
| [0057](0057-api-query-guards.md) | API query guards: max-lookback and max-aggregation limits | Accepted | 2026-06-28 |
| [0058](0058-public-read-api-optional.md) | Public read access to metrics; authentication for writes and admin | Accepted | 2026-06-28 |
| [0059](0059-request-authentication.md) | Request authentication: server-side sessions (no JWT) + credential precedence | Accepted | 2026-06-28 |
| [0060](0060-api-keys.md) | API keys: prefixed, hashed at rest, optional expiry | Accepted | 2026-06-28 |
| [0061](0061-csrf-nonce.md) | CSRF protection: one-time nonce for cookie/form auth | Accepted | 2026-06-28 |
| [0062](0062-encryption-at-rest.md) | Encryption at rest: Fernet for secrets, decrypt at point of use, redact in responses | Accepted | 2026-06-28 |
| [0063](0063-rate-limiting.md) | Rate limiting: login, read, and write categories | Accepted | 2026-06-28 |
| [0064](0064-deployment-security-posture.md) | Deployment security posture: reverse-proxy TLS, security headers, explicit CORS | Accepted | 2026-06-28 |
| [0065](0065-jit-provisioning.md) | External-IdP just-in-time provisioning, never auto-downgrade | Accepted | 2026-06-28 |
| [0066](0066-notification-system.md) | Notifications: plugin-based, fire-and-forget, severity-gated | Accepted | 2026-06-28 |
| [0067](0067-notification-channels.md) | Notification channels: built-in core channels + plugin channels | Accepted | 2026-06-28 |
| [0068](0068-observability.md) | Observability: structured JSON logs to stdout, request-id correlation, OpenTelemetry | Accepted | 2026-06-28 |
| [0069](0069-frontend-stack.md) | Frontend stack: server-rendered Jinja2 + Alpine.js + ECharts/MapLibre (no SPA build) | Accepted | 2026-06-28 |
| [0070](0070-vendored-frontend-libraries.md) | Vendored frontend libraries: no npm/CDN at runtime | Accepted | 2026-06-28 |
| [0071](0071-theming-semantic-tokens.md) | Theming: semantic design tokens (CSS custom properties), layered resolution | Accepted | 2026-06-28 |
| [0072](0072-real-time-transport-sse.md) | Real-time transport: SSE for continuous charts, polling for everything else | Accepted | 2026-06-28 |
| [0073](0073-inter-agency-signal-sharing.md) | Inter-agency signal sharing via the OpenPhase/NATS push plane | Accepted | 2026-06-28 |
| [0074](0074-config-audit-valid-time.md) | Config audit: bitemporal valid-time via effective_at | Accepted | 2026-06-28 |
| [0075](0075-cv-v2x-scope.md) | CV/V2X scope: out of the time-critical path; SPaT ingestion deferred | Accepted | 2026-06-28 |
| [0076](0076-warm-tier-columnar-per-dialect.md) | Warm tier via native per-dialect columnar compression | Accepted | 2026-06-28 |
| [0077](0077-operator-managed-tuning.md) | Fine-tuning is operator-managed, never hardcoded or env-bound | Accepted | 2026-07-16 |
| [0078](0078-signal-identity-tsigma-owned.md) | Signal identity is TSIGMA-owned, not vendor-provided | Accepted | 2026-07-16 |
| [0079](0079-database-access-in-process-not-plugin.md) | The database access layer is in-process, not a gRPC plugin | Accepted | 2026-07-16 |
| [0080](0080-decode-scales-as-format-pool.md) | Decoding scales as a format-keyed pool, not by shard | Accepted | 2026-07-16 |
| [0081](0081-pin-dependency-versions-exactly.md) | Dependency versions are pinned exactly | Accepted | 2026-08-22 |
| [0082](0082-plugins-live-in-their-own-repositories.md) | gRPC plugins live in their own repositories, not in the core | Accepted | 2026-08-29 |

---

## Open questions to settle (when authoring the relevant ADR)

Open questions surfaced during the review. **The dev docs are old and possibly
outdated**, so a doc-vs-doc mismatch usually means one doc is stale — these are
resolved by deciding the coherent design (the ADRs are authoritative) and
reconciling the docs after, not by treating stale text as a competing option.
Genuine design forks are decided with the user as they come up.

| # | Question | Sources | Decide with |
|---|----------|---------|-------------|
| ~~Q1~~ | **RESOLVED (ADR-0034 + ADR-0046)** — flag-never-block; L1 deterministic runs inline between decode and persist, L2/L3 run async after persist. | — | — |
| ~~Q2~~ | **RESOLVED (ADR-0027)** — no repository or generic services layer; routes use the facade/ORM, reports use the SDK. The `CODING_GUIDELINES.md` "repository pattern" line is stale. | — | — |
| ~~Q3~~ | **RESOLVED (ADR-0055)** — both in scope: versioned REST with OpenAPI/Swagger (primary) + a read-only GraphQL surface. The UI happens to use REST. | — | — |
| ~~Q4~~ | **RESOLVED (ADR-0072)** — SSE for continuous live-updating charts; polling for discrete updates (notifications, alerts, status); no WebSocket. | — | — |
| ~~Q5~~ | **RESOLVED (ADR-0074)** — adopt `effective_at` valid-time on config-audit tables (default `changed_at`); bitemporal-capable, valid-time queries built now. | — | — |
| ~~Q6~~ | **RESOLVED (ADR-0032)** — S3/storage secrets are sensitive *storage* config (encrypted at rest / default credential chain), not DB credentials; the "no DB creds" rule is unaffected. | — | — |

---

## Backfill queue

Inventory of latent architectural decisions extracted from the developer docs
during the decisions-first review. Grouped roughly by architecture area (the
order we'll likely walk). `(F)` = foundational, `(N)` = notable. We author these
as MADR ADRs, pulling in the relevant `holding/` ADR at its topic. Pure
coding-style rules (line length, isort, docstrings, nesting/file caps, BOM, etc.)
are excluded — they live in STYLE_GUIDE.md / CODING_GUIDELINES.md.

> Nearly every item below is a **core** decision — the core is the bulk of the
> ADR set. The gRPC plugin contract and the database layout are its two faces,
> authored after the core tiers.

**Core — principles, patterns & data model** — authored as 0004–0014 (see index above). Remaining:
- (N) Latency budget defines core-vs-edge responsibility → authored as ADR-0075 (CV/V2X scope)

**Platform / plugin architecture (pull in holding-0002)**
- (F) Everything is a gRPC plugin (the extension boundary; the shared, language-neutral contract) — holding-0002
- (N) Python 3.13 runtime target — ARCHITECTURE
- (F) Flat single-package layout, env-toggled component activation — ARCHITECTURE §5/§16
- (F) No repository/data-access layer; routes use SQLAlchemy via injected session — ARCHITECTURE §16.2 → authored as ADR-0027
- (N) No generic services layer; domain-specific services only — ARCHITECTURE §16.3
- (N) Models are the schema AND the API contract (no DTO layer) — DATABASE_FACADE_PATTERN §5/§6
- (F) Additive-only, idempotent migrations; no downgrades — ARCHITECTURE §15, DATABASE
- (N) Structured JSON logging + request-id correlation middleware — ARCHITECTURE §14
- (N) Ordered lifespan startup/shutdown sequence — ARCHITECTURE §4
- (N) TDD is a project requirement, not optional — CODING_GUIDELINES

**Database & dialect abstraction**
- (F) DatabaseFacade hides connection/session/dialect from routes — DATABASE_FACADE_PATTERN §3
- (F) DialectHelper: pure dialect SQL generation (PG/MS-SQL/Oracle/MySQL) — DATABASE_FACADE_PATTERN §2
- (N) Four-schema logical separation (config/events/aggregation/identity); MySQL prefix fallback — DATABASE §Schema-Org
- (N) Application-level string validation; TEXT over VARCHAR(n) — DATABASE_SCHEMA §Validation-Strategy
- (N) INTEGER event_code/event_param (real-world ATSPM compatibility) — DATABASE_SCHEMA §Core-Event-Table
- (N) UTF-8 default DB encoding (LATIN1 optional, immutable) — DATABASE §Encoding
- (N) Network triple (ip/port/protocol) as first-class indexed columns — DATABASE_SCHEMA §signal

**Storage tiers**
- (F) Three-tier hot/warm/cold storage lifecycle — ARCHITECTURE §8, DATABASE §Tiers
- (F) TimescaleDB hypertable for event log; daily chunk default — DATABASE_SCHEMA §Core-Event-Table
- (F) Dual-mode aggregates: continuous aggregates (Timescale) vs scheduled delete-reinsert (other DBs) — MULTI_DATABASE_AGGREGATES
- (F) Cold-tier unified query via DuckDB (FDW for PG, app-layer otherwise) — STORAGE §Cold-Tier-Query
- (N) Pluggable storage backend factory (filesystem/S3) — STORAGE §Architecture ⚠Q6
- (N) Raw-file 90-day retention for reprocessing — ARCHITECTURE §10

**Ingestion planes & integrity (pull in holding-0003, holding-0004)**
- (F) Two ingest planes feeding one HiRes store — holding-0003
- (F) Ingestion integrity: never-lose-data + poison-aware, host-owned spine — holding-0004 ⚠Q1
- (F) Persistent non-destructive polling checkpoint — INGESTION §Checkpoint, DATABASE_SCHEMA
- (F) Four-part checkpoint resilience (file-immunity / future-cap / drift-detect / auto-recovery) — INGESTION, WATCHDOG
- (N) File-ingest provenance + replacement/config-drift/temporal-integrity detection → review queue — DECODERS
- (N) Controller clock-offset trending watchdog — DECODERS §Clock-health
- (F) Two-tier watchdog (inline per-cycle + scheduled daily) — WATCHDOG

**Ingestion methods & orchestration**
- (F) Three ingestion execution modes, one registry (Polling/Listener/EventDriven) — INGESTION, ARCHITECTURE §7
- (N) Separate Collector vs Listener orchestrators — INGESTION §Orchestration
- (N) DeviceSource abstraction (signals + roadside sensors) — ARCHITECTURE §7, DATABASE_SCHEMA
- (N) Three-layer listener config (lifecycle gate / server conn / per-device JSONB) — LISTENERS
- (N) Multi-instance discriminator for multi-broker deployments — LISTENERS §Multi-Instance
- (N) Source-IP B-tree routing for inbound TCP/UDP — LISTENERS §Source-IP
- (N) Signal sharding via WORKER_ID/WORKER_COUNT — INGESTION §Sharding
- (N) Bounded high-concurrency polling (asyncio semaphore) — HIGH_CONCURRENCY_POLLING
- (N) Event pipeline abstraction (direct/postgres/valkey) — INGESTION §Pipeline
- (N) Legacy SOAP/WCF compatibility endpoint + REST poll-trigger — INGESTION §On-Demand

**Decoders**
- (N) Decoder registry + glob auto-discovery + extension/content auto-detection — DECODERS
- (N) decode() vs decode_bytes() (optional provenance envelope) — DECODERS §Envelope
- (N) OpenPhase multi-format decode + NTCIP-with-EventType-fallback — DECODERS §OpenPhase

**Validation**
- (N) Validation plugin registry + 3-layer stratification (L1 now; L2/L3 ML deferred) — VALIDATION ⚠Q1
- (N) Per-event validation_metadata JSONB with worst-status merge — VALIDATION

**Config (pull in holding-0005 for valid-time)**
- (F) Three-tier config precedence (env > file > DB) + TTL cache + Valkey pub/sub invalidation — ARCHITECTURE §13, CONFIG_LAYERING
- (N) Typed runtime-settings registry as source of truth (System 2) — ARCHITECTURE §13, CONFIG_LAYERING

**Analytics**
- (F) Three-tier analytics precomputation (continuous-agg / on-demand API / scheduled jobs) — ARCHITECTURE §9, ANALYTICS
- (N) Custom analytics job isolation (own tables, read-only core, idempotent) — ANALYTICS, ARCHITECTURE §11

**API**
- (F) REST /api/v1 with content negotiation (JSON/CSV/XML) — API §Content-Negotiation ⚠Q3
- (N) GraphQL (Strawberry) alongside REST — API §GraphQL ⚠Q3
- (N) API max-lookback / max-aggregation guards — ARCHITECTURE §8, API

**Auth & security**
- (F) Server-side opaque sessions; JWT rejected — API, SECURITY §3
- (F) Single active auth provider (local|oidc|oauth2) via env — SECURITY §1
- (N) Auth precedence: API key > Bearer > session cookie — API §Auth-Priority
- (F) Two-role RBAC (admin/viewer) — SECURITY §2
- (N) Jurisdiction-scoped visibility enforced at ORM layer — SECURITY §2
- (N) JIT external-IdP provisioning + never-downgrade-admin — SECURITY §1.3
- (F) Fernet encryption-at-rest (3-source key; encrypt-on-write/decrypt-at-poll; response redaction) — SECURITY §7 ⚠Q6
- (N) API keys: bcrypt hash + tsgm_ prefix + optional expiry — SECURITY §10
- (N) CSRF one-time nonce (5-min TTL) on form auth — API, SECURITY
- (N) Rate limiting in three categories (login/read/write) — API, SECURITY §9
- (F) No in-app TLS termination; reverse proxy required — SECURITY §4
- (N) Defense-in-depth security headers + explicit CORS + security-first middleware order — SECURITY §4/§8

**Notifications**
- (F) Notification plugin system: fire-and-forget, severity-gated, comma-sep providers, no per-alert routing — NOTIFICATIONS

**UI & theming**
- (F) Server-rendered Jinja2 + Alpine.js + ECharts/MapLibre; no SPA build — UI, UI_ARCHITECTURE
- (F) Vendor-committed frontend libs; no npm/CDN at runtime — UI_ARCHITECTURE
- (F) Semantic-token theming (CSS custom properties) + layered theme resolution — THEMING
- (N) Tailwind scoped content detection / safelist contract — THEMING §8
- (N) Real-time transport strategy — UI ⚠Q4

**Audit**
- (N) Dialect-aware audit triggers + transaction-scoped session-var user attribution — AUDITING ⚠Q5
- (N) Three audit categories (config triggers / auth_audit_log / request-id) — AUDITING

## When NOT to write an ADR

- Trivial implementation choices (names, function decomposition)
- Decisions already captured in coding standards (STYLE_GUIDE.md / CODING_GUIDELINES.md)
- Reversible day-to-day decisions inside an established architecture
- Documentation-only changes
