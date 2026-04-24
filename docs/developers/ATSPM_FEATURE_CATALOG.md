# TSIGMA Feature Catalog

Complete inventory of TSIGMA platform features with implementation status.

**Status Legend:**

| Status | Meaning |
|--------|---------|
| IMPLEMENTED | Fully built, tested, and operational |
| IN PROGRESS | Partially implemented, work underway |
| DESIGNED | Architecture finalized, implementation not started |
| PLANNED | Scoped for future development |
| FUTURE | Desired capability, no design exists yet |
| NOT PLANNED | Explicitly excluded from TSIGMA scope |

---

## 1. Plugin Architecture

TSIGMA uses seven self-registering plugin systems. All follow the same pattern: auto-discovery via `__init__.py` glob imports, decorator-based registration, modular deployment, and third-party extensibility. Zero core code changes required to add a plugin.

| Plugin System | Registry Decorator | Status |
|---------------|-------------------|--------|
| Ingestion Methods | `@IngestionMethodRegistry.register("name")` | IMPLEMENTED |
| Decoders | `@DecoderRegistry.register` (class decorator) | IMPLEMENTED |
| Background Jobs | `@JobRegistry.register(name="name", trigger="cron", ...)` | IMPLEMENTED |
| Reports | `@ReportRegistry.register("name")` | IMPLEMENTED |
| Auth Providers | `@AuthProviderRegistry.register("name")` | IMPLEMENTED |
| Notifications | `@NotificationRegistry.register("name")` | IMPLEMENTED |
| Validators | `@ValidationRegistry.register("name")` | IMPLEMENTED |

---

## 2. Ingestion Methods

| Method | File | Status |
|--------|------|--------|
| FTP/FTPS/SFTP pull | `ftp_pull.py` | IMPLEMENTED |
| HTTP pull | `http_pull.py` | IMPLEMENTED |
| UDP listener | `udp_server.py` | IMPLEMENTED |
| TCP listener | `tcp_server.py` | IMPLEMENTED |
| Directory watch | `directory_watch.py` | IMPLEMENTED |
| NATS listener | `nats_listener.py` | IMPLEMENTED |
| MQTT listener | `mqtt_listener.py` | IMPLEMENTED |
| gRPC server | `grpc_server.py` | IMPLEMENTED |
| SOAP endpoint (ATSPM 4.x WCF compatibility) | — | IMPLEMENTED |
| REST on-demand poll trigger | — | IMPLEMENTED |


---

## 3. Decoders

| Decoder | File Extensions | Status |
|---------|----------------|--------|
| ASC/3 Econolite binary | `.dat`, `.datz` | IMPLEMENTED |
| Siemens SEPAC text | `.log`, `.txt`, `.csv` | IMPLEMENTED |
| Peek/McCain ATC binary | `.bin`, `.dat`, `.atc` | IMPLEMENTED |
| MaxTime/Intelight XML+binary | `.xml`, `.maxtime`, `.mtl` | IMPLEMENTED |
| Generic CSV | `.csv`, `.txt`, `.tsv` | IMPLEMENTED |
| OpenPhase Protobuf | — | IMPLEMENTED |
| Auto-detect wrapper | — | IMPLEMENTED |

---

## 4. Polling Checkpoint System

| Feature | Status |
|---------|--------|
| Persistent checkpoint per signal per method | IMPLEMENTED |
| File-based checkpoint for FTP (name + mtime + hash) | IMPLEMENTED |
| Event-timestamp checkpoint for HTTP | IMPLEMENTED |
| Non-destructive collection (never deletes from controller) | IMPLEMENTED |
| Crash-safe (checkpoint advances only after successful ingest) | IMPLEMENTED |
| Clock drift detection (future-dated events) | IMPLEMENTED |
| Checkpoint cap at server_time + tolerance | IMPLEMENTED |
| Silent signal detection (N consecutive zero-event cycles) | IMPLEMENTED |
| Poisoned checkpoint auto-recovery | IMPLEMENTED |
| Bulk timestamp correction API | IMPLEMENTED |
| Anchor-based timestamp correction API | IMPLEMENTED |

---

## 5. Data Quality / Watchdog

| Check | Status |
|-------|--------|
| Silent signal detection | IMPLEMENTED |
| Clock drift / future timestamp detection | IMPLEMENTED |
| Poisoned checkpoint auto-recovery | IMPLEMENTED |
| Low event count detection | IMPLEMENTED |
| Missing data window detection | IMPLEMENTED |
| Stuck detector detection | IMPLEMENTED |
| Stuck pedestrian button detection | IMPLEMENTED |
| Phase termination anomaly detection | IMPLEMENTED |
| Low hit count detection | IMPLEMENTED |
| Alert suppression rules | IMPLEMENTED |

---

## 6. Notification System

| Feature | Status |
|---------|--------|
| Email provider | IMPLEMENTED |
| Slack provider | IMPLEMENTED |
| Microsoft Teams provider | IMPLEMENTED |
| Configurable severity thresholds per provider | IMPLEMENTED |
| Fan-out to all active providers | IMPLEMENTED |

---

## 7. Authentication and Authorization

| Feature | Status |
|---------|--------|
| Local username/password (bcrypt + server-side sessions) | IMPLEMENTED |
| Azure AD OIDC | IMPLEMENTED |
| Generic OAuth2 (Google, Okta, Auth0, Keycloak, Cognito) | IMPLEMENTED |
| API key authentication (X-API-Key / Bearer header) | IMPLEMENTED |
| CSRF nonces on login (Valkey-backed, one-time use) | IMPLEMENTED |
| Rate limiting (per-IP login, per-session read/write) | IMPLEMENTED |
| Role-based access control (admin/viewer) | IMPLEMENTED |
| JIT user provisioning from external IdP | IMPLEMENTED |

---

## 8. Configuration Management

| Feature | Status |
|---------|--------|
| Signal CRUD with JSONB metadata | IMPLEMENTED |
| Approach CRUD | IMPLEMENTED |
| Detector CRUD | IMPLEMENTED |
| Route management | IMPLEMENTED |
| Jurisdiction management | IMPLEMENTED |
| Region/Area management | IMPLEMENTED |

---

## 9. Auditing

| Feature | Status |
|---------|--------|
| Signal configuration audit (PG trigger, JSONB snapshots) | IMPLEMENTED |
| Signal audit API endpoint | IMPLEMENTED |
| TimestampMixin on all core models | IMPLEMENTED |
| Request ID tracking middleware | IMPLEMENTED |
| Structured JSON logging | IMPLEMENTED |
| User attribution (changed_by field) | IMPLEMENTED |
| Approach/Detector audit tables | IMPLEMENTED |
| Login/logout audit log | IMPLEMENTED |

---

## 10. Reports

### Report Framework

| Feature | Status |
|---------|--------|
| Report registry with auto-discovery | IMPLEMENTED |
| REST API: list reports, execute, export | IMPLEMENTED |
| Export formats: CSV, JSON, NDJSON | IMPLEMENTED |
| Background execution for long reports | IMPLEMENTED |

### ATSPM Standard Report Types

| Report | Status |
|--------|--------|
| Approach Delay | IMPLEMENTED |
| Approach Volume | IMPLEMENTED |
| Approach Speed | IMPLEMENTED |
| Split Monitor | IMPLEMENTED |
| Split Fail | IMPLEMENTED |
| Phase Termination | IMPLEMENTED |
| Purdue Coordination Diagram (PCD) | IMPLEMENTED |
| Arrivals on Green | IMPLEMENTED |
| Arrival on Red | IMPLEMENTED |
| Pedestrian Delay | IMPLEMENTED |
| Yellow/Red Activations | IMPLEMENTED |
| Timing and Actuations | IMPLEMENTED |
| Preemption (basic entry/exit pairs) | IMPLEMENTED |
| Preempt Detail (full cycle lifecycle state machine) | IMPLEMENTED |
| Preempt Service (plan-indexed service counts, event 105) | IMPLEMENTED |
| Preempt Service Request (plan-indexed demand counts, event 102) | IMPLEMENTED |
| Left Turn Gap Analysis | IMPLEMENTED |
| Left Turn Gap Data Check (pre-flight eligibility gate) | IMPLEMENTED |
| Left Turn Volume (HCM decision-boundary analysis) | IMPLEMENTED |
| Wait Time | IMPLEMENTED |
| Turning Movement Counts | IMPLEMENTED |
| Green Time Utilization | IMPLEMENTED |
| Transit Signal Priority | IMPLEMENTED |
| Ramp Metering | IMPLEMENTED |
| Time-Space Diagram (single-window) | IMPLEMENTED |
| Time-Space Diagram Average (multi-day median cycle) | IMPLEMENTED |
| Link Pivot (corridor optimization) | IMPLEMENTED |
| Red Light Monitor | IMPLEMENTED |
| Bike Volume | IMPLEMENTED |

---

## 11. Aggregation Engine

### Architecture

| Feature | Status |
|---------|--------|
| Three-tier pre-computation architecture | IMPLEMENTED |
| Tier 1: TimescaleDB continuous aggregates (PostgreSQL) / scheduler-populated tables (all other DBs) | IMPLEMENTED |
| Tier 2: API endpoint computed metrics | IMPLEMENTED |
| Tier 3: APScheduler background jobs | IMPLEMENTED |
| Multi-database support (PostgreSQL native + fallback for MS-SQL/Oracle/MySQL) | IMPLEMENTED |

### Bin Sizes

Bin size is a parameter, not a separate feature. The SDK's `bin_timestamp(event_time, bin_minutes)` supports any integer minute value. Reports and aggregates choose their own bin size.

| Bin Size | Status |
|----------|--------|
| Any interval (5, 15, 30, 60 min, etc.) | IMPLEMENTED |

### Aggregation Types

| Aggregation | Status |
|-------------|--------|
| Approach delay (15-minute intervals) | IMPLEMENTED |
| Arrival on red (hourly) | IMPLEMENTED |
| Coordination quality (hourly) | IMPLEMENTED |
| Detector occupancy (hourly) | IMPLEMENTED |
| Detector volume (hourly) | IMPLEMENTED |
| Phase termination (gap-out, max-out, force-off — hourly) | IMPLEMENTED |
| Split failure (hourly) | IMPLEMENTED |
| Cycle boundary (per-cycle phase timing) | IMPLEMENTED |
| Cycle detector arrival (per-activation phase state) | IMPLEMENTED |
| Cycle summary (15-minute binned arrival-on-green) | IMPLEMENTED |
| Approach speed (15th/85th percentile) | DESIGNED |
| Phase cycle (green/yellow/red time) | IN PROGRESS |
| Phase left turn gap (11 gap bins) | IN PROGRESS |
| Phase pedestrian (walks, calls, delay) | IN PROGRESS |
| Preemption (requests, services) | IN PROGRESS |
| Priority (early green, extended green) | IMPLEMENTED |
| Signal event count | IMPLEMENTED |
| Signal plan | IMPLEMENTED |
| Yellow/Red activation | IN PROGRESS |

---

## 12. Database

| Feature | Status |
|---------|--------|
| PostgreSQL primary (with TimescaleDB optional) | IMPLEMENTED |
| Four schemas (config, events, aggregation, identity) | IMPLEMENTED |
| Single schema fallback for MySQL compatibility | IMPLEMENTED |
| Hypertable for event data (TimescaleDB) | IMPLEMENTED |
| Data retention policies (hot/warm/cold) | IMPLEMENTED |
| Idempotent migrations | IMPLEMENTED |
| Multi-database dialect support (PostgreSQL, MS-SQL, Oracle, MySQL) | IMPLEMENTED |

---

## 13. Pipeline Modes

| Mode | Status |
|------|--------|
| Direct mode (single process, less than 2,000 signals) | IMPLEMENTED |
| PostgreSQL queue mode (persistence, retries) | FUTURE |
| Valkey stream mode (high throughput) | FUTURE |
| Signal sharding (multi-worker) | FUTURE |

---

## 14. Background Jobs

| Feature | Status |
|---------|--------|
| APScheduler integration | IMPLEMENTED |
| Cron and interval triggers | IMPLEMENTED |
| Job registry with auto-discovery | IMPLEMENTED |

---

## 15. API

| Feature | Status |
|---------|--------|
| REST API (FastAPI) | IMPLEMENTED |
| SOAP endpoint (ATSPM 4.x WCF compatibility) | IMPLEMENTED |
| OpenAPI/Swagger auto-generated docs | IMPLEMENTED |
| GraphQL (Strawberry) | IMPLEMENTED |

---

## 16. User Interface

| Feature | Status |
|---------|--------|
| Server-rendered (zero-build, no SPA) | IMPLEMENTED |
| Alpine.js for interactivity | IMPLEMENTED |
| ECharts for visualization | IMPLEMENTED |
| MapLibre for geographic display | IMPLEMENTED |
| Tailwind CSS | IMPLEMENTED |

---

## 17. Infrastructure

| Feature | Status |
|---------|--------|
| Docker Compose deployment | IMPLEMENTED |
| Structured JSON logging | IMPLEMENTED |
| Health check endpoints | IMPLEMENTED |
| Request ID tracking | IMPLEMENTED |
| Kubernetes deployment | IMPLEMENTED |

---

## 18. Comparison with ATSPM 4.x / 5.x

### Summary

| Category | TSIGMA | ATSPM 5.x | Assessment |
|----------|--------|-----------|------------|
| Architecture | 7 extensible plugin registries | Monolithic C# solution | TSIGMA ahead |
| Collection safety | Non-destructive, crash-safe checkpoints | Deletes files after download | TSIGMA ahead |
| Clock drift handling | Detects, caps, auto-recovers | Breaks on future-dated data | TSIGMA ahead |
| Authentication | Local + Azure AD + generic OAuth2 | Local + Azure AD | TSIGMA ahead |
| Notifications | Email + Slack + Teams (plugin system) | Email only | TSIGMA ahead |
| Tech stack | Python, FastAPI, async, PostgreSQL | C#, ASP.NET, SQL Server | TSIGMA ahead |
| Decoders | 7 vendor decoders + auto-detect | 5+ vendor decoders | TSIGMA ahead |
| Ingestion methods | 8 methods (FTP/FTPS/SFTP, HTTP, UDP, TCP, dir watch, NATS, MQTT, gRPC) + SOAP | 4.x: FTP/FTPS, SNMP, TCP, UDP, SOAP; 5.x: FTP, SFTP, HTTP, SNMP | TSIGMA ahead |
| Report types | 22 report types implemented | 27+ implemented | Near parity |
| Aggregation | 10 aggregate models implemented, pipeline running | 16 aggregation services populated | ATSPM ahead |
| Watchdog checks | 4 implemented (silent, drift, poisoned, stuck detector) | 7+ implemented | ATSPM ahead |
| SNMP communication | SNMP v1/v2c/v3 SET for rotate mode | Implemented | Parity |
| Device emulator | Not planned | Implemented | ATSPM ahead |
| Location versioning | Audit trail (JSONB snapshots) | Full version history with start dates | ATSPM ahead |

### Key TSIGMA Advantages

- **Plugin architecture**: All seven subsystems (ingestion, decoders, jobs, reports, auth, notifications, validators) are extensible without modifying core code. Third parties can add plugins by dropping a file into the correct directory.
- **Non-destructive collection**: TSIGMA never deletes files from the traffic controller. ATSPM deletes after successful download, creating data loss risk if downstream processing fails.
- **Persistent checkpoints**: Collection state survives crashes, restarts, and network failures. Checkpoint advances only after successful ingest, eliminating data gaps.
- **Clock drift resilience**: ATSPM breaks entirely when controllers produce future-dated timestamps. TSIGMA detects clock drift, caps checkpoints at server_time + tolerance, and provides bulk and anchor-based timestamp correction APIs.
- **Multiple auth providers**: TSIGMA supports local, Azure AD, and generic OAuth2 (Google, Okta, Auth0, Keycloak, Cognito) out of the box via its auth plugin system. ATSPM supports local and Azure AD.
- **Notification plugins**: TSIGMA sends alerts via email, Slack, and Teams with configurable severity thresholds. ATSPM supports email only.
- **Streaming ingestion**: TSIGMA supports NATS, MQTT, and gRPC listeners for real-time event streaming in addition to traditional file-based polling. ATSPM does not support streaming protocols.
- **Modern stack**: Python with FastAPI (async), PostgreSQL with TimescaleDB, Alpine.js/ECharts/MapLibre UI. No Windows dependency, no IIS, no SQL Server requirement.

### Key Areas Where ATSPM 5.x Is Ahead

- **Report types**: ATSPM 5.x has 27+ report types. TSIGMA has 22 implemented.
- **Pre-computed aggregation**: ATSPM 5.x has 16 aggregation services. TSIGMA has 10 aggregate models with a running pipeline — gap is narrowing.
- **Watchdog coverage**: ATSPM 5.x has 7+ watchdog check types (stuck ped, force-off, low hit count, max-out). TSIGMA has 4 checks implemented (silent signal, clock drift, poisoned checkpoint, stuck detector) with more planned.
- **SNMP device communication**: ATSPM communicates with controllers via SNMP. TSIGMA now supports SNMP v1/v2c/v3 SET operations for rotate-mode logging control, reaching parity. ATSPM may still have broader SNMP GET/TRAP capabilities.
- **Device emulator**: ATSPM includes a testing emulator for controller simulation. TSIGMA does not have an equivalent.
- **Location versioning**: ATSPM maintains full version history with start dates for signal configurations. TSIGMA has JSONB audit snapshots but not formal location versioning with date ranges.
