# TSIGMA — The Next Generation ATSPM

**Traffic Signal Intelligence: Gathering Metrics & Analytics**

An open source replacement for ATSPM (Automated Traffic Signal Performance Measures).

---

## Overview

TSIGMA is a modern, modular platform for collecting, storing, and analyzing traffic signal performance data. It replaces the aging ATSPM codebase with a clean architecture built for scale, performance, and extensibility.

## Quick Start

### Requirements
- Python 3.14+ (free-threaded for true multi-threaded performance)
- MS-SQL, Oracle, MySQL, PostgreSQL 18+ (recommended) (with TimescaleDB also recommended)

### Installation

```bash
# Clone repository
git clone https://github.com/OpenPhase-Labs/TSIGMA.git
cd TSIGMA

# Create virtual environment
python3.14 -m venv .venv
source .venv/bin/activate

# Install in development mode
pip install -e ".[dev]"

# Configure database
cp .env.example .env
# Edit .env with your PostgreSQL connection

# Run migrations
alembic upgrade head

# Start server
python -m tsigma.main
# Or: uvicorn tsigma.app:app --host 0.0.0.0 --port 8080 --reload
```

### Verify It's Running

```bash
# Health check (no auth required)
curl http://localhost:8080/health

# Readiness check (verifies DB connection)
curl http://localhost:8080/ready

# OpenAPI docs
# Open http://localhost:8080/docs in a browser
```

---

### Why Replace ATSPM?

| Aspect | ATSPM 4.x | ATSPM 5.x | TSIGMA |
|--------|-----------|-----------|--------|
| Query performance | Hours | Hours | Seconds (on hot data) |
| Event storage | Individual indexed rows | Compressed binary blobs | Individual indexed rows |
| Event code queries | Direct SQL WHERE clause | Decompress → filter in app | Direct SQL WHERE clause |
| Storage | 4+ TB / 3 weeks (unpartitioned) | Unknown (compressed; ratio not characterized) | ~2.5 TB / 3 weeks (partitioned) |
| Architecture | 77 projects, 172K lines C# | 26 projects, 156K lines C# | Modular Python |
| Deployment | Manual / IIS | Docker (6+ containers) | Docker (1-2 containers) |
| Maintainability | Complex codebase | Microservices complexity | Clean, testable |
| Protocol support | FTP/FTPS, SNMP, TCP, UDP, SOAP | FTP, SFTP, HTTP, SNMP | Multi-protocol (FTP/FTPS/SFTP, HTTP, UDP, TCP, SOAP, NATS, MQTT, gRPC, directory watch) |
| Database | MS-SQL only | Multi-DB (unoptimized) | Multi-DB (PostgreSQL + TimescaleDB preferred, MS-SQL, Oracle, MySQL) |
| Frontend | ASP.NET | Angular SPA | Hybrid (server + Alpine.js) |
| API style | REST (WCF + WebAPI 2 / .NET Framework) | REST (ASP.NET Core; ConfigApi / ReportApi / IdentityApi / WatchdogApi split) | REST (FastAPI w/ auto-OpenAPI) + GraphQL |
| API documentation | Minimal / undocumented | Swagger/OpenAPI per service | Swagger/OpenAPI + GraphQL introspection at `/graphql` |
| Raw IHR event log API | Yes — `/api/data/controllerEventLogs*` (auth + record-cap gated) | None | Indexed SQL access (events are individually rowed) |
| Report API | Pre-aggregated data only (`/api/data/*Aggregate` endpoints, ~10); reports themselves rendered via WebForms — adding a new report requires C# code + recompile | One REST controller per report — adding a new report requires a new controller class + recompile | Plugin architecture: any module that registers via `@ReportRegistry.register("name")` is automatically discovered and exposed at `POST /api/v1/reports/{name}` (no code changes to the API layer). Generic `GET /api/v1/reports/` lists all registered reports; `/api/v1/reports/{name}/export` for CSV/JSON; plus 13 dedicated analytics endpoints under `/api/v1/analytics/*` |
| Programmatic config CRUD | Limited (read-mostly) | Full (ConfigApi) | Full CRUD (signals, detectors, approaches, routes, corridors, regions, jurisdictions) |
| Authentication | JWT bearer | ASP.NET Identity / OIDC | Session cookie + OIDC + OAuth2 |

### Key Differences

- **Queryable event storage.** Events are individually indexed rows. Queries by signal/event-code/time hit the index directly and return in milliseconds — no app-side decompression like ATSPM 5.x's compressed-blob model, no full-table scans like ATSPM 4.x's unpartitioned tables.
- **Plugin architecture end-to-end.** Every extensible surface is a registry-driven plugin — add a new one with a one-line decorator and it's automatically discovered, exposed via REST, and surfaced in the UI. No controller class, no recompile, no core changes:
  - **Decoders** — `@DecoderRegistry.register` (ASC/3, Intelight MaxTime, Siemens, Peek, Wavetronics, OpenPhase Protobuf, CSV, auto-detect, …)
  - **Ingestion methods** — `@IngestionMethodRegistry.register` (FTP/FTPS/SFTP, HTTP, NATS, MQTT, gRPC, TCP, UDP, directory watch, SOAP, …)
  - **Reports** — `@ReportRegistry.register` (PCD, split monitor, preempt detail, etc. — 29 currently shipped)
  - **Validators** — `@ValidatorRegistry.register` (schema/range, temporal anomaly, cross-signal corridor, …)
  - **Storage backends** — `@StorageRegistry.register` (filesystem, S3, …)
  - **Scheduled jobs** — `@JobRegistry.register` (aggregation, compression, watchdog, …)

  Adding support for a new controller vendor, a new transport, a new report, or a new validation pass is a single new file. ATSPM has none of this — every change requires editing core projects and recompiling the appropriate microservice.
- **Multi-protocol streaming ingestion.** Native support for NATS, MQTT, gRPC, HTTP push, TCP/UDP listeners, directory watch, and SOAP — alongside traditional FTP/SFTP/HTTP polling. ATSPM is file-pull only.
- **Database portability.** Runs on PostgreSQL (preferred, with TimescaleDB for compression/partitioning), MS-SQL, Oracle, or MySQL via a dialect abstraction layer. ATSPM 4.x is MS-SQL-only; 5.x claims multi-DB but is unoptimized for anything but its primary target.
- **Modern API surface.** REST (FastAPI) with auto-generated OpenAPI docs, plus GraphQL with introspection. Raw IHR event log access remains available — ATSPM 5.x removed that capability entirely.
- **Operational simplicity.** Runs in 1–2 Docker containers vs. ATSPM 5.x's 6+ microservices. Single modular Python codebase vs. 26–77 separate C# projects (156K–172K lines of C#) that you have to learn before contributing.
- **Validation pipeline.** Built-in three-layer validation (schema/range, temporal/anomaly, cross-signal corridor) with a plugin SDK and per-deployment configurability. ATSPM has no equivalent — invalid data lands in the same table as good data.

---

## Architecture

```
┌───────────────────────────────────────────────────────────────────┐
│                           TSIGMA                                  │
│    Traffic Signal Intelligence: Gathering Metrics & Analytics     │
│                                                                   │
│  ┌─────────────────┐    ┌─────────────┐    ┌─────────────┐        │
│  │ Collection      │    │   Storage   │    │  Base API   │        │
│  │ (FTP/HTTP/NATS) │--->│ PostgreSQL  │--->│  + Charts   │        │
│  │ Decoders        │    │ + Parquet   │    │             │        │
│  └─────────────────┘    └─────────────┘    └─────────────┘        │
│                                                                   │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐            │
│  │   Config    │    │  Watchdog   │    │   Web UI    │            │
│  │ Management  │    │  Alerting   │    │             │            │
│  └─────────────┘    └─────────────┘    └─────────────┘            │
│                                                                   │
│  ┌─────────────────┐    ┌─────────────────────────────────────┐   │
│  │   Validation    │    │         Module Interface            │   │
│  │  (Layer 1-3)    │    │                                     │   │
│  └─────────────────┘    └─────────────────────────────────────┘   │
│                                        │                          │
│                        ┌───────────────┼──────────────┐           │
│                        v               v              v           │
│                 ┌───────────┐   ┌───────────┐   ┌───────────┐     │
│                 │ Module A  │   │ Module B  │   │ Module C  │     │
│                 │ (SigOps)  │   │ (future)  │   │ (future)  │     │
│                 └───────────┘   └───────────┘   └───────────┘     │
└───────────────────────────────────────────────────────────────────┘
```

---

## Core Components

### Collection Layer
- **Ingestion methods:** FTP/FTPS/SFTP, HTTP(S), NATS, MQTT, gRPC, TCP, UDP, directory watch, SOAP
- **Vendor decoders:** ASC/3 (Econolite), Intelight (MaxTime XML), Siemens, Peek, Wavetronics, OpenPhase Protobuf, CSV, auto-detect
- Per-signal collection configuration via JSONB metadata
- Async/parallel processing with semaphore-bounded concurrency
- Scheduled collection with configurable intervals
- Checkpoint-based incremental collection (never re-downloads data)

### Storage Layer
- **PostgreSQL 18+** with TimescaleDB (preferred) or pg_partman for partition management
- **Hot storage:** 3-4 weeks in PostgreSQL (fast queries, configurable)
- **Warm storage:** TimescaleDB compression (~10x reduction after configurable threshold)
- **Cold archive:** Export old partitions to Parquet files
- Query cold data with DuckDB or Polars when historical analysis is needed
- Multi-database support: PostgreSQL, MS-SQL, Oracle, MySQL (dialect abstraction layer)

### Validation Pipeline
- **Layer 1 (always on):** Deterministic schema/range validation against NTCIP 1202 event code definitions
- **Layer 2 (plugin, planned):** Temporal/anomaly detection via NTCIP 1202 SLM + MCP
- **Layer 3 (plugin, planned):** Cross-signal corridor correlation via SLM + MCP
- Plugin SDK for standardized validation metadata (JSONB on each event record)
- Admin-configurable validation depth per deployment
- Post-ingestion async processing — never blocks real-time ingestion

### Configuration
- Signals, detectors, approaches, phases
- Controller types and vendor settings
- Routes and corridors
- Regions and jurisdictions (hierarchical)
- Signal plans
- Audit trail (JSONB snapshots)

### Reports (plugin architecture)

Reports are self-registering plugins — any module that registers with the report registry is automatically discovered, surfaced via `GET /api/v1/reports/`, and executable via `POST /api/v1/reports/{name}`. No API or core changes are needed to ship new reports.

Currently shipped (29):
- Purdue Coordination Diagram (PCD)
- Split Monitor / Split Failure
- Approach Volume / Delay / Speed
- Arrival on Green
- Yellow/Red Actuations
- Pedestrian Delay
- Preemption Analysis / Preempt Detail
- Left Turn Gap Analysis
- Turning Movement Counts
- Time-Space Diagram
- Link Pivot
- Green Time Utilization
- Timing and Actuations
- Ramp Metering
- Red Light Monitor
- Bike Volume
- Wait Time
- Transit Signal Priority

### Aggregation Pipeline
- Three-tier aggregation (15-minute, hourly, daily)
- 7 aggregate models: approach delay, arrival on red, coordination quality, detector occupancy, detector volume, phase termination, split failure
- Scheduled aggregation jobs with configurable lookback

### Analytics Functions

| Module | Functions | Description |
|--------|-----------|-------------|
| **detector** | `find_stuck_detectors()`, `analyze_gaps()`, `detector_occupancy()` | Detector health diagnostics |
| **phase** | `find_skipped_phases()`, `split_monitor()`, `phase_termination_summary()` | Phase timing analysis |
| **coordination** | `analyze_offset_drift()`, `pattern_history()`, `coordination_quality()` | Coordination monitoring |
| **preemption** | `analyze_preemptions()`, `preemption_recovery_time()` | Preemption impact analysis |
| **health** | `score_detector_health()`, `score_signal_health()` | Composite health scores (0-100) |

### Watchdog & Alerting
- Silent signal detection (zero events for N consecutive cycles)
- Clock drift detection (future-dated events)
- Poisoned checkpoint auto-recovery
- Notification providers: Email, Slack, Microsoft Teams (plugin system)

### API
- RESTful API (FastAPI) — 126 routes
- GraphQL API (Strawberry, at `/graphql`)
- OpenAPI documentation (auto-generated at `/docs`)
- CORS support (configurable origins)
- Health/readiness probes (`/health`, `/ready`)
- SOAP endpoint (ATSPM 4.x WCF compatibility)

### Web UI
- **Zero build step** — No npm, no webpack, no bundlers
- **Jinja2** — Server-rendered HTML templates
- **Alpine.js** — Lightweight UI state (modals, dropdowns, tabs)
- **Vanilla JavaScript** — Data fetching via JSON APIs
- **ECharts** — Interactive charts (efficient data updates)
- **MapLibre GL JS** — Map visualization
- **Tailwind CSS** — Utility-first styling
- **WebSocket** — Optional real-time updates
- **Air-gapped compatible** — All libraries vendor-downloaded and committed

### Authentication
- Local username/password (bcrypt)
- Azure AD / Entra ID (OIDC)
- Generic OAuth2 (any provider)
- Pluggable provider registry
- Valkey-backed session store (or in-memory fallback)

---

## Module Interface

TSIGMA provides a plugin interface for extending functionality. Modules can:

- Read from TSIGMA's event storage (individual indexed rows, not compressed blobs)
- Add custom metrics and calculations
- Provide additional charts and dashboards
- Integrate external data sources

---

## Tech Stack

| Component | Technology |
|-----------|------------|
| Language | Python 3.14+ |
| Web Framework | FastAPI + uvicorn |
| Database | PostgreSQL 18+ with TimescaleDB (preferred); MS-SQL, Oracle, MySQL supported |
| Migrations | Alembic (async) |
| ORM | SQLAlchemy 2.0 (async) |
| GraphQL | Strawberry |
| Data Processing | Pandas |
| Authentication | Local + OIDC + OAuth2 (pluggable) |
| Session Store | Valkey (or in-memory fallback) |
| Background Jobs | APScheduler |
| HTTP Clients | httpx, aiohttp |
| Notifications | Email (SMTP), Slack, Microsoft Teams |
| File Watching | watchdog |

---

## Deployment Options

### Standard (< 2,000 signals)
Single container deployment:
```bash
docker compose up
```

### Enterprise (2,000+ signals)
Kubernetes with horizontal scaling:
- Multiple API pods
- Distributed ingestion workers
- Valkey for session storage

### Database Encoding

TSIGMA uses **UTF-8** encoding by default, providing universal character support for international deployments.

| Encoding | Character Support | When to Use |
|----------|-------------------|-------------|
| **UTF-8** (Default) | All languages | Recommended for all deployments |
| **LATIN1** | Western European only | Legacy system constraints |

> **Warning**: If your agency may use signal identifiers with characters from Polish, Turkish, Greek, Cyrillic, or Asian languages, you **must** use UTF-8. LATIN1 encoding cannot be changed after database creation.

See [DATABASE_SCHEMA.md](docs/dev/DATABASE_SCHEMA.md#character-encoding) for detailed configuration instructions.

---

## Storage Estimates

*Based on ~9,000 signals (~1.2B rows/day)*

| Storage Type | Duration | Size | Query Speed |
|--------------|----------|------|-------------|
| **Hot (PostgreSQL)** | 3 weeks | ~2.5 TB | Fast (seconds) |
| **Warm (TimescaleDB compression)** | Months | ~50 GB/week | Slower |
| **Cold (Parquet)** | Years | ~50 GB/week | Slowest |

**Comparison with ATSPM:**
| Metric | ATSPM (MS-SQL) | TSIGMA (PostgreSQL) |
|--------|----------------|---------------------|
| Storage/week | ~1.4 TB | ~800 GB |
| Index overhead | ~1 TB | Included |
| Fragmentation | ~40% wasted | Minimal |
| Query time | Hours | Seconds |

**Storage strategy:**
- Keep recent data (3 weeks) in PostgreSQL for fast queries
- Export older partitions to Parquet files for archival
- Drop PostgreSQL partitions after export to reclaim space
- Query cold data with DuckDB/Polars when historical analysis is needed

### Validated at Scale

- **5.27 billion rows** ingested in ~24 hours
- **~61,000 rows/sec** sustained throughput
- **569 GB** uncompressed (12 days of data)
- **TimescaleDB compression** reduces to ~50-60 GB after 7-day policy kicks in

---

## Implementation Status

```
tsigma/
├── app.py              # FastAPI application factory + lifespan
├── main.py             # uvicorn entrypoint
├── config.py           # Pydantic settings (env vars)
├── dependencies.py     # FastAPI dependency injection
├── logging.py          # JSON/console log formatters
├── middleware.py        # RequestID, Timing, Logging, Security headers
├── models/             # SQLAlchemy 2.0 ORM models (15 models + 7 aggregates)
├── database/           # DatabaseFacade, dialect abstraction, TimescaleDB setup
├── auth/               # Authentication (local, OIDC, OAuth2)
│   └── providers/      # Pluggable auth provider registry
├── collection/         # Data collection layer
│   ├── decoders/       # ASC/3, Siemens, Peek, MaxTime/Intelight, CSV, auto-detect
│   ├── methods/        # FTP, HTTP, TCP, UDP, directory watch
│   └── sdk/            # Checkpoint, persistence, decoder resolution
├── validation/         # Post-ingestion event validation
│   ├── validators/     # Layer 1 schema/range (built-in)
│   └── sdk/            # Validation plugin SDK
├── scheduler/          # APScheduler job registry
│   └── jobs/           # Aggregation, compression, cold export, watchdog
├── reports/            # 22 report implementations + plugin registry
│   └── sdk/            # Report SDK (event queries, time bins, occupancy)
├── notifications/      # Email, Slack, Teams (plugin registry)
├── api/
│   ├── v1/             # REST API (126 routes)
│   └── graphql/        # GraphQL API (Strawberry)
├── templates/          # Jinja2 server-rendered UI
├── static/             # Vendor JS/CSS (Alpine, ECharts, MapLibre, Tailwind)
└── tests/              # 50 test files
```

---

## Documentation

### User Documentation

| Document | Description |
|----------|-------------|
| [API_REFERENCE.md](docs/user/API_REFERENCE.md) | REST API endpoint reference |
| [DEPLOYMENT.md](docs/user/DEPLOYMENT.md) | Deployment guide |
| [PRODUCTION_DEPLOYMENT.md](docs/user/PRODUCTION_DEPLOYMENT.md) | Production deployment guide |
| [EVENT_CODES.md](docs/user/EVENT_CODES.md) | NTCIP 1202 event code reference |
| [METRICS.md](docs/user/METRICS.md) | Metric calculations and algorithms |
| [FTP_POLLING_GUIDE.md](docs/user/FTP_POLLING_GUIDE.md) | FTP polling setup |
| [LARGE_SCALE_POLLING.md](docs/user/LARGE_SCALE_POLLING.md) | Large-scale polling tuning |
| [BACKFILL_GUIDE.md](docs/user/BACKFILL_GUIDE.md) | Historical data backfill |
| [TESTING_WITHOUT_DATA.md](docs/user/TESTING_WITHOUT_DATA.md) | Testing without live data |
| [UI_QUICK_START.md](docs/user/UI_QUICK_START.md) | UI quick start |

### Developer Documentation

| Document | Description |
|----------|-------------|
| [ARCHITECTURE.md](docs/dev/ARCHITECTURE.md) | System architecture |
| [API.md](docs/dev/API.md) | REST API design and conventions |
| [AUDITING.md](docs/dev/AUDITING.md) | Audit trail system (triggers, auth logging) |
| [DATABASE.md](docs/dev/DATABASE.md) | Database configuration and setup |
| [DATABASE_SCHEMA.md](docs/dev/DATABASE_SCHEMA.md) | Database schema and indexing |
| [DATABASE_FACADE_PATTERN.md](docs/dev/DATABASE_FACADE_PATTERN.md) | Database facade and dialect abstraction |
| [INGESTION.md](docs/dev/INGESTION.md) | Data collection and ingestion internals |
| [DECODERS.md](docs/dev/DECODERS.md) | Decoder implementation guide |
| [VALIDATION.md](docs/dev/VALIDATION.md) | Post-ingestion validation pipeline and plugin SDK |
| [ANALYTICS_IMPLEMENTATION_GUIDE.md](docs/dev/ANALYTICS_IMPLEMENTATION_GUIDE.md) | Analytics implementation |
| [REPORTS.md](docs/dev/REPORTS.md) | Report plugin development |
| [NOTIFICATIONS.md](docs/dev/NOTIFICATIONS.md) | Notification provider plugins (email, Slack, Teams) |
| [WATCHDOG.md](docs/dev/WATCHDOG.md) | Data quality monitoring and alerting |
| [SECURITY.md](docs/dev/SECURITY.md) | Security architecture and credential encryption |
| [STORAGE.md](docs/dev/STORAGE.md) | File storage backends (filesystem, S3) |
| [HIGH_CONCURRENCY_POLLING.md](docs/dev/HIGH_CONCURRENCY_POLLING.md) | High-concurrency polling architecture |
| [MULTI_DATABASE_AGGREGATES.md](docs/dev/MULTI_DATABASE_AGGREGATES.md) | Multi-database aggregate support |
| [UI.md](docs/dev/UI.md) | Web UI implementation |
| [UI_ARCHITECTURE.md](docs/dev/UI_ARCHITECTURE.md) | UI architecture |
| [CODING_GUIDELINES.md](docs/dev/CODING_GUIDELINES.md) | Coding standards |
| [TESTING.md](docs/dev/TESTING.md) | Testing guide |
| [ATSPM_FEATURE_CATALOG.md](docs/dev/ATSPM_FEATURE_CATALOG.md) | ATSPM feature comparison |

---

## Developed By

**OpenPhase Labs**

TSIGMA is developed by OpenPhase Labs as an open source ATSPM replacement.

---

## Licensing

- **TSIGMA Application:** This software is licensed under the [Mozilla Public License 2.0](LICENSE).
- **Protocol Definitions:** The underlying Protobuf definitions (located in `/proto`) are also licensed under the [Mozilla Public License 2.0](proto/v1/LICENSE) and are subject to the patent grants defined therein.

MPL 2.0 allows DOTs and integrators to use and modify TSIGMA freely. Modifications to MPL-licensed files must be shared; proprietary code that merely links to TSIGMA stays proprietary. This balances open source accessibility with Heritage Grid IP protection.

---

## Contributing

Contributions welcome. See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.
