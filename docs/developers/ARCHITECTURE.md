# TSIGMA - Architecture Design Document

> **Traffic Signal Intelligence: Gathering Metrics & Analytics**
> The Next Generation ATSPM
> Version: 1.0
> Last Updated: April 2026

---

## Table of Contents

1. [Overview](#1-overview)
2. [Design Principles](#2-design-principles)
3. [Technology Stack](#3-technology-stack)
4. [System Architecture](#4-system-architecture)
5. [Project Structure](#5-project-structure)
6. [Layer Rules](#6-layer-rules)
7. [Plugin Architecture](#7-plugin-architecture)
8. [Data Storage Tiers](#8-data-storage-tiers)
9. [Analytics Architecture](#9-analytics-architecture)
10. [File Storage](#10-file-storage)
11. [Background Jobs & Scheduling](#11-background-jobs--scheduling)
12. [Security Architecture](#12-security-architecture)
13. [Configuration Management](#13-configuration-management)
14. [Logging & Observability](#14-logging--observability)
15. [Database Migrations](#15-database-migrations)
16. [Architectural Decisions](#16-architectural-decisions)
17. [Related Documents](#17-related-documents)

---

## 1. Overview

### Purpose

TSIGMA (Traffic Signal Intelligence: Gathering Metrics & Analytics) is a next-generation traffic signal analytics platform that replaces ATSPM. It collects, processes, and visualizes performance data from traffic signal controllers to help transportation agencies optimize signal timing and improve traffic flow.

### Goals for Next Generation

- **Cloud-agnostic**: Runs anywhere (VM, bare-metal, Docker, K8s)
- **Scale-adaptive**: Works for 100 signals or 10,000+
- **Minimal dependencies**: Fewer packages = fewer CVEs
- **Server-rendered UI**: No SPA security headaches
- **Time-series first**: Built for event data from the start
- **Simple deployment**: `docker compose up` for 90% of DOTs

### Target Users

- State DOTs (Departments of Transportation)
- Municipal traffic agencies
- Transportation consultants
- Open source deployment by any agency

### Scale Profiles

| Profile | Signals | Deployment | Scaling |
|---------|---------|------------|---------|
| **Standard** | < 2,000 | Single container | None needed |
| **Enterprise** | 2,000+ | K8s / Cloud Run | Horizontal workers |

---

## 2. Design Principles

### Single Responsibility

Each component has one clear purpose:
- Models define structure
- Database facade handles connection, session, and dialect abstraction
- API routes query directly via SQLAlchemy ORM (no separate repository layer)
- APIs expose functionality

### Configuration over Code

Behavior changes through configuration, not code changes:
- Environment variables
- Config files
- Feature flags

### Plugin Architecture

Extensibility through plugins, not core code modifications:
- Ingestion methods register themselves via decorators
- Protocol decoders register themselves with DecoderRegistry
- Add/remove functionality by adding/removing modules
- Third-party packages can extend TSIGMA without forking
- Zero coupling between plugins (shared contract via base classes)

### Horizontal Scaling by Design

Standard deployment runs everything in one process. Enterprise deployment scales by adding workers -- same codebase, different config.

### No Vendor Lock-in

- PostgreSQL-compatible databases
- Generic OIDC authentication
- Standard protocols (REST, GraphQL)
- Container-based deployment

---

## 3. Technology Stack

### Backend

| Component | Choice | Rationale |
|-----------|--------|-----------|
| **Language** | Python 3.14+ | Free-threaded (PEP 703), no GIL, data analysis ecosystem |
| **Web Framework** | FastAPI | Async, OpenAPI docs, Pydantic integration |
| **ORM** | SQLAlchemy 2.0 (async) | Mature, flexible, async support, multi-DB |
| **DB Driver (Primary)** | asyncpg | High performance PostgreSQL |
| **DB Driver (MS-SQL)** | aioodbc | Enterprise database support |
| **DB Driver (Oracle)** | oracledb (async) | Enterprise database support |
| **DB Driver (MySQL)** | aiomysql | Enterprise database support |
| **GraphQL** | Strawberry | Modern, type-safe, async |
| **Data Processing** | pandas | Widely supported, DataFrame integration |
| **Background Jobs** | APScheduler | Lightweight, no broker needed |
| **Validation** | Pydantic v2 | Fast, type-safe |

#### Python 3.14 Free-Threaded Mode

Python 3.14 includes **PEP 703** (optional GIL removal), enabling true parallel execution of Python threads. This is particularly valuable for TSIGMA's data processing workload:

**Benefits for TSIGMA:**
- **Concurrent event ingestion**: Process multiple controller files in parallel without multiprocessing overhead
- **Faster view refreshes**: Materialized view computation can leverage multiple cores
- **Scalable background jobs**: APScheduler can run jobs in parallel threads efficiently

**Deployment:**
```bash
# Enable free-threaded mode (Python 3.14+)
PYTHON_GIL=0 python -m tsigma

# Or build Python with --disable-gil
python3.14t  # "t" suffix indicates free-threaded build
```

**Compatibility:**
- Free-threaded mode is **opt-in** in Python 3.14
- TSIGMA works in both standard and free-threaded modes
- For maximum performance on multi-core systems, use free-threaded mode
- All dependencies (FastAPI, SQLAlchemy, Polars) support free-threaded Python

### Database

| Component | Choice | Rationale |
|-----------|--------|-----------|
| **Preferred** | PostgreSQL 18+ w/TimescaleDB | Time-series optimization, compression, best performance |
| **Partitioning** | pg_partman | Automatic partition management |
| **Scheduling** | pg_cron (optional) | DB-level job scheduling |
| **Compatible** | AlloyDB | Cloud-managed PostgreSQL (GCP) |
| **On-Prem Supported** | MS-SQL Server 2019+ | Enterprise compatibility |
| **On-Prem Supported** | Oracle 19c+ | Enterprise compatibility |
| **On-Prem Supported** | MySQL 8.0+ | Enterprise compatibility |
| **Dev only** | SQLite | Local development convenience |

**Deployment Recommendations:**
- **SaaS / Cloud**: PostgreSQL 18+ with TimescaleDB (required for Warm tier compression)
- **On-Prem (preferred)**: PostgreSQL 18+ with TimescaleDB (full feature support)
- **On-Prem (legacy DB)**: MS-SQL, Oracle, or MySQL (Hot -> Cold tiers only, no Warm tier)

**Database Abstraction:**

TSIGMA uses a database facade pattern (`tsigma/database/db.py`) with a separated `DialectHelper` class to handle dialect-specific operations:

| Operation | PostgreSQL | MS-SQL | Oracle | MySQL |
|-----------|------------|--------|--------|-------|
| **Time bucketing** | `time_bucket()` | `DATEADD()` + `DATEDIFF()` | `TRUNC()` | `DATE_FORMAT()` |
| **Materialized views** | `REFRESH MATERIALIZED VIEW CONCURRENTLY` | Indexed views (auto-refresh) | `DBMS_MVIEW.REFRESH()` | Not supported (use tables) |
| **Partitioning** | Native partitioning + pg_partman | Native partitioning | Native partitioning | Native partitioning |
| **JSON fields** | `jsonb` | `nvarchar(max)` (JSON functions) | `CLOB` or `JSON` type | `json` type |
| **Array fields** | Native arrays | Delimited strings or JSON | Delimited strings or JSON | JSON |
| **Audit user context** | `SET LOCAL app.current_user` | `sp_set_session_context` | `DBMS_SESSION.SET_CONTEXT` | `SET @app_user` |

All SQL queries go through SQLAlchemy 2.0, which provides database-agnostic query construction. The `DialectHelper` handles dialect-specific edge cases not covered by SQLAlchemy (time bucketing, audit triggers, lookback predicates, delete windows).

### Frontend

| Component | Choice | Rationale |
|-----------|--------|-----------|
| **Templating** | Jinja2 | Server-rendered HTML, no Node.js |
| **UI State** | Alpine.js (vendor downloaded) | Lightweight reactivity (modals, dropdowns, tabs) |
| **Data Fetching** | Vanilla JavaScript | Native browser, JSON API calls |
| **Charts** | ECharts (vendor downloaded) | Interactive visualizations, efficient data updates |
| **Maps** | MapLibre GL JS (vendor downloaded) | Vector tiles, handles 8,000+ markers |
| **Styling** | Tailwind CSS (vendor downloaded) | Utility-first, zero build step |
| **Real-time** | WebSocket (native API) | Optional live dashboard updates |
| **Build System** | None | All libraries vendor-downloaded, air-gapped compatible |

### Infrastructure

| Component | Choice | Rationale |
|-----------|--------|-----------|
| **Session Storage** | Memory / Valkey | Memory for single pod, Valkey for multi-pod |
| **Logging** | structlog | Structured, JSON, Uvicorn-integrated |
| **Config** | Pydantic Settings | Type-safe, dotenv support |
| **Credential Encryption** | Fernet (cryptography) | Symmetric encryption for credentials at rest |

---

## 4. System Architecture

### Deployment Model

A single `tsigma` process runs all components by default. Components are enabled via environment variables, allowing the same image to serve any deployment size.

```env
TSIGMA_ENABLE_API=true        # REST API, GraphQL, Web UI
TSIGMA_ENABLE_COLLECTOR=true  # Controller polling and ingestion
TSIGMA_ENABLE_SCHEDULER=true  # View refresh and watchdog jobs
```

### Small Deployment (< 2,000 signals)

```
┌─────────────────────────────────────────────────────────────┐
│                  tsigma (Single Container)                  │
│                                                             │
│   TSIGMA_ENABLE_API=true                                    │
│   TSIGMA_ENABLE_COLLECTOR=true                              │
│   TSIGMA_ENABLE_SCHEDULER=true                              │
│                                                             │
│   ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────────┐    │
│   │ Web UI  │  │  REST   │  │ GraphQL │  │  Collector  │    │
│   │ (Jinja2)│  │   API   │  │   API   │  │  Scheduler  │    │
│   └─────────┘  └─────────┘  └─────────┘  └─────────────┘    │
└─────────────────────────────────────────────────────────────┘
                          │
                          ▼
                 ┌──────────────┐
                 │   Database   │
                 └──────────────┘
```

### Large Deployment (2,000+ signals, self-hosted)

```
┌──────────────────────────────────────────────────────────────┐
│                        Load Balancer                         │
└──────────────────────────────────────────────────────────────┘
     │              │              │
     ▼              ▼              ▼
┌──────────┐ ┌──────────┐ ┌──────────────┐
│  API #1  │ │  API #2  │ │  Collector   │
│  :8000   │ │  :8000   │ │  Scheduler   │
└──────────┘ └──────────┘ └──────────────┘
     │              │              │
     └──────────────┼──────────────┘
                    │
                    ▼
           ┌──────────────┐
           │   Database   │
           └──────────────┘
```

Only one instance should run the scheduler (to avoid duplicate view refreshes).

> **Database:** TSIGMA is database-agnostic via SQLAlchemy. Supported: PostgreSQL (+ TimescaleDB), MS-SQL, Oracle, MySQL. **PostgreSQL + TimescaleDB is the reference architecture** -- it enables continuous aggregates, hypertable compression, and time-series optimizations that other backends lack.

### Lifespan Startup Sequence

The FastAPI lifespan manager (`tsigma/app.py`) initializes components in order:

1. Configure structured logging
2. Create `DatabaseFacade` and connect
3. Initialize session store (Valkey if configured, in-memory fallback)
4. Seed default admin user and system settings
5. Initialize and mount the active auth provider
6. Initialize notification providers
7. Start `CollectorService` (if enabled)
8. Start `SchedulerService` (if enabled)
9. Start `ValidationService` (if enabled)

Shutdown reverses the order: validation stops first, then scheduler, then collector, then Valkey, then database.

---

## 5. Project Structure

### Flat Package Layout

TSIGMA is a single Python package. All components (API, collector, scheduler, validation) live in the same package and are enabled via environment variables.

All seven plugin subsystems (ingestion methods, decoders, reports, notifications, auth providers, validators, storage backends) use the same self-registering plugin pattern: an ABC base class, a registry with `@Registry.register(name)` decorator, and auto-discovery via `__init__.py` imports.

```
tsigma/
├── app.py                        # FastAPI app factory + lifespan manager
├── main.py                       # uvicorn entrypoint
├── config.py                     # Pydantic settings (all env vars, TSIGMA_ prefix)
├── config_resolver.py            # Configuration resolution logic
├── crypto.py                     # Fernet credential encryption / redaction
├── dependencies.py               # FastAPI dependency injection (get_session, get_audited_session)
├── logging.py                    # JSON/console log formatters
├── middleware.py                  # RequestID, Timing, Logging, Security headers
├── settings_service.py           # Default system settings seeding
│
├── models/                       # SQLAlchemy 2.0 ORM models (one model per file)
│   ├── base.py                   # DeclarativeBase, TimestampMixin
│   ├── signal.py                 # Signal, SignalAudit
│   ├── event.py                  # ControllerEventLog (+ validation_metadata JSONB)
│   ├── approach.py               # Approach
│   ├── detector.py               # Detector
│   ├── checkpoint.py             # PollingCheckpoint (composite PK: signal_id + method)
│   ├── reference.py              # EventCodeDefinition, ControllerType, DirectionType, etc.
│   ├── aggregates.py             # 10 aggregate models (hourly/15-min rollups + PCD)
│   ├── audit.py                  # ApproachAudit, DetectorAudit, AuthAuditLog
│   ├── route.py                  # Route, RouteSignal, RoutePhase, RouteDistance
│   ├── signal_plan.py            # SignalPlan
│   └── system_setting.py         # SystemSetting (key-value app config)
│
├── database/                     # Database facade + dialect abstraction
│   ├── db.py                     # DatabaseFacade, DialectHelper (PostgreSQL, MS-SQL, Oracle, MySQL)
│   └── init.py                   # First-run setup (tables, TimescaleDB hypertables, indexes)
│
├── collection/                   # Data collection (enabled via TSIGMA_ENABLE_COLLECTOR)
│   ├── registry.py               # BaseIngestionMethod, PollingIngestionMethod, etc.
│   ├── service.py                # CollectorService — orchestrates polling methods
│   ├── sdk/                      # Plugin SDK (checkpoint, persistence, decoder resolution)
│   │
│   ├── methods/                  # Ingestion method plugins (self-registering)
│   │   ├── ftp_pull.py           # FTP/FTPS/SFTP poller — registers as "ftp_pull"
│   │   ├── http_pull.py          # HTTP poller (Intelight MaxTime XML) — registers as "http_pull"
│   │   ├── tcp_server.py         # TCP listener (push mode) — registers as "tcp_server"
│   │   ├── udp_server.py         # UDP listener (push mode) — registers as "udp_server"
│   │   └── directory_watch.py    # Directory watcher (event-driven) — registers as "directory_watch"
│   │
│   └── decoders/                 # Protocol decoders (separate plugin registry)
│       ├── base.py               # DecodedEvent, BaseDecoder, DecoderRegistry
│       ├── sdk/                  # Decoder SDK (attribute names, timestamp parsing, delimiters)
│       ├── asc3.py               # Econolite ASC/3 binary — registers as "asc3"
│       ├── siemens.py            # Siemens SEPAC text — registers as "siemens"
│       ├── peek.py               # Peek/McCain ATC binary — registers as "peek"
│       ├── maxtime.py            # Intelight MaxTime XML + binary — registers as "maxtime"
│       ├── csv_decoder.py        # Generic CSV (configurable columns) — registers as "csv"
│       └── auto.py               # Auto-detect decoder — registers as "auto"
│
├── validation/                   # Post-ingestion event validation
│   ├── registry.py               # BaseValidator, ValidationLevel, ValidationRegistry
│   ├── service.py                # ValidationService — async batch processing
│   ├── sdk/                      # Validation SDK (result builders, status constants)
│   └── validators/               # Built-in validators
│       └── schema_range.py       # Layer 1: NTCIP 1202 schema/range — registers as "schema_range"
│
├── auth/                         # Authentication (pluggable providers)
│   ├── registry.py               # AuthProviderRegistry
│   ├── router.py                 # Auth endpoints (login, logout, me, provider)
│   ├── dependencies.py           # require_access() dependency
│   ├── models.py                 # AuthUser
│   ├── sessions.py               # ValkeySessionStore, InMemorySessionStore, CSRF nonces
│   ├── seed.py                   # Default admin user seeding
│   └── providers/                # Auth provider plugins
│       ├── local.py              # Username/password (bcrypt) — registers as "local"
│       ├── oidc.py               # Azure AD / Entra ID — registers as "oidc"
│       └── oauth2.py             # Generic OAuth2 — registers as "oauth2"
│
├── notifications/                # Notification providers (plugin registry)
│   ├── registry.py               # BaseNotificationProvider, NotificationRegistry
│   └── providers/
│       ├── email.py              # SMTP email — registers as "email"
│       ├── slack.py              # Slack webhook — registers as "slack"
│       └── teams.py              # MS Teams webhook — registers as "teams"
│
├── reports/                      # Report plugins (22 implementations)
│   ├── registry.py               # BaseReport, ReportRegistry
│   ├── sdk/                      # Report SDK (event queries, time bins, occupancy, cycles)
│   ├── purdue_diagram.py         # PCD — registers as "purdue_diagram"
│   ├── split_monitor.py          # Split monitor — registers as "split_monitor"
│   └── ...                       # 20 more report implementations
│
├── scheduler/                    # Background job system
│   ├── registry.py               # JobRegistry
│   ├── service.py                # SchedulerService (APScheduler)
│   └── jobs/                     # Job implementations
│       ├── aggregate.py          # Three-tier aggregation pipeline
│       ├── compress_chunks.py    # TimescaleDB chunk compression
│       ├── export_cold.py        # Parquet cold-tier export
│       ├── refresh_views.py      # Materialized view refresh
│       ├── signal_plan.py        # Signal plan ingestion
│       └── watchdog.py           # Silent signal / clock drift detection
│
├── storage/                      # File storage backends
│   ├── base.py                   # BaseStorageBackend
│   ├── factory.py                # StorageFactory
│   ├── filesystem.py             # Local filesystem — registers as "filesystem"
│   └── s3.py                     # S3-compatible — registers as "s3"
│
├── api/                          # FastAPI REST + GraphQL API
│   ├── ui.py                     # Server-rendered UI routes
│   ├── graphql/                  # GraphQL API (Strawberry)
│   │   ├── schema.py             # Query types and resolvers
│   │   └── types.py              # Strawberry type definitions
│   └── v1/                       # REST API v1
│       ├── signals.py            # Signal CRUD + audit trail
│       ├── approaches.py         # Approach CRUD
│       ├── detectors.py          # Detector CRUD
│       ├── collection.py         # SOAP/REST poll + checkpoint management
│       ├── reference.py          # 6 reference tables CRUD
│       ├── regions.py            # Region CRUD (hierarchical)
│       ├── corridors.py          # Corridor CRUD
│       ├── routes.py             # Route/RouteSignal/RoutePhase/RouteDistance
│       ├── jurisdictions.py      # Jurisdiction CRUD
│       ├── reports.py            # Report execution + export
│       ├── settings.py           # System settings CRUD
│       ├── schemas.py            # Pydantic v2 request/response schemas
│       ├── analytics_schemas.py  # Analytics-specific schemas
│       ├── crud_factory.py       # Generic CRUD router factory
│       └── analytics/            # Analytics endpoints
│           ├── detectors.py      # Stuck detectors, gaps, occupancy
│           ├── phases.py         # Skipped phases, split monitor
│           ├── coordination.py   # Offset drift, pattern history
│           ├── preemption.py     # Preemption analysis
│           └── health.py         # Signal/detector health scores
│
├── templates/                    # Jinja2 server-rendered HTML
├── static/                       # Vendor JS/CSS (Alpine, ECharts, MapLibre, Tailwind)
│
└── alembic/                      # Database migrations
    ├── env.py
    └── versions/
```

### Package Dependencies

```
              \             /
               \           /
                ───────────
                     │
             ┌───────┴──────────┐
             │    database/      │
             │  (facade pattern) │
             └───────┬──────────┘
                     │
                 models/

           Ingestion Architecture (Plugin-based)
           =====================================

                  collector/
                      │
          ┌───────────┼───────────┐
          │           │           │
      registry/    base.py   service.py
                      │
          ┌───────────┴───────────┐
          │                       │
    methods/                  decoders/
    (plugins)                 (plugins)
       │                          │
   ┌───┴────┐               ┌────┴────┐
   │        │               │         │
 ftp_pull  http_pull      asc3     siemens
 tcp_srv   udp_server     peek     maxtime
 dir_watch ...            csv       auto
```

---

## 6. Layer Rules

| Layer | Responsibility | Can import from |
|-------|---------------|-----------------|
| `models/` | Table definitions only | Nothing in tsigma |
| `database/` | Connection, session, dialect helpers | `models/` |
| `collection/` | Ingestion orchestration (registry, service, SDK) | `models/`, `database/` |
| `collection/methods/` | Ingestion method plugins (self-registering) | `collection/` (registry, SDK), `models/` |
| `collection/decoders/` | Protocol decoder plugins (self-registering, separate registry) | `models/` |
| `validation/` | Post-ingestion event validation (registry, service, SDK) | `models/`, `database/` |
| `validation/validators/` | Validator plugins (self-registering) | `validation/` (registry, SDK), `models/` |
| `reports/` | Report plugins (self-registering) | `models/`, `database/` |
| `notifications/` | Notification provider plugins | `config` |
| `auth/` | Auth provider plugins | `models/`, `database/` |
| `scheduler/` | Background job orchestration | `models/`, `database/` |
| `api/` | Routes and responses (direct SQLAlchemy queries) | `models/`, `database/` |

**No repository layer.** API routes query SQLAlchemy models directly using sessions from `get_session()`. The `DatabaseFacade` handles connection pooling, dialect abstraction, and session lifecycle.

---

## 7. Plugin Architecture

### Unified Pattern

TSIGMA uses the same **self-registering plugin pattern** across all seven subsystems:

| Subsystem | Registry | Base Class | Plugins |
|-----------|----------|------------|---------|
| Ingestion methods | `IngestionMethodRegistry` | `BaseIngestionMethod` | ftp_pull, http_pull, tcp_server, udp_server, directory_watch |
| Decoders | `DecoderRegistry` | `BaseDecoder` | asc3, siemens, peek, maxtime, csv, auto |
| Validators | `ValidationRegistry` | `BaseValidator` | schema_range (Layer 1) |
| Reports | `ReportRegistry` | `BaseReport` | 22 report implementations |
| Notifications | `NotificationRegistry` | `BaseNotificationProvider` | email, slack, teams |
| Auth providers | `AuthProviderRegistry` | `BaseAuthProvider` | local, oidc, oauth2 |
| Storage backends | `StorageFactory` | `BaseStorageBackend` | filesystem, s3 |

Every registry follows the same pattern:

```python
class SomeRegistry:
    _items: dict[str, type[BaseClass]] = {}

    @classmethod
    def register(cls, name: str):
        def wrapper(item_class):
            cls._items[name] = item_class
            return item_class
        return wrapper

    @classmethod
    def get(cls, name: str) -> type[BaseClass]:
        if name not in cls._items:
            raise ValueError(f"Unknown: {name}")
        return cls._items[name]

    @classmethod
    def list_available(cls) -> list[str]:
        return list(cls._items.keys())
```

### Auto-Discovery

Plugins are discovered at import time. Each subsystem's `__init__.py` imports all modules in its directory, triggering `@register` decorators:

```python
# tsigma/collection/decoders/__init__.py
from pathlib import Path

decoders_dir = Path(__file__).parent
for module_file in decoders_dir.glob("*.py"):
    if module_file.stem not in ("__init__", "base"):
        __import__(f"tsigma.collection.decoders.{module_file.stem}")
```

The application startup (`app.py`) also triggers auto-discovery via bare imports:

```python
import tsigma.auth.providers       # noqa: F401
import tsigma.notifications        # noqa: F401
import tsigma.validation           # noqa: F401
```

### Ingestion Method Types

Three execution modes, each with its own base class in `tsigma/collection/registry.py`:

```python
class ExecutionMode(str, Enum):
    POLLING = "polling"           # Scheduled poll cycles (FTP, HTTP)
    LISTENER = "listener"         # Long-lived servers (TCP, UDP)
    EVENT_DRIVEN = "event_driven" # External triggers (directory watch)

class BaseIngestionMethod(ABC):
    name: ClassVar[str]
    execution_mode: ClassVar[ExecutionMode]

    @abstractmethod
    async def health_check(self) -> bool: ...

class PollingIngestionMethod(BaseIngestionMethod):
    execution_mode = ExecutionMode.POLLING

    @abstractmethod
    async def poll_once(self, signal_id: str, config: dict, session_factory) -> None: ...

class ListenerIngestionMethod(BaseIngestionMethod):
    execution_mode = ExecutionMode.LISTENER

    @abstractmethod
    async def start(self, config: dict, session_factory) -> None: ...
    @abstractmethod
    async def stop(self) -> None: ...

class EventDrivenIngestionMethod(BaseIngestionMethod):
    execution_mode = ExecutionMode.EVENT_DRIVEN

    @abstractmethod
    async def start(self, config: dict, session_factory) -> None: ...
    @abstractmethod
    async def stop(self) -> None: ...
```

### Per-Signal Configuration

Each signal stores its own collection config in the `signal_metadata` JSONB column:

```json
{
  "collection": {
    "method": "ftp_pull",
    "protocol": "ftps",
    "username": "admin",
    "password": "***",
    "remote_dir": "/logs",
    "decoder": "asc3"
  }
}
```

This allows different signals to use different methods, protocols, and decoders.

### CollectorService

The `CollectorService` orchestrates polling methods via the scheduler:

```python
# tsigma/collection/service.py

class CollectorService:
    def __init__(self, session_factory, settings):
        self._session_factory = session_factory
        self._settings = settings
        self._semaphore = asyncio.Semaphore(settings.collector_max_concurrent)

    async def start(self):
        # Instantiate all registered polling methods
        for name, cls in IngestionMethodRegistry.get_polling_methods().items():
            self._polling_instances[name] = cls()

        # Register each as a scheduler job
        for method_name in self._polling_instances:
            JobRegistry.register_func(
                name=f"poll_cycle_{method_name}",
                func=partial(self._run_poll_cycle, method_name),
                trigger="interval",
                seconds=self._settings.collector_poll_interval,
            )

    async def _run_poll_cycle(self, method_name):
        # Query enabled signals configured for this method
        # Fan out to semaphore-bounded _process_signal() calls
        ...
```

### Ingestion SDK

Plugins use shared helpers from `tsigma/collection/sdk/`:

- `load_checkpoint()` -- load per-signal polling checkpoint
- `record_error()` -- record poll error without advancing checkpoint
- `persist_events()` -- idempotent INSERT ON CONFLICT DO NOTHING
- `persist_events_with_drift_check()` -- same + clock drift detection + notifications
- `resolve_decoder_by_name()` / `resolve_decoder_by_extension()` -- decoder lookup

### Report SDK

Report plugins use shared helpers from `tsigma/reports/sdk/`:

- **Event queries**: `fetch_events()`, `fetch_events_split()` -- query `controller_event_log` by signal_id, event_time range, event codes; return DataFrames (no session parameter)
- **Cycle data**: `fetch_cycle_boundaries()`, `fetch_cycle_arrivals()`, `fetch_cycle_summary()` -- return DataFrames (no session parameter)
- **Time bins**: `parse_time()`, `bin_timestamp()`, `bin_index()`, `total_bins()`
- **Config lookups**: `load_channel_to_phase()`, `load_channel_to_approach()`, `load_channels_for_phase()`
- **Signal plans**: `fetch_plans()`, `plan_at()`, `programmed_split()`
- **Occupancy**: `calculate_occupancy()`, `accumulate_on_time()`, `bin_occupancy_pct()`
- **Aggregates**: `safe_avg()`, `safe_min()`, `safe_max()`, `pct()`, `percentile_from_sorted()`
- **Event constants**: `EVENT_PHASE_GREEN`, `EVENT_YELLOW_CLEARANCE`, `EVENT_DETECTOR_ON`, etc.

### Decoder Architecture

Each decoder is a self-contained module registered with `DecoderRegistry`:

```python
# tsigma/collection/decoders/base.py

@dataclass
class DecodedEvent:
    timestamp: datetime
    event_code: int
    event_param: int

class BaseDecoder(ABC):
    name: ClassVar[str]
    extensions: ClassVar[list[str]]
    description: ClassVar[str]

    @abstractmethod
    def decode_bytes(self, data: bytes) -> list[DecodedEvent]: ...

    @classmethod
    @abstractmethod
    def can_decode(cls, data: bytes) -> bool: ...
```

### Validation Pipeline

Three-layer validation architecture in `tsigma/validation/registry.py`:

```python
class ValidationLevel(str, Enum):
    LAYER1 = "layer1"  # Schema/range checks (NTCIP 1202)
    LAYER2 = "layer2"  # Temporal/anomaly detection (requires SLM)
    LAYER3 = "layer3"  # Cross-signal correlation (requires SLM + corridors)

class BaseValidator(ABC):
    name: ClassVar[str]
    level: ClassVar[ValidationLevel]
    description: ClassVar[str]

    @abstractmethod
    async def validate_events(
        self, events: list[dict], signal_id: str, session_factory
    ) -> list[dict]: ...
```

Each layer is independently toggleable:

```env
TSIGMA_VALIDATION_ENABLED=true
TSIGMA_VALIDATION_LAYER1_ENABLED=true   # Schema/range (always recommended)
TSIGMA_VALIDATION_LAYER2_ENABLED=false  # Temporal/anomaly (requires SLM)
TSIGMA_VALIDATION_LAYER3_ENABLED=false  # Cross-signal (requires SLM + corridors)
```

### Adding a New Plugin

The process is identical for all subsystems:

1. Create a new module in the appropriate directory
2. Subclass the base class
3. Decorate with `@Registry.register("name")`
4. Implement required abstract methods
5. Import in the subsystem's `__init__.py` for auto-discovery
6. No changes needed to service code -- auto-discovered on import

---

## 8. Data Storage Tiers

TSIGMA uses a three-tier data storage model for event data. The tier configuration depends on deployment mode.

### Tier Overview

```
          On-Prem (PG + TimescaleDB)              On-Prem (other DBs)
┌──────────────────────────────┐    ┌──────────────────────────────┐
│  HOT (uncompressed)          │    │  HOT (uncompressed)          │
│  TimescaleDB chunks          │    │  Native DB tables            │
│  Age: 0 – warm_after         │    │  Age: 0 – cold_after         │
│  Fast inserts + queries      │    │  Fast inserts + queries      │
├──────────────────────────────┤    ├──────────────────────────────┤
│  WARM (compressed)           │    │  (no warm tier)              │
│  TimescaleDB compression     │    │                              │
│  Age: warm_after – cold_after│    │                              │
│  10-15x compression ratio    │    │                              │
│  Transparent SQL queries     │    │                              │
├──────────────────────────────┤    ├──────────────────────────────┤
│  COLD (Parquet)              │    │  COLD (Parquet)              │
│  Queryable via parquet_fdw   │    │  External tools only         │
│  Age: cold_after – retention │    │  (DuckDB, Polars, etc.)      │
│  Local, NAS, or S3 endpoint  │    │  Age: cold_after – retention │
└──────────────────────────────┘    └──────────────────────────────┘
```

### Tier Details

| Tier | Storage | Compression | Query Speed | Data Age |
|------|---------|-------------|-------------|----------|
| **Hot** | Database (uncompressed) | None | Fastest | 0 -- `warm_after` |
| **Warm** | TimescaleDB (compressed) | 10-15x | Fast (transparent decompression) | `warm_after` -- `cold_after` |
| **Cold** | Parquet files | 15-20x | Seconds (FDW) or external tools | `cold_after` -- `retention` |

### Database Support Matrix

| Database | Hot -> Warm | Hot/Warm -> Cold (export) | Cold queryable from DB? | Deployment |
|----------|-----------|--------------------------|-------------------------|------------|
| **PostgreSQL 18+ w/TimescaleDB** | Compression policy | Yes | Yes (`parquet_fdw` / `duckdb_fdw`) | **Preferred** |
| **PostgreSQL 18+ (plain)** | -- | Yes | Yes (`parquet_fdw` / `duckdb_fdw`) | **Supported** |
| **MS-SQL Server 2019+** | -- | Yes | No (external tools only) | **Supported** |
| **Oracle 19c+** | -- | Yes | No (external tools only) | **Supported** |
| **MySQL 8.0+** | -- | Yes | No (external tools only) | **Supported** |
| **SQLite** | -- | -- | -- | Dev only |

> The Warm tier (TimescaleDB compression) only applies to PostgreSQL + TimescaleDB. All other databases skip directly from Hot to Cold. The Cold export itself is database-agnostic -- TSIGMA reads rows via SQLAlchemy and writes Parquet files. The difference is whether that cold data remains queryable from within the database (PostgreSQL via FDW) or requires external tools (MS-SQL, Oracle, MySQL).
>
> **On-Prem deployments** can choose any supported database based on existing infrastructure. Organizations already invested in MS-SQL, Oracle, or MySQL can deploy TSIGMA without needing to introduce PostgreSQL. However, PostgreSQL 18+ with TimescaleDB is **strongly recommended** for optimal performance and the full three-tier storage model.

### Unified Cold View (PostgreSQL)

A unified view makes all tiers transparently queryable:

```sql
CREATE VIEW controller_event_log_all AS
SELECT * FROM controller_event_log         -- hot + warm (TimescaleDB)
UNION ALL
SELECT * FROM controller_event_log_cold;   -- cold (Parquet via FDW)
```

### Configuration

```env
# Warm tier — TimescaleDB compression (age before compressing)
TSIGMA_STORAGE_WARM_AFTER=7 days          # Agency-configurable

# Cold tier — Parquet export (On-Prem only)
TSIGMA_STORAGE_COLD_ENABLED=true
TSIGMA_STORAGE_COLD_AFTER=6 months        # Export to Parquet after this age
TSIGMA_STORAGE_COLD_PATH=/var/lib/tsigma/cold

# Retention — drop data entirely
TSIGMA_STORAGE_RETENTION=2 years
```

### Compression Settings (Warm Tier)

TimescaleDB compression is configured with segment-by and order-by for optimal query performance:

```sql
ALTER TABLE controller_event_log SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'signal_id',
    timescaledb.compress_orderby = 'event_time DESC'
);

-- Interval set from TSIGMA_STORAGE_WARM_AFTER (default: 7 days)
SELECT add_compression_policy('controller_event_log', INTERVAL '7 days');
```

The `compress_segmentby = 'signal_id'` setting allows TimescaleDB to decompress only the segments for a queried signal rather than the entire chunk.

### Disk-Based Compression Trigger (TimescaleDB Only)

In addition to time-based compression, TSIGMA supports a disk-pressure trigger that compresses the oldest hot chunks when uncompressed data exceeds a configurable threshold:

```
Compress when:
  chunk age > TSIGMA_STORAGE_WARM_AFTER          (time-based, floor)
  OR
  hot tier size > TSIGMA_STORAGE_WARM_MAX_DISK    (disk-based, pressure)
```

When `TSIGMA_STORAGE_WARM_MAX_DISK` is not set, only the time-based policy applies.

### Storage Estimates (GDOT Scale -- 9,000 signals)

| Tier | Data Age | Size per Week | Cumulative (1 Year) |
|------|----------|---------------|---------------------|
| **Hot** | 0-3 days | ~400 GB | ~400 GB (rolling) |
| **Warm** | 3 days -- 6 months | ~65-85 GB/week | ~1.7-2.2 TB |
| **Cold** | 6 months+ | ~50-60 GB/week (Parquet) | ~1.3-1.6 TB |
| **Total** | 1 year | -- | ~3.5-4.2 TB |

Without tiered storage (uncompressed only): **~52 TB/year**

---

## 9. Analytics Architecture

### Three-Tier Pre-Computation

TSIGMA uses a **three-tier analytics architecture** to balance query performance, data freshness, and system efficiency:

1. **TimescaleDB Continuous Aggregates** (50%) -- Automatic time-series rollups
2. **API Endpoints** (30%) -- On-demand queries for custom parameters
3. **APScheduler Jobs** (20%) -- Application logic, watchdog, alerts

### Tier 1: TimescaleDB Continuous Aggregates (Database Level)

**Purpose**: Pre-compute heavy time-series aggregations at database level.

**Advantages**:
- Incremental updates (only processes new data since last refresh)
- Automatic maintenance (no APScheduler job needed for refresh)
- Real-time freshness (15 min lag configurable)
- 10-100x faster queries (dashboard reads from aggregate, not raw events)
- No locking during refresh (TimescaleDB CONCURRENTLY option)

**Core event table** (`controller_event_log`):

| Column | Type | Description |
|--------|------|-------------|
| `signal_id` | TEXT | Traffic signal identifier |
| `event_time` | TIMESTAMPTZ | Event timestamp |
| `event_code` | INTEGER | Indiana hi-res event code (0-255+) |
| `event_param` | INTEGER | Event parameter (phase, detector channel, etc.) |

**Continuous Aggregates** (10 models):

| Aggregate | Granularity | Source |
|-----------|-------------|--------|
| `detector_volume_hourly` | 1 hour | controller_event_log |
| `detector_occupancy_hourly` | 1 hour | controller_event_log |
| `split_failure_hourly` | 1 hour | controller_event_log |
| `coordination_quality_hourly` | 1 hour | controller_event_log |
| `approach_delay_15min` | 15 min | controller_event_log |
| `arrival_on_red_hourly` | 1 hour | controller_event_log |
| `phase_termination_hourly` | 1 hour | controller_event_log |
| `cycle_boundary` | Per cycle | controller_event_log |
| `cycle_detector_arrival` | Per activation | controller_event_log |
| `cycle_summary_15min` | 15 min | cycle_boundary + cycle_detector_arrival |

**PCD Aggregates** (`cycle_boundary`, `cycle_detector_arrival`, `cycle_summary_15min`):

These three models support the Purdue Coordination Diagram and related analyses:

- `cycle_boundary` -- one row per signal/phase/cycle: green_start, yellow_start, red_start, cycle_end, durations, termination_type
- `cycle_detector_arrival` -- one row per detector activation during a cycle: arrival_time, phase_state (green/yellow/red), time_in_cycle_seconds
- `cycle_summary_15min` -- 15-minute binned roll-up: total_cycles, avg_cycle_length, total_arrivals, arrivals_on_green/yellow/red, arrival_on_green_pct

**Example**:
```sql
-- Hourly detector volume (auto-maintained by TimescaleDB)
CREATE MATERIALIZED VIEW detector_volume_hourly
WITH (timescaledb.continuous) AS
SELECT
    signal_id,
    event_param AS detector_channel,
    time_bucket('1 hour', event_time) AS hour_start,
    COUNT(*) FILTER (WHERE event_code = 82) AS volume,
    COUNT(*) FILTER (WHERE event_code = 81) AS activations
FROM controller_event_log
WHERE event_code IN (81, 82)
GROUP BY signal_id, event_param, time_bucket('1 hour', event_time);

-- Auto-refresh policy (refresh last 2 hours every 15 minutes)
SELECT add_continuous_aggregate_policy('detector_volume_hourly',
    start_offset => INTERVAL '2 hours',
    end_offset => INTERVAL '0 minutes',
    schedule_interval => INTERVAL '15 minutes'
);
```

### Tier 2: API Endpoints (Application Level)

**Purpose**: On-demand queries for user-specific parameters and fast analytics.

**Use for**:
- PCD (Purdue Coordination Diagram) -- user selects custom time window
- Gap analysis -- user selects specific detector and date range
- Preemption analysis -- infrequent events, fast query on raw events
- Custom reports -- user-defined parameters

**Query Strategy**:
- Read from continuous aggregates when possible (pre-aggregated data)
- Query raw `controller_event_log` for custom time windows
- Combine both (aggregate for baseline, raw events for drill-down)

```python
# Fast: Read from continuous aggregate
@router.get("/volume/hourly")
async def get_hourly_volume(signal_id: str, start: datetime, end: datetime):
    result = await session.execute(
        select(DetectorVolumeHourly).where(
            DetectorVolumeHourly.signal_id == signal_id,
            DetectorVolumeHourly.hour_start.between(start, end),
        )
    )
    return result.scalars().all()  # fast pre-aggregated read

# On-demand: Query raw events for PCD
@router.get("/pcd")
async def get_pcd_data(signal_id: str, phase: int, start: datetime, end: datetime):
    result = await session.execute(
        select(ControllerEventLog).where(
            ControllerEventLog.signal_id == signal_id,
            ControllerEventLog.event_param == phase,
            ControllerEventLog.event_code.in_([1, 8, 10]),  # green, yellow, red
            ControllerEventLog.event_time.between(start, end),
        ).order_by(ControllerEventLog.event_time)
    )
    return result.scalars().all()
```

### Tier 3: APScheduler Jobs (Application Logic)

**Purpose**: Application-level background jobs for complex business logic.

**Use for**:
- Detector health scans (run analytics, cache results, trigger alerts)
- Split failure daily summaries (compute once, reuse all day)
- Signal health scoring (complex multi-factor algorithm)
- Watchdog scans (check thresholds, send notifications)
- Report generation (PDF, Excel export)

**When to use APScheduler instead of Continuous Aggregates**:
- Complex business rules (if/then logic, thresholds, scoring algorithms)
- External integrations (email, webhooks)
- Multi-step workflows (compute -> cache -> alert)
- Non-SQL logic (Python-based calculations)

### Architecture Decision Matrix

| Metric | Continuous Aggregate | API On-Demand | APScheduler | Rationale |
|--------|---------------------|---------------|-------------|-----------|
| **Hourly Volume** | Primary | Read from CA | | Time-series rollup, heavily reused |
| **Hourly Occupancy** | Primary | Read from CA | | Time-series rollup, dashboard metric |
| **Split Failures** | Hourly counts | Read from CA | Daily summary + alerts | Pre-compute + alerting |
| **Stuck Detectors** | | Real-time query | Cache + alert (15 min) | Fast query + watchdog scan |
| **PCD Chart** | | On-demand | | User selects custom time window |
| **Gap Analysis** | | On-demand | | Fast query, user-specific detector |
| **Offset Drift** | Hourly quality | Read from CA | | Track coordination trends |
| **Preemption** | | On-demand | | Infrequent events, fast query |
| **Signal Health** | | Read cache | Compute every 30 min | Complex scoring algorithm |
| **Approach Delay** | 15-minute intervals | Read from CA | | Real-time dashboard metric |
| **Arrival on Red** | Hourly percentage | Read from CA | | Heavy computation, cache results |

### Performance Expectations

Performance depends on deployment scale and hardware. General guidance:

- **Dashboard metrics** (volume, occupancy, splits): Fastest — reads from continuous aggregates
- **Health status** (detector, signal scores): Fast — reads from APScheduler cache
- **Custom analysis** (PCD, gaps, preemption): Moderate — queries raw events with indexes
- **Heavy reports** (corridor analysis, daily summaries): Slowest — assembles from aggregates + API

Performance targets should be validated through benchmarking after deployment.

---

## 10. File Storage

### Purpose

Raw device files are stored for recovery purposes. Traffic signal controllers typically don't retain much historical data, so keeping the raw files allows for:
- Reprocessing if decoders are improved/fixed
- Debugging data issues
- Audit trails
- Disaster recovery

### Retention Policy

| Data Type | Retention | Reason |
|-----------|-----------|--------|
| **Raw device files** | 90 days | Recovery/reprocessing |
| **Processed events** | As configured | Primary data source |
| **Report exports** | 30 days | User downloads |

### Storage Backend Abstraction

Supports filesystem (default) or object storage (S3/MinIO) with the same interface:

```python
class StorageBackend(ABC):
    async def put(self, key: str, data: bytes, metadata: dict | None = None) -> StoredFile: ...
    async def get(self, key: str) -> bytes: ...
    async def delete(self, key: str) -> None: ...
    async def exists(self, key: str) -> bool: ...
    async def list(self, prefix: str) -> AsyncIterator[StoredFile]: ...
    async def get_url(self, key: str, expires_in: int = 3600) -> str: ...
```

### Key Structure

```
raw/
├── {device_id}/
│   └── {date}/
│       └── {filename}          # Original filename from device
│
exports/
├── reports/
│   └── {user_id}/
│       └── {report_type}_{date}_{uuid}.{format}
│
└── bulk/
    └── {export_id}/
        └── {filename}
```

### Configuration

```env
# Filesystem (default)
TSIGMA_STORAGE_BACKEND=filesystem
TSIGMA_STORAGE_PATH=/var/lib/tsigma/storage

# S3
TSIGMA_STORAGE_BACKEND=s3
TSIGMA_STORAGE_S3_BUCKET=tsigma-data
TSIGMA_STORAGE_S3_REGION=us-east-1

# MinIO (S3-compatible)
TSIGMA_STORAGE_BACKEND=s3
TSIGMA_STORAGE_S3_BUCKET=tsigma-data
TSIGMA_STORAGE_S3_ENDPOINT=http://minio:9000
```

---

## 11. Background Jobs & Scheduling

### Plugin-Based Job Architecture

Background jobs are extensible via the plugin pattern. Custom jobs can be added without modifying core scheduler code. APScheduler runs in-process when `TSIGMA_ENABLE_SCHEDULER=true`. Only one instance should run the scheduler in multi-pod deployments.

### Job Registry Pattern

```python
# tsigma/scheduler/registry.py

class JobRegistry:
    _jobs: dict[str, Callable] = {}

    @classmethod
    def register(cls, name: str, trigger: str, **trigger_kwargs):
        def wrapper(job_func: Callable) -> Callable:
            cls._jobs[name] = {
                "func": job_func,
                "trigger": trigger,
                "trigger_kwargs": trigger_kwargs
            }
            return job_func
        return wrapper

    @classmethod
    def get_all_jobs(cls) -> dict:
        return cls._jobs
```

### Core Jobs

| Job | Trigger | Purpose | Module |
|-----|---------|---------|--------|
| **aggregate** | Interval | Three-tier aggregation pipeline | `tsigma/scheduler/jobs/aggregate.py` |
| **refresh_views** | Cron (every 15 min) | Refresh materialized views | `tsigma/scheduler/jobs/refresh_views.py` |
| **watchdog** | Cron (daily 06:00) | Data quality and detector health checks | `tsigma/scheduler/jobs/watchdog.py` |
| **compress_chunks** | Interval (5 min) | Disk-based compression trigger (TimescaleDB only) | `tsigma/scheduler/jobs/compress_chunks.py` |
| **export_cold** | Cron (weekly) | Export old partitions to Parquet | `tsigma/scheduler/jobs/export_cold.py` |
| **signal_plan** | Cron | Signal plan ingestion | `tsigma/scheduler/jobs/signal_plan.py` |

The scheduler calls `db.refresh_materialized_views()` -- the facade handles PostgreSQL `REFRESH MATERIALIZED VIEW CONCURRENTLY`, MS-SQL indexed view refresh, and Oracle `DBMS_MVIEW.REFRESH()`.

### Job Auto-Discovery

```python
# tsigma/scheduler/jobs/__init__.py
from pathlib import Path

jobs_dir = Path(__file__).parent
for module_file in jobs_dir.glob("*.py"):
    if module_file.stem not in ("__init__",):
        __import__(f"tsigma.scheduler.jobs.{module_file.stem}")
```

### Database Access Rules for Custom Jobs

Custom jobs can create and manage their own tables, but **must not modify base TSIGMA tables**.

**Allowed:**
- Create custom tables (e.g., `corridor_metrics`, `custom_analytics_cache`)
- Insert/update/delete in your own tables
- Read from any TSIGMA base tables (via SQLAlchemy ORM)
- Create indexes on your own tables
- Create views that read from TSIGMA tables

**Prohibited:**
- Modify TSIGMA core tables (`controller_event_log`, `signal`, `detector`, etc.)
- Drop or alter TSIGMA core table schemas
- Delete data from TSIGMA core tables
- Modify TSIGMA indexes or constraints

**Namespace convention:** Custom tables should use a prefix to avoid conflicts.

**Migration requirement:** Custom jobs that need database tables must include idempotent Alembic migrations.

---

## 12. Security Architecture

### Credential Encryption

TSIGMA encrypts sensitive fields (passwords, SSH keys) at rest using Fernet symmetric encryption (`tsigma/crypto.py`). Credentials are encrypted before writing to the database and decrypted at poll time when the `CollectorService` needs them.

**Key sources** (checked in order):
1. `TSIGMA_SECRET_KEY` environment variable (Fernet key, base64-encoded 32 bytes)
2. `TSIGMA_SECRET_KEY_FILE` path to a file containing the key
3. `TSIGMA_SECRET_KEY_VAULT_URL` + `TSIGMA_SECRET_KEY_VAULT_PATH` (HashiCorp Vault)

**Sensitive fields** encrypted in `signal_metadata["collection"]`:
- `password`
- `ssh_key_path`

**API redaction**: The `redact_metadata()` function replaces sensitive field values with `***` before returning signal metadata in API responses. Credentials are never exposed to API consumers.

**Key generation:**
```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

### CSRF Nonces on Login

Login forms are protected by one-time-use CSRF nonces stored in the session store (Valkey or in-memory). The flow:

1. Server generates a random nonce via `session_store.create_csrf()` and embeds it in the login form
2. Client submits the nonce with the login POST
3. Server calls `session_store.validate_csrf(token)` which atomically deletes the nonce (one-time use)
4. If the nonce is missing, expired (5-minute TTL), or already consumed, the login is rejected

Both `InMemorySessionStore` and `ValkeySessionStore` implement the `create_csrf()` / `validate_csrf()` contract from `BaseSessionStore`.

### Session Management

Server-side sessions with two implementations (`tsigma/auth/sessions.py`):

| Implementation | Use Case | Storage | Expiry |
|----------------|----------|---------|--------|
| `InMemorySessionStore` | Single-process dev/testing | Python dict | Lazy cleanup |
| `ValkeySessionStore` | Production, multi-process | Valkey (Redis-compatible) | Server-side TTL + sliding expiry |

Sessions store: `user_id`, `username`, `role`, `created_at`, `expires_at`. Session IDs are `secrets.token_urlsafe(32)` stored in httponly cookies.

The `ValkeySessionStore` implements sliding expiry -- each access refreshes the TTL, keeping active sessions alive.

### Audit Triggers

TSIGMA uses dialect-aware database triggers to capture all changes to audited tables. The `DialectHelper.audit_trigger_sql()` method generates trigger DDL for all four supported databases:

| Database | User Context Mechanism | Trigger Style |
|----------|----------------------|---------------|
| PostgreSQL | `SET LOCAL app.current_user` (transaction-scoped) | Single trigger function, `to_jsonb(OLD/NEW)` |
| MS-SQL | `SESSION_CONTEXT(N'current_user')` | Single trigger, `INSERTED`/`DELETED` pseudo-tables, `FOR JSON` |
| Oracle | `SYS_CONTEXT('CLIENTCONTEXT', 'current_user')` | Single trigger, `:OLD`/`:NEW` references |
| MySQL | `SET @app_user` (session variable) | Separate triggers per operation (INSERT, UPDATE, DELETE) |

**Audited tables**: `signal` -> `signal_audit`, `approach` -> `approach_audit`, `detector` -> `detector_audit`, plus `auth_audit_log` for authentication events.

**User attribution**: The `get_audited_session()` dependency (`tsigma/dependencies.py`) reads the authenticated user from the session cookie and sets the database-level user context via `DialectHelper.set_app_user_sql()` before any write operation. Using `SET LOCAL` on PostgreSQL ensures the context is transaction-scoped and safe with connection pooling.

### Authentication Providers

Three pluggable auth providers via `AuthProviderRegistry`:

| Provider | Registration | Mechanism |
|----------|-------------|-----------|
| `local` | `@AuthProviderRegistry.register("local")` | Username/password with bcrypt hashing |
| `oidc` | `@AuthProviderRegistry.register("oidc")` | Azure AD / Entra ID via OpenID Connect |
| `oauth2` | `@AuthProviderRegistry.register("oauth2")` | Generic OAuth2 (configurable endpoints) |

The active provider is selected by `TSIGMA_AUTH_MODE` and initialized during lifespan startup. Provider-specific routes are dynamically mounted at `/api/v1/auth`.

### Middleware Security

The `SecurityHeadersMiddleware` adds security headers to all responses:
- `X-Content-Type-Options: nosniff`
- `X-Frame-Options: DENY`
- `X-XSS-Protection: 1; mode=block`
- Strict `Content-Security-Policy`

The `RequestIDMiddleware` adds a unique `X-Request-ID` to every request for tracing.

---

## 13. Configuration Management

### Configuration Priority

TSIGMA uses a **three-tier configuration priority system**:

```
1. Environment Variables (highest priority) — Deployment-specific overrides
2. YAML Files                               — Version-controlled configuration
3. Database                 (lowest priority) — Shared configuration with hot-reload
```

**How it works**:
1. **Database** provides shared baseline configuration (controller configs, polling schedules, etc.)
2. **YAML files** override specific values (useful for version control and staged rollouts)
3. **Environment variables** override everything (deployment-specific: secrets, hosts, ports)

### Settings Pattern

All application settings are in a single `Settings` class (`tsigma/config.py`) using Pydantic Settings with `TSIGMA_` prefix:

```python
class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="TSIGMA_",
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    # Database
    db_type: str = "postgresql"
    pg_host: str = "localhost"
    pg_port: int = 5432
    pg_database: str = "tsigma"

    # Component toggles
    enable_api: bool = True
    enable_collector: bool = True
    enable_scheduler: bool = True

    # Storage tiers
    timescale_chunk_interval: str = "1 day"
    storage_warm_after: str = "7 days"
    storage_retention: str = "2 years"

    # Collector
    collector_max_concurrent: int = 50
    collector_poll_interval: int = 900   # controller cadence (15 min)
    sensor_poll_interval: int = 900      # roadside-sensor cadence (15 min)

    # Aggregation
    aggregation_enabled: bool = True
    aggregation_interval_minutes: int = 15
    aggregation_lookback_hours: int = 2

    # Valkey (session store, cache)
    valkey_url: str = ""

    # Authentication
    auth_mode: str = "local"
    auth_session_ttl_minutes: int = 480

    # Validation layers
    validation_enabled: bool = True
    validation_layer1_enabled: bool = True
    validation_layer2_enabled: bool = False
    validation_layer3_enabled: bool = False

    # Credential encryption
    secret_key: str = ""
    secret_key_file: str = ""
    secret_key_vault_url: str = ""

    # ... (see tsigma/config.py for full list)
```

### System Config (DB-stored)

Runtime configuration via `system_setting` table. Provides typed async getters with env var override (`TSIGMA_` prefix takes precedence over DB values).

### Example .env File

```env
# Database (PostgreSQL + TimescaleDB)
TSIGMA_DB_TYPE=postgresql
TSIGMA_PG_HOST=db
TSIGMA_PG_PORT=5432
TSIGMA_PG_DATABASE=tsigma
TSIGMA_PG_USER=tsigma
TSIGMA_PG_PASSWORD=secret

TSIGMA_ENABLE_API=true
TSIGMA_ENABLE_COLLECTOR=true
TSIGMA_ENABLE_SCHEDULER=true

# Session store
TSIGMA_VALKEY_URL=redis://localhost:6379/0

# Credential encryption
TSIGMA_SECRET_KEY_FILE=/run/secrets/tsigma_key
```

---

## 14. Logging & Observability

### Structured Logging with structlog

```python
def setup_logging(log_level: str, log_format: str) -> None:
    # Configures structlog with JSON (production) or console (development) renderer
    # Integrates with Uvicorn loggers
    ...
```

### Log Output

**JSON (Production):**
```json
{"app": "tsigma", "level": "info", "timestamp": "2025-12-09T10:30:00Z", "event": "device polled", "device_id": "xyz-789", "events_count": 1523}
```

**Text (Development):**
```
2025-12-09 10:30:01 [info     ] device polled                  app=tsigma device_id=xyz-789 events_count=1523
```

### Middleware Stack

Request processing passes through middleware in order (outermost first):

1. `GZipMiddleware` -- compress responses > 1KB
2. `RequestIDMiddleware` -- assign unique request ID
3. `TimingMiddleware` -- measure request duration
4. `LoggingMiddleware` -- log request/response details
5. `SecurityHeadersMiddleware` -- add security headers

### Health Endpoints

| Endpoint | Purpose | Auth |
|----------|---------|------|
| `GET /health` | Liveness probe (process running) | None |
| `GET /ready` | Readiness probe (database connected) | None |

---

## 15. Database Migrations

**ALL migrations MUST be idempotent.** This is non-negotiable.

### Core Requirements

1. **IDEMPOTENT** -- Safe to run multiple times
   - Use `if not _table_exists()` guards for all CREATE TABLE
   - Use `if not _index_exists()` guards for all CREATE INDEX
   - Use `if not _column_exists()` guards for all ADD COLUMN
   - Use `ON CONFLICT DO NOTHING` for all seed data inserts

2. **ADDITIVE ONLY** -- Never destructive
   - No `DROP TABLE`
   - No `DROP COLUMN`
   - No `DELETE FROM` (user data)
   - Only `CREATE`, `ALTER ADD`, `INSERT ... ON CONFLICT DO NOTHING`

3. **NO ROLLBACKS** -- `downgrade()` raises error
   - Write a new forward migration to undo changes
   - Supports blue/green deployments (old version still running)

**Why idempotent?**
- CI/CD pipelines always run `alembic upgrade head` on deploy
- Corrupted `alembic_version` table won't cause cascading failures
- Manual re-runs during troubleshooting are safe
- Blue/green deployments can run migrations before traffic switches

**See [DATABASE.md](DATABASE.md#migrations-alembic)** for complete implementation guide with helper functions.

---

## 16. Architectural Decisions

### 16.1 SRP Decision: Unified Package

> **Decision:** Keep current flat package structure with adjustments
> **Analysis:** See [SRP_REVIEW.md](SRP_REVIEW.md) for full evaluation

**Context:** The package structure was reviewed for Single Responsibility Principle compliance. Four alternatives were evaluated, scoring 6.9 to 7.5 on a weighted scale.

**Decision Rationale:** For a small team (1-3 developers), simplicity outweighs strict SRP adherence:

1. **Infrastructure is stable** -- PostgreSQL and SQLAlchemy 2.0 are committed choices; no need to abstract for hypothetical swaps
2. **Cognitive load matters** -- One package with clear internal organization beats four packages with complex dependency management
3. **Weighted scores are close** -- The 0.6 point difference doesn't justify 4x package management overhead

### 16.2 No Repository Layer

API routes query SQLAlchemy models directly using the injected session from `get_session()`. The `DatabaseFacade` handles connection pooling, dialect abstraction, and session lifecycle. A separate repository layer would add indirection without value -- SQLAlchemy's ORM already provides the query interface.

```python
@router.get("/{signal_id}")
async def get_signal(signal_id: str, session: AsyncSession = Depends(get_session)):
    result = await session.execute(select(Signal).where(Signal.signal_id == signal_id))
    signal = result.scalar_one_or_none()
    ...
```

### 16.3 No Services Layer

Business logic lives in route handlers for simple operations and in dedicated modules (scheduler jobs, collection service, validation service) for complex operations. A generic `services/` layer would add indirection without clear separation of concerns.

### 16.4 DialectHelper Extraction

The `DialectHelper` class was extracted from `DatabaseFacade` per SRP -- connection management and SQL dialect logic are separate concerns. The facade delegates to `self.dialect` for all dialect-specific SQL generation (time bucketing, audit triggers, lookback predicates, delete windows, app user context).

### Revisit Triggers

Reconsider the unified package decision if:
- Team grows beyond 4 developers
- Multiple teams need to work on different features independently
- Package size exceeds 50 files or 10,000 lines
- Circular dependency issues emerge

### Guidelines for Future Development

| Scenario | Guidance |
|----------|----------|
| Need simple CRUD? | Use SQLAlchemy session directly in route |
| Need complex query? | Build query in route or extract to a helper function |
| Adding a new model? | Add to appropriate file in `models/` |
| Team grows to 4+? | Consider splitting into vertical slice structure |

---

## 17. Related Documents

| Document | Covers | Audience |
|----------|--------|----------|
| [Database Design](DATABASE.md) | Schema, tables, materialized views, encoding | Backend / DBA |
| [API Design](API.md) | REST, GraphQL, auth, authorization | API developers |
| [API Reference](API_REFERENCE.md) | Endpoint specs, request/response examples | API consumers |
| [Ingestion Pipeline](INGESTION.md) | Polling, decoders, sharding | Collector developers |
| [Decoder Documentation](DECODERS.md) | Decoder formats, registration, SDK | Decoder developers |
| [Report Architecture](REPORTS.md) | Report base class, registry, execution | Analytics developers |
| [Web UI](UI.md) | Jinja2, Alpine.js, ECharts, MapLibre | Frontend developers |
| [Deployment](DEPLOYMENT.md) | Docker Compose, env vars, scaling | DevOps |
| [Testing Strategy](TESTING.md) | Test organization, TDD workflow, fixtures | All developers |

Additional references:
- [SRP Review](SRP_REVIEW.md) -- packages/core SRP analysis
