# Contributing to TSIGMA

Thanks for your interest in contributing. TSIGMA is built around a plugin architecture, so most contributions — new vendor decoders, new ingestion transports, new reports, new validators — are a single new file with no core changes required. This document covers how to get a development environment running, the plugin patterns, and the workflow for getting changes merged.

---

## Table of contents

- [Code of conduct](#code-of-conduct)
- [Development setup](#development-setup)
- [Plugin development (the most common contribution)](#plugin-development)
  - [Decoders](#decoders)
  - [Ingestion methods](#ingestion-methods)
  - [Reports](#reports)
  - [Validators](#validators)
  - [Storage backends](#storage-backends)
  - [Scheduled jobs](#scheduled-jobs)
- [Coding standards](#coding-standards)
- [Testing](#testing)
- [Database migrations](#database-migrations)
- [Branch and commit workflow](#branch-and-commit-workflow)
- [Pull request checklist](#pull-request-checklist)
- [Reporting bugs and security issues](#reporting-bugs-and-security-issues)
- [License and contributor agreement](#license-and-contributor-agreement)

---

## Code of conduct

Be respectful, be specific, and assume good intent. Hostile, harassing, or discriminatory behavior toward any contributor will result in removal from the project. Disagree with code, not with people.

---

## Development setup

### Prerequisites

- Python 3.14+ (free-threaded build recommended)
- PostgreSQL 18+ for development (TimescaleDB extension recommended; runs against MS-SQL/Oracle/MySQL too via the dialect abstraction)
- Git

### One-time setup

```bash
git clone https://github.com/OpenPhase-Labs/TSIGMA.git
cd TSIGMA

python3.14 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate

pip install -e ".[dev]"            # editable install + test/lint deps

cp .env.example .env               # then edit DB connection string
alembic upgrade head               # apply schema migrations
```

### Run the dev server

```bash
python -m tsigma.main
# Or with auto-reload:
uvicorn tsigma.app:app --host 0.0.0.0 --port 8080 --reload
```

Verify it's up: `curl http://localhost:8080/ready` (returns 200 only when the DB is reachable). Open `http://localhost:8080/docs` for the auto-generated Swagger UI.

---

## Plugin development

The fast path to contributing. Every extensible surface in TSIGMA is a registry-driven plugin: write one file, decorate the class, and the plugin is automatically discovered, exposed via REST, and surfaced in the UI. **No edits to the API layer, no new controller, no recompile.**

### Decoders

Parse vendor-specific event-log formats into TSIGMA's internal `DecodedEvent` shape. See [docs/developers/DECODERS.md](docs/developers/DECODERS.md) for the full SDK.

```python
# tsigma/collection/decoders/my_vendor.py
from typing import ClassVar
from .base import BaseDecoder, DecodedEvent, DecoderRegistry

@DecoderRegistry.register
class MyVendorDecoder(BaseDecoder):
    name: ClassVar[str] = "my_vendor"
    extensions: ClassVar[list[str]] = [".myv", ".mvz"]
    description: ClassVar[str] = "MyVendor controller event log"

    def decode_bytes(self, data: bytes) -> list[DecodedEvent]:
        ...
```

That's it. The decoder is now selectable via `signal_metadata.collection.decoder = "my_vendor"` and listed in `/api/v1/collection/decoders`.

### Ingestion methods

Add a new transport for getting data into TSIGMA. See [docs/developers/INGESTION.md](docs/developers/INGESTION.md). Existing examples: FTP/FTPS/SFTP, HTTP, NATS, MQTT, gRPC, TCP/UDP listeners, directory watch, SOAP.

```python
# tsigma/collection/methods/my_transport.py
from ..registry import IngestionMethodRegistry, ListenerIngestionMethod
# (or PollingIngestionMethod / EventDrivenIngestionMethod)

@IngestionMethodRegistry.register("my_transport")
class MyTransportMethod(ListenerIngestionMethod):
    name = "my_transport"

    async def health_check(self) -> bool: ...
    async def start(self, config, session_factory) -> None: ...
    async def stop(self) -> None: ...
```

### Reports

Add a new analytics output. See [docs/developers/REPORTS.md](docs/developers/REPORTS.md). Reports are pandas-based and execute against the indexed event log. Once registered they're automatically callable via `POST /api/v1/reports/{name}` and exportable via `/api/v1/reports/{name}/export`.

```python
# tsigma/reports/my_report.py
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession
import pandas as pd

from .registry import Report, ReportMetadata, ReportRegistry
from .sdk import fetch_events, parse_time

class MyReportParams(BaseModel):
    signal_id: str = Field(...)
    start: str = Field(...)
    end: str = Field(...)

@ReportRegistry.register("my_report")
class MyReport(Report[MyReportParams]):
    metadata = ReportMetadata(
        name="my_report",
        description="One-line description.",
        category="standard",
        estimated_time="fast",
        export_formats=["csv", "json", "ndjson"],
    )

    async def execute(self, params: MyReportParams, session: AsyncSession) -> pd.DataFrame:
        ...
```

### Validators

Add a new data-quality check that runs post-ingestion. See [docs/developers/VALIDATION.md](docs/developers/VALIDATION.md). Validation results are written to the event row's JSONB metadata column and are non-blocking by default.

```python
# tsigma/validation/validators/my_check.py
from ..registry import ValidatorRegistry
from ..sdk import BaseValidator

@ValidatorRegistry.register("my_check")
class MyCheckValidator(BaseValidator):
    name = "my_check"
    layer = 1   # 1 = deterministic; 2 = temporal/anomaly; 3 = cross-signal

    async def validate(self, event, context) -> ValidationResult:
        ...
```

### Storage backends

Add a new cold-archive backend (filesystem and S3 ship by default). See [docs/developers/STORAGE.md](docs/developers/STORAGE.md).

```python
# tsigma/storage/my_backend.py
from .base import BaseStorageBackend
from .factory import StorageRegistry

@StorageRegistry.register("my_backend")
class MyBackend(BaseStorageBackend):
    ...
```

### Scheduled jobs

Add a recurring background task. See [docs/developers/ARCHITECTURE.md](docs/developers/ARCHITECTURE.md) (scheduler section). Use cron-style trigger args.

```python
# tsigma/scheduler/jobs/my_job.py
from sqlalchemy.ext.asyncio import AsyncSession
from tsigma.scheduler.registry import JobRegistry

@JobRegistry.register(name="my_job", trigger="cron", hour="*", minute="*/15")
async def my_job(session: AsyncSession) -> None:
    ...
```

---

## Coding standards

Read first:

- [docs/developers/CODING_GUIDELINES.md](docs/developers/CODING_GUIDELINES.md) — the canonical style guide
- [docs/developers/STYLE_GUIDE.md](docs/developers/STYLE_GUIDE.md) — formatting and naming
- [docs/developers/ARCHITECTURE.md](docs/developers/ARCHITECTURE.md) — module boundaries and the database facade pattern
- [docs/developers/DATABASE_FACADE_PATTERN.md](docs/developers/DATABASE_FACADE_PATTERN.md) — how to write database-portable code

Quick rules:

- **Type hints everywhere.** Use modern syntax (`list[int]`, `str | None`, not `List[int]` / `Optional[str]`).
- **Async by default.** TSIGMA is FastAPI/asyncio top to bottom; sync DB calls are not allowed in request paths.
- **Database portability.** Use the database facade — never write dialect-specific SQL outside `tsigma/database/dialects/`.
- **No print statements.** Use the `logging` module; logger per module.
- **Comments are sparse.** Code names should explain what; comments only when the *why* isn't obvious.

---

## Testing

Read first: [docs/developers/TESTING.md](docs/developers/TESTING.md).

```bash
# Full unit suite (~2000 tests, ~30 seconds)
pytest tests/unit/

# Specific file
pytest tests/unit/test_my_thing.py -v

# With coverage
pytest tests/unit/ --cov=tsigma --cov-report=term-missing
```

Requirements for new code:

- **Every plugin needs a test file.** New decoder → `tests/unit/test_decoder_<name>.py`. New ingestion method → `tests/unit/test_<name>.py`. New report → `tests/unit/test_<report_name>.py`.
- **Cover registration, success path, and at least one failure path.** Decoders should also test `can_decode()` rejection of garbage input.
- **No test should hit a real DB or network.** Use the mocking patterns established in `tests/unit/test_nats_listener.py`, `test_grpc_server.py`, etc.
- **The full unit suite must pass before merge.** PRs are gated on CI green.

Some test files (`test_preempt_detail.py`, `test_left_turn_gap_data_check.py`, `test_time_space_diagram_average.py`) currently have known failures unrelated to this PR scope — they use `assert np.True_ is True` style assertions that are brittle. Don't add new tests in that style.

---

## Database migrations

Schema changes use Alembic. Read [docs/developers/DATABASE.md](docs/developers/DATABASE.md) and [docs/developers/DATABASE_SCHEMA.md](docs/developers/DATABASE_SCHEMA.md).

```bash
# Generate a new migration after editing models
alembic revision --autogenerate -m "add my_table"

# Review the generated file in alembic/versions/ — autogen is a starting
# point, NOT a substitute for thinking through the migration

# Apply
alembic upgrade head

# Roll back one revision
alembic downgrade -1
```

Migrations must be reversible (implement `downgrade()`) and must be portable across PostgreSQL / MS-SQL / Oracle / MySQL — use the database facade for any dialect-specific operations.

---

## Branch and commit workflow

- **Branch off `main`.** Use a descriptive branch name: `feature/grpc-ingestion`, `fix/preempt-units`, `docs/contributing-guide`.
- **One logical change per PR.** Big sprawling PRs get sent back. If you find yourself fixing three unrelated things, that's three PRs.
- **Atomic commits.** Each commit should pass tests on its own. Don't ship "WIP" or "fix typo" commits — squash or rebase before opening the PR.
- **Commit message format.**
  ```
  <area>: short imperative summary (≤ 70 chars)

  Optional body explaining the why. Wrap at ~72 chars. Reference
  issue numbers as #123. Don't repeat what the diff already shows —
  explain motivation, tradeoffs, anything non-obvious.
  ```
  Examples: `decoders: add MyVendor support`, `reports: fix preempt unit conversion`, `docs: contributing guide`.

---

## Pull request checklist

Before opening a PR, verify:

- [ ] `pytest tests/unit/` passes
- [ ] New code has type hints and follows [CODING_GUIDELINES.md](docs/developers/CODING_GUIDELINES.md)
- [ ] New plugins have at least one test file covering registration + success + one failure path
- [ ] If you added a new ingestion method / decoder / report, the corresponding row was added to [docs/developers/ATSPM_FEATURE_CATALOG.md](docs/developers/ATSPM_FEATURE_CATALOG.md)
- [ ] If you added a database column or table, an Alembic migration was generated and reviewed
- [ ] If you added a new dependency, it's in `pyproject.toml` (not just installed locally) and you considered the install footprint
- [ ] PR description explains the *why*, not just the *what* (the diff shows the what)
- [ ] Linked to any relevant issue

---

## Reporting bugs and security issues

- **Bugs**: open an issue on GitHub with reproduction steps, expected behavior, and actual behavior. Include the TSIGMA commit/version, Python version, and DB engine.
- **Security vulnerabilities**: do NOT open a public issue. Email the maintainers (see [SECURITY.md](docs/developers/SECURITY.md) once published, or contact through the Heritage Grid security mailbox). We'll acknowledge within 72 hours.

---

## License and contributor agreement

TSIGMA is licensed under the Mozilla Public License 2.0 (MPL-2.0) — see [LICENSE](LICENSE). By submitting a contribution you agree to license your work under the same terms. There is no separate CLA at this time, but we may add one before the project's first non-pre-release tag — contributors will be notified if so.

The OpenPhase Protobuf wire format that TSIGMA consumes lives in a separate repository under MPL-2.0 with an additional patent grant; see [PATENTS.md](https://github.com/OpenPhase-Labs/OPENPHASE/blob/main/PATENTS.md) in that repo for terms.
