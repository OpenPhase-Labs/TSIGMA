"""
Integration-test fixtures — multi-dialect edition.

Every integration test in this directory is parametrised over the four
supported dialects (postgresql, mssql, oracle, mysql).  For each
dialect, the fixture chain is:

    1. Prefer a dev-provided DB via environment variable:
         TSIGMA_TEST_PG_URL    (or legacy TSIGMA_TEST_DB_URL for PG)
         TSIGMA_TEST_MSSQL_URL
         TSIGMA_TEST_ORACLE_URL
         TSIGMA_TEST_MYSQL_URL
    2. Fall back to a Docker container spun up on demand via
       ``testcontainers`` (session-scoped — one container shared across
       the whole pytest session).
    3. Skip that dialect's parametrisation if both paths are unusable
       (no env var, and either ``testcontainers`` is not installed or
       Docker is not reachable).

The fixtures apply the Alembic migration to the resulting DB so tests
exercise the real migration pipeline, not ``Base.metadata.create_all``
— partitioning, TimescaleDB hypertable conversion, and other
migration-side behaviour matter for correctness and must be covered.

Fixtures:

    ``dialect_name``         — parametrises over the four dialects.
    ``dialect_async_url``    — SQLAlchemy async URL for the current
                                dialect (skips if unavailable).
    ``dialect_sync_url``     — sync URL variant for Alembic.
    ``dialect_engine``       — session-scoped ``AsyncEngine`` with the
                                migration applied.
    ``dialect_session``      — function-scoped ``AsyncSession``
                                wrapped in a rolled-back transaction.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Callable, Optional

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

# ---------------------------------------------------------------------------
# Dialect registry
# ---------------------------------------------------------------------------


@dataclass
class DialectSpec:
    """Everything needed to stand up and talk to one dialect.

    Attributes:
        name: Dialect name as SQLAlchemy uses it.
        async_scheme: SQLAlchemy URL scheme for async drivers
            (``postgresql+asyncpg``, ``mssql+aioodbc``, ...).
        sync_scheme: SQLAlchemy URL scheme for Alembic (sync drivers).
        env_vars: Ordered list of environment variable names to check
            for a pre-provided DB URL.  First match wins.
        container_factory: Callable returning a started testcontainers
            container instance, or ``None`` if testcontainers isn't
            installed / Docker isn't available.
    """

    name: str
    async_scheme: str
    sync_scheme: str
    env_vars: tuple[str, ...]
    container_factory: Optional[Callable[[], object]] = None
    # Populated lazily — one container per dialect per session.
    _container: object | None = field(default=None, repr=False)


def _pg_container_factory():
    try:
        from testcontainers.postgres import PostgresContainer
    except ImportError:
        return None
    # TimescaleDB image so hypertable calls in the migration work.
    return PostgresContainer(
        "timescale/timescaledb:latest-pg16",
        dbname="tsigma_test",
        username="tsigma",
        password="tsigma",
    )


def _mssql_container_factory():
    try:
        from testcontainers.mssql import SqlServerContainer
    except ImportError:
        return None
    return SqlServerContainer(
        "mcr.microsoft.com/mssql/server:2022-latest",
        password="TsigmaTest1!",
    )


def _oracle_container_factory():
    try:
        from testcontainers.oracle import OracleDbContainer
    except ImportError:
        return None
    # gvenzl/oracle-free is the lightweight free Oracle image maintained
    # by the testcontainers ecosystem.
    return OracleDbContainer("gvenzl/oracle-free:23-slim-faststart")


def _mysql_container_factory():
    try:
        from testcontainers.mysql import MySqlContainer
    except ImportError:
        return None
    return MySqlContainer(
        "mysql:8",
        username="tsigma",
        password="tsigma",
        dbname="tsigma_test",
    )


DIALECTS: dict[str, DialectSpec] = {
    "postgresql": DialectSpec(
        name="postgresql",
        async_scheme="postgresql+asyncpg",
        sync_scheme="postgresql+psycopg2",
        # TSIGMA_TEST_DB_URL kept as an alias for back-compat with the
        # pre-existing tests/conftest.py::db_url fixture.
        env_vars=("TSIGMA_TEST_PG_URL", "TSIGMA_TEST_DB_URL"),
        container_factory=_pg_container_factory,
    ),
    "mssql": DialectSpec(
        name="mssql",
        async_scheme="mssql+aioodbc",
        sync_scheme="mssql+pyodbc",
        env_vars=("TSIGMA_TEST_MSSQL_URL",),
        container_factory=_mssql_container_factory,
    ),
    "oracle": DialectSpec(
        name="oracle",
        async_scheme="oracle+oracledb_async",
        sync_scheme="oracle+oracledb",
        env_vars=("TSIGMA_TEST_ORACLE_URL",),
        container_factory=_oracle_container_factory,
    ),
    "mysql": DialectSpec(
        name="mysql",
        async_scheme="mysql+aiomysql",
        sync_scheme="mysql+pymysql",
        env_vars=("TSIGMA_TEST_MYSQL_URL",),
        container_factory=_mysql_container_factory,
    ),
}


# ---------------------------------------------------------------------------
# URL resolution: env var -> container -> None
# ---------------------------------------------------------------------------


def _env_url(spec: DialectSpec) -> Optional[str]:
    for var in spec.env_vars:
        url = os.environ.get(var)
        if url:
            return url
    return None


def _docker_available() -> bool:
    """Best-effort check that a Docker daemon is reachable.

    ``docker`` package imports cheaply; ``from_env`` is what talks to
    the socket.  Any failure means we're not going to be able to start
    testcontainers and should skip.
    """
    try:
        import docker
    except ImportError:
        return False
    try:
        client = docker.from_env()
        client.ping()
        return True
    except Exception:
        return False


def _container_url(spec: DialectSpec) -> Optional[str]:
    """Start (or reuse) a session-scoped container for ``spec`` and return its
    SQLAlchemy URL, or ``None`` if testcontainers / Docker isn't usable."""
    if spec.container_factory is None:
        return None
    if not _docker_available():
        return None

    if spec._container is None:
        container = spec.container_factory()
        if container is None:
            return None
        container.start()
        spec._container = container

    # testcontainers ships get_connection_url() on every DB container.
    raw = spec._container.get_connection_url()
    return raw


def _normalise_async_url(spec: DialectSpec, raw_url: str) -> str:
    """Force the scheme onto the async driver for runtime queries.

    testcontainers hands back sync scheme URLs by default
    (e.g. ``postgresql+psycopg2://...``); dev-provided env vars may or
    may not.  We rewrite the scheme to the async one.
    """
    if "://" not in raw_url:
        return raw_url
    _, rest = raw_url.split("://", 1)
    return f"{spec.async_scheme}://{rest}"


def _normalise_sync_url(spec: DialectSpec, raw_url: str) -> str:
    """Force the scheme onto the sync driver for Alembic."""
    if "://" not in raw_url:
        return raw_url
    _, rest = raw_url.split("://", 1)
    return f"{spec.sync_scheme}://{rest}"


# ---------------------------------------------------------------------------
# Session-scoped teardown of any containers started during the run
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session", autouse=True)
def _cleanup_containers():
    """Stop every container that got started during the session.

    Yields immediately — cleanup runs on session teardown.  Safe to
    call repeatedly if nothing was started.
    """
    yield
    for spec in DIALECTS.values():
        container = spec._container
        if container is not None:
            try:
                container.stop()
            except Exception:
                # Best-effort; don't blow up teardown because Docker
                # disappeared mid-session.
                pass
            spec._container = None


# ---------------------------------------------------------------------------
# Dialect parametrisation
# ---------------------------------------------------------------------------


@pytest.fixture(params=list(DIALECTS.keys()))
def dialect_name(request) -> str:
    """Parametrises the current test over every supported dialect."""
    return request.param


@pytest.fixture
def dialect_spec(dialect_name: str) -> DialectSpec:
    return DIALECTS[dialect_name]


@pytest.fixture
def dialect_raw_url(dialect_spec: DialectSpec) -> str:
    """Raw URL (still whatever scheme the source provided) or skip."""
    url = _env_url(dialect_spec) or _container_url(dialect_spec)
    if url is None:
        pytest.skip(
            f"{dialect_spec.name}: no DB available "
            f"(env vars {dialect_spec.env_vars} unset, "
            f"testcontainers/Docker unavailable)"
        )
    return url


@pytest.fixture
def dialect_async_url(dialect_spec: DialectSpec, dialect_raw_url: str) -> str:
    return _normalise_async_url(dialect_spec, dialect_raw_url)


@pytest.fixture
def dialect_sync_url(dialect_spec: DialectSpec, dialect_raw_url: str) -> str:
    return _normalise_sync_url(dialect_spec, dialect_raw_url)


# ---------------------------------------------------------------------------
# Alembic-migrated engine (function scope: most tests want a fresh DB)
# ---------------------------------------------------------------------------


def _run_alembic_upgrade(sync_url: str) -> None:
    """Run ``alembic upgrade head`` against the given sync URL.

    Alembic is sync by design — it drives DDL via sync SQLAlchemy
    regardless of the app's async runtime.
    """
    from alembic.config import Config

    from alembic import command

    # alembic.ini lives at the repo root alongside the migrations dir.
    cfg = Config("alembic.ini")
    cfg.set_main_option("sqlalchemy.url", sync_url)
    command.upgrade(cfg, "head")


def _run_alembic_downgrade(sync_url: str, target: str = "base") -> None:
    from alembic.config import Config

    from alembic import command

    cfg = Config("alembic.ini")
    cfg.set_main_option("sqlalchemy.url", sync_url)
    command.downgrade(cfg, target)


@pytest_asyncio.fixture
async def dialect_engine(
    dialect_async_url: str, dialect_sync_url: str,
) -> AsyncEngine:
    """Async engine backed by a freshly-migrated database.

    Function-scoped so each test starts clean — partition-management
    / migration tests cannot share state, and the transactional
    rollback pattern doesn't cover DDL that sits outside the
    transaction (TimescaleDB hypertables, MS-SQL partition schemes).
    Container reuse keeps the expensive part (container spin-up)
    amortised across the session.
    """
    # Migrate up.
    _run_alembic_upgrade(dialect_sync_url)

    engine = create_async_engine(dialect_async_url, echo=False)
    try:
        yield engine
    finally:
        await engine.dispose()
        # Drop everything back to base so the next test starts clean.
        # Wrapped in try/except because a broken test can leave the
        # schema in a state that alembic can't unwind — we'd rather
        # surface the original failure than this cleanup error.
        try:
            _run_alembic_downgrade(dialect_sync_url, "base")
        except Exception:
            pass


@pytest_asyncio.fixture
async def dialect_session(dialect_engine: AsyncEngine) -> AsyncSession:
    """Per-test async session wrapped in a transaction that rolls back.

    Suitable for row-level CRUD tests.  Tests that exercise DDL
    (migration / partition management) should use ``dialect_engine``
    directly.
    """
    async with dialect_engine.connect() as conn:
        transaction = await conn.begin()
        factory = async_sessionmaker(
            bind=conn, class_=AsyncSession, expire_on_commit=False,
        )
        async with factory() as session:
            yield session
        await transaction.rollback()


# ---------------------------------------------------------------------------
# Handy aliases for single-dialect tests
# ---------------------------------------------------------------------------
#
# Some integration tests only make sense against one dialect — e.g. a
# TimescaleDB-specific continuous-aggregate check.  Those tests can
# skip the parametrisation and use the ``pg_engine`` alias directly;
# see tests/integration/test_collector_e2e.py for an example.


@pytest_asyncio.fixture
async def pg_engine() -> AsyncEngine:
    """Single-dialect PostgreSQL engine (migration applied)."""
    spec = DIALECTS["postgresql"]
    raw = _env_url(spec) or _container_url(spec)
    if raw is None:
        pytest.skip(
            "postgresql: no DB available "
            "(TSIGMA_TEST_PG_URL / TSIGMA_TEST_DB_URL unset, "
            "testcontainers/Docker unavailable)"
        )
    sync_url = _normalise_sync_url(spec, raw)
    async_url = _normalise_async_url(spec, raw)

    _run_alembic_upgrade(sync_url)

    engine = create_async_engine(async_url, echo=False)
    try:
        yield engine
    finally:
        await engine.dispose()
        try:
            _run_alembic_downgrade(sync_url, "base")
        except Exception:
            pass
