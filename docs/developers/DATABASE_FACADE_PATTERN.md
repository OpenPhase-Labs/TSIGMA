# Database Facade Pattern - TSIGMA

> **Complete database abstraction with no DTOs**
> Models are the schema. Routes never touch connection details.

---

## 1. Overview

TSIGMA uses a **complete database facade** that handles all database concerns:
- Connection management
- Connection pooling
- Dialect abstraction (PostgreSQL, MS-SQL, Oracle, MySQL)
- Session lifecycle
- Transaction management

**No DTOs needed** - SQLAlchemy 2.0 models with Pydantic validators serve as both:
- Database schema definitions
- API request/response validators
- Type-safe dataclasses

---

## 2. DialectHelper (`tsigma/database/db.py`)

Dialect-specific SQL generation is isolated in `DialectHelper`. It is a pure
function class — no dependency on engine or session. `DatabaseFacade` delegates
to it via `self.dialect`.

```python
# tsigma/database/db.py

class DialectHelper:
    """
    Dialect-specific SQL generation.

    Pure functions of db_type with no dependency on engine or session.
    Extracted from DatabaseFacade per SRP — connection management and
    SQL dialect logic are separate concerns.
    """

    def __init__(self, db_type: Literal["postgresql", "mssql", "oracle", "mysql"]):
        self.db_type = db_type

    _SAFE_INTERVALS = frozenset({
        "1 hour", "1 day", "15 minutes", "5 minutes", "30 minutes",
        "hour", "day", "minute", "HH", "DD", "MI",
    })

    def time_bucket(self, timestamp_column: str, interval: str) -> str:
        """Dialect-specific time bucketing. Returns SQL expression string."""
        ...

    def delete_window_sql(self, table: str, time_column: str, hours: int) -> str:
        """Dialect-specific DELETE for rows within a lookback window."""
        ...

    def lookback_predicate(self, time_column: str, hours: int) -> str:
        """Dialect-specific WHERE predicate for the lookback window."""
        ...

    def audit_trigger_sql(self, table: str, audit_table: str, id_columns: list[str]) -> list[str]:
        """Dialect-specific SQL statements to create an audit trigger."""
        ...

    def set_app_user_sql(self) -> str:
        """Dialect-specific SQL to set the current application user for audit attribution."""
        ...
```

All identifiers are validated against a strict regex (`^[A-Za-z_][A-Za-z0-9_]*$`)
and intervals are whitelisted to prevent SQL injection.

---

## 3. Database Facade (`tsigma/database/db.py`)

### Rules

1. **ORM first** — `execute()` rejects raw SQL strings. All queries should use `select()`, `insert()`, `update()`, `delete()` from SQLAlchemy Core/ORM. `text()` is accepted as a last resort when no ORM construct exists (e.g., database-specific DDL, extension queries), but should never be the default choice.
2. **No database-specific extensions** — Do not use `from sqlalchemy.dialects.postgresql import ...` or any dialect-specific imports in reports, API routes, or plugins. All dialect-specific behavior is handled inside `DialectHelper`. If you need something dialect-specific, add it to `DialectHelper` — never import a dialect directly outside of `database/db.py`.
3. **Pandas stays in the facade** — Reports and plugins do not `import pandas` or call `db_facade.get_dataframe()` directly. Reports use SDK helpers (`fetch_events()`, `fetch_events_split()`, `fetch_cycle_boundaries()`, etc.) which internally call `db_facade.get_dataframe()`. Reports receive DataFrames from the SDK, not from the facade.
4. **Everything goes through `execute()`** — `get_one()`, `get_many()`, `get_dataframe()`, `dataframe_tosql()` all funnel through `execute()`. One place for connection management, error handling, logging. SRP.

### Complete Facade Pattern

```python
# tsigma/database/db.py

from typing import Any, AsyncIterator, Literal
from contextlib import asynccontextmanager
from sqlalchemy import URL, text
from sqlalchemy.engine import Result
from sqlalchemy.ext.asyncio import (
    create_async_engine,
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
)
import pandas as pd

class DatabaseFacade:
    """
    Complete database facade.

    Routes never see connections, pooling, or dialect details.
    Everything is abstracted here.

    Dialect-specific SQL generation is delegated to DialectHelper
    (self.dialect). Proxy methods are provided for backward compatibility.
    """

    def __init__(self, db_type: Literal["postgresql", "mssql", "oracle", "mysql"], **config):
        self.db_type = db_type
        self.config = config
        self.dialect = DialectHelper(db_type)
        self._engine: AsyncEngine | None = None
        self._session_factory: async_sessionmaker | None = None

    async def connect(self) -> None:
        """Connect to database and create connection pool.
        Safe to call multiple times - will skip if already connected."""
        if self._engine is not None:
            return
        connection_url = self._build_connection_url()
        self._engine = create_async_engine(
            connection_url,
            pool_size=self.config.get("pool_size", 20),
            max_overflow=self.config.get("max_overflow", 10),
            pool_pre_ping=True,
            echo=self.config.get("echo_sql", False)
        )
        self._session_factory = async_sessionmaker(
            self._engine,
            class_=AsyncSession,
            expire_on_commit=False
        )

    async def disconnect(self) -> None:
        """Close all connections and dispose of pool.
        Safe to call multiple times - will skip if already disconnected."""
        if self._engine:
            await self._engine.dispose()
            self._engine = None
            self._session_factory = None

    @asynccontextmanager
    async def session(self) -> AsyncIterator[AsyncSession]:
        """
        Get a database session.

        Handles:
        - Session lifecycle
        - Transaction commit/rollback
        - Connection cleanup

        Routes use this. They never see connection details.
        """
        if not self._session_factory:
            raise RuntimeError("DatabaseFacade not initialized")

        async with self._session_factory() as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise

    async def execute(self, stmt: Any) -> Result:
        """
        Execute a SQLAlchemy ORM/Core statement and return the result.

        Accepts ORM constructs (select, insert, update, delete) and
        text() as a last resort when no ORM equivalent exists.
        Raw strings are rejected — use text() if you must write SQL.

        Raises TypeError if a raw string is passed.
        """
        async with self.session() as session:
            return await session.execute(stmt)

    async def get_one(self, stmt: Any) -> dict[str, Any] | None:
        """Execute ORM statement and return single row as dict, or None."""
        result = await self.execute(stmt)
        row = result.first()
        return dict(row._mapping) if row else None

    async def get_many(self, stmt: Any) -> list[dict[str, Any]]:
        """Execute ORM statement and return all rows as list of dicts."""
        result = await self.execute(stmt)
        rows = result.all()
        return [dict(row._mapping) for row in rows]

    async def get_dataframe(self, stmt: Any) -> pd.DataFrame:
        """Execute ORM statement and return results as pandas DataFrame."""
        result = await self.execute(stmt)
        rows = result.all()
        if not rows:
            return pd.DataFrame(columns=result.keys())
        return pd.DataFrame([dict(row._mapping) for row in rows])

    async def dataframe_tosql(self, df: pd.DataFrame, table_name: str,
                              if_exists: Literal["fail", "replace", "append"] = "append") -> Result:
        """Write DataFrame to database table via ORM bulk insert."""
        ...

    def _build_connection_url(self) -> URL:
        """Build dialect-specific connection URL.
        Uses SQLAlchemy URL.create() so the password is never
        embedded as a plain string in logs or tracebacks."""
        ...

    # -- Dialect proxy methods (delegate to self.dialect) --
    def time_bucket(self, timestamp_column: str, interval: str) -> str: ...
    def delete_window_sql(self, table: str, time_column: str, hours: int) -> str: ...
    def lookback_predicate(self, time_column: str, hours: int) -> str: ...
    async def has_timescaledb(self, session: AsyncSession) -> bool: ...
    async def refresh_materialized_views(self, session: AsyncSession) -> None: ...


# Global facade instance (initialized in app.py lifespan)
db_facade: DatabaseFacade | None = None


def get_db_facade() -> DatabaseFacade:
    """Dependency injection - get database facade."""
    if db_facade is None:
        raise RuntimeError("Database facade not initialized")
    return db_facade
```

---

## 4. Database Initialization (`tsigma/database/init.py`)

```python
# tsigma/database/init.py

from tsigma.database.db import DatabaseFacade
from tsigma.models.base import Base

async def initialize_database(
    db_facade: DatabaseFacade,
    enable_timescale: bool = True,
    chunk_time_interval_days: int = 7,
    compression_after_days: int = 7
) -> None:
    """
    Initialize database on first run.

    Creates all tables from SQLAlchemy models, sets up TimescaleDB
    hypertables and compression policies (PostgreSQL only), and
    creates performance indexes.
    """
    ...
```

Exports from `tsigma/database/__init__.py`:

```python
from tsigma.database.db import DatabaseFacade, db_facade, get_db_facade
from tsigma.database.init import initialize_database
```

---

## 5. Models (No DTOs!)

### SQLAlchemy 2.0 Mapped Models

```python
# tsigma/models/signal.py

from datetime import date, datetime
from decimal import Decimal
from typing import Optional
from uuid import UUID
from sqlalchemy import BigInteger, Boolean, Date, ForeignKey, Index, Text, func
from sqlalchemy.dialects.postgresql import INET, JSONB, TIMESTAMP
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import Mapped, mapped_column
from .base import Base, TimestampMixin


class Signal(Base, TimestampMixin):
    """
    Traffic signal/intersection configuration.

    This model serves as both:
    1. Database schema (SQLAlchemy ORM)
    2. API data structure

    NO separate DTO needed!
    """
    __tablename__ = "signal"

    signal_id: Mapped[str] = mapped_column(Text, primary_key=True)
    primary_street: Mapped[str] = mapped_column(Text, nullable=False)
    secondary_street: Mapped[Optional[str]] = mapped_column(Text)
    latitude: Mapped[Optional[Decimal]] = mapped_column()
    longitude: Mapped[Optional[Decimal]] = mapped_column()
    jurisdiction_id: Mapped[Optional[UUID]] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("jurisdiction.jurisdiction_id"),
    )
    region_id: Mapped[Optional[UUID]] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("region.region_id"),
    )
    corridor_id: Mapped[Optional[UUID]] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("corridor.corridor_id"),
    )
    controller_type_id: Mapped[Optional[UUID]] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("controller_type.controller_type_id"),
    )
    ip_address: Mapped[Optional[str]] = mapped_column(INET)
    note: Mapped[Optional[str]] = mapped_column(Text)
    signal_metadata: Mapped[Optional[dict]] = mapped_column("metadata", JSONB)
    enabled: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=True, server_default="true",
    )
    start_date: Mapped[Optional[date]] = mapped_column(Date)
```

### Base Model

```python
# tsigma/models/base.py

from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.dialects.postgresql import TIMESTAMP
from sqlalchemy import func


class Base(DeclarativeBase):
    """Base class for all TSIGMA models."""
    pass


class TimestampMixin:
    """Mixin for models that need created_at and updated_at timestamps."""
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now(),
        onupdate=func.now(),
    )
```

---

## 6. API Routes (No DTOs!)

### Routes Use Models Directly

```python
# Example API route

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from tsigma.database.db import DatabaseFacade, get_db_facade
from tsigma.models.signal import Signal

router = APIRouter(prefix="/api/v1/signals", tags=["Signals"])


@router.get("/", response_model=list[Signal])
async def list_signals(
    db: DatabaseFacade = Depends(get_db_facade),
    limit: int = 100,
    offset: int = 0
):
    """
    List all signals.

    NO DTO conversion needed:
    - Signal model validates itself
    - Signal model serializes itself
    - Route never sees database connections
    """
    async with db.session() as session:
        result = await session.execute(
            select(Signal)
            .where(Signal.enabled == True)
            .limit(limit)
            .offset(offset)
        )
        signals = result.scalars().all()
        return list(signals)


@router.get("/{signal_id}", response_model=Signal)
async def get_signal(
    signal_id: str,
    db: DatabaseFacade = Depends(get_db_facade)
):
    """Get signal by ID."""
    async with db.session() as session:
        result = await session.execute(
            select(Signal).where(Signal.signal_id == signal_id)
        )
        signal = result.scalar_one_or_none()
        if not signal:
            raise HTTPException(status_code=404, detail="Signal not found")
        return signal
```

---

## 7. App Initialization

```python
# tsigma/app.py

from contextlib import asynccontextmanager
from fastapi import FastAPI
from tsigma.database.db import DatabaseFacade, db_facade
from tsigma.config import get_settings


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    App lifespan - initialize database facade on startup.
    Routes never touch this. They just use get_db_facade() dependency.
    """
    global db_facade

    config = get_settings()
    db_config = {
        "host": config.pg_host,
        "port": config.pg_port,
        "database": config.pg_database,
        "user": config.pg_user,
        "password": config.pg_password,
        "pool_size": 20,
        "max_overflow": 10
    }

    db_facade = DatabaseFacade(db_type=config.db_type, **db_config)
    await db_facade.connect()

    yield

    await db_facade.disconnect()
```

---

## 8. Summary

### Architecture Benefits

| Aspect | Benefit |
|--------|---------|
| **No DTOs** | Single source of truth -- model is the schema |
| **Database facade** | Routes never see connections, pooling, dialect details |
| **DialectHelper** | SQL dialect logic separated from connection management (SRP) |
| **SQLAlchemy 2.0** | Type-safe, validated, auto-serializing models |
| **Dialect abstraction** | Swap databases without changing route code |
| **Connection pooling** | Handled in facade, invisible to routes |

### What Routes Know

Routes know:
- Models (Signal, Detector, etc.)
- Business logic
- HTTP concerns (status codes, headers)

Routes DON'T know:
- Database connections
- Connection pooling
- Database dialect
- Connection strings

### File Structure

- `tsigma/models/signal.py` (SQLAlchemy ORM model)
- `tsigma/models/base.py` (Base class + TimestampMixin)
- `tsigma/database/db.py` (DatabaseFacade + DialectHelper)
- `tsigma/database/init.py` (Database initialization)
- `tsigma/database/__init__.py` (Package exports)
