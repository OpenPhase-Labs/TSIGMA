# Report Architecture

> Part of [TSIGMA Architecture](../ARCHITECTURE.md)

---

## Plugin-Based Architecture

**CRITICAL CONCEPT:** Reports are **optional plugin modules**. You only include the reports you actually use.

### Modular Deployment

```
Need PCD + Split Monitor only?       -> Include only purdue_diagram.py + split_monitor.py
Need all standard reports?           -> Include all report modules
Building custom ATSPM for agency?    -> Include only their required reports
```

**Benefits:**
- **Minimal code footprint** - Don't bundle unused reports
- **Faster testing** - Test only what you deploy
- **Clear separation** - Each report is self-contained
- **Third-party reports** - External packages can add custom reports

---

## Design Principles

1. **Reports are plugin modules** - Self-registering via decorator pattern
2. **Pydantic params** - Validated, typed input with clear error messages
3. **DataFrame results** - Reports return pandas DataFrames; the framework handles serialization
4. **SDK toolbox** - Reports use the SDK for data access, never touch the database facade directly
5. **Hybrid data source** - Pre-computed aggregates for speed, raw events for detail

## Report Categories

| Category | Data Source | Response Time | Example Reports |
|----------|-------------|---------------|-----------------|
| **Dashboard** | Aggregates only | < 100ms | Current status, daily summary |
| **Standard** | Aggregates + light events | < 2s | Approach delay, split monitor |
| **Detailed** | Heavy event queries | Async | Purdue diagram, raw exports |

---

## Report Base Class

```python
# tsigma/reports/registry.py

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Generic, TypeVar

import pandas as pd
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

TParams = TypeVar("TParams", bound=BaseModel)


@dataclass
class ReportMetadata:
    """Metadata describing a report plugin."""
    name: str
    description: str
    category: str       # 'dashboard' | 'standard' | 'detailed'
    estimated_time: str  # 'fast' | 'medium' | 'slow'
    supports_export: bool = True
    export_formats: list[str] | None = None  # defaults to ["csv", "json"]


class Report(ABC, Generic[TParams]):
    """
    Base class for all report plugins.

    Reports define typed Pydantic params and return pandas DataFrames.
    The framework handles serialization to CSV, JSON, or NDJSON.
    """
    metadata: ReportMetadata

    @abstractmethod
    async def execute(
        self,
        params: TParams,
        session: AsyncSession,
    ) -> pd.DataFrame:
        """Execute the report and return results as a DataFrame."""
        ...

    async def export(
        self,
        params: TParams,
        session: AsyncSession,
        format: str = "csv",
    ) -> bytes:
        """Execute and serialize to the requested format."""
        df = await self.execute(params, session)
        return self._serialize(df, format)

    @staticmethod
    def _serialize(df: pd.DataFrame, format: str) -> bytes:
        """Serialize a DataFrame to CSV, JSON, or NDJSON."""
        if format == "json":
            return df.to_json(orient="records", date_format="iso").encode()
        elif format == "ndjson":
            return df.to_json(orient="records", lines=True, date_format="iso").encode()
        elif format == "csv":
            return df.to_csv(index=False).encode()
        raise ValueError(f"Unsupported format: {format}")
```

**Key design decisions:**
- `Report` is generic over `TParams` (Pydantic BaseModel) for type-safe params
- `execute()` returns `pd.DataFrame` -- the universal tabular format
- `_serialize()` handles CSV/JSON/NDJSON automatically from the DataFrame
- Reports declare supported formats in `metadata.export_formats`; the framework enforces it
- `session` stays on `execute()` for config lookups via `get_config_at()`

---

## Example: Approach Delay Report

```python
# tsigma/reports/approach_delay.py

import logging
from typing import Literal

import pandas as pd
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from ..config_resolver import get_config_at
from .registry import Report, ReportMetadata, ReportRegistry
from .sdk import (
    EVENT_DETECTOR_ON,
    EVENT_PHASE_GREEN,
    fetch_events_split,
    parse_time,
)

logger = logging.getLogger(__name__)


class ApproachDelayParams(BaseModel):
    """Parameters for approach delay analysis."""
    signal_id: str = Field(..., description="Signal identifier")
    start: str = Field(..., description="Analysis window start (ISO-8601)")
    end: str = Field(..., description="Analysis window end (ISO-8601)")
    bin_size: Literal["15min", "hour", "day"] = Field(
        "15min", description="Time bin size for aggregation"
    )


@ReportRegistry.register("approach-delay")
class ApproachDelayReport(Report[ApproachDelayParams]):
    """Calculates average vehicular delay per approach."""

    metadata = ReportMetadata(
        name="approach-delay",
        description="Average delay per approach based on detector activation vs green start.",
        category="standard",
        estimated_time="fast",
        export_formats=["csv", "json", "ndjson"],
    )

    async def execute(
        self, params: ApproachDelayParams, session: AsyncSession
    ) -> pd.DataFrame:
        signal_id = params.signal_id          # attribute access, not dict
        start = parse_time(params.start)
        end = parse_time(params.end)
        bin_size = params.bin_size

        config = await get_config_at(session, signal_id, as_of=start)

        if not config.approaches:
            return pd.DataFrame(columns=["approach_id", "period", "avg_delay_seconds", "volume"])

        # ... build channel mappings from config ...

        # Fetch events via SDK helper (returns DataFrame)
        df = await fetch_events_split(
            signal_id, start, end,
            phase_codes=[EVENT_PHASE_GREEN],
            det_channels=list(channel_to_approach.keys()),
            det_codes=[EVENT_DETECTOR_ON],
        )

        if df.empty:
            return pd.DataFrame(columns=["approach_id", "period", "avg_delay_seconds", "volume"])

        # Process events, aggregate with pandas
        # ...

        return grouped  # return the DataFrame directly
```

### What the plugin author gets

- **Validated params**: `ApproachDelayParams(signal_id="S1", start="2024-01-01")` raises `ValidationError: Field required` for missing `end`
- **IDE autocomplete**: `params.signal_id` with type hints, not `params["signal_id"]`
- **Auto-generated API schema**: `ApproachDelayParams.model_json_schema()` produces OpenAPI-ready JSON
- **No serialization code**: Return a DataFrame, framework handles CSV/JSON/NDJSON

---

## Report SDK

Reports use the SDK for all data access. The SDK handles database queries
and returns pandas DataFrames. Report authors never import `db_facade` or
SQLAlchemy directly.

### Event Queries

```python
from .sdk import fetch_events, fetch_events_split

# Simple: fetch events by code
df = await fetch_events(signal_id, start, end, event_codes=(82,))
# Returns DataFrame: event_code, event_param, event_time

# Split: phase events + detector events in one query
df = await fetch_events_split(
    signal_id, start, end,
    phase_codes=(1, 8, 9),          # green, yellow, red
    det_channels=[1, 2, 3],
    det_codes=(82,),                # detector on
)
```

### Aggregate Queries (pre-computed tables)

```python
from .sdk import fetch_cycle_boundaries, fetch_cycle_arrivals

# Cycle timing boundaries
df = await fetch_cycle_boundaries(signal_id, phase, start, end)
# Returns: green_start, yellow_start, red_start, cycle_end, durations, termination_type

# Detector arrivals within cycles
df = await fetch_cycle_arrivals(signal_id, phase, start, end)
# Returns: arrival_time, detector_channel, green_start, time_in_cycle_seconds, phase_state
```

### Config Helpers

```python
from .sdk import load_channel_to_phase, load_channel_to_approach

channel_map = await load_channel_to_phase(session, signal_id, as_of=start)
# Returns: {detector_channel: phase_number}
```

### Utilities

```python
from .sdk import parse_time, bin_timestamp, calculate_occupancy

start = parse_time("2024-01-01T00:00:00")   # str -> datetime
bin_key = bin_timestamp(event_time, 15)       # floor to 15-min bin
occ = calculate_occupancy(det_events, t, 5.0) # 5-second occupancy window
```

---

## Report Registry

**Self-registering plugin system** -- same pattern as ingestion methods and background jobs:

```python
class ReportRegistry:
    _reports: dict[str, type[Report]] = {}

    @classmethod
    def register(cls, name: str):
        """Decorator to register a report plugin."""
        def wrapper(report_class):
            cls._reports[name] = report_class
            return report_class
        return wrapper

    @classmethod
    def get(cls, name: str) -> type[Report]: ...

    @classmethod
    def list_all(cls) -> dict[str, type[Report]]: ...
```

### Auto-Discovery

```python
# tsigma/reports/__init__.py

from pathlib import Path
from .registry import BaseReport, ReportRegistry

# Auto-discover and import all report modules
reports_dir = Path(__file__).parent
for module_file in reports_dir.glob("*.py"):
    if module_file.stem not in ("__init__", "registry"):
        __import__(f"tsigma.reports.{module_file.stem}")

__all__ = ["BaseReport", "ReportRegistry"]
```

---

## API Integration

```python
# tsigma/api/v1/reports.py

from fastapi import APIRouter, Depends, HTTPException
from pydantic import ValidationError
from tsigma.reports.registry import ReportRegistry

router = APIRouter(prefix="/api/v1/reports")


@router.get("/")
async def list_reports():
    """List all available reports with metadata and param schemas."""
    return [
        {
            "name": name,
            "description": cls.metadata.description,
            "category": cls.metadata.category,
            "estimated_time": cls.metadata.estimated_time,
            "export_formats": cls.metadata.export_formats,
            "params_schema": cls.__orig_bases__[0].__args__[0].model_json_schema(),
        }
        for name, cls in ReportRegistry.list_all().items()
    ]


@router.post("/{report_name}")
async def run_report(
    report_name: str,
    params: dict,
    session: AsyncSession = Depends(get_session),
):
    """Execute a report with validated params."""
    report_cls = ReportRegistry.get(report_name)
    report = report_cls()

    # Extract the Pydantic params class from the generic type
    params_class = report_cls.__orig_bases__[0].__args__[0]

    try:
        validated_params = params_class(**params)
    except ValidationError as exc:
        raise HTTPException(status_code=422, detail=exc.errors())

    df = await report.execute(validated_params, session)
    return {"status": "complete", "data": df.to_dict(orient="records")}


@router.get("/{report_name}/export")
async def export_report(
    report_name: str,
    params: dict,
    format: str = "csv",
    session: AsyncSession = Depends(get_session),
):
    """Export report as file (CSV, JSON, or NDJSON)."""
    report_cls = ReportRegistry.get(report_name)
    report = report_cls()

    params_class = report_cls.__orig_bases__[0].__args__[0]
    validated_params = params_class(**params)

    data = await report.export(validated_params, session, format)

    content_types = {"csv": "text/csv", "json": "application/json", "ndjson": "application/x-ndjson"}
    return Response(
        content=data,
        media_type=content_types.get(format, "application/octet-stream"),
        headers={"Content-Disposition": f"attachment; filename={report_name}.{format}"},
    )
```

---

## Adding Custom Reports

### Quick Start

1. Create `tsigma/reports/my_report.py`
2. Define a Pydantic params class
3. Subclass `Report[MyParams]`
4. Set `metadata = ReportMetadata(...)`
5. Implement `execute()` -- return a `pd.DataFrame`
6. Decorate with `@ReportRegistry.register("my-report")`
7. No changes needed to API routes -- auto-discovered on import

### Template

```python
# tsigma/reports/corridor_performance.py

import pandas as pd
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from .registry import Report, ReportMetadata, ReportRegistry
from .sdk import fetch_events, parse_time


class CorridorPerformanceParams(BaseModel):
    """Parameters for corridor performance analysis."""
    corridor_id: str = Field(..., description="Corridor identifier")
    start: str = Field(..., description="Analysis window start (ISO-8601)")
    end: str = Field(..., description="Analysis window end (ISO-8601)")
    include_speed: bool = Field(True, description="Include speed metrics")


@ReportRegistry.register("corridor-performance")
class CorridorPerformanceReport(Report[CorridorPerformanceParams]):
    """Corridor travel time and coordination quality report."""

    metadata = ReportMetadata(
        name="corridor-performance",
        description="Corridor travel time and coordination quality",
        category="standard",
        estimated_time="fast",
        export_formats=["csv", "json", "ndjson"],
    )

    async def execute(
        self, params: CorridorPerformanceParams, session: AsyncSession
    ) -> pd.DataFrame:
        start = parse_time(params.start)
        end = parse_time(params.end)

        # Your report logic here using SDK helpers
        # ...

        return result_df
```

**That's it!** The report is automatically discovered and available via
`/api/v1/reports/corridor-performance`. The API serves the param schema
at `/api/v1/reports/` so consumers know what fields to send.

---

## Report Catalog

All report types. Reports are plugins -- each is a self-contained `.py` file in `tsigma/reports/`. Exclude any you don't need.

#### Phase and Timing

| Report | Module | Category |
|--------|--------|----------|
| **split-monitor** | `split_monitor.py` | Standard |
| **split-failure** | `split_failure.py` | Standard |
| **phase-termination** | `phase_termination.py` | Standard |
| **timing-and-actuations** | `timing_and_actuations.py` | Detailed |
| **green-time-utilization** | `green_time_utilization.py` | Standard |

#### Volume, Speed, and Delay

| Report | Module | Category |
|--------|--------|----------|
| **approach-delay** | `approach_delay.py` | Standard |
| **approach-volume** | `approach_volume.py` | Standard |
| **approach-speed** | `approach_speed.py` | Standard |
| **turning-movement-counts** | `turning_movement_counts.py` | Standard |
| **wait-time** | `wait_time.py` | Standard |
| **bike-volume** | `bike_volume.py` | Standard |

#### Coordination

| Report | Module | Category |
|--------|--------|----------|
| **purdue-diagram** | `purdue_diagram.py` | Detailed |
| **arrivals-on-green** | `arrivals_on_green.py` | Standard |
| **arrival-on-red** | `arrival_on_red.py` | Standard |
| **time-space-diagram** | `time_space_diagram.py` | Detailed |
| **time-space-diagram-average** | `time_space_diagram_average.py` | Detailed |
| **link-pivot** | `link_pivot.py` | Detailed |

#### Pedestrian

| Report | Module | Category |
|--------|--------|----------|
| **ped-delay** | `ped_delay.py` | Standard |

#### Left Turn

| Report | Module | Category |
|--------|--------|----------|
| **left-turn-gap** | `left_turn_gap.py` | Detailed |
| **left-turn-gap-data-check** | `left_turn_gap_data_check.py` | Standard |
| **left-turn-volume** | `left_turn_volume.py` | Standard |

#### Safety

| Report | Module | Category |
|--------|--------|----------|
| **yellow-red-actuations** | `yellow_red_actuations.py` | Standard |
| **red-light-monitor** | `red_light_monitor.py` | Standard |

#### Preemption and Priority

| Report | Module | Category |
|--------|--------|----------|
| **preemption** | `preemption.py` | Standard |
| **preempt-detail** | `preempt_detail.py` | Detailed |
| **preempt-service** | `preempt_service.py` | Standard |
| **preempt-service-request** | `preempt_service_request.py` | Standard |
| **transit-signal-priority** | `transit_signal_priority.py` | Standard |

#### Specialized

| Report | Module | Category |
|--------|--------|----------|
| **ramp-metering** | `ramp_metering.py` | Standard |

---

## When to Use Raw Events vs Aggregates

| Use Case | Data Source | Reason |
|----------|-------------|--------|
| Dashboard widgets | Aggregates | Speed |
| Time-series charts | Aggregates | Sufficient granularity |
| Purdue diagrams | Events | Needs cycle-level detail |
| Statistical analysis | Aggregates | Pre-computed stats |
| Debugging/auditing | Events | Full detail |
| Multi-day trends | Aggregates (daily) | Performance |
