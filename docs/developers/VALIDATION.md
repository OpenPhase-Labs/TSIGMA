# Validation Pipeline

Developer documentation for the TSIGMA post-ingestion validation system.

Source: `tsigma/validation/`

---

## Overview

The validation pipeline runs **after** ingestion. It never blocks or delays event collection. Events land in `controller_event_log` with `validation_metadata = NULL`, and a periodic background job picks them up, runs validators, and writes results back to the same row's `validation_metadata` JSONB column.

The pipeline is designed around three layers of increasing complexity:

| Layer | Name | Type | Status |
|-------|------|------|--------|
| Layer 1 | Schema / Range | Deterministic | Implemented |
| Layer 2 | Temporal / Anomaly | ML-assisted | Future (requires NTCIP 1202 SLM via MCP) |
| Layer 3 | Cross-signal | ML-assisted | Future (requires SLM + corridor definitions) |

---

## Architecture

Four components form the validation system:

### ValidationLevel

Enum in `tsigma/validation/registry.py`. Three values: `LAYER1`, `LAYER2`, `LAYER3`. Every validator declares which layer it belongs to.

### BaseValidator

Abstract base class. Subclasses must declare three `ClassVar` attributes and implement one async method:

```python
class BaseValidator(ABC):
    name: ClassVar[str]
    level: ClassVar[ValidationLevel]
    description: ClassVar[str]

    @abstractmethod
    async def validate_events(
        self,
        events: list[dict[str, Any]],
        signal_id: str,
        session_factory,
    ) -> list[dict[str, Any]]:
        ...
```

The method receives a batch of event dicts for a single signal and returns one result dict per event (same length as input, same order). Each result dict is built with `sdk.build_result()`.

### ValidationRegistry

Class-level registry at `tsigma/validation/registry.py`. Validators self-register with the `@ValidationRegistry.register("name")` decorator. The registry provides:

- `register(name)` -- decorator to register a validator class
- `get(name)` -- retrieve a validator class by name
- `list_available()` -- list all registered validator names
- `get_by_level(level)` -- get all validators for a given `ValidationLevel`

### ValidationService

Orchestration layer at `tsigma/validation/service.py`. Instantiated and started in `app.py` during the lifespan startup when `validation_enabled` is `True`. The service:

1. Reads enabled levels from settings (master toggle + per-layer toggles)
2. Instantiates all validators for enabled levels
3. Registers a `validation_cycle` job with the scheduler's `JobRegistry` at the configured interval
4. Each cycle:
   - Queries up to `validation_batch_size` rows where `validation_metadata IS NULL`, ordered by `event_time DESC`
   - Groups events by `signal_id`
   - Runs every enabled validator on each signal's batch
   - Merges per-validator results with `sdk.merge_results()`
   - Writes the merged JSONB back to each event row
5. On shutdown, unregisters the job from `JobRegistry`

---

## Plugin Pattern

Validation uses the same `@Registry.register` decorator pattern as all other TSIGMA plugin systems (auth providers, notification providers, decoders).

Auto-discovery chain:

```
app.py
  import tsigma.validation          # __init__.py
    import tsigma.validation.validators   # validators/__init__.py
      from . import schema_range          # triggers @ValidationRegistry.register
```

Adding a new validator module to `validators/` and importing it in `validators/__init__.py` is all that is needed for registration.

---

## SDK

Module: `tsigma/validation/sdk/__init__.py`

### Status Constants

```python
STATUS_UNVALIDATED = "unvalidated"  # Not yet validated (default)
STATUS_CLEAN      = "clean"         # Passed all checks
STATUS_SUSPECT    = "suspect"       # Anomalous but not definitively invalid
STATUS_INVALID    = "invalid"       # Failed deterministic checks
```

Severity ordering (worst wins when merging): `unvalidated < clean < suspect < invalid`.

### build_result()

```python
def build_result(
    validator_name: str,
    status: str,
    *,
    rules_failed: Optional[list[str]] = None,
    confidence: Optional[float] = None,   # 0.0-1.0, for ML validators
    details: Optional[str] = None,
) -> dict[str, Any]:
```

Returns a dict like:

```json
{
  "validator": "schema_range",
  "status": "invalid",
  "rules_failed": ["negative_param", "unknown_event_code"]
}
```

Only keys with non-None values are included. `confidence` is intended for Layer 2/3 ML-based validators.

### merge_results()

```python
def merge_results(results: list[dict[str, Any]]) -> dict[str, Any]:
```

Takes the list of per-validator result dicts and produces the final `validation_metadata` object. The overall status is the worst (highest severity) across all validators. If no results are provided, the status is `"unvalidated"`.

---

## validation_metadata JSONB Structure

The `validation_metadata` column on `controller_event_log` stores this structure:

```json
{
  "status": "clean",
  "validators": {
    "schema_range": {
      "validator": "schema_range",
      "status": "clean"
    }
  }
}
```

When a validator reports failures:

```json
{
  "status": "invalid",
  "validators": {
    "schema_range": {
      "validator": "schema_range",
      "status": "invalid",
      "rules_failed": ["unknown_event_code", "param_out_of_range"]
    }
  }
}
```

With multiple validators (future):

```json
{
  "status": "suspect",
  "validators": {
    "schema_range": {
      "validator": "schema_range",
      "status": "clean"
    },
    "temporal_anomaly": {
      "validator": "temporal_anomaly",
      "status": "suspect",
      "confidence": 0.72,
      "details": "Phase 2 green time 3x median for time-of-day"
    }
  }
}
```

The top-level `status` is always the worst status from any individual validator.

A row with `validation_metadata = NULL` has not been validated yet. A row with `"status": "unvalidated"` was processed but had no validator results (edge case).

---

## Layer 1: schema_range

Source: `tsigma/validation/validators/schema_range.py`

The only built-in validator. Deterministic, no ML. Checks each event against three rules:

### Rules

| Rule | Condition | Result |
|------|-----------|--------|
| `negative_param` | `event_param < 0` | `STATUS_INVALID` |
| `unknown_event_code` | `event_code` not found in `event_code_definition` table | `STATUS_INVALID` |
| `param_out_of_range` | `event_param` outside the range for its `param_type` | `STATUS_INVALID` |

If any rule fails, the event is `STATUS_INVALID`. If all pass, it is `STATUS_CLEAN`. This validator never produces `STATUS_SUSPECT`.

### PARAM_RANGE_BY_TYPE

Practical NTCIP 1202 maximums used for range checking:

```python
PARAM_RANGE_BY_TYPE = {
    "phase":         (0, 40),
    "detector":      (0, 128),
    "overlap":       (0, 16),
    "ring":          (0, 8),
    "channel":       (0, 64),
    "preempt":       (0, 10),
    "coord_pattern": (0, 255),
    "unit":          (0, 255),
    "other":         (0, 65535),
}
```

If a `param_type` is not in this table, the default range `(0, 65535)` is used.

### Reference Table Lookup

The validator queries `event_code_definition` to build an `event_code -> param_type` mapping. The `EventCodeDefinition` model (in `tsigma/models/reference.py`) has columns:

- `event_code` (SmallInteger, PK)
- `name` (Text)
- `description` (Text, nullable)
- `category` (Text)
- `param_type` (Text) -- used for range lookup

The mapping is loaded once per `validate_events()` call (once per signal batch per cycle).

---

## Admin Configuration

All settings are in `tsigma/config.py` under the `TSIGMA_` environment variable prefix.

| Setting | Env Var | Default | Description |
|---------|---------|---------|-------------|
| `validation_enabled` | `TSIGMA_VALIDATION_ENABLED` | `True` | Master toggle for the entire pipeline |
| `validation_layer1_enabled` | `TSIGMA_VALIDATION_LAYER1_ENABLED` | `True` | Enable Layer 1 (schema/range) |
| `validation_layer2_enabled` | `TSIGMA_VALIDATION_LAYER2_ENABLED` | `False` | Enable Layer 2 (temporal/anomaly) |
| `validation_layer3_enabled` | `TSIGMA_VALIDATION_LAYER3_ENABLED` | `False` | Enable Layer 3 (cross-signal) |
| `validation_batch_size` | `TSIGMA_VALIDATION_BATCH_SIZE` | `5000` | Max events per validation cycle |
| `validation_interval` | `TSIGMA_VALIDATION_INTERVAL` | `60` | Seconds between validation runs |

The master toggle (`validation_enabled`) gates the entire `ValidationService` startup in `app.py`. Per-layer toggles are checked inside `ValidationService._get_enabled_levels()`. Setting `validation_layer2_enabled = True` without a registered Layer 2 validator has no effect -- it just means the service looks for Layer 2 validators and finds none.

---

## How to Write a New Validator Plugin

### 1. Create the module

Add a new file in `tsigma/validation/validators/`, e.g. `temporal_anomaly.py`.

### 2. Implement the validator class

```python
from typing import Any, ClassVar

from ..registry import BaseValidator, ValidationLevel, ValidationRegistry
from ..sdk import STATUS_CLEAN, STATUS_SUSPECT, build_result


@ValidationRegistry.register("temporal_anomaly")
class TemporalAnomalyValidator(BaseValidator):
    name: ClassVar[str] = "temporal_anomaly"
    level: ClassVar[ValidationLevel] = ValidationLevel.LAYER2
    description: ClassVar[str] = "Temporal anomaly detection using SLM"

    async def validate_events(
        self,
        events: list[dict[str, Any]],
        signal_id: str,
        session_factory,
    ) -> list[dict[str, Any]]:
        results = []
        for event in events:
            # Your validation logic here.
            # Use session_factory for DB reads if needed:
            #   async with session_factory() as session:
            #       ...
            results.append(build_result(self.name, STATUS_CLEAN))
        return results
```

Requirements:
- Return a list the same length as `events`, in the same order
- Each element must be a dict from `build_result()`
- Return `None` for an event to skip it (the service handles this)
- If the validator raises an exception, the service logs it and continues with other validators -- the failing validator's results are simply absent from the merged output

### 3. Register for auto-discovery

Add the import to `tsigma/validation/validators/__init__.py`:

```python
from . import schema_range   # noqa: F401
from . import temporal_anomaly  # noqa: F401
```

### 4. Enable the layer

Set the appropriate layer toggle in the environment:

```
TSIGMA_VALIDATION_LAYER2_ENABLED=true
```

### 5. Verify

On startup, the log will show:

```
Instantiated validator: temporal_anomaly (layer2)
ValidationService started -- 2 validators, 60-second interval
```

---

## Future: Layers 2 and 3

Layer 2 (temporal/anomaly) and Layer 3 (cross-signal) are designed to use the NTCIP 1202 Signal Logic Model (SLM) accessed via MCP. The registry supports all three levels, per-layer toggles exist in config, and `ValidationService` instantiates validators for any enabled level.

Layer 2 would detect temporal anomalies such as implausible phase durations, detector actuations outside expected patterns, and timing plan violations.

Layer 3 would detect cross-signal anomalies such as coordination failures, progression breakdowns, and corridor-level inconsistencies by analyzing events across multiple signals. This requires corridor definitions to be configured in the system.

Both layers are expected to use `STATUS_SUSPECT` with a `confidence` score rather than the binary `STATUS_INVALID` used by Layer 1.
