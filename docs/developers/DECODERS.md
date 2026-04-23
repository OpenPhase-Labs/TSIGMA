# TSIGMA Decoder Reference

This document describes the event log file formats supported by TSIGMA and how to use the decoder system.

---

## Plugin-Based Architecture

**CRITICAL CONCEPT:** Decoders are **optional plugin modules**. You only include the decoders you actually use.

### Modular Deployment

```
Need ASC/3 only?                    → Include only asc3.py
Need ASC/3 + Siemens?               → Include only asc3.py + siemens.py
Need auto-detection for all?       → Include all decoder modules
Don't need file ingestion at all?  → Include no decoder modules
```

**Benefits:**
- ✅ **Minimal code footprint** - Don't bundle unused decoders
- ✅ **Faster imports** - Only load decoders you need
- ✅ **Clear separation** - Each decoder is self-contained
- ✅ **Third-party decoders** - External packages can add custom decoders

**If you don't need a decoder, simply exclude that `.py` file from your deployment.**

---

## Overview

TSIGMA supports multiple event log formats from different traffic signal controller manufacturers:

| Decoder | Manufacturer | Format | Extensions |
|---------|--------------|--------|------------|
| `asc3` | Econolite | Binary | `.dat`, `.datz` |
| `siemens` | Siemens | Text (SEPAC) | `.log`, `.txt`, `.csv`, `.sepac` |
| `peek` | Peek/McCain | Binary (ATC) | `.bin`, `.dat`, `.atc`, `.log` |
| `maxtime` | Intelight/Trafficware | XML/Binary | `.xml`, `.maxtime`, `.mtl`, `.bin`, `.synchro` |
| `csv` | Generic | CSV | `.csv`, `.txt`, `.tsv` |
| `openphase` | OpenPhase | Protobuf | `.pb`, `.proto`, `.bin` |
| `auto` | Auto-detect | Any | `.*` |

---

## ASC/3 Decoder (Econolite)

### Format: `asc3`

Decodes binary `.dat` files from Econolite ASC/3 controllers (Indiana Hi-Resolution format).

### File Structure

```
Bytes 0-19:   Date string (ASCII) "MM/DD/YYYY HH:MM:SS "
Bytes 20-N:  7 header lines (newline terminated)
Bytes N+:    Binary records (4 bytes each)
```

### Record Format (4 bytes)

| Byte | Content |
|------|---------|
| 0 | Event Code (0-255) |
| 1 | Event Parameter (0-255) |
| 2-3 | Time offset (big-endian int16, tenths of seconds) |

### Compression Support

- **Uncompressed**: Standard `.dat` files
- **Zlib**: Files starting with `0x78 0x9C` or `0x78 0xDA`
- **Gzip**: Files with `.datz` extension or `0x1F 0x8B` magic

### Usage

```python
from tsigma.collection.decoders.asc3 import ASC3Decoder

decoder = ASC3Decoder()
events = decoder.decode_bytes(open("/path/to/signal_1234_20240115.dat", "rb").read())

for event in events:
    print(f"{event.timestamp} | Code: {event.event_code} | Param: {event.event_param}")
```

### Example Output

```
2024-01-15 08:00:00.100 | Code: 1 | Param: 2    # Phase 2 Green
2024-01-15 08:00:15.300 | Code: 82 | Param: 5   # Detector 5 On
2024-01-15 08:00:15.800 | Code: 81 | Param: 5   # Detector 5 Off
2024-01-15 08:00:45.000 | Code: 8 | Param: 2    # Phase 2 Yellow
```

---

## Siemens SEPAC Decoder

### Format: `siemens`

Decodes text-based event logs from Siemens controllers using SEPAC format.

### File Structure

Text file with tab, comma, or semicolon delimited columns:

```
SEPAC Event Log
Signal: 1234
Date: 01/15/2024

Timestamp	Event Code	Parameter
08:00:00.1	1	2
08:00:15.3	82	5
08:00:15.8	81	5
```

### Column Detection

The decoder auto-detects columns by name via the shared SDK column name sets (case-insensitive):

| Field | Recognized Column Names |
|-------|------------------------|
| Timestamp | `timestamp`, `time`, `datetime`, `date_time`, `event_time` |
| Event Code | `event_code`, `code`, `eventcode`, `ec`, `event_id` |
| Parameter | `event_param`, `param`, `eventparam`, `ep`, `parameter` |

### Date Formats Supported

- `YYYY-MM-DD HH:MM:SS.fff`
- `MM/DD/YYYY HH:MM:SS`
- `DD/MM/YYYY HH:MM:SS`
- `HH:MM:SS.fff` (time only, uses file date)

### Usage

```python
from tsigma.collection.decoders.siemens import SiemensDecoder

decoder = SiemensDecoder()
events = decoder.decode_bytes(open("/path/to/sepac_log.txt", "rb").read())
```

---

## Peek/McCain ATC Decoder

### Format: `peek`

Decodes binary event logs from Peek and McCain ATC-compliant controllers.

### Header Formats

The decoder auto-detects the header format:

| Magic | Format | Header Size | Timestamp Resolution |
|-------|--------|-------------|---------------------|
| `PEEK` | Peek standard | 16 bytes | Milliseconds |
| `MCCN` | McCain | 32 bytes | Microseconds |
| `ATC\0` | Generic ATC | 16 bytes | Tenths of seconds |
| (none) | Headerless | 0 bytes | Tenths of seconds |

### Record Format (8 bytes)

| Bytes | Content |
|-------|---------|
| 0-3 | Timestamp offset (little-endian uint32) |
| 4 | Event Code |
| 5 | Event Parameter |
| 6-7 | Reserved/Checksum |

### Peek Header Structure (16 bytes)

```
Bytes 0-3:   "PEEK" magic
Bytes 4-7:   Unix timestamp (seconds)
Bytes 8-9:   Record count (uint16)
Bytes 10-11: Record size (uint16)
Bytes 12-15: Reserved
```

### McCain Header Structure (32 bytes)

```
Bytes 0-3:   "MCCN" magic
Bytes 4-5:   Version (uint16)
Bytes 6-7:   Flags (uint16)
Bytes 8-15:  Timestamp (uint64, microseconds since epoch)
Bytes 16-19: Record count (uint32)
Bytes 20-21: Record size (uint16)
Bytes 22-31: Reserved
```

### Usage

```python
from tsigma.collection.decoders.peek import PeekDecoder

decoder = PeekDecoder()
events = decoder.decode_bytes(open("/path/to/atc_log.bin", "rb").read())
```

---

## MaxTime/Intelight Decoder

### Format: `maxtime`

Decodes event logs from Intelight MaxTime, Q-Free MaxView, and Trafficware/Synchro controllers.

### Supported Formats

1. **MaxTime XML** - Standard XML export
2. **Trafficware XML** - Synchro XML format
3. **MaxView XML** - Q-Free format
4. **MaxTime Binary** - `.mtl` files with `MXTM` magic

### XML Structure

```xml
<?xml version="1.0"?>
<MaxTimeEvents>
  <Event timestamp="2024-01-15T08:00:00.100" code="1" param="2"/>
  <Event timestamp="2024-01-15T08:00:15.300" code="82" param="5"/>
</MaxTimeEvents>
```

### Attribute Name Detection

The decoder recognizes multiple XML attribute names via the shared SDK attribute name sets (case-insensitive):

| Field | Recognized Attributes |
|-------|----------------------|
| Timestamp | `timestamp`, `ts`, `time`, `datetime`, `event_time` |
| Event Code | `event_code`, `ec`, `code`, `eventcode` |
| Parameter | `event_param`, `ep`, `param`, `eventparam`, `parameter` |

### Binary Format (MXTM)

```
Bytes 0-3:   "MXTM" magic
Bytes 4-7:   Base epoch (little-endian uint32, seconds since epoch)
Bytes 8-19:  Reserved
Bytes 20+:   Records (8 bytes each)
```

Each 8-byte record: `[timestamp_offset(4 LE uint32, milliseconds), event_code(1), event_param(1), reserved(2)]`

### Usage

```python
from tsigma.collection.decoders.maxtime import MaxTimeDecoder

decoder = MaxTimeDecoder()
events = decoder.decode_bytes(open("/path/to/maxtime_export.xml", "rb").read())
```

---

## Generic CSV Decoder

### Format: `csv`

Highly configurable decoder for custom CSV formats from various export tools.

### Default Column Detection

Automatically detects columns by name via the shared SDK column name sets (case-insensitive):

| Field | Recognized Column Names |
|-------|------------------------|
| Timestamp | `timestamp`, `time`, `datetime`, `date_time`, `event_time` |
| Event Code | `event_code`, `code`, `eventcode`, `ec`, `event_id` |
| Parameter | `event_param`, `param`, `eventparam`, `ep`, `parameter` |

### Delimiter Auto-Detection

Automatically detects: `,`, `\t`, `;`, `|`

### Custom Configuration

```python
from tsigma.collection.decoders.csv_decoder import CSVDecoder, CSVConfig

config = CSVConfig(
    delimiter=";",
    timestamp_col=0,         # Column index for timestamp
    event_code_col=1,        # Column index for event code
    event_param_col=2,       # Column index for event param
    date_format="%d/%m/%Y %H:%M:%S",
    skip_rows=2,             # Skip 2 metadata rows before header
)

decoder = CSVDecoder(config)
events = decoder.decode_bytes(open("/path/to/custom_export.csv", "rb").read())
```

### CSVConfig Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `timestamp_col` | `int \| None` | `None` | Column index for timestamp (None = auto-detect by name) |
| `event_code_col` | `int \| None` | `None` | Column index for event code (None = auto-detect by name) |
| `event_param_col` | `int \| None` | `None` | Column index for event param (None = auto-detect by name) |
| `delimiter` | `str \| None` | `None` | Delimiter (auto-detect if None) |
| `date_format` | `str \| None` | `None` | Date format (auto-detect if None) |
| `skip_rows` | `int` | `0` | Rows to skip before header |

---

## OpenPhase Protobuf Decoder

### Format: `openphase`

Decodes OpenPhase v1 protobuf messages. Supports three message formats:

1. **IntersectionUpdate** -- single event in the master envelope (oneof payload = AtspmEvent). Used for real-time NATS/MQTT streaming.
2. **CompactEventBatch** -- delta-encoded batch of events with a shared base timestamp. Optimized for bandwidth-constrained backhaul.
3. **IntersectionUpdateBatch** -- batch of full IntersectionUpdate envelopes. Used for bulk transport.

### Event Code Resolution

The decoder prefers NTCIP event codes when present (`ntcip_event_code` / `ntcip_param`). When NTCIP fields are zero, it falls back to mapping the `EventType` enum to Indiana Hi-Res event codes (e.g., `DETECTOR_ON` -> 81, `PHASE_BEGIN_GREEN` -> 7).

### Usage

```python
from tsigma.collection.decoders.openphase import OpenPhaseDecoder

decoder = OpenPhaseDecoder()
events = decoder.decode_bytes(protobuf_data)
```

Proto definitions are compiled into `tsigma/collection/decoders/proto/openphase/v1/`.

---

## Auto-Detection Decoder

### Format: `auto`

Automatically detects the file format and uses the appropriate decoder.

### Detection Priority

1. **ASC/3** - Checks for date string header or compression magic
2. **Peek/McCain** - Checks for `PEEK`, `MCCN`, `ATC\0` magic bytes
3. **MaxTime** - Checks for XML with MaxTime/Trafficware markers
4. **Siemens** - Checks for SEPAC markers in text
5. **CSV** - Fallback for text files with delimiters

### Usage

```python
from tsigma.collection.decoders.auto import AutoDecoder

decoder = AutoDecoder()

# Auto-detect format and decode
events = decoder.decode_bytes(open("/path/to/unknown_format.dat", "rb").read())
```

---

## Decoder Registry Pattern

**Self-registering plugin system** - same pattern as ingestion methods, background jobs, and reports.

### Registry Implementation

```python
# tsigma/collection/decoders/base.py

class DecoderRegistry:
    """Central registry for all decoder plugins."""

    _decoders: dict[str, type[BaseDecoder]] = {}

    @classmethod
    def register(cls, decoder_cls: type[BaseDecoder]) -> type[BaseDecoder]:
        """Register a decoder plugin (used as bare class decorator)."""
        cls._decoders[decoder_cls.name] = decoder_cls
        return decoder_cls

    @classmethod
    def get(cls, name: str) -> type[BaseDecoder]:
        """Get a registered decoder by name."""
        if name not in cls._decoders:
            raise ValueError(f"Unknown decoder: {name}")
        return cls._decoders[name]

    @classmethod
    def get_for_extension(cls, extension: str) -> list[type[BaseDecoder]]:
        """Get all decoders that support a file extension."""
        extension = extension.lower()
        return [
            decoder_cls
            for decoder_cls in cls._decoders.values()
            if extension in decoder_cls.extensions
        ]

    @classmethod
    def list_all(cls) -> dict[str, type[BaseDecoder]]:
        """List all registered decoders."""
        return cls._decoders.copy()
```

Note: `DecoderRegistry.register` is a bare class decorator (not a function call). This differs from `ReportRegistry.register("name")` and `NotificationRegistry.register("name")`, which take a name argument. The decoder's `name` attribute is read from the class itself.

### Using the Registry

```python
from tsigma.collection.decoders import DecoderRegistry

# Get decoder class by name
decoder_cls = DecoderRegistry.get("asc3")
decoder = decoder_cls()

# Find all decoders that support .dat files
decoders = DecoderRegistry.get_for_extension(".dat")
# Returns: [ASC3Decoder, PeekDecoder]

# List all registered decoders
for name, decoder_cls in DecoderRegistry.list_all().items():
    print(f"{name}: {decoder_cls.description}")
```

### Auto-Discovery

```python
# tsigma/collection/decoders/__init__.py

"""Decoder plugins auto-discovery."""

from pathlib import Path
from .base import BaseDecoder, DecodedEvent, DecoderRegistry

# Auto-discover and import all decoder modules
decoders_dir = Path(__file__).parent
for module_file in decoders_dir.glob("*.py"):
    if module_file.stem not in ("__init__", "base"):
        __import__(f"tsigma.collection.decoders.{module_file.stem}")

__all__ = ["BaseDecoder", "DecodedEvent", "DecoderRegistry"]
```

### Creating a Custom Decoder

**Example: Custom vendor-specific decoder**

```python
# tsigma/collection/decoders/myvendor.py

from typing import ClassVar
from .base import BaseDecoder, DecodedEvent, DecoderRegistry

@DecoderRegistry.register
class MyVendorDecoder(BaseDecoder):
    """Decoder for MyVendor controller event logs."""

    name: ClassVar[str] = "myvendor"
    extensions: ClassVar[list[str]] = [".myv", ".mvlog"]
    description: ClassVar[str] = "MyVendor Controller Event Log"

    def decode_bytes(self, data: bytes) -> list[DecodedEvent]:
        """Decode raw bytes into events."""
        events = []
        # Parse data and create DecodedEvent objects
        # ...
        return events

    @classmethod
    def can_decode(cls, data: bytes) -> bool:
        """Check if data appears to be MyVendor format."""
        return data.startswith(b"MYVENDOR")
```

**That's it!** The decoder is automatically discovered and available via `DecoderRegistry.get("myvendor")`.

---

## Collection SDK

The collection SDK (`tsigma/collection/sdk/`) provides shared helpers used by ingestion method plugins. It handles event persistence, clock-drift detection, checkpoint management, and decoder resolution.

### Decoding and Persisting Events

```python
from tsigma.collection.sdk import (
    resolve_decoder_by_name,
    resolve_decoder_by_extension,
    persist_events_with_drift_check,
    persist_events,
)

# Resolve decoder by name
decoder = resolve_decoder_by_name("asc3")
events = decoder.decode_bytes(raw_data)

# Resolve decoder by file extension (with optional override)
decoder = resolve_decoder_by_extension("signal_1234.dat", explicit_decoder=None)
events = decoder.decode_bytes(raw_data)

# Persist with clock-drift detection (polling plugins)
await persist_events_with_drift_check(events, signal_id, session_factory)

# Persist without drift detection (push/event-driven plugins)
await persist_events(events, signal_id, session_factory)
```

### Available Decoders Query

```python
from tsigma.collection import DecoderRegistry

for name, decoder_cls in DecoderRegistry.list_all().items():
    print(f"{name}: {decoder_cls.description} ({decoder_cls.extensions})")
```

Output:
```
asc3: Econolite ASC/3 Hi-Resolution event log (['.dat', '.datz'])
siemens: Siemens SEPAC event log (['.log', '.txt', '.csv', '.sepac'])
peek: Peek/McCain binary event log (['.bin', '.dat', '.atc', '.log'])
maxtime: MaxTime/Trafficware/MaxView event log (['.xml', '.maxtime', '.mtl', '.bin', '.synchro'])
csv: Generic CSV/TSV event log (['.csv', '.txt', '.tsv'])
openphase: OpenPhase v1 Protobuf (events and batches) (['.pb', '.proto', '.bin'])
auto: Auto-detect event log format (['.*'])
```

---

## Adding New Decoder Support

### Adding a New Decoder

1. Create `tsigma/collection/decoders/myvendor.py`
2. Subclass `BaseDecoder`
3. Decorate with `@DecoderRegistry.register`
4. Implement `decode_bytes()` and `can_decode()` methods
5. No changes needed to other files — auto-discovered on import

### Template

```python
"""
MyVendor Event Decoder

Decodes event logs from MyVendor controllers.
"""

from typing import ClassVar
from .base import BaseDecoder, DecodedEvent, DecoderRegistry

@DecoderRegistry.register
class MyVendorDecoder(BaseDecoder):
    """Decoder for MyVendor controller event logs."""

    name: ClassVar[str] = "myvendor"
    extensions: ClassVar[list[str]] = [".myv", ".mvlog"]
    description: ClassVar[str] = "MyVendor Controller Event Log"

    def decode_bytes(self, data: bytes) -> list[DecodedEvent]:
        """Decode raw bytes into events."""
        events = []
        # Parse data and create DecodedEvent objects
        # ...
        return events

    @classmethod
    def can_decode(cls, data: bytes) -> bool:
        """Check if data appears to be MyVendor format."""
        return data.startswith(b"MYVENDOR")
```

### Benefits

| Benefit | Impact |
|---------|--------|
| **Extensibility** | Add custom decoders without touching core code |
| **Third-party plugins** | External packages can add decoders: `pip install tsigma-decoder-myvendor` |
| **Testability** | Unit test each decoder independently; mock the registry |
| **Deployment flexibility** | Include only the decoders you need |
| **Auto-registration** | New decoders automatically available in `IngestionService` |

---

## Troubleshooting

### "No decoder found for the provided data"

The auto-detector couldn't identify the format. Try:

1. Use explicit decoder: `decoder="csv"`
2. Check file is not corrupted
3. Verify file has expected structure

### "Unknown signal: XXXX"

The signal identifier is not in the database. Add the signal first:

```sql
INSERT INTO signal (signal_identifier, primary_name)
VALUES ('1234', 'Main St & 1st Ave');
```

### "Invalid date header"

ASC/3 decoder couldn't parse the date string. Check:

1. File is not truncated
2. Date format matches expected patterns
3. File encoding is correct

### Decoder selection for ambiguous extensions

Files with `.dat` extension could be ASC/3 or Peek format. The auto-detector examines file content, not just extension. To force a specific decoder:

```python
result = await service.ingest_file(path, signal_id, decoder="asc3")
```
