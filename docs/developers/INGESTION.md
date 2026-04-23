# Ingestion Pipeline

> Part of [TSIGMA Architecture](../ARCHITECTURE.md)

---

## Plugin-Based Architecture

**CRITICAL CONCEPT:** Ingestion methods are **optional plugin modules**. You only include the modules you actually use.

### Modular Deployment

```
Need FTP polling only?          → Include only ftp_pull.py
Need SFTP + directory watch?    → Include only ftp_pull.py (handles SFTP via protocol enum) + directory_watch.py
Need HTTP pull only?             → Include only http_pull.py
Don't need any polling?          → Include no ingestion method modules
```

**Benefits:**
- ✅ **Minimal dependencies** - Only install libraries for protocols you use
- ✅ **Smaller deployments** - Don't bundle unused code
- ✅ **Reduced attack surface** - Fewer packages = fewer CVEs
- ✅ **Clear separation** - Each method is self-contained with zero coupling

### Available Plugin Modules

Each module in `tsigma/collection/methods/` is an independent plugin:

#### Pull Methods (PollingIngestionMethod)

TSIGMA connects to the controller and retrieves data on a schedule.

| Module | Protocol | Dependencies | Use Case |
|--------|----------|--------------|----------|
| `ftp_pull.py` | FTP/FTPS/SFTP | aioftp, asyncssh | Legacy controllers (file-based logs) |
| `http_pull.py` | HTTP/HTTPS | aiohttp | Modern controllers with REST API |

#### Push/Listener Methods (ListenerIngestionMethod)

External devices push data to TSIGMA. These run as long-lived async servers.

| Module | Protocol | Dependencies | Use Case |
|--------|----------|--------------|----------|
| `udp_server.py` | UDP listener | asyncio (built-in) | Wavetronics speed sensors (port 10088) |
| `tcp_server.py` | TCP listener | asyncio (built-in) | Legacy Wavetronics speed sensors (port 10088) |
| `nats_listener.py` | NATS | nats-py | Real-time event streaming via NATS subjects |
| `mqtt_listener.py` | MQTT | aiomqtt | Real-time event streaming via MQTT topics |

#### File-Based Methods (EventDrivenIngestionMethod)

TSIGMA watches local directories for files dropped by external processes.

| Module | Protocol | Dependencies | Use Case |
|--------|----------|--------------|----------|
| `directory_watch.py` | Filesystem events | watchdog | CSV/dat batch import from local directory |

#### ATSPM 4.x Parity

| ATSPM 4.x Method | TSIGMA Equivalent | Notes |
|-------------------|-------------------|-------|
| FTP/FTPS pull | `ftp_pull.py` | Also handles SFTP via protocol enum |
| HTTP pull | `http_pull.py` | Econolite MaxTime XML API |
| UDP listener | `udp_server.py` | Wavetronics speed data |
| TCP listener | `tcp_server.py` | Legacy Wavetronics alternative |
| CSV file import | `directory_watch.py` | Watch directory for dropped files |
| N/A (new) | `nats_listener.py` | Real-time NATS streaming |
| N/A (new) | `mqtt_listener.py` | Real-time MQTT streaming |
| WCF remote trigger | `POST /api/v1/signals/{id}/poll` | REST API endpoint (not a plugin) |
| SNMP logging toggle | `ftp_pull.py` rotate mode | SNMP-controlled file rotation for controllers that append to log files |

**If you don't use a method, simply exclude that `.py` file from your deployment.**

---

## Decoder Plugin Architecture

**CRITICAL CONCEPT:** Decoders are also **optional plugin modules**. You only include the decoders you actually use.

### Available Decoder Plugins

Each module in `tsigma/collection/decoders/` is an independent plugin:

| Module | Vendor | Format | Extensions | Use Case |
|--------|--------|--------|------------|----------|
| `asc3.py` | Econolite | Binary (Hi-Res) | `.dat`, `.datz` | Econolite ASC/3 controllers |
| `siemens.py` | Siemens | Text (SEPAC) | `.log`, `.txt`, `.csv`, `.sepac` | Siemens controllers |
| `peek.py` | Peek/McCain | Binary (ATC) | `.bin`, `.dat`, `.atc`, `.log` | Peek/McCain ATC controllers |
| `maxtime.py` | Intelight | XML/Binary | `.xml`, `.maxtime`, `.mtl`, `.bin`, `.synchro` | MaxTime/Trafficware controllers |
| `csv_decoder.py` | Generic | CSV | `.csv`, `.txt`, `.tsv` | Custom CSV exports |
| `openphase.py` | OpenPhase | Protobuf | `.pb`, `.proto`, `.bin` | OpenPhase v1 protobuf (NATS/MQTT streaming) |
| `auto.py` | Auto-detect | Any | `.*` | Auto-detection wrapper |

**If you don't need a decoder, simply exclude that `.py` file from your deployment.**

### Decoder Registry Pattern

Same self-registering pattern as ingestion methods:

```python
# tsigma/collection/decoders/base.py

class DecoderRegistry:
    """Central registry for all decoder plugins."""
    _decoders: dict[str, type[BaseDecoder]] = {}

    @classmethod
    def register(cls, decoder_cls: type[BaseDecoder]) -> type[BaseDecoder]:
        """Register a decoder plugin."""
        cls._decoders[decoder_cls.name] = decoder_cls
        return decoder_cls

# tsigma/collection/decoders/asc3.py

@DecoderRegistry.register
class ASC3Decoder(BaseDecoder):
    name = "asc3"
    extensions = [".dat", ".datz"]
    description = "Econolite ASC/3 Hi-Resolution event log"

    def decode_bytes(self, data: bytes) -> list[DecodedEvent]:
        # ASC/3-specific decoding logic
        ...
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

### Adding a Custom Decoder

1. Create `tsigma/collection/decoders/myvendor.py`
2. Subclass `BaseDecoder`
3. Decorate with `@DecoderRegistry.register`
4. Implement `decode_bytes()` and `can_decode()` methods
5. No changes needed to ingestion service — auto-discovered on import

**See [DECODERS.md](DECODERS.md) for complete decoder documentation.**

---

## Polling Checkpoint (Persistent State)

**CRITICAL CONCEPT:** TSIGMA uses a persistent checkpoint mechanism to track polling progress. This is a deliberate departure from ATSPM's approach.

### Why This Exists

| System | Approach | Problem |
|--------|----------|---------|
| ATSPM 4.x | Deletes files from controller after FTP download | Destructive — second consumer gets nothing |
| ATSPM 5.x | Excludes newest file by modification time (in-memory) | Lost on restart — re-downloads everything |
| TSIGMA | Persistent `polling_checkpoint` table in database | Non-destructive, restartable, multi-consumer safe |

### How It Works

Each `(signal_id, method)` pair maintains an independent checkpoint row in the `polling_checkpoint` table. The checkpoint is only advanced after successful ingest — never on failure.

**FTP/SFTP polling:**
1. Read checkpoint: `last_file_mtime`, `files_hash`
2. List remote files → compute hash of filenames
3. If `files_hash` unchanged → skip (no new files)
4. Filter files where `file.mtime > last_file_mtime`
5. Download → decode → ingest new files only
6. Update checkpoint on success

**HTTP polling:**
1. Read checkpoint: `last_event_timestamp`
2. Query controller API with `?since=last_event_timestamp`
3. Decode → ingest returned events
4. Update checkpoint on success

### Checkpoint Guarantees

- **Non-destructive**: Files are never deleted from the controller
- **Crash-safe**: Checkpoint only advances after successful ingest
- **Restart-safe**: Persistent in database, survives service restarts
- **Multi-consumer safe**: Each ingestion method maintains its own checkpoint
- **Idempotent**: Re-processing the same file produces duplicate events, but the composite PK on `controller_event_log` rejects duplicates via `ON CONFLICT DO NOTHING`

### Error Handling

The checkpoint tracks `consecutive_errors`, `last_error`, and `last_error_time`. This enables:

- **Automatic backoff**: Skip signals with too many consecutive failures
- **Health monitoring**: Query `WHERE consecutive_errors > 0` for failing signals
- **Self-healing**: `consecutive_errors` resets to 0 on next successful poll

### Schema Reference

See [DATABASE_SCHEMA.md — Polling Checkpoint Table](DATABASE_SCHEMA.md#polling-checkpoint-table) for the full table definition, indexes, and lifecycle documentation.

---

## Checkpoint Resilience

**Problem:** Current ATSPM 4.x/5.x systems break entirely when a controller's clock produces future-dated files or events. The checkpoint advances past real time and all subsequent polls return zero new data. Additionally, ATSPM 4.x deletes files from the controller after FTP download, risking data loss if ingestion fails between download and database insert.

TSIGMA addresses this with a four-part resilience system.

### 1. File-Based Checkpointing (FTP/SFTP)

FTP/SFTP methods use **file identity only** (name + size + mtime) for checkpointing. Event timestamps inside files are never used to determine the checkpoint position.

This means future-dated events inside a file have zero effect on the checkpoint. The checkpoint tracks which files have been downloaded, not what timestamps those files contain.

### 2. Checkpoint Cap (HTTP/Push Methods)

For HTTP and push methods that use event timestamps for incremental collection, the checkpoint is capped:

```
saved_checkpoint = min(latest_event_timestamp, server_time + tolerance)
```

Configuration:
```python
checkpoint_future_tolerance_seconds: int = 300  # 5 minutes (default)
```

If a controller returns events dated 3 hours in the future, the checkpoint advances only to `now + 5 minutes` rather than 3 hours ahead. The future-dated events are still ingested (data is never dropped) but they cannot poison the checkpoint.

### 3. Clock Drift Detection and Notification

During event persistence, all events are checked against `server_time + tolerance`. Events exceeding this threshold trigger a WARNING notification via the [notification plugin system](NOTIFICATIONS.md):

- **Alert type**: `clock_drift`
- **Severity**: WARNING
- **Metadata**: signal_id, future_event_count, total_event_count, max_drift_seconds, latest_event_time

Events are always ingested regardless of timestamp. TSIGMA never drops data.

### 4. Silent Signal Detection and Auto-Recovery

After each poll cycle, the `CollectorService` checks for signals that returned zero events:

1. **Increment**: `consecutive_silent_cycles` counter for each signal that produced no events this cycle (determined by checking if `last_successful_poll` is older than `poll_interval * 1.5`)
2. **Threshold**: After N consecutive silent cycles (configurable via `checkpoint_silent_cycles_threshold`, default 3), investigate the checkpoint
3. **Investigate**: Check if `last_event_timestamp > server_time + tolerance` (poisoned checkpoint)
4. **Auto-recover**: If poisoned, roll back `last_event_timestamp` to current server time and reset `consecutive_silent_cycles` to 0
5. **Notify**: CRITICAL notification for poisoned recovery, WARNING for non-poisoned silence

| Scenario | Checkpoint State | Action | Severity |
|----------|-----------------|--------|----------|
| Signal silent, checkpoint in future | Poisoned | Auto-rollback to server time | CRITICAL |
| Signal silent, checkpoint normal | Communication issue | Notify operator | WARNING |

### Timestamp Correction Tools

For already-ingested poisoned data, operators can correct timestamps via admin-only API endpoints:

**Bulk correction** (`POST /api/v1/collection/corrections/bulk`):
```json
{
  "signal_id": "gdot-0142",
  "start_time": "2026-04-01T00:00:00Z",
  "end_time": "2026-04-01T12:00:00Z",
  "offset_seconds": -10800
}
```
Applies a fixed offset to all event_time values in the specified window.

**Anchor correction** (`POST /api/v1/collection/corrections/anchor`):
```json
{
  "signal_id": "gdot-0142",
  "event_time": "2026-04-01T15:30:00Z",
  "actual_time": "2026-04-01T12:30:00Z",
  "start_time": "2026-04-01T00:00:00Z",
  "end_time": "2026-04-01T23:59:59Z"
}
```
Operator identifies a known-good event and its real-world timestamp. The system computes the offset (`actual_time - event_time = -10800s`) and applies it to all events in the range.

### Configuration Reference

| Setting | Default | Description |
|---------|---------|-------------|
| `checkpoint_future_tolerance_seconds` | 300 | Maximum allowed drift before capping (seconds) |
| `checkpoint_silent_cycles_threshold` | 3 | Consecutive zero-event cycles before alerting |

See [WATCHDOG.md](WATCHDOG.md) for the complete data quality monitoring design.

---

## Pipeline Abstraction

Same code works with different backends:

```python
class EventPipeline(ABC):
    @abstractmethod
    async def publish(self, stage: str, payload: dict) -> None: ...

    @abstractmethod
    async def subscribe(self, stage: str) -> AsyncIterator[dict]: ...
```

## Pipeline Modes

| Mode | Backend | Use Case |
|------|---------|----------|
| **direct** | None | Default, < 2,000 signals |
| **postgres** | PostgreSQL table | Persistence, retries |
| **valkey** | Valkey streams | High throughput |

### Direct Mode (Default)

```
Download → Decode → Store (all in one process)
```

### Queue Mode (Enterprise)

```
Download → Queue → Decode → Queue → Store

Benefits:
- Failed decode retries automatically
- Scale decoders independently
- Download continues regardless of decode backlog
```

### Configuration

```yaml
# Standard (default)
ingestion:
  pipeline: direct

# Enterprise
ingestion:
  pipeline: postgres  # or: valkey
```

## Signal Sharding (Multi-Worker)

```python
WORKER_ID = int(os.environ.get("WORKER_ID", 0))
WORKER_COUNT = int(os.environ.get("WORKER_COUNT", 1))

def get_my_signals(all_signals: list[Signal]) -> list[Signal]:
    return [s for i, s in enumerate(all_signals) if i % WORKER_COUNT == WORKER_ID]
```

## Ingestion Method Base Classes

### Config Validation

Each ingestion method defines a Pydantic `BaseModel` config class for its
connection parameters (host, port, credentials, paths, etc.). The raw
`dict[str, Any]` from `signal_metadata` JSONB is validated on construction:

```python
from pydantic import BaseModel, Field

class FTPPullConfig(BaseModel):
    host: str                           # required — missing raises ValidationError
    signal_id: str
    protocol: FTPProtocol = FTPProtocol.FTP
    port: int | None = None
    username: str = "anonymous"
    password: str = ""
    remote_dir: str = "/"
    file_extensions: list[str] = Field(default_factory=lambda: [".dat", ".csv", ".log"])
```

Bad config fails immediately at startup with clear field-level errors,
not at 2am when the first poll runs.

### Base Classes

Three base classes in `registry.py` support different collection patterns:

### PollingIngestionMethod (Pull)

TSIGMA connects to the controller on a schedule. CollectorService calls `poll_once()` per signal per interval.

```python
class PollingIngestionMethod(BaseIngestionMethod):
    execution_mode: ClassVar[ExecutionMode] = ExecutionMode.POLLING

    @abstractmethod
    async def poll_once(self, signal_id: str, config: dict[str, Any], session_factory) -> None:
        """Execute one poll cycle for a single signal."""
        ...
```

**Implementations:** `ftp_pull.py`, `http_pull.py`

### ListenerIngestionMethod (Push)

External devices push data to TSIGMA. Long-lived async servers with start/stop lifecycle managed by CollectorService.

```python
class ListenerIngestionMethod(BaseIngestionMethod):
    execution_mode: ClassVar[ExecutionMode] = ExecutionMode.LISTENER

    @abstractmethod
    async def start(self, config: dict[str, Any], session_factory) -> None:
        """Start listening for incoming data."""
        ...

    @abstractmethod
    async def stop(self) -> None:
        """Stop listening and release resources."""
        ...
```

**Implementations:** `udp_server.py`, `tcp_server.py`, `nats_listener.py`, `mqtt_listener.py`

### EventDrivenIngestionMethod (Watch)

TSIGMA watches local directories for files dropped by external processes. Uses filesystem events, not polling intervals.

```python
class EventDrivenIngestionMethod(BaseIngestionMethod):
    execution_mode: ClassVar[ExecutionMode] = ExecutionMode.EVENT_DRIVEN

    @abstractmethod
    async def start(self, config: dict[str, Any], session_factory) -> None:
        """Start watching for filesystem events."""
        ...

    @abstractmethod
    async def stop(self) -> None:
        """Stop watching and release resources."""
        ...
```

**Implementations:** `directory_watch.py`

## On-Demand Poll API (WCF Compatibility)

ATSPM 4.x exposes a WCF `UploadControllerData` SOAP endpoint that lets external tools trigger an immediate FTP pull for a specific signal. DOTs have existing SOAP client integrations built around this call — we cannot break them.

TSIGMA provides **two interfaces** to the same internal trigger:

### 1. SOAP Endpoint (ATSPM 4.x Compatible)

`POST /api/v1/soap/GetControllerData`

Accepts the same SOAP envelope that ATSPM 4.x WCF expects. DOTs point their existing SOAP clients at TSIGMA with zero code changes.

**ATSPM 4.x WCF Contract (from `IGetControllerData.cs`):**
```csharp
void UploadControllerData(
    string IPAddress,
    string SignalID,
    string UserName,
    string Password,
    string LocalDir,
    string RemoteDir,
    bool DeleteFiles,         // Accepted but ignored — TSIGMA never deletes
    int SNMPRetry,            // Accepted but ignored — TSIGMA doesn't toggle logging
    int SNMPTimeout,          // Accepted but ignored
    int SNMPPort,             // Accepted but ignored
    bool ImportAfterFTP,
    bool ActiveMode,
    int WaitBetweenRecords,
    BulkCopyOptions Options
)
```

**TSIGMA SOAP request (same envelope format):**
```xml
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
  <s:Body>
    <UploadControllerData xmlns="http://tempuri.org/">
      <IPAddress>192.168.1.100</IPAddress>
      <SignalID>gdot-0142</SignalID>
      <UserName>atspm</UserName>
      <Password>secret123</Password>
      <LocalDir>/tmp/tsigma/ftp_cache</LocalDir>
      <RemoteDir>/data/logs</RemoteDir>
      <DeleteFiles>false</DeleteFiles>
      <SNMPRetry>0</SNMPRetry>
      <SNMPTimeout>0</SNMPTimeout>
      <SNMPPort>161</SNMPPort>
      <ImportAfterFTP>true</ImportAfterFTP>
      <ActiveMode>false</ActiveMode>
      <WaitBetweenRecords>0</WaitBetweenRecords>
    </UploadControllerData>
  </s:Body>
</s:Envelope>
```

**TSIGMA SOAP response:**
```xml
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
  <s:Body>
    <UploadControllerDataResponse xmlns="http://tempuri.org/">
      <Status>Accepted</Status>
      <SignalID>gdot-0142</SignalID>
    </UploadControllerDataResponse>
  </s:Body>
</s:Envelope>
```

**Implementation:** A thin FastAPI route that:
1. Accepts `Content-Type: text/xml` with the SOAP envelope
2. Parses `UploadControllerData` parameters from XML body via `lxml` or `xml.etree`
3. Maps SOAP parameters to TSIGMA config (ignores `DeleteFiles`, `SNMP*` fields)
4. Triggers `poll_once()` with the extracted config
5. Returns a SOAP response envelope

```python
@router.post("/soap/GetControllerData")
async def soap_upload_controller_data(request: Request):
    body = await request.body()
    params = _parse_soap_envelope(body)  # Extract UploadControllerData fields

    config = {
        "host": params["IPAddress"],
        "username": params["UserName"],
        "password": params["Password"],
        "remote_dir": params["RemoteDir"],
        "passive_mode": not params.get("ActiveMode", False),
        "protocol": "ftp",
    }

    method = collector_service.get_method("ftp_pull")
    asyncio.create_task(
        method.poll_once(params["SignalID"], config, session_factory)
    )

    return Response(
        content=_build_soap_response(params["SignalID"], "Accepted"),
        media_type="text/xml",
    )
```

**Ignored parameters** (accepted for compatibility, logged, not acted on):
- `DeleteFiles` — TSIGMA never deletes files from controllers
- `SNMPRetry`, `SNMPTimeout`, `SNMPPort` — TSIGMA handles SNMP via rotate mode config, not SOAP parameters
- `LocalDir` — TSIGMA manages its own cache
- `WaitBetweenRecords`, `BulkCopyOptions` — not applicable to async pipeline

### 2. REST Endpoint (New Integrations)

`POST /api/v1/signals/{signal_id}/poll`

Modern REST interface for new integrations. Same internal trigger, cleaner API.

**Request:**
```json
{
  "method": "ftp_pull"
}
```

**Response (202 Accepted):**
```json
{
  "signal_id": "gdot-0142",
  "method": "ftp_pull",
  "status": "started",
  "message": "Poll cycle triggered"
}
```

```python
@router.post("/signals/{signal_id}/poll", status_code=202)
async def trigger_poll(signal_id: str, request: PollRequest):
    method = collector_service.get_method(request.method)
    signal = await get_signal(signal_id)
    config = signal.signal_metadata.get("collection", {})
    config["host"] = str(signal.ip_address)
    asyncio.create_task(
        method.poll_once(signal_id, config, session_factory)
    )
    return {"signal_id": signal_id, "method": request.method, "status": "started"}
```

### Migration Path

| Phase | DOT Action | TSIGMA Endpoint |
|-------|-----------|-----------------|
| **Day 1** | No changes — existing SOAP clients work | `/api/v1/soap/GetControllerData` |
| **Optional** | Update scripts to REST when convenient | `/api/v1/signals/{id}/poll` |
| **Future** | SOAP endpoint remains indefinitely | Both endpoints supported |

## Event Decoders

Support for multiple event log formats:

| Format | Decoder | Description |
|--------|---------|-------------|
| **ASC/3** | `asc3` | Econolite ASC/3 Hi-Resolution event log |
| **SEPAC** | `siemens` | Siemens SEPAC event log |
| **ATC/Peek** | `peek` | Peek/McCain binary event log |
| **MaxTime** | `maxtime` | MaxTime/Trafficware/MaxView event log |
| **CSV** | `csv` | Generic CSV/TSV event log |
| **OpenPhase** | `openphase` | OpenPhase v1 Protobuf (events and batches) |
| **Auto** | `auto` | Auto-detect event log format |

```python
# Decoder abstraction (tsigma/collection/decoders/base.py)

@dataclass
class DecodedEvent:
    """Single decoded event from a controller event log."""
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

# Auto-detection (tsigma/collection/decoders/auto.py)
# Probes registered decoders in priority order: asc3 → peek → maxtime → siemens → csv
_PRIORITY = ["asc3", "peek", "maxtime", "siemens", "csv"]

class AutoDecoder(BaseDecoder):
    def decode_bytes(self, data: bytes) -> list[DecodedEvent]:
        for name in _PRIORITY:
            decoder_cls = DecoderRegistry.get(name)
            if decoder_cls.can_decode(data):
                return decoder_cls().decode_bytes(data)
        raise ValueError("No decoder found for the provided data")
```

## Signal Configuration for Protocols

Per-signal collection configuration is stored in the `signal_metadata` JSONB column on the `signal` table. The `CollectorService` reads the `collection` key to determine method, credentials, and decoder.

```python
# signal_metadata JSONB example: FTP pull with explicit decoder
{
    "collection": {
        "method": "ftp_pull",
        "protocol": "sftp",
        "remote_dir": "/logs",
        "decoder": "asc3",
        "username": "atspm",
        "password": "encrypted:...",
    }
}

# signal_metadata JSONB example: NATS listener
{
    "collection": {
        "method": "nats_listener",
        "url": "nats://localhost:4222",
        "subject": "signals.1001.events",
        "decoder": "openphase",
    }
}
```
