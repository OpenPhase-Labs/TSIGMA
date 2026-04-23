# TSIGMA Coding Guidelines

**Purpose**: Base requirements for TSIGMA code contributions.

**Last Updated**: 2026-03-05

---

## Core Principles

TSIGMA follows three non-negotiable principles:

1. **TDD** - Test-Driven Development
2. **SRP** - Single Responsibility Principle
3. **DRY** - Don't Repeat Yourself

These aren't strict dogma, but they're base requirements. Code that violates these principles won't be merged.

---

## 1. Test-Driven Development (TDD)

### The Rule

**Write tests first, then write code to pass them.**

### Workflow

```
1. RED   - Write a failing test (import error or assertion failure)
2. GREEN - Write minimum code to make it pass
3. REFACTOR - Clean up while keeping tests green
```

### Example

**1. RED - Write failing test first:**
```python
# tests/unit/collection/decoders/test_asc3.py

def test_decode_phase_event():
    """Test decoding ASC/3 phase begin event."""
    raw = bytes([0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08])
    decoder = ASC3Decoder()  # Doesn't exist yet - test fails

    events = decoder.decode(raw)

    assert len(events) == 1
    assert events[0].event_code == 1
```

**2. GREEN - Write minimum code to pass:**
```python
# tsigma/collection/decoders/asc3.py

class ASC3Decoder:
    def decode(self, raw: bytes) -> list[DecodedEvent]:
        # Minimum implementation to pass test
        event_code = raw[0]
        return [DecodedEvent(event_code=event_code, ...)]
```

**3. REFACTOR - Clean up:**
```python
# tsigma/collection/decoders/asc3.py

class ASC3Decoder:
    """ASC/3 binary event log decoder."""

    HEADER_SIZE = 20
    RECORD_SIZE = 4

    def decode(self, raw: bytes) -> list[DecodedEvent]:
        """Decode ASC/3 binary format."""
        # Skip header
        data = raw[self.HEADER_SIZE:]

        # Parse records
        events = []
        for i in range(0, len(data), self.RECORD_SIZE):
            record = data[i:i + self.RECORD_SIZE]
            events.append(self._parse_record(record))

        return events

    def _parse_record(self, record: bytes) -> DecodedEvent:
        """Parse single ASC/3 record."""
        event_code = record[0]
        event_param = record[1]
        timestamp = struct.unpack('<H', record[2:4])[0]

        return DecodedEvent(
            event_code=event_code,
            event_param=event_param,
            timestamp=self._decode_timestamp(timestamp)
        )
```

### When to Write Tests

**Always.** Every new function, class, or module needs tests.

**Exceptions** (rare):
- Throwaway prototypes (mark clearly as "prototype", delete before merge)
- Scripts (one-off data migration scripts)

### Test Organization

See [TESTING.md](TESTING.md) for complete test structure and fixtures.

---

## 2. Single Responsibility Principle (SRP)

### The Rule

**Each class/function should do ONE thing.**

### Examples

#### ✅ Good: Single Responsibility

```python
# Each class has ONE clear purpose

class ASC3Decoder:
    """Decodes ASC/3 binary format."""
    def decode(self, raw: bytes) -> list[DecodedEvent]: ...

class FTPPoller:
    """Polls FTP servers for event files."""
    async def poll(self, device: Device) -> list[File]: ...

class IngestionService:
    """Ingests decoded events into database."""
    async def ingest(self, events: list[DecodedEvent]) -> int: ...
```

#### ❌ Bad: Multiple Responsibilities

```python
# This class does TOO MUCH
class ControllerDataHandler:
    """Handles controller data."""  # Vague purpose

    async def process_controller(self, device: Device):
        # 1. Polls FTP
        files = await self._ftp_poll(device)

        # 2. Decodes data
        events = self._decode_asc3(files[0])

        # 3. Stores in database
        await self._insert_events(events)

        # 4. Sends email notification
        await self._send_email(device.admin_email)

        # 5. Updates cache
        await self._refresh_cache(device.id)
```

**Fix**: Split into separate classes (FTPPoller, ASC3Decoder, IngestionService, EmailNotifier, CacheManager).

### How to Check

Ask: "Can I describe this class/function in one sentence without using 'and'?"

- ✅ "ASC3Decoder decodes ASC/3 binary format"
- ❌ "ControllerDataHandler polls FTP **and** decodes data **and** stores events **and** sends emails"

If you need "and", split it up.

---

## 3. Don't Repeat Yourself (DRY)

### The Rule

**Don't copy-paste code. Extract shared logic.**

### Examples

#### ❌ Bad: Repeated Code

```python
# tsigma/collection/methods/ftp_pull.py
async def poll_ftp(device: Device):
    if not device.enabled:
        return
    if device.last_poll_time and (datetime.now() - device.last_poll_time) < timedelta(minutes=15):
        return
    # ... FTP logic

# tsigma/collection/methods/sftp_pull.py
async def poll_sftp(device: Device):
    if not device.enabled:
        return
    if device.last_poll_time and (datetime.now() - device.last_poll_time) < timedelta(minutes=15):
        return
    # ... SFTP logic
```

**Problem**: Repeated enabled check and rate limiting logic.

#### ✅ Good: Extract Shared Logic

```python
# tsigma/collection/base.py
class BaseIngestionMethod(ABC):
    """Base class for all ingestion methods."""

    def _should_poll(self, device: Device) -> bool:
        """Check if device should be polled (shared logic)."""
        if not device.enabled:
            return False

        if device.last_poll_time:
            elapsed = datetime.now() - device.last_poll_time
            if elapsed < timedelta(minutes=15):
                return False

        return True

    @abstractmethod
    async def poll(self, device: Device) -> list[File]:
        """Poll device (subclass implements protocol-specific logic)."""
        ...

# tsigma/collection/methods/ftp_pull.py
class FTPPullMethod(BaseIngestionMethod):
    async def poll(self, device: Device) -> list[File]:
        if not self._should_poll(device):  # Shared logic
            return []

        # FTP-specific logic only
        async with aioftp.Client() as client:
            await client.connect(device.host)
            files = await client.list()
            return files

# tsigma/collection/methods/sftp_pull.py
class SFTPPullMethod(BaseIngestionMethod):
    async def poll(self, device: Device) -> list[File]:
        if not self._should_poll(device):  # Shared logic
            return []

        # SFTP-specific logic only
        async with asyncssh.connect(device.host) as conn:
            files = await conn.listdir()
            return files
```

### When to Extract

**Rule of thumb**: If you copy-paste code more than once, extract it.

**Two instances**: Consider extracting (depends on complexity)
**Three instances**: Extract immediately

---

## Additional Guidelines (Not Strict, But Recommended)

### Naming Conventions

- **Classes**: `PascalCase` (e.g., `ASC3Decoder`, `FTPPoller`)
- **Functions**: `snake_case` (e.g., `decode_events`, `poll_device`)
- **Constants**: `UPPER_SNAKE_CASE` (e.g., `HEADER_SIZE`, `MAX_RETRIES`)
- **Private methods**: `_leading_underscore` (e.g., `_parse_record`)

### Function Length

**Guideline**: Keep functions under 50 lines.

**Why**: Long functions usually violate SRP.

**If a function exceeds 50 lines**: Consider splitting into smaller functions.

### Module Size

**HARD LIMIT: Files must not exceed 1000 lines.**

**Why**: Large files violate SRP and are hard to maintain.

**If a module approaches 1000 lines**: Split into sub-modules NOW.

**Don't cheat**: No cramming logic into complicated one-liners to reduce line count.

```python
# ❌ Bad: Complicated one-liner to game line count
result = [compute_quality(c) for c in corridors if c.enabled and c.signal_count > 5 and validate_corridor(c) and c.last_update > cutoff]

# ✅ Good: Clear, readable code
result = []
for corridor in corridors:
    if not corridor.enabled:
        continue
    if corridor.signal_count <= 5:
        continue
    if not validate_corridor(corridor):
        continue
    if corridor.last_update <= cutoff:
        continue

    result.append(compute_quality(corridor))
```

### Line Length

**LIMIT: 100 characters per line.**

**Why**: Code should be readable in split-screen editors and code reviews.

**Can be exceeded**: Only with good reason (long URLs, string literals, etc.)

```python
# ❌ Bad: Exceeds limit unnecessarily
signal = await session.execute(select(Signal).where(Signal.signal_id == signal_id).where(Signal.enabled == True))

# ✅ Good: Break into multiple lines
result = await session.execute(
    select(Signal)
    .where(Signal.signal_id == signal_id)
    .where(Signal.enabled == True)
)

# ✅ Acceptable: Long URL (good reason to exceed)
download_url = "https://cdn.jsdelivr.net/npm/echarts@5.4.3/dist/echarts.min.js?integrity=sha384-abc123..."
```

### Indentation Depth

**HARD LIMIT: Maximum 4 levels of indentation.**

**Why**: Deep nesting is hard to read and usually violates SRP.

**Solution**: Use early exits (guard clauses).

#### ❌ Bad: Deep Nesting (6 levels)

```python
async def process_event(event: Event):
    if event.enabled:
        if event.event_code > 0:
            if event.signal_id:
                signal = await get_signal(event.signal_id)
                if signal:
                    if signal.enabled:
                        if await validate_event(event):
                            # Actual logic buried 6 levels deep
                            await store_event(event)
```

#### ✅ Good: Early Exits (2 levels max)

```python
async def process_event(event: Event):
    # Guard clauses - exit early
    if not event.enabled:
        return

    if event.event_code <= 0:
        return

    if not event.signal_id:
        return

    signal = await get_signal(event.signal_id)
    if not signal:
        return

    if not signal.enabled:
        return

    if not await validate_event(event):
        return

    # Actual logic at top level (1 indent)
    await store_event(event)
```

### How to Fix Deep Nesting

1. **Use early returns** (guard clauses)
2. **Extract nested logic** into separate functions
3. **Invert conditions** to flatten structure

### JavaScript Documentation

**ALL JavaScript functions** must have JSDoc comments.

```javascript
/**
 * Load volume data from API and update chart.
 *
 * @param {string} signalId - Signal identifier
 * @param {string} startDate - Start date (ISO 8601)
 * @param {string} endDate - End date (ISO 8601)
 * @returns {Promise<void>}
 */
async function loadVolumeData(signalId, startDate, endDate) {
    const response = await fetch(`/api/v1/analytics/volume?` + new URLSearchParams({
        signal_id: signalId,
        start: startDate,
        end: endDate
    }));

    const data = await response.json();

    chart.setOption({
        xAxis: { data: data.bins },
        series: [{ data: data.volumes }]
    });
}

/**
 * Initialize PCD chart with empty state.
 *
 * @returns {echarts.ECharts} Initialized chart instance
 */
function initializePCDChart() {
    const chart = echarts.init(document.getElementById('pcd-chart'));

    chart.setOption({
        title: { text: 'Purdue Coordination Diagram' },
        xAxis: { type: 'time' },
        yAxis: { name: 'Detector' },
        series: []
    });

    return chart;
}
```

**Even simple helpers need JSDoc:**
```javascript
/**
 * Format timestamp for display.
 *
 * @param {Date} date - Date object
 * @returns {string} Formatted string (YYYY-MM-DD HH:mm:ss)
 */
function formatTimestamp(date) {
    return date.toISOString().replace('T', ' ').slice(0, 19);
}
```

---

## Type Hints

**Required** for all function signatures.

```python
# ✅ Good
async def get_events(
    session: AsyncSession,
    signal_id: str,
    start: datetime,
    end: datetime
) -> list[ControllerEventLog]:
    ...

# ❌ Bad (no type hints)
async def get_events(session, location_id, start, end):
    ...
```

---

## Error Handling

### Be Specific

```python
# ❌ Bad: Catch everything
try:
    data = await decode_file(path)
except Exception:
    pass  # Silently fails

# ✅ Good: Catch specific errors
try:
    data = await decode_file(path)
except FileNotFoundError:
    logger.warning("file_not_found", path=path)
    return None
except DecodeError as e:
    logger.error("decode_failed", path=path, error=str(e))
    raise
```

### Don't Swallow Errors

If you catch an exception, **do something** with it:
- Log it
- Re-raise it
- Return a default value

**Never** silently catch and ignore.

---

## Documentation

### Docstrings Required For

**ALL Python functions and classes.** No exceptions.

- Public functions: Always
- Private functions: Always
- Simple one-liners: Always (even if obvious)
- Helper functions: Always

### Format

```python
def compute_corridor_quality(
    corridors: list[Route],
    start: datetime,
    end: datetime
) -> dict[UUID, float]:
    """
    Compute coordination quality for corridors.

    Args:
        corridors: List of corridor routes
        start: Analysis period start
        end: Analysis period end

    Returns:
        Dictionary mapping corridor ID to quality score (0-100)

    Raises:
        ValueError: If start >= end
    """
    ...
```

---

## Database Access

### Use Repositories for Complex Queries

```python
# ✅ Good: Complex query in repository
class EventRepository:
    async def get_events_for_pcd(
        self,
        session: AsyncSession,
        signal_id: str,
        phase: int,
        start: datetime,
        end: datetime
    ) -> list[Event]:
        """Get events for PCD chart (complex join/filter)."""
        query = (
            select(Event)
            .join(Detector)
            .where(Event.signal_id == signal_id)
            .where(Event.event_param == phase)
            .where(Event.event_time.between(start, end))
            .order_by(Event.event_time)
        )
        result = await session.execute(query)
        return result.scalars().all()

# ❌ Bad: Complex query scattered in route handler
@app.get("/api/v1/analytics/pcd")
async def get_pcd(signal_id: str, ...):
    # Don't put SQL logic directly in routes
    query = select(Event).join(Detector).where(...)  # ❌
    ...
```

### Use SQLAlchemy Directly for Simple CRUD

```python
# ✅ Good: Simple CRUD doesn't need a repository
@app.get("/api/v1/signals/{signal_id}")
async def get_signal(signal_id: str, session: AsyncSession = Depends(get_session)):
    signal = await session.get(Signal, signal_id)
    if not signal:
        raise HTTPException(404)
    return signal
```

---

## Migrations

**MUST be idempotent.** Non-negotiable.

See [ARCHITECTURE.md § Database Migrations](ARCHITECTURE.md#database-migrations---critical-rules) and [DATABASE.md § Migrations](DATABASE.md#migrations-alembic) for complete rules.

---

## Plugin Pattern

Use the registry pattern for extensibility:

- **Ingestion methods**: `@IngestionMethodRegistry.register("method_name")`
- **Background jobs**: `@JobRegistry.register(name="job_name", trigger="cron")`
- **Reports**: `@ReportRegistry.register("report-name")`

See [INGESTION.md](INGESTION.md), [ARCHITECTURE.md § Background Jobs](ARCHITECTURE.md#9-background-jobs--scheduling), and [REPORTS.md](REPORTS.md) for details.

---

## Code Review Checklist

Before submitting a PR:

- [ ] **Tests written first** (TDD - RED, GREEN, REFACTOR)
- [ ] **All tests passing** (`pytest tests/`)
- [ ] **Single Responsibility** - Each class/function does ONE thing
- [ ] **No code duplication** - Shared logic extracted to common functions
- [ ] **Type hints** - All function signatures have types
- [ ] **Docstrings** - Public functions documented
- [ ] **Migrations idempotent** - If touching database schema
- [ ] **No core table modifications** - Custom jobs use their own tables only
- [ ] **Plugin pattern** - If adding ingestion method, job, or report

---

## When These Rules Can Be Bent

**Prototypes**: Mark clearly as "prototype", don't merge until properly tested.

**Scripts**: One-off data migration scripts don't need full TDD (but should have basic error handling).

**Emergency fixes**: Production hotfixes can skip TDD initially, but **must** have tests added before merge to main.

---

## Questions?

If you're unsure whether your code violates these principles, ask before spending time on implementation.

Better to clarify upfront than to rewrite later.

---

**Document Version**: 1.0
**Last Updated**: 2026-03-05
**Owner**: TSIGMA Development Team
