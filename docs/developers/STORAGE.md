# File Storage Backend

## Overview

TSIGMA includes a pluggable file storage subsystem for cold-tier exports, backups, raw device file archives, and any other blob-oriented I/O. Two backends ship out of the box:

- **Filesystem** -- stores files under a local directory (default).
- **S3** -- stores files in any S3-compatible object store (AWS S3, MinIO, etc.).

The active backend is selected at startup via the `TSIGMA_STORAGE_BACKEND` environment variable and accessed through a single factory function.

## Architecture

```
tsigma/storage/
    __init__.py          Public API: StorageBackend, StoredFile, get_storage_backend
    base.py              ABC + StoredFile dataclass
    factory.py           get_storage_backend() factory
    filesystem.py        FilesystemBackend
    s3.py                S3Backend (requires aiobotocore)
```

### StoredFile

A `@dataclass` returned by `put()` and `list_files()`:

| Field            | Type               | Default                      |
|------------------|--------------------|------------------------------|
| `key`            | `str`              | --                           |
| `size`           | `int`              | --                           |
| `last_modified`  | `datetime`         | --                           |
| `metadata`       | `dict[str, str]`   | `{}`                         |
| `content_type`   | `str`              | `"application/octet-stream"` |

### StorageBackend ABC

All backends implement six async methods:

| Method       | Signature                                                        | Notes                                                                 |
|--------------|------------------------------------------------------------------|-----------------------------------------------------------------------|
| `put`        | `(key, data, metadata=None) -> StoredFile`                       | Stores bytes at `key`. Optional metadata dict.                        |
| `get`        | `(key) -> bytes`                                                 | Returns raw bytes. Raises `FileNotFoundError` if missing.             |
| `delete`     | `(key) -> None`                                                  | Deletes the key. No-op if it does not exist.                          |
| `exists`     | `(key) -> bool`                                                  | Returns `True` when the key exists.                                   |
| `list_files` | `(prefix) -> AsyncIterator[StoredFile]`                          | Yields `StoredFile` for every file under the prefix.                  |
| `get_url`    | `(key, expires_in=3600) -> str`                                  | Filesystem: `file://` URI. S3: presigned URL (default 1-hour expiry). |

### StorageFactory

`get_storage_backend()` reads `settings.storage_backend`, instantiates the matching backend, and returns it. If the value is unrecognized it logs a warning and falls back to `FilesystemBackend`.

```python
from tsigma.storage import get_storage_backend

storage = get_storage_backend()
info = await storage.put("exports/2025-04-10.csv", csv_bytes)
```

The S3 backend import is deferred (inside the `if` branch) so that `aiobotocore` is only required when S3 is actually selected.

## Filesystem Backend

`FilesystemBackend` stores blobs as plain files under a configurable root directory.

**Constructor:** `FilesystemBackend(base_path: str)`

Key behaviors:

- **Path traversal protection** -- `_resolve()` calls `Path.resolve()` and checks `is_relative_to()` against the base path. Any key that would escape the root raises `ValueError`.
- **Directory auto-creation** -- `put()` creates parent directories as needed (`mkdir(parents=True, exist_ok=True)`).
- **Blocking I/O offloaded** -- all filesystem calls run through `asyncio.to_thread()` so the event loop is never blocked.
- **`list_files(prefix)`** -- uses `rglob("*")` under the resolved prefix path. If the prefix points to a single file, yields only that file.
- **`get_url(key)`** -- returns the `file://` URI via `Path.as_uri()`. Raises `FileNotFoundError` if the file does not exist.

### Configuration

| Environment Variable     | Default                    | Description              |
|--------------------------|----------------------------|--------------------------|
| `TSIGMA_STORAGE_BACKEND` | `filesystem`               | Must be `"filesystem"`.  |
| `TSIGMA_STORAGE_PATH`    | `/var/lib/tsigma/storage`  | Root directory for files. |

## S3 Backend

`S3Backend` stores blobs in an S3-compatible bucket using `aiobotocore` for async access.

**Constructor:**

```python
S3Backend(
    bucket: str,
    region: str = "us-east-1",
    endpoint_url: str | None = None,   # custom endpoint for MinIO, etc.
    access_key: str | None = None,
    secret_key: str | None = None,
)
```

Key behaviors:

- **Lazy client** -- the `aiobotocore` session and S3 client are created on the first operation (`_get_client()`), not at construction time.
- **Credentials** -- if `access_key` and `secret_key` are both provided they are passed directly. Otherwise `aiobotocore` falls back to its default credential chain (env vars, instance profile, etc.).
- **Custom endpoint** -- set `endpoint_url` for MinIO or other S3-compatible services.
- **`list_files(prefix)`** -- uses the `list_objects_v2` paginator so it handles buckets with arbitrarily many keys.
- **`get_url(key, expires_in)`** -- generates a presigned GET URL. Default expiry is 3600 seconds (1 hour).
- **`close()`** -- S3Backend exposes an explicit `close()` coroutine to shut down the client context. Call it during application shutdown.
- **Error handling** -- `get()` and `exists()` catch `NoSuchKey` and `ClientError` with code `404` via the helper `_is_not_found()` and translate them to `FileNotFoundError` or `False`.
- **Dependency** -- requires `aiobotocore`. The import is guarded; if missing, `__init__` raises `ImportError` with install instructions.

### Configuration

| Environment Variable            | Default        | Description                                             |
|---------------------------------|----------------|---------------------------------------------------------|
| `TSIGMA_STORAGE_BACKEND`        | --             | Must be `"s3"`.                                         |
| `TSIGMA_STORAGE_S3_BUCKET`      | `""`           | Bucket name (required).                                 |
| `TSIGMA_STORAGE_S3_REGION`      | `us-east-1`    | AWS region.                                             |
| `TSIGMA_STORAGE_S3_ENDPOINT`    | `""`           | Custom endpoint URL (e.g. `http://minio:9000`). Empty = AWS default. |
| `TSIGMA_STORAGE_S3_ACCESS_KEY`  | `""`           | AWS access key. Empty = use default credential chain.   |
| `TSIGMA_STORAGE_S3_SECRET_KEY`  | `""`           | AWS secret key. Empty = use default credential chain.   |

## Configuration Reference

All storage settings live in `tsigma.config.Settings` with the `TSIGMA_` env-var prefix (case-insensitive).

| Setting                    | Env Var                         | Type   | Default                    |
|----------------------------|---------------------------------|--------|----------------------------|
| `storage_backend`          | `TSIGMA_STORAGE_BACKEND`        | `str`  | `"filesystem"`             |
| `storage_path`             | `TSIGMA_STORAGE_PATH`           | `str`  | `"/var/lib/tsigma/storage"`|
| `storage_s3_bucket`        | `TSIGMA_STORAGE_S3_BUCKET`      | `str`  | `""`                       |
| `storage_s3_region`        | `TSIGMA_STORAGE_S3_REGION`      | `str`  | `"us-east-1"`              |
| `storage_s3_endpoint`      | `TSIGMA_STORAGE_S3_ENDPOINT`    | `str`  | `""`                       |
| `storage_s3_access_key`    | `TSIGMA_STORAGE_S3_ACCESS_KEY`  | `str`  | `""`                       |
| `storage_s3_secret_key`    | `TSIGMA_STORAGE_S3_SECRET_KEY`  | `str`  | `""`                       |

Related cold-tier settings (not part of the storage backend, but relevant to data lifecycle). The first five rows are runtime-registry keys in `tsigma.settings_service` — see [`docs/operations/runtime-settings.md`](../operations/runtime-settings.md) for the admin API, env-var override rules, and audit log details. Only `storage_cold_path` remains a Pydantic config attribute, because the cold-tier filesystem path is fixed at deployment time.

| Registry key                         | Env Var Override                          | Type   | Default | Category   |
|--------------------------------------|-------------------------------------------|--------|---------|------------|
| `storage.cold_enabled`               | `TSIGMA_STORAGE_COLD_ENABLED`             | `bool` | `false` | cold_tier  |
| `storage.cold_after_days`            | `TSIGMA_STORAGE_COLD_AFTER_DAYS`          | `int`  | `180`   | cold_tier  |
| `storage.cold_delete_after_export`   | `TSIGMA_STORAGE_COLD_DELETE_AFTER_EXPORT` | `bool` | `true`  | cold_tier  |
| `cold_tier.query_enabled`            | `TSIGMA_COLD_TIER_QUERY_ENABLED`          | `bool` | `true`  | cold_tier  |
| `cold_tier.threshold_days`           | `TSIGMA_COLD_TIER_THRESHOLD_DAYS`         | `int`  | `180`   | cold_tier  |

| Pydantic setting       | Env Var                          | Type   | Default                     |
|------------------------|----------------------------------|--------|-----------------------------|
| `storage_cold_path`    | `TSIGMA_STORAGE_COLD_PATH`       | `str`  | `"/var/lib/tsigma/cold"`    |

The `cold_tier.*` keys (query routing) are consumed by the cold-tier query layer; see [Cold-Tier Query](#cold-tier-query) below.

## Warm Placement (partition relocation)

Warm tiering relocates old event-log partitions to cheaper "warm" storage while keeping
them in-table and queryable (transparent - same queries, no read layer). It is opt-in:

| Setting                  | Env Var                         | Type   | Default | Notes |
|--------------------------|---------------------------------|--------|---------|-------|
| `warm_placement_enabled` | `TSIGMA_WARM_PLACEMENT_ENABLED` | `bool` | `false` | Master switch for the relocate job. |
| `warm_tablespace_target` | `TSIGMA_WARM_TABLESPACE_TARGET` | `str`  | `""`    | Deployment-specific tablespace/filegroup. Job no-ops if empty. |
| `storage_warm_after`     | `TSIGMA_STORAGE_WARM_AFTER`     | `str`  | `"7 days"` | Age past which a partition is warm-eligible. |

The `move_to_warm` scheduled job (`scheduler/jobs/move_to_warm.py`, mirrors `compress_chunks`)
relocates partitions older than `storage_warm_after` for the managed event tables, via
per-dialect SQL from `DialectHelper.move_partition_tablespace_sql`:

| Dialect | Relocation SQL | Status |
|---------|----------------|--------|
| PostgreSQL | `ALTER TABLE events.<part> SET TABLESPACE <target>` | generated; live-validate per instance |
| Oracle | `ALTER TABLE <t> MOVE PARTITION <p> TABLESPACE <target>` | generated; live-validate |
| MS-SQL | best-effort index `REBUILD ... ON <filegroup>` | **best-effort, live-validate** (true filegroup move needs partition-scheme `NEXT USED`) |
| MySQL | `[]` (placement is creation-time `DATA DIRECTORY`) | deferred |

Safety: the job is inert until BOTH `warm_placement_enabled` and `warm_tablespace_target`
are set; a malformed `storage_warm_after` skips relocation (demotes nothing) rather than
collapsing the window. TimescaleDB deployments use native chunk compression
(`compress_chunks`) instead. **Live per-engine relocation behavior is validated post-boot
against real instances** - the SQL strings are unit-tested, not the engine effect.

## Cold-Tier Query

The cold-tier query layer (`tsigma.database.cold_tier.ColdTierQuery`) reads partitioned Parquet archives written by the `export_cold` scheduler job. It works database-agnostically — PostgreSQL deployments can also read cold Parquet in-database via `pg_duckdb` (preferred), `duckdb_fdw`, or `parquet_fdw` and expose the result through the unified view (see [ARCHITECTURE.md § Unified Cold View](ARCHITECTURE.md#unified-cold-view-postgresql)), but every other database family (MS-SQL, Oracle, MySQL) relies on this application-layer reader.

### Partition layout

```
{storage_cold_path}/{signal_id}/{YYYY-MM-DD}/events.parquet
```

Same layout for filesystem and S3 backends. The cold path comes from the Pydantic `storage_cold_path` setting (deploy-time, not runtime-tunable); the signal ID and date come from the archived rows.

### API

```python
class ColdTierQuery:
    def __init__(self, backend: StorageBackend) -> None: ...

    async def list_partitions(
        self,
        signal_id: str,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> list[ColdPartition]: ...

    async def fetch_events(
        self,
        signal_id: str,
        start: datetime,
        end: datetime,
        event_codes: Iterable[int] | None = None,
        *,
        event_param_in: Iterable[int] | None = None,
        where_sql_fragment: str | None = None,
    ) -> pd.DataFrame: ...

    async def aggregate_events(
        self,
        signal_id_or_ids: str | list[str] | None,
        start: datetime,
        end: datetime,
        *,
        agg: list[tuple],
        group_by: list[str] | None = None,
        filters: dict[str, Any] | None = None,
    ) -> pd.DataFrame: ...
```

`list_partitions` returns partitions sorted by `event_date` ascending. Empty list when the signal has no archived data or when no partition falls within the date range.

`fetch_events` returns a DataFrame with the canonical columns `event_code`, `event_param`, `event_time`, ordered by `event_time` ascending. The shape matches the hot-tier `tsigma.reports.sdk.queries.fetch_events`, so tier-aware callers can `pd.concat` results from both tiers without column alignment. Returns an empty DataFrame with the same schema (and `int64` / `datetime64[us]` dtypes) when no partitions exist for the signal or no rows match the filters.

`fetch_events` supports three predicate modes — at most one of `event_codes` and `where_sql_fragment` may be supplied:

- **Filter mode**: pass `event_codes` (and optionally `event_param_in`); the WHERE clause is built as `event_code IN (...) [AND event_param IN (...)]` with values int-cast and inlined.
- **Fragment mode**: pass `where_sql_fragment` — a DuckDB expression that replaces the IN-list predicate entirely. Used by `tsigma.reports.sdk.queries.fetch_events_split` (B8) to express `phase_codes OR (det_codes AND event_param IN det_channels)` in a single scan. **Trust contract:** `where_sql_fragment` is **SDK-internal code** — the same trust boundary as the `count_if` / `max_if` expressions in `aggregate_events`. It is **not** validated against an allowlist. Callers must build the fragment in Python from controlled values and never interpolate API-surface input.
- **No-filter mode**: pass neither. Every row in the `[start, end]` window for the signal is returned, regardless of event code. Used by the GraphQL `events` query when the client omits the `event_codes` argument (B9).

Passing BOTH `event_codes` and `where_sql_fragment` raises `ValueError` (ambiguous).

`aggregate_events` runs one DuckDB query with all projections in a single SELECT across the matched partitions for one or more signals. Result columns: each entry in `group_by` (preserved order) followed by one `agg_<i>` column per spec, where `i` is the spec's position in the `agg` list.

**Aggregation specs:**

| Spec | DuckDB form |
|---|---|
| `("count", "*")` | `COUNT(*)` |
| `("count_if", "<duckdb_expr>")` | `COUNT(*) FILTER (WHERE <expr>)` |
| `("max", "<col>")` | `MAX(<col>)` |
| `("max_if", "<col>", "<duckdb_expr>")` | `MAX(<col>) FILTER (WHERE <expr>)` |
| `("min", "<col>")` | `MIN(<col>)` |
| `("sum", "<col>")` | `SUM(<col>)` |

**Column safety:** every column reference from `group_by`, `filters` keys, and column-flavored agg specs (`max`, `max_if`, `min`, `sum`) is validated against a hardcoded allowlist of the canonical event-schema columns (`signal_id`, `event_time`, `event_code`, `event_param`, `device_id`) before any SQL composition. The raw expressions in `count_if` / `max_if` are NOT validated — they're SDK-internal trusted code (e.g. constructed by the B10 SDK helper), not API-surface input. Filter values are inlined after `int()` cast (controlled values, no injection surface); time bounds and parquet sources are parameterized.

```python
@dataclass(frozen=True)
class ColdPartition:
    signal_id: str
    event_date: date
    key: str   # storage key (relative path for fs; bucket-relative for S3)
```

The `key` is suitable for passing to `backend.get(...)`, building a DuckDB `read_parquet(...)` argument, or constructing an `s3://bucket/key` URL.

### Backend dispatch

`ColdTierQuery` is one concrete class. Internal `isinstance(self.backend, FilesystemBackend)` vs `isinstance(self.backend, S3Backend)` branches per method — no abstract base, no per-backend subclasses. New backend support is added by extending the dispatch in `tsigma/database/cold_tier.py`, mirroring the [Adding a New Storage Backend](#adding-a-new-storage-backend) recipe below.

- **Filesystem branch:** directory walk under `backend._base / signal_id`. Parses date directory names with `date.fromisoformat`; skips any directory whose name isn't a valid ISO date, and any date directory missing `events.parquet`.
- **S3 branch:** `backend.list_files(prefix=f"{signal_id}/")` (uses `list_objects_v2` paginator internally). Each returned key is matched against `^[^/]+/(\d{4}-\d{2}-\d{2})/events\.parquet$`; non-matching keys are skipped.

`fetch_events` builds on `list_partitions` then runs one DuckDB query over the matched parquet files:

- **Filesystem:** resolves each partition's `key` to an absolute path via `backend._resolve(key)`, runs `read_parquet([paths])` in `asyncio.to_thread`. No network, no extension load required (DuckDB reads local parquet natively).
- **S3:** builds `s3://{bucket}/{key}` URLs, loads the pre-installed `httpfs` extension, and constructs a session-scoped `CREATE SECRET` from the `S3Backend`'s `_access_key` / `_secret_key` / `_region` / `_endpoint_url` when credentials are present. When credentials are absent (using the default AWS credential chain), the secret step is skipped and DuckDB falls back to its built-in AWS credential resolution. URL_STYLE is `path` (MinIO-compatible) and USE_SSL derives from the endpoint scheme.
- **Predicate pushdown:** `event_time` bounds are passed as bound parameters; `event_code` and `event_param` IN-lists are inlined after `int()` casting (controlled values — no SQL injection surface). DuckDB applies these as predicate pushdown against the parquet row groups.

`aggregate_events` uses the same isinstance dispatch and the same S3 secret setup. It gathers partitions across all signals in `signal_id_or_ids`, then issues a single SELECT against the combined `read_parquet([...])` source so the aggregation runs in one DuckDB pass — no concat-then-reaggregate.

`aggregate_events` accepts four modes for `signal_id_or_ids`:

- `None` or `[]` → no signals requested; returns an empty DataFrame with the canonical `[*group_by, agg_<i>]` column shape immediately. No partition enumeration, no settings round-trip, no I/O on either tier. The empty-result short-circuit fires before any hot/cold dispatch.
- `"All"` → fleet-wide aggregation across every signal present in storage. The cold-tier path enumerates the top-level signal directories (filesystem: `backend._base.iterdir()`) or key prefixes (S3: paginated `list_files(prefix="")`, deduplicated on the leading path segment) via the private `ColdTierQuery._list_all_signal_ids()` helper, then runs the same multi-signal aggregation. Empty storage produces an empty DataFrame with the canonical column shape. On the hot path the `signal_id IN (...)` predicate is omitted entirely when `"All"` is passed.
- `str` → single specific signal.
- `list[str]` (non-empty) → specific signal subset.

The contract is consistent across the SDK (`tsigma.reports.sdk.aggregates.aggregate_events`) and `ColdTierQuery` levels.

### Tier-aware SDK

`ColdTierQuery` is the low-level cold reader. The Report SDK (`tsigma.reports.sdk.queries.fetch_events`) wraps it with hot/cold routing so callers don't think about tiers — see [ARCHITECTURE.md § Tier-Aware Query Routing](ARCHITECTURE.md#tier-aware-query-routing) for the decision tree. Routing is gated by the `cold_tier.query_enabled` registry key; operators can disable cold reads globally without restarting replicas via the runtime settings API.

The SDK `fetch_events` exposes the same three predicate modes as `ColdTierQuery.fetch_events` — **IN-list** (`event_codes=[...]`), **fragment** (`where_sql_fragment="..."`), and **no-filter** (both `None`). Fragment mode is used for OR-predicates and other shapes the `(event_codes, event_param_in)` pair cannot express, e.g. `event_code = 104 OR (event_code = 1 AND event_param = 2)`. The kwarg propagates to both tier paths: hot uses `sqlalchemy.text(...)` to inline the fragment in the SQLAlchemy `WHERE`, cold forwards to `ColdTierQuery.fetch_events`'s fragment mode. **Same trust contract as B8:** `where_sql_fragment` is SDK-internal code, never user input — construct it in Python from controlled constants. Passing both `event_codes` and `where_sql_fragment` raises `ValueError` ("at most one") at the SDK layer before any I/O.

The public REST event-list endpoint `GET /api/v1/signals/{signal_id}/events` (`tsigma.api.v1.signals.list_signal_events`) is also tier-aware as of B12.2 — it routes through `fetch_events` rather than issuing a direct `select(ControllerEventLog)`, and returns a paginated `{items, next_cursor}` envelope. The handler applies the `api.max_lookback_days` and `api.max_aggregation_days` guards before any read, and clamps `limit` to `api.max_page_size` via `paginated_event_list`. Pass `next_cursor` back as the `after` query parameter to fetch the next page; the cursor is opaque (base64url-encoded JSON of the last row's `(event_time, signal_id, event_code, event_param)` tuple).

B12 Stage 2a extends the same routing to the row-iteration analytics REST endpoints: `coordination/offset-drift`, `coordination/patterns`, `coordination/quality`, `detectors/gaps`, `detectors/occupancy`, `phases/split-monitor`, `preemptions/summary`, `preemptions/recovery`, and the coordination sub-component of `health/signal` now consume CEL rows through `fetch_events` and apply the same `api.max_lookback_days` / `api.max_aggregation_days` guards before reading. The `preemptions/recovery` handler uses the SDK-internal `where_sql_fragment` predicate mode to express the `(event_code = 104) OR (event_code = 1 AND event_param = 2)` OR predicate that the `(event_codes, event_param_in)` parameter pair cannot.

B12 Stage 2b finishes the analytics REST migration by routing the remaining SQL-aggregation endpoints through the tier-aware `aggregate_events` SDK helper (`tsigma.reports.sdk.aggregates.aggregate_events`): `detectors/stuck` (three group-by aggregations — last-ON, last-OFF, ON-count per `(signal_id, event_param)`), `health/detector` (one four-spec aggregation — on/off counts and last-ON/OFF timestamps for the requested `(signal_id, detector_channel)`), the detector / phase / communication sub-components of `health/signal` (three per-channel-or-per-phase aggregations plus a window-wide total event count; the coordination sub-component continues to use Stage 2a's `fetch_events`), `phases/skipped` (one per-phase Phase Green count), and `phases/terminations` (one per-phase four-spec aggregation — cycles + gap-out / max-out / force-off counts). All five endpoints apply the `api.max_lookback_days` and `api.max_aggregation_days` guards before any SDK call. After Stage 2b no analytics REST endpoint issues a direct `session.execute` for CEL aggregation.

B12 Stage 2c migrates an internal report call-site that still hand-rolled a direct `db_facade.get_dataframe` query: `tsigma.reports.arrivals_on_green.ArrivalsOnGreenReport._fetch_raw_events` now fetches phase + detector events in one tier-routed query via `fetch_events_split` (B8). The scheduler's `signal_plan` job is intentionally scoped out — its ≤ 2-hour lookback never crosses the cold threshold, so hot-only is the correct routing.

B12 Stage 3 finishes the migration by giving the GraphQL `Query.events` resolver (`tsigma.api.graphql.schema`) opaque-cursor pagination via `paginated_event_list`. The resolver's `limit: int = 10000` argument is replaced with `first: Int` (page size, clamped to `api.max_page_size`) and `after: String` (opaque cursor), and the return type changes from `[EventType!]!` to a new `EventListPage` (`items: [EventType!]!`, `nextCursor: String`). The resolver attaches a constant `signal_id` column to the SDK DataFrame before invoking the helper so the cursor tuple `(event_time, signal_id, event_code, event_param)` is stable — same pattern as the Stage 1 REST endpoint. Tier-aware routing remains the B9 behavior; only the pagination surface changed.

### Construction

Build a `ColdTierQuery` by wrapping the cold-tier backend from the factory:

```python
from tsigma.storage.factory import get_cold_storage_backend
from tsigma.database.cold_tier import ColdTierQuery

backend = get_cold_storage_backend()
cold = ColdTierQuery(backend)
parts = await cold.list_partitions("SIG_001")
```

The factory returns a `FilesystemBackend` or `S3Backend` based on `TSIGMA_STORAGE_BACKEND`. Cold-tier reads use the same backend choice as cold-tier writes (the `export_cold` job).

### Testing the S3 path

S3-backed cold-tier tests use moto's `ThreadedMotoServer` (not the `@mock_aws` decorator) because `aiobotocore`'s async response handling does not compose with moto's in-process monkey-patch. The server pattern runs a real HTTP server in-process and points `S3Backend.endpoint_url` at it; this exercises the actual aiobotocore code path. Install with `pip install -e ".[dev,s3]"` — the `dev` extra includes `moto[s3,server]>=5.0`.

## Tile Cache (MapLibre proxy)

The dashboard map renders raster tiles through a local caching proxy instead of
hitting the upstream tile server on every pan/zoom. `GET /tiles/{z}/{x}/{y}.png`
(mounted at app root, not under `/api/v1`) is a closed proxy for the single
configured `tile_source_url`.

**Backend:** `get_tile_storage_backend()` (in `storage/factory.py`) mirrors the
cold-tier factory, rooted at `tile_storage_path`. Cache key:
`tiles/{source}/{z}/{x}/{y}.png`.

**Request flow:**
- **Hit (fresh):** cached tile newer than `tile_cache_ttl_days` -> served directly, no upstream call.
- **Hit (stale):** older than the TTL -> the stale tile is served immediately and a
  background task refreshes it (stale-while-revalidate).
- **Miss:** fetched once from `tile_source_url` (with `tile_user_agent`), stored, served.
- **Single-flight:** concurrent misses for the same key coalesce into one upstream
  fetch (in-process per-key Future).
- Responses carry `Cache-Control: public, max-age=<ttl>` and an `ETag`.
- **Bounds:** `z > tile_max_zoom`, or `x`/`y` outside `[0, 2**z)`, returns 404.

**Off-switch:** with `tile_cache_enabled=false`, the `/tiles` route is not mounted
and the dashboard renderer falls back to `tile_source_url` directly (the view injects
the chosen URL; attribution is preserved either way).

**Config (env prefix `TSIGMA_`):**

| Setting | Default | Description |
|---------|---------|-------------|
| `tile_cache_enabled` | `true` | Mount the proxy + point the renderer at it. |
| `tile_source_url` | `https://tile.openstreetmap.org/{z}/{x}/{y}.png` | Upstream raster tile template. |
| `tile_cache_ttl_days` | `30` | Freshness window before a tile is treated as stale. |
| `tile_storage_path` | `/var/lib/tsigma/tiles` | Filesystem root for the tile cache. |
| `tile_max_zoom` | `19` | Upper zoom bound (closed proxy). |
| `tile_user_agent` | `TSIGMA/1.0` | User-Agent sent on upstream fetches. |

**Offline seed (follow-on, not in the MVP):** a scheduled job can walk an agency's
bbox x zoom to prime the cache and emit a read-only MBTiles pack for air-gapped sites.

## Adding a New Storage Backend

1. **Create the module** -- add `tsigma/storage/yourbackend.py`.

2. **Subclass `StorageBackend`** -- implement all six abstract methods (`put`, `get`, `delete`, `exists`, `list_files`, `get_url`). All methods are async.

   ```python
   from tsigma.storage.base import StorageBackend, StoredFile

   class YourBackend(StorageBackend):
       def __init__(self, ...):
           ...

       async def put(self, key, data, metadata=None) -> StoredFile:
           ...

       async def get(self, key) -> bytes:
           ...

       async def delete(self, key) -> None:
           ...

       async def exists(self, key) -> bool:
           ...

       async def list_files(self, prefix):
           ...

       async def get_url(self, key, expires_in=3600) -> str:
           ...
   ```

3. **Add configuration** -- add any required settings to `tsigma.config.Settings` following the `storage_yourbackend_*` naming convention so they map to `TSIGMA_STORAGE_YOURBACKEND_*` env vars.

4. **Register in the factory** -- edit `tsigma/storage/factory.py` and add a branch for your backend name. Use a deferred import to keep the dependency optional:

   ```python
   if backend_type == "yourbackend":
       from .yourbackend import YourBackend
       return YourBackend(
           setting_a=settings.storage_yourbackend_setting_a,
           ...
       )
   ```

5. **Export (optional)** -- if callers should be able to import the class directly, add it to `tsigma/storage/__init__.py`'s `__all__`.

6. **Error semantics** -- follow the conventions established by the existing backends:
   - `get()` raises `FileNotFoundError` when the key does not exist.
   - `delete()` is a no-op when the key does not exist.
   - `exists()` returns `bool`, never raises for missing keys.
