# API Request Limits

TSIGMA's public REST and GraphQL endpoints enforce four request-shape
limits to bound the cost of any single query. All four are runtime-tunable
via Phase A's settings registry — operators can adjust them without a
service restart, and every change is recorded in the per-key audit log.

When a client exceeds a limit, the API returns:

```
HTTP/1.1 400 Bad Request
Content-Type: application/json

{
  "detail": "<human-readable message naming the violated key>"
}
```

The `detail` field always names the registry key responsible. Clients can
parse it; operators can grep it against the audit log.

## The four keys

| Key                              | Type | Default | Enforced by                                          |
|----------------------------------|------|---------|------------------------------------------------------|
| `api.max_page_size`              | int  | 1000    | `paginated_event_list` (cursor-paged endpoints)      |
| `api.max_aggregation_days`       | int  | 92      | `require_max_aggregation_days` guard                 |
| `api.max_signals_per_request`    | int  | 100     | `require_max_signals_per_request` guard              |
| `api.max_lookback_days`          | int  | 92      | `require_max_lookback` guard                         |

### `api.max_page_size`

**Ceiling for the per-page row count on cursor-paginated event-list
endpoints.** Clients pass `?limit=<int>` (or GraphQL `first: Int`); the
server clamps to this ceiling regardless of what the client requests. No
rejection — just a smaller page than asked for. Use the returned
`next_cursor` to walk further.

Bounds both the REST `GET /api/v1/signals/{signal_id}/events` endpoint
(`?limit=&after=`) and the GraphQL `Query.events` field (`first: Int`,
`after: String`) — both surfaces return the same `{items, next_cursor}`
shape (GraphQL exposes it as `EventListPage`) and share the same opaque
cursor encoding.

### `api.max_aggregation_days`

**Maximum size of the `[start, end]` window on aggregation endpoints.**
Calculated as `(end - start).days` (integer day resolution; sub-day
fractions don't count). Larger windows are rejected with HTTP 400.

Example rejection:

```json
{"detail": "requested window (180 days) exceeds api.max_aggregation_days (92)"}
```

### `api.max_signals_per_request`

**Maximum number of distinct signal IDs the client may supply per
aggregation request.** A single-signal request counts as 1. Lists exceeding
the ceiling are rejected with HTTP 400.

Example rejection:

```json
{"detail": "requested signal count (250) exceeds api.max_signals_per_request (100)"}
```

### `api.max_lookback_days`

**Hard floor on how far back in time a request may ask.** Computed against
`datetime.now(UTC)`. Requests whose `start` predates `now() - api.max_lookback_days`
are rejected with HTTP 400 — independent of `api.max_aggregation_days`
(which caps the window *size*, not its *age*).

Example rejection:

```json
{"detail": "start=2024-11-15T00:00:00+00:00 predates the api.max_lookback_days ceiling (92 days)"}
```

## Changing a limit

### Read current value

```
GET /api/v1/settings/{key}
```

Example: `GET /api/v1/settings/api.max_page_size`

### Set a new value

```
PUT /api/v1/settings/{key}
Content-Type: application/json

{
  "value": "<new-value-as-string>",
  "reason": "optional explanation logged in the audit row"
}
```

Phase A's Valkey pub/sub invalidates the in-process cache on every replica
within milliseconds — no restart required. The new value is honored by the
next request.

### Inspect the audit log

```
GET /api/v1/settings/{key}/audit
```

Returns the last 50 changes for that key — old value, new value, actor,
timestamp, and the optional `reason` from each PUT. Use this to correlate
client-side HTTP 400s with operator-side limit changes.

## Tuning guidance

- **`api.max_page_size`** — bumping above ~5000 risks slow serialization and
  large payloads. Prefer cursor pagination over a single huge page.
- **`api.max_aggregation_days`** — tied to the cold-tier query cost; large
  windows that span the threshold trigger both hot and cold scans. Keep
  modest unless cold-tier I/O is fast (e.g., local NVMe).
- **`api.max_signals_per_request`** — affects `SELECT ... WHERE signal_id IN (...)`
  cardinality. Postgres handles thousands fine; OLTP databases under heavy
  load may want lower ceilings.
- **`api.max_lookback_days`** — the hard floor protects you from clients
  accidentally pulling years of cold data. Set higher only when you've
  validated the cold-tier query path can keep up.

## See also

- [`docs/developers/ARCHITECTURE.md`](../developers/ARCHITECTURE.md#tier-aware-query-routing) — how tier routing interacts with these limits
- [`docs/developers/STORAGE.md`](../developers/STORAGE.md#cold-tier-query) — cold-tier query layer
- [`docs/operations/runtime-settings.md`](../operations/runtime-settings.md) — the full settings-registry reference
