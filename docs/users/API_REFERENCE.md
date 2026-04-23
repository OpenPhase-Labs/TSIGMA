# TSIGMA API Reference

This document describes the REST and GraphQL APIs provided by TSIGMA.

## Overview

TSIGMA provides two API interfaces:

| Interface | Base Path | Use Case |
|-----------|-----------|----------|
| **REST** | `/api/v1` | Simple queries, CRUD operations |
| **GraphQL** | `/graphql` | Complex queries, flexible data fetching |

Both APIs require authentication for write operations. Read operations on public endpoints are open by default.

---

## Authentication

### Session-Based (Web UI)
```http
POST /api/v1/auth/login
Content-Type: application/json

{
    "username": "admin",
    "password": "secret"
}
```

### API Key (Programmatic)
```http
GET /api/v1/signals
Authorization: Bearer <api-key>
```

### OAuth/OIDC (Enterprise)
```http
GET /api/v1/signals
Authorization: Bearer <jwt-token>
```

---

## REST API

### Base URL
```
https://tsigma.example.com/api/v1
```

### Common Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `start` | datetime | Period start (ISO 8601) |
| `end` | datetime | Period end (ISO 8601) |
| `signal_id` | string | Filter by signal |
| `limit` | int | Max results (default: 100) |
| `offset` | int | Pagination offset |

---

## Configuration Endpoints

### Signals

#### List Signals
```http
GET /api/v1/signals
```

**Query Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `jurisdiction_id` | UUID | Filter by jurisdiction |
| `region_id` | UUID | Filter by region |
| `search` | string | Search by name/identifier |
| `limit` | int | Max results |
| `offset` | int | Pagination offset |

**Response:**
```json
{
    "items": [
        {
            "signal_id": "GDOT-0142",
            "primary_street": "Main St",
            "secondary_street": "1st Ave",
            "latitude": "33.7490",
            "longitude": "-84.3880",
            "enabled": true
        }
    ]
}
```

#### Get Signal
```http
GET /api/v1/signals/{signal_id}
```

#### Create Signal
```http
POST /api/v1/signals
Content-Type: application/json

{
    "signal_id": "GDOT-0142",
    "primary_street": "Main St",
    "secondary_street": "1st Ave",
    "latitude": 33.7490,
    "longitude": -84.3880,
    "jurisdiction_id": "550e8400-e29b-41d4-a716-446655440000"
}
```

#### Update Signal
```http
PUT /api/v1/signals/{signal_id}
Content-Type: application/json

{
    "primary_street": "Main Street"
}
```

#### Delete Signal
```http
DELETE /api/v1/signals/{signal_id}
```

---

### Approaches

#### List Approaches for Signal
```http
GET /api/v1/signals/{signal_id}/approaches
```

#### Get Approach
```http
GET /api/v1/approaches/{approach_id}
```

#### Create Approach
```http
POST /api/v1/signals/{signal_id}/approaches
Content-Type: application/json

{
    "direction_type_id": 1,
    "description": "Northbound Through",
    "protected_phase_number": 2,
    "permissive_phase_number": null,
    "mph": 45
}
```

---

### Detectors

#### List Detectors for Approach
```http
GET /api/v1/approaches/{approach_id}/detectors
```

#### Get Detector
```http
GET /api/v1/detectors/{detector_id}
```

#### Create Detector
```http
POST /api/v1/approaches/{approach_id}/detectors
Content-Type: application/json

{
    "detector_channel": 5,
    "distance_from_stop_bar": 400,
    "lane_number": 1
}
```

---

### Jurisdictions

#### List Jurisdictions
```http
GET /api/v1/jurisdictions
```

#### Get Jurisdiction
```http
GET /api/v1/jurisdictions/{jurisdiction_id}
```

#### Create Jurisdiction
```http
POST /api/v1/jurisdictions
Content-Type: application/json

{
    "name": "City of Atlanta",
    "mpo_name": "Atlanta Regional Commission"
}
```

---

## Analytics Endpoints

### Detector Analytics

#### Find Stuck Detectors
```http
GET /api/v1/analytics/detectors/stuck
```

**Query Parameters:**
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `signal_id` | string | - | Filter by signal (optional) |
| `start` | datetime | -24h | Period start |
| `end` | datetime | now | Period end |
| `threshold_minutes` | int | 30 | Stuck threshold |

**Response:**
```json
{
    "items": [
        {
            "signal_id": "GDOT-0142",
            "detector_channel": 5,
            "status": "STUCK_ON",
            "duration_seconds": 3600.5,
            "last_event_time": "2024-01-15T08:00:00Z",
            "event_count": 0
        }
    ]
}
```

#### Gap Analysis
```http
GET /api/v1/analytics/detectors/gaps
```

**Query Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `signal_id` | string | Signal UUID (required) |
| `detector_channel` | int | Detector channel (optional) |
| `start` | datetime | Period start |
| `end` | datetime | Period end |

**Response:**
```json
{
    "items": [
        {
            "signal_id": "...",
            "detector_channel": 5,
            "period_start": "2024-01-15T08:00:00Z",
            "period_end": "2024-01-15T09:00:00Z",
            "total_actuations": 450,
            "avg_gap_seconds": 8.2,
            "min_gap_seconds": 0.5,
            "max_gap_seconds": 45.3,
            "gap_out_count": 12,
            "max_out_count": 3
        }
    ]
}
```

#### Detector Occupancy
```http
GET /api/v1/analytics/detectors/occupancy
```

**Query Parameters:**
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `signal_id` | string | - | Signal UUID (required) |
| `detector_channel` | int | - | Detector channel (required) |
| `start` | datetime | - | Period start |
| `end` | datetime | - | Period end |
| `bin_minutes` | int | 15 | Time bin size |

**Response:**
```json
{
    "signal_id": "...",
    "detector_channel": 5,
    "bins": [
        {
            "bin_start": "2024-01-15T08:00:00Z",
            "bin_end": "2024-01-15T08:15:00Z",
            "occupancy_pct": 23.5
        },
        {
            "bin_start": "2024-01-15T08:15:00Z",
            "bin_end": "2024-01-15T08:30:00Z",
            "occupancy_pct": 45.2
        }
    ]
}
```

---

### Phase Analytics

#### Find Skipped Phases
```http
GET /api/v1/analytics/phases/skipped
```

**Query Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `signal_id` | string | Signal UUID (required) |
| `start` | datetime | Period start |
| `end` | datetime | Period end |

**Response:**
```json
{
    "items": [
        {
            "signal_id": "...",
            "phase": 4,
            "expected_cycles": 100,
            "actual_cycles": 85,
            "skip_count": 15,
            "skip_rate_pct": 15.0,
            "period_start": "2024-01-15T08:00:00Z",
            "period_end": "2024-01-15T09:00:00Z"
        }
    ]
}
```

#### Split Monitor
```http
GET /api/v1/analytics/phases/split-monitor
```

**Query Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `signal_id` | string | Signal UUID (required) |
| `phase` | int | Phase number (optional) |
| `start` | datetime | Period start |
| `end` | datetime | Period end |

**Response:**
```json
{
    "items": [
        {
            "signal_id": "...",
            "phase": 2,
            "period_start": "2024-01-15T08:00:00Z",
            "period_end": "2024-01-15T09:00:00Z",
            "cycle_count": 60,
            "avg_green_seconds": 25.3,
            "min_green_seconds": 15.0,
            "max_green_seconds": 45.0,
            "avg_yellow_seconds": 4.0,
            "avg_red_clearance_seconds": 1.5,
            "gap_out_pct": 65.0,
            "max_out_pct": 25.0,
            "force_off_pct": 10.0
        }
    ]
}
```

#### Phase Terminations
```http
GET /api/v1/analytics/phases/terminations
```

**Response:**
```json
{
    "items": [
        {
            "signal_id": "...",
            "phase": 2,
            "gap_outs": 39,
            "max_outs": 15,
            "force_offs": 6,
            "skips": 0,
            "total_cycles": 60
        }
    ]
}
```

---

### Coordination Analytics

#### Offset Drift Analysis
```http
GET /api/v1/analytics/coordination/offset-drift
```

**Query Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `signal_id` | string | Signal UUID (required) |
| `start` | datetime | Period start |
| `end` | datetime | Period end |

**Response:**
```json
{
    "signal_id": "...",
    "period_start": "2024-01-15T08:00:00Z",
    "period_end": "2024-01-15T09:00:00Z",
    "expected_cycle_seconds": 120,
    "cycle_count": 30,
    "avg_drift_seconds": 0.5,
    "max_drift_seconds": 2.3,
    "drift_stddev": 0.8
}
```

#### Pattern History
```http
GET /api/v1/analytics/coordination/patterns
```

**Response:**
```json
{
    "items": [
        {
            "timestamp": "2024-01-15T06:00:00Z",
            "from_pattern": 1,
            "to_pattern": 2,
            "duration_seconds": 28800
        },
        {
            "timestamp": "2024-01-15T14:00:00Z",
            "from_pattern": 2,
            "to_pattern": 3,
            "duration_seconds": 14400
        }
    ]
}
```

#### Coordination Quality
```http
GET /api/v1/analytics/coordination/quality
```

**Query Parameters:**
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `signal_id` | string | - | Signal UUID (required) |
| `tolerance_seconds` | float | 2.0 | Offset tolerance |

**Response:**
```json
{
    "signal_id": "...",
    "period_start": "2024-01-15T08:00:00Z",
    "period_end": "2024-01-15T09:00:00Z",
    "total_cycles": 30,
    "cycles_within_tolerance": 27,
    "quality_pct": 90.0,
    "avg_offset_error_seconds": 0.8
}
```

---

### Preemption Analytics

#### Preemption Summary
```http
GET /api/v1/analytics/preemptions/summary
```

**Response:**
```json
{
    "signal_id": "...",
    "period_start": "2024-01-15T00:00:00Z",
    "period_end": "2024-01-16T00:00:00Z",
    "total_preemptions": 12,
    "by_preempt_number": {
        "1": 8,
        "2": 4
    },
    "avg_duration_seconds": 45.2,
    "max_duration_seconds": 120.0,
    "total_preemption_time_seconds": 542.4,
    "pct_time_preempted": 0.63
}
```

#### Preemption Recovery Time
```http
GET /api/v1/analytics/preemptions/recovery
```

**Response:**
```json
{
    "items": [
        {
            "preempt_end_time": "2024-01-15T08:15:30Z",
            "recovery_complete_time": "2024-01-15T08:17:15Z",
            "recovery_seconds": 105.0
        }
    ],
    "avg_recovery_seconds": 95.5,
    "max_recovery_seconds": 180.0
}
```

---

### Health Analytics

#### Detector Health Score
```http
GET /api/v1/analytics/health/detector
```

**Query Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `signal_id` | string | Signal UUID (required) |
| `detector_channel` | int | Detector channel (required) |
| `start` | datetime | Period start |
| `end` | datetime | Period end |

**Response:**
```json
{
    "signal_id": "...",
    "detector_channel": 5,
    "score": 85,
    "grade": "Good",
    "factors": {
        "stuck_penalty": 0,
        "chatter_penalty": -5,
        "variance_penalty": -5,
        "activity_penalty": 0,
        "balance_penalty": -5
    },
    "status": "HEALTHY"
}
```

#### Signal Health Score
```http
GET /api/v1/analytics/health/signal
```

**Response:**
```json
{
    "signal_id": "...",
    "overall_score": 78,
    "overall_grade": "Good",
    "components": {
        "detector_health": {
            "score": 85,
            "weight": 0.35
        },
        "phase_health": {
            "score": 70,
            "weight": 0.25
        },
        "coordination_health": {
            "score": 80,
            "weight": 0.20
        },
        "communication_health": {
            "score": 75,
            "weight": 0.20
        }
    },
    "issues": [
        "Phase 4 has 15% skip rate",
        "Detector 12 showing high variance"
    ]
}
```

---

## GraphQL API

### Endpoint
```
POST /graphql
```

### Schema Introspection
```
GET /graphql/schema
```

### Example Queries

#### Get Signal with Approaches
```graphql
query GetSignal($id: String!) {
    signal(id: $id) {
        signalId
        primaryStreet
        secondaryStreet
        latitude
        longitude
        approaches {
            approachId
            description
            directionTypeId
            protectedPhaseNumber
            detectors {
                detectorId
                detectorChannel
            }
        }
    }
}
```

#### Search Signals
```graphql
query SearchSignals($search: String, $limit: Int) {
    signals(search: $search, limit: $limit) {
        items {
            signalId
            primaryStreet
            secondaryStreet
            jurisdiction {
                name
            }
        }
        total
    }
}
```

#### Get Detector Analytics
```graphql
query DetectorAnalytics($signalId: String!, $start: DateTime!, $end: DateTime!) {
    stuckDetectors(signalId: $signalId, start: $start, end: $end) {
        detectorChannel
        status
        durationSeconds
    }

    detectorOccupancy(
        signalId: $signalId
        detectorChannel: 5
        start: $start
        end: $end
        binMinutes: 15
    ) {
        bins {
            binStart
            binEnd
            occupancyPct
        }
    }
}
```

#### Get Signal Health
```graphql
query SignalHealth($signalId: String!) {
    signalHealth(signalId: $signalId) {
        overallScore
        overallGrade
        detectorHealth {
            score
            grade
        }
        phaseHealth {
            score
            grade
        }
        coordinationHealth {
            score
            grade
        }
        issues
    }
}
```

### Mutations

#### Create Signal
```graphql
mutation CreateSignal($input: CreateSignalInput!) {
    createSignal(input: $input) {
        signalId
        primaryStreet
        secondaryStreet
    }
}
```

#### Update Signal
```graphql
mutation UpdateSignal($id: String!, $input: UpdateSignalInput!) {
    updateSignal(id: $id, input: $input) {
        signalId
        primaryStreet
        updatedAt
    }
}
```

---

## Error Responses

### Standard Error Format
```json
{
    "detail": "Signal not found",
    "status_code": 404,
    "error_code": "SIGNAL_NOT_FOUND"
}
```

### Validation Error
```json
{
    "detail": [
        {
            "loc": ["body", "signal_id"],
            "msg": "field required",
            "type": "value_error.missing"
        }
    ],
    "status_code": 422
}
```

### HTTP Status Codes

| Code | Meaning |
|------|---------|
| 200 | Success |
| 201 | Created |
| 204 | No Content (successful delete) |
| 400 | Bad Request |
| 401 | Unauthorized |
| 403 | Forbidden |
| 404 | Not Found |
| 422 | Validation Error |
| 500 | Internal Server Error |

---

## Rate Limiting

Default limits:
- **Anonymous**: 100 requests/minute
- **Authenticated**: 1000 requests/minute
- **Analytics endpoints**: 30 requests/minute (computationally expensive)

Rate limit headers:
```http
X-RateLimit-Limit: 100
X-RateLimit-Remaining: 95
X-RateLimit-Reset: 1705334400
```

---

## OpenAPI Documentation

Interactive API documentation is available at:

- **Swagger UI**: `http://localhost:8080/docs`
- **ReDoc**: `http://localhost:8080/redoc`
- **OpenAPI JSON**: `http://localhost:8080/openapi.json`

---

## SDK Examples

### Python
```python
import httpx

client = httpx.Client(
    base_url="https://tsigma.example.com/api/v1",
    headers={"Authorization": "Bearer <token>"}
)

# Get signals
response = client.get("/signals", params={"limit": 10})
signals = response.json()["items"]

# Get detector health
response = client.get("/analytics/health/detector", params={
    "signal_id": "GDOT-0142",
    "detector_channel": 5
})
health = response.json()
print(f"Detector health: {health['score']} ({health['grade']})")
```

### JavaScript/TypeScript
```typescript
const response = await fetch(
    'https://tsigma.example.com/api/v1/signals?limit=10',
    {
        headers: {
            'Authorization': 'Bearer <token>'
        }
    }
);

const { items: signals } = await response.json();
```

### cURL
```bash
# List signals
curl -X GET "https://tsigma.example.com/api/v1/signals?limit=10" \
    -H "Authorization: Bearer <token>"

# Get stuck detectors
curl -X GET "https://tsigma.example.com/api/v1/analytics/detectors/stuck" \
    -H "Authorization: Bearer <token>"
```
