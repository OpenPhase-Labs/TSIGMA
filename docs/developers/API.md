# API Design

> Part of [TSIGMA Architecture](ARCHITECTURE.md)
> See also: [Security Architecture](SECURITY.md) for auth details

---

## REST API

Standard CRUD operations at `/api/v1/`:

```
# Signals
POST   /api/v1/signals
GET    /api/v1/signals
GET    /api/v1/signals/{signal_id}
PUT    /api/v1/signals/{signal_id}
DELETE /api/v1/signals/{signal_id}
GET    /api/v1/signals/{signal_id}/audit

# Approaches (nested under signals)
GET    /api/v1/signals/{signal_id}/approaches
POST   /api/v1/signals/{signal_id}/approaches
GET    /api/v1/approaches/{approach_id}
PUT    /api/v1/approaches/{approach_id}
DELETE /api/v1/approaches/{approach_id}

# Detectors (nested under approaches)
GET    /api/v1/approaches/{approach_id}/detectors
POST   /api/v1/approaches/{approach_id}/detectors
GET    /api/v1/detectors/{detector_id}
PUT    /api/v1/detectors/{detector_id}
DELETE /api/v1/detectors/{detector_id}

# Reference data (CRUD via crud_factory)
/api/v1/direction-types/
/api/v1/controller-types/
/api/v1/lane-types/
/api/v1/movement-types/
/api/v1/detection-hardware/
/api/v1/event-codes/

# Organization
/api/v1/regions/
/api/v1/corridors/
/api/v1/jurisdictions/

# Routes
/api/v1/routes/
/api/v1/routes/{route_id}/signals
/api/v1/routes/{route_id}/signals/{route_signal_id}/phases
/api/v1/routes/{route_id}/distances

# Reports
GET    /api/v1/reports                              # list available
POST   /api/v1/reports/{name}/execute               # run report
POST   /api/v1/reports/{name}/export?format=csv     # export

# Analytics
GET    /api/v1/analytics/detectors/stuck
GET    /api/v1/analytics/detectors/gaps
GET    /api/v1/analytics/detectors/occupancy
GET    /api/v1/analytics/phases/skipped
GET    /api/v1/analytics/phases/split-monitor
GET    /api/v1/analytics/phases/terminations
GET    /api/v1/analytics/coordination/offset-drift
GET    /api/v1/analytics/coordination/quality
GET    /api/v1/analytics/preemption/analysis
GET    /api/v1/analytics/preemption/recovery
GET    /api/v1/analytics/health/detector
GET    /api/v1/analytics/health/signal

# Collection
POST   /api/v1/soap/GetControllerData               # ATSPM 4.x SOAP compat
POST   /api/v1/signals/{signal_id}/poll              # on-demand poll
GET    /api/v1/checkpoints                           # list all checkpoints
GET    /api/v1/checkpoints/{signal_id}               # per-signal checkpoints
POST   /api/v1/corrections/bulk                      # timestamp correction
POST   /api/v1/corrections/anchor                    # anchor correction

# Settings
GET    /api/v1/settings
PUT    /api/v1/settings/{key}
GET    /api/v1/settings/access-policy
PUT    /api/v1/settings/access-policy

# Auth
GET    /api/v1/auth/csrf                             # CSRF nonce for login
GET    /api/v1/auth/provider                         # active auth mode
POST   /api/v1/auth/login                            # local login
POST   /api/v1/auth/logout
GET    /api/v1/auth/me                               # current user
POST   /api/v1/auth/api-keys                         # create API key
GET    /api/v1/auth/api-keys                         # list user's keys
DELETE /api/v1/auth/api-keys/{key_id}                # revoke key

# System
GET    /health                                       # liveness probe
GET    /ready                                        # readiness probe
```

## GraphQL API

Complex queries at `/graphql` (Strawberry):

```graphql
type Query {
    signals(
        regionId: UUID
        corridorId: UUID
    ): [Signal!]!

    signal(signalId: String!): Signal

    regions: [Region!]!
    corridors: [Corridor!]!
    jurisdictions: [Jurisdiction!]!

    availableReports: [ReportInfo!]!

    runReport(
        name: String!
        params: JSON!
    ): JSON!

    events(
        signalId: String!
        start: DateTime!
        end: DateTime!
        eventCodes: [Int!]
    ): [Event!]!
}

type Signal {
    signalId: String!
    primaryStreet: String!
    secondaryStreet: String
    latitude: Float
    longitude: Float
    enabled: Boolean!
    approaches: [Approach!]!
}

type Approach {
    approachId: UUID!
    signalId: String!
    description: String
    mph: Int
    detectors: [Detector!]!
}
```

## Example Queries

**REST — Signal CRUD:**
```bash
# Create signal
curl -X POST /api/v1/signals \
  -H "Content-Type: application/json" \
  -H "Cookie: tsigma_session=..." \
  -d '{"signal_id": "GDOT-0001", "primary_street": "Main St", "secondary_street": "1st Ave"}'

# Get signal (metadata is redacted — passwords show as ***)
curl /api/v1/signals/GDOT-0001 \
  -H "Cookie: tsigma_session=..."

# Or with API key
curl /api/v1/signals/GDOT-0001 \
  -H "X-API-Key: tsgm_abc123..."
```

**GraphQL — Complex query:**
```graphql
query {
    signals(regionId: "xyz-789") {
        signalId
        primaryStreet
        approaches {
            description
            detectors {
                detectorChannel
            }
        }
    }
}
```

---

## Authentication & Authorization

### Authentication Modes

| Mode | Use Case | Configuration |
|------|----------|---------------|
| **Local** | Small DOTs, air-gapped | Username/password with bcrypt |
| **OIDC** | Enterprise SSO (Azure AD) | `TSIGMA_AUTH_MODE=oidc` |
| **OAuth2** | Generic SSO | `TSIGMA_AUTH_MODE=oauth2` |
| **API Key** | Machine-to-machine | `X-API-Key` or `Authorization: Bearer` header |

### Authentication Priority

When a request arrives, authentication is checked in order:

1. `X-API-Key` header → validate against `api_key` table
2. `Authorization: Bearer` header → validate as API key
3. Session cookie (`tsigma_session`) → validate against session store

### CSRF Protection

Login forms require a one-time CSRF nonce:

```
GET  /api/v1/auth/csrf          → {"csrf_token": "..."}
POST /api/v1/auth/login          → {"username": "...", "password": "...", "csrf_token": "..."}
```

Nonces are stored in the session store (Valkey or in-memory) with a 5-minute TTL and consumed on use.

### Session Management

Server-side sessions only — no JWTs.

| Backend | Use Case | Storage |
|---------|----------|---------|
| `InMemorySessionStore` | Single-process dev/testing | Python dict |
| `ValkeySessionStore` | Production, multi-process | Valkey with TTL |

Sessions store `user_id`, `username`, `role`, `created_at`, `expires_at`. Session IDs are `secrets.token_urlsafe(32)` in httponly cookies with `SameSite=strict`.

### Authorization Model

**Roles:**

| Role | Permissions |
|------|-------------|
| **admin** | Full access, user management, system settings |
| **viewer** | View dashboards, reports, signal data |

### Rate Limiting

| Category | Key | Default Limit |
|----------|-----|---------------|
| `login` | Client IP | 5 / minute |
| `read` | Session or IP | 100 / minute |
| `write` | Session or IP | 30 / minute |

Exceeded limits return `429 Too Many Requests` with `Retry-After` header.

### Configuration

```env
# Auth mode
TSIGMA_AUTH_MODE=local

# OIDC (Azure AD)
TSIGMA_AUTH_MODE=oidc
TSIGMA_OIDC_TENANT_ID=your-tenant-id
TSIGMA_OIDC_CLIENT_ID=your-client-id
TSIGMA_OIDC_CLIENT_SECRET=your-client-secret

# OAuth2 (generic)
TSIGMA_AUTH_MODE=oauth2
TSIGMA_OAUTH2_ISSUER_URL=https://auth.example.com
TSIGMA_OAUTH2_CLIENT_ID=your-client-id
TSIGMA_OAUTH2_CLIENT_SECRET=your-client-secret

# Rate limiting
TSIGMA_RATE_LIMIT_LOGIN=5
TSIGMA_RATE_LIMIT_READ=100
TSIGMA_RATE_LIMIT_WRITE=30
```
