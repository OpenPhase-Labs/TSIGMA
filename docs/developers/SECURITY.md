# Security Architecture

> Part of [TSIGMA Architecture](ARCHITECTURE.md)
> See also: [API Design](API.md) | [Database Schema](DATABASE_SCHEMA.md)

---

## Table of Contents

1. [Authentication](#1-authentication)
    - [Auth Provider Plugin System](#11-auth-provider-plugin-system)
    - [Local Authentication](#12-local-authentication)
    - [External Authentication (OIDC / OAuth2)](#13-external-authentication-oidc--oauth2)
2. [Authorization](#2-authorization)
3. [Session Management](#3-session-management)
4. [Transport Security](#4-transport-security)
5. [API Security](#5-api-security)
6. [Data Security](#6-data-security)
7. [Credential Encryption](#7-credential-encryption)
    - [Key Management](#71-key-management)
    - [Sensitive Fields](#72-sensitive-fields)
    - [Credential Flow](#73-credential-flow)
    - [API Redaction](#74-api-redaction)
    - [Backward Compatibility](#75-backward-compatibility)
8. [Middleware Stack](#8-middleware-stack)
9. [Rate Limiting](#9-rate-limiting)
10. [API Key Authentication](#10-api-key-authentication)
11. [Related Documents](#11-related-documents)

---

## 1. Authentication

### 1.1 Auth Provider Plugin System

Authentication follows the same plugin pattern used by all seven TSIGMA plugin systems
(ingestion methods, background jobs, reports, decoders, notifications, auth providers, validators).

**Registry:** `AuthProviderRegistry`

**Base class:** `BaseAuthProvider` (ABC)

```python
from abc import ABC, abstractmethod
from typing import ClassVar

from fastapi import APIRouter

class BaseAuthProvider(ABC):
    """Abstract base for all authentication providers."""

    name: ClassVar[str]
    description: ClassVar[str]

    @abstractmethod
    async def initialize(self) -> None:
        """Initialize the provider (called once at startup)."""
        ...

    @abstractmethod
    def get_router(self) -> APIRouter:
        """Return a FastAPI APIRouter with this provider's login routes."""
        ...
```

**Registered providers:**

| Provider | Decorator | Use Case |
|----------|-----------|----------|
| `local` | `@AuthProviderRegistry.register("local")` | Username/password with bcrypt. Small DOTs, air-gapped networks. |
| `oidc` | `@AuthProviderRegistry.register("oidc")` | Azure AD Authorization Code Flow. Enterprise SSO. |
| `oauth2` | `@AuthProviderRegistry.register("oauth2")` | Generic OAuth2: Google, Okta, Auth0, Keycloak, Cognito. |

**Provider selection** is controlled by a single config value:

```yaml
# settings.yaml (or TSIGMA_AUTH_MODE env var)
auth_mode: "local"   # "local" | "oidc" | "oauth2"
```

Only one provider is active at a time. The active provider is resolved at startup:

```python
provider = AuthProviderRegistry.get(settings.auth_mode)
```

**Session layer is shared.** Regardless of which provider authenticates the user, the
outcome is always the same: a server-side session is created with a consistent schema
(see [Session Management](#3-session-management)). Downstream code never needs to know
which provider was used.

---

### 1.2 Local Authentication

Local mode stores credentials directly in the TSIGMA database.

**Password storage:**

- Passwords are hashed with **bcrypt** before storage.
- Plaintext passwords are never written to disk, logged, or returned in API responses.
- The `password_hash` column stores the full bcrypt hash string (e.g., `$2b$12$...`).

**Login flow:**

```
1. User submits username + password via POST /auth/login
2. Server fetches user row by username
3. bcrypt.checkpw(password, stored_hash)
4. On success: create server-side session, set httponly cookie
5. On failure: return 401 (generic message, no user enumeration)
```

**Admin seed:**

On first startup, if `auth_mode` is `local` and no users exist in the database, TSIGMA
creates a default admin account:

| Field | Value |
|-------|-------|
| Username | `admin` |
| Password | Generated or read from `TSIGMA_ADMIN_PASSWORD` env var |
| Role | `admin` |

The seed runs only once. If any user row exists, it is skipped.

---

### 1.3 External Authentication (OIDC / OAuth2)

Both `oidc` and `oauth2` providers follow the same general pattern:

```
Browser --> TSIGMA /auth/login --> Redirect to IdP
IdP authenticates user --> Redirect to TSIGMA /auth/callback
TSIGMA validates tokens, provisions user --> Session cookie
```

**Just-In-Time (JIT) user provisioning:**

When a user authenticates via an external IdP for the first time, TSIGMA automatically
creates a local user record from the IdP claims:

| Field | Source |
|-------|--------|
| `username` | `preferred_username` or `email` claim |
| `email` | `email` claim |
| `full_name` | `name` claim |
| `password_hash` | `"!external"` (sentinel value) |
| `role` | Derived from IdP group membership |

The sentinel value `"!external"` is not a valid bcrypt hash. This prevents external users
from ever authenticating via the local login form.

**Role mapping from IdP groups:**

The `admin_groups` configuration defines which IdP groups grant admin privileges:

```yaml
# settings.yaml
oidc:
  admin_groups:
    - "TSIGMA-Admins"
    - "Traffic-Engineers"
```

Role assignment rules:

| Condition | Assigned Role | Notes |
|-----------|---------------|-------|
| User belongs to any `admin_groups` group | `admin` | Checked on every login |
| User does not belong to any `admin_groups` group | `viewer` | Default for new users |
| User was `viewer`, now qualifies for `admin` | `admin` | Upgrade on login |
| User was `admin`, no longer in `admin_groups` | `admin` | Never downgrade |

The "never downgrade" rule prevents accidental lockouts caused by IdP group
misconfiguration. Admin removal must be done explicitly in the TSIGMA admin UI.

**OIDC-specific configuration (Azure AD):**

```yaml
oidc:
  client_id: "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
  client_secret: "${TSIGMA_OIDC_CLIENT_SECRET}"
  tenant_id: "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
  redirect_uri: "https://tsigma.example.com/auth/callback"
  admin_groups:
    - "TSIGMA-Admins"
```

**Generic OAuth2 configuration:**

```yaml
oauth2:
  provider: "google"  # google | okta | auth0 | keycloak | cognito
  client_id: "..."
  client_secret: "${TSIGMA_OAUTH2_CLIENT_SECRET}"
  authorize_url: "https://accounts.google.com/o/oauth2/v2/auth"
  token_url: "https://oauth2.googleapis.com/token"
  userinfo_url: "https://openidconnect.googleapis.com/v1/userinfo"
  redirect_uri: "https://tsigma.example.com/auth/callback"
  scopes: ["openid", "email", "profile"]
  admin_groups:
    - "tsigma-admins"
```

---

## 2. Authorization

TSIGMA uses role-based access control (RBAC) with two roles:

| Role | Permissions |
|------|-------------|
| `admin` | Full access: read, write, modify, delete all resources. User management. System configuration. |
| `viewer` | Read-only access: view dashboards, reports, locations, devices. No modifications. |

**Enforcement via FastAPI dependencies:**

```python
# Any authenticated user (admin or viewer)
@router.get("/api/v1/signals")
async def list_signals(user: SessionData = Depends(get_current_user)):
    ...

# Admin-only endpoints
@router.post("/api/v1/signals")
async def create_signal(user: SessionData = Depends(require_admin)):
    ...

# Configurable access policy (public or authenticated)
@router.get("/analytics/something")
async def my_endpoint(
    user: SessionData | None = Depends(require_access("analytics")),
):
    ...
```

Dependencies are defined in `tsigma/auth/dependencies.py`:

- `get_current_user_optional` — checks API key headers first, then session cookie. Returns `SessionData | None`.
- `get_current_user` — wraps `get_current_user_optional`, raises 401 if not authenticated.
- `require_admin` — wraps `get_current_user`, raises 403 if role is not `admin`.
- `require_access(category)` — factory that returns a dependency enforcing the access policy for the given category. If the category policy is `"public"`, the request passes without authentication.

**General access rules:**

| Operation | Required Role |
|-----------|---------------|
| View dashboards, reports, maps | `viewer` or `admin` |
| View signals, approaches, detectors | `viewer` or `admin` |
| Create / update / delete any resource | `admin` |
| User management (create, edit, disable) | `admin` |
| System settings, ingestion config | `admin` |
| Background job management | `admin` |

**Jurisdiction scoping:**

Jurisdiction-level visibility controls access to signal data:

- `admin` users see all jurisdictions
- `viewer` users see only signals within their assigned jurisdiction(s)
- Scoping is enforced at the query level (SQLAlchemy filters)

---

## 3. Session Management

TSIGMA uses **server-side sessions** exclusively. There are no JWTs.

**Session store:**

Sessions are managed by a `BaseSessionStore` abstraction (`tsigma/auth/sessions.py`) with two implementations:

| Implementation | Storage | Use Case |
|----------------|---------|----------|
| `InMemorySessionStore` | Python dict | Single-process dev/testing |
| `ValkeySessionStore` | Valkey (Redis-compatible) | Production, multi-process, survives restarts |

The active store is selected at startup based on whether `TSIGMA_VALKEY_URL` is configured. Session IDs are generated with `secrets.token_urlsafe(32)`.

```python
@dataclass
class SessionData:
    user_id: UUID
    username: str
    role: str          # "admin" | "viewer"
    created_at: datetime
    expires_at: datetime
```

Both stores also provide CSRF nonce management (`create_csrf`, `validate_csrf`) with a 5-minute TTL for one-time-use tokens.

**Cookie configuration:**

| Attribute | Value | Rationale |
|-----------|-------|-----------|
| `httponly` | `True` | Prevents JavaScript access (XSS mitigation) |
| `secure` | `True` (production) | Cookie only sent over HTTPS |
| `samesite` | `lax` | Prevents CSRF on cross-origin POST requests |
| `path` | `/` | Available to all routes |

**Session lifecycle:**

| Parameter | Default | Configurable |
|-----------|---------|--------------|
| Session TTL | 480 minutes (8 hours) | Yes (`session_ttl_minutes`) |
| Cleanup interval | Periodic (background) | Automatic |

**Why not JWT:**

| Concern | Server-side session | JWT |
|---------|-------------------|-----|
| Revocation | Immediate (delete from dict) | Impossible until expiry (or complex blocklist) |
| Token theft | Session ID is opaque, no payload to decode | Token contains claims, replay risk |
| Size | Small cookie (~36 bytes UUID) | Large cookie (hundreds of bytes) |
| State | Server must store sessions | Stateless (but revocation adds state anyway) |

For a single-instance deployment (TSIGMA's primary target), server-side sessions are
simpler and more secure.

---

## 4. Transport Security

TSIGMA does **not** terminate TLS itself. TLS is handled by the reverse proxy in front
of the application.

**Supported reverse proxies:**

| Proxy | Typical Deployment |
|-------|-------------------|
| nginx | On-premise, VM |
| Caddy | Small deployments (automatic HTTPS) |
| Cloud LB | GCP, AWS, Azure managed load balancers |

**Required proxy configuration:**

- TLS 1.2 minimum (TLS 1.3 preferred)
- HSTS header: `Strict-Transport-Security: max-age=31536000; includeSubDomains`
- Forward `X-Forwarded-For`, `X-Forwarded-Proto` headers to TSIGMA

**Application-level enforcement:**

TSIGMA's `SecurityHeadersMiddleware` adds security headers to every response, regardless
of proxy configuration (defense in depth):

```
Content-Security-Policy: default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; font-src 'self'; connect-src 'self'; frame-ancestors 'none'
X-Frame-Options: DENY
X-Content-Type-Options: nosniff
Referrer-Policy: strict-origin-when-cross-origin
Strict-Transport-Security: max-age=63072000; includeSubDomains[; preload]
Permissions-Policy: camera=(), microphone=(), geolocation=()
```

#### HSTS Preload

To opt into the [HSTS preload list](https://hstspreload.org/), set the environment variable:

```bash
TSIGMA_HSTS_PRELOAD=true
```

This appends `; preload` to the `Strict-Transport-Security` header. **Before enabling**,
you must also submit your domain at [hstspreload.org](https://hstspreload.org/). Once
submitted and accepted, browsers will enforce HTTPS for your domain even on the first
visit. This is irreversible without a lengthy removal process — only enable this if your
deployment will always be served over HTTPS.

Default: `false` (disabled).

#### SNMP Protocol Security

TSIGMA supports SNMP v1, v2c, and v3 for controller logging control in rotate mode.

| Version | Authentication | Encryption | Recommendation |
|---------|---------------|------------|----------------|
| v1 | Community string (plaintext) | None | Legacy only |
| v2c | Community string (plaintext) | None | Legacy only |
| v3 | USM (SHA/SHA-256/SHA-512, MD5) | AES-128/192/256, DES | **Recommended** |

**Use SNMPv3 when your controllers support it.** v1/v2c community strings are
transmitted in plaintext and can be captured by anyone on the network.

SNMPv3 passphrases (`snmp_auth_passphrase`, `snmp_priv_passphrase`) are encrypted
at rest in the database via the same Fernet encryption used for FTP passwords.

See [FTP Polling Guide](../users/FTP_POLLING_GUIDE.md) for configuration details.

---

## 5. API Security

### CORS

Cross-Origin Resource Sharing is restricted to explicitly configured origins:

```yaml
cors:
  allowed_origins:
    - "https://tsigma.example.com"
  allowed_methods: ["GET", "POST", "PUT", "DELETE"]
  allowed_headers: ["Content-Type", "X-Request-ID"]
  allow_credentials: true
```

No wildcard origins (`*`) are permitted in production.

### Request ID Tracking

Every request receives a unique UUID via `RequestIDMiddleware`:

- A UUID4 is generated for every request.
- The request ID is included in all log entries for that request.
- The response includes the `X-Request-ID` header for client-side correlation.

This enables end-to-end audit trail correlation across logs, errors, and downstream
services.

### Input Validation

All API endpoints use **Pydantic models** for request validation:

```python
class SignalCreate(BaseModel):
    signal_id: str = Field(..., min_length=1, max_length=50)
    name: str = Field(..., min_length=1, max_length=200)
    latitude: float | None = Field(None, ge=-90, le=90)
    longitude: float | None = Field(None, ge=-180, le=180)
    jurisdiction_id: UUID | None = None
```

- Invalid input returns `422 Unprocessable Entity` with field-level error details.
- Type coercion and constraint validation happen before any business logic executes.
- Path parameters and query parameters are also validated via FastAPI's type annotations.

### SQL Injection Prevention

TSIGMA uses **SQLAlchemy ORM** exclusively for database access:

- All queries use parameterized statements.
- There are no raw SQL execution paths (`text()`, `execute()` with string concatenation).
- User input never appears in SQL strings.

### Rate Limiting

See [Additional Security Requirements](#9-additional-security-requirements).

---

## 6. Data Security

### Database Credentials

- Database connection strings are provided via environment variables (`TSIGMA_DATABASE_URL`).
- Credentials are never stored in configuration files, source code, or version control.
- Docker deployments use Docker secrets or `.env` files (excluded from `.gitignore`).

### Device Password Storage

Signal controller device passwords (used for FTP/SFTP polling) are stored encrypted:

| Column | Type | Contents |
|--------|------|----------|
| `password_encrypted` | `BYTEA` | Encrypted device password |

- Encryption key is provided via environment variable (`TSIGMA_ENCRYPTION_KEY`).
- Passwords are encrypted at the application layer before database write.
- Decryption occurs only at the moment of use (FTP/SFTP connection).

### Signal Metadata Credentials

FTP/SFTP credentials stored in the `signal_metadata` JSONB column follow the same
pattern:

- Sensitive fields are encrypted at the application layer.
- Database-level encryption at rest provides a second layer (if configured on the
  database).

### Log Sanitization

- Passwords, tokens, and secret values are filtered from all log output.
- Structured logging (JSON format) ensures consistent field handling.
- Request/response logging redacts `Authorization` headers and request bodies
  containing password fields.

---

## 7. Credential Encryption

TSIGMA encrypts sensitive fields in `signal_metadata` at rest using **Fernet symmetric
encryption** (`cryptography.fernet.Fernet`). The implementation lives in
`tsigma/crypto.py`.

Fernet provides authenticated encryption (AES-128-CBC + HMAC-SHA256). Each encrypted
value is a self-contained token that includes a timestamp, so tampered or corrupted
ciphertext is detected on decryption.

### 7.1 Key Management

The encryption key is a base64-encoded 32-byte Fernet key. Generate one with:

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

Three key sources are checked in order (first match wins):

| Priority | Source | Configuration |
|----------|--------|---------------|
| 1 | Environment variable | `TSIGMA_SECRET_KEY` — the Fernet key string directly |
| 2 | Key file | `TSIGMA_SECRET_KEY_FILE` — path to a file containing the key |
| 3 | HashiCorp Vault | `TSIGMA_SECRET_KEY_VAULT_URL` + `TSIGMA_SECRET_KEY_VAULT_PATH` (requires `VAULT_TOKEN` env var) |

**Vault integration** supports both KV v1 and KV v2 secret engines. The field name
within the vault secret defaults to `secret_key` and is configurable via
`TSIGMA_SECRET_KEY_VAULT_FIELD`.

If no key source is configured, `encrypt()` and `decrypt()` raise `CryptoError`
(a `RuntimeError` subclass). The key is loaded once and cached via `@lru_cache`.

### 7.2 Sensitive Fields

The `SENSITIVE_FIELDS` constant defines which fields within `signal_metadata["collection"]`
are encrypted:

```python
SENSITIVE_FIELDS = frozenset({"password", "ssh_key_path"})
```

Only values under the `collection` key in the metadata dict are affected. Other metadata
keys are stored in plaintext.

### 7.3 Credential Flow

Credentials follow a three-stage lifecycle:

```
API Write (POST/PUT)          Database (at rest)           Poll Time (CollectorService)
┌──────────────────┐         ┌──────────────────┐         ┌──────────────────┐
│ plaintext input  │──encrypt──▶│ Fernet ciphertext│──decrypt──▶│ plaintext usage  │
│ from admin user  │         │ in signal_metadata│         │ for FTP/SFTP conn │
└──────────────────┘         └──────────────────┘         └──────────────────┘
```

**Encrypt on write** — `encrypt_sensitive_fields()` is called in the signals API
(`POST /api/v1/signals/` and `PUT /api/v1/signals/{id}`) before the metadata is
persisted. Already-encrypted values (detected by the `gAAAAA` Fernet token prefix) are
skipped to prevent double-encryption.

**Decrypt at poll time** — `CollectorService._run_poll_cycle()` calls
`decrypt_sensitive_fields()` on each signal's metadata immediately before passing the
config to the polling method. Decrypted credentials exist only in memory for the
duration of the poll.

**Encryption is optional** — both write and poll paths check `has_encryption_key()`
before calling encrypt/decrypt. If no key is configured, metadata is stored and used
as plaintext. This allows TSIGMA to run without encryption configured (development,
legacy deployments) while strongly recommending it for production.

### 7.4 API Redaction

Sensitive fields are never exposed in API responses, regardless of encryption state.

**`redact_metadata()`** (in `tsigma/crypto.py`) replaces each sensitive field
value in `signal_metadata["collection"]` with the string `"***"` before returning the
response. This applies to `GET /api/v1/signals/{id}` and any endpoint that includes
signal metadata. The redaction operates on a deep copy so the database object is not
modified.

**`ControllerTypeResponse`** (in `tsigma/api/v1/reference.py`) excludes the `password`
field entirely from the Pydantic response model. The field exists on the database model
and the create/update schemas but is omitted from the response schema, so it is never
serialized.

### 7.5 Backward Compatibility

`decrypt_sensitive_fields()` is designed for safe migration from plaintext to encrypted
storage:

- Only values that look like Fernet tokens (starting with `gAAAAA`) are decrypted.
- Plaintext values pass through unchanged.
- This means existing signals with plaintext credentials continue to work after
  encryption is enabled. On the next `PUT` update, the field will be encrypted.

There is no batch migration command — encryption happens incrementally as signals are
created or updated through the API.

---

## 8. Middleware Stack

Middleware executes in order for every request. The stack is applied in `main.py`:

| Order | Middleware | Purpose | Header / Side Effect |
|-------|-----------|---------|---------------------|
| 1 | `SecurityHeadersMiddleware` | Adds CSP, X-Frame-Options, X-Content-Type-Options, Referrer-Policy, Permissions-Policy | Multiple response headers |
| 2 | `RequestIDMiddleware` | Assigns or propagates a unique request UUID | `X-Request-ID` |
| 3 | `TimingMiddleware` | Measures request processing duration | `X-Process-Time` |
| 4 | `LoggingMiddleware` | Structured request/response logging | Log entries (stdout/file) |

Middleware ordering matters: `SecurityHeadersMiddleware` runs first to ensure headers
are present even if later middleware or the route handler raises an exception.

---

## 9. Rate Limiting

`RateLimitMiddleware` classifies each request into a category and enforces per-key sliding window limits. Non-API paths (health, static, docs) are not rate limited.

| Category | Key | Default Limit | Configurable Via |
|----------|-----|---------------|------------------|
| `login` | Client IP | 5 / minute | `TSIGMA_RATE_LIMIT_LOGIN` |
| `read` | Session cookie or IP | 100 / minute | `TSIGMA_RATE_LIMIT_READ` |
| `write` | Session cookie or IP | 30 / minute | `TSIGMA_RATE_LIMIT_WRITE` |

Exceeded limits return `429 Too Many Requests` with a `Retry-After` header.

**Backends:**
- **In-memory** (default) — sliding window via dict, suitable for single-instance deployments
- **Valkey** — `INCR` + `EXPIRE` pattern, shared across instances in multi-pod deployments

The backend is selected automatically based on whether `TSIGMA_VALKEY_URL` is configured.

## 10. API Key Authentication

Machine-to-machine integrations (SOAP ingestion, external monitoring, CI/CD) authenticate via API keys instead of browser-based login flows.

**Key lifecycle:**
- Created via `POST /api/v1/auth/api-keys` (authenticated user required)
- Plaintext key returned exactly once at creation — not retrievable afterward
- Keys are bcrypt-hashed before storage (same principle as passwords)
- Keys use a `tsgm_` prefix for identification
- Optional expiration date (`expires_at`)
- Revocable via `DELETE /api/v1/auth/api-keys/{key_id}` without affecting the user's interactive session
- Listed via `GET /api/v1/auth/api-keys` (metadata only, no hashes)

**Authentication flow:**
1. Client sends `X-API-Key: tsgm_...` header (or `Authorization: Bearer tsgm_...`)
2. `get_current_user_optional` extracts the key before checking session cookies
3. Key prefix is used to look up candidates in the `api_key` table
4. bcrypt verifies the full key against stored hash
5. If valid, non-expired, and non-revoked, returns `SessionData` with the key's user and role

**Model:** `api_key` table with `id`, `user_id`, `name`, `key_hash`, `key_prefix`, `role`, `created_at`, `expires_at`, `revoked_at`, `last_used_at`

---

## 11. Related Documents

| Document | Description |
|----------|-------------|
| [ARCHITECTURE.md](ARCHITECTURE.md) | System architecture overview |
| [API.md](API.md) | API design, endpoints, authentication modes |
| [DATABASE_SCHEMA.md](DATABASE_SCHEMA.md) | Database tables and relationships |
| [CODING_GUIDELINES.md](CODING_GUIDELINES.md) | Code standards and patterns |
