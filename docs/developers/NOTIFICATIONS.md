# Notification System

> Part of [TSIGMA Architecture](../ARCHITECTURE.md)

---

## Plugin-Based Architecture

**CRITICAL CONCEPT:** Notification providers are **optional plugin modules**. You only include the providers you actually use.

### Modular Deployment

```
Need email alerts only?              -> Include only email.py
Need email + Slack?                  -> Include email.py + slack.py
Building custom provider for PagerDuty? -> Add pagerduty.py, no core changes
Don't need notifications at all?     -> Set notification_providers = "" (empty)
```

**Benefits:**
- Minimal code footprint -- don't bundle unused providers
- Self-registering via decorator -- no core code changes to add providers
- Fault-isolated -- one failing provider never blocks others
- Third-party extensible -- external packages can register custom providers

---

## Overview

TSIGMA's notification system delivers operational alerts when the platform detects anomalies during ingestion and processing. It is the 6th plugin system, following the same registry pattern as Ingestion Methods, Decoders, Jobs, Reports, and Auth Providers.

**Key design decisions:**

1. **Fire-and-forget** -- `notify()` never raises. Individual provider failures are caught and logged.
2. **Fan-out** -- Every active provider receives every notification (subject to severity filtering).
3. **Severity gating** -- Providers declare a `min_severity` threshold; notifications below that threshold are silently skipped.
4. **Non-fatal initialization** -- If a provider fails to initialize (bad credentials, unreachable host), it is logged and skipped. The application continues without it.

---

## Severity Levels

Three severity levels, ordered lowest to highest:

| Level | Value | Default Behavior | Use Case |
|-------|-------|-------------------|----------|
| `INFO` | `"info"` | Skipped by default (most providers default to WARNING) | Informational notices, successful recoveries |
| `WARNING` | `"warning"` | Delivered to all providers with default config | Anomalies requiring attention but not immediate action |
| `CRITICAL` | `"critical"` | Always delivered | Data integrity issues, safety-relevant failures |

Severity comparison is ordinal: `INFO < WARNING < CRITICAL`. A provider with `min_severity = WARNING` will receive `WARNING` and `CRITICAL` notifications but not `INFO`.

---

## Registry

**Location:** `tsigma/notifications/registry.py`

### NotificationRegistry

```python
class NotificationRegistry:
    @classmethod
    def register(cls, name: str):
        """Class decorator. Registers a provider under the given name."""
        ...

    @classmethod
    def get(cls, name: str) -> type[BaseNotificationProvider]:
        """Retrieve a registered provider class by name."""
        ...

    @classmethod
    def list_available(cls) -> list[str]:
        """Return names of all registered providers."""
        ...
```

**Registration pattern** (identical to all other TSIGMA plugin systems):

```python
@NotificationRegistry.register("myservice")
class MyServiceProvider(BaseNotificationProvider):
    ...
```

### Provider Lifecycle

```
Application startup
    |
    v
initialize_providers(settings)
    |
    +-- Read settings.notification_providers  ("email,slack")
    |
    +-- For each name in comma-separated list:
    |       |
    |       +-- NotificationRegistry.get(name)
    |       +-- Instantiate provider
    |       +-- await provider.initialize(settings)
    |       +-- On success: add to _active_providers
    |       +-- On failure: log warning, skip (never fatal)
    |
    v
Providers ready -- notify() fans out to _active_providers
```

---

## Base Class

**Location:** `tsigma/notifications/registry.py`

```python
class BaseNotificationProvider(ABC):
    name: ClassVar[str]
    min_severity: str = "warning"  # Default threshold

    @abstractmethod
    async def initialize(self, settings) -> None:
        """
        Called once at startup. Read configuration from settings,
        validate credentials, establish connections.

        Raise any exception to indicate initialization failure
        (provider will be skipped, not crash the app).
        """
        ...

    @abstractmethod
    async def send(
        self,
        subject: str,
        message: str,
        severity: str,
        metadata: dict | None = None,
    ) -> None:
        """
        Deliver a single notification.

        Args:
            subject:  Short summary (suitable for email subject or Slack header).
            message:  Full notification body (plain text).
            severity: One of "info", "warning", "critical".
            metadata: Optional structured data specific to the alert type.
        """
        ...
```

---

## The notify() Function

**Location:** `tsigma/notifications/registry.py`

```python
async def notify(
    subject: str,
    message: str,
    severity: str = "warning",
    metadata: dict | None = None,
) -> None:
```

**Behavior:**

1. Iterates over all active providers (those that initialized successfully).
2. For each provider, compares the notification's `severity` against the provider's `min_severity`.
3. If the notification severity is equal to or higher than the provider threshold, calls `provider.send()`.
4. Wraps each `send()` call in a try/except. Exceptions are logged but never propagated.
5. Returns `None`. Callers never need to handle notification errors.

**Concurrency note:** Providers are called sequentially within `notify()`. If a provider has high latency (e.g., slow SMTP server), it will delay subsequent providers in the same `notify()` call. Providers should implement reasonable timeouts internally.

---

## Built-In Providers

### Email

**Location:** `tsigma/notifications/providers/email.py`

| Property | Value |
|----------|-------|
| Registry name | `"email"` |
| Transport | SMTP (with optional TLS) |
| Default min_severity | `"warning"` |

**Configuration:**

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `smtp_host` | `str` | `""` | SMTP server hostname |
| `smtp_port` | `int` | `587` | SMTP server port |
| `notification_from_email` | `str` | `""` | Sender address |
| `notification_to_emails` | `str` | `""` | Recipient addresses (comma-separated) |
| `smtp_username` | `str` | `""` | SMTP auth username |
| `smtp_password` | `str` | `""` | SMTP auth password |
| `smtp_use_tls` | `bool` | `True` | Enable STARTTLS |

**Example .env:**

```ini
NOTIFICATION_PROVIDERS=email
TSIGMA_SMTP_HOST=smtp.agency.gov
TSIGMA_SMTP_PORT=587
TSIGMA_NOTIFICATION_FROM_EMAIL=tsigma@agency.gov
TSIGMA_NOTIFICATION_TO_EMAILS=ops@agency.gov,oncall@agency.gov
TSIGMA_SMTP_USERNAME=tsigma
TSIGMA_SMTP_PASSWORD=secret
TSIGMA_SMTP_USE_TLS=true
```

### Slack

**Location:** `tsigma/notifications/providers/slack.py`

| Property | Value |
|----------|-------|
| Registry name | `"slack"` |
| Transport | HTTP webhook (Incoming Webhook) |
| Default min_severity | `"warning"` |

**Configuration:**

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `notification_slack_webhook_url` | `str` | `""` | Slack Incoming Webhook URL |

**Example .env:**

```ini
NOTIFICATION_PROVIDERS=slack
NOTIFICATION_SLACK_WEBHOOK_URL=https://hooks.slack.com/services/T00/B00/xxxx
```

### Microsoft Teams

**Location:** `tsigma/notifications/providers/teams.py`

| Property | Value |
|----------|-------|
| Registry name | `"teams"` |
| Transport | HTTP webhook (Incoming Webhook connector) |
| Default min_severity | `"warning"` |

**Configuration:**

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `notification_teams_webhook_url` | `str` | `""` | Teams Incoming Webhook URL |

**Example .env:**

```ini
NOTIFICATION_PROVIDERS=teams
NOTIFICATION_TEAMS_WEBHOOK_URL=https://outlook.office.com/webhook/xxx
```

### Enabling Multiple Providers

Providers are comma-separated in a single setting:

```ini
NOTIFICATION_PROVIDERS=email,slack,teams
```

All three will receive every notification that meets their severity threshold.

---

## Alert Types

The following table documents all current `notify()` call sites in the codebase.

| Alert | Severity | Source File | Trigger Condition |
|-------|----------|-------------|-------------------|
| Clock Drift | `WARNING` | `collection/sdk/__init__.py` | Events with future timestamps detected during `persist_events_with_drift_check()` |
| Checkpoint Cap | `WARNING` | `collection/sdk/__init__.py` | Checkpoint capped at server_time + tolerance during `save_checkpoint()` |
| Silent Signal | `WARNING` | `collection/service.py` | A signal produced zero events for N consecutive polling cycles |
| Poisoned Checkpoint Recovery | `CRITICAL` | `collection/service.py` | Checkpoint auto-rolled-back after data poisoning detected |
| Silent Signals (daily) | `WARNING` | `scheduler/jobs/watchdog.py` | Signals with no events in 24+ hours (daily watchdog scan) |
| Stuck Detectors | `WARNING` | `scheduler/jobs/watchdog.py` | Detectors with excessive ON events in last hour (daily watchdog scan) |

---

## Metadata Schemas

Each alert type passes a structured `metadata` dict to `notify()`. These are not enforced by a schema class but follow consistent conventions per alert type.

### Clock Drift

```python
{
    "signal_id": str,           # Traffic signal identifier
    "future_event_count": int,  # Number of events with future timestamps
    "total_event_count": int,   # Total events in the batch
    "max_drift_seconds": float, # Maximum observed drift (seconds)
    "latest_event_time": str,   # ISO-8601 timestamp of the most future-dated event
    "alert_type": "clock_drift",
}
```

### Silent Signal

```python
{
    "signal_id": str,           # Traffic signal identifier
    "method": str,              # Ingestion method name (e.g. "ftp_pull")
    "silent_cycles": int,       # Number of consecutive zero-event cycles
    "last_poll": str | None,    # ISO-8601 timestamp of last successful poll
    "alert_type": "silent_signal",
}
```

### Poisoned Checkpoint Recovery

```python
{
    "signal_id": str,           # Traffic signal identifier
    "method": str,              # Ingestion method name
    "old_checkpoint": str,      # The poisoned checkpoint timestamp (ISO-8601)
    "server_time": str,         # Current server time at detection (ISO-8601)
    "drift_seconds": float,     # How far ahead the checkpoint was
    "rollback_target": str,     # The new checkpoint value after rollback (ISO-8601)
    "alert_type": "poisoned_checkpoint_recovery",
}
```

---

## Writing a Custom Provider

### Step 1: Create the Module

Create a new file at `tsigma/notifications/providers/pagerduty.py` (or any name):

```python
from tsigma.notifications.registry import (
    BaseNotificationProvider,
    NotificationRegistry,
)

@NotificationRegistry.register("pagerduty")
class PagerDutyProvider(BaseNotificationProvider):
    name = "pagerduty"
    min_severity = "critical"  # Only page for critical alerts

    async def initialize(self, settings) -> None:
        self.routing_key = settings.notification_pagerduty_routing_key
        if not self.routing_key:
            raise ValueError("PagerDuty routing key is required")

    async def send(
        self,
        subject: str,
        message: str,
        severity: str,
        metadata: dict | None = None,
    ) -> None:
        import httpx

        payload = {
            "routing_key": self.routing_key,
            "event_action": "trigger",
            "payload": {
                "summary": subject,
                "severity": severity,
                "source": "tsigma",
                "custom_details": metadata or {},
            },
        }
        async with httpx.AsyncClient() as client:
            await client.post(
                "https://events.pagerduty.com/v2/enqueue",
                json=payload,
                timeout=10.0,
            )
```

### Step 2: Add Configuration

Add settings to your configuration (environment variables or config file):

```ini
NOTIFICATION_PROVIDERS=email,pagerduty
NOTIFICATION_PAGERDUTY_ROUTING_KEY=abc123...
```

Add the corresponding field to your settings class in `config.py`:

```python
notification_pagerduty_routing_key: str = ""
```

### Step 3: Deploy

No further changes needed. The provider module is auto-discovered on import via the `__init__.py` glob pattern in `tsigma/notifications/providers/`. Adding it to `notification_providers` activates it at startup.

---

## Auto-Discovery

Provider modules in `tsigma/notifications/providers/` are auto-imported at package load time. The `__init__.py` uses a glob import pattern to find all `.py` files in the directory (excluding `__init__.py` itself). This means:

- Dropping a new `.py` file into the providers directory is sufficient for registration.
- Removing a `.py` file removes the provider from the registry.
- No import statements need to be manually maintained.

This is the same auto-discovery mechanism used by all six TSIGMA plugin systems.

---

## Error Handling

The notification system is designed to never interfere with core platform operation.

| Failure Mode | Behavior |
|-------------|----------|
| Provider not found in registry | Logged as warning during `initialize_providers()`, skipped |
| Provider `initialize()` raises | Logged as warning, provider excluded from active list |
| Provider `send()` raises | Logged as error, other providers still receive the notification |
| All providers fail | All errors logged, `notify()` returns normally |
| No providers configured | `notify()` is a no-op (returns immediately) |
| Invalid severity value | Provider may skip or deliver depending on implementation |

---

## Related Documents

- [ARCHITECTURE.md](../ARCHITECTURE.md) -- Plugin system overview and registry pattern
- [INGESTION.md](INGESTION.md) -- Ingestion methods (source of clock drift and checkpoint alerts)
- [WATCHDOG.md](WATCHDOG.md) -- Watchdog service (source of silent signal and data quality alerts)
