# Data Quality Monitoring & Watchdog System

> Part of [TSIGMA Architecture](../ARCHITECTURE.md)

---

## Table of Contents

1. [Overview](#1-overview)
2. [Implemented: Checkpoint Resilience](#2-implemented-checkpoint-resilience)
   - [Clock Drift Detection](#21-clock-drift-detection)
   - [Checkpoint Cap](#22-checkpoint-cap-httppush-methods)
   - [Silent Signal Detection](#23-silent-signal-detection)
   - [Poisoned Checkpoint Auto-Recovery](#24-poisoned-checkpoint-auto-recovery)
   - [Timestamp Correction Tools](#25-timestamp-correction-tools)
3. [Configuration](#3-configuration)
4. [Checkpoint Model Fields](#4-checkpoint-model-fields)
5. [Data Quality Checks](#5-data-quality-checks)
   - [Low Event Count Detection](#51-low-event-count-detection)
   - [Missing Data Windows](#52-missing-data-windows)
   - [Stuck Detector Detection](#53-stuck-detector-detection)
   - [Stuck Pedestrian Button](#54-stuck-pedestrian-button)
   - [Phase Termination Anomalies](#55-phase-termination-anomalies)
   - [Low Hit Count (Detector)](#56-low-hit-count-detector)
6. [Architecture](#6-architecture)
   - [Notification Integration](#61-notification-integration)
   - [Job Integration](#62-job-integration)
   - [Check Configuration](#63-check-configuration)
7. [Comparison with ATSPM 4.x/5.x WatchDog](#7-comparison-with-atspm-4x5x-watchdog)
8. [Related Documents](#8-related-documents)

---

## 1. Overview

TSIGMA's data quality monitoring system detects anomalies in ingested traffic signal data and alerts operators when problems are found. The system operates at two levels:

- **Real-time (per poll cycle):** Checkpoint resilience checks run inline as part of the `CollectorService` post-poll-cycle analysis. These catch clock drift, silent signals, and poisoned checkpoints immediately as data arrives.
- **Scheduled (background jobs):** Deeper analytics checks run as registered background jobs via `@JobRegistry.register()`. These perform historical analysis across time windows to detect stuck detectors, missing data, and phase termination anomalies.

All alerts route through the notification plugin system, which fans out to configured providers (email, Slack, Teams, etc.).

---

## 2. Implemented: Checkpoint Resilience

The following checks are implemented in the ingestion pipeline and run automatically during each poll cycle.

**Source files:** `collection/service.py`, `collection/sdk/__init__.py`, `collection/methods/ftp_pull.py`, `collection/methods/http_pull.py`

### 2.1 Clock Drift Detection

During event persistence (`persist_events_with_drift_check` in `collection/sdk/`), events with timestamps exceeding `server_time + checkpoint_future_tolerance_seconds` (default 300s / 5 minutes) are flagged as future-dated.

**Behavior:**
- Future-dated events are **still ingested** -- data is never dropped
- A `WARNING` notification is triggered with `alert_type: "clock_drift"`

**Notification metadata:**

| Field | Description |
|-------|-------------|
| `signal_id` | The signal that produced the future-dated events |
| `future_event_count` | Number of events with timestamps beyond the tolerance |
| `total_event_count` | Total events in the batch |
| `max_drift_seconds` | Maximum observed drift (event time - server time) |
| `latest_event_time` | Timestamp of the most future-dated event |

**Example scenario:**

```
Server time:      2026-04-01 14:00:00 UTC
Tolerance:        300 seconds (5 minutes)
Max allowed:      2026-04-01 14:05:00 UTC
Event timestamp:  2026-04-01 14:12:30 UTC  --> flagged (drift = 750s)
```

The event is ingested normally, but the clock drift alert fires so operators can investigate the controller's NTP configuration.

### 2.2 Checkpoint Cap (HTTP/Push Methods)

The `save_checkpoint` function in `collection/sdk/` caps `last_event_timestamp` to prevent future-dated events from advancing the watermark past real time:

```python
capped_timestamp = min(latest_event_timestamp, server_time + tolerance)
```

**Why this matters:** Without the cap, a controller sending events timestamped days in the future would advance the checkpoint watermark. On the next poll cycle, the system would request events after that future timestamp, missing all real events in between.

**Method immunity:**
- **HTTP/Push methods:** Use event-timestamp-based checkpoints -- vulnerable to clock drift, cap applied
- **FTP/SFTP methods:** Use file-based checkpoints (name + size + mtime) -- immune to clock drift, no cap needed

A warning is logged whenever capping occurs.

### 2.3 Silent Signal Detection

After each poll cycle, `CollectorService._check_silent_signals()` queries checkpoints for signals that were polled but returned zero events.

**Detection logic:**

```
if last_successful_poll < (now - poll_interval * 1.5):
    signal is "silent" for this cycle
    consecutive_silent_cycles += 1
```

**Escalation:**

| Consecutive Silent Cycles | Action |
|--------------------------|--------|
| 1 | Counter incremented, no alert |
| 2 | Counter incremented, no alert |
| 3 (default threshold) | Investigation triggered via `_handle_silent_signal()` |

The threshold is configurable via `checkpoint_silent_cycles_threshold`.

### 2.4 Poisoned Checkpoint Auto-Recovery

When a silent signal hits the threshold, `CollectorService._handle_silent_signal()` performs root cause analysis:

**Decision tree:**

```
Signal silent for N consecutive cycles
  |
  +--> Is checkpoint timestamp > server_time + tolerance?
  |      |
  |      +--> YES: Checkpoint is "poisoned"
  |      |      - Roll back checkpoint to server_time
  |      |      - Reset consecutive_silent_cycles to 0
  |      |      - Send CRITICAL notification
  |      |
  |      +--> NO: Checkpoint is valid
  |             - May be a communication issue (controller offline, network)
  |             - Send WARNING notification
```

**CRITICAL notification metadata (poisoned checkpoint):**

| Field | Description |
|-------|-------------|
| `signal_id` | The affected signal |
| `method` | Ingestion method name |
| `old_checkpoint` | The poisoned checkpoint timestamp (ISO-8601) |
| `server_time` | Current server time at detection (ISO-8601) |
| `drift_seconds` | How far ahead the checkpoint was (seconds) |
| `rollback_target` | The new checkpoint value after rollback (ISO-8601) |
| `alert_type` | `"poisoned_checkpoint_recovery"` |

This is a fully automatic recovery mechanism. No operator intervention is required to resume data collection from a poisoned checkpoint, though the CRITICAL alert ensures visibility.

### 2.5 Timestamp Correction Tools

Two API endpoints allow administrators to correct event timestamps after the fact.

#### Bulk Correction

```
POST /api/v1/collection/corrections/bulk
```

**Request body:**

```json
{
    "signal_id": "SIG-1234",
    "start_time": "2026-04-01T08:00:00Z",
    "end_time": "2026-04-01T12:00:00Z",
    "offset_seconds": -750
}
```

Updates all `event_time` values within the specified range by adding `offset_seconds`. Use a negative offset to shift events backward in time.

**Access:** Admin-only (requires admin role).

#### Anchor Correction

```
POST /api/v1/collection/corrections/anchor
```

**Request body:**

```json
{
    "signal_id": "SIG-1234",
    "start_time": "2026-04-01T08:00:00Z",
    "end_time": "2026-04-01T12:00:00Z",
    "recorded_time": "2026-04-01T10:12:30Z",
    "actual_time": "2026-04-01T10:00:00Z"
}
```

Convenience wrapper. The operator identifies a single event whose real-world time is known (e.g., from a traffic camera timestamp). The system computes the offset (`actual_time - recorded_time = -750s`) and applies it to all events in the range.

**Access:** Admin-only (requires admin role).

---

## 3. Configuration

Current configuration settings in `config.py`:

```python
# Data quality / checkpoint resilience
checkpoint_future_tolerance_seconds: int = 300  # 5 minutes
checkpoint_silent_cycles_threshold: int = 3     # Alert after N silent cycles
```

| Setting | Default | Description |
|---------|---------|-------------|
| `checkpoint_future_tolerance_seconds` | `300` | Maximum allowed future drift (seconds) before flagging events |
| `checkpoint_silent_cycles_threshold` | `3` | Consecutive silent poll cycles before triggering investigation |

---

## 4. Checkpoint Model Fields

The `PollingCheckpoint` model (`models/checkpoint.py`) tracks per-signal health state:

```python
class PollingCheckpoint:
    # ... standard checkpoint fields ...
    consecutive_silent_cycles: int    # Reset to 0 on successful ingest
    consecutive_errors: int           # Reset to 0 on success
    last_error: str | None            # Most recent error message
    last_error_time: datetime | None  # When the last error occurred
```

| Field | Reset Condition | Purpose |
|-------|----------------|---------|
| `consecutive_silent_cycles` | Successful ingest (events > 0) | Tracks how many cycles produced zero events |
| `consecutive_errors` | Successful poll (no exception) | Tracks consecutive poll failures (network, auth, etc.) |
| `last_error` | Never cleared (overwritten on next error) | Diagnostic: most recent error message |
| `last_error_time` | Never cleared (overwritten on next error) | Diagnostic: when the last error occurred |

---

## 5. Data Quality Checks

The watchdog job (`tsigma/scheduler/jobs/watchdog.py`) runs daily at 06:00 UTC. Each check is registered as a background job using `@JobRegistry.register()`.

### 5.1 Silent Signal Detection (Daily)

**Purpose:** Detect signals with no events in the last 24 hours.

**Logic:**
- Query `controller_event_log` for the latest `event_time` per `signal_id`
- Flag signals whose latest event is older than 24 hours

**Alert type:** `silent_signal`

**Severity:** WARNING

### 5.2 Stuck Detector Detection

**Purpose:** Detect vehicle detectors reporting excessive ON events (stuck in active state).

**Logic:**
- Query detector ON events (event code 82) in the last hour from `controller_event_log`
- Group by `signal_id` and `event_param` (detector channel)
- Flag channels exceeding `STUCK_DETECTOR_THRESHOLD` (default: 3600 events/hour)

**Alert type:** `stuck_detector`

**Severity:** WARNING

### 5.3 Low Event Count Detection (Planned — not yet implemented)

**Purpose:** Detect signals producing significantly fewer events than expected.

**Logic:**
- Compare event count per signal per time window against a historical baseline
- Flag signals where event count falls below a configurable threshold

**Configuration:**
- Scanning window size (e.g., 1 hour)
- Minimum expected events per window (e.g., 50)

**Alert type:** `low_event_count`

**Severity:** WARNING

### 5.4 Missing Data Windows (Planned — not yet implemented)

**Purpose:** Detect gaps in event data during expected active hours.

**Logic:**
- Scan for time windows where a signal produced zero events
- Only flag during expected active hours (configurable start/end)
- Optionally restrict to weekdays only

**Configuration:**
- `scan_day_start_hour` — beginning of active monitoring window (e.g., 6 AM)
- `scan_day_end_hour` — end of active monitoring window (e.g., 10 PM)
- `weekday_only` — skip weekends

**Alert type:** `missing_data`

**Severity:** WARNING

### 5.5 Stuck Pedestrian Button (Planned — not yet implemented)

**Purpose:** Detect pedestrian push buttons stuck in the pressed state.

**Logic:**
- Query pedestrian event codes for constant actuation patterns
- A healthy ped button produces intermittent actuations; a stuck button produces continuous or abnormally frequent actuations

**Configuration:**
- Maximum expected ped events per scan window (e.g., 200)
- Consecutive cycles threshold

**Alert type:** `stuck_ped_button`

**Severity:** WARNING

### 5.6 Phase Termination Anomalies (Planned — not yet implemented)

**Purpose:** Monitor force-off and max-out ratios to detect phases with abnormal termination patterns.

**Logic:**
- For each phase, compute the ratio of force-offs and max-outs to total phase terminations
- Flag phases where either ratio exceeds a configurable threshold (e.g., 90%)
- A phase that consistently maxes out may indicate insufficient green time
- A phase that consistently force-offs may indicate timing coordination issues

**Configuration:**
- `min_phase_terminations` — minimum sample size before flagging (e.g., 50)
- `percent_threshold` — ratio threshold to trigger alert (e.g., 0.9 = 90%)

**Alert type:** `phase_termination_anomaly`

**Severity:** WARNING

### 5.7 Low Hit Count (Detector) (Planned — not yet implemented)

**Purpose:** Detect detectors with unexpectedly low actuation counts.

**Logic:**
- Count detector actuations per scan window
- Flag detectors below the low-hit threshold
- Differs from stuck detector: the detector is not reporting constant state, but is producing fewer actuations than expected (may indicate a sensitivity issue or partial failure)

**Configuration:**
- `low_hit_threshold` — minimum expected actuations per scan window (e.g., 50)

**Alert type:** `low_hit_count`

**Severity:** WARNING

### 5.8 Alert Suppression Rules (Planned — not yet implemented)

**Purpose:** Allow operators to suppress specific watchdog alerts per signal, component, or check type. Prevents repeated notifications for known issues (e.g., a detector reported as stuck but awaiting maintenance).

Based on ATSPM 5.x `WatchDogIgnoreEvent` pattern.

**Model:** `watchdog_suppression` table in the `config` schema:

| Column | Type | Description |
|--------|------|-------------|
| `id` | Integer (PK) | Auto-incrementing ID |
| `signal_id` | Text | Signal to suppress alerts for |
| `component_type` | Text | `signal`, `approach`, or `detector` |
| `component_id` | Integer (nullable) | Specific detector channel or approach ID (null = all) |
| `issue_type` | Text | Alert type to suppress (e.g., `stuck_detector`, `low_hit_count`, `silent_signal`) |
| `phase` | Integer (nullable) | Specific phase number (null = all phases) |
| `start` | Timestamptz | Suppression window start |
| `end` | Timestamptz | Suppression window end |
| `created_by` | Text | User who created the suppression |
| `reason` | Text | Why the alert is suppressed (e.g., "detector maintenance scheduled 2026-05-01") |

**Issue types** (matches watchdog check alert types):

| Issue Type | Check |
|-----------|-------|
| `silent_signal` | Silent signal detection |
| `stuck_detector` | Stuck detector detection |
| `low_event_count` | Low event count detection |
| `missing_data` | Missing data window detection |
| `stuck_ped_button` | Stuck pedestrian button |
| `phase_termination_anomaly` | Phase termination anomalies |
| `low_hit_count` | Low hit count (detector) |

**Logic:**
- Before sending a notification, the watchdog checks the `watchdog_suppression` table
- If a matching active suppression exists (signal + issue type + component, within start/end window), the alert is skipped
- Expired suppressions (past `end` date) are ignored — alerts resume automatically
- Suppressions with `component_id = NULL` suppress all components of that type for the signal
- Suppressions with `phase = NULL` suppress all phases

**API Endpoints:**

```
POST   /api/v1/watchdog/suppressions          # Create suppression (admin)
GET    /api/v1/watchdog/suppressions          # List active suppressions
DELETE /api/v1/watchdog/suppressions/{id}     # Remove suppression (admin)
```

**Configuration:**
- No global config — suppressions are per-signal, per-check
- Suppression windows are time-bounded — operators must set an end date
- Audit trail via `created_by` field

---

## 6. Architecture

### 6.1 Notification Integration

All alerts use the notification plugin system (`notify()` from `tsigma/notifications/registry.py`).

**Notification flow:**

```
Check detects anomaly
  --> notify(alert_type, severity, metadata)
      --> NotificationRegistry iterates active providers
          --> Each provider checks min_severity threshold
              --> If severity >= threshold: deliver alert
              --> If severity < threshold: skip silently
```

**Severity levels:**

| Level | Use Case |
|-------|----------|
| `INFO` | Informational, no action required |
| `WARNING` | Anomaly detected, operator should investigate |
| `CRITICAL` | Automatic recovery taken or immediate attention needed |

**Key properties:**
- Notifications never block the ingestion pipeline -- all exceptions are caught and logged
- Each provider has an independently configurable `min_severity` threshold
- Fans out to all active providers (email, Slack, Teams, or any custom provider)
- Providers are registered via `@NotificationRegistry.register("name")` decorator

### 6.2 Job Integration

The watchdog registers as a background job via the `@JobRegistry.register` decorator:

```python
@JobRegistry.register(name="watchdog", trigger="cron", hour="6", minute="0")
async def watchdog(session: AsyncSession) -> None:
    """Check for silent signals and stuck detectors."""
    try:
        await _check_silent_signals(session)
    except Exception:
        logger.exception("Silent-signal check failed")

    try:
        await _check_stuck_detectors(session)
    except Exception:
        logger.exception("Stuck-detector check failed")
```

**Source file:** `tsigma/scheduler/jobs/watchdog.py`

The watchdog runs daily at 06:00 UTC and performs two checks:

1. **Silent signals** -- queries `controller_event_log` for signals whose latest `event_time` is older than 24 hours. Sends a WARNING notification listing all silent signals.

2. **Stuck detectors** -- queries `controller_event_log` for detector channels with more than 3600 ON events (event code 82) in the last hour. Sends a WARNING notification listing suspected stuck detectors.

**Design principles:**
- Each check is a discrete async function within the job
- Checks query the `controller_event_log` table directly
- Individual check failures are caught and logged independently
- Results are delivered via the notification plugin system (`notify()`)
- The job does not modify any data -- it is read-only

### 6.3 Check Configuration

The watchdog job uses a hardcoded threshold for stuck detector detection:

```python
# tsigma/scheduler/jobs/watchdog.py
STUCK_DETECTOR_THRESHOLD = 3600  # Max ON events per hour before flagging
```

Checkpoint resilience settings in `config.py` (used by `CollectorService` and collection SDK):

```python
# Data quality / checkpoint resilience
checkpoint_future_tolerance_seconds: int = 300  # 5 minutes
checkpoint_silent_cycles_threshold: int = 3     # Alert after N silent cycles
```

| Setting | Default | Description |
|---------|---------|-------------|
| `checkpoint_future_tolerance_seconds` | `300` | Maximum allowed future drift (seconds) before flagging events |
| `checkpoint_silent_cycles_threshold` | `3` | Consecutive silent poll cycles before triggering investigation |
| `STUCK_DETECTOR_THRESHOLD` | `3600` | Module-level constant: max detector ON events per hour |

---

## 7. Comparison with ATSPM 4.x/5.x WatchDog

| Feature | ATSPM 4.x/5.x | TSIGMA |
|---------|---------------|--------|
| Record existence check | CLI tool, email alerts | Silent signal detection (per-cycle in CollectorService + daily in watchdog job) |
| Clock drift / future timestamps | Not handled (breaks system) | Cap + notify + auto-recover (collection SDK + CollectorService) |
| Poisoned checkpoint recovery | Not possible | Auto-rollback (CollectorService) |
| Stuck detector | WatchDog CLI | Daily watchdog job (`_check_stuck_detectors`) |
| Notification system | Email only | Plugin-based (email, Slack, Teams, extensible) |
| Timestamp correction | Not available | Bulk + anchor API endpoints |
| Real-time detection | None (scheduled only) | Per poll cycle inline checks (CollectorService) |
| Automatic recovery | None (manual intervention) | Poisoned checkpoint auto-rollback (CollectorService) |

---

## 8. Related Documents

- [INGESTION.md](INGESTION.md) -- Checkpoint system details, polling methods, ingestion pipeline
- [ARCHITECTURE.md](../ARCHITECTURE.md) -- Background job system, configuration management
- [NOTIFICATIONS.md](NOTIFICATIONS.md) -- Notification provider plugin system
