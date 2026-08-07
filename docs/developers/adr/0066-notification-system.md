# Notifications: plugin-based, fire-and-forget, severity-gated

- **Status**: Accepted
- **Date**: 2026-06-28
- **Deciders**: jsloan@celldara.com

## Context and Problem Statement

TSIGMA's watchdog and integrity checks (ADR-0045, ADR-0044) produce alerts (clock
drift, silent signals, review-queue items) that need to reach operators via email,
chat, etc. How are notifications structured and delivered?

## Decision Drivers

- Alerting must never block or fail ingestion/analytics (best-effort, ADR-0045).
- Agencies use different channels (email, Slack, Teams, SMS) — the set must be extensible.
- Operators want control over how much noise they get (severity).
- Simplicity: a small ops team shouldn't have to manage per-alert routing rules.

## Considered Options

- Plugin notifiers, fire-and-forget, severity-gated, no per-alert routing
- Per-alert / per-user subscription routing
- A hardcoded single channel

## Decision Outcome

**Notifications are a plugin subsystem (ADR-0018), delivered fire-and-forget and
severity-gated:**

- **Plugin notifiers** — each channel (email, Slack, Teams, SMS, …) is a
  notification plugin; built-ins ship for the common ones (ADR-0067).
- **Fire-and-forget / best-effort** — a notification failure is caught and logged;
  it never blocks or fails the work that raised the alert (ADR-0045).
- **Severity-gated, no per-alert routing** — each configured provider has a minimum
  severity (INFO / WARNING / CRITICAL); every alert at or above that severity goes
  to that provider. The active provider set is simple deployment config (e.g. a
  comma-separated list); there is **no per-alert or per-user subscription routing**.

(If finer-grained routing/subscriptions are ever needed, they can be added later
without changing the plugin boundary.)

### Consequences

- Alerting can't take down ingestion (best-effort).
- New channels are plugins — no core change.
- Operators tune noise per provider via a severity threshold, with no routing rules to maintain.
- Routing is coarse: you can't send only "stuck detector" alerts to one channel and "clock drift" to another (acceptable for the target ops scale).

### Confirmation

Notifiers are plugins; delivery is best-effort (failures logged, never blocking);
each provider has a min-severity; alerts ≥ threshold go to all such providers; there
is no per-alert/per-user routing.

## Pros and Cons of the Options

### Plugin + fire-and-forget + severity-gated (chosen)

- Good, because it never blocks work, is extensible, and is simple to operate (no routing rules).
- Bad, because routing is coarse (severity only).

### Per-alert / per-user subscriptions

- Good, because of fine-grained control.
- Bad, because of subscription/routing management overhead a small ops team doesn't want.

### Hardcoded single channel

- Bad, because it isn't extensible — one channel for everyone.

## More Information

- ADR-0045 (watchdog / best-effort alerts), ADR-0044 (review-queue findings), ADR-0018 (notifiers are plugins), ADR-0067 (channels)
