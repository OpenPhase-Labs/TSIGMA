# Notification channels: built-in core channels + plugin channels

- **Status**: Accepted
- **Date**: 2026-06-28
- **Deciders**: jsloan@celldara.com

## Context and Problem Statement

Notifications (ADR-0066) go out over channels. Some channels are universal and
should work out of the box; others are agency-specific. Which ship in the core, and
which are plugins?

## Decision Drivers

- A fresh install should be able to notify without writing a plugin (complete-as-is, ADR-0014).
- In-app and email are near-universal; chat/SMS vary by agency.
- The channel boundary should follow the plugin model (ADR-0018).

## Considered Options

- Built-in core channels (in-app + email/SMTP) + everything else as plugins
- All channels are plugins
- All channels built into the core

## Decision Outcome

**A small set of built-in channels ships in the core — in-app notifications and
email (SMTP)** — so a zero-plugin install can alert operators (ADR-0014). **All
other channels (Slack, Teams, SMS, webhooks, push, ticketing, …) are notification
plugins** (ADR-0018 / ADR-0066). Built-ins and plugins present the same notifier
interface; severity gating (ADR-0066) applies uniformly.

### Consequences

- A fresh install can notify (in-app + email) with no plugins.
- Agency-specific channels are added as plugins without core changes.
- The common case (email) needs no plugin; the long tail stays out of the core.
- The core carries SMTP / in-app delivery code (small, universal).

### Confirmation

In-app + email/SMTP are built in and work with zero plugins; other channels are
plugins; all use the same notifier interface and severity gating.

## Pros and Cons of the Options

### Built-in (in-app + email) + plugin channels (chosen)

- Good, because it gives zero-plugin alerting, an extensible long tail, and keeps the core small.
- Bad, because there's a line to draw (what's "universal" enough to be built in).

### All channels as plugins

- Bad, because a fresh install can't alert without a plugin (violates complete-as-is).

### All channels in the core

- Bad, because the core bloats with every chat/SMS vendor's SDK.

## More Information

- ADR-0066 (notification model / severity), ADR-0014 (complete-as-is needs built-in alerting), ADR-0018 (channel plugins)
