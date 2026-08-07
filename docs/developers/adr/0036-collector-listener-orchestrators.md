# Separate Collector and Listener orchestrators

- **Status**: Accepted
- **Date**: 2026-06-28
- **Deciders**: jsloan@celldara.com

## Context and Problem Statement

Polling methods and listener/event-driven methods have different lifecycle shapes
(scheduled per-device vs long-lived per-method). What orchestrates them in the host?

## Decision Drivers

- Polling = scheduled `poll_once()` per device; listeners = long-lived `start`/`stop` per method.
- Different shapes need independent scaling, restart, and failure isolation.
- A listener-heavy DOT may run no polling; a polling-only DOT may run no listeners.

## Considered Options

- Two orchestrators: CollectorService (polling) + ListenerService (listeners + event-driven)
- One orchestrator for everything

## Decision Outcome

**Two independent orchestrators.** **CollectorService** drives the polling methods
(scheduling `poll_once()` across devices); **ListenerService** drives the listener
and event-driven methods (managing long-lived `start()`/`stop()`). They run, scale,
restart, and fail independently. With the env-toggled deployment (ADR-0003), a
deployment can run the collector tier, the listener tier, or both. Both consume a
DeviceSource (ADR-0037).

### Consequences

- Independent scaling/restart/failure isolation per lifecycle shape.
- A deployment runs only the orchestrators it needs (collector-only, listener-only, or both).
- Two orchestration paths to build and operate.

### Confirmation

CollectorService schedules polling methods; ListenerService manages long-lived
listeners; each is independently env-gated; neither blocks the other.

## Pros and Cons of the Options

### Two orchestrators (chosen)

- Good, because it matches the two lifecycle shapes and allows independent scale/restart and deploy-only-what's-needed.
- Bad, because there are two orchestration paths.

### One orchestrator

- Bad, because it forces two lifecycle shapes into one and couples scaling/restart of unrelated work.

## More Information

- ADR-0035 (execution modes), ADR-0037 (DeviceSource), ADR-0003 (env-toggled tiers)
