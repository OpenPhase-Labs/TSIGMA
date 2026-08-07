# Three ingestion execution modes, one registry

- **Status**: Accepted
- **Date**: 2026-06-28
- **Deciders**: jsloan@celldara.com

## Context and Problem Statement

Ingestion methods vary in how they run: some pull on a schedule (poll an FTP/HTTP
source), some hold a long-lived connection and receive pushes (a broker
subscription, an inbound socket), some watch a resource and react (a directory).
A single lifecycle doesn't fit all. How are ingestion methods modeled?

## Decision Drivers

- Pull, push, and watch methods have fundamentally different lifecycles.
- All methods should be discoverable/registered uniformly.
- Orchestration differs: polling is scheduled per-device; listeners are long-lived per-method.

## Considered Options

- Three execution-mode base classes, one registry
- One generic method interface for all
- Separate registries per mode

## Decision Outcome

**Three execution-mode base classes sharing one registry:**

- **PollingIngestionMethod** — pull; the host calls `poll_once()` per device on a schedule.
- **ListenerIngestionMethod** — push; one long-lived `start()`/`stop()` per method holds the connection (broker subscription, inbound socket).
- **EventDrivenIngestionMethod** — watch; reacts to external events (e.g. a directory/file appearing).

All three self-register in one method registry so they're uniformly discoverable;
the host orchestrates each by its mode.

### Consequences

- Each mode gets the lifecycle it needs (scheduled vs long-lived vs reactive).
- One registry keeps discovery/config uniform across modes.
- The host's orchestrators dispatch by mode (ADR-0036).
- A method author picks the base class matching their transport.

### Confirmation

Methods subclass one of the three bases and register in the single method registry;
the host runs polling methods on a schedule and listener methods as long-lived tasks.

## Pros and Cons of the Options

### Three modes, one registry (chosen)

- Good, because each transport gets the right lifecycle and discovery stays uniform.
- Bad, because there are three base classes to maintain.

### One generic interface

- Bad, because it forces pull/push/watch into one shape with an awkward lifecycle.

### Separate registries per mode

- Bad, because it fragments discovery/config for no gain.

## More Information

- ADR-0036 (Collector/Listener orchestrators), ADR-0018 (methods are plugins), ADR-0033 (ingest planes)
