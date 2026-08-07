# DeviceSource abstraction (signals and roadside sensors)

- **Status**: Accepted
- **Date**: 2026-06-28
- **Deciders**: jsloan@celldara.com

## Context and Problem Statement

Ingestion targets aren't only traffic-signal controllers — TSIGMA also ingests
from roadside sensors (radar/LiDAR). Orchestrators and methods need to enumerate
"the devices I'm responsible for" without hardcoding which device family. How is
the device set abstracted?

## Decision Drivers

- More than one device family is ingested (signal controllers; roadside sensors).
- The same method/transport may serve controllers, sensors, or both.
- Orchestrators (ADR-0036) must enumerate devices uniformly.

## Considered Options

- A DeviceSource abstraction (per-family implementations)
- Hardcode signals only
- A separate method per device family

## Decision Outcome

**A `DeviceSource` abstraction with per-family implementations** (e.g.
`SignalDeviceSource`, `RoadsideSensorDeviceSource`). Both CollectorService and
ListenerService (ADR-0036) take a DeviceSource to enumerate the devices to poll or
route to. One listener type (e.g. a NATS or TCP listener) can serve controllers,
sensors, or both via its configured device source(s) — the transport is decoupled
from the device family.

### Consequences

- New device families are added as new DeviceSource implementations, not new orchestrators.
- One transport/method can serve multiple device families.
- Orchestrators enumerate devices uniformly.
- Device-family-specific schema still lives in its typed tables (ADR-0010).

### Confirmation

Orchestrators take a DeviceSource; signal and roadside-sensor sources exist; a
single listener can be wired to more than one source.

## Pros and Cons of the Options

### DeviceSource abstraction (chosen)

- Good, because it decouples transport from device family, extends to new families, and enumerates uniformly.
- Bad, because of an extra abstraction layer.

### Hardcode signals only

- Bad, because roadside sensors can't be ingested without forking the orchestrators.

### Method per family

- Bad, because it duplicates transport code per family.

## More Information

- ADR-0036 (orchestrators consume DeviceSource), ADR-0010 (typed per-family tables), ADR-0035 (methods)
