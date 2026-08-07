# Canonical HiRes event model: OPENPHASE / NTCIP / Indiana Hi-Res basis

- **Status**: Accepted
- **Date**: 2026-06-28
- **Deciders**: jsloan@celldara.com

## Context and Problem Statement

TSIGMA is built around high-resolution signal events. It needs canonical schemas
covering NTCIP 1202 controller data, SAE J2735 SPaT, Indiana Hi-Res (ATSPM)
events, health, faults, and discovery. Designing these from scratch duplicates
public-domain work and risks drifting from the standards that make ATSPM
compatibility and inter-agency exchange possible.

## Decision Drivers

- Standards conformance: NTCIP 1202 v03A, SAE J2735, Indiana Hi-Res events.
- ATSPM compatibility — be a drop-in IHR producer/consumer (UDOT ATSPM consumes IHR).
- License alignment with the core (MPL-2.0, ADR-0007).
- Transport-independent (gRPC baseline + NATS for a future push plane).
- The architecture anticipates future controllers emitting OPENPHASE natively; one canonical model must serve both them and decoded legacy events.

## Considered Options

- Adopt OPENPHASE as the canonical basis
- Design TSIGMA-native protos from scratch
- No schema (JSON over HTTP)

## Decision Outcome

**Adopt OPENPHASE** (https://github.com/OpenPhase-Labs/OPENPHASE, MPL-2.0) as the
basis for TSIGMA's canonical HiRes event model — eight per-domain `.proto` files:
common envelope, ntcip (1202 v03A), spat (J2735), ihr_events (Indiana Hi-Res),
health, faults, security, discovery.

Today, legacy controllers are **decoded into** this canonical shape (see
ADR-0011). The design also anticipates a future push plane where next-gen
controllers emit OPENPHASE natively (no decoder) — the same canonical model serves
both. For domains OPENPHASE doesn't cover (e.g. some roadside-sensor data), follow
the same per-domain, versioned, MPL-2.0 pattern and contribute upstream.

### Consequences

- The signal schema is "already done"; standards-conformance is verifiable; ATSPM compatibility is essentially free.
- The canonical shape is the contract everything downstream (storage, analytics, reports, inter-agency) speaks.
- An upstream dependency — schema evolution coordinates with OPENPHASE rather than being unilateral.

### Confirmation

OPENPHASE `.proto` files vendored/submoduled; the core consumes OPENPHASE shapes;
decoders for legacy controllers emit them; non-covered domains follow the same pattern.

## Pros and Cons of the Options

### Adopt OPENPHASE (chosen)

- Good, because it covers the signal domains TSIGMA needs, is license-identical, has standards alignment built in, and makes ATSPM compatibility free.
- Bad, because schema evolution depends on an upstream, and it covers signals — not all roadside-sensor data.

### TSIGMA-native protos from scratch

- Bad, because it duplicates public work, forces independent tracking of NTCIP/J2735/IHR revisions, and weakens the conformance story.

### No schema (JSON over HTTP)

- Bad, because it loses type safety, codegen, and efficient encoding, and gives NATS consumers no contract.

## More Information

- OPENPHASE: https://github.com/OpenPhase-Labs/OPENPHASE
- ADR-0007 (MPL-2.0 alignment); forthcoming: ADR-0011 (semantics-in-core / wire-in-decoders), gRPC plugin contract
