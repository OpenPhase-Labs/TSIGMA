# Subsystems are gRPC plugins (separate processes; optional NATS for pub/sub)

- **Status**: Accepted
- **Date**: 2026-06-28
- **Deciders**: jsloan@celldara.com

## Context and Problem Statement

TSIGMA must talk to a long, growing tail of controller formats and integrate
reports, notifiers, auth providers, storage, and validators — and let third
parties (including closed vendors) extend it without forking the core. Embedding
every protocol in the core makes each new device class a core change. How are
extensions integrated?

## Decision Drivers

- The protocol/integration surface is unbounded and grows over time.
- Vendors must ship adapters for proprietary products in any language without forking the core or releasing source (the MPL boundary, ADR-0007).
- Crash isolation: a buggy adapter must not take down the core.
- A simple plugin should be a few hundred lines in any language.
- Multi-consumer telemetry benefits from pub/sub; simple request/response devices don't.
- The core stays complete with zero plugins, except live ingestion (ADR-0014).

## Considered Options

- gRPC contract baseline (required), optional NATS pub/sub per plugin — separate processes
- NATS for every plugin
- Protocols embedded in the core (in-tree)
- In-process registration as the production path

## Decision Outcome

**Every extensible subsystem is a plugin running as a separate process, speaking
gRPC over a defined, language-neutral `.proto` contract.** TSIGMA's plugin
subsystems are: decoder, method (poller/listener), report, notification, auth,
storage, validator. Handshake, capability discovery, commands, queries, and
server-streaming telemetry flow over gRPC. A plugin may opt into **NATS/JetStream**
in its manifest where pub/sub genuinely fits (multi-consumer telemetry, the future
push plane); nothing forces it. Schemas are protobuf, OPENPHASE-aligned for
controller domains (ADR-0009).

The gRPC contract is a stable, language-neutral surface that third-party
(including closed) plugins build against. This is the **go-forward** model and
**supersedes the earlier in-process Python registry** (decorator + auto-discovery)
described in the current docs — that was the pre-gRPC approach and will be
reconciled out. (Whether a thin in-process harness survives for development/testing
is an implementation detail.)

### Consequences

- The core stops growing with every new format/vendor — protocol code lives in plugins.
- Closed vendor plugins are possible (separate process/binary/copyright is the legal + technical seam, ADR-0007); any gRPC-capable language works.
- A single plugin can be upgraded without redeploying the core; a crashing plugin doesn't take the core down.
- Two transports (gRPC + optional NATS) widen the test matrix.
- The stale in-process-registry docs need rewriting (the pre-gRPC approach).

### Confirmation

A reference `.proto` defines the plugin surface (discover, poll/stream, command,
health, register/metadata); the plugin host manages lifecycle and the gRPC plane;
no protocol-specific code lives in the core; new integration lands as a new plugin
process, not a core patch; in-process registration appears only in dev/test.

## Pros and Cons of the Options

### gRPC contract + optional NATS, separate processes (chosen)

- Good, because the floor is low (simple plugins stay simple) and the ceiling is high (pub/sub when needed); one protobuf schema serves both transports; closed/any-language plugins; crash isolation.
- Bad, because the host handles two shapes and the stale docs need rewriting.

### NATS for every plugin

- Bad, because it forces a broker on trivial plugins and request/response over pub/sub is awkward.

### Protocols embedded in the core

- Bad, because every device class becomes a core change, closed/any-language is impossible, and a buggy adapter crashes the core.

### In-process registration as the production path

- Bad, because there are no closed/any-language plugins, no crash/dependency isolation, and the boundary the product needs doesn't exist.

## More Information

- ADR-0007 (MPL boundary), ADR-0009 (OPENPHASE schemas), ADR-0014 (complete-as-is; ingestion is the plugin exception), ADR-0002 (core/plugin boundary)
- Following: process model, manifest/registration, installation, versioning
