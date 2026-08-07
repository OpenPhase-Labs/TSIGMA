# Inter-agency signal sharing via the OpenPhase/NATS push plane

- **Status**: Accepted
- **Date**: 2026-06-28
- **Deciders**: jsloan@celldara.com

## Context and Problem Statement

Some signals are co-owned or shared between agencies (e.g. a state DOT and a
city/county); both want the data, but each agency runs its own single-tenant
install (ADR-0006). One agency physically polls/owns a given signal. How does it
share that signal's data with a partner agency?

## Decision Drivers

- Each agency has its own install (single-tenant, ADR-0006); sharing is peer-to-peer between installs.
- One install owns (ingests) a given shared signal; the partner needs the same data.
- A receiving install shouldn't need vendor decoders for a peer's controllers.
- TSIGMA already defines a canonical event shape (OpenPhase, ADR-0009) and a push plane (NATS/JetStream, ADR-0033).

## Considered Options

- Re-publish shared signals as OpenPhase Protobuf over NATS/JetStream; partner consumes via the push plane
- A bespoke install-to-install REST/file sync
- Heavyweight federation (agreements, signing, separate peer-data tables)

## Decision Outcome

**The owning install shares signals by publishing their canonical events as OpenPhase
Protobuf over NATS/JetStream; the partner install consumes them via the next-gen push
plane (ADR-0033) and ingests them as normal signals.**

- A native OpenPhase CU is simply a **JetStream producer** — it fans its OpenPhase
  events to JetStream and any subscriber consumes them. For a legacy controller,
  **TSIGMA plays that same producer role**: it **imports** (its normal poll + decode,
  legacy → OpenPhase) and then **transmits** (publishes the OpenPhase to JetStream).
  The only difference from a native CU is the import step.
- The partner subscribes to the JetStream stream with a durable consumer and **ingests
  every signal on it as if it were any other signal** — **no decoder**, no separate
  peer-data tables. Provenance records that the data arrived via the inter-agency stream.
- Trust between installs is NATS/JetStream authentication (credentials / mTLS); details
  defer to implementation.

There is therefore **no separate share plane** — there is one JetStream/OpenPhase
substrate. Native CUs and TSIGMA-bridges are both **producers** to it; agency installs
are **consumers**. This gives the JetStream/OpenPhase plane a **present** use case —
TSIGMA bridging legacy controllers onto it — even before native OpenPhase controllers
(SOMA/APEX) ship.

### Consequences

- Sharing reuses the canonical event model + push plane — no new transport or format.
- The receiver needs no vendor decoders for a peer's controllers (data arrives canonical).
- The owning install bridges legacy controllers onto OpenPhase/NATS, which doubles as forward-readiness for native push.
- Shared signals are normal signals on the receiver (flagged with inter-agency provenance), keeping the model simple.
- Inter-agency sharing requires running a NATS/JetStream channel between installs and managing its auth.

### Confirmation

The owning install publishes shared signals as OpenPhase over NATS/JetStream
(normalizing legacy via its decoder); the partner ingests them via a durable JetStream
consumer as normal signals with no decoder; provenance marks the inter-agency origin;
channel auth is configured.

## Pros and Cons of the Options

### OpenPhase/NATS push-plane sharing (chosen)

- Good, because it reuses the canonical model + push plane, needs no decoders on the receiver, unifies the share and push planes, and gives NATS a present use case.
- Bad, because it requires a NATS/JetStream channel + auth between installs, and the owning install carries the producer/bridge role.

### Bespoke REST/file sync

- Bad, because it's a new transport/format to build and likely needs decoders or custom mapping on the receiver.

### Heavyweight federation

- Bad, because agreements/signing/separate peer tables are more than this sharing needs — over-engineered.

## More Information

- ADR-0033 (two ingest planes / the push plane reused here), ADR-0009 (canonical OpenPhase events), ADR-0006 (single-tenant installs; peer-to-peer), ADR-0011 (decoder normalizes legacy → canonical on the owning side)
