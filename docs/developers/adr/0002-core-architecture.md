# Core architecture: a single host-owned center, extended only through plugins

- **Status**: Accepted
- **Date**: 2026-06-28
- **Deciders**: jsloan@celldara.com

## Context and Problem Statement

TSIGMA ingests traffic-signal event data from many untrusted sources (controllers
with wrong clocks, stale configs, swapped hardware), persists it, and serves
analytics — and it must be extensible by third parties, including vendors shipping
closed plugins. Before we can say *what* extends the system or *how* (the plugin
boundary, the database layout), we have to define the thing being extended: the
**core**.

What is TSIGMA's core, what does it own, and how is functionality added to it?

## Decision Drivers

- **Data integrity is the product's moat.** Never-lose-data + poison-aware
  handling requires a trusted center that owns the `fetch → decode → validate →
  persist` spine so nothing can route around validation or supply its own
  identity.
- **Untrusted extensions.** Vendor plugins may be closed and buggy; they need a
  hard boundary, crash isolation, and *no* direct access to the database or its
  credentials.
- **One deployable, many shapes.** The same software must run as a single process
  for a small DOT and as separate API / collector / listener / scheduler
  containers at GDOT scale.
- **Self-contained open-source.** The core stands on its own; the only surfaces
  designed to be shared/portable are the gRPC plugin contract and the database
  schema/abstraction.
- **Avoid a kitchen sink.** A single trusted center is only safe if it stays
  small and disciplined.

## Considered Options

- **A. Host-owned core, plugins as leaves.** One core codebase that, at runtime,
  acts as the *host*: it owns the data plane and the orchestration spine, and is
  extended only at typed plugin boundaries. Plugins are leaves; they never modify
  or bypass the core.
- **B. Library / framework, no central host.** Subsystems wire themselves in
  process (decorator + import). This is the inherited starting point.
- **C. Microservices.** Decompose the core into independently deployed services
  from the start.

## Decision Outcome

**Chosen option: A — a host-owned core extended only through plugins.**

The **core** is the non-plugin TSIGMA codebase. At runtime it is the **host**,
and it owns:

- the **data plane** — the database connection, credentials, sessions, and the
  schema/abstraction; plugins get *no* DB credentials and never touch the
  database directly. The host serves plugin data needs through its own sessions,
  scope-checked.
- the **integrity / orchestration spine** — `fetch → decode → validate →
  persist`; the host attaches identity (`signal_id` / `device_id`) and
  validation metadata. Plugins are pure transforms and leaves.
- **lifecycle** — ordered startup/shutdown of subsystems.
- **config & audit** — the runtime-settings registry and the audit spine.
- **the API host** — HTTP surface, middleware, and auth enforcement.

The founding principle: **extend via plugins, never modify core code.**
Functionality is added at defined plugin boundaries (decoder, ingestion method,
report, notification, auth, storage, validator); the core itself is not a
plugin and is not edited to add a capability.

The same core is deployed as one process or many via environment toggles — the
deployment topology does not change what the core *is*.

Two parts of this core design are deliberately stable and language-neutral so
they can be reused beyond this codebase, and each gets its own ADR:

- the **gRPC plugin contract** — the extension boundary (ADR-0003);
- the **database schema / abstraction layout** — the data plane's shape (ADR-0004).

### Consequences

- Good, because identity and the integrity spine live in one trusted place; a
  plugin cannot bypass validation or invent identity.
- Good, because plugins hold no database credentials or schema; the host brokers
  their data, which preserves security and scope isolation.
- Good, because the trust boundary is explicit: a trusted, stateful core vs.
  leaf plugins (some untrusted).
- Good, because one codebase scales from single-process to multi-container by
  environment toggles alone.
- Bad, because the core is a single locus that must stay small and disciplined —
  it provides substrate and data, not domain workflows (those belong in plugins
  and jobs).
- Bad, because every new capability incurs a recurring "core or plugin?" design
  decision.

### Confirmation

- Code review: the only extension points are plugin boundaries; the core is not
  edited to add a capability.
- Plugins never import the database facade or SQLAlchemy; they receive data
  through the host.
- Custom/background jobs treat core tables as read-only.

## Pros and Cons of the Options

### A. Host-owned core, plugins as leaves

- Good, because it gives integrity, identity, and data a single trusted owner.
- Good, because it yields a clean, language-neutral plugin boundary and crash isolation.
- Neutral, because it requires the discipline to keep the core minimal.
- Bad, because the host is a central component that must be carefully scoped.

### B. Library / framework, no central host

- Good, because it is the simplest to start from (in-process registration).
- Bad, because in-process plugins cannot protect vendor IP, give no crash or
  dependency isolation, and would need direct database access.
- Bad, because nothing structurally prevents a component from bypassing validation.

### C. Microservices

- Good, because maximal isolation and independent scaling.
- Bad, because operational overhead is unjustified for the common single-site DOT.
- Bad, because it sacrifices the single-deployable simplicity that env toggles provide.

## More Information

- ADR-0001 (records this practice)
- ADR-0003 (the gRPC plugin contract — the core's extension boundary) — pulls in `holding/0002`
- ADR-0004 (the database schema / abstraction layout — the core's data plane)
- Prior art (inherited, abandoned repo): [`holding/0002-everything-is-a-grpc-plugin.md`](holding/0002-everything-is-a-grpc-plugin.md), [`holding/0004-ingestion-integrity.md`](holding/0004-ingestion-integrity.md)
- `ARCHITECTURE.md` (§5 layout, §7 plugins, §16 SRP review)
