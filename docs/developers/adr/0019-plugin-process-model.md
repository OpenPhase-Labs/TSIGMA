# Plugin process model: core-managed children + cron-scheduled + externally orchestrated

- **Status**: Accepted
- **Date**: 2026-06-28
- **Deciders**: jsloan@celldara.com

## Context and Problem Statement

Plugins span a spectrum: simple adapters running alongside the core, periodic
pollers that only wake on a schedule, and long-lived agents (e.g. a future
inter-agency/Method-B agent or push-plane consumer) that may run on other hosts.
One lifecycle model doesn't fit all. How does the core handle plugin process
lifecycle?

## Decision Drivers

- Plugins range from "hello-world decoder" to "stateful long-lived agent."
- Small single-server installs need plugins to "just work" with no orchestrator.
- Large (GDOT-scale) deployments need plugins to scale independently of the core.
- Air-gapped agencies can't assume an external orchestrator.
- Periodic work shouldn't hold a long-lived process slot.
- The gRPC (+ optional NATS) contract stays identical regardless of how the process starts.

## Considered Options

- Core-managed children only
- Externally orchestrated only
- Hybrid: core-managed children + cron-scheduled + externally orchestrated
- In-tree single-process

## Decision Outcome

**Hybrid — three deployment patterns, same connection contract:**

1. **Core-managed children (most plugins)** — the core forks/execs the plugin and
   supervises it (start/restart/stop/health); gRPC over a Unix socket or loopback.
   Works with zero external orchestrator (small installs).
2. **Cron-scheduled** — periodic work (scheduled syncs, batch exports,
   reconciliation); started at interval, runs, exits. No long-lived slot for
   sporadic work.
3. **Externally orchestrated** — long-running / host-independent / separately-scaled
   plugins; the core discovers and connects via manifest + registry; lifecycle is
   the orchestrator's job (systemd, k8s). **k8s pods scaled per family** is the
   canonical large-scale pattern.

Plugin code is mode-agnostic — the same binary runs as a core-managed child in dev
and a k8s pod in production.

### Consequences

- The simplest plugins stay simplest; mature ops shops use orchestration where it helps.
- Periodic work doesn't waste a process slot; scaling out needs no plugin re-architecture.
- The core implements three lifecycle paths (more supervision code, more failure modes).
- Sandboxing differs per mode (core-applied for children, env-provided for external, scheduler's job for cron).

### Confirmation

The plugin host supports all three paths; the manifest declares a `process_model`
preference; lifecycle events flow into the plugin audit table; reference plugins
exist in each mode.

## Pros and Cons of the Options

### Hybrid (chosen)

- Good, because each pattern matches a real workload, plugins are mode-agnostic, and it scales single-server → GDOT.
- Bad, because the core implements three lifecycle code paths.

### Core-managed only

- Bad, because it's wasteful for periodic work and can't scale plugin processes independently.

### Externally orchestrated only

- Bad, because small/air-gapped agencies must run an orchestrator to load one plugin, and the dev loop is painful.

### In-tree single-process

- Bad, because it breaks the closed-plugin boundary (ADR-0007), a crash takes down the core, it's one-language-only, and upgrades redeploy the core.

## More Information

- ADR-0018 (plugin contract), ADR-0020 (manifest declares process model), ADR-0021 (installation), ADR-0015 (plugin lifecycle events audited)
