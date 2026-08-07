# Observability: structured JSON logs to stdout, request-id correlation, OpenTelemetry

- **Status**: Accepted
- **Date**: 2026-06-28
- **Deciders**: jsloan@celldara.com

## Context and Problem Statement

Operators and developers need to debug a distributed, multi-replica deployment
(ADR-0003): follow a request across components, correlate logs, and watch metrics.
What is TSIGMA's observability approach?

## Decision Drivers

- Multi-replica deployments need request correlation across logs.
- Logs should be machine-parseable and aggregation-friendly.
- 12-factor: the app shouldn't own log files/rotation/shipping — the environment aggregates stdout.
- Traces/metrics should use an open, vendor-neutral standard (no lock-in).

## Considered Options

- Structured JSON logs to stdout + request-id correlation + OpenTelemetry (OTLP)
- Plaintext logs to files
- A vendor-specific APM agent

## Decision Outcome

**Three-part observability:**

- **Structured JSON logging to stdout** — every log line is JSON (machine-parseable);
  the app writes to stdout and does **not** manage files/rotation/shipping (the
  deployment aggregates stdout — 12-factor).
- **Request-id correlation** — middleware assigns/propagates a request id on every
  request and attaches it to every log line for that request, so a request can be
  followed across replicas and subsystems.
- **OpenTelemetry** — traces and metrics are instrumented via OpenTelemetry and
  exported over OTLP to whatever backend the agency runs (Jaeger/Tempo/Prometheus/
  vendor). Vendor-neutral, no lock-in.

This is **operational** observability, distinct from the durable **audit** record
(ADR-0005, ADR-0015–0017): logs/traces are ephemeral diagnostics; audit is the
tamper-resistant accountability trail.

### Consequences

- Logs are aggregation-ready and correlatable by request id across replicas.
- The app stays out of log-file management (the platform handles it).
- Traces/metrics work with any OTel-compatible backend the agency already runs.
- An OTel collector/backend is an optional deployment piece (logs + request-id work without it).

### Confirmation

Logs are JSON on stdout (no app-managed files); a request-id is generated/propagated
and present on every log line; OpenTelemetry traces/metrics export via OTLP.

## Pros and Cons of the Options

### JSON stdout + request-id + OTel (chosen)

- Good, because it's correlatable, aggregation-friendly, vendor-neutral, and 12-factor.
- Bad, because raw JSON logs are less human-readable (tooling mitigates) and getting traces needs an OTel backend.

### Plaintext logs to files

- Bad, because they're not machine-parseable, the app owns rotation/shipping, and cross-replica correlation is hard.

### Vendor-specific APM agent

- Bad, because of lock-in — agencies may run different backends.

## More Information

- ADR-0003 (multi-replica deployment being observed), ADR-0012 (coordination), ADR-0015 (audit is separate from operational logs)
