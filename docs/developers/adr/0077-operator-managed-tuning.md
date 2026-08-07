# Fine-tuning is operator-managed, never hardcoded or env-bound

- **Status**: Accepted
- **Date**: 2026-07-16
- **Deciders**: jsloan@celldara.com

## Context and Problem Statement

TSIGMA's operators are traffic-operations staff, not system administrators. The
running system must be tuned during normal operation — worker counts, polling
cadence, signals per worker queue, per-worker concurrency, thresholds, retention —
and that tuning is routine operation, not deployment. Who tunes it, and through
what surface?

## Decision Drivers

- The target operator is Traffic Ops; managing the app must not require shell/host access or a sysadmin.
- Fine-tuning happens while the system runs — it is operation, not a redeploy.
- A hardcoded constant needs a code change + redeploy to tune: out of an operator's reach.
- An env var needs host access + a restart: a sysadmin/deploy task, not an operator task.
- A bad value must not break a subsystem — tuning still has to be typed and bounds-checked (ADR-0051).

## Considered Options

- Operational tuning lives in the runtime registry (ADR-0051), UI-managed by Traffic Ops
- Operational tuning via env vars / config files (host access + restart)
- Operational tuning hardcoded (code change + redeploy)

## Decision Outcome

**Chosen option**: "runtime registry, UI-managed", because it is the only option an
operator can use without host access, a sysadmin, or a restart.

Any value Traffic Ops tunes to operate the signal system — worker counts,
signals-per-worker-queue, polling cadence, per-worker concurrency, thresholds,
retention ages, feature toggles — is a **runtime-registry knob** (ADR-0051): typed,
bounded, safe-defaulted, UI-editable at runtime, audited, no restart. It is never a
hardcoded constant and never requires an env var to set.

The dividing line:

- **Operator-tunable** (runtime knob, ADR-0051): the operational values above.
- **Bootstrap-only** (env/file, ADR-0050): pre-DB infrastructure and secrets — DB
  connection, broker URLs, secret-key source, ports, storage backend. Setting these
  *is* a deploy/sysadmin task.
- **Fixed** (code + migration): semantic invariants — the canonical event model,
  wire semantics, schema and audit rules. Tunable by no one at runtime.
- **Per-process identity** (e.g. a worker's own shard index) is bootstrap identity,
  not a fleet tuning value.

For a runtime knob, env remains only a debug/deploy override in the ADR-0050
precedence chain — never the path an operator uses to tune.

### Consequences

- Good, because Traffic Ops runs and tunes the whole system from the app — no host access, no sysadmin, no restart.
- Good, because every operational knob is typed, bounded, safe-defaulted, and audited (ADR-0051/ADR-0015): bad values are rejected, changes are traceable.
- Good, because TSIGMA starts and runs zero-config; the defaults are operable out of the box.
- Bad, because values a boot-time design would read once (worker count, concurrency, cadence) must now be read live: orchestrators (ADR-0036) watch their knobs and re-scale/re-shard without a restart.
- Bad, because this refines ADR-0039 (worker count/concurrency were env-set) and ADR-0050's env framing; both must be reconciled.

### Confirmation

Operational tuning is settable from the admin UI without host access or restart; no
operational value is hardcoded or env-required; every knob is registered (ADR-0051)
with type/bounds/default; changes are audited (ADR-0015); bootstrap keys stay
env/file-only.

## Pros and Cons of the Options

### Runtime registry, UI-managed (chosen)

- Good, because Traffic Ops tunes the live system with no host access, sysadmin, or restart.
- Good, because values are typed, bounded, safe-defaulted, and audited.
- Bad, because orchestrators must react to knob changes live rather than reading config once at boot.

### Env vars / config files

- Bad, because tuning requires host access and a restart — a sysadmin task the operator can't perform.
- Bad, because values aren't typed/bounded or audited, and there's no operator-facing surface.

### Hardcoded

- Bad, because every tuning change is a code change and redeploy — entirely out of an operator's reach.

## More Information

- ADR-0050 (config layering/precedence — refined: env is a debug-only override for operational knobs, not an operator path)
- ADR-0051 (runtime-settings registry — the mechanism these knobs use)
- ADR-0039 (poll scale-out — refined: worker count and concurrency become registry knobs, not env vars; a worker's shard *identity* stays bootstrap)
- ADR-0036 (orchestrators watch their scale knobs and adjust live)
- ADR-0015 (every knob change audited)
