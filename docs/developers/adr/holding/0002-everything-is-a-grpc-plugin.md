# ADR-0002: Everything is a gRPC plugin

- Status: Accepted (decision); implementation pending
- Date: 2026-06-27
- Deciders: Jim Sloan

## Context

The current foundation wires subsystems in-process via simple Python inits
(`ReportRegistry.register` decorator + module imports). That cannot support:
vendor-authored plugins in the vendor's own language, shipped as binaries with
proprietary logic hidden; crash isolation; or true CPU/dependency isolation
(one interpreter = one GIL, one `site-packages`).

## Decision

**Every subsystem is a gRPC plugin** over the frozen go-plugin wire protocol
(defined in `TSIGMA-Contract`): decoder, method (poller/listener), report,
auth, notify, storage, validator. First-party components are open-source
plugins; third-party are closed; **same boundary, same ABI**. In-process
registration is **demoted to a dev/test harness**, not the production path.

The **plug point (host + contract) ships proactively - it is the product**, not
demand-gated. Only the *leaves* are gated: a specific vendor's plugin (until
that vendor exists) and the SOMA/APEX NATS producer (until that hardware
ships).

## Rationale

- One architecture, not two (the global simplicity win over per-component
  in-process convenience).
- The contract is dogfooded by first-party code, so it stays honest; no
  first/second-class split between OpenPhase and vendors.
- Vendor IP protection *requires* a language-neutral binary boundary - the
  primary driver.
- GIL: only a separate process gives real parallelism + crash isolation + an
  independent dependency set.
- A plugin architecture's whole point is that the extension point exists
  *ahead of* the extensions; a "plugin" you must rebuild the host to accept is
  not a plugin.

## Consequences

- Existing in-process Python reports are **migrated** onto the boundary (a
  migration, not a purely additive plug point).
- **Broker data-plane:** plugins get no DB credentials/schema; the host serves
  their data callbacks from its own session, tenant-scoped.
- **Trust tiers:** untrusted-vendor-OK (decoder/method/report/notify) vs
  privileged first-party/DOT-sanctioned (auth, storage; validator TBD).
- **Multi-contract binaries** are allowed (one binary serving several
  contracts) but contracts stay separate - the host orchestrates; never a
  merged black box (see ADR-0004).
- Wire protocol, wire types (Arrow/protobuf), never-send-max, and per-subsystem
  service shapes are **contract concerns** - see `TSIGMA-Contract`.

## Related

- `Software/TSIGMA-Contract` (protocol + per-subsystem contracts + their ADRs)
- `REPORTS.md`, `DECODERS.md`, `LISTENERS.md`, `NOTIFICATIONS.md`,
  `STORAGE.md`, `VALIDATION.md`, `SECURITY.md`
- ADR-0001, ADR-0003, ADR-0004
