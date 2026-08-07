# CV/V2X scope: out of the time-critical path; SPaT ingestion deferred

- **Status**: Accepted
- **Date**: 2026-06-28
- **Deciders**: jsloan@celldara.com

## Context and Problem Statement

Connected-vehicle / V2X functions (broadcasting SPaT to approaching vehicles, BSM,
RTCM) run on single-digit-millisecond latency budgets at the roadside. TSIGMA is a
signal-event ingestion + analytics platform. Does CV/V2X belong in TSIGMA?

## Decision Drivers

- V2X broadcast is a millisecond-budget, safety-critical edge/roadside function; a round-trip through an analytics platform can't meet that budget.
- TSIGMA's value is latency-tolerant: ingest, store, analyze, report.
- The canonical event model already carries SPaT semantics (OpenPhase / J2735), so a future SPaT feed would need no new format.
- TSIGMA has no present need for SPaT data.

## Considered Options

- Out of the time-critical CV/V2X path; SPaT ingestion deferred (not today, possible future via the normal planes)
- TSIGMA participates in the real-time V2X broadcast path
- CV/V2X entirely out of scope forever

## Decision Outcome

**Latency budget defines the boundary: TSIGMA stays out of the time-critical CV/V2X
data path.** Millisecond-budget, vehicle-facing functions (SPaT broadcast, BSM,
RTCM) live at the **edge/roadside**; TSIGMA owns only the latency-tolerant side
(config, protocol semantics, audit, analytics) and never sits in the real-time
broadcast loop.

**SPaT data ingestion is not needed today and is out of present scope.** If a future
need arises, SPaT enters through the **normal ingest planes** (ADR-0033) and the
**canonical model** (OpenPhase already defines J2735 SPaT, ADR-0009) — analytics
only, still without TSIGMA in the broadcast path. No architecture is built for it now.

### Consequences

- TSIGMA doesn't take on a real-time safety path it can't meet — a clean scope boundary.
- No SPaT ingestion or CV/V2X code is built today.
- A future SPaT-analytics need reuses the existing planes + canonical model (no new transport/format), so deferring costs nothing structurally.
- The latency-budget rule generalizes: any future millisecond-budget function is an edge concern, not a TSIGMA one.

### Confirmation

No CV/V2X real-time broadcast path in TSIGMA; no SPaT ingestion built today; a future
SPaT need arrives via the normal ingest planes + canonical model, analytics-only.

## Pros and Cons of the Options

### Out-of-path + SPaT deferred (chosen)

- Good, because the scope is honest (no unmeetable real-time path), nothing is built prematurely, and a future path is cheap (reuses planes/model).
- Bad, because if SPaT analytics is wanted later, it's net-new work then (though structurally ready).

### Participate in the real-time V2X path

- Bad, because TSIGMA can't meet the latency budget — the wrong layer for safety-critical broadcast.

### Out of scope forever

- Bad, because it forecloses a plausible future SPaT-analytics use the model already supports.

## More Information

- ADR-0009 (canonical model already carries J2735 SPaT), ADR-0033 (ingest planes a future SPaT feed would use), ADR-0002 (core scope)
