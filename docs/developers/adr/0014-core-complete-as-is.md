# Core is complete as-is (usable without plugins, except live ingestion)

- **Status**: Accepted
- **Date**: 2026-06-28
- **Deciders**: jsloan@celldara.com

## Context and Problem Statement

TSIGMA is plugin-extensible (decoders, methods, reports, notify, auth, storage,
validators). A failure mode of plugin-heavy designs is a hollow core that's
useless until specific plugins are installed. But TSIGMA's defining job — live
ingestion from controllers — inherently needs method and decoder plugins. What is
the completeness contract for the core?

## Decision Drivers

- An agency must get value and be able to evaluate, demo, and train before wiring live integrations.
- Manually-entered and uploaded data must be first-class, not second-class.
- The plugin boundary must stay clean: if a *non-ingestion* feature needs a plugin, the boundary is wrong.
- TSIGMA's defining capability (live field ingestion) genuinely requires plugins — the one explicit exception.

## Considered Options

- Complete-as-is **except live ingestion** (scoped)
- Fully complete with zero plugins (every feature usable with none)
- Framework/shell requiring foundational plugins to be usable at all

## Decision Outcome

**The core is complete as-is, with one explicit exception: live field ingestion.**
With zero plugins installed, the core is fully usable for: defining
signals/approaches/detectors/sensors and their config; uploading event files for
analysis; querying and reporting on existing or loaded data; the full API and UI;
and audit/config history.

**Live ingestion** — polling/listening to controllers and decoding vendor wire
formats — is the explicit plugin-provided capability (method and decoder plugins).
Everything else must work end-to-end without any plugin; if a non-ingestion
feature requires a plugin, the boundary is reconsidered. Manually-entered and
uploaded data use the same schema, audit, and workflows as plugin-ingested data.

(First-party decoders/methods ship as open plugins, so a default install can
ingest out of the box — but that is plugins doing their job, not the core
depending on them.)

### Consequences

- Agencies can evaluate, demo, and train on a plugin-free install with manual/uploaded data.
- A sharp PR test for non-ingestion features: "does this work with zero plugins?"
- The ingestion exception is explicit and bounded — not a slope toward "everything needs a plugin."
- Manual/uploaded data is first-class — costs UI/UX to show "connected vs unconnected/manual" cleanly.

### Confirmation

Every non-ingestion feature passes an end-to-end smoke test with zero plugins;
uploaded/manual data uses the same schema/audit/workflow as ingested data; PR
review asks "works with zero plugins?" for non-ingestion features.

## Pros and Cons of the Options

### Complete-as-is except live ingestion (chosen)

- Good, because it gives day-one evaluation/training, keeps boundary discipline for everything but ingestion, and is honest about TSIGMA's nature.
- Bad, because the ingestion exception must be guarded so it doesn't expand, and manual data needs first-class UI care.

### Fully complete with zero plugins

- Bad, because it doesn't fit — TSIGMA can't ingest live data without method/decoder plugins.

### Framework/shell requiring plugins

- Bad, because there's no day-one evaluation path and the boundary becomes "whatever we extracted."

## More Information

- ADR-0002 (core principle), ADR-0004 (not-kitchen-sink), ADR-0007 (license / open plugins)
