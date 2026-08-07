# Holding — ADRs inherited from an abandoned repo

These five ADRs (originally numbered 0001–0005) were pulled from a now-abandoned
repo. They are **NOT authoritative** and are **NOT part of the active ADR set**.
They are kept here as reference / prior art.

As the fresh ADR effort walks the architecture, each is **pulled back in at its
topic** — reconsidered, rewritten in the MADR template, and given a fresh
sequential number if accepted.

TSIGMA is the open-source version. Its ADRs do not describe any closed-source
implementation; the only surfaces shared with such an implementation are the
**gRPC plugin contract** and the **database schema / abstraction layout**.

| Holding file | Topic / where it gets reconsidered |
|---|---|
| 0001-two-editions-one-shared-contract.md | **Not carried in** — two-editions framing is out of scope for the open-source set. Only the published gRPC-contract idea survives, via the plugin ADR. |
| 0002-everything-is-a-grpc-plugin.md | Plugin architecture (the extension boundary; the shared, language-neutral contract) |
| 0003-two-ingest-planes.md | Ingestion planes (legacy poll + next-gen push) |
| 0004-ingestion-integrity.md | Ingestion integrity (never-lose-data + poison-aware) |
| 0005-config-effective-date.md | Config / audit valid-time axis |

Original numbers here do **not** reserve numbers in the fresh set.
