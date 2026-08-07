# Pluggable file storage backend (filesystem + S3)

- **Status**: Accepted
- **Date**: 2026-06-28
- **Deciders**: jsloan@celldara.com

## Context and Problem Statement

Cold exports, raw-file archives, and backups need blob storage. Agencies run
on-prem (local disk / NAS) and cloud (S3 / MinIO). How is blob storage abstracted?

## Decision Drivers

- On-prem and air-gapped need a local filesystem; cloud-native want S3-compatible object stores.
- The cloud dependency (`aiobotocore`) shouldn't burden filesystem-only installs.
- One interface so cold-tier export/query and other blob I/O don't care which backend is active.
- S3 credentials are sensitive and must be handled safely.

## Considered Options

- Pluggable backend factory (filesystem + S3), extensible
- Filesystem only
- S3 only

## Decision Outcome

**A pluggable storage backend behind a single ABC + factory.** Two built-in
backends ship: **filesystem** (default) and **S3** (any S3-compatible store). The
backend is chosen at startup (`TSIGMA_STORAGE_BACKEND`); the S3 dependency is
imported lazily so filesystem-only installs don't need it. The ABC is
`put/get/delete/exists/list_files/get_url`; a new backend is added by implementing
it and extending the factory.

S3 credentials are **sensitive storage config, encrypted at rest** (or sourced
from the default cloud credential chain / instance profile) — they are *not*
database credentials, so they sit outside the "plugins get no DB creds" rule;
they're the core's own blob-store access.

### Consequences

- On-prem (filesystem) and cloud (S3/MinIO) are both supported with no code change — config only.
- Filesystem-only installs avoid the cloud SDK dependency (lazy import).
- Cold-tier export and query use the same backend choice.
- Adding a backend is contained (implement the ABC + a factory branch).

### Confirmation

The factory selects the backend from config and falls back to filesystem on an
unknown value; the S3 import is deferred; S3 secrets are encrypted at rest or use
the default credential chain; the filesystem backend enforces path-traversal
protection.

## Pros and Cons of the Options

### Pluggable factory (filesystem + S3) (chosen)

- Good, because it fits on-prem and cloud, keeps the cloud dependency optional, and presents one interface.
- Bad, because the cold-tier reader dispatches per backend (no abstract base there).

### Filesystem only

- Bad, because there's no cloud-native deployment path.

### S3 only

- Bad, because it forces an object store + SDK on every install (painful for air-gapped/on-prem).

## More Information

- ADR-0031 (cold-tier Parquet/DuckDB uses this backend), ADR-0029 (lifecycle)
- Resolves Q6: storage-backend secrets are sensitive config (encrypted at rest / default credential chain), distinct from DB credentials.
