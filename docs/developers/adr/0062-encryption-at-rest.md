# Encryption at rest: Fernet for secrets, decrypt at point of use, redact in responses

- **Status**: Accepted
- **Date**: 2026-06-28
- **Deciders**: jsloan@celldara.com

## Context and Problem Statement

TSIGMA stores sensitive values — controller/source credentials (FTP/SFTP/HTTP/broker
passwords) and other secrets. A database leak must not expose them, and API
responses must never return them. How are secrets protected at rest?

## Decision Drivers

- A leaked database must not yield usable credentials.
- The app needs the plaintext at the point of use (e.g. to connect to a controller at poll time).
- Secrets must never appear in API responses or logs.
- Key material must come from the deployment, with a clear precedence.

## Considered Options

- Fernet symmetric encryption; key from a prioritized source; decrypt at point of use; redact in responses
- Plaintext secrets in the database
- Hash-only (no decryption)

## Decision Outcome

**Sensitive fields are encrypted at rest with Fernet** (symmetric authenticated
encryption). The key is resolved from a **prioritized source order** (e.g. env var →
key file → …); the app **encrypts on write** and **decrypts only at the point of
use** (e.g. when a poller connects to a controller), never eagerly. Secrets are
**redacted from all API responses** and logs — they are write-only over the API.
Hashing isn't applicable here: these values must be recoverable to be used.

### Consequences

- A DB leak yields ciphertext, not usable credentials.
- Plaintext exists only transiently, at the point of use.
- API responses and logs never expose secrets (write-only fields).
- Key management becomes a deployment responsibility — losing the key loses the secrets (rotation/backup needed).

### Confirmation

Sensitive fields are Fernet-encrypted at rest; the key comes from the prioritized
source; decryption happens only at point of use; API responses redact secrets; no
secret appears in logs.

## Pros and Cons of the Options

### Fernet + point-of-use decrypt + redaction (chosen)

- Good, because it's leak-safe, minimizes plaintext exposure, and never returns secrets.
- Bad, because key management/rotation is a deployment burden and key loss means secret loss.

### Plaintext in the database

- Bad, because a leak hands out live credentials.

### Hash-only

- Bad, because credentials must be recoverable to connect — hashing can't be used.

## More Information

- ADR-0050 (key source via bootstrap config), ADR-0010 (where sensitive fields live), ADR-0060 (API keys are hashed — a different category)
