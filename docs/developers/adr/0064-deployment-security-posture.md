# Deployment security posture: reverse-proxy TLS, security headers, explicit CORS

- **Status**: Accepted
- **Date**: 2026-06-28
- **Deciders**: jsloan@celldara.com

## Context and Problem Statement

TSIGMA is deployed behind agency infrastructure. Where is TLS terminated, and what
HTTP security controls does the app enforce itself?

## Decision Drivers

- TLS termination, certs, and renewal are better handled by mature reverse proxies than reimplemented in-app.
- Browsers need defense-in-depth headers (clickjacking, MIME-sniffing, transport security, etc.).
- Cross-origin access must be explicit, not wildcard.
- Security checks must run before application logic.

## Considered Options

- No in-app TLS (reverse proxy required) + defense-in-depth headers + explicit CORS + security-first middleware order
- In-app TLS termination
- Minimal / no security headers

## Decision Outcome

**The app serves HTTP and does not terminate TLS — a reverse proxy
(nginx/Caddy/etc.) terminates TLS** and forwards to the app; deployment requires the
proxy. The app enforces **defense-in-depth security headers** (HSTS,
X-Content-Type-Options, X-Frame-Options / frame-ancestors, a Content-Security-Policy,
referrer policy), an **explicit CORS allowlist** (no wildcard with credentials), and
a **security-first middleware order** (auth / headers / rate-limit middleware run
before application handlers).

### Consequences

- TLS/cert lifecycle is handled by proven proxy tooling, not app code.
- Browser-side attack surface (clickjacking, MIME-sniffing, mixed content) is reduced by headers.
- Cross-origin access is deliberate and auditable.
- Deployment must include and correctly configure a reverse proxy (a documented requirement).

### Confirmation

The app serves HTTP only (no TLS in-process); deploy docs require a TLS-terminating
proxy; security headers are set on responses; CORS is an explicit allowlist; security
middleware precedes handlers.

## Pros and Cons of the Options

### Reverse-proxy TLS + headers + explicit CORS (chosen)

- Good, because it uses proven TLS tooling, gives strong browser defenses, makes CORS deliberate, and orders middleware fail-closed.
- Bad, because a proxy is a hard deployment dependency.

### In-app TLS termination

- Bad, because it reimplements cert/renewal/cipher management the proxy already does well.

### Minimal headers

- Bad, because it leaves browsers exposed to common attacks.

## More Information

- ADR-0003 (deployment topology), ADR-0061 (CSRF / SameSite), ADR-0058 (CORS for the public read surface)
