# On-demand poll API: legacy SOAP compatibility + REST trigger

- **Status**: Accepted
- **Date**: 2026-06-28
- **Deciders**: jsloan@celldara.com

## Context and Problem Statement

Some existing deployments drive polling via the legacy SOAP/WCF `GetControllerData`
interface; migrating agencies have tooling that calls it. New integrations want a clean REST trigger.
How does TSIGMA expose on-demand polling?

## Decision Drivers

- Zero-change migration for existing SOAP/WCF clients eases adoption.
- New integrations shouldn't inherit SOAP baggage.
- Some legacy SOAP parameters describe poller mechanics TSIGMA doesn't use.

## Considered Options

- Both: a SOAP/WCF compatibility endpoint + a native REST trigger
- REST only
- SOAP only

## Decision Outcome

**Both.** A **SOAP/WCF compatibility endpoint** (`POST /soap/GetControllerData`)
accepts the legacy SOAP envelope, maps its parameters to TSIGMA config, and triggers
`poll_once()` asynchronously — so existing SOAP clients work unchanged. Legacy
params that don't apply (file deletion, SNMP retry/timeout/port, local dir,
bulk-copy options) are **accepted but ignored and logged**, because they reflect
legacy poller mechanics TSIGMA's design doesn't use. A native **REST trigger**
(`POST /api/v1/signals/{signal_id}/poll` with `{"method": "ftp_pull"}`) serves new
integrations cleanly.

### Consequences

- Existing SOAP/WCF tooling migrates with zero changes.
- New integrations use a clean REST trigger without SOAP baggage.
- Ignored legacy params are logged so operators see what was dropped.
- Two trigger surfaces to maintain.

### Confirmation

The SOAP endpoint accepts the legacy envelope and triggers an async poll; inapplicable
params are logged-and-ignored; the REST endpoint triggers a poll by method.

## Pros and Cons of the Options

### Both SOAP + REST (chosen)

- Good, because it gives zero-change legacy migration and a clean modern trigger.
- Bad, because there are two surfaces to maintain.

### REST only

- Bad, because it breaks existing SOAP/WCF clients (migration friction).

### SOAP only

- Bad, because it saddles new integrations with legacy SOAP.

## More Information

- ADR-0035 (`poll_once` on polling methods), ADR-0036 (CollectorService)
