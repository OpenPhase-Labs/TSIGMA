# ADR-0001: Two editions, one shared contract

- Status: Accepted
- Date: 2026-06-27
- Deciders: Jim Sloan

## Context

TSIGMA must get DOTs the data they need while letting controller/sidecar
vendors keep their IP protected, even as OpenPhase pushes its own stack fully
open. (Full strategy lives under `business/`; this ADR carries only the
architecture-context slice.)

## Decision

TSIGMA ships as **two editions of one product**:

- **Python = the open edition**, the standard-bearer; anyone can run the
  ecosystem on it for free. "TSIGMA" (this repo) is the Python edition.
- **Go = the commercial edition**, the high-performance / high-density build.

Both are **implementations of one shared, language-neutral, independently
governed, published contract**: `Software/TSIGMA-Contract`. Neither edition
owns the contract; both codegen from it. One vendor plugin built against the
contract runs on either host.

## Rationale

- Open data/contract layer + commercial implementations = the Heritage Grid /
  Qualcomm-Dolby model (open interface, proprietary implementations).
- "Works in both" is enforced **by construction** - one contract source, two
  codegens - not by vigilance.
- Publishing the contract artifact (even though one author owns both editions)
  **is** the open data layer.

## Consequences

- The contract is a separate governed/published artifact. **Contract-level
  ADRs live in `TSIGMA-Contract`, not here.**
- The commercial moat is performance / scale / closed add-ons (MCP-PRO,
  per-customer) / services + hosting - NOT the plugin capability, which is open
  in both editions.
- Both editions are authored by OpenPhase (one author); there is no external
  Go team.

## Related

- `business/` - full multi-repo strategy
- `Software/TSIGMA-Contract` - the contract + its ADRs
- ADR-0002 (the plugin model both editions implement)
