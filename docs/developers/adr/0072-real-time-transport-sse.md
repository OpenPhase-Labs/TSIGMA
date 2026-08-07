# Real-time transport: SSE for continuous charts, polling for everything else

- **Status**: Accepted
- **Date**: 2026-06-28
- **Deciders**: jsloan@celldara.com

## Context and Problem Statement

Parts of the UI update live — continuously-updating charts (a live event feed, a
real-time signal-state visualization) and more discrete things (in-app
notifications, watchdog alerts, status indicators, list refreshes). The UI
otherwise pulls metric data via GETs. How does the server deliver live updates to
the browser?

## Decision Drivers

- The need is one-way server→client; there's no bidirectional client→server streaming requirement.
- Continuous live charts benefit from a steady push; discrete/occasional updates (notifications, alerts, status) are fine on a periodic poll and don't justify a persistent connection.
- The transport must work through the reverse proxy (ADR-0064) and across replicas (ADR-0012).
- Simplicity for a small team and the server-rendered stack (ADR-0069).

## Considered Options

- SSE for continuous charts + polling for discrete updates (no WebSocket)
- SSE for everything live
- WebSocket
- Polling only

## Decision Outcome

**Server-Sent Events (SSE) carry continuous, live-updating chart data; everything
that isn't a continuous chart uses polling. WebSocket is not used.** SSE streams the
data behind continuously-updating visualizations (e.g. a live event feed /
real-time signal-state chart), where a steady push genuinely matters; cross-replica
delivery uses Valkey pub/sub (ADR-0012) so any replica can serve any subscriber.
Discrete or occasional updates — in-app notifications, watchdog alerts, status
indicators, list refreshes — use **periodic polling** of the relevant endpoint,
which is simpler and avoids holding an SSE connection for low-frequency data. There
is no bidirectional channel — client→server actions go through the normal REST API
(ADR-0055).

### Consequences

- Live charts get a steady push; SSE connections exist only where continuous streaming is actually needed, not for every live element.
- Notifications/alerts/status use simple polling — no persistent connection for low-frequency data.
- Works through standard reverse proxies and across replicas (Valkey fan-out).
- SSE is one-way — any client→server interaction uses REST (the only need).

### Confirmation

Continuous chart data is delivered over SSE (with Valkey cross-replica fan-out);
notifications/alerts/status and other discrete updates are polled; there is no
WebSocket; client→server goes through REST.

## Pros and Cons of the Options

### SSE for continuous charts + polling for discrete updates (chosen)

- Good, because each update type gets the cheapest fit — steady push only for live charts, simple polling for the rest — while avoiding WebSocket complexity.
- Bad, because there are two mechanisms (SSE + polling) rather than one.

### SSE for everything live

- Bad, because it holds a persistent connection even for low-frequency notifications/status, for no benefit.

### WebSocket

- Bad, because it brings bidirectional complexity with no bidirectional requirement and a heavier proxy/scaling story.

### Polling only

- Bad, because continuous charts would poll constantly — latency and load with no true push.

## More Information

- ADR-0012 (Valkey pub/sub fan-out across replicas), ADR-0064 (reverse proxy / SSE), ADR-0067 (in-app notifications — delivered by polling, not SSE), ADR-0069 (server-rendered UI), ADR-0055 (REST for client→server)
