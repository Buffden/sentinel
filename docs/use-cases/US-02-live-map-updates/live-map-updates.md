# US-02: Continuous Map Updates via WebSocket

**Actor:** Operator
**Status:** Defined

---

## Story

As an operator, I want the map to update continuously without page refresh, and in final v1 only receive entities and alerts within my configured scope.

---

## Acceptance Criteria

- The dashboard maintains a persistent authenticated WebSocket connection to the API.
- Position updates can be pushed without REST polling.
- Reconnection is handled automatically.
- After workspace scope is introduced, the API loads the saved scope and applies it server-side to pushed positions and alerts.
- Scope filtering uses geographic bounds, entity-type filters, and alert-type rules defined by ADR-012.
- Reconnection reloads current saved scope.

---

## Flow Diagrams

### Connection Setup

![Connection Setup](../../../diagrams/docs/use-cases/US-02-live-map-updates/connection-setup.svg)

### Update Push

![Update Push](../../../diagrams/docs/use-cases/US-02-live-map-updates/update-push.svg)

The Position Consumer publishes `position-updates` to Redis pub/sub; API instances forward relevant events to their local WebSocket connections.

---

## Phase Boundary Note

This use case describes **final v1 behavior**. The roadmap intentionally introduces it incrementally:

- Phase 03: authenticated API/WebSocket foundation and first alert delivery on the current API instance;
- Phase 07: durable workspace configuration and server-side scope filtering;
- Phase 08: multi-instance alert fan-out through Redis `alert-events`.

Do not pull Phase 07/08 functionality into Phase 03 solely because the final-state use-case diagrams show it.

---

## Architectural Justification

Justifies: [ADR-008 - Express API Layer](../../adr/ADR-008-express-api-layer.md), [ADR-012 - Workspace Scope and Server-Side Alert Filtering](../../adr/ADR-012-workspace-scope-alert-filtering.md)

WebSockets provide low-latency push without frequent REST polling. Scope is enforced on the server once workspace functionality exists so irrelevant/out-of-scope events are not intentionally sent to the client.
