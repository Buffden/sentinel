# US-02: Continuous Map Updates via WebSocket

**Actor:** Operator
**Status:** Defined

---

## Story

As an operator, I want the map to update continuously without requiring a page refresh so that I see position changes as they happen.

---

## Acceptance Criteria

- The dashboard maintains a persistent WebSocket connection to the API
- Position updates and alert events are pushed to the client as they occur
- Reconnection is handled automatically if the WebSocket connection drops
- The operator does not need to manually refresh to see new data

---

## Flow Diagrams

**Connection setup** - how the dashboard establishes and recovers a WebSocket connection with the API.

**Update push** - how a position update flows from the position consumer through to the operator's map via WebSocket.

---

## Architectural Justification

Justifies: [ADR-008 - Express API Layer](../../adr/ADR-008-express-api-layer.md)

REST polling at the required refresh rate would produce excessive request volume and add unnecessary latency. A persistent WebSocket connection, served via the `ws` library attached to the Express HTTP server, pushes updates as they arrive with minimal overhead. Node's event loop is a natural fit for holding many concurrent WebSocket connections open while fanning live updates out to dashboard clients.
