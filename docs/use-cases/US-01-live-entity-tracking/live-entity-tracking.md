# US-01: Live Entity Tracking on Map

**Actor:** Operator
**Status:** Defined

---

## Story

As an operator, I want to see currently tracked entities on a live map so that I can monitor their latest known positions.

---

## Acceptance Criteria

- Initial map load comes from Redis `entity:live:*` through the API.
- Only entities allowed by the operator's current server-side workspace scope are returned once workspace enforcement is available.
- Ongoing position updates arrive via WebSocket (US-02).
- An entity marker represents the latest accepted source-event position, never a stale replay that regressed Redis state.
- Client-side staleness removes markers when `now() - last_seen_ms` exceeds the configured display/liveness threshold.
- Redis `entity:live:*` uses a 24h TTL only as a safety net against permanent ghost keys; TTL expiry is not the live-map cleanup trigger and not the signal-loss detector.
- Hundreds of simultaneously visible entities remain usable without requiring the dashboard to query TimescaleDB for every update.

---

## Flow Diagrams

### Write Path

![Write Path](../../../diagrams/docs/use-cases/US-01-live-entity-tracking/write-path.svg)

The Position Consumer persists history and updates monotonic Redis live state before broadcasting accepted live position updates.

### Read Path

![Read Path](../../../diagrams/docs/use-cases/US-01-live-entity-tracking/read-path.svg)

The API serves current Redis state on initial load and pushes subsequent updates over authenticated WebSocket connections.

### Entity Expiry

![Entity Expiry](../../../diagrams/docs/use-cases/US-01-live-entity-tracking/entity-expiry.svg)

If an entity stops broadcasting, no new live update arrives. The dashboard removes a stale marker by comparing its `last_seen_ms` with the configured threshold. The Redis key remains available long enough for the Alert Evaluator's signal-loss scan because its 24h safety TTL is intentionally independent of alert timing.

---

## Architectural Justification

Justifies: [ADR-004 - Redis for Live Entity State](../../adr/ADR-004-redis-live-state.md), [ADR-012 - Workspace Scope](../../adr/ADR-012-workspace-scope-alert-filtering.md)

Redis is used because the access pattern is "latest state by entity" plus high-frequency fan-out. TimescaleDB remains the durable history source and is used for historical/investigation queries rather than every map refresh.
