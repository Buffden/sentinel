# US-01: Live Entity Tracking on Map

**Actor:** Operator
**Status:** Defined

---

## Story

As an operator, I want to see currently tracked entities on a live map so that I can monitor their latest known positions.

---

## Acceptance Criteria

- All entities within the operator's saved scope whose latest position is still considered live are visible on the map.
- Initial map load comes from Redis `entity:live:*` current-state hashes.
- Ongoing position changes arrive via WebSocket (US-02).
- Client-side staleness uses `last_seen_ms`; the 24h Redis TTL is only a safety net and is not the live-map expiry threshold.
- Entities outside the configured scope are not sent to the dashboard after workspace scoping is introduced.
- The map supports at least hundreds of simultaneous entities without unacceptable rendering degradation.

---

## Flow Diagrams

### Write Path

![Write Path](../../../diagrams/docs/use-cases/US-01-live-entity-tracking/write-path.svg)

A source position travels through Kafka and the Position Consumer into durable TimescaleDB history and Redis live state.

### Read Path

![Read Path](../../../diagrams/docs/use-cases/US-01-live-entity-tracking/read-path.svg)

The API reads current positions from Redis for initial load and relays live updates over WebSocket.

### Entity Expiry

![Entity Expiry](../../../diagrams/docs/use-cases/US-01-live-entity-tracking/entity-expiry.svg)

When no new position arrives, the dashboard removes the marker when its staleness rule based on `last_seen_ms` is exceeded. Redis retains the live-state key for up to 24h as a safety net so the Alert Evaluator still has state to inspect for signal loss. Redis TTL expiry is therefore not the normal dashboard cleanup trigger.

---

## Phase Boundary Note

This use case describes the **final v1 behavior**. Phase 02 establishes Redis live state and position publication. Authentication/workspace scope and full server-side scoped delivery arrive in later roadmap phases. Earlier phases should not implement future workspace functionality merely because it appears in this final-state use case.

---

## Architectural Justification

Justifies: [ADR-004 - Redis for Live Entity State](../../adr/ADR-004-redis-live-state.md), [ADR-012 - Workspace Scope and Server-Side Alert Filtering](../../adr/ADR-012-workspace-scope-alert-filtering.md)

Redis is used because the map needs fast access to the latest entity state. TimescaleDB remains the durable historical store. `last_seen_ms`, not Redis TTL, defines operational freshness; the TTL only prevents permanently abandoned keys.
