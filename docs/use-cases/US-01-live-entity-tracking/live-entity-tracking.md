# US-01: Live Entity Tracking on Map

**Actor:** Operator
**Status:** Defined

---

## Story

As an operator, I want to see all currently tracked entities on a live map so that I can monitor their positions in real time.

---

## Acceptance Criteria

- All entities with a position update within the last configurable TTL window are visible on the map
- Each entity is rendered at its most recently known position
- Entities that stop broadcasting are removed from the map after the TTL expires
- The map handles at least hundreds of simultaneous entities without degrading render performance

---

## Flow Diagrams

**Write path** - position ping travels from the feed through Kafka and the position consumer into Redis and TimescaleDB.

**Read path** - the dashboard receives live positions from Redis via the API WebSocket and renders them on the map.

---

## Architectural Justification

Justifies: [ADR-004 - Redis for Live Entity State](../../adr/ADR-004-redis-live-state.md)

The map refresh rate demands sub-millisecond reads for current entity positions. TimescaleDB stores the full position history but is optimised for range queries, not point lookups at high frequency. Redis holds only the latest position per entity (`entity:live:{entity_id}`) and expires stale entries automatically via TTL - matching this access pattern exactly.
