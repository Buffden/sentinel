# ADR-004: Redis for Live Entity State

**Status:** Accepted
**Date:** 2026-08-05

---

## Context

The dashboard, Alert Evaluator, and Correlation Worker need low-latency access to current entity state. TimescaleDB is the durable position-history source, but repeatedly querying latest-row-per-entity would add unnecessary load and latency.

Sentinel also needs ephemeral state for live H3 candidate lookup, anomaly episodes, leader election, and API fan-out.

---

## Decision

Use Redis for ephemeral live state and coordination:

- `entity:live:{entity_id}` hashes for latest accepted position/liveness state;
- `geo-cell:{live_geo_cell}` sorted sets for live spatial candidate lookup;
- alert/deviation/proximity episode keys defined in `DATA_MODEL.md`;
- `alert-evaluator:leader` lease;
- `position-updates` and `alert-events` pub/sub channels.

Redis is not the durable source of truth for historical position or alert lifecycle state.

---

## Live Entity State

`entity:live:{entity_id}` includes location, entity type, `last_seen_ms`, and `live_geo_cell`.

The Position Consumer must reject source-time regression: an event older than the stored `last_seen_ms` cannot replace current live state or move the entity backward into an older live H3 cell.

The key uses a 24h TTL as a safety net only. It must outlive signal-loss thresholds so the Alert Evaluator still has state to inspect.

Dashboard marker cleanup and signal-loss detection both compare `last_seen_ms`; neither relies on Redis TTL expiry.

---

## Live H3 Spatial Index

Each `geo-cell:{h3_cell_id}` is a sorted set with:

- member=`entity_id`;
- score=`last_seen_ms`.

On accepted cell movement the Position Consumer removes the entity from the previous live cell and adds it to the new one. Correlation Worker uses score-bounded reads across the current cell plus computed neighbors, then calculates exact distance.

---

## Recovery

Redis loss does not destroy durable history, but live state must be reconstructed carefully.

Preferred recovery options:

1. latest-row-per-entity reconstruction from TimescaleDB; or
2. a deliberately bounded recent Kafka tail if it is known to contain only relevant current state.

Do **not** replay arbitrary full historical telemetry through normal live processing merely to warm Redis, because that can generate stale ephemeral side effects. Historical backfill uses a separate mode/group and suppresses live-state/pub-sub/alert side effects.

---

## Pub/Sub

`position-updates` distributes accepted live position changes to API instances.

`alert-events` distributes durable alert lifecycle changes between API instances once Phase 08 enables multi-instance fan-out.

Redis pub/sub is not durable messaging. Kafka/TimescaleDB remain the recovery boundaries for durable facts.

---

## Alternatives Considered

### Query TimescaleDB for every current-state read — rejected

Correct but unnecessarily expensive and higher-latency for high-frequency current-position access.

### Memcached — rejected

Does not provide the sorted sets, pub/sub, or lease primitives Sentinel already needs.

### Redis as durable alert/history store — rejected

Sentinel requires relational queryability and transactional lifecycle operations for durable alerts; those belong in TimescaleDB/PostgreSQL.

---

## Consequences

- Redis state is explicitly ephemeral and reconstructible.
- All live-state writers must enforce source-time monotonicity.
- H3 live membership is a Redis concern, not a TimescaleDB partitioning concern.
- Redis restart is a required failure-lab scenario.
- The developer must know how to inspect hashes, sorted sets, TTLs, and leader ownership directly.
