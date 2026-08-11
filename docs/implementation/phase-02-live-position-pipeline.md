# Phase 02 — Live Position Pipeline

## Goal

Build the first complete streaming path:

```text
External/Synthetic Event
        ↓
Ingestion Poller
        ↓
adsb.raw / ais.raw
        ↓
Position Consumer
        ↓
TimescaleDB
        ↓
Redis
        ↓
position.normalized
```

---

## Suggested Checkpoints

1. Produce and consume one Kafka event from Node.js
2. Implement one ingestion source (OpenSky ADS-B)
3. Normalize one source-native event into the canonical schema
4. Implement DLQ routing for malformed events
5. Persist normalized events to `position_history` with idempotency
6. Prove duplicate insert safety (`ON CONFLICT DO NOTHING`)
7. Write Redis live state (`entity:live:{entity_id}` hash)
8. Prove stale event protection (timestamp guard on Redis writes)
9. Compute H3 cell IDs (`history_geo_cell` and `live_geo_cell`)
10. Maintain the Redis live spatial index: remove stale membership from the previous `geo-cell:{h3_cell_id}` sorted set and `ZADD` the entity to the current `geo-cell:{live_geo_cell}` with score=`last_seen_ms`
11. Prove H3 boundary movement is correct: when an entity moves to a new live cell, it disappears from the old cell and appears in the new cell without stale duplicate membership
12. Publish `position.normalized` to Kafka

---

## Important Learning

- producer/consumer model
- consumer groups and offset management
- event serialization
- at-least-once delivery
- idempotency
- Kafka replay
- event time vs processing time
- stale and out-of-order events
- ephemeral live state vs durable history
- H3 spatial indexing
- maintaining a live spatial candidate index as entities move between H3 cells

---

## Required Failure Experiments

Deliberately test:

- duplicate Kafka delivery → assert no duplicate `position_history` row
- process crash after TimescaleDB write but before Kafka offset commit → observe replay on restart
- telemetry with an older timestamp arriving after a newer one → assert Redis is not regressed
- replay/out-of-order telemetry → assert stale events do not move an entity backward into an older `geo-cell:*` membership
- entity crosses an H3 cell boundary → assert old-cell membership is removed and exactly one current-cell membership remains
- malformed input → assert event goes to DLQ, consumer does not crash
- event with null/missing position → assert skipped with a metric, not DLQ'd

---

## Exit Criteria

- a real OpenSky event flows end-to-end from the poller through to `position_history` and `entity:live:*`
- duplicate inserts produce no duplicate rows
- stale events do not overwrite newer Redis state
- `geo-cell:*` sorted sets reflect the entity's current live H3 membership and do not accumulate stale membership after cell changes
- malformed events land in `adsb.dlq` with a rejection reason
- `position.normalized` events appear in Kafka and contain both H3 cell IDs
- developer can verify all of the above using CLI tools without reading the code
