# position.normalized Publish

---

## What it is

`position.normalized` is the canonical event that crosses the ingestion boundary. The Position Consumer owns everything upstream: raw parsing, validation, normalization, TimescaleDB persistence, and Redis live state. Everything downstream (Deviation Detector, Correlation Worker) consumes `position.normalized` and nothing else from the ingestion side. No downstream service reads `adsb.raw`.

Publishing to `position.normalized` is the last write-side responsibility of the Position Consumer for a valid event.

`position.normalized` is **at-least-once**. A crash between a successful publish and the source offset commit causes the source record to be redelivered, and the publish to be repeated. Downstream consumers must tolerate duplicate events and process them idempotently. Do not assume exactly-once delivery.

---

## At-least-once delivery and the crash-before-commit scenario

The publish happens before the source `adsb.raw` offset is committed. This sequence is intentional — if the publish fails, the offset is not committed and Kafka redelivers. But it creates a duplicate path:

```
publishNormalized succeeds
  → process crashes before source offset commit
  → Kafka redelivers adsb.raw message on restart
  → publishNormalized runs again
  → position.normalized receives a duplicate event
```

This is the standard at-least-once pattern for Kafka-to-Kafka pipelines. Downstream consumers must handle it:

- **Idempotent state writes**: use source `(entity_id, timestamp_ms)` as the effective identity. A duplicate event with the same pair produces the same output.
- **Do not use `position.normalized` offset as a uniqueness key**: the same logical event may appear at different `position.normalized` offsets across replays.

## Why it blocks offset commit

A failed `position.normalized` publish propagates out of the message handler. The offset is not committed. Kafka redelivers the message on the next consumer start. All prior writes replay idempotently:

- `raw_events`: `ON CONFLICT (source_topic, source_partition, source_offset) DO NOTHING`
- `position_history`: `ON CONFLICT (entity_id, observed_at) DO NOTHING`
- Redis Lua guard: equal or older timestamp rejected, hash unchanged
- Redis geo-cell sorted sets: `ZADD` upserts the score; `ZREM` on a non-existent member is a no-op

Not blocking would silently drop the event for all downstream detectors with no recovery path short of a full replay from `adsb.raw` with a separate consumer group.

---

## Published for all valid positions

`publishNormalized` is called for every valid normalized position, regardless of whether the Redis live state was accepted. A stale event (older timestamp already in Redis) still carries a valid position coordinate and source event time that the Deviation Detector needs to evaluate against its reference route geometry.

---

## Kafka key: entity_id

The message is keyed by `entity_id`. Kafka uses the key to select a partition via a hash. All position events for the same entity land on the same partition, in the order they were published. This gives downstream consumers a stable per-entity ordering guarantee without a global ordering requirement across all entities.

If `entity_id` were omitted, events for the same entity could land on different partitions and arrive out of order at the consumer. The Deviation Detector's route evaluation and the Correlation Worker's episode tracking both assume per-entity ordering.

---

## Payload schema

All fields from `NormalizedPosition` plus both H3 cells. Null values are preserved as JSON `null` — do not substitute empty strings. Downstream consumers expect typed nulls, not empty strings, for optional fields.

| Added field | Value |
| --- | --- |
| `history_geo_cell` | H3 cell at `HISTORY_H3_RESOLUTION` |
| `live_geo_cell` | H3 cell at `LIVE_H3_RESOLUTION` |

The full canonical schema is defined in `docs/DATA_MODEL.md` under `position.normalized`.

---

## Relationship to position_history

`position.normalized` and `position_history` carry the same position data. They are not alternatives — both exist for different consumers:

- `position_history` is the durable queryable record, partitioned by time, indexed by geo_cell.
- `position.normalized` is the real-time event stream for stateless/stateful detectors that process each ping as it arrives.

A detector that needs historical context reads `position_history` directly. One that evaluates each event independently (Deviation Detector) or incrementally (Correlation Worker) reads `position.normalized`.
