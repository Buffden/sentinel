# Phase 02: Live Position Pipeline

Build Sentinel's first real streaming data plane: telemetry flows from an external source through Redpanda and into durable state stores, with a normalized canonical position event published downstream.

---

## Goal

```text
External / OpenSky telemetry
        ↓
Ingestion Poller
        ↓
adsb.raw / ais.raw
        ↓
Position Consumer
        ↓
TimescaleDB position_history
        ↓
Redis live state / spatial index
        ↓
position.normalized
```

---

## Services introduced

| Service | Directory | Role |
| --- | --- | --- |
| Ingestion Poller | `services/ingestion-poller/` | Fetch raw telemetry, publish to `adsb.raw` |
| Position Consumer | `services/position-consumer/` | Normalize, persist, maintain live state, publish `position.normalized` |

---

## Checkpoint progress

| CP | Scope | Status |
| --- | --- | --- |
| CP1 | Kafka experiment: produce and consume one event; observe consumer group and offset behavior | Done |
| CP2 | OpenSky ingestion poller: real ADS-B telemetry → `adsb.raw` | Done |
| CP3 | Parse + normalize → log canonical position object; schema contract docs | Done — verified against real adsb.raw records |
| CP4 | Validation + DLQ routing: malformed records → `adsb.dlq` with rejection reason | Done — all four rejection paths verified |
| CP5 | TimescaleDB: idempotent `position_history` write; `raw_events` write; offset commit after both | Pending |
| CP6 | Redis `entity:live:{entity_id}`: monotonic timestamp guard; stale-event protection | Pending |
| CP7 | H3 geo-cell: `geo-cell:{cell}` sorted-set membership; cell-change ZREM/ZADD | Pending |
| CP8 | `position.normalized` Kafka publish; Redis `position-updates` pub/sub; replay and failure lab | Pending |

---

## Important learning

- Producer/consumer model and consumer group protocol
- Consumer group offset management: record offset vs committed offset vs current position
- Event serialization and at-least-once delivery
- Idempotency: why durable side effects must tolerate redelivery
- Kafka replay: crash recovery vs intentional historical backfill
- Event time vs processing time
- Stale and out-of-order events: monotonic Redis guard
- Ephemeral live state vs durable history
- H3 spatial indexing: history column vs live sorted-set candidate index
- Maintaining a live spatial candidate index as entities move between H3 cells

---

## Bootstrap sequence

```bash
make up          # start all four containers; wait for healthy
make topics      # provision 8 canonical Kafka topics (idempotent)
```

Kafka experiment:

```bash
# Terminal A — start consumer first
cd services/position-consumer
node_modules/.bin/tsx src/consume.ts

# Terminal B — run producer
cd services/ingestion-poller
node_modules/.bin/tsx src/produce.ts
```

---

## Required failure experiments

| Experiment | Expected result |
| --- | --- |
| Duplicate Kafka delivery | No duplicate `position_history` row |
| Process crash after TimescaleDB write but before offset commit | Replay on restart; idempotent insert |
| Telemetry with older timestamp arriving after a newer one | Redis live state not regressed |
| Replay/out-of-order telemetry | Stale event does not move entity to older `geo-cell:*` membership |
| Entity crosses H3 cell boundary | Old-cell membership removed; exactly one current-cell entry remains |
| Malformed input | Event lands in `adsb.dlq` with rejection reason; consumer does not crash |
| Valid event with null/missing position | Skipped with a warn log; not DLQ'd |

---

## Exit criteria

- A real OpenSky event flows end-to-end from the poller through to `position_history` and `entity:live:*`
- Duplicate inserts produce no duplicate rows
- Stale events do not overwrite newer Redis state
- `geo-cell:*` sorted sets reflect the entity's current live H3 membership without stale entries after cell changes
- Malformed events land in `adsb.dlq` with a rejection reason
- `position.normalized` events appear in Kafka and contain both H3 cell IDs
- Developer can verify all of the above using CLI tools without reading the code

---

## Contents

| Path | Description |
| --- | --- |
| [`concepts/`](concepts/README.md) | Concept notes and checkpoint debriefs, in reading order |
| [`exit-verification.md`](exit-verification.md) | Final store inspection and exit criteria evaluation (filled in at phase completion) |
