# Phase 02 Exit Verification

Final inspection of the live position pipeline before Phase 03 begins. Records the state of every service, Kafka topic, database table, and Redis key introduced or written by Phase 02, reproducibility check results, consistency review, and formal exit criteria evaluation.

Filled in when all Phase 02 checkpoints are complete.

---

## 1. Container health

| Container | Image | Status |
| --- | --- | --- |
| sentinel-redpanda | | |
| sentinel-timescaledb | | |
| sentinel-redis | | |
| sentinel-neo4j | | |

---

## 2. Services

| Service | Introduced in | Status |
| --- | --- | --- |
| ingestion-poller | Kafka experiment | |
| position-consumer | Kafka experiment | |

---

## 3. Kafka topics written by Phase 02

| Topic | Producer | Records observed |
| --- | --- | --- |
| `adsb.raw` | Ingestion Poller | |
| `position.normalized` | Position Consumer | |
| `adsb.dlq` | Position Consumer | |

---

## 4. TimescaleDB: position_history

| Check | Result |
| --- | --- |
| Row count after real OpenSky event | |
| Duplicate insert result (`ON CONFLICT DO NOTHING`) | |
| Chunk range covering observed events | |
| `EXPLAIN ANALYZE` on `(entity_id, observed_at)` query | |

---

## 5. Redis live state

| Key pattern | Check | Result |
| --- | --- | --- |
| `entity:live:{entity_id}` | Fields present after first ping | |
| `entity:live:{entity_id}` | Stale event ignored (older timestamp) | |
| `geo-cell:{h3_cell}` | Entity appears in correct cell after first ping | |
| `geo-cell:{old_cell}` | Entity removed from old cell after H3 boundary crossing | |

---

## 6. Consumer group state

| Group | Topic | CURRENT-OFFSET | LOG-END-OFFSET | LAG |
| --- | --- | --- | --- | --- |
| position-consumer | adsb.raw | | | |

---

## 7. Reproducibility check

| Command | Outcome |
| --- | --- |
| `make up` | |
| `make topics` | |
| Restart position-consumer after clean shutdown | Resumes from committed offset, no reprocessing |
| Restart position-consumer after simulated crash (uncommitted offset) | Replays from last committed offset, idempotent result |

---

## 8. Failure experiments completed

| Experiment | Expected result | Observed result |
| --- | --- | --- |
| Duplicate Kafka delivery | No duplicate `position_history` row | |
| Crash after TimescaleDB write, before offset commit | Replay on restart; idempotent insert | |
| Stale telemetry (older timestamp) | Redis live state not regressed | |
| Entity crosses H3 cell boundary | Old cell membership removed; one current-cell entry | |
| Malformed input | Event in `adsb.dlq`; consumer does not crash | |
| Valid event with null position | Skipped with log warn; not DLQ'd | |

---

## 9. Exit criteria

| Criterion | Result |
| --- | --- |
| Real OpenSky event flows end-to-end to `position_history` and `entity:live:*` | |
| Duplicate inserts produce no duplicate rows | |
| Stale events do not overwrite newer Redis state | |
| `geo-cell:*` sorted sets reflect current live H3 membership without stale entries | |
| Malformed events land in `adsb.dlq` with a rejection reason | |
| `position.normalized` events appear in Kafka with both H3 cell IDs | |
| Developer can verify all of the above using CLI tools without reading the code | |

**Phase 02 exit:**
