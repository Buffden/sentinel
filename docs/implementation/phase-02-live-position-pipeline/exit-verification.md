# Phase 02 Exit Verification

Final inspection of the live position pipeline before Phase 03 begins. Records the state of every service, Kafka topic, database table, and Redis key introduced or written by Phase 02, reproducibility check results, consistency review, and formal exit criteria evaluation.

Verified: 2026-08-25

---

## 1. Container health

| Container | Image | Status |
| --- | --- | --- |
| sentinel-redpanda | redpandadata/redpanda:v24.1.2 | Up 12h (healthy) |
| sentinel-timescaledb | timescale/timescaledb:2.15.3-pg16 | Up 12h (healthy) |
| sentinel-redis | redis:7.2.4-alpine | Up 12h (healthy) |
| sentinel-neo4j | neo4j:5.19.0-community | Up 12h (healthy) |

---

## 2. Services

| Service | Introduced in | Status |
| --- | --- | --- |
| ingestion-poller | OpenSky ingestion | Verified — polls OpenSky, publishes to `adsb.raw` |
| position-consumer | Kafka experiment | Verified — full pipeline end-to-end |

---

## 3. Kafka topics written by Phase 02

| Topic | Producer | Records observed |
| --- | --- | --- |
| `adsb.raw` | Ingestion Poller | 13 records (offsets 0–12); LOG-END-OFFSET = 13 |
| `position.normalized` | Position Consumer | Records confirmed; both H3 cells present in payload |
| `adsb.dlq` | Position Consumer | Records confirmed; `rejection_reason` present on each |

---

## 4. TimescaleDB: position_history

| Check | Result |
| --- | --- |
| Row count | 6 total rows; 3 with `geo_cell` populated (written after H3 checkpoint), 3 NULL (written before, not backfilled) |
| Duplicate insert result (`ON CONFLICT DO NOTHING`) | `INSERT 0 0` — conflict suppressed; row count unchanged |
| Distinct identity pairs | 6 distinct `(entity_id, observed_at)` pairs; equals total row count — no duplicates |
| Chunk ranges | Two chunks: `2023-11-14 – 2023-11-15` (test data), `2026-08-25 – 2026-08-26` (live session) |
| `EXPLAIN ANALYZE` on `(entity_id, observed_at)` query | `ChunkAppend` with `Index Scan` on `position_history_entity_time_idx`; chunk exclusion active |

---

## 5. Redis live state

| Key pattern | Check | Result |
| --- | --- | --- |
| `entity:live:def456` | All fields present after first ping | `HGETALL` returns 16 fields including `live_geo_cell` |
| `entity:live:def456` | TTL set to 24h on accepted write | `TTL` returned ~85834 immediately after write |
| `entity:live:def456` | Stale event ignored (full replay with older timestamps) | `last_seen_ms` unchanged at `1787634583000` after replay; `PASS` |
| `geo-cell:87194ad33ffffff` | Entity appears in correct cell after accepted ping | `ZRANGEBYSCORE -inf +inf WITHSCORES` returns `def456 1787634583000` |
| `geo-cell:87194ad33ffffff` | Removed after H3 boundary crossing (lat 51.5 → 51.54) | `ZRANGEBYSCORE -inf +inf` returns empty |
| `geo-cell:87194ad36ffffff` | Entity present in new cell after crossing | `ZRANGEBYSCORE -inf +inf WITHSCORES` returns `def456` with updated score |

---

## 6. Consumer group state

| Group | Topic | CURRENT-OFFSET | LOG-END-OFFSET | LAG |
| --- | --- | --- | --- | --- |
| position-consumer | adsb.raw | 13 | 13 | 0 |

---

## 7. Reproducibility check

| Command | Outcome |
| --- | --- |
| `make up` | All four containers reach healthy state |
| `make topics` | All eight canonical topics provisioned; existing topics skipped |
| Restart position-consumer after clean shutdown | Resumed from committed offset 13; no reprocessing |
| Full replay (`rpk group seek --to start`) | All 13 messages reprocessed; idempotent inserts on `raw_events` and `position_history`; Redis monotonic guard rejected all stale timestamps |

---

## 8. Failure experiments completed

| Experiment | Expected result | Observed result |
| --- | --- | --- |
| Duplicate Kafka delivery (full replay) | No duplicate `position_history` row | 6 total rows before and after replay; `INSERT 0 0` on direct duplicate attempt |
| Stale telemetry (older timestamp replayed) | Redis live state not regressed | `last_seen_ms` held at `1787634583000` across all 13 replayed offsets; `live_state_accepted: false` on every prior event |
| Entity crosses H3 cell boundary | Old cell membership removed; one current-cell entry | `geo-cell:87194ad33ffffff` empty; `geo-cell:87194ad36ffffff` contains entity |
| Malformed input (`parse_error`) | Event in `adsb.dlq`; consumer does not crash | DLQ record confirmed with `rejection_reason`; consumer continued processing |
| Missing entity ID | Event in `adsb.dlq`; consumer does not crash | DLQ record confirmed; consumer continued |
| Valid event with null position (`no_position`) | Skipped with log warn; not DLQ'd | `skipping record with no position` log at warn; no DLQ entry for that offset |

---

## 9. Exit criteria

| Criterion | Result |
| --- | --- |
| Valid event flows end-to-end to `position_history` and `entity:live:*` | PASS |
| `position_history.geo_cell` populated with H3 res=5 cell | PASS |
| `entity:live:*` hash contains `live_geo_cell` field | PASS |
| `geo-cell:*` sorted sets reflect current live H3 membership without stale entries | PASS — boundary crossing verified |
| Duplicate inserts produce no duplicate rows | PASS — `INSERT 0 0` on conflict; row count stable across replay |
| Stale events do not overwrite newer Redis state | PASS — monotonic guard held across full replay |
| Malformed events land in `adsb.dlq` with a rejection reason | PASS |
| `position.normalized` events appear in Kafka with both H3 cell IDs | PASS — `history_geo_cell` and `live_geo_cell` confirmed in payload |
| Developer can verify all of the above using CLI tools without reading the code | PASS — all checks above use `rpk`, `redis-cli`, and `psql` only |

**Phase 02 exit: COMPLETE — 2026-08-25**
