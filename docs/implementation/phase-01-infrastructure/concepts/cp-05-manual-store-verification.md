# Checkpoint 5 Debrief -- Manual Store Verification

All four infrastructure stores verified by direct CLI interaction before any application code is written.

---

## Infrastructure status

| Container | Status |
| --- | --- |
| sentinel-redpanda | healthy |
| sentinel-timescaledb | healthy |
| sentinel-redis | healthy |
| sentinel-neo4j | healthy |

---

## 1. Kafka / Redpanda

### What I did

Listed all 8 canonical topics; inspected `adsb.raw` partition metadata; produced one synthetic record; consumed it by explicit offset; consumed with a named group and inspected committed state.

### Commands

`scripts/cp-05/kafka.sh`

### What I observed

`adsb.raw` partition metadata:

| PARTITION | LEADER | EPOCH | REPLICAS | LOG-START-OFFSET | HIGH-WATERMARK |
| --- | --- | --- | --- | --- | --- |
| 0 | 0 | 3 | [0] | 0 | 2 |

`EPOCH 3` is the current leadership epoch. It shows that the partition leadership metadata has advanced, commonly due to broker restarts/elections, but it should not be interpreted as exactly three leadership changes. Data survived because of the named Docker volume.

Producing the CP5 record:

```text
Produced to partition 0 at offset 1 with timestamp ...
```

Consuming at explicit offset 1:

```json
{"topic":"adsb.raw","value":"{\"checkpoint\":5,\"source\":\"manual-store-verification\"}","partition":0,"offset":1}
```

Consumer group `cp5-manual-test` result after consuming from `--offset start` (read offset 0):

| Field | Value | Meaning |
| --- | --- | --- |
| CURRENT-OFFSET | 1 | group committed through offset 0; next read starts at 1 |
| LOG-END-OFFSET | 2 | two records in the partition |
| LAG | 1 | one record (offset 1) not yet consumed by this group |

`position.normalized` and `alerts` both at HIGH-WATERMARK 0 -- no application services have written to them yet.

### Concept demonstrated

A record's offset is fixed and immutable. A consumer group's committed offset is separate: it records where that named group last confirmed progress. Consuming offset 0 without a group does not affect what another group sees. LAG = LOG-END-OFFSET - CURRENT-OFFSET.

### Failure observed

```bash
rpk topic consume definitely-does-not-exist
# UNKNOWN_TOPIC_OR_PARTITION -- exit code 1
```

### Cleanup

Consumer group `cp5-manual-test` left in place (Redpanda does not support dropping groups cleanly via `rpk` in this version). It holds no application state. The disposable CP5 record at offset 1 of `adsb.raw` remains -- records are immutable; deletion is by retention policy, not explicit delete.

---

## 2. TimescaleDB

### What I did

Verified all 6 expected tables exist; inspected hypertable metadata, chunk interval, and retention policy; inserted one synthetic row; re-inserted the same row to test idempotency; inspected the chunk; deleted the row.

### Commands

`scripts/cp-05/timescaledb.sql`

### What I observed

Tables present: `position_history`, `users`, `user_workspaces`, `route_references`, `route_reference_points`, `alerts`.

Hypertable metadata before insert:

| hypertable_name | num_chunks | time_interval | retention_policy |
| --- | --- | --- | --- |
| position_history | 0 | 1 day | policy_retention (1 day schedule) |

`time_interval` comes from `timescaledb_information.dimensions`; the `hypertables` view does not expose it directly.

After inserting `cp5-test-entity` at `timestamp_ms = 1700000000000`:

| num_chunks | chunk_name | range_start | range_end |
| --- | --- | --- | --- |
| 1 | _hyper_1_1_chunk | 2023-11-14 00:00:00 | 2023-11-15 00:00:00 |

Idempotency test:

| Attempt | Result | Meaning |
| --- | --- | --- |
| First insert | INSERT 0 1 | row written |
| Second insert | INSERT 0 0 | silently discarded by ON CONFLICT DO NOTHING |
| Row count | 1 | no duplicate |

After `DELETE FROM position_history WHERE entity_id = 'cp5-test-entity'`: `num_chunks` remains 1 -- chunk persists after row deletion.

### Concept demonstrated

TimescaleDB creates chunks lazily on first insert into a time range. Zero rows and zero chunks are different facts: zero rows means the hypertable currently contains no rows; zero chunks means no current TimescaleDB chunk exists. A table can have zero rows while still having chunks because deleting rows does not remove their chunks.

`ON CONFLICT (entity_id, observed_at) DO NOTHING` is the idempotency mechanism for at-least-once Kafka redelivery: the same event reprocessed produces no duplicate row.

### Failure observed

```sql
SELECT 1/0;
-- ERROR: division by zero -- exit code 1
```

### Cleanup

Row deleted. Chunk persists (expected -- chunks are structural, not per-row).

---

## 3. Redis

### What I did

Created a canonical-shape `entity:live:*` hash; inspected TTL behavior; created a `geo-cell:*` sorted set with two members and demonstrated a freshness (ZRANGEBYSCORE) query.

### Commands

`scripts/cp-05/redis.sh`

### What I observed

Hash contents after `HSET entity:live:cp5-test-entity ...`:

| Field | Value |
| --- | --- |
| lat | 40.7128 |
| lon | -74.0060 |
| entity_type | aircraft |
| last_seen_ms | 1700000000000 |
| live_geo_cell | 8928308280fffff |

TTL behavior:

| Command | Result | Meaning |
| --- | --- | --- |
| TTL entity:live:cp5-test-entity | -1 | key exists, no expiry set |
| PEXPIRE ... 30000 | 1 | TTL set to 30 000ms |
| PTTL entity:live:cp5-test-entity | ~29941 | milliseconds remaining |

`-1` means the key exists but has no expiry set. `-2` would mean the key does not exist.

Sorted set freshness query -- members with scores above 1700000003000:

| Member | Score | Result |
| --- | --- | --- |
| cp5-test-entity-b | 1700000005000 | included: fresh |
| cp5-test-entity-a | 1700000000000 | excluded: stale |

### Concept demonstrated

The `geo-cell:*` sorted set uses `last_seen_ms` as the score so the Correlation Worker can filter candidates by freshness in a single range query rather than needing a per-member TTL. Stale members accumulate but are logically invisible when queried with a freshness lower bound.

Redis hashes store all live entity state in one key, making a single `HGETALL` or targeted `HGET` sufficient for any reader.

### Failure observed

```text
ZADD geo-cell:cp5-test-cell not-a-number cp5-test-entity-c
ERR value is not a valid float
```

`redis-cli` returns exit code 0 even on errors -- error visibility is at the application layer (client library raises an exception). The error text itself is clear.

### Cleanup

`DEL entity:live:cp5-test-entity geo-cell:cp5-test-cell` -- both keys gone (the hash had already expired from the 30s TTL before DEL ran). `EXISTS` returned 0 for both.

---

## 4. Neo4j

### What I did

Verified CP4 constraints; created two Entity nodes with a KNOWN_ASSOCIATE relationship; created a PROXIMITY_EVENT relationship; attempted a duplicate PROXIMITY_EVENT via CREATE (rejected); demonstrated MERGE as the replay-safe alternative; cleaned up.

### Commands

`scripts/cp-05/neo4j.cypher`

### What I observed

Constraints (from `SHOW CONSTRAINTS`):

| Name | Type | Target | Label | Properties |
| --- | --- | --- | --- | --- |
| entity_id_unique | UNIQUENESS | NODE | Entity | [id] |
| proximity_event_idempotency_key_unique | RELATIONSHIP_UNIQUENESS | RELATIONSHIP | PROXIMITY_EVENT | [idempotency_key] |

KNOWN_ASSOCIATE query result:

| entity_a | entity_b | rel_type | established_at |
| --- | --- | --- | --- |
| cp5-aircraft | cp5-vessel | same-fleet | 1700000000000 |

PROXIMITY_EVENT query result:

| entity_a | entity_b | key | start_ms | min_dist_m | lat | lon |
| --- | --- | --- | --- | --- | --- | --- |
| cp5-aircraft | cp5-vessel | cp5-test-pair:1234567890 | 1234567890 | 85.5 | 51.5074 | -0.1278 |

Duplicate CREATE attempt:

```text
Relationship(N) already exists with type `PROXIMITY_EVENT` and property `idempotency_key` = 'cp5-test-pair:1234567890'
exit code: 1
```

`N` is the internal Neo4j relationship ID, which varies per run.

MERGE on same idempotency_key: found the existing edge, updated `last_seen_ms` to `1234568500`. Exit code 0. No duplicate created.

### Concept demonstrated

`CREATE` and `MERGE` behave differently under the uniqueness constraint:

- `CREATE` always attempts to create a new relationship. The constraint rejects it if the idempotency_key already exists.
- `MERGE` finds the existing relationship if present and may update properties with `ON MATCH SET`. This is the correct pattern for at-least-once Kafka redelivery: reprocessing the same event is safe because MERGE finds the existing edge.

The constraint catches concurrent race conditions at the storage level. MERGE handles the sequential replay case at the application level. Both are needed.

### Failure observed

```cypher
MATCH (n:Entity) WHERE n.nonexistent_function() RETURN n
-- Unknown function 'n.nonexistent_function'
-- exit code: 1
```

### Cleanup

`MATCH (n:Entity) WHERE n.id STARTS WITH 'cp5-' DETACH DELETE n` -- removed both nodes and both relationships (KNOWN_ASSOCIATE + PROXIMITY_EVENT). Zero cp5- entities remain.

---

## Deferred

- Redis `alert-state`, `recent-loss`, `deviation-state`, `proximity-episode` key patterns: first written by application services (Phases 03-05)
- Redis pub/sub (`position-updates`, `alert-events`): first exercised in Phase 03
- TimescaleDB alert rows, user/workspace rows: first written in Phase 03/07
- Neo4j application MERGE write sequence with Kafka integration: Phase 05
- H3 cell coordinate conversion and k-ring computation: Phase 05
- WebSocket behavior: Phase 03
