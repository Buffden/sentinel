# Phase 01 Exit Verification

Final inspection of all infrastructure stores before Phase 02 begins. Records the state of every store, reproducibility check results, consistency review, and formal exit criteria evaluation.

---

## 1. Container health

| Container | Image | Status |
| --- | --- | --- |
| sentinel-redpanda | redpanda:v24.1.2 | Up (healthy) |
| sentinel-timescaledb | timescale/timescaledb:2.15.3-pg16 | Up (healthy) |
| sentinel-redis | redis:7.2.4-alpine | Up (healthy) |
| sentinel-neo4j | neo4j:5.19.0-community | Up (healthy) |

Docker Compose healthchecks:

| Service | Test | Interval | Retries | Start period |
| --- | --- | --- | --- | --- |
| Redpanda | `rpk topic list` | 15s | 10 | 30s |
| TimescaleDB | `pg_isready` | 10s | 10 | 20s |
| Redis | `redis-cli ping` | 10s | 10 | 10s |
| Neo4j | `wget --spider :7474` | 15s | 10 | 60s |

`make logs SERVICE=<name>` streams stdout for any container.

---

## 2. Kafka / Redpanda

Cluster: `redpanda.<uuid>`, 1 broker, node 0, `localhost:9092`.

Topics:

| Topic | Partitions | Replicas |
| --- | --- | --- |
| adsb.raw | 1 | 1 |
| adsb.dlq | 1 | 1 |
| ais.raw | 1 | 1 |
| ais.dlq | 1 | 1 |
| alerts | 1 | 1 |
| deviation.candidates | 1 | 1 |
| position.normalized | 1 | 1 |
| proximity.candidates | 1 | 1 |

Consumer groups:

| Broker | Group | Members |
| --- | --- | --- |
| 0 | manual-store-verification-test | 0 (no active members) |

No application consumer groups exist yet. `__consumer_offsets` (3 partitions) is the internal Redpanda topic for persisting committed offsets; it is created automatically and is not in the provisioned list.

---

## 3. TimescaleDB

### Tables

| Table | Notes |
| --- | --- |
| `position_history` | Hypertable, partitioned by `observed_at` |
| `users` | |
| `user_workspaces` | |
| `route_references` | |
| `route_reference_points` | |
| `alerts` | |

### Hypertable and chunks

| hypertable_name | num_dimensions | num_chunks |
| --- | --- | --- |
| position_history | 1 | 1 |

| chunk_name | range_start | range_end |
| --- | --- | --- |
| _hyper_1_1_chunk | 2023-11-14 00:00:00+00 | 2023-11-15 00:00:00+00 |

One chunk exists from the manual store verification insert. Chunks are created lazily on first write into a time range. The chunk boundaries confirm TimescaleDB is partitioning `position_history` by `observed_at` as defined in migration 002.

### Retention policy

| job_id | proc_name | schedule_interval | config |
| --- | --- | --- | --- |
| 1000 | policy_retention | 1 day | drop_after: 30 days |

### Indexes

| Index | Type | Columns |
| --- | --- | --- |
| position_history_entity_observed_uidx | UNIQUE | (entity_id, observed_at) |
| position_history_entity_time_idx | btree | (entity_id, observed_at DESC) |
| position_history_geocell_time_idx | btree | (geo_cell, observed_at DESC) |
| position_history_observed_at_idx | btree | (observed_at DESC) |
| alerts_entity_time_idx | btree | (entity_id, detected_at DESC) |
| alerts_counterparty_time_idx | btree | (counterparty_entity_id, detected_at DESC) |
| alerts_status_time_idx | btree | (status, detected_at DESC) |
| alerts_type_time_idx | btree | (alert_type, detected_at DESC) |
| route_references_entity_idx | btree | (entity_id) |
| users_google_sub_key | UNIQUE | (google_sub) |

Plus primary keys on all tables. All indexes from migrations are present.

---

## 4. Redis

| Check | Result |
| --- | --- |
| PING | PONG |
| redis_version | 7.2.4 |
| tcp_port | 6379 |
| SCAN 0 COUNT 100 | empty cursor, no keys |

Redis is empty. The test keys written during manual store verification have expired or were deleted. Redis holds live ephemeral state; absence of keys before any service has run is expected.

---

## 5. Neo4j

### Neo4j constraints

| Name | Type | Target | Label | Properties |
| --- | --- | --- | --- | --- |
| entity_id_unique | UNIQUENESS | NODE | Entity | [id] |
| proximity_event_idempotency_key_unique | RELATIONSHIP_UNIQUENESS | RELATIONSHIP | PROXIMITY_EVENT | [idempotency_key] |

### Neo4j indexes

| Name | State | Type | Target | Properties |
| --- | --- | --- | --- | --- |
| entity_id_unique | ONLINE | RANGE | NODE | [id] |
| proximity_event_idempotency_key_unique | ONLINE | RANGE | RELATIONSHIP | [idempotency_key] |
| index_343aff4e | ONLINE | LOOKUP | NODE | system |
| index_f7700477 | ONLINE | LOOKUP | RELATIONSHIP | system |

Two RANGE indexes back the uniqueness constraints (created automatically). Two LOOKUP indexes are system-managed and always present. No redundant manual indexes exist.

### Graph state

| Query | Result |
| --- | --- |
| `MATCH (n) RETURN count(n)` | 0 nodes |
| `MATCH ()-[r]->() RETURN count(r)` | 0 relationships |

The graph is empty. Entity nodes and the PROXIMITY_EVENT relationship written during manual store verification were deleted in that session's cleanup step. Schema constraints persist independently of graph data.

---

## 6. Reproducibility check

| Command | Outcome |
| --- | --- |
| `make migrate` | All 6 migrations applied. `IF NOT EXISTS` guards skipped existing objects. NOTICE lines confirmed. Exit 0. |
| `make topics` | All 8 topics reported `exists`. No topic re-created. Exit 0. |
| `make neo4j-schema` | Both `CREATE CONSTRAINT IF NOT EXISTS` statements found existing constraints and skipped. Exit 0. |

All three provisioning operations are fully idempotent. Running them multiple times against an existing stack is safe.

---

## 7. Makefile and CLI usability

`make help` lists all documented targets with descriptions. One bug found and fixed: the grep pattern `^[a-zA-Z_-]+:` excluded digits, so `neo4j-schema` (containing `4`) did not appear. Pattern corrected to `^[a-zA-Z0-9_-]+:`.

```text
up           Start all infrastructure services in detached mode
down         Stop and remove containers (named volumes are preserved)
reset        WARNING: destroy containers AND all named volumes -- all local data is lost
ps           Show container status and health
logs         Tail logs; filter by service with SERVICE=<name>
migrate      Apply database migrations in order (run after make up)
topics       Provision canonical Kafka topics (run after make up)
neo4j-schema Apply canonical Neo4j constraints and indexes (run after make up)
help         Show this help
```

Also corrected: `infra/scripts/migrate.sh` echo lines that used padded column alignment were simplified to unpadded form.

---

## 8. Consistency review

| Checkpoint | Artifact | Status |
| --- | --- | --- |
| Docker Compose | `docker-compose.yml`, 4 services, named volumes, healthchecks | Consistent. All 4 containers healthy. |
| TimescaleDB migrations | `infra/migrations/001-006`, `infra/scripts/migrate.sh` | Consistent. 6 tables, hypertable, chunk, retention policy, all indexes present. |
| Kafka topics | `infra/kafka/topics.sh`, 8 canonical topics | Consistent. All 8 topics provisioned with 1 partition, 1 replica. |
| Neo4j schema | `infra/neo4j/schema.cypher`, `infra/neo4j/apply-schema.sh` | Consistent. 2 constraints, 4 indexes, all ONLINE. |
| Manual store verification | [`concepts/manual-store-verification.md`](concepts/manual-store-verification.md) | Consistent. Chunk in TimescaleDB confirms hands-on insert occurred. Redis empty post-TTL. Neo4j graph empty after cleanup. |
| Logging convention | [`concepts/structured-logging-convention.md`](concepts/structured-logging-convention.md) | Consistent. No application services exist yet; convention documented for Phase 02+. |
| Health/readiness convention | [`concepts/health-readiness-convention.md`](concepts/health-readiness-convention.md) | Consistent. No application services exist yet; convention documented for Phase 02+. |

No inconsistencies found between infrastructure state and documented artifacts.

---

## 9. Exit criteria

| Criterion | Result |
| --- | --- |
| All infrastructure containers start cleanly | PASS: all 4 healthy |
| TimescaleDB migrations are idempotent and complete | PASS: 6 tables, hypertable, retention policy, all indexes |
| Kafka topic provisioning is idempotent | PASS: 8 topics, `exists` on re-run |
| Neo4j constraints and indexes are applied and idempotent | PASS: 2 constraints, 4 indexes, ONLINE |
| Every store is directly inspectable from CLI | PASS: rpk, psql, redis-cli, cypher-shell all confirmed |
| Structured logging convention documented | PASS: structured-logging-convention.md |
| Health/readiness convention documented | PASS: health-readiness-convention.md |
| Makefile covers all provisioning operations | PASS: up, migrate, topics, neo4j-schema, help (bug fixed) |
| Manual datastore hands-on requirement complete | PASS: manual-store-verification.md |
| No substantial application code prematurely introduced | PASS: Phase 01 contains only infrastructure, schemas, and convention documents |

**Phase 01 exit: PASS.**

All infrastructure is reproducible, inspectable, and consistent. Logging and health conventions are documented for every service introduced in Phase 02+.
