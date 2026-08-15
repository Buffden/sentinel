# Phase 01 Exit Verification

Checkpoint 8. Final inspection of all infrastructure stores before Phase 02 begins.

This document records the state of every store, the reproducibility check results, a CP1-CP7 consistency review, and the formal Phase 01 exit criteria evaluation.

---

## 1. Container health

All four containers running and healthy at verification time:

```text
NAME                   IMAGE                                  STATUS
sentinel-redpanda      redpanda:v24.1.2                       Up (healthy)
sentinel-timescaledb   timescale/timescaledb:2.15.3-pg16      Up (healthy)
sentinel-redis         redis:7.2.4-alpine                     Up (healthy)
sentinel-neo4j         neo4j:5.19.0-community                 Up (healthy)
```

Docker Compose healthchecks:

| Service | Test | Interval | Retries | Start period |
| --- | --- | --- | --- | --- |
| Redpanda | `rpk topic list` | 15s | 10 | 30s |
| TimescaleDB | `pg_isready` | 10s | 10 | 20s |
| Redis | `redis-cli ping` | 10s | 10 | 10s |
| Neo4j | `wget --spider :7474` | 15s | 10 | 60s |

Log inspection command: `make logs SERVICE=<name>` streams stdout for any container. Structured log output from application services (Phase 02+) will appear there, following the CP6 convention.

---

## 2. Kafka / Redpanda

```text
rpk cluster info

CLUSTER: redpanda.<uuid>
BROKERS: 1 broker, node 0, localhost:9092

rpk topic list

NAME                  PARTITIONS  REPLICAS
adsb.dlq              1           1
adsb.raw              1           1
ais.dlq               1           1
ais.raw               1           1
alerts                1           1
deviation.candidates  1           1
position.normalized   1           1
proximity.candidates  1           1

rpk group list

BROKER  GROUP
0       cp5-manual-test
```

All 8 canonical topics are present. `cp5-manual-test` is the consumer group created during CP5 manual verification; it holds no active members. No application consumer groups exist yet.

`__consumer_offsets` (3 partitions) is the internal Kafka topic Redpanda uses to persist committed offsets. It is created automatically and is not in the provisioned list.

---

## 3. TimescaleDB

### Tables

```text
public schema:
  position_history         (hypertable)
  users
  user_workspaces
  route_references
  route_reference_points
  alerts
```

The migration set 001-006 installs the TimescaleDB extension and creates the six expected application tables.

### Hypertable and chunks

```text
hypertable_name   num_dimensions   num_chunks
position_history  1                1

chunk_name          range_start              range_end
_hyper_1_1_chunk    2023-11-14 00:00:00+00   2023-11-15 00:00:00+00
```

One chunk exists from the CP5 manual insert. Chunks are created lazily on first write into a time range. The chunk boundaries confirm TimescaleDB is partitioning `position_history` by `observed_at` as defined in migration 002.

### Retention policy

```text
job_id  proc_name         schedule_interval  config
1000    policy_retention  1 day              {"drop_after": "30 days", "hypertable_id": 1}
```

The 30-day retention policy on `position_history` is registered and scheduled daily.

### Indexes

```text
position_history_entity_observed_uidx   UNIQUE   (entity_id, observed_at)
position_history_entity_time_idx        btree    (entity_id, observed_at DESC)
position_history_geocell_time_idx       btree    (geo_cell, observed_at DESC)
position_history_observed_at_idx        btree    (observed_at DESC)
alerts_entity_time_idx                  btree    (entity_id, detected_at DESC)
alerts_counterparty_time_idx            btree    (counterparty_entity_id, detected_at DESC)
alerts_status_time_idx                  btree    (status, detected_at DESC)
alerts_type_time_idx                    btree    (alert_type, detected_at DESC)
route_references_entity_idx             btree    (entity_id)
users_google_sub_key                    UNIQUE   (google_sub)
... plus PKs on all tables
```

All indexes from migrations are present.

---

## 4. Redis

```text
redis-cli PING   -> PONG
redis_version:   7.2.4
tcp_port:        6379
uptime:          ~67 minutes

SCAN 0 COUNT 100 -> (empty cursor, no keys)
```

Redis is empty. The test keys written in CP5 (hash, sorted set, TTL key) have expired or were deleted. This is correct: Redis holds live ephemeral state. The absence of keys before any service has run is expected.

---

## 5. Neo4j

### Constraints

```text
id  name                                     type                    entityType    labelsOrTypes    properties
4   entity_id_unique                         UNIQUENESS              NODE          Entity           [id]
6   proximity_event_idempotency_key_unique   RELATIONSHIP_UNIQUENESS RELATIONSHIP  PROXIMITY_EVENT  [idempotency_key]
```

Both constraints are present: the node uniqueness constraint on `Entity.id` and the relationship uniqueness constraint on `PROXIMITY_EVENT.idempotency_key`. Both are Neo4j 5.7+ Community Edition features confirmed in CP4.

### Indexes

```text
id  name                                     state   type   entityType    properties
3   entity_id_unique                         ONLINE  RANGE  NODE          [id]
1   index_343aff4e                           ONLINE  LOOKUP NODE          (system)
2   index_f7700477                           ONLINE  LOOKUP RELATIONSHIP  (system)
5   proximity_event_idempotency_key_unique   ONLINE  RANGE  RELATIONSHIP  [idempotency_key]
```

Two RANGE indexes back the uniqueness constraints (created automatically). Two LOOKUP indexes are system-managed token indexes always present in Neo4j. No redundant manual indexes exist.

### Graph state

```text
MATCH (n) RETURN count(n) -> 0 nodes
MATCH ()-[r]->() RETURN count(r) -> 0 relationships
```

The graph is empty. The Entity nodes and PROXIMITY_EVENT relationship written in CP5 were deleted during that checkpoint's cleanup step. Schema constraints persist independently of graph data.

---

## 6. Reproducibility check

All three provisioning commands were re-run against the running stack:

### `make migrate`

All 6 migrations applied without error. Every statement uses `IF NOT EXISTS` or `CREATE INDEX IF NOT EXISTS` or `add_retention_policy` with duplicate detection. NOTICE lines confirm existing objects were skipped, not recreated. Exit 0.

### `make topics`

All 8 topics reported `exists`. No topic was re-created. Exit 0.

### `make neo4j-schema`

Schema applied without error. Both `CREATE CONSTRAINT IF NOT EXISTS` statements found existing constraints and skipped. Exit 0.

All three provisioning operations are fully idempotent. Running them multiple times against an existing stack is safe.

---

## 7. Makefile and CLI usability

`make help` lists all documented targets with descriptions. One bug was found and fixed during this checkpoint: the grep pattern `^[a-zA-Z_-]+:` excluded digits, so `neo4j-schema` (containing `4`) did not appear. Pattern corrected to `^[a-zA-Z0-9_-]+:`.

Final `make help` output:

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

Also corrected in this checkpoint: `infra/scripts/migrate.sh` echo lines that used padded column alignment (e.g. `"Container : $CONTAINER"`) were simplified to unpadded form (`"Container: $CONTAINER"`).

---

## 8. CP1-CP7 consistency review

| Checkpoint | Artifact | Status |
| --- | --- | --- |
| CP1: Docker Compose | `docker-compose.yml`, 4 services, named volumes, healthchecks | Consistent. All 4 containers healthy. |
| CP2: TimescaleDB migrations | `infra/migrations/001-006`, `infra/scripts/migrate.sh` | Consistent. 6 tables, hypertable, chunk, retention policy, all indexes present. |
| CP3: Kafka topics | `infra/kafka/topics.sh`, 8 canonical topics | Consistent. All 8 topics provisioned with 1 partition, 1 replica. |
| CP4: Neo4j schema | `infra/neo4j/schema.cypher`, `infra/neo4j/apply-schema.sh` | Consistent. 2 constraints, 4 indexes, all ONLINE. Community Edition supports RELATIONSHIP_UNIQUENESS confirmed. |
| CP5: Manual verification | `concepts/cp-05-manual-store-verification.md` | Consistent. CP5 chunk in TimescaleDB confirms hands-on insert occurred. Redis empty as expected post-TTL. Neo4j graph empty after cleanup. |
| CP6: Logging convention | `cp-06-structured-logging-convention.md` | Consistent. No application services exist yet; convention is documented for Phase 02+. |
| CP7: Health/readiness convention | `cp-07-health-readiness-convention.md` | Consistent. No application services exist yet; convention is documented for Phase 02+. |

No inconsistencies found between infrastructure state and documented artifacts.

---

## 9. Phase 01 exit criteria

| Criterion | Result |
| --- | --- |
| All infrastructure containers start cleanly | PASS: all 4 healthy |
| TimescaleDB migrations are idempotent and complete | PASS: 6 tables, hypertable, retention policy, all indexes |
| Kafka topic provisioning is idempotent | PASS: 8 topics, `exists` on re-run |
| Neo4j constraints and indexes are applied and idempotent | PASS: 2 constraints, 4 indexes, ONLINE |
| Every store is directly inspectable from CLI | PASS: rpk, psql, redis-cli, cypher-shell all confirmed |
| Structured logging convention documented | PASS: cp-06 |
| Health/readiness convention documented | PASS: cp-07 |
| Makefile covers all provisioning operations | PASS: up, migrate, topics, neo4j-schema, help (bug fixed this checkpoint) |
| Manual datastore hands-on requirement complete | PASS: cp-05 |
| No substantial application code prematurely introduced | PASS: Phase 01 contains only infrastructure, schemas, and convention documents |

**Phase 01 exit: PASS.**

All infrastructure is reproducible, inspectable, and consistent. Logging and health conventions are documented for every service that will be introduced in Phase 02+.

---

## 10. README follow-up items

These items are not blockers for Phase 02 but should be addressed in the README before the project is shared:

- Document the `make up && make migrate && make topics && make neo4j-schema` bootstrap sequence explicitly.
- Document the `.env` file requirement and what variables are expected (POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB, NEO4J_AUTH, and optional port overrides).
- Note the Node.js / TypeScript runtime prerequisites for Phase 02+ service code.
