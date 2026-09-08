# Service Assembly Debrief

---

## Setup

```bash
make up
make topics        # position.normalized, proximity.candidates already provisioned
make neo4j-schema
cd services/correlation-worker
pnpm install
```

Confirmed both topics already existed on this dev stack:

```text
position.normalized   1  1
proximity.candidates  1  1
```

---

## Experiment 1: `handlePosition` against real Redis, Neo4j, and a real Kafka topic

Four integration tests, publishing to and consuming from the actual `proximity.candidates` topic (no live consumer exists yet in this codebase to pollute, unlike the Alert Evaluator's equivalent test against the shared `alerts` topic):

```text
Test Files  8 passed (8)
      Tests  38 passed (38)
```

(34 from earlier checkpoints, 4 new here.)

| Test | Proves |
| --- | --- |
| publishes a real `proximity.candidates` message for a genuinely close, unscheduled pair | Full pipeline: candidate lookup, distance filter, episode creation, Neo4j evidence, real Kafka publish, `candidate_published` confirmed `'1'` |
| does not publish for a known-associate pair, but still records graph evidence | The known-associate gate holds through the full assembled path, not just in isolation |
| does not publish a second time for a later ping in the same episode | Episode continuation correctly suppresses a repeat publish |
| does nothing when no candidates are within range | No episode key created, no Kafka call, for an entity with no nearby traffic |

One floating-point test bug caught and fixed, not glossed over: `computeMidpoint`'s own unit test initially asserted exact equality (`toEqual`) against a hand-computed average, which failed on ordinary IEEE-754 rounding (`37.001000000000005` vs `37.001`). Fixed with `toBeCloseTo`. A reminder that averaging floats and asserting exact equality almost never survives contact with real floating-point arithmetic.

A recurring but non-blocking `TimeoutNegativeWarning` from Node appears on every consumer/producer connect, both in the test suite and when running the real service directly — traced to kafkajs's own internals (reproduced identically outside any test code), not this codebase, and does not affect correctness of any test or the manual run below.

Cleanup verified: no leftover `test-worker-*` keys in Redis, no leftover `test-worker-*` nodes in Neo4j.

---

## Experiment 2: manual end-to-end proof against the real running service

Unlike everything before it, this is not an automated test — it's the one thing only a running process against the real dev stack can prove.

```bash
# Seed a real candidate entity
docker exec sentinel-redis redis-cli HSET entity:live:demo-plane-b lat 37.0003 lon -121.0 entity_type aircraft last_seen_ms <now_ms>
docker exec sentinel-redis redis-cli ZADD geo-cell:8729a9749ffffff <now_ms> demo-plane-b

# Start the real service
FROM_BEGINNING=false node_modules/.bin/tsx src/worker.ts

# Produce a real position.normalized message (compression none -- see
# phase-03's Redpanda/KafkaJS compatibility note)
echo '{"entity_id":"demo-plane-a","entity_type":"aircraft","timestamp_ms":<now_ms>,"lat":37.0,"lon":-121.0}' \
  | docker exec -i sentinel-redpanda rpk topic produce position.normalized --compression none
```

Observed, in order:

```text
docker exec sentinel-redpanda rpk group describe correlation-worker
# CURRENT-OFFSET caught up to the produced message's offset, LAG 0

docker exec sentinel-redis redis-cli HGETALL proximity-episode:demo-plane-a:demo-plane-b
# episode_start_ms  <now_ms>
# last_seen_ms      <now_ms>
# candidate_published  1

docker exec sentinel-redpanda rpk topic consume proximity.candidates --offset -5 --num 5 -f '%v\n'
# {"pair_key":"demo-plane-a:demo-plane-b","entity_a_id":"demo-plane-a","entity_b_id":"demo-plane-b",
#  "episode_start_ms":<now_ms>,"lat":37.000150000000005,"lon":-121,"distance_at_detection":33.35851559290627}

docker exec sentinel-neo4j cypher-shell -u neo4j -p sentinel-dev \
  "MATCH (a:Entity {id:'demo-plane-a'})-[r:PROXIMITY_EVENT]-(b:Entity {id:'demo-plane-b'}) RETURN a.id, b.id, r.idempotency_key, r.min_distance_metres;"
# "demo-plane-a", "demo-plane-b", "demo-plane-a:demo-plane-b:<now_ms>", 33.35851559290627
```

All three stores agree on the same distance (~33.36m) and the same episode identity, produced by one real message through the real service.

| Check | Expected | Observed |
| --- | --- | --- |
| Real message consumed, offset committed | LAG 0 after processing | PASS |
| Redis episode created with `candidate_published = 1` | yes | PASS |
| Real `proximity.candidates` message on the real topic | matches expected schema | PASS |
| Neo4j `PROXIMITY_EVENT` edge with matching distance | yes | PASS |

Cleanup:

```bash
docker exec sentinel-redis redis-cli DEL entity:live:demo-plane-b proximity-episode:demo-plane-a:demo-plane-b
docker exec sentinel-redis redis-cli ZREM geo-cell:8729a9749ffffff demo-plane-b
docker exec sentinel-neo4j cypher-shell -u neo4j -p sentinel-dev \
  "MATCH (e:Entity) WHERE e.id IN ['demo-plane-a','demo-plane-b'] DETACH DELETE e;"
```

Confirmed removed on both stores afterward.

---

## Engineering debrief

**Data flow:** `handlePosition` computes the incoming entity's H3 cell, finds candidates, filters to real distance matches, and for each match runs the full episode/evidence/publish-gate decision, publishing to the real `proximity.candidates` topic and confirming with `markCandidatePublished` only after the send succeeds. `run()` is the thin Kafka wiring around it: subscribe, `eachMessage`, manual offset commit after the handler returns.

**Trade-off:** malformed messages are logged and skipped (offset still committed) rather than retried indefinitely — a single unparseable message must not permanently stall the partition, and this topic's producer (Position Consumer) is trusted to emit canonical shapes, so this is a boundary guard against genuinely unexpected input, not an expected steady-state path.

**Failure behaviour:** every write in the assembled pipeline (episode state, Neo4j MERGE, `candidate_published` gating) was already proven idempotent/replay-safe in isolation; assembling them into one consumer changes nothing about that — a crash mid-message just causes Kafka to redeliver, and every downstream effect tolerates being repeated.

## Manual inspection commands

```bash
# Run the real service against the dev stack
cd services/correlation-worker
FROM_BEGINNING=false node_modules/.bin/tsx src/worker.ts

# Watch consumer lag
docker exec sentinel-redpanda rpk group describe correlation-worker

# Watch real candidates as they're published
docker exec sentinel-redpanda rpk topic consume proximity.candidates -f '%v\n'
```

## Knowledge-check questions

1. Why is `handlePosition` tested directly while the Kafka consumer loop around it isn't?
2. Why does the candidate freshness bound use the incoming message's own timestamp rather than `Date.now()`?
3. What happens when `position.normalized` delivers a message that fails `parsePosition`'s shape check, and why doesn't it block the partition?
4. Walk through what the manual end-to-end proof showed that no automated test alone could.

## Optional manual tweak

Seed a second candidate closer than `demo-plane-b` and confirm `filterByDistance`'s nearest-first ordering surfaces it as the first match processed — though since every match is now evaluated independently, ordering only affects processing sequence, not correctness.

## Next

Alert Evaluator integration: consume `proximity.candidates` and emit a deterministic `UNSCHEDULED_PROXIMITY` alert through the existing alert path — the same serving path signal-loss already proved end-to-end.

---

## Key observations

| Concept | Observed |
| --- | --- |
| Real end-to-end run | position.normalized in -> proximity.candidates out, matching evidence in Redis and Neo4j |
| Distance computed consistently across all three stores | ~33.36m in Redis episode context, Kafka payload, and Neo4j edge |
| Floating-point exact-equality test bug | Caught by a real failing assertion, fixed with `toBeCloseTo` |
| `TimeoutNegativeWarning` | Reproduced from kafkajs itself, outside any test code; non-blocking |
| Test cleanup (Redis + Neo4j + manual demo) | PASS |
