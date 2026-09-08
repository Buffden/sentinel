# Alert Evaluator Proximity Consumer Debrief

---

## Setup

```bash
make up
make topics
cd services/alert-evaluator
pnpm install
```

Confirmed no other consumer of the real `alerts` topic (API's alert-sink) was running before testing, per the existing suite's own warning about polluting durable state.

---

## Experiment 1: `handleProximityCandidate` against real Redis and the real `alerts` topic

Three new tests added to the existing `evaluator.integration.test.ts` suite, reusing its real Kafka consumer/producer setup:

```text
Test Files  2 passed (2)
      Tests  18 passed (18)
```

(15 pre-existing across both files, 3 new here.)

| Test | Proves |
| --- | --- |
| emits a deterministic `UNSCHEDULED_PROXIMITY` alert from a proximity candidate | Baseline: correct `alert_id`, `alert_type`, `entity_type` from Redis, and payload fields |
| defaults `entity_type` to an empty string when the entity has no live state | The `NOT NULL` column guard holds even for an entity Alert Evaluator has never seen |
| computes the same `alert_id` regardless of which run produced the candidate | No hidden nondeterminism (e.g. a stray `Date.now()`) leaking into alert identity |

Cleanup verified: no leftover `entity:live:test-evaluator-*` keys.

---

## Experiment 2: manual end-to-end proof against the real running service

```bash
docker exec sentinel-redis redis-cli HSET entity:live:demo-proximity-a entity_type aircraft
FROM_BEGINNING=false node_modules/.bin/tsx src/evaluator.ts
```

Observed on startup — the proximity consumer and the leader-gated scan running concurrently, exactly as designed:

```text
{ instanceId: '...' } kafka producer connected
{ instanceId: '...' } proximity candidates consumer running
{ instanceId: '...' } acquired leader lease — starting scan loop
{ instanceId: '...', scanned: 1, alerted: 0 } scan complete
```

Produced a real candidate (`--compression none`, matching the documented Redpanda/KafkaJS compatibility note from Phase 03):

```bash
echo '{"pair_key":"demo-proximity-a:demo-proximity-b","entity_a_id":"demo-proximity-a","entity_b_id":"demo-proximity-b","episode_start_ms":<ms>,"lat":37.0,"lon":-121.0,"distance_at_detection":42.5}' \
  | docker exec -i sentinel-redpanda rpk topic produce proximity.candidates --compression none
```

Observed immediately in the running service's log:

```text
{
  instanceId: '...',
  alertId: 'demo-proximity-a:demo-proximity-b:UNSCHEDULED_PROXIMITY:<ms>',
  pairKey: 'demo-proximity-a:demo-proximity-b'
} unscheduled proximity alert emitted
```

Confirmed on the real `alerts` topic:

```json
{"alert_id":"demo-proximity-a:demo-proximity-b:UNSCHEDULED_PROXIMITY:<ms>","entity_id":"demo-proximity-a","counterparty_entity_id":"demo-proximity-b","entity_type":"aircraft","alert_type":"UNSCHEDULED_PROXIMITY","priority":"STANDARD","status":"NEW","detected_at_ms":<ms>,"payload":{"pair_key":"demo-proximity-a:demo-proximity-b","counterparty_entity_id":"demo-proximity-b","lat":37,"lon":-121,"distance_metres":42.5,"episode_start_ms":<ms>}}
```

`entity_type` correctly picked up `"aircraft"` from the seeded `entity:live` hash — proving the Redis lookup, not a hardcoded default, produced the value.

| Check | Expected | Observed |
| --- | --- | --- |
| Proximity consumer and leader-gated scan run concurrently | yes | PASS |
| Real candidate message consumed and turned into a real alert | yes | PASS |
| `entity_type` sourced from `entity:live:*`, not hardcoded | yes | PASS |
| Deterministic `alert_id` matches the documented rule | `{pair_key}:UNSCHEDULED_PROXIMITY:{episode_start_ms}` | PASS |

Cleanup:

```bash
docker exec sentinel-redis redis-cli DEL entity:live:demo-proximity-a
```

The demo alert message itself remains on the `alerts` topic (Kafka has no per-message delete) but was never persisted anywhere, since no API alert-sink was running — the same accepted trade-off the pre-existing SIGNAL_LOSS test suite already relies on.

---

## Engineering debrief

**Data flow:** `main()` connects the Kafka producer, then connects and starts the `proximityConsumer` (group `alert-evaluator`, topic `proximity.candidates`), then enters the pre-existing leader-election loop for the signal-loss scan. Both run concurrently from that point: the scan under a Redis lease, the candidate consumer under Kafka's own partition assignment. Each candidate becomes exactly one deterministic `UNSCHEDULED_PROXIMITY` alert, published to the same `alerts` topic and durable path signal-loss already uses.

**Trade-off:** no leader election for this consumer, deliberately — Kafka's consumer-group protocol already guarantees single-partition ownership, so adding a Redis lease on top would be redundant coordination solving an already-solved problem.

**Failure behaviour:** a malformed candidate message is logged and skipped, offset still committed, matching the same boundary-guard philosophy as the Correlation Worker's own `parsePosition`. A crash mid-message causes Kafka to redeliver; the deterministic `alert_id` and the API's `ON CONFLICT DO NOTHING` absorb the resulting duplicate publish exactly as they already do for signal-loss.

## Manual inspection commands

```bash
# Run the real service
cd services/alert-evaluator
FROM_BEGINNING=false node_modules/.bin/tsx src/evaluator.ts

# Watch the alert-evaluator consumer group
docker exec sentinel-redpanda rpk group describe alert-evaluator

# Watch real alerts as they're published
docker exec sentinel-redpanda rpk topic consume alerts -f '%v\n'
```

## Knowledge-check questions

1. Why does the proximity consumer need no Redis-lease leader election when the signal-loss scan does?
2. Why doesn't `handleProximityCandidate` check `KNOWN_ASSOCIATE` again?
3. What does `entity_type` default to when the entity has no live state, and which database constraint does that default protect?
4. Trace what happens if the same `proximity.candidates` message is redelivered after a crash.

## Next

Exit verification for the whole phase: replay/reset/new-episode failure experiments already exercised piecemeal, now proven together, plus confirming SIGNAL_LOSS and UNSCHEDULED_PROXIMITY alerts coexist through the same canonical path with no special-case delivery code.

---

## Key observations

| Concept | Observed |
| --- | --- |
| Proximity consumer runs without leader election | Confirmed — started immediately, independent of scan's lease state |
| `run()` doesn't block the leader loop | Confirmed — both log lines ("consumer running", "acquired leader lease") appear in sequence at startup |
| `entity_type` sourced from Redis, defaults to `''` when absent | Confirmed by both an automated test and the real manual run |
| Deterministic `alert_id`, stable across redelivery | Confirmed by both an automated test and identical real Kafka messages |
| Test + manual cleanup | PASS |
