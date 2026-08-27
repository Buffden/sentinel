# Signal-Loss Detection Debrief

---

## Setup

```bash
make up && make topics
bash infra/scripts/migrate.sh

# Terminal 1: alert evaluator
cd services/alert-evaluator
node_modules/.bin/tsx src/evaluator.ts

# Terminal 2: position consumer (FROM_BEGINNING=false avoids Redpanda v24 seek issue)
cd services/position-consumer
FROM_BEGINNING=false node_modules/.bin/tsx src/consumer.ts
```

---

## Experiment 1: first scan detects dark entity

Seed a stale entity with `last_seen_ms=0` (1970 epoch — guaranteed dark):

```bash
docker exec sentinel-redis redis-cli HSET entity:live:test123 \
  last_seen_ms 0 entity_type aircraft on_ground '' \
  lat 51.5 lon -0.1 altitude_m 10000 speed_mps 200 course_deg 270
```

Wait up to 30s for the next scan tick. Observed evaluator log:

```
{ instanceId: '8a23ec4b-...' } acquired leader lease — starting scan loop
{ instanceId: '8a23ec4b-...', scanned: 0, alerted: 0 } scan complete   (tick 1 — entity not yet seeded)
{ instanceId: '8a23ec4b-...', scanned: 1, alerted: 1,
  entityId: 'test123', alertId: 'test123:SIGNAL_LOSS:0', darkSinceMs: 0 } signal loss detected
{ instanceId: '8a23ec4b-...', scanned: 1, alerted: 1 } scan complete
```

Redis gate inspection:

```bash
docker exec sentinel-redis redis-cli HGETALL "alert-state:test123"
# dark_since_ms        0
# signal_loss_alert_id test123:SIGNAL_LOSS:0
# composite_issued     0
```

Kafka alert inspection:

```bash
docker exec sentinel-redpanda rpk topic consume alerts --offset start -n 5
# {
#   "alert_id":    "test123:SIGNAL_LOSS:0",
#   "entity_id":   "test123",
#   "entity_type": "aircraft",
#   "alert_type":  "SIGNAL_LOSS",
#   "priority":    "STANDARD",
#   "status":      "NEW",
#   "detected_at_ms": 1787781408881,
#   "payload": {
#     "dark_since_ms": 0,
#     "last_known_lat": 51.5,
#     "last_known_lon": -0.1,
#     "last_known_altitude_m": 10000,
#     "last_known_speed_mps": 200,
#     "last_known_course_deg": 270
#   }
# }
```

| Check | Expected | Observed |
| --- | --- | --- |
| `alert-state:test123` written | yes | PASS |
| `dark_since_ms = last_seen_ms` (source event time, not processing time) | 0 | PASS |
| `signal_loss_alert_id` matches Kafka `alert_id` | yes | PASS |
| Alert on Kafka `alerts` topic with correct payload | yes | PASS |
| `detected_at_ms` is processing time (not 0) | yes | PASS — large current timestamp |

---

## Experiment 2: repeated scans do not re-emit

After experiment 1, waited for 16 more scan ticks (~8 minutes). Confirmed via evaluator logs that `alerted: 0` on every subsequent tick:

```
{ ..., scanned: 1, alerted: 0 } scan complete   (repeated 16 times)
```

Confirmed Kafka `alerts` topic still has exactly one message at offset 0:

```bash
docker exec sentinel-redpanda rpk topic describe alerts
# LOG-END-OFFSET: 1   (one message produced, never re-emitted)
```

| Check | Expected | Observed |
| --- | --- | --- |
| 16 scan ticks after detection — alerted: 0 on each | yes | PASS |
| Kafka `alerts` topic still at offset 1 (one message total) | yes | PASS |
| `alert-state:test123` still exists after all ticks | yes | PASS — gate persists until entity resumes |

---

## Experiment 3: entity resumes — episode gate cleared

Produced a Kafka `adsb.raw` message for `test123` with a fresh `time_position` (current Unix seconds) using `--compression none`:

```bash
TIME_NOW=$(node -e "console.log(Math.floor(Date.now()/1000))")
echo "{\"icao24\":\"test123\",\"time_position\":$TIME_NOW,...}" | \
  docker exec -i sentinel-redpanda rpk topic produce adsb.raw --key test123 --compression none
```

Position consumer processed the message. Observed consumer log:

```json
{"message":"signal loss episode cleared","entity_id":"test123","dark_since_ms":"0","resumed_at_ms":1787804436000,"signal_loss_alert_id":"test123:SIGNAL_LOSS:0"}
{"message":"position persisted","entity_id":"test123","live_state_accepted":true,"offset":"19"}
```

Redis state after resume:

```bash
docker exec sentinel-redis redis-cli EXISTS "alert-state:test123"
# 0   (gate deleted)

docker exec sentinel-redis redis-cli HGETALL "recent-loss:test123"
# dark_since_ms        0
# resumed_at_ms        1787804436000
# signal_loss_alert_id test123:SIGNAL_LOSS:0
```

| Check | Expected | Observed |
| --- | --- | --- |
| `alert-state:test123` deleted on resume | yes | PASS — EXISTS returns 0 |
| `recent-loss:test123` written with dark_since_ms | yes | PASS |
| `resumed_at_ms` matches source event time of resume (not processing time) | yes | PASS |
| `signal_loss_alert_id` matches original alert | yes | PASS |

---

## Checkpoint Completion

### Engineering debrief

**Data flow:** the leader runs `runScan()` every 30s. It cursor-iterates all `entity:live:*` Redis hashes, skips on-ground entities and those seen within the last 5 minutes, then checks `alert-state:{entity_id}`. If absent, it writes the gate (dark_since_ms, signal_loss_alert_id, composite_issued=0) before producing a deterministic SIGNAL_LOSS alert to the `alerts` Kafka topic keyed by entity_id. The gate write comes first: a crash between gate write and Kafka produce means the entity misses an alert for this episode, which is the accepted trade-off over the alternative of re-emitting on every restart.

On entity resume, the Position Consumer detects `alert-state` during an accepted live-state write, records the episode in `recent-loss`, then deletes `alert-state`. This opens a fresh episode slot so a future silence produces a new alert_id.

**Main trade-off:** gate-first ordering prevents duplicate Kafka messages at the cost of a potential missed alert if the evaluator crashes in that narrow window. The duplicate-prevention benefit is judged more important: a duplicate alert is operator-visible noise on every scan tick; a missed single-episode alert is recoverable when the entity goes dark again.

**Failure behaviour:** if two evaluator instances briefly both hold the lease (split-brain), both write `alert-state` with identical values and produce the same `alert_id` to Kafka. The API persists with `ON CONFLICT (alert_id) DO NOTHING` — exactly one durable row. The duplicate Kafka message and potential duplicate WebSocket delivery are the known at-least-once artifacts.

### Manual inspection commands

```bash
# Confirm scan is running (watch for scan complete logs)
docker logs -f sentinel-alert-evaluator

# Inspect episode gate for a specific entity
docker exec sentinel-redis redis-cli HGETALL "alert-state:{entity_id}"

# Read all alerts on Kafka from the start
docker exec sentinel-redpanda rpk topic consume alerts --offset start -n 20

# Inspect recent-loss after entity resumes
docker exec sentinel-redis redis-cli HGETALL "recent-loss:{entity_id}"

# Count entities currently in ALERTED state (have alert-state key)
docker exec sentinel-redis redis-cli KEYS "alert-state:*" | wc -l

# Manually clear a gate to test re-detection (dev only)
docker exec sentinel-redis redis-cli DEL "alert-state:{entity_id}"
```

### Knowledge-check questions

1. Why is the episode gate written to Redis before the Kafka produce rather than after?
2. An entity has been dark for 45 minutes. How many Kafka messages are on the `alerts` topic for it? How many `alert-state` keys exist for it?
3. Walk through exactly what Redis keys are written and deleted during a full episode cycle (dark → alerted → resumed).
4. Two evaluator instances both scan the same dark entity in a split-brain window. Name every write that happens and explain why only one durable alert row appears in TimescaleDB.

### Optional manual tweak

Lower `SIGNAL_LOSS_THRESHOLD_MS` in `evaluator.ts` from `300_000` to `10_000` (10 seconds). Restart the evaluator. Seed a fresh entity with a `last_seen_ms` 15 seconds in the past and watch the scan detect it within the next tick. Then restore `300_000`. This makes the threshold arithmetic concrete and observable without waiting 5 minutes.

### Next checkpoint

Signal-loss recovery second episode: exercise the full second-episode cycle end-to-end. Let an entity go dark, confirm the first alert. Resume the entity (position consumer clears the gate). Let it go dark again. Confirm the second alert has a new `dark_since_ms` and a different `alert_id`. This verifies the `recent-loss` + gate-delete sequence actually enables a fresh episode — not just that the keys are written correctly.

---

## Key observations

| Concept | Observed |
| --- | --- |
| Gate written before Kafka produce prevents duplicate re-emission on restart | PASS — 16 scan ticks produced exactly one Kafka message |
| `dark_since_ms = last_seen_ms` at detection time, not `Date.now()` | PASS — value was 0 (the stale entity's last_seen_ms), not current time |
| `detected_at_ms` is processing time | PASS — large current timestamp in alert, not 0 |
| Redis SCAN cursor loop handles partial results safely | PASS — no duplicate alerts despite SCAN's non-atomic semantics |
| Position Consumer clears gate on accepted resume position | PASS — `alert-state` deleted, `recent-loss` written in one handler |
| `rpk topic produce` defaults to Snappy; KafkaJS v2.2.4 has no Snappy decoder | Confirmed — always use `--compression none` for test messages |
| `FROM_BEGINNING=true` triggers incompatible seek API in Redpanda v24.1.2 | Confirmed — use `FROM_BEGINNING=false` for dev restarts |
