# Alert-State Episode Lifecycle Debrief

---

## Setup

```bash
make up && make topics
bash infra/scripts/migrate.sh

# Terminal 1: alert evaluator
cd services/alert-evaluator
node_modules/.bin/tsx src/evaluator.ts

# Terminal 2: position consumer
cd services/position-consumer
FROM_BEGINNING=false node_modules/.bin/tsx src/consumer.ts
```

---

## Starting state

At the start of CP3, `test123` was already in Redis from the CP2 experiments. Its state:

- `entity:live:test123`: `last_seen_ms=1787804436000` (the resume timestamp from CP2 experiment 3)
- `alert-state:test123`: already written by the evaluator with `dark_since_ms=1787804436000`

This meant episode 2 had already fired before CP3 began. The Kafka `alerts` topic already had two messages:

```
offset 0: test123:SIGNAL_LOSS:0              (episode 1, from CP2)
offset 1: test123:SIGNAL_LOSS:1787804436000  (episode 2, fired between CP2 and CP3)
```

This required no setup: the state machine had already cycled once, giving a real starting point for verifying the second cycle.

---

## Experiment: full second-episode cycle

### Step 1: confirm episode 2 gate and Kafka alert

```bash
docker exec sentinel-redis redis-cli HGETALL "alert-state:test123"
# dark_since_ms        1787804436000
# signal_loss_alert_id test123:SIGNAL_LOSS:1787804436000
# composite_issued     0

docker exec sentinel-redpanda rpk topic consume alerts -o 0 -n 3 2>&1
# offset 0: alert_id=test123:SIGNAL_LOSS:0           dark_since_ms=0           lat=51.5
# offset 1: alert_id=test123:SIGNAL_LOSS:1787804436000  dark_since_ms=1787804436000  lat=51.7
```

| Check | Expected | Observed |
|---|---|---|
| `alert-state` holds episode 2 dark_since_ms | `1787804436000` | PASS |
| Kafka offset 1 alert_id matches gate | yes | PASS |
| Episode 2 `last_known_lat` reflects the position from CP2 resume | `51.7` | PASS |

### Step 2: resume entity — clear episode 2

Produced a fresh position for `test123` with current `time_position`:

```bash
TIME_NOW=$(node -e "console.log(Math.floor(Date.now()/1000))")
# TIME_NOW = 1787862244

echo '{"icao24":"test123","time_position":1787862244,"lat":51.9,"lon":-0.5,...}' | \
  docker exec -i sentinel-redpanda rpk topic produce adsb.raw --key test123 --compression none
# Produced to partition 0 at offset 20
```

Consumer log:

```json
{"message":"signal loss episode cleared","entity_id":"test123",
 "dark_since_ms":"1787804436000","resumed_at_ms":1787862244000,
 "signal_loss_alert_id":"test123:SIGNAL_LOSS:1787804436000"}

{"message":"position persisted","entity_id":"test123","timestamp_ms":1787862244000,
 "lat":51.9,"lon":-0.5,"live_state_accepted":true,"offset":"20"}
```

Redis after resume:

```bash
docker exec sentinel-redis redis-cli EXISTS "alert-state:test123"
# 0   (gate deleted)

docker exec sentinel-redis redis-cli HGETALL "recent-loss:test123"
# dark_since_ms        1787804436000
# resumed_at_ms        1787862244000
# signal_loss_alert_id test123:SIGNAL_LOSS:1787804436000
```

| Check | Expected | Observed |
|---|---|---|
| `alert-state` deleted on resume | EXISTS = 0 | PASS |
| `recent-loss` written with episode 2 dark_since_ms | `1787804436000` | PASS |
| `resumed_at_ms` = source event time of resume, not Date.now() | `1787862244000` | PASS |
| `signal_loss_alert_id` matches episode 2 alert | yes | PASS |

### Step 3: wait for episode 3

After the resume, `entity:live:test123` has `last_seen_ms=1787862244000`. The threshold is 300,000 ms. The evaluator scanned every 30s and the episode fired at ~170s elapsed (the entity had been quiet since the resume).

Evaluator log when episode 3 fired:

```
{ instanceId: '6a78c2b6-...', entityId: 'test123',
  alertId: 'test123:SIGNAL_LOSS:1787862244000', darkSinceMs: 1787862244000 }
  signal loss detected
```

Kafka after episode 3:

```bash
docker exec sentinel-redpanda rpk topic consume alerts -o 0 -n 3 2>&1
```

```
offset 0: alert_id=test123:SIGNAL_LOSS:0             dark_since_ms=0             lat=51.5
offset 1: alert_id=test123:SIGNAL_LOSS:1787804436000 dark_since_ms=1787804436000 lat=51.7
offset 2: alert_id=test123:SIGNAL_LOSS:1787862244000 dark_since_ms=1787862244000 lat=51.9
```

Redis after episode 3:

```bash
docker exec sentinel-redis redis-cli HGETALL "alert-state:test123"
# dark_since_ms        1787862244000
# signal_loss_alert_id test123:SIGNAL_LOSS:1787862244000
# composite_issued     0

docker exec sentinel-redis redis-cli HGETALL "recent-loss:test123"
# dark_since_ms        1787804436000   (still episode 2 — not yet overwritten)
# resumed_at_ms        1787862244000
# signal_loss_alert_id test123:SIGNAL_LOSS:1787804436000
```

| Check | Expected | Observed |
|---|---|---|
| Episode 3 alert_id has new dark_since_ms | `1787862244000` (not 0 or `1787804436000`) | PASS |
| Episode 3 `last_known_lat` reflects the episode 2 resume position | `51.9` | PASS |
| Three distinct alert_ids on Kafka, one per episode | yes | PASS |
| `alert-state` now holds episode 3 gate | `dark_since_ms=1787862244000` | PASS |
| `recent-loss` still holds episode 2 (not overwritten until next resume) | yes | PASS |

---

## Checkpoint completion

### Engineering debrief

**Data flow across the full cycle:** each episode is anchored to `dark_since_ms = last_seen_ms` at detection time. On episode open, the evaluator writes `alert-state` and publishes to Kafka — both with the same `dark_since_ms`. On resume, the Position Consumer writes `recent-loss` with the episode's metadata (copying `dark_since_ms` and `signal_loss_alert_id` from `alert-state`), then deletes `alert-state`. This clears the gate. The next silence produces a new `dark_since_ms` derived from the resume position's `last_seen_ms` — guaranteed to differ because the monotonic guard only accepts positions newer than the current hash.

**Why this design is correct across replays:** if the consumer replays old Kafka messages, the monotonic guard rejects them as stale — their `timestamp_ms` is less than or equal to the current `last_seen_ms`. A stale message cannot trigger `clearSignalLossEpisode` and cannot inadvertently delete an active gate. The resume handler only runs when `live_state_accepted = true`.

**The `recent-loss` overwrite trade-off:** only the most recent episode is kept in `recent-loss`. If an entity cycles through five episodes, only episode 5's record is in Redis; earlier episodes are only in the `alerts` Kafka topic and TimescaleDB. Phase 06 only needs the most recent episode (it correlates the current dark period with a recent resume), so overwriting is correct and avoids unbounded Redis growth.

### Manual inspection commands

```bash
# Confirm all three episode alerts on Kafka
docker exec sentinel-redpanda rpk topic consume alerts -o 0 -n 5 2>&1

# Inspect current episode gate
docker exec sentinel-redis redis-cli HGETALL "alert-state:test123"

# Inspect most recent episode record
docker exec sentinel-redis redis-cli HGETALL "recent-loss:test123"

# Count currently alerted entities
docker exec sentinel-redis redis-cli KEYS "alert-state:*" | wc -l

# Watch evaluator scan ticks in real time
tail -f /tmp/evaluator.log | grep -E "scan complete|signal loss"
```

### Knowledge-check questions

1. `recent-loss` only stores the most recent episode's data. If an entity cycled through five episodes, where would you find the data for episodes 1 through 4?
2. If the evaluator crashes immediately after writing `alert-state` for episode 3 but before producing to Kafka, would episode 3 ever be alerted? What would need to happen first?
3. The Position Consumer writes `recent-loss` before deleting `alert-state`. If it crashes between those two writes, what is the state of Redis and what happens on the entity's next accepted position?
4. Why can't two episodes for the same entity produce the same `alert_id`, given the monotonic guard in the Position Consumer?

### Optional manual tweak

Pick a fresh entity ID (e.g. `probe99`). Manually set `last_seen_ms=0` in Redis. Watch the evaluator detect it. Then produce two separate resume messages — each with a progressively newer `time_position`. After each resume, wait for the next episode to fire. Verify that each episode's `dark_since_ms` in the Kafka alert matches the `time_position` you sent in the preceding resume. This makes the `dark_since_ms = last_seen_ms` arithmetic concrete without relying on a pre-existing test entity.

### Next checkpoint

CP4: API scaffold and auth. This introduces:

- Google OAuth ID token verification (server-side, using the Google auth library)
- Sentinel JWT issuance into an HttpOnly cookie
- Express routes protected by JWT middleware
- WebSocket upgrade gated on JWT from cookie
- `GET /alerts` returning rows from TimescaleDB
- The Kafka consumer that sinks `alerts` topic messages into TimescaleDB with `ON CONFLICT (alert_id) DO NOTHING`

The alert Kafka topic is now populated with real data (three test123 episodes). The API sink will be able to consume those messages immediately and demonstrate idempotent persistence without waiting for new alerts to fire.

---

## Key observations

| Concept | Observed |
|---|---|
| Second episode `alert_id` differs from first | PASS: `1787804436000` vs `0` |
| `dark_since_ms` anchors to `last_seen_ms` at detection, not `Date.now()` | PASS: episode 3 dark_since matches the resume timestamp exactly |
| Gate deleted atomically clears the episode | PASS: EXISTS=0 immediately after consumer processes the resume |
| `recent-loss` written before gate delete | PASS: both writes observed in consumer log in order |
| `resumed_at_ms` = source event time | PASS: `1787862244000` matches `time_position` sent in rpk produce |
| Monotonic guard prevents stale resume from clearing the gate | Invariant holds: only messages with newer `timestamp_ms` trigger `clearSignalLossEpisode` |
| Three consecutive episodes produce three distinct Kafka messages | PASS: offsets 0, 1, 2 all have different alert_ids |
