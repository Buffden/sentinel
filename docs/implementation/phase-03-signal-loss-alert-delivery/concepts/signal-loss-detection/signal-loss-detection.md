# Signal-Loss Detection — Design and Learning Reference

Plain language first, then technical depth, then the code. Use this to understand, inspect, and defend the CP2 implementation.

---

![Signal-Loss Detection Flow](../../../../../../diagrams/docs/implementation/phase-03-signal-loss-alert-delivery/concepts/signal-loss-flow.svg)

## What this checkpoint adds

CP1 gave the Alert Evaluator leader election: one instance holds the lease, the rest stand by. The scan loop existed but `runScan()` was a stub that only printed a log line.

CP2 fills in `runScan()`. The leader now:

1. Walks every `entity:live:*` key in Redis.
2. Applies eligibility filters (on-ground, missing fields, below threshold, already alerted).
3. Writes `alert-state:{entity_id}` as the episode gate.
4. Publishes a deterministic SIGNAL_LOSS alert to the `alerts` Kafka topic.

This is the first real anomaly the system detects and the first message on the `alerts` topic. Everything in the API and dashboard phase reads from that topic.

---

## Concepts in plain language

### 1. What "signal loss" means in Sentinel

`last_seen_ms` in `entity:live:{entity_id}` holds the source event time of the most recent accepted position update — the timestamp embedded in the telemetry, not the wall clock when the Position Consumer processed it.

Signal loss is declared when:

```
now_ms - last_seen_ms >= SIGNAL_LOSS_THRESHOLD_MS (300,000 ms = 5 min)
```

An aircraft that stopped transmitting 6 minutes ago has `last_seen_ms` 6 minutes in the past. The scan fires, the difference exceeds the threshold, and the entity is declared dark.

The Redis hash TTL (24h) is not the detector. It is a cleanup mechanism that removes entities that have been permanently silent for a full day. Signal loss is detected by this arithmetic comparison.

### 2. Why on-ground entities are excluded

Aircraft legitimately power down their transponders when parked. Triggering a SIGNAL_LOSS alert every time a plane lands and goes quiet would flood the operator with false positives. In v1, `on_ground = 'true'` is the exclusion condition. Unknown ground state (`on_ground` null or empty string) is treated conservatively — included in detection — because the system does not know whether the silence is legitimate.

### 3. The episode gate: `alert-state:{entity_id}`

Without a gate, every scan tick would emit a new alert for the same silent entity. The gate prevents this.

The gate is a Redis hash key `alert-state:{entity_id}`. Its presence means "this entity is in an active dark episode — alert already emitted." The scan's first action before emitting is `EXISTS alert-state:{entity_id}`. If the key is present, the entity is skipped.

The key is written before the Kafka produce. If the evaluator crashes after writing Redis but before producing to Kafka, the next scan sees the key and skips — and the entity never gets an alert for this episode. This is a deliberate trade-off: the recovery is manual or the entity must resume and go dark again. The alternative (write Redis after Kafka) would produce the alert but leave the gate absent, causing the next scan to re-emit.

No TTL is set on `alert-state`. The key persists until the Position Consumer deletes it when the entity resumes. The gate must outlast any scan interval.

### 4. Deterministic alert_id

```
{entity_id}:SIGNAL_LOSS:{dark_since_ms}
```

`dark_since_ms` is `last_seen_ms` at detection time — the timestamp of the last accepted position, not processing time. Two scan ticks for the same dark episode produce the same `alert_id` because `last_seen_ms` does not change while the entity is silent.

Two different dark episodes produce different `alert_id` values because `dark_since_ms` will differ — the entity resumed, received a new position, then went silent again from a later `last_seen_ms`.

This is what makes duplicate Kafka messages safe. If two evaluator instances both publish the same alert (split-brain window), the API's `ON CONFLICT (alert_id) DO NOTHING` absorbs the second insert. Exactly one durable row is written.

### 5. Redis SCAN is cursor-based, not atomic

`redis.scan(cursor, 'MATCH', 'entity:live:*', 'COUNT', 100)` returns a cursor and a batch of keys. To walk all keys you must call SCAN repeatedly until the cursor returns to 0. This is not atomic — other writes can happen between calls, and the same key can appear more than once.

Consequences for Sentinel:
- A key appearing twice: the entity is evaluated twice in one scan. The second evaluation finds `alert-state` already written and skips. Safe.
- A key added mid-scan (new entity): may or may not appear in this scan. Next scan catches it. Acceptable.
- A key deleted mid-scan (entity expired its 24h TTL): HGETALL returns an empty hash; the scan skips on missing `last_seen_ms`. Safe.

### 6. What `detected_at_ms` is allowed to be

Most timestamps in Sentinel use source event time. `detected_at_ms` in the alert payload is explicitly processing time — the wall clock when the scan ran. This is the one field in alerts where processing time is correct: it answers "when did the evaluator notice this entity was dark?" not "when did the entity go dark?" `dark_since_ms` in the payload answers the second question.

### 7. Signal-loss recovery (Position Consumer, also Phase 03)

When the Position Consumer writes an accepted position for an entity that has an `alert-state:{entity_id}` key, it:

1. Writes `recent-loss:{entity_id}` with `dark_since_ms`, `resumed_at_ms`, and `signal_loss_alert_id`.
2. Deletes `alert-state:{entity_id}`.

This clears the episode gate. When the entity goes silent again, `dark_since_ms` will be a different (later) value, producing a new `alert_id` and a new alert.

`recent-loss` is read by Phase 06 (composite correlation). Writing it now costs nothing and avoids retrofitting the Position Consumer later.

---

## Failure modes

### Crash after gate write, before Kafka produce

`alert-state:{entity_id}` exists. The entity never receives an alert for this episode. On restart, the scan finds the gate and skips. The entity must resume and go dark again for a new alert to be emitted.

This is a deliberate trade-off: write the gate first to prevent duplicate publishes, at the cost of a potential missed alert on crash.

### Split-brain: two instances both scan the same dark entity

Both write `alert-state:{entity_id}` with the same values (both read the same `last_seen_ms`). Both produce to Kafka with the same `alert_id`. The second Redis write is a no-op (same fields). The API's `ON CONFLICT DO NOTHING` absorbs the second Kafka message. One durable row.

### Entity resumes between gate check and Kafka produce

The scan checks `EXISTS alert-state` (absent), then the Position Consumer writes the entity's new position and deletes `alert-state`. Then the scan writes `alert-state` and publishes the alert. The entity just resumed, so the alert is technically stale. This is an acceptable edge case in v1.

### Redis SCAN misses an entity

An entity that goes dark just before the scan starts may not appear in this scan's cursor walk. It will appear in the next scan (30s later). Acceptable — the detection SLA is "eventually within one scan interval after the threshold is crossed."

---

## Data flow

```
[every SCAN_INTERVAL_MS, leader only]

  for cursor in SCAN('entity:live:*'):
    HGETALL entity:live:{entity_id}
      skip if on_ground = 'true'
      skip if last_seen_ms missing or empty
      skip if now_ms - last_seen_ms < SIGNAL_LOSS_THRESHOLD_MS
    EXISTS alert-state:{entity_id}
      skip if present
    HSET alert-state:{entity_id}
          dark_since_ms        = last_seen_ms
          signal_loss_alert_id = alert_id
          composite_issued     = 0
    Kafka produce -> alerts topic
          alert_id       = {entity_id}:SIGNAL_LOSS:{dark_since_ms}
          entity_id
          entity_type    (from Redis hash)
          alert_type     = SIGNAL_LOSS
          priority       = STANDARD
          status         = NEW
          detected_at_ms = Date.now()
          payload.dark_since_ms
          payload.last_known_lat / lon / altitude_m / speed_mps / course_deg
```

---

## Manual inspection commands

Run these after CP2 is wired up and the stack is running.

```bash
# 1. Confirm scan ticks appear every 30s
docker logs -f sentinel-alert-evaluator

# 2. Force a dark entity by writing a stale last_seen_ms
docker exec sentinel-redis redis-cli HSET entity:live:test123 \
  last_seen_ms 0 entity_type aircraft on_ground '' \
  lat 51.5 lon -0.1 altitude_m 10000 speed_mps 200 course_deg 270

# 3. Wait for the next scan tick. Confirm alert-state was written.
docker exec sentinel-redis redis-cli HGETALL "alert-state:test123"
# Expected: dark_since_ms 0  signal_loss_alert_id test123:SIGNAL_LOSS:0  composite_issued 0

# 4. Confirm the alert landed on Kafka
docker exec sentinel-redpanda rpk topic consume alerts --offset start -n 5

# 5. Confirm repeated scans do NOT emit a second message
#    Run step 4 again after two more scan ticks; count should not increase

# 6. Confirm deterministic alert_id matches
#    alert_id in the Kafka message should be: test123:SIGNAL_LOSS:0

# 7. Clear the gate and observe re-detection
docker exec sentinel-redis redis-cli DEL "alert-state:test123"
#    Wait for next scan; a new Kafka message appears with the same alert_id.
#    The API ON CONFLICT DO NOTHING absorbs the duplicate at the DB layer.
```

---

## Map mental model to code

| Concept | Where in code |
| --- | --- |
| Scan interval | `SCAN_INTERVAL_MS = 30_000` — `evaluator.ts` |
| Silence threshold | `SIGNAL_LOSS_THRESHOLD_MS = 300_000` — `evaluator.ts` |
| On-ground skip | `entity.on_ground === 'true'` check in `runScan` |
| Episode gate check | `redis.exists('alert-state:...')` in `runScan` |
| Gate write | `redis.hset('alert-state:...')` in `runScan` |
| Deterministic alert_id | `` `${entityId}:SIGNAL_LOSS:${darkSinceMs}` `` in `runScan` |
| Kafka produce | `producer.send({ topic: 'alerts', ... })` in `runScan` |
| detected_at_ms | `Date.now()` — processing time, explicitly allowed here |

---

## Retention questions

1. Why is `dark_since_ms = last_seen_ms` rather than `dark_since_ms = Date.now()`?
2. An entity has been dark for 10 minutes. Five scan ticks have fired. How many Kafka messages are on the `alerts` topic for this entity? Why?
3. The evaluator crashes immediately after writing `alert-state:{entity_id}` but before calling `producer.send`. What happens to this entity's alerting? Is this the correct trade-off?
4. Why is no TTL set on `alert-state:{entity_id}`? What would happen if a short TTL (e.g. 60s) were set?
5. Two evaluator instances are both in a brief split-brain window and both scan the same dark entity. Walk through every write that happens and explain why there is only one durable alert row in TimescaleDB.
6. `detected_at_ms` uses `Date.now()` (processing time). Why is this one of the only places in Sentinel where processing time is correct for this field?
7. An entity has `on_ground = ''` (empty string). Does the scan include or exclude it, and why?
8. What is the difference between the Redis 24h TTL on `entity:live:*` and the `SIGNAL_LOSS_THRESHOLD_MS`? Could you use the TTL expiry as the detection mechanism instead?

---

## CP2 completion checklist

- [ ] I can explain why the episode gate is written before the Kafka produce, not after
- [ ] I can state the deterministic `alert_id` format and explain why two episodes for the same entity produce different IDs
- [ ] I can trace what happens when two instances scan the same dark entity simultaneously
- [ ] I can explain why `detected_at_ms` is processing time but `dark_since_ms` is source event time
- [ ] I can describe what Redis SCAN returns and why it is not atomic
- [ ] I can run the manual inspection commands and interpret the output
- [ ] I can explain why on-ground entities with unknown ground state (`on_ground = ''`) are included in detection
