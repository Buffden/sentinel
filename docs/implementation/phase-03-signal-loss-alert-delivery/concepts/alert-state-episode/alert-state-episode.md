# Alert-State Episode Lifecycle

CP2 proved one alert fires per dark period. CP3 verifies the full cycle: dark → alerted → resumed → dark again → new alert with a new identity.

---

## The four states

![Episode State Machine](../../../../../../diagrams/docs/implementation/phase-03-signal-loss-alert-delivery/concepts/alert-state-episode/episode-state-machine.svg)

```
TRACKING  entity sending positions within threshold
LOST      silent beyond threshold, not yet alerted (evaluator-only, no Redis key)
ALERTED   alert emitted; alert-state:{entity_id} exists
RECOVERED gate deleted, recent-loss written; next scan re-enters TRACKING or LOST
```

Only ALERTED and RECOVERED have durable Redis keys. Everything else is derived from `last_seen_ms` at scan time.

---

## Episode identity: why `dark_since_ms`, not a counter

`dark_since_ms = last_seen_ms` at detection time. Two episodes for the same entity have different `dark_since_ms` values because a resume updates `last_seen_ms` to a later timestamp.

A counter would require atomic coordination across evaluator instances. `dark_since_ms` is self-contained: two instances scanning the same dark entity compute the same value from the same Redis hash, producing the same `alert_id`. Duplicates collapse at the API's `ON CONFLICT DO NOTHING`.

---

## `alert-state:{entity_id}` fields

| Field | Value |
|---|---|
| `dark_since_ms` | `last_seen_ms` at detection |
| `signal_loss_alert_id` | `{entity_id}:SIGNAL_LOSS:{dark_since_ms}` |
| `composite_issued` | `0` (Phase 06 sets to `1`) |

No TTL. Persists until the Position Consumer deletes it on resume.

---

## Episode open (evaluator)

```
EXISTS alert-state:{entity_id}  → skip if present
HSET   alert-state:{entity_id}  dark_since_ms, signal_loss_alert_id, composite_issued=0
Kafka produce → alerts           alert_id = {entity_id}:SIGNAL_LOSS:{dark_since_ms}
```

Gate is written before Kafka produce. Crash between the two: entity misses this episode's alert. Accepted trade-off over the alternative (re-emit on every scan tick).

---

## Episode close (position consumer, on accepted resume)

```
HSET recent-loss:{entity_id}    dark_since_ms, resumed_at_ms, signal_loss_alert_id
DEL  alert-state:{entity_id}
```

`recent-loss` first, `alert-state` second. Crash between the two: both keys exist. Next accepted position for this entity overwrites `recent-loss` (idempotent) and deletes `alert-state`. Cleanup completes on the second attempt.

`resumed_at_ms` = `position.timestamp_ms` (source event time, not `Date.now()`). A Kafka replay hours later would give a misleading time if processing time were used.

---

## Why `recent-loss` exists

Phase 06 (composite correlation) needs to know: did this entity recently recover from a signal-loss episode, and is it now near a suspicious counterpart? It needs `dark_since_ms`, `resumed_at_ms`, and `signal_loss_alert_id`. Writing it here costs nothing — the data is already in memory during the resume handler.

Only the most recent episode is kept. Earlier episodes are in the `alerts` table.

---

## How a second episode gets a new `alert_id`

Resume sets `last_seen_ms = T2` (new timestamp). Next silence: `dark_since_ms = T2`. `alert_id = entityId:SIGNAL_LOSS:T2`. Different from episode 1's `entityId:SIGNAL_LOSS:T1` because `T2 > T1` (monotonic guard enforces this — only newer positions are accepted).

---

## Failure modes

| Scenario | Result |
|---|---|
| Crash after gate write, before Kafka produce | Entity misses alert for this episode; must resume and go dark again |
| Crash after `recent-loss` write, before `DEL alert-state` | Next accepted position completes the cleanup |
| Two instances scan same dark entity (split-brain) | Same `dark_since_ms` → same `alert_id` → `ON CONFLICT DO NOTHING` at API |
| Stale Kafka message replayed after resume | Monotonic guard rejects it; gate not cleared |

---

## Manual inspection

```bash
# Seed a dark entity
docker exec sentinel-redis redis-cli HSET entity:live:probe1 \
  last_seen_ms 0 entity_type aircraft on_ground '' \
  lat 51.5 lon -0.1 altitude_m 10000 speed_mps 200 course_deg 270

# After scan fires — confirm gate
docker exec sentinel-redis redis-cli HGETALL "alert-state:probe1"

# Resume
TIME_NOW=$(node -e "console.log(Math.floor(Date.now()/1000))")
echo "{\"icao24\":\"probe1\",\"time_position\":$TIME_NOW,\"lat\":51.7,\"lon\":-0.3,\"baro_altitude\":10500,\"velocity\":210,\"true_track\":280,\"vertical_rate\":0,\"on_ground\":false,\"callsign\":\"PROBE1\"}" | \
  docker exec -i sentinel-redpanda rpk topic produce adsb.raw --key probe1 --compression none

# Confirm gate deleted, recent-loss written
docker exec sentinel-redis redis-cli EXISTS "alert-state:probe1"       # 0
docker exec sentinel-redis redis-cli HGETALL "recent-loss:probe1"

# After episode 2 fires — confirm new alert_id on Kafka
docker exec sentinel-redpanda rpk topic consume alerts -o 0 -n 5 2>&1
```

---

## Retention questions

1. An entity resumes. The evaluator scans again before the next position arrives, and the entity's new `last_seen_ms` is already beyond the threshold. What happens?
2. The consumer crashes after writing `recent-loss` but before deleting `alert-state`. Describe the Redis state and the recovery path.
3. Why can't two episodes for the same entity produce the same `alert_id`, assuming the monotonic guard is working?
4. `composite_issued = '0'` is written during episode open. Why write it now rather than in Phase 06?

---

## Completion checklist

- [ ] I can draw the four states and name the Redis key for each durable one
- [ ] I can explain why `dark_since_ms` is the episode anchor rather than a counter
- [ ] I can trace all Redis writes and deletes across a full episode cycle
- [ ] I can explain the crash-between-two-writes scenario and the recovery path
- [ ] I can explain why `resumed_at_ms` uses source event time
- [ ] I can run the inspection commands and verify all three states
