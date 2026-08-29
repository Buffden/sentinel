# Alert Sink Implementation Debrief

---

## Setup

```bash
# Ensure infra is running
docker compose up -d

# Terminal 1: start the API (from services/api)
cd services/api
node_modules/.bin/tsx --env-file=.env src/index.ts
```

Expected startup output:

```
{"level":"info","msg":"API listening","port":3000}
{"level":"info","msg":"alert sink consumer started","brokers":["localhost:9092"],"topic":"alerts","group":"api"}
{"level":"INFO",...,"message":"[Consumer] Starting","groupId":"api"}
{"level":"INFO",...,"message":"[ConsumerGroup] Consumer has joined the group","groupId":"api",...}
```

The consumer must join the group before any message is processed. Confirm this before producing test alerts.

---

## Implementation issues encountered

### 1. File writes going to wrong path

**Symptom:** The API started and printed `API listening` but the `startAlertSink` trace logs never appeared. `rpk group describe api` showed `STATE: Dead` with 0 members even after producing messages.

**Root cause:** The tool writing files used a path relative to the project root (`/sentinel`), but the shell working directory had drifted to `services/api` during `pnpm install`. When the server ran `src/index.ts`, it loaded the old version of the file — the new imports for `alertsRouter` and `startAlertSink` were written to the correct path on disk, but `index.ts` itself was not updated. The running server imported nothing new.

**How it was found:** `cat /Users/.../services/api/src/index.ts` showed the old file content, even though the editor view showed the new content.

**Fix:** Write files using absolute paths. Verify with `cat <absolute_path>` before running the server. Never assume a file write succeeded without checking what's actually on disk.

---

### 2. `@types/ioredis` is deprecated

**Symptom:** TypeScript error: `This expression is not constructable` when using `import Redis from 'ioredis'`.

**Root cause:** ioredis v6 ships its own TypeScript types. `@types/ioredis` is a deprecated stub for an older API. The default export no longer matches.

**Fix:** Use the named export:

```typescript
import { Redis } from 'ioredis';
```

Do not install `@types/ioredis` — ioredis v6 types are bundled.

---

### 3. `TimeoutNegativeWarning` from KafkaJS

**Symptom:** On startup: `TimeoutNegativeWarning: -1787982159660 is a negative number. Timeout duration was set to 1.`

**Root cause:** KafkaJS computes an internal timeout using `sessionTimeout - heartbeatInterval`. With the system date set to 2026, some internal timestamp arithmetic produces a large negative number. KafkaJS clamps it to 1ms internally.

**Impact:** None. The consumer connects, subscribes, joins the group, and processes messages normally. This warning can be ignored in this environment.

---

## Experiment 1: alert consumed and persisted

Produced a test SIGNAL_LOSS alert directly to the `alerts` topic:

```bash
echo '{"alert_id":"test-entity-99:SIGNAL_LOSS:1787634000000","entity_id":"test-entity-99","entity_type":"AIRCRAFT","alert_type":"SIGNAL_LOSS","priority":"STANDARD","status":"NEW","detected_at_ms":1787634060000,"payload":{"dark_since_ms":1787634000000,"last_known_lat":51.5,"last_known_lon":-0.1,"last_known_altitude_m":10150,"last_known_speed_mps":220.5,"last_known_course_deg":270}}' \
  | docker exec -i sentinel-redpanda rpk topic produce alerts --compression none
```

Observed API log:

```json
{"level":"info","msg":"alert sinked","alert_id":"test-entity-99:SIGNAL_LOSS:1787634000000","alert_type":"SIGNAL_LOSS"}
```

Verify TimescaleDB row:

```bash
psql postgres://sentinel:sentinel-dev@localhost:5433/sentinel \
  -c "SELECT alert_id, alert_type, status, detected_at FROM alerts ORDER BY detected_at DESC LIMIT 5;"
```

Verify no duplicates:

```bash
psql postgres://sentinel:sentinel-dev@localhost:5433/sentinel \
  -c "SELECT COUNT(*), COUNT(DISTINCT alert_id) FROM alerts;"
# Both counts must be equal.
```

Verify consumer group offset advanced:

```bash
docker exec sentinel-redpanda rpk group describe api
# LAG should be 0 after the message is processed.
```

---

## Experiment 2: malformed message — validation skip

During replay, offset 5 contained `{"test":true}` — a message produced during an earlier isolated KafkaJS test. It parsed as valid JSON but had no `alert_id`, `entity_id`, or `detected_at_ms`. `new Date(undefined)` → invalid timestamp → Postgres threw `22007 invalid input syntax for type timestamp`. KafkaJS retried the message indefinitely.

**Finding:** the try/catch in `eachMessage` only covered JSON parsing. Valid JSON with missing/invalid fields reached the DB and caused infinite retry.

**Fix:** added a validation step after parsing that checks all required fields are present and `detected_at_ms` is a finite number. Invalid messages are committed and skipped, same as JSON parse failures.

Observed log after fix:

```json
{"level":"error","msg":"alert validation failed — skipping","raw":"{\"test\":true}"}
```

The consumer advanced past offset 5 and continued processing normally.

---

## Experiment 3: idempotency under replay

Reset the consumer group offset to 0 and restart the API. The same messages are redelivered. Confirm the `alerts` row count does not increase:

```bash
# Kill API, wait ~35s for session to expire, then seek
docker exec sentinel-redpanda rpk group seek api --to start --topics alerts

# Restart API and wait for consumer to process
# Check count — must match pre-replay count
psql postgres://sentinel:sentinel-dev@localhost:5433/sentinel \
  -c "SELECT COUNT(*), COUNT(DISTINCT alert_id) FROM alerts;"
```

Observed: ran two full replays from offset 0. Row count was 7 before and after each replay. `ON CONFLICT (alert_id) DO NOTHING` returned `INSERT 0 0` for every already-known `alert_id`.

Note: `rpk group seek` requires the consumer group to be in `Empty` state. Kill the API and wait ~35s for the KafkaJS session timeout to expire before seeking.

---

## Experiment 4: `GET /alerts` returns persisted rows

```bash
curl -s --cookie "sentinel_jwt=<token>" http://localhost:3000/alerts | jq .
```

Response must include the test alert with `status: NEW`.

---

## Experiment 5: Redis `alert-events` publish

Open a Redis subscriber before producing:

```bash
redis-cli SUBSCRIBE alert-events
```

Produce an alert. Confirm the raw JSON appears in the subscriber output immediately after the `alert sinked` log. This is the pub/sub delivery that CP6 will fan out to WebSocket clients.

---

## Engineering debrief

**Data flow:** KafkaJS consumer (group `api`) polls the `alerts` topic. For each message: parse JSON, `INSERT INTO alerts ... ON CONFLICT (alert_id) DO NOTHING`, `PUBLISH alert-events` to Redis, `commitOffsets`. Commit is last — a crash before it causes safe Kafka redeliver; the idempotent insert absorbs the duplicate.

**Main trade-off:** commit-last means Redis publish and WebSocket delivery are at-least-once. A client may receive the same alert event twice. Clients must deduplicate by `alert_id`. This is the accepted model for v1.

**`GET /alerts`** queries TimescaleDB directly for all `NEW` and `ACKNOWLEDGED` rows. It is the hydration path, not the live path. The live path is `alert-events` pub/sub, consumed in CP6.

---

## Key observations

| Concept | Observed |
| --- | --- |
| Consumer joins group on startup | PASS — `STATE: Stable`, 1 member, `[ConsumerGroup] Consumer has joined the group` |
| `alert sinked` log on message consume | PASS |
| TimescaleDB row written with correct fields | PASS — `SELECT alert_id, alert_type, status, detected_at` returned expected row |
| Consumer group LAG = 0 after processing | PASS — `rpk group describe api` showed `LAG: 0` |
| `ON CONFLICT (alert_id) DO NOTHING` — no duplicate on two full replays | PASS — 7 rows, 7 distinct, unchanged across both replays |
| Malformed JSON → parse error → skip + commit | PASS |
| Valid JSON with missing fields → validation error → skip + commit | PASS — found and fixed during testing |
| KafkaJS session timeout must expire before `rpk group seek` | PASS — seek fails with `INVALID_OPERATION` if group is non-empty; wait ~35s |

---

## Dev environment note: always use `--compression none` with rpk

```bash
# Correct
echo '...' | docker exec -i sentinel-redpanda rpk topic produce alerts --compression none

# Wrong — Redpanda defaults to Snappy; KafkaJS v2.2.4 has no Snappy decoder
echo '...' | docker exec -i sentinel-redpanda rpk topic produce alerts
```

KafkaJS silently stops the polling loop on a Snappy-compressed batch. The consumer appears healthy but never delivers messages.
