# Alert Sink — Design and Learning Reference

CP5 wires the API to the `alerts` Kafka topic: consume each alert message, persist it idempotently to TimescaleDB, publish to Redis `alert-events`, commit the offset, and serve open alerts via `GET /alerts`. The WebSocket fan-out to browser clients is built in CP6.

---

## What this checkpoint adds

The Alert Evaluator already publishes deterministic `SIGNAL_LOSS` alerts to Kafka. CP5 closes the loop on the server side:

1. Kafka consumer (group `api`) reading from the `alerts` topic.
2. Idempotent `INSERT INTO alerts ON CONFLICT (alert_id) DO NOTHING`.
3. Redis `PUBLISH alert-events` after the DB write.
4. Kafka offset commit after both succeed.
5. `GET /alerts` returning open alerts for dashboard hydration.

---

## Concepts in plain language

### 1. Why the API consumes from Kafka instead of receiving HTTP pushes

The Alert Evaluator does not know the API exists. It publishes to a Kafka topic and moves on. Kafka buffers the messages durably. The API pulls from Kafka at its own pace.

This means: if the API restarts, crashes, or redeploys, Kafka holds the messages. On restart the API resumes from its last committed offset and processes everything it missed. No alerts are lost permanently.

Direct HTTP push from evaluator to API would require the evaluator to know the API's address, handle retries on failure, and give up eventually. Kafka removes all of that.

### 2. Why the commit happens last

The processing order per message is:

1. Parse and validate
2. `INSERT INTO alerts ON CONFLICT (alert_id) DO NOTHING`
3. `PUBLISH alert-events` to Redis
4. `commitOffsets()`

If the API crashes at step 2 or 3 before committing, Kafka redelivers the message on restart. The DB insert is idempotent — the second attempt hits `ON CONFLICT DO NOTHING` and produces no duplicate row. The Redis publish may happen twice, but WebSocket clients deduplicate by `alert_id`.

If the commit happened first (before the DB write), a crash after the commit would mean the offset advances but the alert is never written. That alert would be permanently lost — Kafka would not redeliver it. Commit-last is the only safe order.

### 3. What `ON CONFLICT (alert_id) DO NOTHING` actually does

`alert_id` is the primary key on the `alerts` table. A primary key constraint is unique. When you `INSERT` a row with a duplicate `alert_id`, Postgres detects the conflict and silently skips the insert rather than returning an error. The row already in the table is unchanged. The application sees `INSERT 0 0` (0 rows inserted, 0 rows affected) rather than a unique violation error.

This makes every insert safe to run multiple times. Replay a Kafka partition from offset 0 and all the rows already in the table are untouched. Only new alert IDs produce new rows.

### 4. Why Redis `PUBLISH` before offset commit

Publishing to `alert-events` before committing the offset means: if the API crashes between the publish and the commit, Kafka redelivers and the publish happens again. Clients receive the same alert event twice. They deduplicate by `alert_id` — this is the accepted at-least-once delivery model for the WebSocket feed.

Publishing after the offset commit would mean: if the API crashes between commit and publish, the alert is persisted in TimescaleDB but the WebSocket clients never receive the live event. They would only see it on the next `GET /alerts` hydration. That is a worse outcome for a live alert system — a missed real-time event is harder to recover from than a duplicate.

### 5. What `GET /alerts` is for

It returns all `NEW` and `ACKNOWLEDGED` alerts from TimescaleDB, ordered by `detected_at DESC`. This is the hydration endpoint. The dashboard calls it on page load and on every WebSocket reconnect to recover any alerts missed during a disconnected window. It is not the live delivery path — that is the `alert-events` pub/sub channel.

### 6. Group `api` — what the consumer group means

KafkaJS maintains a committed offset per consumer group per topic partition. The group `api` is the API's identity with Kafka. On restart, the API rejoins the group, Kafka assigns the partitions back, and the consumer starts from the last committed offset. No messages are re-read unless the offset was not committed before the crash.

---

## Data flow

See [`alert-sink-flow.puml`](alert-sink-flow.puml) for the full sequence diagram.

The Alert Evaluator produces to the `alerts` Kafka topic. The API consumer (group `api`) polls each message, parses it, writes to TimescaleDB with `ON CONFLICT (alert_id) DO NOTHING`, publishes to Redis `alert-events`, then commits the offset.

`GET /alerts` is the hydration path: the dashboard calls it on load and reconnect, querying TimescaleDB for all `NEW` and `ACKNOWLEDGED` alerts ordered by `detected_at DESC`.

---

## Guarantee

Every alert written to TimescaleDB has a unique `alert_id`. Replaying the same Kafka message any number of times produces exactly one durable row per `alert_id`. The Redis publish and downstream WebSocket delivery are at-least-once; clients deduplicate by `alert_id`.

---

## Failure modes

| Scenario | Result |
|---|---|
| API crashes after DB write, before offset commit | Kafka redelivers; `ON CONFLICT DO NOTHING`; no duplicate row; Redis publish may repeat |
| API crashes before DB write | Kafka redelivers; alert written on restart; no data loss |
| Redis `PUBLISH` fails | Alert is in TimescaleDB; real-time WS event lost for this delivery; client recovers via `GET /alerts` on next reconnect |
| Two Alert Evaluator instances both publish the same `alert_id` | Both messages consumed; second insert hits `ON CONFLICT DO NOTHING`; one row in DB |
| Kafka topic has no messages | Consumer polls indefinitely; no alerts; no error |
| `alerts` table missing | Consumer crashes on first insert; structured error logged; restart after migration |

---

## Environment variables

| Variable | Purpose |
|---|---|
| `KAFKA_BROKERS` | Broker address for the alert sink consumer |
| `PG_URL` | TimescaleDB connection string |
| `REDIS_URL` | Redis connection string for `PUBLISH alert-events` |

---

## Manual inspection

After a `SIGNAL_LOSS` alert flows through:

1. Confirm the alert row landed in TimescaleDB:
   ```bash
   psql postgres://sentinel:sentinel-dev@localhost:5433/sentinel \
     -c "SELECT alert_id, alert_type, status, detected_at FROM alerts ORDER BY detected_at DESC LIMIT 5;"
   ```

2. Confirm no duplicates after replay:
   ```bash
   psql postgres://sentinel:sentinel-dev@localhost:5433/sentinel \
     -c "SELECT COUNT(*), COUNT(DISTINCT alert_id) FROM alerts;"
   ```
   Both counts must be equal.

3. Watch Redis `alert-events` in real time while an alert is consumed:
   ```bash
   redis-cli SUBSCRIBE alert-events
   ```

4. Check the consumer group offset to confirm it advanced:
   ```bash
   docker exec sentinel-redpanda rpk group describe api
   ```
   `LAG` should be 0 after the consumer processes all messages.

5. Replay idempotency test: reset the `api` group offset to 0, restart the API, confirm the `alerts` row count does not increase:
   ```bash
   docker exec sentinel-redpanda rpk group seek api --to start --topics alerts
   # restart API
   psql postgres://sentinel:sentinel-dev@localhost:5433/sentinel \
     -c "SELECT COUNT(*) FROM alerts;"
   # count must match before-replay count
   ```

6. Verify `GET /alerts` returns the persisted alert:
   ```bash
   curl -s --cookie "sentinel_jwt=<token>" http://localhost:3000/alerts | jq .
   ```

---

## Map mental model to code

| Concept | Where in code |
|---|---|
| Kafka consumer setup | `kafkajs` `Kafka`, `consumer.connect()`, `consumer.subscribe({ topic: 'alerts' })` |
| Per-message handler | `consumer.run({ eachMessage })` in `src/sink/alertSink.ts` |
| Idempotent DB insert | `INSERT INTO alerts ... ON CONFLICT (alert_id) DO NOTHING` |
| Redis publish | `redis.publish('alert-events', JSON.stringify(alert))` |
| Offset commit | `consumer.commitOffsets([{ topic, partition, offset }])` after DB + Redis |
| `GET /alerts` | `SELECT * FROM alerts WHERE status IN ('NEW','ACKNOWLEDGED') ORDER BY detected_at DESC` |

---

## Retention questions

1. The API crashes between the DB write and the Kafka offset commit. Walk through exactly what happens on restart: which steps repeat, what the DB state looks like, and what the Redis and WebSocket clients see.
2. Why is `ON CONFLICT (alert_id) DO NOTHING` safe for replay but `ON CONFLICT DO UPDATE` would not be appropriate here?
3. The `GET /alerts` endpoint and the `alert-events` pub/sub channel both deliver alerts. What is each one for and when does the dashboard use each?
4. Two instances of the Alert Evaluator both emit the same `alert_id` to Kafka (duplicate publish, possible during a brief dual-leader window). Trace the message from Kafka through to TimescaleDB. How many rows end up in the `alerts` table?
5. Why does committing the Kafka offset before the DB write make the system unsound, even if the DB write succeeds most of the time?

---

## Completion checklist

- [ ] I can explain why the Kafka consumer commits the offset last
- [ ] I can trace a crash at each step and describe what Kafka, TimescaleDB, and Redis each contain on restart
- [ ] I can verify idempotency by replaying from offset 0 and confirming the row count does not change
- [ ] I can explain what `ON CONFLICT (alert_id) DO NOTHING` does at the Postgres level
- [ ] I can observe the `alert-events` Redis channel receive a publish while the consumer processes a message
- [ ] I can explain the difference between `GET /alerts` (hydration) and `alert-events` (live delivery)
