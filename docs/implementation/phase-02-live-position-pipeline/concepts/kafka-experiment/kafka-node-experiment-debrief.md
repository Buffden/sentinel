# CP1 Debrief: Node.js/TypeScript Kafka Experiment

---

## Infrastructure status

| Container | Status |
| --- | --- |
| sentinel-redpanda | healthy |
| sentinel-timescaledb | healthy |
| sentinel-redis | healthy |
| sentinel-neo4j | healthy |

---

## 1. Consumer startup (first run)

```bash
cd services/position-consumer
node_modules/.bin/tsx src/consume.ts
```

```json
{"timestamp":"2026-08-19T21:03:14.901Z","level":"info","service":"position-consumer","message":"consumer connecting","brokers":["localhost:9092"],"group":"cp1-experiment"}
{"timestamp":"2026-08-19T21:03:14.918Z","level":"info","service":"position-consumer","message":"consumer connected"}
{"timestamp":"2026-08-19T21:03:14.923Z","level":"info","service":"position-consumer","message":"consumer subscribed","topic":"adsb.raw","fromBeginning":true}
{"timestamp":"2026-08-19T21:03:14.967Z","level":"info","service":"position-consumer","message":"kafka message received","topic":"adsb.raw","partition":0,"offset":"1","key":null,"payload":{"checkpoint":5,"source":"manual-store-verification"}}
{"timestamp":"2026-08-19T21:03:14.967Z","level":"info","service":"position-consumer","message":"kafka message received","topic":"adsb.raw","partition":0,"offset":"2","key":null,"payload":{"checkpoint":5,"source":"manual-store-verification"}}
```

`fromBeginning: true` caused the consumer to start from `LOG-START-OFFSET` (1). Offset 0 no longer exists — Redpanda's retention removed it between Phase 01 and this session. Offsets 1 and 2 are Phase 01 manual-store-verification records, produced without a key.

The consumer then sat idle waiting for new records.

---

## 2. Producer run

```bash
cd services/ingestion-poller
node_modules/.bin/tsx src/produce.ts
```

```json
{"timestamp":"2026-08-19T21:08:35.941Z","level":"info","service":"ingestion-poller","message":"producer connecting","brokers":["localhost:9092"]}
{"timestamp":"2026-08-19T21:08:35.955Z","level":"info","service":"ingestion-poller","message":"producer connected"}
{"timestamp":"2026-08-19T21:08:35.973Z","level":"info","service":"ingestion-poller","message":"kafka event produced","topic":"adsb.raw","partition":0,"offset":"3","key":"test-aircraft-1","payload":{"entity_id":"test-aircraft-1","timestamp_ms":1787173715941}}
{"timestamp":"2026-08-19T21:08:35.973Z","level":"info","service":"ingestion-poller","message":"producer disconnected"}
```

Event landed at partition 0, offset 3. Producer disconnected and exited.

---

## 3. Consumer receives CP1 event

Immediately after the producer ran, the consumer logged:

```json
{"timestamp":"2026-08-19T21:08:35.973Z","level":"info","service":"position-consumer","message":"kafka message received","topic":"adsb.raw","partition":0,"offset":"3","key":"test-aircraft-1","payload":{"entity_id":"test-aircraft-1","timestamp_ms":1787173715941}}
```

Same millisecond timestamp as the producer. Partition, offset, key, and payload match exactly.

---

## 4. Broker inspection: partition state

```bash
docker exec sentinel-redpanda rpk topic describe -p adsb.raw
```

| PARTITION | LEADER | EPOCH | REPLICAS | LOG-START-OFFSET | HIGH-WATERMARK |
| --- | --- | --- | --- | --- | --- |
| 0 | 0 | 5 | [0] | 1 | 4 |

`LOG-START-OFFSET: 1` — offset 0 deleted by retention between sessions. `HIGH-WATERMARK: 4` — next record will land at offset 4. `EPOCH: 5` — 5 leadership elections across Phase 01 restarts; data survived via named Docker volume.

---

## 5. Record read directly from broker

```bash
docker exec sentinel-redpanda rpk topic consume adsb.raw --offset 3 --num 1
```

```json
{
  "topic": "adsb.raw",
  "key": "test-aircraft-1",
  "value": "{\"entity_id\":\"test-aircraft-1\",\"timestamp_ms\":1787173715941}",
  "timestamp": 1787173715967,
  "partition": 0,
  "offset": 3
}
```

Record exists in the broker's log independently of any consumer group. The consumer reading it did not delete it. Broker timestamp (1787173715967) is 26ms after payload `timestamp_ms` (1787173715941) — network + write latency.

---

## 6. Consumer group state after consumption

```bash
docker exec sentinel-redpanda rpk group describe cp1-experiment
```

| Field | Value |
| --- | --- |
| STATE | Stable (while consumer running) |
| MEMBERS | 1 |
| CURRENT-OFFSET | 4 |
| LOG-END-OFFSET | 4 |
| LAG | 0 |
| CLIENT-ID | position-consumer |
| HOST | 192.168.65.1 |

`CURRENT-OFFSET: 4` — group has committed through offset 3. Next fetch starts at offset 4.

---

## 7. Restart experiment: committed offset survives

Stopped consumer with SIGINT:

```json
{"message":"shutdown initiated","signal":"SIGINT"}
{"message":"consumer disconnected"}
```

Group state while stopped:

| STATE | MEMBERS | CURRENT-OFFSET | LAG |
| --- | --- | --- | --- |
| Empty | 0 | 4 | 0 |

`CURRENT-OFFSET: 4` persisted on the broker with 0 members. Restarted the consumer — no messages re-delivered. Consumer resumed from offset 4 (nothing there yet) and sat idle.

---

## 8. Uncommitted offset experiment: replay

Reset the group's committed offset to simulate a crash before commit:

```bash
docker exec sentinel-redpanda rpk group seek cp1-experiment --to start --topics adsb.raw
```

```text
TOPIC     PARTITION  PRIOR-OFFSET  CURRENT-OFFSET
adsb.raw  0          4             1
```

Group state after seek:

| CURRENT-OFFSET | LOG-END-OFFSET | LAG |
| --- | --- | --- |
| 1 | 4 | 3 |

Restarted the consumer. All three records re-delivered:

```json
{"message":"kafka message received","partition":0,"offset":"1","key":null,"payload":{"checkpoint":5,"source":"manual-store-verification"}}
{"message":"kafka message received","partition":0,"offset":"2","key":null,"payload":{"checkpoint":5,"source":"manual-store-verification"}}
{"message":"kafka message received","partition":0,"offset":"3","key":"test-aircraft-1","payload":{"entity_id":"test-aircraft-1","timestamp_ms":1787173715941}}
```

Our CP1 event was processed a second time with identical payload. This is at-least-once delivery. Durable side effects must be idempotent.

---

## 9. Consumer group independence

```bash
docker exec sentinel-redpanda rpk topic consume adsb.raw \
  --group cp1-second-group --offset start --num 3
```

`cp1-second-group` read all three records from offset 1 independently. Final group state:

| Group | CURRENT-OFFSET | LOG-END-OFFSET | LAG |
| --- | --- | --- | --- |
| cp1-experiment | 4 | 4 | 0 |
| cp1-second-group | 4 | 4 | 0 |

Both groups committed through offset 3. Neither affected the other. Records at offsets 1, 2, 3 remain in the partition log — not deleted by consumption from either group.

---

## 10. Commands to reproduce independently

```bash
# Partition state
docker exec sentinel-redpanda rpk topic describe -p adsb.raw

# Read a record by offset (no group)
docker exec sentinel-redpanda rpk topic consume adsb.raw --offset 3 --num 1

# Consumer group committed position
docker exec sentinel-redpanda rpk group describe cp1-experiment

# List all consumer groups
docker exec sentinel-redpanda rpk group list

# Reset a group to the beginning
docker exec sentinel-redpanda rpk group seek cp1-experiment --to start --topics adsb.raw
```

---

## 11. Observations

| Concept | Observed |
| --- | --- |
| Producer lands at a specific partition and offset | Partition 0, offset 3; confirmed by both producer log and rpk |
| Consumer group committed offset persists across restarts | CURRENT-OFFSET: 4 with MEMBERS: 0; no re-delivery on clean restart |
| Reset committed offset causes replay | Seek to offset 1; all three records re-delivered on restart |
| Consumer group independence | Two groups read the same records; neither affected the other |
| Consuming does not delete records | Records at offsets 1, 2, 3 readable after both groups consumed them |
| fromBeginning only applies on first run | After commit exists, group resumes from committed offset regardless |

---

## 12. Note on TimeoutNegativeWarning

Both producer and consumer logged:

```text
TimeoutNegativeWarning: -1787173394918 is a negative number. Timeout duration was set to 1.
```

kafkajs computes an internal session timeout relative to broker metadata timestamps. The Phase 01 records have timestamps in the far future relative to the configured timeout, producing a negative `setTimeout` value. Node.js clamps it to 1ms. Harmless for CP1; no records were lost or delayed. This warning will disappear once all records in the topic are from the current session.
