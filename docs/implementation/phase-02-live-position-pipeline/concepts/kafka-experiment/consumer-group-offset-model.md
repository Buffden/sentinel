# Consumer Group and Offset Model

## Three distinct offsets

| Offset concept | What it is | Where it lives | Survives crash? |
| --- | --- | --- | --- |
| **Record offset** | Fixed position of a record in a partition. Assigned by the broker on write. Never changes. | Broker partition log | N/A — immutable |
| **Consumer current position** | Where the consumer is reading right now. Advances as records are fetched. | Consumer process memory | No |
| **Committed consumer-group offset** | Position stored on the broker for a named group. The resume point after restart. | Broker (`__consumer_offsets`) | Yes |

```text
Partition 0 of adsb.raw:
  [0] {"entity_id": "test-aircraft-1", "timestamp_ms": ...}
  [1] {"entity_id": "test-aircraft-2", "timestamp_ms": ...}
  [2] ...
```

---

## autoCommit

kafkajs default: `autoCommit: true`. After each `eachMessage` handler returns without throwing, kafkajs commits the offset on a background timer (default 5 s).

If the process crashes after processing a record but before the timer fires, the offset is not committed. On restart the consumer re-reads from the last committed offset — the record is processed again.

---

## fromBeginning

```typescript
await consumer.subscribe({ topic: TOPIC, fromBeginning: true });
```

On first run, the group has no committed offset. `fromBeginning: true` → start from offset 0. Without it, the consumer starts from the latest offset and misses records produced before it started.

After the first commit, `fromBeginning` has no effect — the group resumes from its committed offset.

---

## Restart behavior

Normal (offset committed before stop):

```text
consumer reads offset 0 → handler returns → autoCommit fires → CURRENT-OFFSET = 1
consumer stops → restarts → resumes from offset 1 → offset 0 not re-delivered
```

Crash before commit:

```text
consumer reads offset 0 → handler returns → process crashes before autoCommit
CURRENT-OFFSET still = 0 → consumer restarts → reads offset 0 again → re-delivered
```

Re-delivery is the at-least-once guarantee. All durable side effects must be idempotent.

---

## Consumer group independence

Each consumer group maintains its own committed offset. One group consuming a record has no effect on any other group.

```text
adsb.raw partition 0:
  [0]  {"entity_id": "test-aircraft-1", ...}

cp1-experiment:    CURRENT-OFFSET = 1  (consumed offset 0)
cp1-second-group:  CURRENT-OFFSET = 0  (has not consumed anything)
```

Records are not deleted on consumption. Deletion occurs only when the retention window expires. Consumers are independent pointers into a retained log.

This enables:

| Property | Sentinel use |
| --- | --- |
| Fan-out | `position.normalized` consumed independently by Deviation Detector and Correlation Worker |
| Replay | New consumer group starts from offset 0 and reprocesses topic history |
| Isolated crash recovery | One group crashing does not affect others |

---

## `rpk` inspection commands

```bash
# Partition state and HIGH-WATERMARK
docker exec sentinel-redpanda rpk topic describe -p adsb.raw

# Committed offset for a consumer group
docker exec sentinel-redpanda rpk group describe cp1-experiment

# Read a specific record by offset (no group involved)
docker exec sentinel-redpanda rpk topic consume adsb.raw --offset 0 --num 1

# Reset a group's committed offset to the beginning
docker exec sentinel-redpanda rpk group seek cp1-experiment --to start --topics adsb.raw
```

`rpk group describe` columns:

| Column | Meaning |
| --- | --- |
| `CURRENT-OFFSET` | Next offset the group will read from |
| `LOG-END-OFFSET` | Next offset the broker will write to (HIGH-WATERMARK) |
| `LAG` | `LOG-END-OFFSET` minus `CURRENT-OFFSET`; unconsumed records for this group |
