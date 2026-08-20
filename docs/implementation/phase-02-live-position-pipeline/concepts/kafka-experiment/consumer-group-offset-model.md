# Consumer Group and Offset Model

## There are three different things called "offset" and they're easy to confuse

Picture the partition log as a list of records, each numbered from zero:

```text
adsb.raw — Partition 0:
  [0]  {"checkpoint":5, "source":"manual-store-verification"}
  [1]  {"checkpoint":5, "source":"manual-store-verification"}
  [2]  ...
  [3]  {"entity_id":"test-aircraft-1", "timestamp_ms":...}
```

The number next to each record is its **record offset**. The broker assigns it when the record is written. It never changes. Even after a consumer reads a record, its offset stays the same and the record stays in the log. Consumption is not deletion.

Now, when your consumer process is running, it keeps track of where it's currently reading — "I'm about to fetch offset 2." That's the **consumer's current position**, and it lives only in memory. Kill the process and it's gone.

The third offset is the one that actually survives a restart: the **committed consumer-group offset**. This is what the broker stores on behalf of a named group in an internal topic called `__consumer_offsets`. It's the answer to "if this consumer restarts, where should it resume from?"

These three are independent. The record offset is set by the broker when a record is written. The current position is set by the consumer as it fetches. The committed offset is set by the consumer when it explicitly (or automatically) tells the broker "I've processed up to here."

---

## autoCommit: what it does and when it matters

kafkajs defaults to `autoCommit: true`. After your `eachMessage` handler returns without throwing, kafkajs queues an offset commit. It doesn't fire immediately — it runs on a background timer, every 5 seconds by default.

This gap is the source of at-least-once delivery. If your process crashes after the handler runs but before that 5-second timer fires, the offset was never committed. When the process restarts, the broker has no record of that commit, so it delivers the same record again.

That's not a bug to fix — it's the guarantee Kafka gives you, and it's honest about it. The implication is that anything your handler does with that record (writing to TimescaleDB, updating Redis) needs to be safe to do twice with the same input.

---

## fromBeginning: only matters the first time

```typescript
await consumer.subscribe({ topic: TOPIC, fromBeginning: true });
```

When a consumer group has never run before, there's no committed offset for it. `fromBeginning: true` tells kafkajs to start from the earliest available record in the partition. Without it, the consumer would start from the latest offset — meaning it would skip everything that was produced before it first connected.

Once the group has committed an offset at least once, `fromBeginning` is ignored. The group always resumes from its committed position.

In the Kafka experiment this is why the consumer picked up the Phase 01 records at offsets 1 and 2 on first run — the `kafka-experiment` group had no committed offset yet, so it started from the beginning of whatever was left in the log.

---

## What happens on restart

Clean shutdown (offset was committed before the process stopped):

```text
consumer reads offset 3
handler returns
autoCommit fires — CURRENT-OFFSET = 4
process stops
process restarts
resumes from offset 4 — offset 3 not re-delivered
```

Crash before commit (the dangerous case):

```text
consumer reads offset 3
handler returns
process crashes — autoCommit never fired
CURRENT-OFFSET still = 3
process restarts
reads offset 3 again — re-delivered
```

The second scenario is not hypothetical — network kills, OOM events, and deploy restarts all produce it. This is why the position-consumer's TimescaleDB write uses `ON CONFLICT DO NOTHING` and the Redis write checks timestamps before overwriting.

---

## Consumer group independence: why this matters for Sentinel

Each consumer group maintains its own committed offset, completely independently. One group reading a record has zero effect on any other group.

This is why `position.normalized` can be consumed independently by the Deviation Detector and the Correlation Worker without either one affecting the other. They're separate groups, separate committed offsets, separate lag counters.

It also means replay is free: create a new consumer group, start it from the beginning, and it reprocesses the entire topic without touching any other group's state. No coordination needed.

You can see this yourself with `rpk`:

```bash
# See every consumer group and their lag
docker exec sentinel-redpanda rpk group list

# Inspect a specific group's committed position
docker exec sentinel-redpanda rpk group describe kafka-experiment

# Read a record by offset without involving any consumer group
docker exec sentinel-redpanda rpk topic consume adsb.raw --offset 3 --num 1

# Reset a group to the beginning (simulates a crash / force replay)
docker exec sentinel-redpanda rpk group seek kafka-experiment --to start --topics adsb.raw
```

The columns in `rpk group describe` that matter:

- `CURRENT-OFFSET` — the next offset the group will fetch from (not the last one it read)
- `LOG-END-OFFSET` — the next offset the broker will write to (one past the last record)
- `LAG` — how many records the group hasn't consumed yet (`LOG-END-OFFSET` minus `CURRENT-OFFSET`)

LAG = 0 means the group is caught up. LAG > 0 means records are waiting.
