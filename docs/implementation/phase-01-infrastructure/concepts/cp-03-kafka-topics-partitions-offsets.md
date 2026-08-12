# Kafka Topics, Partitions, and Offsets

Concepts exercised in Checkpoint 3. This is not a full Kafka reference.

---

## Topic

A named, ordered log of records. Producers write to it; consumers read from it. Topics are independent: reading `position.normalized` has no effect on a reader of `alerts`.

Sentinel's 8 canonical topics map directly to data-flow boundaries:

| Topic | Description |
|---|---|
| `adsb.raw` | Raw ADS-B telemetry (Ingestion Poller → Position Consumer) |
| `ais.raw` | Raw AIS telemetry (Ingestion Poller → Position Consumer) |
| `position.normalized` | Canonical positions (Position Consumer → Correlation Worker, Deviation Detector) |
| `deviation.candidates` | Per-ping route classifications (Deviation Detector → Alert Evaluator) |
| `proximity.candidates` | New proximity episodes (Correlation Worker → Alert Evaluator) |
| `alerts` | Logical alert events (Alert Evaluator → API) |
| `adsb.dlq` | Rejected ADS-B records |
| `ais.dlq` | Rejected AIS records |

---

## Partition

A topic is split into one or more partitions. Each partition is an independent ordered log.

In Checkpoint 3, every Sentinel topic has 1 partition. This is a local implementation choice:
- single Redpanda broker in dev-container mode
- no application consumers yet, so parallelism requirements are unknown
- 1 partition gives total ordering within each topic, simplest mental model

This is NOT a production sizing decision. Partition counts become relevant when throughput, ordering per entity-key, and consumer parallelism are understood.

---

## Record

A record is one message written to a partition. It has a value (payload), optional key, and a timestamp.

The key is used for partition assignment when there are multiple partitions (records with the same key go to the same partition, preserving per-key ordering). With 1 partition, the key has no routing effect.

---

## Offset

An offset is the position of a record within one partition. It is a sequential integer starting at 0.

```
partition 0:
  offset 0  -> {"checkpoint":3,"source":"manual-test","topic":"adsb.raw"}
  offset 1  -> (next record will land here)
```

What an offset is NOT:
- not a globally unique Kafka event ID
- not a timestamp
- not a database row ID
- not globally ordered across all partitions or all topics

Because Checkpoint 3 uses 1 partition per topic, all records within a topic are globally ordered by offset. In multi-partition topics, offsets are per-partition only.

---

## High watermark

The offset of the next record to be written. Before any produces: `HIGH-WATERMARK: 0`. After writing one record at offset 0: `HIGH-WATERMARK: 1`.

This is what `rpk topic describe -p` shows as the log end:

```
PARTITION  LEADER  EPOCH  REPLICAS  LOG-START-OFFSET  HIGH-WATERMARK
0          0       1      [0]       0                 1
```

---

## Producer

A process that writes records to a topic. No consumer needs to exist.

Manual produce in Checkpoint 3:
```bash
echo '{"checkpoint":3}' | docker exec -i sentinel-redpanda rpk topic produce adsb.raw
# Produced to partition 0 at offset 0 with timestamp ...
```

---

## Consumer

A process that reads records from a topic. Consumers are independent -- one consumer reading from offset 0 does not affect another consumer that may have already read to offset 5.

Manual consume:
```bash
docker exec sentinel-redpanda rpk topic consume adsb.raw --num 1
```

---

## Consumer group

A named group of one or more consumers that share the work of consuming a topic and track committed progress together.

When a consumer uses a group, the broker records which offsets the group has successfully processed. If the consumer restarts, it resumes from the last committed offset rather than from the beginning.

Without a group, each `rpk topic consume` starts from the latest offset by default. With a group, it resumes from committed progress.

In Sentinel, the canonical application groups are (defined in docs/ARCHITECTURE.md):
- `position-consumer`
- `correlation-worker`
- `deviation-detector`
- `alert-evaluator`
- `api`

The Checkpoint 3 disposable group `cp3-manual-test` was created only for this exercise.

---

## Three different things called "offset"

These are distinct and should not be conflated:

**Record offset** -- the record's fixed position in its partition. Immutable once written. Offset 0 always refers to the first record.

**Consumer's current position** -- where the consumer is currently reading. Changes as the consumer reads forward. Not stored on the broker by default unless the consumer commits.

**Committed consumer-group offset** -- the position stored on the broker for a named group. The broker records this so the group can resume from the right place after a restart. What `rpk group describe` shows as `CURRENT-OFFSET`.

After consuming offset 0 with group `cp3-manual-test`:
```
TOPIC     PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
adsb.raw  0          1               1               0
```

`CURRENT-OFFSET: 1` means the group has committed through offset 0 and will next read from offset 1. LAG 0 means no records remain unconsumed.

This is the foundation of at-least-once processing: if a consumer crashes after reading but before committing the offset, the next consumer in the group re-reads from the last committed offset. The record may be processed twice. Durable side effects must be idempotent.

---

## Replication factor and EPOCH

Replication factor 1 means only one broker holds the partition. In a multi-broker production cluster (MSK), replication factor > 1 means the partition has copies on multiple brokers. If the leader fails, a follower is elected.

`EPOCH` in `rpk topic describe -p` is the leadership epoch -- it increments each time a new leader is elected. After a broker restart in Checkpoint 3, EPOCH incremented from 1 to 2. Topics and data survived because of the named Docker volume.

---

## Topic creation idempotency

`rpk topic create` in Redpanda v24.1.2 does not have a `--if-not-exists` flag. `topics.sh` achieves idempotency explicitly: it lists existing topics first, then only creates those that are missing. This correctly distinguishes:
- topic already exists -- silently skipped
- broker unreachable -- `rpk topic list` fails; `set -euo pipefail` aborts
- provisioning failure -- `rpk topic create` fails; `set -euo pipefail` aborts

---

## What Redpanda provides locally

Redpanda is a Kafka-compatible broker written in C++. It speaks the Kafka wire protocol, so any Kafka client works without changes. For local development it requires no ZooKeeper, starts as a single process, and ships with `rpk` for topic/group management.

In production, Sentinel targets Amazon MSK. Redpanda is the local stand-in. The application code does not change between environments.

---

## Deferred

These Kafka concepts are intentionally not covered in this checkpoint:

- partition-key design and per-entity ordering guarantees
- consumer group rebalancing
- ISR (in-sync replicas) and leader election internals
- replication failure and partition unavailability
- exactly-once semantics and idempotent producers
- Kafka transactions
- retention tuning and log compaction
- DLQ producer logic and application retry strategy
- offset commit strategies in application code (auto-commit vs manual commit)
- consumer group lag monitoring in production
- MSK infrastructure provisioning
