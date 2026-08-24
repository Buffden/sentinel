# Write Order and Availability

---

## The write order

For every Kafka message, the consumer follows this sequence:

```
1. INSERT raw_events          (all paths — required before any branching)
2. normalize / classify
3. branch:
     parse_error / missing_entity_id  → DLQ publish → commit offset
     no_position                      → warn log → commit offset
     valid position                   → INSERT position_history → commit offset
```

`raw_events` is always step 1. The offset is always the last step.

---

## raw_events is an availability dependency

`raw_events` is not a correctness dependency for downstream services — Correlation Worker, Deviation Detector, and Alert Evaluator never read it. But it is an availability dependency for the consumer itself.

If the `raw_events` insert fails, the error propagates. The offset is not committed. Kafka redelivers the message. The insert is retried. This is required because the archive is supposed to contain every record the consumer ever processed. Silently skipping the archive write on a transient DB error would leave gaps.

The replay insert is always safe: `ON CONFLICT (source_topic, source_partition, source_offset) DO NOTHING`.

---

## Why raw_events comes before normalization

Normalization can fail. A `parse_error` record has no canonical form to store in `position_history`, but it is still a real Kafka record that arrived on the topic. The archive captures it before the consumer even attempts to interpret it.

If `raw_events` were written only on the happy path, the archive would be incomplete: malformed records would be in the DLQ but not in the archive. You could not reconstruct the full set of records the consumer saw from `raw_events` alone.

---

## DLQ publish failure blocks offset commit

In the original DLQ routing implementation, a DLQ publish failure was logged and the offset was committed anyway. This was incorrect.

With raw_events persistence, the DLQ path is:

```
raw_events insert succeeds
    ↓
DLQ publish
    success → commit offset
    failure → throw → offset NOT committed → Kafka redelivers
```

On redeliver, `raw_events` hits `ON CONFLICT DO NOTHING`. DLQ publish is attempted again. This loop continues until either the DLQ publish succeeds or the consumer is manually restarted and investigated.

The rationale: if the broker is unavailable and a malformed record cannot be published to the DLQ, committing the offset would permanently discard any record of that rejection. The `raw_events` row is present, but the DLQ envelope (which contains the rejection reason) is lost. Retrying is safer.

---

## The crash boundary

The offset commit being the last action creates one crash boundary with three safe outcomes:

| Crash point | What Kafka does | DB state | Outcome |
| --- | --- | --- | --- |
| Before `raw_events` insert | Redelivers | Empty for this record | Both inserts succeed on replay |
| Between `raw_events` and `position_history` inserts | Redelivers | `raw_events` row exists | `raw_events` hits `DO NOTHING`; `position_history` succeeds |
| After all writes, before `commitOffsets` | Redelivers | Both rows exist | Both inserts hit `DO NOTHING`; offset committed |
| After `commitOffsets` | No redeliver | Both rows exist | Normal success path |

In every case, the final database state after replay is identical to the state after a clean first run.

---

## geo_cell is NULL until H3 indexing

`position_history.geo_cell` is written as NULL for now. The column is nullable (migration 007 dropped the NOT NULL constraint). The column and its indexes remain in place.

H3 geo-cell indexing owns this. When implemented, the consumer will compute the H3 cell from `lat` and `lon` and write it alongside the position. Persistence is complete and correct without it.
