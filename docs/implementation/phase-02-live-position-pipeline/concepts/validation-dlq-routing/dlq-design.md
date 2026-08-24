# Dead-Letter Queue Design

---

## What a DLQ is for

A dead-letter queue holds records that a consumer cannot process and will never be able to process, so that:

1. The pipeline does not stall waiting to handle a record it cannot handle.
2. Operators can inspect, diagnose, and replay or discard those records out of band.
3. The main topic's committed offset advances so that healthy records behind the bad one are not blocked.

Without a DLQ, an unprocessable record would either crash the consumer (stopping the pipeline) or be silently dropped (losing the record with no trace). Both outcomes are worse than parking the record in a side topic.

---

## The DLQ envelope

Every record published to `adsb.dlq` is wrapped in a rejection envelope:

```json
{
  "raw_payload":    "the original Kafka message value, verbatim",
  "rejection_reason": "parse_error: Unexpected token 'h', \"this is not json\" is not valid JSON",
  "source_topic":   "adsb.raw",
  "source_partition": 0,
  "source_offset":  "105185",
  "consumer_id":    "hostname-12345",
  "timestamp_ms":   1787553985508
}
```

### Why each field is present

| Field | Purpose |
| --- | --- |
| `raw_payload` | Preserves the exact bytes that were rejected so the record can be inspected or replayed without going back to the broker |
| `rejection_reason` | `kind: detail` string; tells the operator what classification was applied and the specific error message |
| `source_topic` | The record might have come from `adsb.raw` or `ais.raw`; makes the envelope self-describing |
| `source_partition` | Kafka offsets are only unique within a partition; needed to reconstruct the exact log position |
| `source_offset` | Combined with topic and partition, uniquely identifies the original record in the broker log |
| `consumer_id` | `hostname-pid`; tells operators which consumer instance rejected the record, useful in multi-instance deploys |
| `timestamp_ms` | Processing time of the rejection — this is an audit timestamp, not source event time |

### `source_partition` is not optional

An offset without a partition number is ambiguous as soon as a topic has more than one partition. Even though `adsb.raw` currently has one partition, the envelope must include `source_partition` now, before the topic is ever scaled, so that the DLQ schema does not silently become incorrect the moment a second partition is added.

### `timestamp_ms` is processing time here

Throughout Sentinel, `timestamp_ms` refers to source event time. The DLQ envelope deliberately breaks this convention. The envelope is an audit record, not a position event. The `timestamp_ms` here records when the consumer decided to reject the record — useful for measuring how long a bad record sat in the topic before being noticed. `raw_payload` contains the original message, which may or may not have a `time_position` field to extract source event time from.

---

## DLQ publish failure isolation

If `adsb.dlq` is unavailable when the consumer tries to publish a rejection envelope, the consumer:

1. Catches the error.
2. Logs it at the `error` level with the rejection reason and source coordinates.
3. Does **not** retry.
4. Does **not** block or crash.
5. Commits the offset and continues to the next record.

```
dlq publish failed; skipping record
  rejection_reason: parse_error: ...
  source_topic: adsb.raw
  source_partition: 0
  source_offset: 105185
  error: Connection refused
```

### Why this is correct

The record at offset 105185 will never be processable. It is malformed. If the consumer blocked on a failed DLQ publish:

- A transient broker blip would stall the entire pipeline on a record it will never be able to process.
- Every healthy record behind offset 105185 would be held up.

The tradeoff: a DLQ publish failure means the rejection envelope is lost (the bad record is neither in the DLQ nor in `position_history`). The operator log at `error` level is the signal. Operators must monitor for DLQ publish errors and investigate both the DLQ availability issue and the original records that were silently skipped.

This tradeoff is explicit and intentional. The alternative (blocking the pipeline) is worse in every scenario involving a transient fault.

---

## What the DLQ does not guarantee

- **No at-least-once for the DLQ itself.** If the publish fails, the envelope is lost. This is accepted.
- **No ordering.** DLQ records arrive in processing order, which is not necessarily source event time order.
- **No deduplication.** If the consumer crashes after publishing to the DLQ but before committing the offset, the record is redelivered, normalized, and published to the DLQ a second time. Both DLQ entries are identical. This is harmless.
- **No routing back to the main pipeline.** The DLQ is a terminal destination. Reprocessing requires manual intervention: fix the source, then replay from the original `adsb.raw` offset using a separate consumer group.
