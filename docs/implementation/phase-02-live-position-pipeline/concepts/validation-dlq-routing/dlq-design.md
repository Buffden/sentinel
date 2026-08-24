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

1. Logs the error at `error` level with the rejection reason and source coordinates.
2. Rethrows the error.
3. Does **not** commit the original Kafka offset.
4. Kafka redelivers the message on the next consumer start.

```
dlq publish failed; not committing offset — Kafka will redeliver
  rejection_reason: parse_error: ...
  source_topic: adsb.raw
  source_partition: 0
  source_offset: 105185
  error: Connection refused
```

### Why the offset is not committed on DLQ failure

At CP5, `raw_events` is written before the DLQ publish attempt. If the DLQ publish fails and the offset were committed anyway, the rejection envelope would be permanently lost. The `raw_events` row exists (it was written first), but the DLQ entry — which carries the rejection reason — would be missing.

Retrying is safer: on redeliver, the `raw_events` insert hits `ON CONFLICT DO NOTHING`, and the DLQ publish is attempted again. This continues until the publish succeeds or the consumer is manually restarted and the broker issue is investigated.

**Note:** this behavior changed at CP5. The original CP4 design committed the offset unconditionally even on DLQ failure. CP5 corrected this once `raw_events` made replay fully idempotent for all record types.

---

## What the DLQ does not guarantee

- **No at-least-once for the DLQ itself.** If the publish fails, the envelope is lost. This is accepted.
- **No ordering.** DLQ records arrive in processing order, which is not necessarily source event time order.
- **No deduplication.** If the consumer crashes after publishing to the DLQ but before committing the offset, the record is redelivered, normalized, and published to the DLQ a second time. Both DLQ entries are identical. This is harmless.
- **No routing back to the main pipeline.** The DLQ is a terminal destination. Reprocessing requires manual intervention: fix the source, then replay from the original `adsb.raw` offset using a separate consumer group.
