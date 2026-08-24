# Offset Commit Contract

---

## The rule

The offset commit is **always the last action** for every message. It is committed only when all required writes for that message succeed.

```
receive message
  → handleMessage (archive, normalize, route to DLQ or persist)
  → commitOffsets (only on success)
```

There is no path where the offset is committed before `handleMessage` finishes. A write failure or DLQ publish failure causes `handleMessage` to throw, and the commit line is never reached.

---

## What "last action" means for correctness

### Crash before commit: redeliver on restart

If the consumer crashes anywhere inside `handleMessage` — or even just after `handleMessage` returns but before `commitOffsets` executes — Kafka redelivers the message on restart. The consumer processes it again from the beginning.

At this stage replay is safe because:
- A `parse_error` record is still malformed on replay. It goes to the DLQ again, potentially creating a duplicate DLQ entry. Duplicate DLQ entries are harmless.
- A `no_position` record is still a no_position. It is skipped again.
- A valid record currently just logs. The `position_history` write added in the next checkpoint is idempotent (`ON CONFLICT DO NOTHING`). A duplicate write changes nothing.

The at-least-once guarantee combined with idempotent downstream effects gives the same correctness as exactly-once without requiring the coordination overhead of transactions.

### Crash after commit: no redeliver

If the consumer crashes after `commitOffsets` returns, the broker does not redeliver. The committed offset persists on the broker even with zero active consumers. On restart the consumer resumes from the next offset. This is the normal success path.

---

## Why DLQ failure prevents commit

A DLQ publish failure causes `handleMessage` to throw. The commit line is not reached. Kafka redelivers.

With `raw_events` persistence, `raw_events` is written before the DLQ attempt. The `raw_events` insert is idempotent on redeliver via `ON CONFLICT DO NOTHING`. The DLQ publish is retried. This continues until either the broker recovers and the publish succeeds, or the consumer is manually restarted and investigated.

**Note:** the original DLQ routing implementation committed the offset unconditionally even on DLQ failure, to avoid stalling the pipeline on an unprocessable record. Adding `raw_events` persistence revised this because `raw_events` now makes every redeliver idempotent — retrying is safe, and losing the DLQ envelope is not acceptable when it can be avoided.

---

## The offset arithmetic

kafkajs `commitOffsets` takes `offset + 1`, not `offset`. This is the Kafka protocol convention: the committed value is the next offset to fetch, not the last offset processed.

```typescript
await consumer.commitOffsets([{
    topic,
    partition,
    offset: (BigInt(offset) + 1n).toString(),
}]);
```

`BigInt` arithmetic is used because Kafka offsets are 64-bit integers. JavaScript `number` loses precision above 2^53. An offset of `9007199254740993` (2^53 + 1) would be rounded to `9007199254740992` with plain `number` arithmetic, causing the consumer to commit the wrong offset. Using `BigInt` throughout avoids this.

At current scales (100K records) this does not matter, but the correct behavior costs nothing and prevents a correctness bug at high scale.

---

## `autoCommit: false` is required

The consumer runs with `autoCommit: false`. Auto-commit would periodically flush committed offsets on a timer, regardless of whether `handleMessage` had finished. This creates a window where:

1. Auto-commit fires.
2. The offset is committed.
3. `handleMessage` continues processing (DLQ publish, downstream writes).
4. Consumer crashes.
5. On restart: the committed offset is past the crash point; the record is never redelivered.

With `autoCommit: false`, the commit only happens when the code explicitly calls it, after all processing for that message is complete. The developer controls exactly when "done" means done.

---

## Summary

| Scenario | Outcome |
| --- | --- |
| Crash before handleMessage completes | Redeliver on restart; idempotent replay |
| Crash between handleMessage and commitOffsets | Redeliver on restart; idempotent replay |
| Crash after commitOffsets | No redeliver; normal at-least-once guarantee |
| DLQ publish fails | Log error; throw; offset not committed; Kafka redelivers |
| Valid record processed successfully | Commit; advance to next record |
