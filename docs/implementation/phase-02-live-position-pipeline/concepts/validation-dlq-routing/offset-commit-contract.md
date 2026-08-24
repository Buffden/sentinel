# Offset Commit Contract

---

## The rule

The offset commit is **always the last action** for every message, and it is **unconditional**.

```
receive message
  → handleMessage (normalize, route to DLQ or log)
  → commitOffsets (always, regardless of outcome)
```

There is no path where the offset is committed before `handleMessage` finishes. There is no path where a rejection or DLQ failure prevents the commit.

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

## Why DLQ failure does not prevent commit

The commit is unconditional by design. Specifically, a DLQ publish failure does not block the commit.

The record at that offset is unprocessable. It was unprocessable before the DLQ attempt and it will be unprocessable on every future attempt. If the commit were withheld until the DLQ publish succeeded:

- A sustained `adsb.dlq` outage would stop the consumer at the first malformed record.
- Every healthy record at higher offsets would be blocked indefinitely.
- The pipeline would halt on a record it cannot process, which is the exact failure mode the DLQ is designed to prevent.

DLQ publish failures are logged at `error` level with full context. Operators can act on those logs. But the pipeline continues.

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
| DLQ publish fails | Log error; commit anyway; pipeline continues |
| Valid record processed successfully | Commit; advance to next record |
