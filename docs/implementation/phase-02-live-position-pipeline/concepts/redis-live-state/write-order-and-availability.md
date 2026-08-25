# Write Order and Availability

---

## Where Redis live state fits in the write sequence

For every valid normalized position, the consumer follows this sequence:

```
1. INSERT raw_events          (all paths — required before any branching)
2. INSERT position_history    (valid positions only)
3. updateLiveState (Redis)    (valid positions only)
4. commitOffsets
```

Redis live state is step 3. It runs after `position_history` has been written and before the offset is committed. A failure at step 3 blocks the offset commit.

---

## Redis is an availability dependency

If the Redis write fails — connection refused, timeout, or any other error — `updateLiveState` throws. The error propagates out of `handleMessage`. The offset is not committed. Kafka redelivers the message on restart.

On redeliver:
- `raw_events` hits `ON CONFLICT DO NOTHING`.
- `position_history` hits `ON CONFLICT DO NOTHING`.
- `updateLiveState` is retried. If Redis is back, the Lua guard runs again.

The Lua guard handles this correctly: if the hash was partially written before the crash (unlikely, since `HSET` and `EXPIRE` are sequential commands), the guard checks `last_seen_ms`. An equal incoming timestamp returns `0` (stale) and leaves the hash as-is. A replay is always safe.

---

## Stale result is not an error

A stale result from the Lua guard (`0` return) is not a Redis failure. It means Redis responded correctly and rejected an out-of-order or replayed event. The consumer logs a warning and continues to step 4 (commit offset).

| Outcome | What happened | Offset committed |
| --- | --- | --- |
| Lua returns `1` | Write accepted, hash updated, TTL refreshed | Yes |
| Lua returns `0` | Write rejected — incoming timestamp not newer | Yes |
| Redis throws | Connection error, timeout, or protocol error | No |

The distinction matters: a stale event is expected and handled gracefully. A Redis error is unexpected and must cause a redeliver.

---

## Replay idempotency

`updateLiveState` is idempotent under replay because the Lua guard uses `>=`:

- If the crash happened before the Redis write: replay writes the hash. Accepted.
- If the crash happened after the Redis write but before `commitOffsets`: replay attempts the same write. The guard finds `current == incoming`, returns `0`. Hash is unchanged. Correct.
- If the crash happened after `commitOffsets`: no redeliver. Normal success path.

In all three cases, the final Redis state after replay is identical to the state after a clean first run.

---

## position_history is written before Redis

`position_history` is written at step 2, before the Redis write at step 3. A crash between steps 2 and 3 leaves `position_history` with a row and the Redis hash without an update.

On redeliver:
- `position_history` insert hits `ON CONFLICT DO NOTHING`.
- Redis write is attempted and accepted (the entity's `last_seen_ms` in the hash, if it exists from a prior message, is older than the incoming timestamp).

This is safe. The durable record in `position_history` is always written first. The ephemeral live state catches up on replay.
