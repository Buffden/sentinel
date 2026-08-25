# Monotonic Guard

---

## The problem: stale events can arrive in any order

Kafka at-least-once delivery means the consumer can receive the same message more than once. Out-of-order delivery can also happen when a consumer restarts and replays from an earlier offset.

Without a guard, a replayed message with an older `timestamp_ms` could overwrite the entity's current live state with a stale position. The Alert Evaluator would then see an older location, older speed, and an older `last_seen_ms`. Signal-loss detection and route deviation evaluation would be wrong.

The monotonic guard prevents this: only a strictly newer source event time is allowed to update the hash.

---

## Why Lua

The naive approach — read `last_seen_ms`, compare, then write if newer — is a read-modify-write cycle. With multiple consumer instances running in parallel, two instances can both read the same `last_seen_ms`, both conclude their incoming event is newer, and both issue an `HSET`. One of them will overwrite the other's write. The result depends on network timing. This is a race condition.

Redis executes Lua scripts atomically. No other Redis command can run between the first line of the script and the last. The check and the write are a single indivisible operation. No concurrent consumer can interleave.

This guarantee comes from Redis's single-threaded command dispatcher, not from any lock. The script runs to completion before Redis processes the next command from any client.

---

## The script

```lua
local current = redis.call('HGET', KEYS[1], 'last_seen_ms')
if current and tonumber(current) >= tonumber(ARGV[1]) then
  return 0
end
local fields = {}
for i = 3, #ARGV do
  fields[#fields + 1] = ARGV[i]
end
redis.call('HSET', KEYS[1], unpack(fields))
redis.call('EXPIRE', KEYS[1], tonumber(ARGV[2]))
return 1
```

| Argument | Value |
| --- | --- |
| `KEYS[1]` | `entity:live:{entity_id}` |
| `ARGV[1]` | incoming `last_seen_ms` (milliseconds, as string) |
| `ARGV[2]` | TTL in seconds (86400) |
| `ARGV[3..N]` | flat field/value pairs for `HSET` |

The script returns `0` if the write was rejected, `1` if it was accepted and the TTL was refreshed.

---

## The >= rejection rule

The guard rejects when `current >= incoming`, not just `current > incoming`.

Equal timestamps are also rejected. An equal timestamp means the same event is being replayed. The existing hash already reflects that event's state — rewriting it would be a no-op at best, and for concurrent consumers it would be a redundant write under the same race condition logic. Treating equal as stale keeps the behavior simple and consistent: one accepted write per source event time per entity.

---

## What the return value means

| Return | Meaning |
| --- | --- |
| `1` | Write accepted. Hash updated. TTL refreshed. |
| `0` | Write rejected. Hash unchanged. TTL unchanged. |

The consumer logs a warning on `0`:

```json
{"level":"warn","message":"live state not updated — stale event","entity_id":"def456","timestamp_ms":1700000100000}
```

A stale result is not an error. Out-of-order and replayed events are normal in at-least-once Kafka pipelines. The warning exists so operators can observe the pattern in logs if needed.

The `position_history` row is written regardless — the durable history still records the event. Only the ephemeral live snapshot is guarded.

---

## TTL is only refreshed on accepted writes

`EXPIRE` is called inside the `if` branch that accepted the write. A stale event does not touch the TTL. This is correct: refreshing the TTL on a stale event would extend the entity's apparent "alive" window based on a replayed historical message, which could suppress a legitimate signal-loss alert.
