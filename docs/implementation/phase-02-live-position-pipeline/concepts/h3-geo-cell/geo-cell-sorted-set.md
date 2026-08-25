# Geo-Cell Sorted Set

---

## Structure

```
geo-cell:{live_geo_cell}
  member: entity_id
  score:  last_seen_ms  (source event time, not processing time)
```

One sorted set per occupied H3 cell at `LIVE_H3_RESOLUTION`. An entity is a member of exactly one sorted set at any time: the set for its current cell.

The score is the source event time of the most recent accepted position. This makes freshness filtering cheap: the Correlation Worker queries `ZRANGEBYSCORE geo-cell:{cell} {now_ms - window_ms} +inf` to get only recently active entities in a cell, without scanning inactive ones.

---

## Update sequence

For every accepted position (Lua guard returned 1):

```
1. computeH3Cells(lat, lon) → live_geo_cell
2. HGET entity:live:{entity_id} live_geo_cell   ← read OLD cell before hash is overwritten
3. Lua guard → HSET (includes live_geo_cell)    ← hash updated atomically
4. if old_cell exists AND old_cell != new_cell:
     ZREM geo-cell:{old_cell} {entity_id}
5. ZADD geo-cell:{new_cell} {last_seen_ms} {entity_id}
```

Step 2 must happen before step 3. The Lua script overwrites the hash in one atomic operation, which includes setting `live_geo_cell` to the new value. Once the script runs, the old cell is gone from the hash. Reading it afterward would give the new cell, not the one the entity was just removed from.

Step 4 is only reached when the cell actually changed. If the entity is still in the same cell, `ZADD` simply updates the score (the entity's `last_seen_ms` moves forward). No `ZREM` is needed.

Steps 4 and 5 are skipped entirely for stale events (Lua guard returned 0). A stale event must not mutate the sorted sets — doing so could remove a more recently written entry from the current cell.

---

## The non-atomic gap

The HGET, Lua write, ZREM, and ZADD are four separate Redis operations. They are not wrapped in a transaction. A crash between ZREM and ZADD would leave the entity absent from both sorted sets until the Kafka message is redelivered and reprocessed.

This gap is acceptable for two reasons:

1. **ZADD is idempotent**: replaying the message re-adds the entity to the correct cell with the same score. The sorted set converges to the correct state.
2. **The Correlation Worker uses a freshness lower bound**: it queries with `ZRANGEBYSCORE ... {now_ms - window_ms} +inf`. An entity absent from the index during the brief gap is simply not a candidate for that one lookup cycle. It is not a data loss — the entity will appear in the next scan after replay.

A brief absence from the live index is much safer than the alternative: using a Redis transaction (MULTI/EXEC) would still not be atomic with the hash Lua write, and would complicate the code significantly for minimal gain.

---

## ZADD on same-cell updates

When an entity transmits a new position in the same cell, there is no ZREM. `ZADD` with an existing member updates the score in place. The sorted set reflects the entity's most recent event time in that cell, even when the cell has not changed.

---

## ZREM on a non-existent member

`ZREM` on a key or member that does not exist is a no-op in Redis. This means:

- First ping for a new entity: `old_cell` is null, the `if` branch is skipped, and only `ZADD` runs.
- Concurrent consumer replays the same event after another consumer already moved the entity: `ZREM` on the old cell finds the member already gone and does nothing.

Both cases are safe.

---

## No TTL on sorted sets

The sorted sets have no TTL. Stale members age out logically by score: a Correlation Worker query with a freshness lower bound will not see entities whose `last_seen_ms` is older than the window. The sorted sets grow monotonically in key count but are bounded by the number of distinct occupied cells.

If an entity permanently stops transmitting, its member entry remains in the sorted set indefinitely but is invisible to freshness-filtered queries. This is intentional: the Alert Evaluator detects the absence from `entity:live` TTL expiry, not from sorted set membership. Cleaning up stale sorted-set members is a future operational concern, not a correctness requirement.
