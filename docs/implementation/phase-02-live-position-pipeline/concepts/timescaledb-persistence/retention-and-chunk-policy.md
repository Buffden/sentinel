# Retention and Chunk Policy: position_history

---

## Why 48 hours

`position_history` exists for three purposes in v1:

| Purpose | How far back needed |
| --- | --- |
| Redis live-state reconstruction after restart | Latest position per entity only |
| Route deviation detection | Current flight track: hours at most |
| Operator investigation (Phase 09) | 24-48 hours covers any realistic incident window |

Signal loss detection reads Redis `last_seen_ms`, not TimescaleDB.
Correlation and proximity work from Redis H3 sorted sets, not TimescaleDB.

There is no analytical use case in v1 that requires history beyond 48 hours.
30 days was never a deliberate decision; 48 hours is.

---

## Why chunk size matters for retention

TimescaleDB drops old data by dropping entire chunks, not by row-level `DELETE`.
A `DELETE WHERE observed_at < now() - interval '48 hours'` on a billion-row table
is a full sequential scan. Dropping a chunk is an `O(1)` filesystem operation.

For drops to be precise, the chunk interval must divide evenly into the retention window.
The retention policy fires after a chunk's end time crosses the retention boundary.
A chunk that straddles the boundary is not dropped until its entire interval has expired.

---

## Chosen values

| Setting | Value | Rationale |
| --- | --- | --- |
| Chunk interval | 1 hour | Divides 48-hour window into 48 clean chunks; each chunk fits well in memory |
| Retention window | 48 hours | Covers all v1 use cases; no speculative extension |
| Compression lag | 1 hour | Compress closed chunks immediately; only the current open chunk is uncompressed |

At any point in time there are at most 48 chunks: 1 open (accepting writes), 47 compressed (read-only).

---

## Row volume per chunk

Scoped European airspace bbox (~2,000 simultaneous flights, 10-second poll interval):

```
2,000 flights × 6 updates/min × 60 min = 720,000 rows per chunk
```

At ~150 bytes per row uncompressed: ~105 MB per chunk before compression.
TimescaleDB typically achieves 90%+ compression on time-series floats with repeated structure.
Compressed chunk size: ~10 MB. 47 compressed chunks: ~470 MB total.

Global OpenSky (~12,000 flights): multiply by 6. Still manageable with compression.
Narrowing the bbox is the single most effective lever for reducing volume.

---

## TimescaleDB policy SQL

```sql
-- Set chunk interval at hypertable creation (or alter after)
SELECT set_chunk_time_interval('position_history', INTERVAL '1 hour');

-- Retention: drop chunks older than 48 hours
SELECT add_retention_policy('position_history', INTERVAL '48 hours');

-- Compression: compress chunks closed more than 1 hour ago
ALTER TABLE position_history SET (
  timescaledb.compress,
  timescaledb.compress_orderby = 'observed_at DESC',
  timescaledb.compress_segmentby = 'entity_id'
);
SELECT add_compression_policy('position_history', INTERVAL '1 hour');
```

`compress_segmentby = 'entity_id'` groups all rows for the same entity within a chunk into
a single compressed segment. Queries filtered by `entity_id` (route deviation, investigation)
decompress only the relevant segments, not the entire chunk.

`compress_orderby = 'observed_at DESC'` stores the most recent rows first within each segment,
which matches the access pattern: "last N positions for entity X."

---

## raw_events has no retention policy

`raw_events` is a regular Postgres table (not a hypertable) and does not participate in
TimescaleDB chunk management. It is an audit/debug archive for Kafka records.

If `raw_events` volume becomes a concern, truncate it manually or configure a cron-based
`DELETE WHERE received_at < now() - interval '48 hours'`. For now it is left unmanaged:
the table is only written to in dev/local environments and its volume is not production-scale.
If this changes, move `raw_events` to S3 object storage rather than trying to make a
Kafka-identity-keyed table into a hypertable.
