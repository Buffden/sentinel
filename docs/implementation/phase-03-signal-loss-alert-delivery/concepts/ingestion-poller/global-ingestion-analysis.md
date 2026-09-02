# Global Ingestion Analysis

What happens if the bbox filter is removed from the ingestion poller and the full
world flight dataset is ingested.

Current default bbox: UK + Western Europe (lat 49–61°N, lon 8°W–10°E).
Configured via `OPENSKY_LAMIN`, `OPENSKY_LOMIN`, `OPENSKY_LAMAX`, `OPENSKY_LOMAX`.

---

## Layer-by-layer impact

### OpenSky API — first constraint

OpenSky's REST API is rate-limited and credit-gated. Access policies, rate limits,
and response sizes depend on account type and may change. The current documentation
describes a credit-based system with OAuth2 client credentials for programmatic access
and notes that using the REST API in a live automated product or service requires prior
written agreement with OpenSky. For a local educational portfolio project this is a
different context, but any production use of OpenSky as a data source should be
reviewed against the current terms at the time of deployment.

At the current anonymous or basic access level, large global requests without a bbox
are likely to be rate-limited, de-prioritised, or timed out. Without a bbox, OpenSky
returns 5,000–15,000 aircraft globally as a 5–8 MB JSON blob per cycle, compared to
300–800 aircraft and ~200 KB for UK+Europe.

The poller's `FETCH_TIMEOUT_MS = 8_000` would trigger on slow responses, silently
dropping entire poll cycles.

Mitigation for V1: keep the bbox. A future source adapter boundary allows replacing
or augmenting OpenSky with a higher-capacity data source when global coverage is
actually needed.

### Kafka `adsb.raw` — expected to hold, not yet benchmarked

1,500 msg/s (15,000 aircraft per 10-second poll) is expected to be well within
Redpanda's capability for a single-partition topic, but Sentinel has not yet
benchmarked this workload on the local M3 Pro + Docker environment. Treat this as an
engineering expectation rather than a measured result.

Each message is one aircraft state vector, keyed by `icao24`. The single partition
keeps per-aircraft message order correct.

Scaling note: partition count can be increased later, but doing so changes the
key-to-partition assignment for newly produced messages, because the number of
partitions participates in partition selection. Existing records remain in their
original partitions and are not redistributed. If strict per-entity ordering across a
partition-count transition matters, partition growth must therefore be planned
carefully.

### Position Consumer — likely first throughput bottleneck, not yet benchmarked

Currently processes 300–800 messages per poll cycle at UK+Europe scale.
Globally: 5,000–15,000 per cycle.

Each message performs five sequential I/O operations:

1. INSERT into `raw_events` (TimescaleDB)
2. INSERT into `position_history` (TimescaleDB)
3. `HGET entity:live:<id>` (Redis — read current geo cell)
4. `HSET entity:live:<id>` (Redis — Lua monotonic guard)
5. `PUBLISH position-updates` (Redis pub/sub)

The current sequential per-message I/O path is likely to become the first internal
throughput bottleneck as message rate increases. It should be benchmarked at 500, 1K,
and 1,500+ msg/s using a synthetic Kafka producer before drawing conclusions about
what tuning or architectural changes are needed.

### TimescaleDB — write volume is significant at any scale

The Position Consumer writes two rows per accepted message: one in `raw_events` and
one in `position_history`. At a 10-second poll interval:

| Aircraft count | Rows/day per table | Total inserts/day (both tables) |
| --- | --- | --- |
| 300 (UK+Europe low) | ~2.6M | ~5.2M |
| 800 (UK+Europe high) | ~6.9M | ~13.8M |
| 5,000 (global low) | ~43M | ~86M |
| 15,000 (global high) | ~130M | ~260M |

The hypertable handles the structural growth, but write throughput and disk use scale
linearly with message rate. Chunk interval tuning, bulk insert batching, and monitoring
of per-insert latency become important well before global scale.

The `(entity_id, observed_at)` unique constraint absorbs duplicates idempotently —
this property holds regardless of ingestion scale.

### Redis live state — memory fine, evaluator scan slower

15,000 `entity:live:*` hashes at ~500 bytes each is approximately 7.5 MB — trivial
for Redis. Memory is not a concern at this scale.

The Alert Evaluator performs a full `SCAN entity:live:*` each cycle, which is O(n)
over all live keys. At 15,000 keys it remains fast. At 50,000+ keys (maritime,
weather, satellites added in future phases) the scan duration starts to matter and
would need to be replaced with a set-based membership index or evaluated per H3 cell.

### Dashboard — client payload bounded, server filtering load increases

`GET /entities/live?bbox=` filters by the client's viewport before returning results.
The WebSocket server applies per-connection bbox filtering before forwarding position
updates. The client therefore only receives what is within its current viewport,
regardless of how many entities are in Redis.

However, the API/WebSocket server still receives and processes every position-update
pub/sub message before deciding whether to forward it. At 15,000 position updates per
10-second cycle, the server-side filtering workload increases proportionally with
global update volume even though client payload remains viewport-bounded.

---

## Summary

| Layer | UK+Europe | Global (no bbox) | Status |
| --- | --- | --- | --- |
| OpenSky fetch | ~200 KB, ~500 aircraft | ~7 MB, ~15K aircraft | Rate-limited / timed out |
| Kafka throughput | Fine | Expected fine, not benchmarked | Expectation only |
| Position Consumer | ~2.6M–6.9M inserts/day | ~86M–260M inserts/day | Benchmark before scaling |
| TimescaleDB | ~2.6M–6.9M rows/day per table | ~43M–130M rows/day per table | Write volume grows linearly |
| Redis memory | ~150–400 KB | ~7.5 MB | Fine |
| Alert Evaluator scan | Fast | Slower but acceptable | Concern at 50K+ keys |
| Dashboard client | Viewport-filtered | Viewport-filtered | Payload bounded |
| API/WS filter load | Low | Scales with update volume | Increases with msg rate |

---

## V1 path to higher throughput

Sentinel v1 retains a single-writer, single-consumer model. The Redis state-transition
logic — monotonic live-state guard, geo-cell sorted-set maintenance, signal-loss episode
gate — depends on the ordering guarantees and simplicity of that model. Multi-consumer
parallelism would require rethinking those guarantees and is not the right first step.

The ordered path for v1:

1. Keep the single consumer. Do not add partitions or parallel consumers yet.
2. Introduce bulk DB writes: batch `raw_events` and `position_history` inserts using
   multi-row `INSERT ... VALUES` rather than one statement per message.
3. Pipeline Redis operations: combine HGET, Lua HSET, ZADD/ZREM, and PUBLISH into a
   single pipeline where the monotonic ordering contract permits.
4. Benchmark 500 → 1K → 1,500+ msg/s using a synthetic Kafka producer. Measure
   consumer group lag, per-message DB latency, and Redis operation latency at each
   step.
5. Monitor consumer group lag (`rpk group describe position-consumer`) — growing lag
   is the primary signal that single-consumer throughput is insufficient.
6. Only revisit partitions and consumer parallelism if a tuned single consumer cannot
   satisfy the required throughput at measured workload.
7. Replace the evaluator full-scan with a set-based membership index when active entity
   count exceeds ~20,000 keys.
8. Keep OpenSky regional for v1. The source-adapter boundary allows adding a
   higher-capacity global data source later without changing the downstream pipeline.

This is a future-phase concern. The bbox is the correct constraint for the current
architecture.
