# H3 Geo-Cell Indexing Debrief

---

## Infrastructure status

| Container | Status |
| --- | --- |
| sentinel-redpanda | healthy |
| sentinel-timescaledb | healthy |
| sentinel-redis | healthy |
| sentinel-neo4j | healthy |

---

## Hands-on H3 experiment

Before implementing, `latLngToCell`, `gridDisk`, and cell comparison were exercised directly in Node.js:

```js
import { latLngToCell, gridDisk } from 'h3-js';

const lat = 51.5, lon = -0.1;  // London

latLngToCell(lat, lon, 5)  // → '85194ad3fffffff'  (HISTORY res)
latLngToCell(lat, lon, 7)  // → '87194ad33ffffff'  (LIVE res)

gridDisk('87194ad33ffffff', 1)
// → 7 cells (the cell itself + 6 hex neighbors)

latLngToCell(51.54, lon, 7)  // → '87194ad36ffffff'
// Cell changed at ~4.5 km shift north: boundary crossing confirmed
```

Key observation: res=5 and res=7 cells share a common prefix (`85194ad3` vs `87194ad3`), confirming the H3 hierarchy. A res=5 cell contains all its child res=7 cells.

---

## First-ping run

A fresh event for `def456` at `lat=51.5, lon=-0.1` with `time_position` set to `Date.now()` was injected and consumed. Consumer log:

```json
{
  "level": "info",
  "message": "position persisted",
  "entity_id": "def456",
  "history_geo_cell": "85194ad3fffffff",
  "live_geo_cell": "87194ad33ffffff",
  "live_state_accepted": true,
  "offset": "10"
}
```

`live_state_accepted: true` confirms the Lua guard accepted the write (timestamp was newer than anything previously in the hash).

Hash inspection after run:

```bash
docker exec sentinel-redis redis-cli HGETALL entity:live:def456
```

```
live_geo_cell    87194ad33ffffff
last_seen_ms     1787633201000
lat              51.5
lon              -0.1
...
```

`live_geo_cell` field now present in the hash.

Sorted set inspection:

```bash
docker exec sentinel-redis redis-cli ZRANGEBYSCORE "geo-cell:87194ad33ffffff" -inf +inf WITHSCORES
```

```
def456
1787633201000
```

Entity is a member with score equal to source event time.

TimescaleDB inspection:

```sql
SELECT entity_id, geo_cell, lat, lon FROM position_history
WHERE entity_id = 'def456' ORDER BY observed_at DESC LIMIT 1;
```

```
 entity_id |    geo_cell     | lat  | lon
-----------+-----------------+------+------
 def456    | 85194ad3fffffff | 51.5 | -0.1
```

`geo_cell` populated with the res=5 history cell. Older rows from prior checkpoints retain `NULL` — correct, no backfill.

---

## Cell boundary crossing

A second event for `def456` was injected at `lat=51.54` (approximately 4.5 km north), with a timestamp 60 seconds newer than the previous one. Consumer log:

```json
{
  "level": "info",
  "message": "position persisted",
  "entity_id": "def456",
  "history_geo_cell": "85195da7fffffff",
  "live_geo_cell": "87194ad36ffffff",
  "live_state_accepted": true,
  "offset": "11"
}
```

Both cells changed: the history cell is now `85195da7fffffff` (different res=5 region) and the live cell is `87194ad36ffffff`.

Sorted set verification:

```bash
# Old cell — should be empty
docker exec sentinel-redis redis-cli ZRANGEBYSCORE "geo-cell:87194ad33ffffff" -inf +inf WITHSCORES
# (empty)

# New cell — should contain def456
docker exec sentinel-redis redis-cli ZRANGEBYSCORE "geo-cell:87194ad36ffffff" -inf +inf WITHSCORES
# def456
# 1787633584000

# Hash live_geo_cell — should reflect new cell
docker exec sentinel-redis redis-cli HGET entity:live:def456 live_geo_cell
# 87194ad36ffffff
```

`ZREM` removed `def456` from `87194ad33ffffff`. `ZADD` added it to `87194ad36ffffff`. Hash reflects the new cell.

---

## Stale-event behaviour

Replaying all previously committed offsets (via `rpk group seek --to start`) produced `live_state_accepted: false` for all `def456` records, because the hash already held a newer `last_seen_ms`. Both cells were logged correctly (H3 computation ran regardless), but the sorted sets were not mutated.

```json
{
  "level": "warn",
  "message": "live state not updated — stale event",
  "entity_id": "def456",
  "timestamp_ms": 1700000100000
}
```

The `position_history` idempotent insert (ON CONFLICT DO NOTHING) ran regardless of the live state outcome, preserving durable history.

---

## Observations

| Concept | Observed |
| --- | --- |
| `live_geo_cell` written to hash on first accepted ping | `HGET entity:live:def456 live_geo_cell` returns cell ID |
| `geo-cell:{cell}` sorted set created on first ping | `ZRANGEBYSCORE` returns entity with correct score |
| `history_geo_cell` written to `position_history.geo_cell` | `SELECT geo_cell FROM position_history` returns res=5 cell |
| Boundary crossing: ZREM from old cell | Old sorted set empty after crossing |
| Boundary crossing: ZADD to new cell | New sorted set contains entity |
| Boundary crossing: hash live_geo_cell updated | Reflects new cell |
| Stale event: sorted sets not mutated | Guard returned 0; ZREM and ZADD skipped |
| Older position_history rows retain NULL geo_cell | No backfill; correct per data model |

---

## Commands to reproduce

```bash
# Start infrastructure
make up
make migrate

# Inject fresh event (timestamp = now)
node --input-type=module << 'EOF'
import { Kafka, Partitioners } from 'kafkajs';
const k = new Kafka({ clientId: 'test-inject', brokers: ['localhost:9092'], logLevel: 0 });
const p = k.producer({ createPartitioner: Partitioners.LegacyPartitioner });
await p.connect();
const now_s = Math.floor(Date.now() / 1000);
await p.send({ topic: 'adsb.raw', messages: [
  { key: 'def456', value: JSON.stringify({
      icao24: 'def456', callsign: 'BA100   ',
      lat: 51.5, lon: -0.1, time_position: now_s,
      on_ground: false, velocity: 220.5, true_track: 270, vertical_rate: 3.5,
      baro_altitude: 10000, geo_altitude: 10150,
      squawk: '1234', spi: false, position_source: 0, category: 3,
      last_contact: now_s, fetched_at_ms: Date.now()
  }) }
]});
await p.disconnect();
EOF

# Run consumer
cd services/position-consumer
FROM_BEGINNING=false pnpm run consumer

# Inspect hash, sorted set, and history
docker exec sentinel-redis redis-cli HGET entity:live:def456 live_geo_cell
docker exec sentinel-redis redis-cli ZRANGEBYSCORE "geo-cell:$(docker exec sentinel-redis redis-cli HGET entity:live:def456 live_geo_cell)" -inf +inf WITHSCORES
docker exec sentinel-timescaledb psql -U sentinel sentinel -c \
  "SELECT entity_id, geo_cell, lat, lon FROM position_history WHERE entity_id = 'def456' ORDER BY observed_at DESC LIMIT 1;"

# Boundary crossing: inject event at lat 51.54 with newer timestamp
# Then re-run consumer and verify:
#   ZRANGEBYSCORE geo-cell:{old_cell} -inf +inf  → empty
#   ZRANGEBYSCORE geo-cell:{new_cell} -inf +inf WITHSCORES  → def456 with new score
#   HGET entity:live:def456 live_geo_cell  → new cell
```
