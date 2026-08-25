# Redis Live State Debrief

---

## Infrastructure status

| Container | Status |
| --- | --- |
| sentinel-redpanda | healthy |
| sentinel-timescaledb | healthy |
| sentinel-redis | healthy |
| sentinel-neo4j | healthy |

---

## Setup

The same four-record test set was injected into `adsb.raw`:

```text
offset 4  parse_error       — "this is not json"
offset 5  missing_entity_id — valid JSON, no icao24 field
offset 6  no_position       — icao24: abc999, lat/lon/time_position null
offset 7  valid             — full ADS-B record, entity_id: def456
```

Only offset 7 produces a live state write. The other three records are either unprocessable or have no position.

---

## First run

```bash
cd services/position-consumer
FROM_BEGINNING=false pnpm run consumer
```

Consumer output (relevant lines):

```json
{"level":"info","message":"consumer starting","features":"raw-events,position-history,redis-live-state"}
{"level":"warn","message":"record routed to dlq","rejection_reason":"parse_error: ...","source_offset":"4"}
{"level":"warn","message":"record routed to dlq","rejection_reason":"missing_entity_id: ...","source_offset":"5"}
{"level":"warn","message":"skipping record with no position","entity_id":"abc999","offset":"6"}
{"level":"info","message":"position persisted","entity_id":"def456","timestamp_ms":1700000100000,"lat":51.5,"lon":-0.1,"altitude_m":10150,"callsign":"BA100","live_state_accepted":true,"offset":"7"}
```

`live_state_accepted: true` confirms the Lua guard ran and accepted the write (new entity — no prior `last_seen_ms` in the hash).

---

## Hash inspection

```bash
docker exec sentinel-redis redis-cli HGETALL entity:live:def456
```

Output:

```
 1) "last_seen_ms"
 2) "1700000100000"
 3) "entity_type"
 4) "aircraft"
 5) "lat"
 6) "51.5"
 7) "lon"
 8) "-0.1"
 9) "altitude_m"
10) "10150"
11) "speed_mps"
12) "113.37..."
13) "course_deg"
14) "270"
15) "heading_deg"
16) ""
17) "vertical_rate_mps"
18) "3.5"
19) "on_ground"
20) "false"
21) "navigation_status"
22) ""
23) "callsign"
24) "BA100"
25) "entity_subtype"
26) "3"
27) "provider"
28) "opensky"
```

`heading_deg` and `navigation_status` are empty strings — null fields stored as `''` so readers always get a string.

---

## TTL inspection

```bash
docker exec sentinel-redis redis-cli TTL entity:live:def456
```

Output:

```
86399
```

24-hour TTL was set. The value decrements from 86400 each second. Confirmed that `EXPIRE` was called inside the Lua script on the accepted write.

---

## Stale event experiment

The consumer group was seeked to offset 0 to force replay of all 8 records from the beginning:

```bash
docker exec sentinel-redpanda rpk group seek position-consumer --to start --topics adsb.raw
```

Consumer restarted. All 8 records were reprocessed. For offset 7 (the valid record for `def456`), the log showed:

```json
{"level":"warn","message":"live state not updated — stale event","entity_id":"def456","timestamp_ms":1700000100000}
```

`live_state_accepted: false` — the Lua guard read the existing `last_seen_ms` of `1700000100000`, compared it to the incoming `1700000100000`, found `current >= incoming` (equal), and returned `0`. The hash was not overwritten.

Hash state after replay:

```bash
docker exec sentinel-redis redis-cli HGET entity:live:def456 last_seen_ms
```

```
1700000100000
```

Unchanged. `position_history` still received its idempotent insert (hit `ON CONFLICT DO NOTHING`). Durable history is unaffected by the live state rejection.

---

## Observations

| Concept | Observed |
| --- | --- |
| New entity accepted | `live_state_accepted: true`; all hash fields written; TTL set to 86400 |
| Hash fields for null values stored as `''` | `heading_deg` and `navigation_status` return empty string, not nil |
| TTL refreshed on accepted write | `TTL` command returns ~86400 immediately after write |
| Stale event (equal timestamp) rejected | Guard returns `0`; log shows `live state not updated — stale event` |
| Hash unchanged after stale write | `HGET last_seen_ms` unchanged after replay |
| `position_history` unaffected by stale guard | Durable row exists regardless of live state outcome |
| Offset committed regardless of stale result | Consumer advances; stale rejection is not an error |

---

## Commands to reproduce

```bash
# Start infrastructure
make up
make migrate

# Inject test records (same set as persistence validation)
cd services/position-consumer
node --input-type=module << 'EOF'
import { Kafka, Partitioners } from 'kafkajs';
const k = new Kafka({ clientId: 'test-inject', brokers: ['localhost:9092'], logLevel: 0 });
const p = k.producer({ createPartitioner: Partitioners.LegacyPartitioner });
await p.connect();
await p.send({ topic: 'adsb.raw', messages: [
  { key: 'err1',   value: 'this is not json' },
  { key: 'err2',   value: JSON.stringify({ callsign: 'TEST' }) },
  { key: 'abc999', value: JSON.stringify({ icao24: 'abc999', lat: null, lon: null, time_position: null, on_ground: false, velocity: null, true_track: null, vertical_rate: null, baro_altitude: null, geo_altitude: null, squawk: null, spi: false, position_source: 0, category: null, last_contact: 1700000000, fetched_at_ms: 1700000001000 }) },
  { key: 'def456', value: JSON.stringify({ icao24: 'def456', callsign: 'BA100   ', lat: 51.5, lon: -0.1, time_position: 1700000100, on_ground: false, velocity: 220.5, true_track: 270, vertical_rate: 3.5, baro_altitude: 10000, geo_altitude: 10150, squawk: '1234', spi: false, position_source: 0, category: 3, last_contact: 1700000100, fetched_at_ms: 1700000101000 }) },
]});
await p.disconnect();
EOF

# Run consumer
FROM_BEGINNING=false pnpm run consumer

# Inspect live state
docker exec sentinel-redis redis-cli HGETALL entity:live:def456
docker exec sentinel-redis redis-cli TTL entity:live:def456

# Stale event experiment
docker exec sentinel-redpanda rpk group seek position-consumer --to start --topics adsb.raw
FROM_BEGINNING=false pnpm run consumer

# Confirm hash unchanged
docker exec sentinel-redis redis-cli HGET entity:live:def456 last_seen_ms
```
