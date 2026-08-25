# Downstream Publishing Debrief

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

A fresh ADS-B event for `def456` was injected into `adsb.raw` with `time_position` set to `Date.now() / 1000 + 120` (two minutes ahead of current time, ensuring the monotonic guard accepts it over all prior test events).

A Redis subscriber was started before the consumer to observe the pub/sub message in real time:

```bash
docker exec sentinel-redis redis-cli SUBSCRIBE position-updates
```

The consumer was then started with `FROM_BEGINNING=false` to process only the new event.

---

## Consumer log

```json
{
  "level": "info",
  "message": "position persisted",
  "entity_id": "def456",
  "entity_type": "aircraft",
  "timestamp_ms": 1787634583000,
  "lat": 51.5,
  "lon": -0.1,
  "speed_mps": 220.5,
  "course_deg": 270,
  "altitude_m": 10150,
  "callsign": "BA100",
  "history_geo_cell": "85194ad3fffffff",
  "live_geo_cell": "87194ad33ffffff",
  "live_state_accepted": true,
  "offset": "12"
}
```

`live_state_accepted: true` confirms the Lua guard accepted the write. Both publish calls completed before the log line was emitted and before offset commit.

---

## position-updates pub/sub (observed live)

The Redis subscriber received the message immediately after the consumer processed offset 12:

```
message
position-updates
{"entity_id":"def456","entity_type":"aircraft","timestamp_ms":1787634583000,"lat":51.5,"lon":-0.1,"altitude_m":10150,"speed_mps":220.5,"course_deg":270,"callsign":"BA100","live_geo_cell":"87194ad33ffffff"}
```

Lightweight payload: no provider, no squawk, no history_geo_cell, no null-valued optional fields. The `live_geo_cell` is present for API-side workspace scoping.

---

## position.normalized (Kafka)

```bash
docker exec sentinel-redpanda rpk topic consume position.normalized --num 1
```

```json
{
  "topic": "position.normalized",
  "key": "def456",
  "value": {
    "entity_id": "def456",
    "entity_type": "aircraft",
    "timestamp_ms": 1787634583000,
    "lat": 51.5,
    "lon": -0.1,
    "speed_mps": 220.5,
    "course_deg": 270,
    "heading_deg": null,
    "source": "adsb",
    "provider": "opensky",
    "altitude_m": 10150,
    "baro_altitude_m": 10000,
    "geo_altitude_m": 10150,
    "vertical_rate_mps": 3.5,
    "on_ground": false,
    "last_contact_ms": 1787634583000,
    "navigation_status": null,
    "rate_of_turn": null,
    "callsign": "BA100",
    "entity_subtype": "fixed_wing",
    "provider_category": "3",
    "squawk": "1234",
    "spi": false,
    "position_source": 0,
    "position_accuracy": null,
    "destination": null,
    "eta": null,
    "draught_m": null,
    "history_geo_cell": "85194ad3fffffff",
    "live_geo_cell": "87194ad33ffffff"
  },
  "partition": 0,
  "offset": 0
}
```

Key points:
- **key**: `def456` — entity_id used for partition affinity
- **null fields preserved as JSON null**: `heading_deg`, `navigation_status`, `rate_of_turn`, `position_accuracy`, `destination`, `eta`, `draught_m` all null (ADS-B aircraft, no AIS fields)
- **both H3 cells present**: `history_geo_cell` and `live_geo_cell` match the values logged by the consumer and stored in Redis/TimescaleDB
- **offset 0**: this topic had no prior messages; this is the first event published to `position.normalized`

---

## Observations

| Concept | Observed |
| --- | --- |
| `position.normalized` published to Kafka after every valid position | Topic consumed successfully; full canonical payload with both H3 cells |
| Key set to entity_id | `"key": "def456"` in rpk consume output |
| Null fields preserved as JSON null | `heading_deg: null` (not `""`) for ADS-B aircraft |
| `position-updates` pub/sub received by live subscriber | Lightweight payload arrived immediately after consumer processed the offset |
| pub/sub payload excludes non-map fields | No provider, squawk, history_geo_cell, or null optional fields in pub/sub message |
| `live_geo_cell` present in pub/sub payload | `"87194ad33ffffff"` for API workspace scoping |
| Consumer offset committed after both publishes | Consumer advanced to offset 13 (12 + 1) and shut down cleanly |

---

## Commands to reproduce

```bash
# Subscribe to position-updates in one terminal
docker exec sentinel-redis redis-cli SUBSCRIBE position-updates

# Inject a fresh event in another terminal (timestamp must be newer than hash)
node --input-type=module << 'EOF'
import { Kafka, Partitioners } from 'kafkajs';
const k = new Kafka({ clientId: 'test-inject', brokers: ['localhost:9092'], logLevel: 0 });
const p = k.producer({ createPartitioner: Partitioners.LegacyPartitioner });
await p.connect();
const now_s = Math.floor(Date.now() / 1000) + 120;
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

# Consume position.normalized
docker exec sentinel-redpanda rpk topic consume position.normalized --num 1
```
