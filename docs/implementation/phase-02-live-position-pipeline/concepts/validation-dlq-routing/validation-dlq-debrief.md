# Validation and DLQ Routing Debrief

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

Four test records were injected into a clean `adsb.raw` topic using a kafkajs producer (no Snappy compression). The consumer was started with `FROM_BEGINNING=true` so it picked up all four records immediately on join.

```text
offset 0  parse_error      — "this is not json"
offset 1  missing_entity_id — valid JSON, no icao24 field
offset 2  no_position       — icao24 present, lat/lon/time_position all null
offset 3  valid             — full ADS-B record, category=3 (small fixed-wing)
```

### Why kafkajs injection, not rpk

`rpk topic produce` compresses records with Snappy by default. kafkajs v2 does not implement Snappy decompression. Even when the consumer is asked to start from an offset after the compressed records, Redpanda returns the full segment batch from its internal segment boundary, which can include earlier Snappy-compressed messages. The consumer crashes with `KafkaJSNotImplemented: Snappy compression not implemented`.

The fix for this session: delete and recreate the topic, then inject all test records via a kafkajs producer script. That produces uncompressed batches that the consumer can decode. Once the poller is running continuously and its records fill the topic, this is no longer a concern — the poller uses kafkajs and produces uncompressed records.

---

## Consumer startup

```bash
cd services/position-consumer
npm run consumer
```

```json
{"level":"info","message":"consumer starting","group":"position-consumer","source_topic":"adsb.raw","dlq_topic":"adsb.dlq","consumer_id":"Harshwardhans-MacBook-Pro.local-20820","from_beginning":true,"checkpoint":"CP4"}
{"level":"info","message":"dlq producer connected"}
{"level":"info","message":"consumer connected"}
{"level":"info","message":"consumer subscribed","topic":"adsb.raw"}
```

---

## Experiment 1: parse_error (offset 0)

Input: `this is not json`

```json
{"level":"warn","message":"record routed to dlq","rejection_reason":"parse_error: Unexpected token 'h', \"this is not json\" is not valid JSON","source_topic":"adsb.raw","source_partition":0,"source_offset":"0","dlq_topic":"adsb.dlq"}
```

DLQ record received:

```json
{
  "raw_payload": "this is not json",
  "rejection_reason": "parse_error: Unexpected token 'h', \"this is not json\" is not valid JSON",
  "source_topic": "adsb.raw",
  "source_partition": 0,
  "source_offset": "0",
  "consumer_id": "Harshwardhans-MacBook-Pro.local-20820",
  "timestamp_ms": 1787553985558
}
```

---

## Experiment 2: missing_entity_id (offset 1)

Input: `{"callsign":"TEST123","lat":51.5,"lon":-0.1,"time_position":1700000000}` — valid JSON but no `icao24`.

```json
{"level":"warn","message":"record routed to dlq","rejection_reason":"missing_entity_id: icao24 field missing or empty","source_topic":"adsb.raw","source_partition":0,"source_offset":"1","dlq_topic":"adsb.dlq"}
```

The JSON parsed cleanly, but `normalizeAdsbRaw` returned `{ ok: false, kind: 'missing_entity_id' }`. DLQ envelope written with correct source coordinates.

---

## Experiment 3: no_position (offset 2)

Input: full ADS-B envelope with `"icao24":"abc999"`, all of `lat`, `lon`, `time_position` set to `null`.

```json
{"level":"warn","message":"skipping record with no position","entity_id":"abc999","offset":"2"}
```

No DLQ record. The consumer skipped with a warn log and committed offset 3 (next). `adsb.dlq` high watermark did not change.

This confirms the distinction between `no_position` (skip) and `parse_error`/`missing_entity_id` (DLQ). A valid source record with no GPS fix is not a broken record.

---

## Experiment 4: valid record (offset 3)

Input: full ADS-B record — `icao24: "def456"`, `callsign: "BA100   "` (padded), `lat: 51.5`, `lon: -0.1`, `category: 3`.

```json
{
  "level": "info",
  "message": "position normalized",
  "entity_id": "def456",
  "entity_type": "aircraft",
  "timestamp_ms": 1700000100000,
  "lat": 51.5,
  "lon": -0.1,
  "speed_mps": 220.5,
  "course_deg": 270,
  "altitude_m": 10150,
  "baro_altitude_m": 10000,
  "geo_altitude_m": 10150,
  "vertical_rate_mps": 3.5,
  "on_ground": false,
  "callsign": "BA100",
  "squawk": "1234",
  "entity_subtype": "fixed_wing",
  "provider_category": "3",
  "provider": "opensky",
  "source": "adsb",
  "offset": "3"
}
```

Notable:

- `callsign: "BA100"` — trailing spaces stripped by normalize.ts, not by the poller. Raw payload preserved `"BA100   "`.
- `altitude_m: 10150` — preferred geo_altitude (10150) over baro_altitude (10000).
- `entity_subtype: "fixed_wing"` — category 3 (small aircraft, 15500–75000 lbs) mapped correctly.
- `provider_category: "3"` — raw category integer preserved as string.
- `timestamp_ms: 1700000100000` — `time_position * 1000`; source event time, not processing time.

---

## Consumer group state after all four records

```bash
docker exec sentinel-redpanda rpk group describe position-consumer
```

| Field | Value |
| --- | --- |
| STATE | Stable |
| MEMBERS | 1 |
| CURRENT-OFFSET | 4 |
| LOG-END-OFFSET | 4 |
| LAG | 0 |

All four offsets committed. Consumer is alive and waiting for the next record.

---

## DLQ inspection

```bash
docker exec sentinel-redpanda rpk topic describe adsb.dlq -p
```

| PARTITION | HIGH-WATERMARK |
| --- | --- |
| 0 | 6 |

Six records total across the session (includes records from earlier test injections before the topic was reset). The two new records from this run are at the highest offsets.

```bash
docker exec sentinel-redpanda rpk topic consume adsb.dlq -o 0 -n 6
```

---

## Observations

| Concept | Observed |
| --- | --- |
| parse_error → DLQ | Non-JSON record produced rejection envelope in adsb.dlq with error detail |
| missing_entity_id → DLQ | JSON without icao24 produced rejection envelope; classified separately from parse_error |
| no_position → warn, not DLQ | Null lat/lon/time_position produced warn log only; DLQ high-watermark unchanged |
| valid record → position normalized | Full record produced normalized log with callsign trimmed and altitude composite applied |
| Offset committed unconditionally | Group CURRENT-OFFSET advanced through all four records including the two DLQ'd ones |
| Source partition in envelope | DLQ envelope carries source_partition: 0; unique log position reconstructable |
| Consumer ID in envelope | envelope carries hostname-pid; traceable to the instance that rejected the record |

---

## Commands to reproduce independently

```bash
# Inject test records (must use kafkajs, not rpk — see setup note above)
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

# Run the consumer
cd services/position-consumer
npm run consumer

# Check consumer group committed offsets
docker exec sentinel-redpanda rpk group describe position-consumer

# Inspect DLQ
docker exec sentinel-redpanda rpk topic consume adsb.dlq -o 0 -n 10

# Seek group to replay from a specific offset
docker exec sentinel-redpanda rpk group seek position-consumer --to <offset> --topics adsb.raw --allow-new-topics
```

---

## Note on Snappy compression and rpk

`rpk topic produce` uses Snappy compression by default. kafkajs v2 does not implement Snappy decompression. Even when fetching from an offset that points to an uncompressed record, Redpanda may return a fetch response that includes earlier Snappy-compressed record batches from the same segment. The consumer crashes with:

```
KafkaJSNotImplemented: Snappy compression not implemented
```

The consumer leaves the group silently. `rpk group describe` shows `STATE: Empty, MEMBERS: 0` even though the process is still alive.

Workaround: delete and recreate the topic to start a clean segment, then inject all test records via kafkajs. Any records produced by the kafkajs poller are also uncompressed. This issue only arises when mixing rpk-injected records and kafkajs consumers in the same topic segment.
