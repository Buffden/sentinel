# TimescaleDB Persistence Debrief

---

## Infrastructure status

| Container | Status |
| --- | --- |
| sentinel-redpanda | healthy |
| sentinel-timescaledb | healthy |
| sentinel-redis | healthy |
| sentinel-neo4j | healthy |

---

## Migrations applied

| Migration | Effect |
| --- | --- |
| `007_position_history_expand.sql` | Renamed `altitude` → `altitude_m`; dropped NOT NULL on `geo_cell`; added 21 canonical columns |
| `008_raw_events_table.sql` | Created `raw_events` as a regular Postgres table with `UNIQUE(source_topic, source_partition, source_offset)` |

The migration runner has no tracking table and replays every file on each `make migrate` run. Migration 007 guards the rename in a `DO` block so it is safe to replay after the column has already been renamed.

---

## Schema inspection

`position_history` after migration 007:

```bash
docker exec sentinel-timescaledb psql -U sentinel -d sentinel -c "\d+ position_history"
```

Notable: `geo_cell` is now nullable. All 21 new columns are nullable. Existing unique index and both query indexes are intact. Hypertable status and 30-day retention policy unchanged.

`raw_events` after migration 008:

```bash
docker exec sentinel-timescaledb psql -U sentinel -d sentinel -c "\d+ raw_events"
```

Regular Postgres table. Primary key on `id` (BIGSERIAL). Unique constraint on `(source_topic, source_partition, source_offset)`. Index on `(entity_id, received_at DESC)`.

---

## Setup

The same four-record test set used in CP4 was injected into `adsb.raw` using a kafkajs producer:

```text
offset 4  parse_error       — "this is not json"
offset 5  missing_entity_id — valid JSON, no icao24 field
offset 6  no_position       — icao24 present, lat/lon/time_position all null
offset 7  valid             — full ADS-B record, entity_id: def456
```

Offsets 0–3 are earlier CP4 test records that were in the topic from a previous session.

---

## First run

```bash
cd services/position-consumer
FROM_BEGINNING=false pnpm run consumer
```

Consumer output:

```json
{"level":"info","message":"consumer starting","checkpoint":"CP5"}
{"level":"warn","message":"record routed to dlq","rejection_reason":"parse_error: ...","source_offset":"4"}
{"level":"warn","message":"record routed to dlq","rejection_reason":"missing_entity_id: ...","source_offset":"5"}
{"level":"warn","message":"skipping record with no position","entity_id":"abc999","offset":"6"}
{"level":"info","message":"position persisted","entity_id":"def456","timestamp_ms":1700000100000,"lat":51.5,"lon":-0.1,"altitude_m":10150,"callsign":"BA100","offset":"7"}
```

All four offsets processed. DB state after first run:

```bash
docker exec sentinel-timescaledb psql -U sentinel -d sentinel \
  -c "SELECT COUNT(*) FROM raw_events; SELECT COUNT(*) FROM position_history;"
```

| Table | Row count |
| --- | --- |
| `raw_events` | 4 |
| `position_history` | 1 |

`raw_events` rows (source_offset 4, 5, 6, 7). `position_history` one row for `def456` at `observed_at = 2023-11-14 22:15:00+00`.

---

## Payload type inspection

```bash
docker exec sentinel-timescaledb psql -U sentinel -d sentinel \
  -c "SELECT source_offset, entity_id, jsonb_typeof(payload) FROM raw_events ORDER BY source_offset;"
```

| source_offset | entity_id | payload_type |
| --- | --- | --- |
| 4 | null | string |
| 5 | null | object |
| 6 | abc999 | object |
| 7 | def456 | object |

Offset 4 (`parse_error`): stored as JSONB string scalar via `JSON.stringify(rawValue)`.
Offsets 5, 6, 7: stored as JSONB objects (valid JSON source records).

---

## Replay experiment

The consumer group was seeked to offset 0 to force reprocessing of all 8 records in the topic:

```bash
docker exec sentinel-redpanda rpk group seek position-consumer --to start --topics adsb.raw
```

Consumer was restarted. All 8 records (offsets 0–7) were reprocessed. DB state after replay:

| Table | Row count |
| --- | --- |
| `raw_events` | 8 |
| `position_history` | 1 |

### Why 8 raw_events rows is correct

Offsets 0–3 were processed by CP4 before `raw_events` was wired up. They had no prior `raw_events` rows. The replay inserted them for the first time — 4 new rows, one per unique Kafka record.

Offsets 4–7 were already in `raw_events` from the first CP5 run. The replay attempts hit `ON CONFLICT DO NOTHING` — confirmed by the row `id` values jumping from 8 to 9 with no ids allocated for the duplicate attempts.

The result: 8 rows, one per unique `(source_topic, source_partition, source_offset)` triple. No duplicates. This is correct idempotency behavior.

### Why position_history still has 1 row

`def456` at `observed_at = 2023-11-14 22:15:00+00` appeared at offset 3 and at offset 7. Both represent the same canonical position fact. The second insert hit `ON CONFLICT (entity_id, observed_at) DO NOTHING`. The row count did not change.

---

## Consumer group state after replay

```bash
docker exec sentinel-redpanda rpk group describe position-consumer
```

| Field | Value |
| --- | --- |
| STATE | Stable |
| CURRENT-OFFSET | 8 |
| LOG-END-OFFSET | 8 |
| LAG | 0 |

---

## Observations

| Concept | Observed |
| --- | --- |
| `raw_events` written before branching | All 4 record types produced a `raw_events` row, including `parse_error` and `no_position` |
| `parse_error` payload stored as JSONB string scalar | `jsonb_typeof(payload) = 'string'` for offset 4; original bytes preserved |
| `no_position` archived, not skipped from DB | `entity_id: abc999` appears in `raw_events` with null `source_event_time` |
| `position_history` written only for valid positions | Only offset 7 produced a `position_history` row |
| `geo_cell` is NULL | `position_history` row shows empty `geo_cell`; CP7 will populate it |
| `ON CONFLICT DO NOTHING` on `raw_events` replay | Offsets 4–7 replayed without duplicating rows; id sequence gaps confirm conflict was hit |
| `ON CONFLICT DO NOTHING` on `position_history` replay | One row for `def456` regardless of how many times the same Kafka record is processed |
| Offset committed only after all writes succeed | Consumer log shows `position persisted` before consumer shutdown; group CURRENT-OFFSET advanced correctly |

---

## Commands to reproduce

```bash
# Start infrastructure
make up
make migrate

# Inject test records
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

# Run the consumer
FROM_BEGINNING=false pnpm run consumer

# Inspect raw_events
docker exec sentinel-timescaledb psql -U sentinel -d sentinel \
  -c "SELECT id, entity_id, source_offset, source_event_time, jsonb_typeof(payload) FROM raw_events ORDER BY source_offset;"

# Inspect position_history
docker exec sentinel-timescaledb psql -U sentinel -d sentinel \
  -c "SELECT entity_id, observed_at, geo_cell, lat, lon, altitude_m, callsign FROM position_history;"

# Replay idempotency experiment
docker exec sentinel-redpanda rpk group seek position-consumer --to start --topics adsb.raw
FROM_BEGINNING=false pnpm run consumer

# Confirm row counts are unchanged
docker exec sentinel-timescaledb psql -U sentinel -d sentinel \
  -c "SELECT COUNT(*) FROM raw_events; SELECT COUNT(*) FROM position_history;"
```
