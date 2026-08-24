# Position Normalization Debrief

---

## Infrastructure status

| Container | Status |
| --- | --- |
| sentinel-redpanda | healthy |
| sentinel-timescaledb | healthy |
| sentinel-redis | healthy |
| sentinel-neo4j | healthy |

---

## 1. Consumer startup

```bash
cd services/position-consumer
npm run consumer
```

```json
{"timestamp":"2026-08-22T...","level":"info","service":"position-consumer","message":"consumer starting","brokers":["localhost:9092"],"group":"position-consumer","source_topic":"adsb.raw","from_beginning":false}
{"level":"info","message":"consumer connected"}
{"level":"info","message":"consumer subscribed","topic":"adsb.raw"}
```

The consumer started with `FROM_BEGINNING=false` so it picked up only new records produced by the live poller. It did not attempt to replay earlier Snappy-compressed records from prior sessions.

---

## 2. Real ADS-B record normalized

The poller produced a poll cycle while the consumer was running. One of the aircraft in the batch:

```json
{
  "level": "info",
  "service": "position-consumer",
  "message": "position normalized",
  "entity_id": "4ca84c",
  "entity_type": "aircraft",
  "timestamp_ms": 1787369118000,
  "lat": 51.4762,
  "lon": -0.4621,
  "speed_mps": 68.12,
  "course_deg": 97.83,
  "altitude_m": 762,
  "baro_altitude_m": 762,
  "geo_altitude_m": null,
  "vertical_rate_mps": 0,
  "on_ground": false,
  "callsign": "EZY456",
  "squawk": "3611",
  "entity_subtype": "fixed_wing",
  "provider_category": "3",
  "provider": "opensky",
  "source": "adsb"
}
```

Notable:

- `timestamp_ms: 1787369118000` — `time_position` (1787369118) multiplied by 1000. Source event time.
- `altitude_m: 762` — taken from `baro_altitude_m` because `geo_altitude_m` is null. The composite fallback worked.
- `callsign: "EZY456"` — trimmed from the padded raw value `"EZY456  "`.
- `entity_subtype: "fixed_wing"` — category 3 maps to the fixed_wing class.
- `vertical_rate_mps: 0` — aircraft is level.

---

## 3. no_position record

Several records in the same poll cycle had null position fields (aircraft with no GPS fix):

```json
{"level":"warn","service":"position-consumer","message":"skipping record with no position","entity_id":"407289","offset":"1021"}
```

These were skipped. No database write, no DLQ. The entity_id is present in the warn log so the skip rate can be monitored per aircraft.

---

## 4. Consumer group state during polling

```bash
docker exec sentinel-redpanda rpk group describe position-consumer
```

| CURRENT-OFFSET | LOG-END-OFFSET | LAG |
| --- | --- | --- |
| 1344 | 1344 | 0 |

The consumer kept pace with the poller. Lag held at 0 throughout a polling session.

---

## 5. Verified fields from the raw payload

The raw record at the same offset in the broker:

```bash
docker exec sentinel-redpanda rpk topic consume adsb.raw --offset 1020 --num 1
```

```json
{
  "icao24": "4ca84c",
  "callsign": "EZY456  ",
  "time_position": 1787369118,
  "lat": 51.4762,
  "lon": -0.4621,
  "baro_altitude": 762,
  "geo_altitude": null,
  "velocity": 68.12,
  "true_track": 97.83,
  "vertical_rate": 0,
  "on_ground": false,
  "squawk": "3611",
  "category": 3,
  "fetched_at_ms": 1787369121430
}
```

Mapping confirmed:

| Raw field | Raw value | Canonical field | Canonical value |
| --- | --- | --- | --- |
| `icao24` | `"4ca84c"` | `entity_id` | `"4ca84c"` |
| `time_position` | `1787369118` | `timestamp_ms` | `1787369118000` |
| `callsign` | `"EZY456  "` | `callsign` | `"EZY456"` |
| `baro_altitude` | `762` | `baro_altitude_m` | `762` |
| `geo_altitude` | `null` | `geo_altitude_m` | `null` |
| — | — | `altitude_m` | `762` (baro fallback) |
| `velocity` | `68.12` | `speed_mps` | `68.12` |
| `true_track` | `97.83` | `course_deg` | `97.83` |
| `category` | `3` | `entity_subtype` | `"fixed_wing"` |
| `category` | `3` | `provider_category` | `"3"` |
| `fetched_at_ms` | `1787369121430` | (not in canonical) | — |

`fetched_at_ms` does not appear in `NormalizedPosition`. It is an operational timestamp — the difference between `fetched_at_ms` and `timestamp_ms` (3430 ms here) represents OpenSky's update lag plus poller fetch latency.

---

## 6. Observations

| Concept | Observed |
| --- | --- |
| Pure normalization function | normalizeAdsbRaw called with raw string; returns typed result; no I/O |
| timestamp_ms is source event time | time_position * 1000; fetched_at_ms excluded from canonical record |
| Altitude composite | geo_altitude null → altitude_m falls back to baro_altitude_m |
| Callsign trimming | Raw "EZY456  " → canonical "EZY456" |
| entity_subtype mapping | category 3 → fixed_wing |
| no_position skip | Null lat/lon/time records warn-logged, not DLQ'd, not persisted |
| Consumer lag | Held at 0 throughout session; consumer keeps pace with 10-second poll cycle |

---

## 7. Commands to reproduce independently

```bash
# Start consumer (picks up from current committed offset)
cd services/position-consumer
FROM_BEGINNING=false npm run consumer

# Read the same record directly from the broker to compare
docker exec sentinel-redpanda rpk topic consume adsb.raw --offset <N> --num 1

# Consumer group committed position
docker exec sentinel-redpanda rpk group describe position-consumer

# Count how many records the poller has produced
docker exec sentinel-redpanda rpk topic describe -p adsb.raw
```
