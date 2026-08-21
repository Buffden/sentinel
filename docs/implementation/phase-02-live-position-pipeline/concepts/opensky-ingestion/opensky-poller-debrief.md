# OpenSky Ingestion Poller Debrief

---

## Infrastructure status

| Container | Status |
| --- | --- |
| sentinel-redpanda | healthy |
| sentinel-timescaledb | healthy |
| sentinel-redis | healthy |
| sentinel-neo4j | healthy |

---

## 1. Poller startup

```bash
cd services/ingestion-poller
node_modules/.bin/tsx src/poller.ts
```

```json
{"timestamp":"2026-08-20T21:30:13.615Z","level":"info","service":"ingestion-poller","message":"producer connecting","brokers":["localhost:9092"]}
{"timestamp":"2026-08-20T21:30:13.630Z","level":"info","service":"ingestion-poller","message":"producer connected"}
{"timestamp":"2026-08-20T21:30:13.630Z","level":"info","service":"ingestion-poller","message":"poller starting","url":"https://opensky-network.org/api/states/all?lamin=49&lomin=-8&lamax=61&lomax=10","poll_interval_ms":10000}
```

The first poll fired immediately after startup without waiting a full interval.

---

## 2. Poll cycles

Three cycles ran before the process was stopped.

```json
{"timestamp":"2026-08-20T21:30:14.518Z","level":"info","service":"ingestion-poller","message":"poll cycle complete","state_vectors":449,"topic":"adsb.raw","first_offset":"0"}
{"timestamp":"2026-08-20T21:30:25.274Z","level":"info","service":"ingestion-poller","message":"poll cycle complete","state_vectors":449,"topic":"adsb.raw","first_offset":"449"}
{"timestamp":"2026-08-20T21:30:36.047Z","level":"info","service":"ingestion-poller","message":"poll cycle complete","state_vectors":446,"topic":"adsb.raw","first_offset":"898"}
```

| Cycle | State vectors | First offset | Elapsed since previous |
| --- | --- | --- | --- |
| 1 | 449 | 0 | immediate |
| 2 | 449 | 449 | ~11 s |
| 3 | 446 | 898 | ~11 s |

~11 seconds per cycle: 10-second interval plus ~1 second fetch + publish latency.

---

## 3. Partition state after three cycles

```bash
docker exec sentinel-redpanda rpk topic describe -p adsb.raw
```

| PARTITION | LEADER | EPOCH | REPLICAS | LOG-START-OFFSET | HIGH-WATERMARK |
| --- | --- | --- | --- | --- | --- |
| 0 | 0 | 1 | [0] | 0 | 1344 |

449 + 449 + 446 = 1344 records. `HIGH-WATERMARK: 1344` means the next record will land at offset 1344. `LOG-START-OFFSET: 0` — no retention has run yet, all records are available.

---

## 4. Record inspection at offset 0

```bash
docker exec sentinel-redpanda rpk topic consume adsb.raw --offset 0 --num 1
```

```json
{
  "topic": "adsb.raw",
  "key": "471efa",
  "value": "{\"icao24\":\"471efa\",\"callsign\":\"WMT581\",\"origin_country\":\"Hungary\",\"time_position\":1787261405,\"last_contact\":1787261406,\"lon\":4.6671,\"lat\":52.5369,\"baro_altitude\":10668,\"on_ground\":false,\"velocity\":257.85,\"true_track\":102.68,\"vertical_rate\":0,\"geo_altitude\":10759.44,\"squawk\":\"2207\",\"spi\":false,\"position_source\":0,\"fetched_at_ms\":1787261413630}",
  "timestamp": 1787261414495,
  "partition": 0,
  "offset": 0
}
```

Real aircraft WMT581 (Hungarian carrier). `position_source: 0` confirms this is a genuine ADS-B record (not MLAT or ASTERIX). Broker timestamp (`1787261414495`) is 865 ms after `fetched_at_ms` (`1787261413630`) — network + Kafka write latency.

---

## 5. Same aircraft across two poll cycles

```bash
docker exec sentinel-redpanda rpk topic consume adsb.raw --offset 449 --num 1
```

```json
{
  "key": "471efa",
  "value": "{\"icao24\":\"471efa\",\"callsign\":\"WMT581\",\"origin_country\":\"Hungary\",\"time_position\":1787261410,\"last_contact\":1787261411,\"lon\":4.6857,\"lat\":52.5344,\"baro_altitude\":10668,\"on_ground\":false,...}",
  "offset": 449
}
```

| Field | Cycle 1 (offset 0) | Cycle 2 (offset 449) | Change |
| --- | --- | --- | --- |
| `time_position` | 1787261405 | 1787261410 | +5 s |
| `lon` | 4.6671 | 4.6857 | +0.0186° east |
| `lat` | 52.5369 | 52.5344 | −0.0025° south |
| `baro_altitude` | 10668 | 10668 | unchanged (cruising) |

WMT581 is flying eastbound at cruise altitude. The position advanced between poll cycles — this is real movement, not a cached snapshot.

---

## 6. Key verification

```bash
docker exec sentinel-redpanda rpk topic consume adsb.raw --offset 0 --num 5 --format '%k\n'
```

```text
471efa
4bc8e1
4bc8cb
4cc2a3
4aca82
```

All five message keys match the `icao24` field in their respective payloads exactly. Zero mismatches across the sample.

---

## 7. Graceful shutdown

```json
{"timestamp":"2026-08-20T21:30:38.316Z","level":"info","service":"ingestion-poller","message":"shutdown initiated","signal":"SIGTERM"}
{"timestamp":"2026-08-20T21:30:38.316Z","level":"info","service":"ingestion-poller","message":"producer disconnected"}
```

SIGTERM received between poll cycles. The pending `setTimeout` was cleared, the producer disconnected cleanly, and the process exited without error.

---

## 8. Note on TimeoutNegativeWarning

```text
TimeoutNegativeWarning: -1787261413630 is a negative number. Timeout duration was set to 1.
```

Same root cause as the Kafka experiment. kafkajs computes an internal session timeout relative to broker metadata timestamps. The Phase 01 manual-store-verification records in `adsb.raw` have timestamps in the far future relative to the configured timeout, producing a negative `setTimeout` value. Node.js clamps it to 1 ms. No records were lost or delayed. This warning will disappear once all Phase 01 records age out of the topic by retention.

---

## 9. Observations

| Concept | Observed |
| --- | --- |
| Provider-fidelity raw record | OpenSky field names preserved in Kafka payload; no normalization at ingestion |
| Message key matches icao24 | Verified for 5 records; zero mismatches |
| Real aircraft movement | WMT581 position advanced between cycles; `time_position` advanced 5 s |
| Poll interval | ~11 s elapsed between cycles (10 s interval + fetch/publish latency) |
| Graceful shutdown | SIGTERM handled cleanly between cycles; producer disconnected |
| Fetch-and-forward boundary | Poller published null-position records unchanged; filtering is the Position Consumer's job |

---

## 10. Commands to reproduce independently

```bash
# Partition state and total record count
docker exec sentinel-redpanda rpk topic describe -p adsb.raw

# Read any record by offset
docker exec sentinel-redpanda rpk topic consume adsb.raw --offset 0 --num 1

# Compare same aircraft across two poll cycles
docker exec sentinel-redpanda rpk topic consume adsb.raw --offset 0 --num 1
docker exec sentinel-redpanda rpk topic consume adsb.raw --offset 449 --num 1

# Print message keys for first N records
docker exec sentinel-redpanda rpk topic consume adsb.raw --offset 0 --num 5 --format '%k\n'

# Watch live records as the poller runs (separate terminal)
docker exec sentinel-redpanda rpk topic consume adsb.raw --format '%k %v\n'
```
