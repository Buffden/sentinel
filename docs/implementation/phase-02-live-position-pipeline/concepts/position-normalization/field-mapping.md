# Field Mapping: OpenSky → Canonical

How each OpenSky state-vector field maps to `NormalizedPosition`, and why.

---

## Source field names

OpenSky returns state vectors as positional arrays. The poller maps index positions to named fields and publishes them verbatim. The canonical names come from `normalize.ts`, not from OpenSky's API documentation.

| OpenSky field | Canonical field | Notes |
| --- | --- | --- |
| `icao24` | `entity_id` | ICAO 24-bit address; stable identity for the aircraft |
| `time_position` | `timestamp_ms` | `time_position * 1000`; source event time in ms |
| `lat` | `lat` | Degrees; no conversion |
| `lon` | `lon` | Degrees; no conversion |
| `velocity` | `speed_mps` | Already in m/s from OpenSky |
| `true_track` | `course_deg` | Degrees clockwise from north |
| `vertical_rate` | `vertical_rate_mps` | m/s; positive = climbing |
| `on_ground` | `on_ground` | Boolean surface indicator |
| `baro_altitude` | `baro_altitude_m` | Barometric; metres above MSL |
| `geo_altitude` | `geo_altitude_m` | GNSS; metres |
| `last_contact` | `last_contact_ms` | `last_contact * 1000`; last transponder message |
| `squawk` | `squawk` | 4-digit transponder code; empty string → null |
| `spi` | `spi` | Special Position Identification pulse |
| `position_source` | `position_source` | 0=ADS-B 1=ASTERIX 2=MLAT 3=FLARM |
| `callsign` | `callsign` | Trimmed; empty or whitespace-only → null |
| `category` | `entity_subtype` + `provider_category` | See category mapping below |

---

## Fields with no OpenSky equivalent (always null for ADS-B)

| Canonical field | Always null | Reason |
| --- | --- | --- |
| `heading_deg` | Yes | ADS-B does not separate heading from course |
| `navigation_status` | Yes | AIS-only (NAVSTAT integer) |
| `rate_of_turn` | Yes | AIS-only (ROT) |
| `position_accuracy` | Yes | AIS-only (PAC flag) |
| `destination` | Yes | AIS-only |
| `eta` | Yes | AIS-only |
| `draught_m` | Yes | AIS-only |

---

## Composite altitude

Three altitude fields exist because providers measure altitude differently:

```
geo_altitude_m   — GNSS; directly measured from satellites
baro_altitude_m  — barometric; calibrated to standard pressure
altitude_m       — composite: geo_altitude_m ?? baro_altitude_m
```

`altitude_m` is the preferred single value for most consumers. The raw fields are preserved so downstream code can reason about which measurement was used (relevant for aircraft crossing altimeter calibration zones).

---

## Callsign trimming

OpenSky pads callsigns to a fixed width with trailing spaces: `"BA100   "`. The canonical field stores the trimmed value `"BA100"`. The raw payload in `raw_events` preserves the original padded string.

Trimming happens in `normalize.ts`, not in the poller. The poller's job is provider fidelity — it preserves the source exactly. Normalization is the consumer's responsibility.

Empty-after-trim callsigns become null. A callsign of all spaces means the aircraft is not squawking a callsign; null is the correct canonical representation.

---

## Category → entity_subtype mapping

OpenSky `category` is only present when the poller URL includes `extended=1`. It is an integer code for the ADS-B emitter category.

| Category | entity_subtype |
| --- | --- |
| 2–7 | `fixed_wing` (various size/performance classes) |
| 8 | `rotorcraft` |
| 10 | `lighter_than_air` |
| 14 | `uav` |
| 0, 1, 9, 11–13, 15–20 | `unknown` |
| null (extended=1 not in URL) | null |

`provider_category` stores the raw integer as a string alongside `entity_subtype`. Downstream code that needs the original code can use it without re-parsing.

---

## timestamp_ms is source event time

`time_position` is when the aircraft's transponder produced this position reading. Multiplied by 1000 to get milliseconds.

`fetched_at_ms` (in the raw payload) is when the poller fetched this data from OpenSky. It is an operational timestamp and is explicitly excluded from `NormalizedPosition`. Using `fetched_at_ms` as `timestamp_ms` would make replay non-deterministic: a record replayed hours later would have a different `timestamp_ms` than on first pass, breaking idempotency identities and Redis monotonic guards.

---

## Fixed fields for all ADS-B records

```
entity_type: 'aircraft'
source:       'adsb'
provider:     'opensky'
```

These do not come from the payload. They are set by the normalization function because they are structural facts about what this pipeline processes. If a new source (AIS, satellite) is added, it gets its own normalization function.
