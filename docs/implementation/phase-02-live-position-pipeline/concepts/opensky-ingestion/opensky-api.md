# OpenSky Network API

## The endpoint

```
GET https://opensky-network.org/api/states/all
```

Optional bounding box parameters narrow the response to a geographic area:

```
?lamin=49&lomin=-8&lamax=61&lomax=10
```

The Sentinel poller defaults to UK + Western Europe. Reducing the area lowers both response size and credit consumption for anonymous accounts.

---

## Response shape

OpenSky does not return an array of objects. It returns an array of arrays — each state vector is a positional array where the field at each index is defined by the API contract, not by a key.

```json
{
  "time": 1787261900,
  "states": [
    ["471efa", "WMT581  ", "Hungary", 1787261895, 1787261897, 4.6671, 52.5369, 10668.0, false, 257.85, 102.68, 0.0, null, 10759.44, "2207", false, 0],
    ...
  ]
}
```

`time` is the Unix timestamp of the response snapshot. `states` is null when there are no aircraft in the bounding box.

---

## Field index map

| Index | Field | Type | Notes |
| --- | --- | --- | --- |
| 0 | `icao24` | string | ICAO 24-bit address; Sentinel `entity_id` for aircraft |
| 1 | `callsign` | string \| null | Padded to 8 characters; trim before use |
| 2 | `origin_country` | string | |
| 3 | `time_position` | number \| null | Unix seconds; last time position (lat/lon) was updated |
| 4 | `last_contact` | number | Unix seconds; last time any transponder message arrived |
| 5 | `longitude` | number \| null | Decimal degrees; null if no recent position fix |
| 6 | `latitude` | number \| null | Decimal degrees; null if no recent position fix |
| 7 | `baro_altitude` | number \| null | Metres above mean sea level (barometric) |
| 8 | `on_ground` | boolean | True when the aircraft is on the ground |
| 9 | `velocity` | number \| null | Ground speed in m/s |
| 10 | `true_track` | number \| null | Heading in degrees clockwise from north |
| 11 | `vertical_rate` | number \| null | m/s; positive = climbing |
| 12 | `sensors` | int[] \| null | Receiver IDs; not needed downstream, skipped |
| 13 | `geo_altitude` | number \| null | Metres; GNSS altitude; may differ from barometric |
| 14 | `squawk` | string \| null | Transponder squawk code |
| 15 | `spi` | boolean | Special purpose indicator |
| 16 | `position_source` | number | 0=ADS-B, 1=ASTERIX, 2=MLAT, 3=FLARM |

---

## `time_position` versus `last_contact`

These are two different timestamps and the distinction matters for source event time.

`time_position` is the last time the aircraft's position (latitude and longitude) was updated. It is null when OpenSky has not received a fresh position fix recently, for example when an aircraft is deep in a coverage gap or has only been seen via non-position messages.

`last_contact` is the last time any transponder message was received — position or otherwise. It is always present.

For Sentinel, `time_position` is the authoritative source event time for a position record. The data model rule is:

```
timestamp_ms = time_position * 1000   (if not null)
             = last_contact  * 1000   (fallback)
```

This matters for replay safety and monotonic Redis guards. Episode anchors, idempotency keys, and Redis stale-event protection all use `timestamp_ms`. Using `last_contact` as a fallback is conservative — it slightly overstates when the position was current — but it is honest about what the source provided.

The Position Consumer performs this mapping. The ingestion poller forwards both fields raw so the consumer has full information.

---

## Rate limits

Anonymous accounts are subject to a credit budget per day. Polling every 10 seconds over a bounded geographic area stays within that budget for development use. The `POLL_INTERVAL_MS` environment variable controls the interval; the default is 10 000 ms.

OpenSky returns HTTP 429 when the rate limit is exceeded. The poller treats any non-2xx response as a transient fetch failure: it logs a warning and skips the cycle without crashing.
