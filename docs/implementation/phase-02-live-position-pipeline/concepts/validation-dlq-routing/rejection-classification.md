# Rejection Classification

The Position Consumer classifies every record into exactly one of four outcomes before committing its offset. The classification is a decision tree, not a set of independent checks.

---

## The four outcomes

| Outcome | Condition | Action |
| --- | --- | --- |
| `parse_error` | `JSON.parse` throws, or lat/lon/time_position is present but wrong type | Publish to `adsb.dlq`; commit offset |
| `missing_entity_id` | `icao24` absent or empty string | Publish to `adsb.dlq`; commit offset |
| `no_position` | `icao24` present; lat, lon, or time_position is null | Warn log; commit offset; no DLQ |
| valid | All required fields present and correctly typed | Log normalized position; commit offset |

---

## Why these three failure kinds are distinct

### `parse_error`

The record cannot be interpreted at all. Either the JSON is malformed (the source sent garbage), or a field that must be a number arrived as a string or null. In both cases the consumer cannot extract any meaningful information — not even the entity identity. The record is unprocessable now and will always be unprocessable. It belongs in the DLQ so an operator can inspect the raw payload and determine whether the source is broken.

### `missing_entity_id`

The JSON parsed correctly, but `icao24` is absent or empty. Without an entity identity, the consumer cannot write to `position_history` (no primary key), cannot write the Redis live key (`entity:live:{entity_id}`), and cannot route the normalized event downstream. The record is technically parseable but useless. It goes to the DLQ for the same reason as a parse error: an operator needs to investigate why the source is omitting the identity field.

### `no_position`

`icao24` is present and the JSON is well-formed, but `lat`, `lon`, or `time_position` is null. This is **not an error in the source**. OpenSky sends null for these fields when an aircraft is reporting via its transponder but does not have a current GPS fix — on the ground with no satellite lock, or recently powered on. The record faithfully represents the aircraft's current reported state. The consumer has nothing useful to persist (no coordinates, no event time), so it skips with a warn log. DLQ is wrong here: there is nothing to investigate. The next poll cycle will likely carry a valid position.

### The ordering matters

The decision tree processes in this order:

1. Parse JSON.
2. Check `icao24`.
3. Check for null position fields.
4. Check position field types.

Step 3 comes before step 4 because a null field is not a type error — it is a valid JSON null that represents "not available". If step 4 ran first, a null lat would produce a misleading `parse_error` instead of the correct `no_position`.

---

## What `no_position` is not

`no_position` is not a safety valve for unclassified failures. If lat arrives as the string `"51.5"` instead of the number `51.5`, that is a `parse_error` (wrong type), not a `no_position`. The distinction matters because `parse_error` goes to the DLQ where an operator can see it, while `no_position` is silently skipped. Only a genuinely null lat/lon/time_position (the OpenSky-defined "no GPS fix" case) produces `no_position`.

---

## Why `no_position` records are logged at warn, not info

Skipping a record is not a normal event. The warn level tells operators they can measure the skip rate without it drowning in info noise. A high `no_position` rate during peak hours is expected (many aircraft taxiing). A sustained high rate from a single aircraft may indicate a transponder fault. The entity_id is included in the warn log so operators can filter by aircraft.
