# Live Hash Design

---

## What entity:live:{entity_id} is for

`entity:live:{entity_id}` is the fast-readable live snapshot of an entity's most recent position. It serves two consumers:

- **Alert Evaluator**: reads `last_seen_ms` to detect signal loss; reads `lat`, `lon`, and `on_ground` for rule evaluation.
- **API / WebSocket layer**: reads the hash to push the current entity state to connected clients.

The hash is not the source of truth. `position_history` in TimescaleDB is the durable record. The Redis hash is an ephemeral index optimised for low-latency point reads by entity ID.

---

## Why a hash, not a string or sorted set

A Redis hash lets the Alert Evaluator read a single field (`last_seen_ms`) without deserializing the entire record. A JSON string stored under a plain key would require a full GET and JSON parse for every field access. A sorted set is the right structure for the geo-cell candidate index (`geo-cell:{cell}`), not for per-entity state.

Hash also keeps the write atomic: `HSET` with multiple field/value pairs is a single command. All fields update together or not at all (within the limits of Redis's single-threaded command processing). This matters for the monotonic guard — see [monotonic-guard.md](monotonic-guard.md).

---

## Field list

| Field | Type stored | Source |
| --- | --- | --- |
| `last_seen_ms` | string (int64) | `timestamp_ms` from normalized position — source event time |
| `entity_type` | string | `entity_type` from normalized position |
| `lat` | string (float) | `lat` |
| `lon` | string (float) | `lon` |
| `altitude_m` | string (float) or `''` | `altitude_m`; empty string when null |
| `speed_mps` | string (float) or `''` | `speed_mps` |
| `course_deg` | string (float) or `''` | `course_deg` |
| `heading_deg` | string (float) or `''` | `heading_deg` |
| `vertical_rate_mps` | string (float) or `''` | `vertical_rate_mps` |
| `on_ground` | string (`'true'`/`'false'`) or `''` | `on_ground` |
| `navigation_status` | string or `''` | `navigation_status` |
| `callsign` | string or `''` | `callsign` |
| `entity_subtype` | string or `''` | `entity_subtype` |
| `provider` | string or `''` | `provider` |

Redis stores everything as strings. Null values are stored as empty string `''` so that readers always get a string back from `HGET`, never `nil`. This simplifies consumer code — no nil-vs-empty branching required.

`live_geo_cell` is absent until H3 geo-cell indexing is implemented. When added, it will hold the H3 cell ID of the entity's current position, used to remove the entity from a stale sorted set on cell change.

---

## TTL policy

The hash expires after 24 hours (86400 seconds). The TTL is reset on every accepted write via `EXPIRE` inside the Lua guard script.

This means the 24h clock restarts on every position update. An aircraft transmitting every few seconds resets the TTL continuously throughout its flight. The hash only expires after 24 consecutive hours of complete silence — no accepted writes for that entity. The TTL is a cleanup window for entities that have permanently stopped transmitting, not a session duration.

The longest non-stop commercial flights (Singapore to New York, ~19 hours) stay well within the 24h window even accounting for periods of reduced ADS-B coverage over open ocean, where there may be no ground receivers in range for several hours at a stretch.

24 hours is intentionally longer than the signal-loss detection window. If an entity stops transmitting, the Alert Evaluator needs to be able to read its last known position after the signal has been lost but before the alert fires. A shorter TTL would cause the hash to disappear before the evaluator can act on it.

On stale writes (the monotonic guard rejects the event), the TTL is not refreshed. Only accepted writes extend the lifetime.

---

## Ephemeral vs durable

The Redis hash can be lost. A Redis restart, eviction under memory pressure, or a TTL expiry clears the entity's live state. This is acceptable because:

- `position_history` always has the durable record.
- The live state is reconstructable from `position_history` or by replaying from `adsb.raw` with a separate consumer group that skips ephemeral live-state side effects.
- Signal-loss detection works from the absence of updates, not the presence of a hash key. A missing key is treated the same as a key with an old `last_seen_ms`.
