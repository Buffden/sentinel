# Normalization as a Pure Function

---

## What "pure function" means here

`normalizeAdsbRaw(rawValue: string): NormalizeResult` has no side effects. It reads one string, returns one result object, and touches nothing else — no database, no network, no shared state. Given the same input string it always returns the same output.

This is deliberate. The normalization logic owns the hardest decision in the pipeline: what shape is a valid position, what makes a record unprocessable, and what the canonical field names and units are. That logic is much easier to read, test, and audit when it is isolated from I/O concerns.

---

## What the function owns and what it does not

| In scope | Out of scope |
| --- | --- |
| JSON parsing and type validation | Writing to any database |
| Entity identity check (icao24 present) | Publishing to Kafka |
| Position field presence check (null lat/lon/time) | Redis live state |
| Unit conversion (Unix seconds → ms, knots → m/s) | H3 geo-cell computation |
| Composite altitude (geo ?? baro) | DLQ publishing |
| Callsign trimming | |
| Category → entity_subtype mapping | |

H3 geo-cell computation is intentionally absent. It requires a spatial library and is only needed for the Redis geo-cell sorted set, which is a separate concern. Keeping it out of normalization means this module has no non-standard dependencies and can be exercised in a plain Node.js REPL.

---

## The result type encodes the outcome

```typescript
type NormalizeResult =
  | { ok: true; position: NormalizedPosition }
  | { ok: false; kind: 'no_position'; entity_id: string }
  | { ok: false; kind: 'parse_error' | 'missing_entity_id'; detail: string };
```

The consumer does not inspect error strings or guess from field presence. It pattern-matches on `result.ok` and `result.kind`. Adding a new rejection class in the future requires adding a new `kind` variant and updating the consumer's branch — the type system will flag every call site that doesn't handle it.

---

## Why no raw field on NormalizedPosition

`NormalizedPosition` does not carry the original JSON string. The normalized record is the canonical form; the raw string is a separate artifact.

When the raw string is needed (for the `raw_events` write), the consumer already has it as the `rawValue` variable in scope. There is no need to embed it in the normalized record. Embedding it would make every downstream consumer of `position.normalized` carry a redundant copy of the raw payload it almost certainly does not need.

---

## Replay safety

Because the function is pure, running it twice on the same input produces the same output. The consumer can replay any offset from the beginning, re-normalize, and produce identical results. This is a prerequisite for:

- Crash recovery: after restart the consumer renormalizes the redelivered message identically.
- Historical backfill: a separate consumer group can read the full topic history and produce the same canonical records.
- Testing: unit tests can call `normalizeAdsbRaw` directly with a fixture string and assert the exact result without any infrastructure.

---

## Testing without infrastructure

```typescript
import { normalizeAdsbRaw } from './normalize.js';

// Happy path
const result = normalizeAdsbRaw('{"icao24":"abc123","lat":51.5,"lon":-0.1,"time_position":1700000000,...}');
assert(result.ok === true);
assert(result.position.entity_id === 'abc123');

// parse_error
const bad = normalizeAdsbRaw('not json');
assert(bad.ok === false && bad.kind === 'parse_error');

// no_position
const nofix = normalizeAdsbRaw('{"icao24":"abc123","lat":null,"lon":null,"time_position":null,...}');
assert(nofix.ok === false && nofix.kind === 'no_position');
```

No Kafka connection. No database. No containers. The logic runs in the test runner directly.
