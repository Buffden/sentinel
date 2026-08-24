# Raw Events Design

---

## Purpose

`raw_events` is the provider-fidelity archive. It records exactly what arrived on the Kafka topic, before normalization, for every message the consumer processes — including malformed records and records with no GPS fix.

Its role is distinct from `position_history`:

| Table | Contains | Identity |
| --- | --- | --- |
| `position_history` | Canonical, queryable position data | `(entity_id, observed_at)` — logical fact identity |
| `raw_events` | Exact provider payload as received | `(source_topic, source_partition, source_offset)` — Kafka record identity |

Downstream services — Correlation Worker, Deviation Detector, Alert Evaluator — never read `raw_events`. It exists for audit, debugging, and future replay of raw payloads without going back to the Kafka broker.

---

## Why it is a regular table, not a hypertable

TimescaleDB hypertable unique indexes must include the partitioning time column. The `raw_events` idempotency key is `(source_topic, source_partition, source_offset)` — a Kafka identity, not a time value. Adding `received_at` (processing time) to the unique constraint would break replay idempotency: a replayed message gets a new processing timestamp and the constraint would not catch the duplicate.

A regular Postgres table with a unique constraint on the Kafka triple is the correct model. If raw volume grows large enough to require time-based partitioning or expiry, S3 object storage is the more natural long-term archive.

---

## Schema

```sql
CREATE TABLE raw_events (
  id                BIGSERIAL    PRIMARY KEY,
  entity_id         TEXT,
  source            TEXT         NOT NULL,
  provider          TEXT,
  source_topic      TEXT         NOT NULL,
  source_partition  INTEGER      NOT NULL,
  source_offset     BIGINT       NOT NULL,
  received_at       TIMESTAMPTZ  NOT NULL,
  source_event_time TIMESTAMPTZ,
  payload           JSONB        NOT NULL,

  UNIQUE (source_topic, source_partition, source_offset)
);
```

`received_at` is processing time (when this row was written). `source_event_time` is provider event time (`to_timestamp(timestamp_ms / 1000.0)`).

---

## Nullable columns

`entity_id`, `provider`, and `source_event_time` are all nullable because:

- `parse_error`: JSON parse failed — the entity identity and event time cannot be extracted.
- `missing_entity_id`: JSON parsed but `icao24` is absent — entity identity is unknown.
- `no_position`: `time_position` was null — provider event time is unavailable.

The archive must accept all three cases. Making these columns NOT NULL would force the consumer to skip archiving bad records, which defeats the purpose.

---

## JSONB payload contract

`payload` is NOT NULL but may be any valid JSONB type:

| Source record type | Stored payload | JSONB type |
| --- | --- | --- |
| Valid JSON Kafka value | Provider JSON object | `object` |
| `no_position` or `missing_entity_id` | Provider JSON object | `object` |
| `parse_error` | `JSON.stringify(rawValue)` | `string` |

For `parse_error`, the raw Kafka value is not valid JSON and cannot be stored as a JSONB object directly. `JSON.stringify(rawValue)` wraps the string in JSON quotes, producing a JSONB string scalar. The column constraint is satisfied, the original bytes are preserved verbatim inside the scalar, and no wrapper object is invented.

You can query by type: `WHERE jsonb_typeof(payload) = 'string'` identifies all archived parse errors.

---

## Linkage to position_history

`raw_events` has no foreign key to `position_history`. Not every `raw_events` row has a corresponding `position_history` row — malformed and no-position records are archived but never produce a canonical position.

`entity_id + source_event_time` may support best-effort investigation but are not guaranteed unique. The authoritative Kafka identity is `(source_topic, source_partition, source_offset)`.
