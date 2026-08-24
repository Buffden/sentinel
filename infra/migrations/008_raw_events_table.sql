-- Migration 008: create raw_events table
--
-- Provider-fidelity raw archive. Regular PostgreSQL table — NOT a hypertable.
--
-- Why not a hypertable:
--   The idempotency key is (source_topic, source_partition, source_offset).
--   Kafka offsets are only unique within a partition. Hypertable unique indexes
--   must include the partitioning time column; adding received_at (processing time)
--   to the unique constraint would break replay idempotency — a replayed message
--   gets a new processing timestamp and the constraint would not catch the duplicate.
--   If raw volume grows large, S3/object storage is the natural long-term archive.
--
-- Idempotency: ON CONFLICT (source_topic, source_partition, source_offset) DO NOTHING.
--   Kafka at-least-once: a replayed message produces the same triple and is silently
--   skipped. Partition is mandatory in the key — offset 42 on partition 0 and offset 42
--   on partition 1 are two distinct Kafka records and must both be stored.
--
-- source_event_time is nullable:
--   Malformed or no-position records may reach raw_events before normalization
--   determines the event time. The archive must accept them regardless.
--
-- payload is JSONB and may legally be an object, array, or string scalar:
--   Valid JSON Kafka values: stored as the parsed provider JSON object.
--   Invalid JSON Kafka values (parse_error): stored as a JSONB string scalar
--   representing the original Kafka value. The column constraint is always
--   satisfied; no {"raw": ...} wrapper is introduced.
--
-- Linkage to position_history:
--   raw_events has no FK or guaranteed correlation key to position_history.
--   entity_id + source_event_time may support best-effort investigation but
--   are not guaranteed unique — the same entity may have multiple raw records
--   at the same source event time (e.g. parse_error and no_position records
--   carry no event time at all).
--   The authoritative Kafka identity is (source_topic, source_partition, source_offset).
--
-- Availability dependency:
--   raw_events is not a downstream position correctness dependency, but
--   successful archival is required before offset commit. If the raw_events
--   insert fails, the offset must NOT be committed — Kafka will redeliver the
--   message and the insert will be retried idempotently.

BEGIN;

CREATE TABLE IF NOT EXISTS raw_events (
  id BIGSERIAL PRIMARY KEY,
  entity_id TEXT,
  source TEXT NOT NULL,
  provider TEXT,
  source_topic TEXT NOT NULL,
  source_partition INTEGER NOT NULL,
  source_offset BIGINT NOT NULL,
  received_at TIMESTAMPTZ NOT NULL,
  source_event_time TIMESTAMPTZ,
  payload JSONB NOT NULL,

  UNIQUE (source_topic, source_partition, source_offset)
);

CREATE INDEX IF NOT EXISTS raw_events_entity_received_idx
  ON raw_events (entity_id, received_at DESC);

COMMIT;
