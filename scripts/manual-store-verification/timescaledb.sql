-- TimescaleDB manual store verification
-- Run: docker exec -it sentinel-timescaledb psql -U sentinel -d sentinel
-- Then paste or \i this file.

-- List all tables in the public schema
\dt

-- Hypertable metadata: num_chunks
SELECT hypertable_name, num_chunks
FROM timescaledb_information.hypertables;

-- Chunk interval (lives in dimensions view, not hypertables)
SELECT hypertable_name, column_name, time_interval
FROM timescaledb_information.dimensions
WHERE hypertable_name = 'position_history';

-- Retention policy
SELECT * FROM timescaledb_information.jobs
WHERE proc_name = 'policy_retention';

-- Insert synthetic row (timestamp_ms 1700000000000 = 2023-11-14)
INSERT INTO position_history
  (entity_id, entity_type, observed_at, timestamp_ms, geo_cell, lat, lon, source)
VALUES (
  'test-entity',
  'aircraft',
  to_timestamp(1700000000000 / 1000.0),
  1700000000000,
  '8928308280fffff',
  40.7128,
  -74.0060,
  'synthetic'
);

-- Inspect chunk created by the insert
SELECT chunk_name, range_start, range_end
FROM timescaledb_information.chunks
WHERE hypertable_name = 'position_history';

-- Re-insert the same row (idempotency test -- should produce INSERT 0 0)
INSERT INTO position_history
  (entity_id, entity_type, observed_at, timestamp_ms, geo_cell, lat, lon, source)
VALUES (
  'test-entity',
  'aircraft',
  to_timestamp(1700000000000 / 1000.0),
  1700000000000,
  '8928308280fffff',
  40.7128,
  -74.0060,
  'synthetic'
)
ON CONFLICT (entity_id, observed_at) DO NOTHING;

-- Verify exactly one row
SELECT COUNT(*) FROM position_history WHERE entity_id = 'test-entity';

-- Cleanup: delete the row
DELETE FROM position_history WHERE entity_id = 'test-entity';

-- Verify chunk persists after row deletion
SELECT chunk_name FROM timescaledb_information.chunks
WHERE hypertable_name = 'position_history';

-- Failure test
SELECT 1/0;
