BEGIN;
CREATE TABLE IF NOT EXISTS position_history (
    entity_id TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    timestamp_ms BIGINT NOT NULL,
    geo_cell TEXT NOT NULL,
    lat DOUBLE PRECISION NOT NULL,
    lon DOUBLE PRECISION NOT NULL,
    altitude REAL,
    source TEXT NOT NULL
);

-- Partition by observed_at only
-- chunk_time_interval: 1 day (canonical)
SELECT create_hypertable(
    'position_history',
    'observed_at',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

-- Idempotent duplicate-prevention identity
-- TimescaleDB requires the partitioning column (observed_at) in any unique index
-- Enables: INSERT ... ON CONFLICT (entity_id, observed_at) DO NOTHING
CREATE UNIQUE INDEX IF NOT EXISTS position_history_entity_observed_uidx
    ON position_history (entity_id, observed_at);

-- Entity history query index 
CREATE INDEX IF NOT EXISTS position_history_entity_time_idx
    ON position_history (entity_id, observed_at DESC);

-- Geo-cell range query index 
CREATE INDEX IF NOT EXISTS position_history_geocell_time_idx
    ON position_history (geo_cell, observed_at DESC);

-- 30-day automatic retention: drops entire time chunks, not row-by-row 
SELECT add_retention_policy(
    'position_history',
    INTERVAL '30 days',
    if_not_exists => TRUE
);
COMMIT;
