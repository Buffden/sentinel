-- CP-02 schema verification
-- Run: docker exec -it sentinel-timescaledb psql -U sentinel -d sentinel -f /dev/stdin < scripts/cp-02/verify-schema.sql
-- Or open psql and paste directly.

-- Confirm TimescaleDB extension is active
\dx

-- Confirm all 6 application tables are present
\dt

-- Inspect position_history columns, indexes, and constraints
\d position_history

-- Hypertable metadata: num_chunks
SELECT hypertable_name, num_chunks FROM timescaledb_information.hypertables;

-- Chunk interval (lives in dimensions view)
SELECT hypertable_name, column_name, time_interval
FROM timescaledb_information.dimensions
WHERE hypertable_name = 'position_history';

-- Retention policy (expect 30-day policy on position_history)
SELECT * FROM timescaledb_information.jobs WHERE proc_name = 'policy_retention';

-- All indexes on position_history
\di+ position_history*
