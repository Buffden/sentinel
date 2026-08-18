-- CP-02 transaction rollback experiment
-- Demonstrates that DDL inside BEGIN/COMMIT rolls back on error.
-- Run: docker exec -it sentinel-timescaledb psql -U sentinel -d sentinel -v ON_ERROR_STOP=1

BEGIN;
CREATE TABLE rollback_experiment (id INTEGER);
SELECT 1/0;  -- deliberate failure: triggers rollback
COMMIT;

-- After running the above, verify the table was NOT created:
SELECT table_name FROM information_schema.tables
WHERE table_schema = 'public' AND table_name = 'rollback_experiment';
-- Expected: 0 rows
