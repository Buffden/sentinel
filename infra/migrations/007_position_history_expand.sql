-- Migration 007: expand position_history to full canonical schema
--
-- Three changes:
--   1. Rename altitude → altitude_m (explicit-units convention)
--   2. Drop NOT NULL on geo_cell — CP7 owns H3 computation; CP5 may persist NULL
--   3. Add all canonical columns introduced by CP3 normalization plan
--
-- All new columns are nullable. Existing rows are not backfilled.
-- Existing indexes on geo_cell remain; they tolerate nullable values.
--
-- Idempotency note:
--   This migration runner has no tracking table and replays every file on each
--   make migrate run. The RENAME is guarded by a DO block so it is safe to
--   replay after the column has already been renamed. All ADD COLUMN statements
--   use IF NOT EXISTS. The DROP NOT NULL is a no-op when geo_cell is already
--   nullable.
--
-- geo_cell checkpoint boundary:
--   CP5 persists position_history rows with geo_cell = NULL.
--   CP7 owns H3 computation (latlon → H3 cell) and populates geo_cell.
--   The geo_cell column and its indexes must not be removed.
--   Do not compute H3 in CP5.
--
-- Multi-provider identity note:
--   The v1 idempotency key UNIQUE(entity_id, observed_at) is preserved.
--   Two providers reporting the same entity at the same event-time second
--   would collide here. That conflict/precedence decision is deferred to a
--   future ADR before adding a second provider for the same telemetry domain.
--   Do not silently change the history identity key.

BEGIN;

-- 1. Rename altitude → altitude_m (guarded; idempotent on replay)
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'position_history'
      AND column_name = 'altitude'
  ) THEN
    ALTER TABLE position_history RENAME COLUMN altitude TO altitude_m;
  END IF;
END $$;

-- 2. Drop NOT NULL on geo_cell; no-op when already nullable
ALTER TABLE position_history
  ALTER COLUMN geo_cell DROP NOT NULL;

-- 3. Add canonical columns (all nullable; no-op on replay via IF NOT EXISTS)
ALTER TABLE position_history
  ADD COLUMN IF NOT EXISTS provider TEXT,
  ADD COLUMN IF NOT EXISTS baro_altitude_m REAL,
  ADD COLUMN IF NOT EXISTS geo_altitude_m REAL,
  ADD COLUMN IF NOT EXISTS speed_mps REAL,
  ADD COLUMN IF NOT EXISTS course_deg REAL,
  ADD COLUMN IF NOT EXISTS heading_deg REAL,
  ADD COLUMN IF NOT EXISTS vertical_rate_mps REAL,
  ADD COLUMN IF NOT EXISTS on_ground BOOLEAN,
  ADD COLUMN IF NOT EXISTS last_contact_ms BIGINT,
  ADD COLUMN IF NOT EXISTS navigation_status TEXT,
  ADD COLUMN IF NOT EXISTS rate_of_turn REAL,
  ADD COLUMN IF NOT EXISTS callsign TEXT,
  ADD COLUMN IF NOT EXISTS entity_subtype TEXT,
  ADD COLUMN IF NOT EXISTS provider_category TEXT,
  ADD COLUMN IF NOT EXISTS squawk TEXT,
  ADD COLUMN IF NOT EXISTS spi BOOLEAN,
  ADD COLUMN IF NOT EXISTS position_source SMALLINT,
  ADD COLUMN IF NOT EXISTS position_accuracy BOOLEAN,
  ADD COLUMN IF NOT EXISTS destination TEXT,
  ADD COLUMN IF NOT EXISTS eta TEXT,
  ADD COLUMN IF NOT EXISTS draught_m REAL;

COMMIT;
