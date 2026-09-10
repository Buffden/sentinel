BEGIN;
-- Pre-CP5B: records a composite supersession that has to wait for its
-- referenced alert to exist. referenced_alert_id is deliberately not an FK,
-- the row it names doesn't exist yet by definition when this row is created.
-- No placeholder alert row is ever inserted for it; see DATA_MODEL.md's
-- "Pre-CP5B: composite supersession convergence protocol" for why.
CREATE TABLE IF NOT EXISTS pending_alert_supersessions (
    referenced_alert_id TEXT PRIMARY KEY,
    composite_alert_id  TEXT NOT NULL REFERENCES alerts (alert_id),
    created_at          TIMESTAMPTZ NOT NULL
);
COMMIT;
