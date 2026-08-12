BEGIN;
CREATE TABLE IF NOT EXISTS route_references (
    route_id TEXT PRIMARY KEY,
    entity_id TEXT NOT NULL,
    route_name TEXT NOT NULL,
    corridor_threshold_metres REAL NOT NULL,
    source TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS route_references_entity_idx
    ON route_references (entity_id);

-- Adjacent (sequence_no, sequence_no+1) pairs define route segments
-- Deviation Detector computes minimum point-to-segment distance against these
CREATE TABLE IF NOT EXISTS route_reference_points (
    route_id TEXT NOT NULL REFERENCES route_references (route_id),
    sequence_no INTEGER NOT NULL,
    lat DOUBLE PRECISION NOT NULL,
    lon DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (route_id, sequence_no)
);
COMMIT;
