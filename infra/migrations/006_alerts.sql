BEGIN;
CREATE TABLE IF NOT EXISTS alerts (
    alert_id TEXT PRIMARY KEY,
    entity_id TEXT NOT NULL,
    counterparty_entity_id TEXT,
    entity_type TEXT NOT NULL,
    alert_type TEXT NOT NULL,
    priority TEXT NOT NULL,
    status TEXT NOT NULL,
    -- Self-referential: composite alert that superseded this one
    -- Plain FK (no DEFERRABLE): application inserts composite first, then updates
    superseded_by TEXT REFERENCES alerts (alert_id),
    payload JSONB NOT NULL,
    detected_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    acknowledged_at TIMESTAMPTZ,
    acknowledged_by UUID REFERENCES users (user_id),
    resolved_at TIMESTAMPTZ,
    resolved_by UUID REFERENCES users (user_id)
);

CREATE INDEX IF NOT EXISTS alerts_entity_time_idx
    ON alerts (entity_id, detected_at DESC);

CREATE INDEX IF NOT EXISTS alerts_counterparty_time_idx
    ON alerts (counterparty_entity_id, detected_at DESC);

CREATE INDEX IF NOT EXISTS alerts_status_time_idx
    ON alerts (status, detected_at DESC);

CREATE INDEX IF NOT EXISTS alerts_type_time_idx
    ON alerts (alert_type, detected_at DESC);
COMMIT;
