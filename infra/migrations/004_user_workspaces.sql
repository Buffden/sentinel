BEGIN;
-- user_id is both PK and FK; one workspace row per user
CREATE TABLE IF NOT EXISTS user_workspaces (
    user_id UUID PRIMARY KEY REFERENCES users (user_id),
    scope JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);
COMMIT;
