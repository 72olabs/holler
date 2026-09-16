CREATE TABLE IF NOT EXISTS channels (
    channel_id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('dm','named')),
    title TEXT NOT NULL,
    created_by TEXT NOT NULL,
    dm_key TEXT UNIQUE,
    policy_revision INTEGER NOT NULL DEFAULT 1,
    next_seq INTEGER NOT NULL DEFAULT 1,
    created_at_ns INTEGER NOT NULL
);
