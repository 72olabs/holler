PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS schema_migrations (
    version INTEGER PRIMARY KEY,
    applied_at_ns INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS messages (
    message_id TEXT PRIMARY KEY,
    schema_version INTEGER NOT NULL,
    idempotency_key TEXT NOT NULL,
    project_id TEXT NOT NULL,
    channel_id TEXT NOT NULL,
    thread_id TEXT,
    from_actor TEXT NOT NULL,
    from_run TEXT NOT NULL,
    from_role TEXT,
    message_type TEXT NOT NULL,
    delivery_request TEXT NOT NULL,
    in_reply_to TEXT REFERENCES messages(message_id),
    body BLOB NOT NULL,
    created_at_ns INTEGER NOT NULL,
    expires_at_ns INTEGER,
    UNIQUE (from_actor, idempotency_key)
);

CREATE INDEX IF NOT EXISTS messages_project_created
    ON messages(project_id, created_at_ns, message_id);

CREATE TABLE IF NOT EXISTS deliveries (
    message_id TEXT NOT NULL REFERENCES messages(message_id),
    recipient_actor TEXT NOT NULL,
    state TEXT NOT NULL,
    attempt INTEGER NOT NULL DEFAULT 0,
    lease_token TEXT,
    terminal_lease_token TEXT,
    lease_expires_at_ns INTEGER,
    claimed_at_ns INTEGER,
    acked_at_ns INTEGER,
    last_error TEXT,
    PRIMARY KEY (message_id, recipient_actor)
);

CREATE TABLE IF NOT EXISTS notification_outbox (
    message_id TEXT NOT NULL REFERENCES messages(message_id),
    recipient_actor TEXT NOT NULL,
    state TEXT NOT NULL,
    attempt INTEGER NOT NULL DEFAULT 0,
    available_at_ns INTEGER NOT NULL,
    last_error TEXT,
    created_at_ns INTEGER NOT NULL,
    PRIMARY KEY (message_id, recipient_actor)
);

CREATE INDEX IF NOT EXISTS notification_outbox_pending
    ON notification_outbox(state, available_at_ns, created_at_ns);

CREATE INDEX IF NOT EXISTS deliveries_inbox
    ON deliveries(recipient_actor, state, lease_expires_at_ns, message_id);

CREATE TABLE IF NOT EXISTS registrations (
    actor TEXT NOT NULL,
    run_id TEXT NOT NULL,
    harness TEXT NOT NULL,
    attention_mode TEXT NOT NULL DEFAULT '',
    session_id TEXT NOT NULL,
    delivery_handle TEXT NOT NULL,
    project_id TEXT NOT NULL,
    working_directory TEXT NOT NULL DEFAULT '',
    epoch INTEGER NOT NULL,
    registered_at_ns INTEGER,
    updated_at_ns INTEGER NOT NULL,
    lease_expires_at_ns INTEGER NOT NULL,
    ended_at_ns INTEGER,
    attention_superseded_at_ns INTEGER,
    PRIMARY KEY (actor, run_id, session_id)
);

CREATE INDEX IF NOT EXISTS registrations_live_actor
    ON registrations(actor, lease_expires_at_ns, updated_at_ns);

CREATE TABLE IF NOT EXISTS actor_profiles (
    actor TEXT NOT NULL,
    revision INTEGER NOT NULL,
    role_text TEXT NOT NULL,
    accepts_json BLOB NOT NULL,
    updated_by_run TEXT NOT NULL,
    project_id TEXT NOT NULL,
    updated_at_ns INTEGER NOT NULL,
    PRIMARY KEY (actor, revision)
);

CREATE INDEX IF NOT EXISTS actor_profiles_current
    ON actor_profiles(actor, revision DESC);

CREATE TABLE IF NOT EXISTS actor_allocations (
    actor TEXT PRIMARY KEY,
    base_actor TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    allocated_at_ns INTEGER NOT NULL,
    provisional INTEGER NOT NULL DEFAULT 0,
    UNIQUE (base_actor, ordinal)
);

CREATE TABLE IF NOT EXISTS continuity_bindings (
    handle TEXT PRIMARY KEY,
    actor TEXT NOT NULL,
    base_actor TEXT NOT NULL,
    created_at_ns INTEGER NOT NULL,
    updated_at_ns INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS continuity_bindings_actor
    ON continuity_bindings(actor);

CREATE TABLE IF NOT EXISTS actor_adoptions (
    source_actor TEXT PRIMARY KEY,
    adopting_actor TEXT NOT NULL,
    adopting_run TEXT NOT NULL,
    project_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    transferred_count INTEGER NOT NULL,
    deduplicated_count INTEGER NOT NULL,
    adopted_at_ns INTEGER NOT NULL,
    UNIQUE (adopting_actor, idempotency_key)
);

CREATE TABLE IF NOT EXISTS partition_counters (
    partition_id TEXT NOT NULL,
    stream TEXT NOT NULL,
    next_position INTEGER NOT NULL,
    PRIMARY KEY (partition_id, stream)
);

CREATE TABLE IF NOT EXISTS events (
    event_id TEXT NOT NULL UNIQUE,
    partition_id TEXT NOT NULL,
    stream TEXT NOT NULL,
    position INTEGER NOT NULL,
    kind TEXT NOT NULL,
    message_id TEXT,
    actor_id TEXT,
    payload BLOB,
    created_at_ns INTEGER NOT NULL,
    PRIMARY KEY (partition_id, stream, position)
);

CREATE INDEX IF NOT EXISTS events_message
    ON events(message_id, created_at_ns);
