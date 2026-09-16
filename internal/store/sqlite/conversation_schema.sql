CREATE TABLE IF NOT EXISTS human_actors (
    actor TEXT PRIMARY KEY CHECK (substr(actor,1,6)='human:'),
    created_at_ns INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS channel_attention_clients (
 actor TEXT NOT NULL,
 run_id TEXT NOT NULL,
 native_ready INTEGER NOT NULL DEFAULT 0,
 monitor_ready INTEGER NOT NULL DEFAULT 0,
 PRIMARY KEY(actor,run_id)
);
CREATE TABLE IF NOT EXISTS supervision_links (
    agent TEXT PRIMARY KEY,
    human TEXT NOT NULL REFERENCES human_actors(actor),
    revision INTEGER NOT NULL,
    active INTEGER NOT NULL CHECK (active IN (0,1)),
    established_by TEXT NOT NULL,
    established_scope TEXT NOT NULL,
    updated_at_ns INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS channel_grants (
	grant_id INTEGER PRIMARY KEY,
    channel_id TEXT NOT NULL REFERENCES channels(channel_id),
    actor TEXT NOT NULL,
    source TEXT NOT NULL,
    can_post INTEGER NOT NULL CHECK (can_post IN (0,1)),
    history_from INTEGER NOT NULL,
    granted_seq INTEGER NOT NULL,
    revoked_seq INTEGER
);
CREATE UNIQUE INDEX IF NOT EXISTS channel_grants_active ON channel_grants(channel_id,actor,source) WHERE revoked_seq IS NULL;
CREATE INDEX IF NOT EXISTS channel_grants_actor ON channel_grants(actor, channel_id, revoked_seq);
CREATE TABLE IF NOT EXISTS channel_events (
    channel_id TEXT NOT NULL REFERENCES channels(channel_id),
    seq INTEGER NOT NULL,
    kind TEXT NOT NULL,
    actor TEXT NOT NULL,
    message_id TEXT,
    payload BLOB,
    created_at_ns INTEGER NOT NULL,
    PRIMARY KEY (channel_id, seq)
);
CREATE TABLE IF NOT EXISTS channel_threads (
    channel_id TEXT NOT NULL REFERENCES channels(channel_id),
    thread_id TEXT NOT NULL,
    root_message_id TEXT NOT NULL REFERENCES messages(message_id) DEFERRABLE INITIALLY DEFERRED,
    PRIMARY KEY (channel_id, thread_id)
);
CREATE TABLE IF NOT EXISTS managed_deliveries (
    message_id TEXT NOT NULL REFERENCES messages(message_id),
    recipient_actor TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'queued',
    attempt INTEGER NOT NULL DEFAULT 0,
    lease_token TEXT,
    terminal_lease_token TEXT,
    lease_expires_at_ns INTEGER,
    claimed_at_ns INTEGER,
    acked_at_ns INTEGER,
    last_error TEXT,
    PRIMARY KEY (message_id, recipient_actor)
);
CREATE INDEX IF NOT EXISTS managed_inbox ON managed_deliveries(recipient_actor, state, message_id);
CREATE INDEX IF NOT EXISTS managed_lease_sweep ON managed_deliveries(state,lease_expires_at_ns);
CREATE TABLE IF NOT EXISTS channel_references (
    message_id TEXT NOT NULL REFERENCES messages(message_id),
    source_message_id TEXT NOT NULL REFERENCES messages(message_id),
    relation TEXT NOT NULL CHECK (relation IN ('discusses','continued_from')),
    PRIMARY KEY (message_id, source_message_id, relation)
);
CREATE TABLE IF NOT EXISTS channel_operations (
    actor TEXT NOT NULL,
    operation TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    request_digest TEXT NOT NULL,
    result BLOB NOT NULL,
    PRIMARY KEY (actor, operation, idempotency_key)
);
CREATE TABLE IF NOT EXISTS channel_message_requests (
    message_id TEXT PRIMARY KEY REFERENCES messages(message_id),
    request_digest TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS channel_views (
    channel_id TEXT NOT NULL REFERENCES channels(channel_id),
    actor TEXT NOT NULL,
    thread_id TEXT NOT NULL DEFAULT '',
    revision INTEGER NOT NULL,
    state BLOB NOT NULL,
    PRIMARY KEY (channel_id, actor, thread_id)
);
CREATE TABLE IF NOT EXISTS channel_responses (
    request_id TEXT PRIMARY KEY,
    channel_id TEXT NOT NULL REFERENCES channels(channel_id),
    message_id TEXT NOT NULL UNIQUE REFERENCES messages(message_id),
    requester TEXT NOT NULL,
    respondent TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('open','answered','declined','withdrawn','revoked')),
    revision INTEGER NOT NULL DEFAULT 1,
    answer_id TEXT REFERENCES messages(message_id)
);
CREATE UNIQUE INDEX IF NOT EXISTS channel_message_seq ON messages(conversation_id, channel_seq) WHERE conversation_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS channel_message_thread ON messages(conversation_id, thread_id, channel_seq);
CREATE VIEW IF NOT EXISTS legacy_messages AS SELECT * FROM messages WHERE conversation_id IS NULL;

CREATE TRIGGER IF NOT EXISTS legacy_event_insert BEFORE INSERT ON events
WHEN EXISTS(SELECT 1 FROM messages WHERE message_id=NEW.message_id AND conversation_id IS NOT NULL)
BEGIN SELECT RAISE(ABORT, 'managed message in legacy event'); END;
CREATE TRIGGER IF NOT EXISTS legacy_event_update BEFORE UPDATE OF message_id ON events
WHEN EXISTS(SELECT 1 FROM messages WHERE message_id=NEW.message_id AND conversation_id IS NOT NULL)
BEGIN SELECT RAISE(ABORT, 'managed message in legacy event'); END;

CREATE TRIGGER IF NOT EXISTS messages_channel_immutable BEFORE UPDATE OF conversation_id ON messages
WHEN OLD.conversation_id IS NOT NEW.conversation_id
BEGIN SELECT RAISE(ABORT, 'message home is immutable'); END;
CREATE TRIGGER IF NOT EXISTS legacy_delivery_insert BEFORE INSERT ON deliveries
WHEN EXISTS(SELECT 1 FROM messages WHERE message_id=NEW.message_id AND conversation_id IS NOT NULL)
BEGIN SELECT RAISE(ABORT, 'managed message in legacy delivery'); END;
CREATE TRIGGER IF NOT EXISTS legacy_delivery_update BEFORE UPDATE OF message_id ON deliveries
WHEN EXISTS(SELECT 1 FROM messages WHERE message_id=NEW.message_id AND conversation_id IS NOT NULL)
BEGIN SELECT RAISE(ABORT, 'managed message in legacy delivery'); END;
CREATE TRIGGER IF NOT EXISTS managed_delivery_insert BEFORE INSERT ON managed_deliveries
WHEN EXISTS(SELECT 1 FROM messages WHERE message_id=NEW.message_id AND conversation_id IS NULL)
BEGIN SELECT RAISE(ABORT, 'legacy message in managed delivery'); END;
CREATE TRIGGER IF NOT EXISTS managed_delivery_update BEFORE UPDATE OF message_id ON managed_deliveries
WHEN EXISTS(SELECT 1 FROM messages WHERE message_id=NEW.message_id AND conversation_id IS NULL)
BEGIN SELECT RAISE(ABORT, 'legacy message in managed delivery'); END;
CREATE TRIGGER IF NOT EXISTS outbox_source_insert BEFORE INSERT ON notification_outbox
WHEN NEW.source NOT IN ('legacy','managed') OR EXISTS(
    SELECT 1 FROM messages WHERE message_id=NEW.message_id
    AND ((conversation_id IS NULL AND NEW.source!='legacy') OR (conversation_id IS NOT NULL AND NEW.source!='managed')))
BEGIN SELECT RAISE(ABORT, 'notification source mismatch'); END;
CREATE TRIGGER IF NOT EXISTS outbox_source_update BEFORE UPDATE OF source, message_id ON notification_outbox
WHEN NEW.source NOT IN ('legacy','managed') OR EXISTS(
    SELECT 1 FROM messages WHERE message_id=NEW.message_id
    AND ((conversation_id IS NULL AND NEW.source!='legacy') OR (conversation_id IS NOT NULL AND NEW.source!='managed')))
BEGIN SELECT RAISE(ABORT, 'notification source mismatch'); END;
