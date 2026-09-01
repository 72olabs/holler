package sqlite

import (
	"context"
	"crypto/rand"
	"database/sql"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	mathrand "math/rand/v2"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/72olabs/holler/internal/bus"
	"golang.org/x/sys/unix"
	sqlitedriver "modernc.org/sqlite"
)

//go:embed schema.sql
var schema string

const migrationVersion = 9

const (
	migrationRetryWindow = 5 * time.Second
	migrationRetryBase   = 20 * time.Millisecond
	migrationRetryJitter = 20 * time.Millisecond
)

type Store struct {
	db    *sql.DB
	now   func() time.Time
	newID func(string) (string, error)
}

type Option func(*Store)

func WithClock(now func() time.Time) Option {
	return func(store *Store) { store.now = now }
}

func WithIDGenerator(generator func(string) (string, error)) Option {
	return func(store *Store) { store.newID = generator }
}

func Open(ctx context.Context, path string, options ...Option) (*Store, error) {
	if strings.TrimSpace(path) == "" {
		return nil, &bus.ValidationError{Field: "database_path", Problem: "is required"}
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("resolve database path: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(abs), 0o700); err != nil {
		return nil, fmt.Errorf("create database directory: %w", err)
	}
	deadline := time.Now().Add(migrationRetryWindow)
	migrationLock, err := acquireMigrationLock(ctx, abs, deadline)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = unix.Flock(int(migrationLock.Fd()), unix.LOCK_UN)
		_ = migrationLock.Close()
	}()

	dsn := (&url.URL{Scheme: "file", Path: abs}).String() +
		"?_pragma=foreign_keys(1)&_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)" +
		"&_pragma=locking_mode(EXCLUSIVE)&_pragma=busy_timeout(5000)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	// V1 deliberately has one in-process connection. In production this store is
	// owned only by hollerd; EXCLUSIVE locking rejects a second daemon or embedded writer.
	db.SetMaxOpenConns(1)
	// Do not retain a connection that lost a cross-process migration race. With
	// exclusive locking, caching that connection can retain a read lock and make
	// all contenders deadlock until their retry windows expire.
	db.SetMaxIdleConns(0)

	store := &Store{db: db, now: time.Now, newID: randomID}
	for _, option := range options {
		option(store)
	}
	for {
		err = store.migrate(ctx)
		if err == nil {
			err = store.retainExclusiveLock(ctx)
		}
		if err == nil {
			break
		}
		if !sqliteBusy(err) {
			db.Close()
			return nil, err
		}
		if !time.Now().Before(deadline) {
			db.Close()
			return nil, fmt.Errorf("%w: %v", bus.ErrDatabaseOwned, err)
		}
		delay := migrationRetryDelay(deadline)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			db.Close()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
	return store, nil
}

func acquireMigrationLock(ctx context.Context, databasePath string, deadline time.Time) (*os.File, error) {
	lock, err := os.OpenFile(databasePath+".migrate.lock", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open migration lock: %w", err)
	}
	for {
		err = unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB)
		if err == nil {
			return lock, nil
		}
		if !errors.Is(err, unix.EWOULDBLOCK) && !errors.Is(err, unix.EAGAIN) {
			_ = lock.Close()
			return nil, fmt.Errorf("acquire migration lock: %w", err)
		}
		if !time.Now().Before(deadline) {
			_ = lock.Close()
			return nil, fmt.Errorf("%w: migration lock did not become available within %s", bus.ErrDatabaseOwned, migrationRetryWindow)
		}
		timer := time.NewTimer(migrationRetryDelay(deadline))
		select {
		case <-ctx.Done():
			timer.Stop()
			_ = lock.Close()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}

func migrationRetryDelay(deadline time.Time) time.Duration {
	delay := migrationRetryBase + time.Duration(mathrand.Int64N(int64(migrationRetryJitter)))
	if remaining := time.Until(deadline); delay > remaining {
		return remaining
	}
	return delay
}

func sqliteBusy(err error) bool {
	var sqliteErr *sqlitedriver.Error
	return errors.As(err, &sqliteErr) && sqliteErr.Code()&0xff == 5
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) migrate(ctx context.Context) error {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquire migration connection: %w", err)
	}
	defer conn.Close()
	// The general connection timeout is five seconds. A migration contender
	// needs a short lock attempt so Open can apply its own jittered retry policy
	// instead of every process timing out in the SQLite busy handler together.
	if _, err := conn.ExecContext(ctx, `PRAGMA busy_timeout = 100`); err != nil {
		return fmt.Errorf("configure migration busy timeout: %w", err)
	}
	// The advisory flock serializes cooperating Holler processes. BEGIN
	// IMMEDIATE remains a second ownership boundary for non-Holler SQLite
	// writers and filesystems that do not preserve local flock semantics.
	if _, err := conn.ExecContext(ctx, `BEGIN IMMEDIATE`); err != nil {
		return fmt.Errorf("begin immediate migration: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			rollbackCtx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_, _ = conn.ExecContext(rollbackCtx, `ROLLBACK`)
		}
	}()
	if err := s.applyMigrations(ctx, conn); err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
		return fmt.Errorf("commit migration: %w", err)
	}
	committed = true
	return nil
}

func (s *Store) retainExclusiveLock(ctx context.Context) error {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquire exclusive store connection: %w", err)
	}
	// MaxIdleConns remains zero until the lock is proven. A losing contender's
	// connection is therefore destroyed instead of caching a lock that can
	// deadlock the other daemon candidates.
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, `PRAGMA busy_timeout = 100`); err != nil {
		return fmt.Errorf("configure exclusive lock timeout: %w", err)
	}
	if _, err := conn.ExecContext(ctx, `BEGIN IMMEDIATE`); err != nil {
		return fmt.Errorf("acquire exclusive store lock: %w", err)
	}
	if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
		_, _ = conn.ExecContext(context.Background(), `ROLLBACK`)
		return fmt.Errorf("commit exclusive store lock: %w", err)
	}
	if _, err := conn.ExecContext(ctx, `PRAGMA busy_timeout = 5000`); err != nil {
		return fmt.Errorf("restore sqlite busy timeout: %w", err)
	}
	// Closing this verified connection into a one-slot idle pool preserves
	// SQLite's EXCLUSIVE lock continuously until Store.Close.
	s.db.SetMaxIdleConns(1)
	return nil
}

func (s *Store) applyMigrations(ctx context.Context, conn *sql.Conn) error {
	var migrationsTable int
	if err := conn.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM sqlite_master
		WHERE type = 'table' AND name = 'schema_migrations'`).Scan(&migrationsTable); err != nil {
		return fmt.Errorf("inspect schema migrations: %w", err)
	}
	var current int
	if migrationsTable != 0 {
		if err := conn.QueryRowContext(ctx, `SELECT COALESCE(MAX(version), 0) FROM schema_migrations`).Scan(&current); err != nil {
			return fmt.Errorf("read schema version: %w", err)
		}
		if current > migrationVersion {
			return fmt.Errorf("database schema version %d is newer than supported version %d", current, migrationVersion)
		}
	}
	if _, err := conn.ExecContext(ctx, schema); err != nil {
		return fmt.Errorf("apply schema: %w", err)
	}
	for _, addition := range []struct {
		table, column, definition string
	}{
		{"deliveries", "terminal_lease_token", "terminal_lease_token TEXT"},
		{"registrations", "ended_at_ns", "ended_at_ns INTEGER"},
		{"registrations", "registered_at_ns", "registered_at_ns INTEGER"},
		{"registrations", "attention_mode", "attention_mode TEXT NOT NULL DEFAULT ''"},
		{"registrations", "attention_superseded_at_ns", "attention_superseded_at_ns INTEGER"},
		{"registrations", "working_directory", "working_directory TEXT NOT NULL DEFAULT ''"},
		{"actor_allocations", "provisional", "provisional INTEGER NOT NULL DEFAULT 0"},
	} {
		hasColumn, err := columnExists(ctx, conn, addition.table, addition.column)
		if err != nil {
			return err
		}
		if !hasColumn {
			statement := `ALTER TABLE ` + addition.table + ` ADD COLUMN ` + addition.definition
			if _, err := conn.ExecContext(ctx, statement); err != nil {
				return fmt.Errorf("add %s.%s: %w", addition.table, addition.column, err)
			}
		}
	}
	if _, err := conn.ExecContext(ctx, `UPDATE registrations SET registered_at_ns = updated_at_ns WHERE registered_at_ns IS NULL`); err != nil {
		return fmt.Errorf("backfill registration timestamps: %w", err)
	}
	// Process-only actor reservations belong to live API connections. No such
	// connection survives a daemon restart, so carrying them forward would leak
	// invisible suffix reservations.
	staleProvisional := `
		SELECT a.actor FROM actor_allocations a
		WHERE a.provisional = 1
		  AND NOT EXISTS (SELECT 1 FROM continuity_bindings c WHERE c.actor = a.actor AND c.handle NOT LIKE 'process:%')
		  AND NOT EXISTS (SELECT 1 FROM actor_profiles p WHERE p.actor = a.actor)
		  AND NOT EXISTS (SELECT 1 FROM registrations r WHERE r.actor = a.actor)
		  AND NOT EXISTS (SELECT 1 FROM messages m WHERE m.from_actor = a.actor)
		  AND NOT EXISTS (SELECT 1 FROM deliveries d WHERE d.recipient_actor = a.actor)`
	if _, err := conn.ExecContext(ctx, `DELETE FROM continuity_bindings WHERE actor IN (`+staleProvisional+`)`); err != nil {
		return fmt.Errorf("clear stale provisional continuity bindings: %w", err)
	}
	if _, err := conn.ExecContext(ctx, `DELETE FROM actor_allocations WHERE actor IN (`+staleProvisional+`)`); err != nil {
		return fmt.Errorf("clear stale provisional actor allocations: %w", err)
	}
	if _, err := conn.ExecContext(ctx,
		`INSERT OR IGNORE INTO schema_migrations(version, applied_at_ns) VALUES (?, ?)`,
		migrationVersion, s.now().UTC().UnixNano()); err != nil {
		return fmt.Errorf("record migration: %w", err)
	}
	return nil
}

func columnExists(ctx context.Context, conn *sql.Conn, table, column string) (bool, error) {
	rows, err := conn.QueryContext(ctx, `PRAGMA table_info(`+table+`)`)
	if err != nil {
		return false, fmt.Errorf("inspect %s columns: %w", table, err)
	}
	defer rows.Close()
	for rows.Next() {
		var cid int
		var name, dataType string
		var notNull, primaryKey int
		var defaultValue interface{}
		if err := rows.Scan(&cid, &name, &dataType, &notNull, &defaultValue, &primaryKey); err != nil {
			return false, err
		}
		if name == column {
			return true, nil
		}
	}
	return false, rows.Err()
}

func (s *Store) Send(ctx context.Context, request bus.SendRequest) (bus.SendResult, error) {
	req, err := bus.NormalizeSendRequest(request)
	if err != nil {
		return bus.SendResult{}, err
	}
	now := s.now().UTC()
	if req.ExpiresAt != nil && !req.ExpiresAt.After(now) {
		return bus.SendResult{}, &bus.ValidationError{Field: "expires_at", Problem: "must be in the future"}
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return bus.SendResult{}, fmt.Errorf("begin send: %w", err)
	}
	defer tx.Rollback()
	if err := assertActorNotAdoptedTx(ctx, tx, req.FromActor); err != nil {
		return bus.SendResult{}, err
	}

	if req.InReplyTo != "" {
		parent, err := getMessageByIDTx(ctx, tx, req.InReplyTo)
		if err != nil {
			if errors.Is(err, bus.ErrNotFound) {
				return bus.SendResult{}, &bus.ValidationError{Field: "in_reply_to", Problem: "message does not exist"}
			}
			return bus.SendResult{}, err
		}
		if req.ProjectID != parent.ProjectID || req.ChannelID != parent.ChannelID {
			return bus.SendResult{}, &bus.ValidationError{Field: "in_reply_to", Problem: "message belongs to a different project or channel"}
		}
		if req.ThreadID == "" {
			req.ThreadID = parent.ThreadID
		} else if req.ThreadID != parent.ThreadID {
			return bus.SendResult{}, &bus.ValidationError{Field: "thread_id", Problem: "does not match in_reply_to message"}
		}
	}
	existing, err := getMessageByIdempotencyTx(ctx, tx, req.FromActor, req.IdempotencyKey)
	if err == nil {
		if !bus.EquivalentRequest(existing, req) {
			return bus.SendResult{}, bus.ErrIdempotencyConflict
		}
		return bus.SendResult{Message: existing, Duplicate: true}, nil
	}
	if !errors.Is(err, bus.ErrNotFound) {
		return bus.SendResult{}, err
	}

	messageID, err := s.newID("msg")
	if err != nil {
		return bus.SendResult{}, fmt.Errorf("generate message id: %w", err)
	}
	message := bus.Message{
		ID:              messageID,
		SchemaVersion:   bus.SchemaVersion,
		IdempotencyKey:  req.IdempotencyKey,
		ProjectID:       req.ProjectID,
		ChannelID:       req.ChannelID,
		ThreadID:        req.ThreadID,
		FromActor:       req.FromActor,
		FromRun:         req.FromRun,
		FromRole:        req.FromRole,
		ToActors:        append([]string(nil), req.ToActors...),
		Type:            req.Type,
		DeliveryRequest: req.DeliveryRequest,
		InReplyTo:       req.InReplyTo,
		Body:            append(json.RawMessage(nil), req.Body...),
		CreatedAt:       now,
		ExpiresAt:       req.ExpiresAt,
	}

	var expires interface{}
	if req.ExpiresAt != nil {
		expires = req.ExpiresAt.UnixNano()
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO messages(
			message_id, schema_version, idempotency_key, project_id, channel_id, thread_id,
			from_actor, from_run, from_role, message_type, delivery_request, in_reply_to, body,
			created_at_ns, expires_at_ns
		) VALUES (?, ?, ?, ?, ?, NULLIF(?, ''), ?, ?, NULLIF(?, ''), ?, ?, NULLIF(?, ''), ?, ?, ?)`,
		message.ID, message.SchemaVersion, message.IdempotencyKey, message.ProjectID,
		message.ChannelID, message.ThreadID, message.FromActor, message.FromRun,
		message.FromRole, message.Type, message.DeliveryRequest, message.InReplyTo, []byte(message.Body),
		message.CreatedAt.UnixNano(), expires); err != nil {
		return bus.SendResult{}, fmt.Errorf("insert message: %w", err)
	}
	for _, recipient := range message.ToActors {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO deliveries(message_id, recipient_actor, state)
			VALUES (?, ?, ?)`, message.ID, recipient, bus.DeliveryQueued); err != nil {
			return bus.SendResult{}, fmt.Errorf("insert delivery for %s: %w", recipient, err)
		}
		if message.DeliveryRequest != bus.DeliveryNonBlocking {
			notificationActor := recipient
			if err := tx.QueryRowContext(ctx, `SELECT adopting_actor FROM actor_adoptions WHERE source_actor = ?`, recipient).
				Scan(&notificationActor); err != nil && !errors.Is(err, sql.ErrNoRows) {
				return bus.SendResult{}, fmt.Errorf("resolve adopted notification actor: %w", err)
			}
			if _, err := tx.ExecContext(ctx, `
				INSERT OR IGNORE INTO notification_outbox(
					message_id, recipient_actor, state, available_at_ns, created_at_ns
				) VALUES (?, ?, 'pending', ?, ?)`, message.ID, notificationActor, now.UnixNano(), now.UnixNano()); err != nil {
				return bus.SendResult{}, fmt.Errorf("enqueue notification for %s: %w", recipient, err)
			}
		}
	}
	provenance := bus.EventProvenance(ctx, message.FromRun)
	provenance["from_run"] = message.FromRun
	provenance["type"] = message.Type
	provenance["recipients"] = message.ToActors
	if err := s.appendEventTx(ctx, tx, message.ProjectID, "durable", "message.sent",
		message.ID, message.FromActor, provenance, now); err != nil {
		return bus.SendResult{}, err
	}
	for _, recipient := range message.ToActors {
		if err := s.appendEventTx(ctx, tx, message.ProjectID, "operational", "delivery.queued",
			message.ID, recipient, nil, now); err != nil {
			return bus.SendResult{}, err
		}
	}
	if err := tx.Commit(); err != nil {
		return bus.SendResult{}, fmt.Errorf("commit send: %w", err)
	}
	result := bus.SendResult{Message: message}
	if message.DeliveryRequest != bus.DeliveryNonBlocking {
		result.NotificationState = "pending"
	}
	return result, nil
}

func (s *Store) CheckInbox(ctx context.Context, actor string, limit int) ([]bus.InboxItem, error) {
	actor = strings.TrimSpace(actor)
	if actor == "" {
		return nil, &bus.ValidationError{Field: "actor", Problem: "is required"}
	}
	if limit <= 0 || limit > 100 {
		limit = 100
	}
	nowNS := s.now().UTC().UnixNano()
	rows, err := s.db.QueryContext(ctx, `
		WITH candidates AS (
			SELECT d.message_id, d.recipient_actor AS original_recipient_actor, d.state, d.attempt,
			       d.lease_expires_at_ns,
			       ROW_NUMBER() OVER (
				   PARTITION BY d.message_id
				   ORDER BY CASE WHEN d.recipient_actor = ? AND a.source_actor IS NULL THEN 0 ELSE 1 END,
				            d.recipient_actor
			       ) AS preference
			FROM deliveries d
			LEFT JOIN actor_adoptions a ON a.source_actor = d.recipient_actor
			WHERE (d.recipient_actor = ? AND a.source_actor IS NULL) OR a.adopting_actor = ?
		)
		SELECT m.message_id, m.project_id, m.channel_id, COALESCE(m.thread_id, ''),
		       m.from_actor, COALESCE(m.from_role, ''), m.message_type, m.delivery_request,
		       c.state, c.attempt, m.created_at_ns, m.expires_at_ns, c.lease_expires_at_ns,
		       c.original_recipient_actor
		FROM candidates c
		JOIN messages m ON m.message_id = c.message_id
		WHERE c.preference = 1 AND c.state IN (?, ?)
		  AND (m.expires_at_ns IS NULL OR m.expires_at_ns > ?)
		ORDER BY m.created_at_ns, m.message_id
		LIMIT ?`, actor, actor, actor, bus.DeliveryQueued, bus.DeliveryClaimed, nowNS, limit)
	if err != nil {
		return nil, fmt.Errorf("query inbox: %w", err)
	}
	defer rows.Close()

	items := make([]bus.InboxItem, 0)
	for rows.Next() {
		var item bus.InboxItem
		var createdNS int64
		var expiresNS, leaseExpiresNS sql.NullInt64
		if err := rows.Scan(&item.MessageID, &item.ProjectID, &item.ChannelID, &item.ThreadID,
			&item.FromActor, &item.FromRole, &item.Type, &item.DeliveryRequest,
			&item.State, &item.Attempt, &createdNS, &expiresNS, &leaseExpiresNS,
			&item.OriginalRecipientActor); err != nil {
			return nil, fmt.Errorf("scan inbox: %w", err)
		}
		item.CreatedAt = time.Unix(0, createdNS).UTC()
		item.ExpiresAt = timeFromNull(expiresNS)
		item.Available = item.State == bus.DeliveryQueued ||
			(item.State == bus.DeliveryClaimed && leaseExpiresNS.Valid && leaseExpiresNS.Int64 <= nowNS)
		item.RecipientActor = actor
		if item.OriginalRecipientActor == actor {
			item.OriginalRecipientActor = ""
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate inbox: %w", err)
	}
	return items, nil
}

func (s *Store) Claim(ctx context.Context, actor, messageID string, lease time.Duration) (bus.Claim, error) {
	actor = strings.TrimSpace(actor)
	messageID = strings.TrimSpace(messageID)
	if actor == "" {
		return bus.Claim{}, &bus.ValidationError{Field: "actor", Problem: "is required"}
	}
	if lease <= 0 || lease > 24*time.Hour {
		return bus.Claim{}, &bus.ValidationError{Field: "lease", Problem: "must be between 0 and 24h"}
	}
	now := s.now().UTC()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return bus.Claim{}, fmt.Errorf("begin claim: %w", err)
	}
	defer tx.Rollback()

	query := `
		WITH candidates AS (
			SELECT d.message_id, d.recipient_actor AS original_recipient_actor, d.state,
			       d.lease_expires_at_ns,
			       ROW_NUMBER() OVER (
				   PARTITION BY d.message_id
				   ORDER BY CASE WHEN d.recipient_actor = ? AND a.source_actor IS NULL THEN 0 ELSE 1 END,
				            d.recipient_actor
			       ) AS preference
			FROM deliveries d
			LEFT JOIN actor_adoptions a ON a.source_actor = d.recipient_actor
			WHERE (d.recipient_actor = ? AND a.source_actor IS NULL) OR a.adopting_actor = ?
		)
		SELECT m.message_id, c.original_recipient_actor
		FROM candidates c JOIN messages m ON m.message_id = c.message_id
		WHERE c.preference = 1
		  AND (c.state = ? OR (c.state = ? AND c.lease_expires_at_ns <= ?))
		  AND (m.expires_at_ns IS NULL OR m.expires_at_ns > ?)`
	args := []interface{}{actor, actor, actor, bus.DeliveryQueued, bus.DeliveryClaimed, now.UnixNano(), now.UnixNano()}
	if messageID != "" {
		query += " AND m.message_id = ?"
		args = append(args, messageID)
	}
	query += " ORDER BY m.created_at_ns, m.message_id LIMIT 1"
	var originalRecipient string
	if err := tx.QueryRowContext(ctx, query, args...).Scan(&messageID, &originalRecipient); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return bus.Claim{}, bus.ErrNoMessage
		}
		return bus.Claim{}, fmt.Errorf("select claimable delivery: %w", err)
	}

	leaseToken, err := s.newID("lease")
	if err != nil {
		return bus.Claim{}, fmt.Errorf("generate lease token: %w", err)
	}
	leaseExpires := now.Add(lease)
	result, err := tx.ExecContext(ctx, `
		UPDATE deliveries
		SET state = ?, attempt = attempt + 1, lease_token = ?, lease_expires_at_ns = ?,
		    claimed_at_ns = ?, acked_at_ns = NULL
		WHERE message_id = ? AND recipient_actor = ?
		  AND (state = ? OR (state = ? AND lease_expires_at_ns <= ?))`,
		bus.DeliveryClaimed, leaseToken, leaseExpires.UnixNano(), now.UnixNano(),
		messageID, originalRecipient, bus.DeliveryQueued, bus.DeliveryClaimed, now.UnixNano())
	if err != nil {
		return bus.Claim{}, fmt.Errorf("claim delivery: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return bus.Claim{}, fmt.Errorf("inspect claim result: %w", err)
	}
	if changed != 1 {
		return bus.Claim{}, bus.ErrNoMessage
	}

	message, err := getMessageByIDTx(ctx, tx, messageID)
	if err != nil {
		return bus.Claim{}, err
	}
	var attempt int
	if err := tx.QueryRowContext(ctx,
		`SELECT attempt FROM deliveries WHERE message_id = ? AND recipient_actor = ?`,
		messageID, originalRecipient).Scan(&attempt); err != nil {
		return bus.Claim{}, fmt.Errorf("read claim attempt: %w", err)
	}
	claimPayload := bus.EventProvenance(ctx, "")
	claimPayload["attempt"] = attempt
	if originalRecipient != actor {
		claimPayload["original_recipient_actor"] = originalRecipient
	}
	if err := s.appendEventTx(ctx, tx, message.ProjectID, "operational", "delivery.claimed",
		message.ID, actor, claimPayload, now); err != nil {
		return bus.Claim{}, err
	}
	// Adapter acceptance is not delivery proof. A successful claim is, so it
	// closes the actor-scoped notification job and stops further wake retries.
	if _, err := tx.ExecContext(ctx, `
		UPDATE notification_outbox SET state = 'done', available_at_ns = ?
		WHERE message_id = ? AND recipient_actor = ? AND state != 'done'`,
		now.UnixNano(), message.ID, actor); err != nil {
		return bus.Claim{}, err
	}
	if err := tx.Commit(); err != nil {
		return bus.Claim{}, fmt.Errorf("commit claim: %w", err)
	}
	return bus.Claim{
		Message: message, RecipientActor: actor, OriginalRecipientActor: adoptionProvenance(actor, originalRecipient), Attempt: attempt,
		LeaseToken: leaseToken, LeaseExpiresAt: leaseExpires,
	}, nil
}

func (s *Store) Ack(ctx context.Context, actor, messageID, leaseToken string) error {
	return s.finish(ctx, actor, messageID, leaseToken, true, false, "")
}

func (s *Store) Extend(ctx context.Context, actor, messageID, leaseToken string, lease time.Duration) (bus.LeaseExtension, error) {
	actor = strings.TrimSpace(actor)
	messageID = strings.TrimSpace(messageID)
	leaseToken = strings.TrimSpace(leaseToken)
	if actor == "" || messageID == "" || leaseToken == "" {
		return bus.LeaseExtension{}, &bus.ValidationError{Field: "delivery", Problem: "actor, message_id, and lease_token are required"}
	}
	if lease <= 0 || lease > 24*time.Hour {
		return bus.LeaseExtension{}, &bus.ValidationError{Field: "lease", Problem: "must be between 0 and 24h"}
	}
	now := s.now().UTC()
	expires := now.Add(lease)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return bus.LeaseExtension{}, fmt.Errorf("begin lease extension: %w", err)
	}
	defer tx.Rollback()
	originalRecipient, err := effectiveDeliveryRecipientTx(ctx, tx, actor, messageID)
	if err != nil {
		return bus.LeaseExtension{}, err
	}
	var state bus.DeliveryState
	var storedToken sql.NullString
	var leaseExpires sql.NullInt64
	if err := tx.QueryRowContext(ctx, `
		SELECT state, lease_token, lease_expires_at_ns FROM deliveries
		WHERE message_id = ? AND recipient_actor = ?`, messageID, originalRecipient).Scan(&state, &storedToken, &leaseExpires); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return bus.LeaseExtension{}, bus.ErrNotFound
		}
		return bus.LeaseExtension{}, err
	}
	if state != bus.DeliveryClaimed || !storedToken.Valid || storedToken.String != leaseToken {
		return bus.LeaseExtension{}, bus.ErrLeaseTokenMismatch
	}
	if !leaseExpires.Valid || leaseExpires.Int64 <= now.UnixNano() {
		return bus.LeaseExtension{}, bus.ErrLeaseExpired
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE deliveries SET lease_expires_at_ns = ?
		WHERE message_id = ? AND recipient_actor = ? AND state = ? AND lease_token = ?`,
		expires.UnixNano(), messageID, originalRecipient, bus.DeliveryClaimed, leaseToken); err != nil {
		return bus.LeaseExtension{}, fmt.Errorf("extend delivery lease: %w", err)
	}
	message, err := getMessageByIDTx(ctx, tx, messageID)
	if err != nil {
		return bus.LeaseExtension{}, err
	}
	payload := bus.EventProvenance(ctx, "")
	payload["lease_expires_at"] = expires
	if originalRecipient != actor {
		payload["original_recipient_actor"] = originalRecipient
	}
	if err := s.appendEventTx(ctx, tx, message.ProjectID, "operational", "delivery.extended", messageID, actor, payload, now); err != nil {
		return bus.LeaseExtension{}, err
	}
	if err := tx.Commit(); err != nil {
		return bus.LeaseExtension{}, err
	}
	return bus.LeaseExtension{MessageID: messageID, RecipientActor: actor, LeaseExpiresAt: expires}, nil
}

func (s *Store) Nack(ctx context.Context, actor, messageID, leaseToken, reason string, final bool) error {
	return s.finish(ctx, actor, messageID, leaseToken, false, final, reason)
}

func (s *Store) finish(ctx context.Context, actor, messageID, leaseToken string, ack, final bool, reason string) error {
	actor = strings.TrimSpace(actor)
	messageID = strings.TrimSpace(messageID)
	leaseToken = strings.TrimSpace(leaseToken)
	if actor == "" || messageID == "" || leaseToken == "" {
		return &bus.ValidationError{Field: "delivery", Problem: "actor, message_id, and lease_token are required"}
	}
	now := s.now().UTC()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin delivery transition: %w", err)
	}
	defer tx.Rollback()
	originalRecipient, err := effectiveDeliveryRecipientTx(ctx, tx, actor, messageID)
	if err != nil {
		return err
	}
	var state bus.DeliveryState
	var storedToken sql.NullString
	var terminalToken sql.NullString
	if err := tx.QueryRowContext(ctx, `
		SELECT state, lease_token, terminal_lease_token
		FROM deliveries WHERE message_id = ? AND recipient_actor = ?`,
		messageID, originalRecipient).Scan(&state, &storedToken, &terminalToken); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return bus.ErrNotFound
		}
		return fmt.Errorf("read delivery: %w", err)
	}
	if state == bus.DeliveryAcked && ack && terminalToken.Valid && terminalToken.String == leaseToken {
		return nil
	}
	if state == bus.DeliveryDeadLettered && final && !ack && terminalToken.Valid && terminalToken.String == leaseToken {
		return nil
	}
	if state == bus.DeliveryAcked || state == bus.DeliveryDeadLettered {
		return bus.ErrDeliveryTerminal
	}
	if state != bus.DeliveryClaimed || !storedToken.Valid || storedToken.String != leaseToken {
		return bus.ErrLeaseTokenMismatch
	}
	message, err := getMessageByIDTx(ctx, tx, messageID)
	if err != nil {
		return err
	}
	var next bus.DeliveryState
	var kind string
	if ack {
		next, kind = bus.DeliveryAcked, "delivery.acked"
	} else if final {
		next, kind = bus.DeliveryDeadLettered, "delivery.dead_lettered"
	} else {
		next, kind = bus.DeliveryQueued, "delivery.nacked"
	}
	var ackedAt interface{}
	var terminalLease interface{}
	if ack {
		ackedAt = now.UnixNano()
	}
	if ack || final {
		terminalLease = leaseToken
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE deliveries
		SET state = ?, lease_token = NULL, lease_expires_at_ns = NULL,
		    terminal_lease_token = ?, acked_at_ns = ?, last_error = NULLIF(?, '')
		WHERE message_id = ? AND recipient_actor = ?`,
		next, terminalLease, ackedAt, reason, messageID, originalRecipient); err != nil {
		return fmt.Errorf("finish delivery: %w", err)
	}
	payload := bus.EventProvenance(ctx, "")
	if reason != "" {
		payload["reason"] = reason
	}
	if originalRecipient != actor {
		payload["original_recipient_actor"] = originalRecipient
	}
	if err := s.appendEventTx(ctx, tx, message.ProjectID, "operational", kind,
		messageID, actor, payload, now); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit delivery transition: %w", err)
	}
	return nil
}

func effectiveDeliveryRecipientTx(ctx context.Context, tx *sql.Tx, actor, messageID string) (string, error) {
	var original string
	err := tx.QueryRowContext(ctx, `
		WITH candidates AS (
			SELECT d.recipient_actor AS original_recipient_actor,
			       ROW_NUMBER() OVER (
				   ORDER BY CASE WHEN d.recipient_actor = ? AND a.source_actor IS NULL THEN 0 ELSE 1 END,
				            d.recipient_actor
			       ) AS preference
			FROM deliveries d
			LEFT JOIN actor_adoptions a ON a.source_actor = d.recipient_actor
			WHERE d.message_id = ?
			  AND ((d.recipient_actor = ? AND a.source_actor IS NULL) OR a.adopting_actor = ?)
		)
		SELECT original_recipient_actor FROM candidates WHERE preference = 1`, actor, messageID, actor, actor).Scan(&original)
	if errors.Is(err, sql.ErrNoRows) {
		return "", bus.ErrNotFound
	}
	if err != nil {
		return "", fmt.Errorf("resolve effective delivery recipient: %w", err)
	}
	return original, nil
}

func adoptionProvenance(actor, original string) string {
	if actor == original {
		return ""
	}
	return original
}

func (s *Store) ListEvents(ctx context.Context, partition, stream string, after int64, limit int) ([]bus.Event, error) {
	partition = strings.TrimSpace(partition)
	stream = strings.TrimSpace(stream)
	if partition == "" || stream == "" {
		return nil, &bus.ValidationError{Field: "event_cursor", Problem: "partition and stream are required"}
	}
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT event_id, partition_id, stream, position, kind,
		       COALESCE(message_id, ''), COALESCE(actor_id, ''), payload, created_at_ns
		FROM events
		WHERE partition_id = ? AND stream = ? AND position > ?
		ORDER BY position LIMIT ?`, partition, stream, after, limit)
	if err != nil {
		return nil, fmt.Errorf("query events: %w", err)
	}
	defer rows.Close()
	events := make([]bus.Event, 0)
	for rows.Next() {
		var event bus.Event
		var payload []byte
		var createdNS int64
		if err := rows.Scan(&event.ID, &event.PartitionID, &event.Stream, &event.Position,
			&event.Kind, &event.MessageID, &event.ActorID, &payload, &createdNS); err != nil {
			return nil, fmt.Errorf("scan event: %w", err)
		}
		event.Payload = append(json.RawMessage(nil), payload...)
		event.CreatedAt = time.Unix(0, createdNS).UTC()
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate events: %w", err)
	}
	return events, nil
}

func (s *Store) appendEventTx(ctx context.Context, tx *sql.Tx, partition, stream, kind,
	messageID, actorID string, payload interface{}, now time.Time) error {
	if _, err := tx.ExecContext(ctx, `
		INSERT OR IGNORE INTO partition_counters(partition_id, stream, next_position)
		VALUES (?, ?, 1)`, partition, stream); err != nil {
		return fmt.Errorf("initialize event position: %w", err)
	}
	var position int64
	if err := tx.QueryRowContext(ctx, `
		SELECT next_position FROM partition_counters WHERE partition_id = ? AND stream = ?`,
		partition, stream).Scan(&position); err != nil {
		return fmt.Errorf("read event position: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE partition_counters SET next_position = next_position + 1
		WHERE partition_id = ? AND stream = ?`, partition, stream); err != nil {
		return fmt.Errorf("advance event position: %w", err)
	}
	eventID, err := s.newID("evt")
	if err != nil {
		return fmt.Errorf("generate event id: %w", err)
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("encode event payload: %w", err)
	}
	if payload == nil {
		encoded = nil
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO events(event_id, partition_id, stream, position, kind, message_id,
		                   actor_id, payload, created_at_ns)
		VALUES (?, ?, ?, ?, ?, NULLIF(?, ''), NULLIF(?, ''), ?, ?)`,
		eventID, partition, stream, position, kind, messageID, actorID, encoded, now.UnixNano()); err != nil {
		return fmt.Errorf("insert event: %w", err)
	}
	return nil
}

func getMessageByIdempotencyTx(ctx context.Context, tx *sql.Tx, actor, key string) (bus.Message, error) {
	var messageID string
	if err := tx.QueryRowContext(ctx,
		`SELECT message_id FROM messages WHERE from_actor = ? AND idempotency_key = ?`,
		actor, key).Scan(&messageID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return bus.Message{}, bus.ErrNotFound
		}
		return bus.Message{}, fmt.Errorf("find idempotent message: %w", err)
	}
	return getMessageByIDTx(ctx, tx, messageID)
}

func getMessageByIDTx(ctx context.Context, tx *sql.Tx, messageID string) (bus.Message, error) {
	var message bus.Message
	var body []byte
	var threadID, fromRole sql.NullString
	var createdNS int64
	var expiresNS sql.NullInt64
	if err := tx.QueryRowContext(ctx, `
		SELECT message_id, schema_version, idempotency_key, project_id, channel_id,
		       thread_id, from_actor, from_run, from_role, message_type,
		       delivery_request, COALESCE(in_reply_to, ''), body, created_at_ns, expires_at_ns
		FROM messages WHERE message_id = ?`, messageID).Scan(
		&message.ID, &message.SchemaVersion, &message.IdempotencyKey, &message.ProjectID,
		&message.ChannelID, &threadID, &message.FromActor, &message.FromRun, &fromRole,
		&message.Type, &message.DeliveryRequest, &message.InReplyTo, &body, &createdNS, &expiresNS); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return bus.Message{}, bus.ErrNotFound
		}
		return bus.Message{}, fmt.Errorf("read message: %w", err)
	}
	message.ThreadID = threadID.String
	message.FromRole = fromRole.String
	message.Body = append(json.RawMessage(nil), body...)
	message.CreatedAt = time.Unix(0, createdNS).UTC()
	message.ExpiresAt = timeFromNull(expiresNS)

	rows, err := tx.QueryContext(ctx,
		`SELECT recipient_actor FROM deliveries WHERE message_id = ? ORDER BY recipient_actor`, messageID)
	if err != nil {
		return bus.Message{}, fmt.Errorf("read recipients: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var recipient string
		if err := rows.Scan(&recipient); err != nil {
			return bus.Message{}, fmt.Errorf("scan recipient: %w", err)
		}
		message.ToActors = append(message.ToActors, recipient)
	}
	if err := rows.Err(); err != nil {
		return bus.Message{}, fmt.Errorf("iterate recipients: %w", err)
	}
	return message, nil
}

func timeFromNull(value sql.NullInt64) *time.Time {
	if !value.Valid {
		return nil
	}
	parsed := time.Unix(0, value.Int64).UTC()
	return &parsed
}

func randomID(prefix string) (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return prefix + "_" + hex.EncodeToString(raw[:]), nil
}
