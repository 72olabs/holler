package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/72olabs/holler/internal/bus"
)

func (s *Store) RegisterSession(ctx context.Context, request bus.RegistrationRequest) (bus.Registration, error) {
	req := request
	req.Actor = strings.TrimSpace(req.Actor)
	req.RunID = strings.TrimSpace(req.RunID)
	req.Harness = strings.TrimSpace(req.Harness)
	req.AttentionMode = strings.TrimSpace(req.AttentionMode)
	req.SessionID = strings.TrimSpace(req.SessionID)
	req.DeliveryHandle = strings.TrimSpace(req.DeliveryHandle)
	req.ProjectID = strings.TrimSpace(req.ProjectID)
	if req.DeliveryHandle == "" {
		req.DeliveryHandle = req.SessionID
	}
	if req.ProjectID == "" {
		req.ProjectID = "default"
	}
	if req.AttentionMode == "" {
		switch req.Harness {
		case "codex":
			req.AttentionMode = "native-queue"
		case "claude":
			req.AttentionMode = "hook-long-poll"
		case "opencode":
			req.AttentionMode = "native-prompt"
		}
	}
	if req.Lease <= 0 || req.Lease > 7*24*time.Hour {
		return bus.Registration{}, &bus.ValidationError{Field: "registration.lease", Problem: "must be between 0 and 7d"}
	}
	for _, field := range []struct{ name, value string }{
		{"actor", req.Actor}, {"run_id", req.RunID}, {"harness", req.Harness},
		{"session_id", req.SessionID}, {"delivery_handle", req.DeliveryHandle},
	} {
		if field.value == "" {
			return bus.Registration{}, &bus.ValidationError{Field: "registration." + field.name, Problem: "is required"}
		}
	}
	if req.Harness != "codex" && req.Harness != "claude" && req.Harness != "opencode" && req.Harness != "test" {
		return bus.Registration{}, &bus.ValidationError{Field: "registration.harness", Problem: "must be codex, claude, opencode, or test"}
	}
	switch req.Harness {
	case "codex":
		if req.AttentionMode != "native-queue" && req.AttentionMode != "startup-only" {
			return bus.Registration{}, &bus.ValidationError{Field: "registration.attention_mode", Problem: "must be native-queue or startup-only for codex"}
		}
	case "claude":
		if req.AttentionMode != "hook-long-poll" && req.AttentionMode != "startup-only" {
			return bus.Registration{}, &bus.ValidationError{Field: "registration.attention_mode", Problem: "must be hook-long-poll or startup-only for claude"}
		}
	case "opencode":
		if req.AttentionMode != "native-prompt" && req.AttentionMode != "startup-only" {
			return bus.Registration{}, &bus.ValidationError{Field: "registration.attention_mode", Problem: "must be native-prompt or startup-only for opencode"}
		}
	}

	now := s.now().UTC()
	expires := now.Add(req.Lease)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return bus.Registration{}, fmt.Errorf("begin registration: %w", err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO registrations(
			actor, run_id, harness, attention_mode, session_id, delivery_handle, project_id,
			epoch, registered_at_ns, updated_at_ns, lease_expires_at_ns
		) VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?)
		ON CONFLICT(actor, run_id, session_id) DO UPDATE SET
			harness = excluded.harness,
			attention_mode = excluded.attention_mode,
			delivery_handle = excluded.delivery_handle,
			project_id = excluded.project_id,
			epoch = registrations.epoch + 1,
			registered_at_ns = excluded.registered_at_ns,
			updated_at_ns = excluded.updated_at_ns,
			lease_expires_at_ns = excluded.lease_expires_at_ns,
			ended_at_ns = NULL,
			attention_superseded_at_ns = NULL`,
		req.Actor, req.RunID, req.Harness, req.AttentionMode, req.SessionID, req.DeliveryHandle,
		req.ProjectID, now.UnixNano(), now.UnixNano(), expires.UnixNano()); err != nil {
		return bus.Registration{}, fmt.Errorf("register session: %w", err)
	}
	registration, err := getRegistrationTx(ctx, tx, req.Actor, req.RunID, req.SessionID)
	if err != nil {
		return bus.Registration{}, err
	}
	payload := bus.EventProvenance(ctx, req.RunID)
	payload["harness"] = req.Harness
	payload["attention_mode"] = req.AttentionMode
	payload["session_id"] = req.SessionID
	payload["epoch"] = registration.Epoch
	if err := s.appendEventTx(ctx, tx, req.ProjectID, "operational", "session.registered",
		"", req.Actor, payload, now); err != nil {
		return bus.Registration{}, err
	}
	if err := tx.Commit(); err != nil {
		return bus.Registration{}, fmt.Errorf("commit registration: %w", err)
	}
	return registration, nil
}

func (s *Store) LiveRegistrations(ctx context.Context, actor string) ([]bus.Registration, error) {
	actor = strings.TrimSpace(actor)
	if actor == "" {
		return nil, &bus.ValidationError{Field: "actor", Problem: "is required"}
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT actor, run_id, harness, attention_mode, session_id, delivery_handle, project_id,
		       epoch, updated_at_ns, lease_expires_at_ns
		FROM registrations
		WHERE actor = ? AND ended_at_ns IS NULL AND attention_superseded_at_ns IS NULL
		  AND lease_expires_at_ns > ?
		ORDER BY registered_at_ns DESC, run_id, session_id`, actor, s.now().UTC().UnixNano())
	if err != nil {
		return nil, fmt.Errorf("query registrations: %w", err)
	}
	defer rows.Close()
	registrations := make([]bus.Registration, 0)
	for rows.Next() {
		registration, err := scanRegistration(rows)
		if err != nil {
			return nil, err
		}
		registrations = append(registrations, registration)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate registrations: %w", err)
	}
	return registrations, nil
}

func (s *Store) HeartbeatRegistrations(ctx context.Context, actor, runID string, lease time.Duration) (int, error) {
	actor = strings.TrimSpace(actor)
	runID = strings.TrimSpace(runID)
	if actor == "" || runID == "" {
		return 0, &bus.ValidationError{Field: "registration", Problem: "actor and run_id are required"}
	}
	if lease <= 0 || lease > 24*time.Hour {
		return 0, &bus.ValidationError{Field: "registration.lease", Problem: "must be between 0 and 24h"}
	}
	now := s.now().UTC()
	result, err := s.db.ExecContext(ctx, `
		UPDATE registrations SET updated_at_ns = ?, lease_expires_at_ns = ?
		WHERE rowid = (
			SELECT rowid FROM registrations
			WHERE actor = ? AND run_id = ?
			ORDER BY registered_at_ns DESC, epoch DESC, rowid DESC
			LIMIT 1
		) AND ended_at_ns IS NULL AND attention_superseded_at_ns IS NULL`, now.UnixNano(), now.Add(lease).UnixNano(), actor, runID)
	if err != nil {
		return 0, err
	}
	changed, err := result.RowsAffected()
	return int(changed), err
}

// AttachMonitor renews the exact passive registration used by a live Claude
// result channel. Passive lease expiry is recoverable; explicit SessionEnd is
// terminal and cannot be revived.
func (s *Store) AttachMonitor(ctx context.Context, actor, runID, sessionID, harness, attentionMode string, lease time.Duration) (bus.Registration, error) {
	actor = strings.TrimSpace(actor)
	runID = strings.TrimSpace(runID)
	sessionID = strings.TrimSpace(sessionID)
	harness = strings.TrimSpace(harness)
	attentionMode = strings.TrimSpace(attentionMode)
	if actor == "" || runID == "" || sessionID == "" {
		return bus.Registration{}, &bus.ValidationError{Field: "registration", Problem: "actor, run_id, and session_id are required"}
	}
	if harness != "claude" || attentionMode != "hook-long-poll" {
		return bus.Registration{}, &bus.ValidationError{Field: "monitor", Problem: "requires claude hook-long-poll"}
	}
	if lease <= 0 || lease > 24*time.Hour {
		return bus.Registration{}, &bus.ValidationError{Field: "registration.lease", Problem: "must be between 0 and 24h"}
	}
	now := s.now().UTC()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return bus.Registration{}, fmt.Errorf("begin monitor attach: %w", err)
	}
	defer tx.Rollback()
	var storedHarness, storedMode string
	var rowID, registeredNS int64
	var endedNS, supersededNS sql.NullInt64
	err = tx.QueryRowContext(ctx, `
		SELECT rowid, harness, attention_mode, registered_at_ns, ended_at_ns, attention_superseded_at_ns FROM registrations
		WHERE actor = ? AND run_id = ? AND session_id = ?`, actor, runID, sessionID).
		Scan(&rowID, &storedHarness, &storedMode, &registeredNS, &endedNS, &supersededNS)
	if errors.Is(err, sql.ErrNoRows) {
		return bus.Registration{}, bus.ErrRegistrationExpired
	}
	if err != nil {
		return bus.Registration{}, fmt.Errorf("read monitor registration: %w", err)
	}
	if endedNS.Valid {
		return bus.Registration{}, bus.ErrSessionEnded
	}
	if supersededNS.Valid {
		return bus.Registration{}, bus.ErrPresenceSuperseded
	}
	if storedHarness != harness || storedMode != attentionMode {
		return bus.Registration{}, &bus.ValidationError{Field: "monitor", Problem: "does not match the registered harness attention adapter"}
	}
	var newer int
	if err := tx.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM registrations
		WHERE actor = ? AND harness = ? AND attention_mode = ?
		  AND ended_at_ns IS NULL AND attention_superseded_at_ns IS NULL
		  AND (registered_at_ns > ? OR (registered_at_ns = ? AND rowid > ?))`,
		actor, harness, attentionMode, registeredNS, registeredNS, rowID).Scan(&newer); err != nil {
		return bus.Registration{}, fmt.Errorf("compare monitor registration: %w", err)
	}
	if newer > 0 {
		if _, err := tx.ExecContext(ctx, `
			UPDATE registrations SET attention_superseded_at_ns = ?
			WHERE actor = ? AND run_id = ? AND session_id = ?`, now.UnixNano(), actor, runID, sessionID); err != nil {
			return bus.Registration{}, fmt.Errorf("supersede older monitor registration: %w", err)
		}
		if err := tx.Commit(); err != nil {
			return bus.Registration{}, fmt.Errorf("commit monitor supersession: %w", err)
		}
		return bus.Registration{}, bus.ErrPresenceSuperseded
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE registrations SET attention_superseded_at_ns = ?
		WHERE actor = ? AND harness = ? AND attention_mode = ?
		  AND ended_at_ns IS NULL AND attention_superseded_at_ns IS NULL
		  AND NOT (run_id = ? AND session_id = ?)`,
		now.UnixNano(), actor, harness, attentionMode, runID, sessionID); err != nil {
		return bus.Registration{}, fmt.Errorf("supersede previous monitor registration: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE registrations SET updated_at_ns = ?, lease_expires_at_ns = ?
		WHERE actor = ? AND run_id = ? AND session_id = ? AND ended_at_ns IS NULL`,
		now.UnixNano(), now.Add(lease).UnixNano(), actor, runID, sessionID); err != nil {
		return bus.Registration{}, fmt.Errorf("renew monitor registration: %w", err)
	}
	registration, err := getRegistrationTx(ctx, tx, actor, runID, sessionID)
	if err != nil {
		return bus.Registration{}, err
	}
	if err := tx.Commit(); err != nil {
		return bus.Registration{}, fmt.Errorf("commit monitor attach: %w", err)
	}
	return registration, nil
}

// RearmAcceptedNotifications makes accepted-but-unclaimed references eligible
// for one more wake after an actual attention-presence transition. The broker,
// not a monitor heartbeat, is responsible for identifying that transition.
func (s *Store) RearmAcceptedNotifications(ctx context.Context, actor string) error {
	actor = strings.TrimSpace(actor)
	if actor == "" {
		return &bus.ValidationError{Field: "actor", Problem: "is required"}
	}
	now := s.now().UTC()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin accepted notification rearm: %w", err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `
		UPDATE notification_outbox
		SET state = 'done', available_at_ns = ?, last_error = 'message expired before recipient claim'
		WHERE recipient_actor = ? AND state = 'accepted'
		  AND EXISTS (
			SELECT 1 FROM messages
			WHERE messages.message_id = notification_outbox.message_id
			  AND expires_at_ns IS NOT NULL AND expires_at_ns <= ?
		  )`, now.UnixNano(), actor, now.UnixNano()); err != nil {
		return fmt.Errorf("close expired accepted notifications: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE notification_outbox
		SET state = 'done', available_at_ns = ?, last_error = 'accepted wake rearm limit reached'
		WHERE recipient_actor = ? AND state = 'accepted' AND attempt >= 5`,
		now.UnixNano(), actor); err != nil {
		return fmt.Errorf("bound accepted notification rearm: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE notification_outbox
		SET state = 'pending', available_at_ns = ?, last_error = NULL
		WHERE recipient_actor = ? AND state = 'accepted' AND attempt < 5
		  AND EXISTS (
			SELECT 1 FROM messages
			WHERE messages.message_id = notification_outbox.message_id
			  AND (expires_at_ns IS NULL OR expires_at_ns > ?)
		  )`, now.UnixNano(), actor, now.UnixNano()); err != nil {
		return fmt.Errorf("rearm accepted notifications: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit accepted notification rearm: %w", err)
	}
	return nil
}

func (s *Store) RecordNotification(ctx context.Context, projectID, messageID string, attempt bus.NotificationAttempt) error {
	now := s.now().UTC()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin notification event: %w", err)
	}
	defer tx.Rollback()
	payload := map[string]interface{}{
		"run_id": attempt.RunID, "session_id": attempt.SessionID,
		"harness": attempt.Harness, "result": attempt.Result,
	}
	for key, value := range bus.EventProvenance(ctx, attempt.RunID) {
		payload[key] = value
	}
	if attempt.Detail != "" {
		payload["detail"] = attempt.Detail
	}
	if err := s.appendEventTx(ctx, tx, projectID, "operational", "delivery.notification",
		messageID, attempt.Actor, payload, now); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit notification event: %w", err)
	}
	return nil
}

func (s *Store) RecordHydration(ctx context.Context, projectID, actor, runID, harness, sessionID string, unread int) error {
	now := s.now().UTC()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin hydration event: %w", err)
	}
	defer tx.Rollback()
	payload := bus.EventProvenance(ctx, runID)
	payload["harness"] = harness
	payload["session_id"] = sessionID
	payload["unread"] = unread
	if err := s.appendEventTx(ctx, tx, projectID, "operational", "startup.hydrated", "", actor,
		payload, now); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit hydration event: %w", err)
	}
	return nil
}

func (s *Store) ExpireRegistration(ctx context.Context, actor, runID, sessionID, reason string) error {
	actor = strings.TrimSpace(actor)
	runID = strings.TrimSpace(runID)
	sessionID = strings.TrimSpace(sessionID)
	if actor == "" || runID == "" || sessionID == "" {
		return &bus.ValidationError{Field: "registration", Problem: "actor, run_id, and session_id are required"}
	}
	now := s.now().UTC()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin registration expiry: %w", err)
	}
	defer tx.Rollback()
	var projectID string
	var expiresNS int64
	var endedNS sql.NullInt64
	err = tx.QueryRowContext(ctx, `
		SELECT project_id, lease_expires_at_ns, ended_at_ns FROM registrations
		WHERE actor = ? AND run_id = ? AND session_id = ?`, actor, runID, sessionID).Scan(&projectID, &expiresNS, &endedNS)
	if errors.Is(err, sql.ErrNoRows) || (err == nil && endedNS.Valid) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read registration for expiry: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE registrations SET updated_at_ns = ?, lease_expires_at_ns = ?, ended_at_ns = ?
		WHERE actor = ? AND run_id = ? AND session_id = ?`,
		now.UnixNano(), now.UnixNano(), now.UnixNano(), actor, runID, sessionID); err != nil {
		return fmt.Errorf("expire registration: %w", err)
	}
	if err := s.appendEventTx(ctx, tx, projectID, "operational", "session.stale", "", actor,
		map[string]interface{}{"run_id": runID, "session_id": sessionID, "reason": reason}, now); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit registration expiry: %w", err)
	}
	return nil
}

func getRegistrationTx(ctx context.Context, tx *sql.Tx, actor, runID, sessionID string) (bus.Registration, error) {
	row := tx.QueryRowContext(ctx, `
		SELECT actor, run_id, harness, attention_mode, session_id, delivery_handle, project_id,
		       epoch, updated_at_ns, lease_expires_at_ns
		FROM registrations WHERE actor = ? AND run_id = ? AND session_id = ?`, actor, runID, sessionID)
	return scanRegistration(row)
}

type registrationScanner interface {
	Scan(...interface{}) error
}

func scanRegistration(scanner registrationScanner) (bus.Registration, error) {
	var registration bus.Registration
	var updatedNS, leaseExpiresNS int64
	if err := scanner.Scan(
		&registration.Actor, &registration.RunID, &registration.Harness, &registration.AttentionMode,
		&registration.SessionID, &registration.DeliveryHandle, &registration.ProjectID,
		&registration.Epoch, &updatedNS, &leaseExpiresNS,
	); err != nil {
		return bus.Registration{}, fmt.Errorf("scan registration: %w", err)
	}
	registration.UpdatedAt = time.Unix(0, updatedNS).UTC()
	registration.LeaseExpiresAt = time.Unix(0, leaseExpiresNS).UTC()
	return registration, nil
}
