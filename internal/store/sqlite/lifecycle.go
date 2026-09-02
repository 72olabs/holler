package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/72olabs/holler/internal/bus"
)

const archivePreviewRunes = 256

func (s *Store) ArchivePreflight(ctx context.Context, actor string, limit int) (bus.ActorArchivePreflight, error) {
	actor = strings.TrimSpace(actor)
	if actor == "" {
		return bus.ActorArchivePreflight{}, &bus.ValidationError{Field: "actor", Problem: "is required"}
	}
	if limit <= 0 || limit > 100 {
		limit = 25
	}
	now := s.now().UTC()
	preflight := bus.ActorArchivePreflight{Actor: actor, Aliases: []string{}, Unread: []bus.ActorArchiveMessage{}, Blockers: []string{}}
	var known int
	if err := s.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM actor_names WHERE actor = ?)`, actor).Scan(&known); err != nil {
		return preflight, err
	}
	if known == 0 {
		return preflight, bus.ErrNotFound
	}
	var state string
	if err := s.db.QueryRowContext(ctx, `SELECT state FROM actor_lifecycle WHERE actor = ?`, actor).Scan(&state); err == nil {
		preflight.Archived = state == "archived"
	} else if !errors.Is(err, sql.ErrNoRows) {
		return preflight, err
	}
	aliasRows, err := s.db.QueryContext(ctx, `SELECT alias FROM actor_aliases WHERE actor = ? ORDER BY alias`, actor)
	if err != nil {
		return preflight, err
	}
	for aliasRows.Next() {
		var alias string
		if err := aliasRows.Scan(&alias); err != nil {
			_ = aliasRows.Close()
			return preflight, err
		}
		preflight.Aliases = append(preflight.Aliases, alias)
	}
	if err := aliasRows.Close(); err != nil {
		return preflight, err
	}
	if err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM registrations
		WHERE actor = ? AND ended_at_ns IS NULL AND attention_superseded_at_ns IS NULL AND lease_expires_at_ns > ?`,
		actor, now.UnixNano()).Scan(&preflight.ControlPresence); err != nil {
		return preflight, err
	}
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM continuity_bindings WHERE actor = ?`, actor).
		Scan(&preflight.ContinuityBindings); err != nil {
		return preflight, err
	}
	if err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM deliveries d
		JOIN messages m ON m.message_id = d.message_id
		LEFT JOIN actor_adoptions a ON a.source_actor = d.recipient_actor
		WHERE COALESCE(a.adopting_actor, d.recipient_actor) = ? AND d.state = ?
		  AND d.lease_expires_at_ns > ? AND (m.expires_at_ns IS NULL OR m.expires_at_ns > ?)`,
		actor, bus.DeliveryClaimed, now.UnixNano(), now.UnixNano()).Scan(&preflight.ActiveClaims); err != nil {
		return preflight, err
	}
	rows, err := s.db.QueryContext(ctx, `
		WITH candidates AS (
			SELECT m.message_id, m.from_actor, m.created_at_ns, COALESCE(m.thread_id, '') AS thread_id, m.message_type, m.body,
			       ROW_NUMBER() OVER (PARTITION BY m.message_id ORDER BY d.recipient_actor) AS preference
			FROM deliveries d
			JOIN messages m ON m.message_id = d.message_id
			LEFT JOIN actor_adoptions a ON a.source_actor = d.recipient_actor
			WHERE COALESCE(a.adopting_actor, d.recipient_actor) = ?
			  AND (d.state = ? OR (d.state = ? AND d.lease_expires_at_ns <= ?))
			  AND (m.expires_at_ns IS NULL OR m.expires_at_ns > ?)
		)
		SELECT message_id, from_actor, created_at_ns, thread_id, message_type, body
		FROM candidates WHERE preference = 1 ORDER BY created_at_ns, message_id LIMIT ?`,
		actor, bus.DeliveryQueued, bus.DeliveryClaimed, now.UnixNano(), now.UnixNano(), limit+1)
	if err != nil {
		return preflight, err
	}
	for rows.Next() {
		var message bus.ActorArchiveMessage
		var createdNS int64
		var body []byte
		if err := rows.Scan(&message.MessageID, &message.FromActor, &createdNS, &message.ThreadID, &message.Type, &body); err != nil {
			_ = rows.Close()
			return preflight, err
		}
		message.CreatedAt = time.Unix(0, createdNS).UTC()
		message.BodyPreview = boundedArchivePreview(body)
		message.PreviewUntrusted = true
		if len(preflight.Unread) < limit {
			preflight.Unread = append(preflight.Unread, message)
		} else {
			preflight.UnreadTruncated = true
		}
	}
	if err := rows.Close(); err != nil {
		return preflight, err
	}
	if len(preflight.Aliases) > 0 {
		preflight.Blockers = append(preflight.Blockers, "aliases_point_to_actor")
	}
	if preflight.ControlPresence > 0 {
		preflight.Blockers = append(preflight.Blockers, "control_presence_live")
	}
	if preflight.ActiveClaims > 0 {
		preflight.Blockers = append(preflight.Blockers, "active_claims")
	}
	preflight.OperatorEligible = len(preflight.Aliases) == 0 && preflight.ControlPresence == 0 && preflight.ActiveClaims == 0
	preflight.AutomaticEligible = preflight.OperatorEligible && len(preflight.Unread) == 0 && !preflight.UnreadTruncated && preflight.ContinuityBindings == 0
	return preflight, nil
}

func boundedArchivePreview(body []byte) string {
	text := strings.ToValidUTF8(string(body), "�")
	if utf8.RuneCountInString(text) <= archivePreviewRunes {
		return text
	}
	runes := []rune(text)
	return string(runes[:archivePreviewRunes]) + "…"
}

func (s *Store) ArchiveActor(ctx context.Context, actor, changedBy string, allowUnread bool) (bus.ActorArchiveResult, error) {
	preflight, err := s.ArchivePreflight(ctx, actor, 100)
	if err != nil {
		return bus.ActorArchiveResult{}, err
	}
	if !preflight.OperatorEligible {
		return bus.ActorArchiveResult{}, &bus.ValidationError{Field: "actor", Problem: "cannot archive while aliases, live control presence, or active claims remain"}
	}
	if (len(preflight.Unread) > 0 || preflight.UnreadTruncated) && !allowUnread {
		return bus.ActorArchiveResult{}, &bus.ValidationError{Field: "allow_unread", Problem: "must be explicitly true after reviewing unread message previews"}
	}
	now := s.now().UTC()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return bus.ActorArchiveResult{}, err
	}
	defer tx.Rollback()
	// Recheck action-time blockers in the same transaction.
	var aliases, live, claims, unread int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM actor_aliases WHERE actor = ?`, actor).Scan(&aliases); err != nil {
		return bus.ActorArchiveResult{}, err
	}
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM registrations WHERE actor = ? AND ended_at_ns IS NULL AND attention_superseded_at_ns IS NULL AND lease_expires_at_ns > ?`, actor, now.UnixNano()).Scan(&live); err != nil {
		return bus.ActorArchiveResult{}, err
	}
	if err := tx.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM deliveries d
		LEFT JOIN actor_adoptions a ON a.source_actor = d.recipient_actor
		WHERE COALESCE(a.adopting_actor, d.recipient_actor) = ? AND d.state = ? AND d.lease_expires_at_ns > ?`,
		actor, bus.DeliveryClaimed, now.UnixNano()).Scan(&claims); err != nil {
		return bus.ActorArchiveResult{}, err
	}
	if err := tx.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM deliveries d JOIN messages m ON m.message_id = d.message_id
		LEFT JOIN actor_adoptions a ON a.source_actor = d.recipient_actor
		WHERE COALESCE(a.adopting_actor, d.recipient_actor) = ? AND (d.state = ? OR (d.state = ? AND d.lease_expires_at_ns <= ?))
		  AND (m.expires_at_ns IS NULL OR m.expires_at_ns > ?)`, actor, bus.DeliveryQueued,
		bus.DeliveryClaimed, now.UnixNano(), now.UnixNano()).Scan(&unread); err != nil {
		return bus.ActorArchiveResult{}, err
	}
	if aliases != 0 || live != 0 || claims != 0 {
		return bus.ActorArchiveResult{}, &bus.ValidationError{Field: "actor", Problem: "archive eligibility changed; run preflight again"}
	}
	if unread != 0 && !allowUnread {
		return bus.ActorArchiveResult{}, &bus.ValidationError{Field: "allow_unread", Problem: "archive eligibility changed; review unread messages again"}
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM continuity_bindings WHERE actor = ?`, actor); err != nil {
		return bus.ActorArchiveResult{}, err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM harness_instance_bindings WHERE actor = ?`, actor); err != nil {
		return bus.ActorArchiveResult{}, err
	}
	withUnread := unread > 0
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO actor_lifecycle(actor, state, archived_with_unread, changed_by_actor, changed_at_ns)
		VALUES (?, 'archived', ?, ?, ?)
		ON CONFLICT(actor) DO UPDATE SET state = 'archived', archived_with_unread = excluded.archived_with_unread,
			changed_by_actor = excluded.changed_by_actor, changed_at_ns = excluded.changed_at_ns`,
		actor, boolInt(withUnread), changedBy, now.UnixNano()); err != nil {
		return bus.ActorArchiveResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return bus.ActorArchiveResult{}, err
	}
	if withUnread {
		condition, conditionErr := s.ObserveCondition(ctx, bus.ConditionObservation{
			Kind: "stale_unread", Subject: actor, ReasonCode: "archived_with_unread",
			Summary: "Archived actor retains unread messages by explicit operator choice",
		})
		if conditionErr == nil {
			_, _ = s.AcknowledgeCondition(ctx, condition.Kind, condition.Subject, condition.Generation)
		}
	}
	return bus.ActorArchiveResult{Actor: actor, Archived: true, WithUnread: withUnread, ChangedAt: now}, nil
}

func (s *Store) RestoreActor(ctx context.Context, actor, changedBy string) (bus.ActorArchiveResult, error) {
	now := s.now().UTC()
	result, err := s.db.ExecContext(ctx, `
		UPDATE actor_lifecycle SET state = 'active', archived_with_unread = 0, changed_by_actor = ?, changed_at_ns = ?
		WHERE actor = ? AND state = 'archived'`, changedBy, now.UnixNano(), actor)
	if err != nil {
		return bus.ActorArchiveResult{}, err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return bus.ActorArchiveResult{}, err
	}
	if changed == 0 {
		return bus.ActorArchiveResult{}, bus.ErrNotFound
	}
	return bus.ActorArchiveResult{Actor: actor, Archived: false, ChangedAt: now}, nil
}

func (s *Store) RevokeDeliveryLease(ctx context.Context, actor, messageID string, crashGrace time.Duration) error {
	if crashGrace <= 0 {
		return &bus.ValidationError{Field: "crash_grace", Problem: "must be positive"}
	}
	now := s.now().UTC()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	live, err := actorHasLivePresenceTx(ctx, tx, actor, now)
	if err != nil {
		return err
	}
	if live {
		return bus.ErrActorLive
	}
	var leaseToken string
	var claimedNS int64
	err = tx.QueryRowContext(ctx, `
		SELECT lease_token, claimed_at_ns FROM deliveries
		WHERE message_id = ? AND recipient_actor = ? AND state = ? AND lease_expires_at_ns > ?`,
		messageID, actor, bus.DeliveryClaimed, now.UnixNano()).Scan(&leaseToken, &claimedNS)
	if errors.Is(err, sql.ErrNoRows) {
		return bus.ErrNotFound
	}
	if err != nil {
		return err
	}
	if time.Unix(0, claimedNS).Add(crashGrace).After(now) {
		return &bus.ValidationError{Field: "crash_grace", Problem: "has not elapsed since the claim"}
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE deliveries SET state = ?, terminal_lease_token = lease_token, lease_token = NULL,
			lease_expires_at_ns = NULL, claimed_at_ns = NULL, last_error = 'operator revoked lease after crash grace'
		WHERE message_id = ? AND recipient_actor = ?`, bus.DeliveryQueued, messageID, actor); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO notification_outbox(message_id, recipient_actor, state, available_at_ns, created_at_ns)
		SELECT ?, ?, 'pending', ?, ? WHERE EXISTS (
			SELECT 1 FROM messages WHERE message_id = ? AND delivery_request <> ?
		)
		ON CONFLICT(message_id, recipient_actor) DO UPDATE SET state = 'pending', available_at_ns = excluded.available_at_ns, last_error = NULL`,
		messageID, actor, now.UnixNano(), now.UnixNano(), messageID, bus.DeliveryNonBlocking); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	_ = leaseToken
	return nil
}

func (s *Store) actorArchivedTx(ctx context.Context, tx *sql.Tx, actor string) (bool, error) {
	var archived int
	err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM actor_lifecycle WHERE actor = ? AND state = 'archived')`, actor).Scan(&archived)
	return archived != 0, err
}

func (s *Store) ArchiveEligibleActors(ctx context.Context, inactiveFor time.Duration) ([]string, error) {
	if inactiveFor <= 0 {
		return nil, &bus.ValidationError{Field: "archive_after", Problem: "must be positive"}
	}
	now := s.now().UTC()
	rows, err := s.db.QueryContext(ctx, `
		SELECT n.actor FROM actor_names n
		LEFT JOIN actor_lifecycle l ON l.actor = n.actor
		WHERE n.actor <> 'operator' AND COALESCE(l.state, 'active') <> 'archived'
		  AND n.first_seen_at_ns <= ?
		  AND NOT EXISTS (SELECT 1 FROM events e WHERE e.actor_id = n.actor AND e.created_at_ns > ?)
		  AND NOT EXISTS (SELECT 1 FROM messages m WHERE m.from_actor = n.actor AND m.created_at_ns > ?)
		  AND NOT EXISTS (SELECT 1 FROM registrations r WHERE r.actor = n.actor AND r.updated_at_ns > ?)
		  AND NOT EXISTS (SELECT 1 FROM actor_profiles p WHERE p.actor = n.actor AND p.updated_at_ns > ?)
		  AND NOT EXISTS (SELECT 1 FROM actor_aliases a WHERE a.actor = n.actor)
		  AND NOT EXISTS (SELECT 1 FROM continuity_bindings c WHERE c.actor = n.actor)
		  AND NOT EXISTS (SELECT 1 FROM actor_adoptions a WHERE a.adopting_actor = n.actor)
		  AND NOT EXISTS (SELECT 1 FROM registrations r WHERE r.actor = n.actor AND r.ended_at_ns IS NULL AND r.attention_superseded_at_ns IS NULL AND r.lease_expires_at_ns > ?)
		  AND NOT EXISTS (
			SELECT 1 FROM deliveries d JOIN messages m ON m.message_id = d.message_id
			WHERE d.recipient_actor = n.actor AND (d.state = ? OR (d.state = ? AND d.lease_expires_at_ns <= ?))
			  AND (m.expires_at_ns IS NULL OR m.expires_at_ns > ?)
		) ORDER BY n.actor`, now.Add(-inactiveFor).UnixNano(), now.Add(-inactiveFor).UnixNano(),
		now.Add(-inactiveFor).UnixNano(), now.Add(-inactiveFor).UnixNano(), now.Add(-inactiveFor).UnixNano(), now.UnixNano(),
		bus.DeliveryQueued, bus.DeliveryClaimed, now.UnixNano(), now.UnixNano())
	if err != nil {
		return nil, err
	}
	actors := make([]string, 0)
	for rows.Next() {
		var actor string
		if err := rows.Scan(&actor); err != nil {
			_ = rows.Close()
			return nil, err
		}
		actors = append(actors, actor)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	sort.Strings(actors)
	for _, actor := range actors {
		if _, err := s.ArchiveActor(ctx, actor, "operator:auto-archive", false); err != nil {
			return nil, fmt.Errorf("auto-archive %s: %w", actor, err)
		}
	}
	return actors, nil
}
