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

func (s *Store) ClaimNotification(ctx context.Context) (bus.NotificationJob, error) {
	now := s.now().UTC()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return bus.NotificationJob{}, err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `
		UPDATE notification_outbox
		SET state = 'done', available_at_ns = ?, last_error = 'message expired before notification'
		WHERE state IN ('pending', 'processing', 'accepted')
		  AND EXISTS (
			SELECT 1 FROM messages
			WHERE messages.message_id = notification_outbox.message_id
			  AND expires_at_ns IS NOT NULL AND expires_at_ns <= ?
		  )`, now.UnixNano(), now.UnixNano()); err != nil {
		return bus.NotificationJob{}, err
	}
	var messageID, recipient string
	if err := tx.QueryRowContext(ctx, `
		SELECT o.message_id, o.recipient_actor
		FROM notification_outbox o JOIN messages m ON m.message_id = o.message_id
		WHERE (o.state = 'pending' OR o.state = 'processing') AND o.available_at_ns <= ?
		  AND (m.expires_at_ns IS NULL OR m.expires_at_ns > ?)
		ORDER BY o.created_at_ns, o.message_id, o.recipient_actor LIMIT 1`,
		now.UnixNano(), now.UnixNano()).Scan(&messageID, &recipient); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return bus.NotificationJob{}, bus.ErrNoMessage
		}
		return bus.NotificationJob{}, err
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE notification_outbox
		SET state = 'processing', attempt = attempt + 1, available_at_ns = ?
		WHERE message_id = ? AND recipient_actor = ?
		  AND (state = 'pending' OR state = 'processing') AND available_at_ns <= ?`,
		now.Add(30*time.Second).UnixNano(), messageID, recipient, now.UnixNano())
	if err != nil {
		return bus.NotificationJob{}, err
	}
	changed, _ := result.RowsAffected()
	if changed != 1 {
		return bus.NotificationJob{}, bus.ErrNoMessage
	}
	message, err := getMessageByIDTx(ctx, tx, messageID)
	if err != nil {
		return bus.NotificationJob{}, err
	}
	var attempt int
	if err := tx.QueryRowContext(ctx, `SELECT attempt FROM notification_outbox WHERE message_id = ? AND recipient_actor = ?`,
		messageID, recipient).Scan(&attempt); err != nil {
		return bus.NotificationJob{}, err
	}
	if err := tx.Commit(); err != nil {
		return bus.NotificationJob{}, err
	}
	return bus.NotificationJob{Message: message, RecipientActor: recipient, Attempt: attempt}, nil
}

func (s *Store) FinishNotification(ctx context.Context, job bus.NotificationJob, disposition bus.NotificationDisposition, detail string) error {
	state := "done"
	now := s.now().UTC()
	available := now
	switch disposition {
	case bus.NotificationAccepted:
		// Adapter acceptance is durable operational evidence, but not message
		// delivery proof. Keep the row visible as accepted until Claim closes it,
		// without injecting duplicate wake references into the harness.
		state = "accepted"
	case bus.NotificationRetry:
		if job.Attempt >= 5 {
			break
		}
		state = "pending"
		backoff := time.Duration(job.Attempt*job.Attempt) * time.Second
		available = available.Add(backoff)
	case bus.NotificationComplete:
	default:
		return fmt.Errorf("unsupported notification disposition %q", disposition)
	}
	detail = strings.TrimSpace(detail)
	if len(detail) > 4096 {
		detail = detail[:4096]
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `
		UPDATE notification_outbox
		SET state = ?, available_at_ns = ?, last_error = NULLIF(?, '')
		WHERE message_id = ? AND recipient_actor = ? AND state = 'processing'`,
		state, available.UnixNano(), detail, job.Message.ID, job.RecipientActor)
	if err != nil {
		return err
	}
	changed, _ := result.RowsAffected()
	if changed != 1 {
		// Claim may have completed the job while the adapter call was in
		// flight. Treat that race as successful completion.
		var existing string
		if scanErr := tx.QueryRowContext(ctx, `SELECT state FROM notification_outbox WHERE message_id = ? AND recipient_actor = ?`,
			job.Message.ID, job.RecipientActor).Scan(&existing); scanErr == nil && existing == "done" {
			return tx.Commit()
		}
		return fmt.Errorf("notification job is not processing")
	}
	if disposition == bus.NotificationRetry && job.Attempt >= 5 {
		payload := bus.EventProvenance(ctx, "")
		payload["attempts"] = job.Attempt
		payload["detail"] = detail
		if err := s.appendEventTx(ctx, tx, job.Message.ProjectID, "operational", "delivery.notification_abandoned",
			job.Message.ID, job.RecipientActor, payload, now); err != nil {
			return err
		}
	}
	return tx.Commit()
}
