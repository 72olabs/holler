package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"github.com/72olabs/holler/internal/bus"
	"time"
)

func (s *Store) EnableChannelAttention(ctx context.Context, p bus.ConversationPrincipal, monitor bool) error {
	if err := conversationPrincipal(p); err != nil {
		return err
	}
	if p.Gateway {
		return bus.ErrChannelDenied
	}
	native, hook := 1, 0
	if monitor {
		native, hook = 0, 1
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO channel_attention_clients(actor,run_id,native_ready,monitor_ready) VALUES(?,?,?,?) ON CONFLICT(actor,run_id) DO UPDATE SET native_ready=MAX(native_ready,excluded.native_ready),monitor_ready=MAX(monitor_ready,excluded.monitor_ready)`, p.Actor, p.Run, native, hook)
	return err
}

func (s *Store) ManagedNotificationAllowed(ctx context.Context, r bus.Registration, id string) (bool, error) {
	if r.Harness == "claude" && r.AttentionMode != "hook-long-poll" {
		return false, nil
	}
	var ready int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM channel_attention_clients c JOIN managed_deliveries d ON d.recipient_actor=c.actor JOIN messages m ON m.message_id=d.message_id WHERE c.actor=? AND c.run_id=? AND d.message_id=? AND d.state='queued' AND (CASE WHEN ?='claude' THEN c.monitor_ready ELSE c.native_ready END)=1 AND EXISTS(SELECT 1 FROM channel_grants g WHERE g.channel_id=m.conversation_id AND g.actor=c.actor AND g.can_post=1 AND g.revoked_seq IS NULL AND g.history_from<=m.channel_seq)`, r.Actor, r.RunID, id, r.Harness).Scan(&ready)
	return ready == 1, err
}

func (s *Store) ResetChannelAttention(ctx context.Context, p bus.ConversationPrincipal, monitor bool) error {
	if err := conversationPrincipal(p); err != nil {
		return err
	}
	column := "native_ready"
	if monitor {
		column = "monitor_ready"
	}
	_, err := s.db.ExecContext(ctx, `UPDATE channel_attention_clients SET `+column+`=0 WHERE actor=? AND run_id=?`, p.Actor, p.Run)
	return err
}

// Rearm only accepted-but-unclaimed jobs, with a ready live recipient, at most
// five total attempts. ACKs, nacks and revoked grants cannot trigger re-wakes.
func (s *Store) RearmStaleManagedNotifications(ctx context.Context, staleAfter time.Duration) error {
	if staleAfter <= 0 {
		staleAfter = 15 * time.Minute
	}
	now := s.now()
	_, err := s.db.ExecContext(ctx, `UPDATE notification_outbox AS o SET state='managed-pending',available_at_ns=? WHERE o.source='managed' AND o.state='managed-accepted' AND o.attempt<5 AND o.available_at_ns<=? AND EXISTS(SELECT 1 FROM managed_deliveries d JOIN messages m ON m.message_id=d.message_id JOIN channel_grants g ON g.channel_id=m.conversation_id AND g.actor=d.recipient_actor WHERE d.message_id=o.message_id AND d.recipient_actor=o.recipient_actor AND d.state='queued' AND g.can_post=1 AND g.revoked_seq IS NULL AND g.history_from<=m.channel_seq AND (m.expires_at_ns IS NULL OR m.expires_at_ns>?)) AND EXISTS(SELECT 1 FROM registrations r JOIN channel_attention_clients c ON c.actor=r.actor AND c.run_id=r.run_id WHERE r.actor=o.recipient_actor AND r.lease_expires_at_ns>? AND r.ended_at_ns IS NULL AND r.attention_superseded_at_ns IS NULL AND ((r.harness IN ('codex','opencode') AND c.native_ready=1 AND r.attention_mode!='startup-only') OR (r.harness='claude' AND r.attention_mode='hook-long-poll' AND c.monitor_ready=1)))`, now.UnixNano(), now.Add(-staleAfter).UnixNano(), now.UnixNano(), now.UnixNano())
	return err
}

func (s *Store) ClaimManagedNotification(ctx context.Context) (bus.NotificationJob, error) {
	var job bus.NotificationJob
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return job, err
	}
	defer tx.Rollback()
	now := s.now()
	// These jobs never pass through legacy conditions, body readers or events.
	err = tx.QueryRowContext(ctx, `SELECT o.message_id,o.recipient_actor,o.attempt,m.project_id FROM notification_outbox o JOIN messages m ON m.message_id=o.message_id JOIN managed_deliveries d ON d.message_id=o.message_id AND d.recipient_actor=o.recipient_actor WHERE o.source='managed' AND o.state IN ('managed-pending','managed-processing') AND o.available_at_ns<=? AND d.state='queued' AND (m.expires_at_ns IS NULL OR m.expires_at_ns>?) AND EXISTS(SELECT 1 FROM channel_grants g WHERE g.channel_id=m.conversation_id AND g.actor=o.recipient_actor AND g.can_post=1 AND g.revoked_seq IS NULL AND g.history_from<=m.channel_seq) ORDER BY o.created_at_ns,o.message_id,o.recipient_actor LIMIT 1`, now.UnixNano(), now.UnixNano()).Scan(&job.Message.ID, &job.RecipientActor, &job.Attempt, &job.Message.ProjectID)
	if errors.Is(err, sql.ErrNoRows) {
		return job, bus.ErrNoMessage
	}
	if err != nil {
		return job, err
	}
	job.Attempt++
	job.Message.SchemaVersion = 2
	job.Message.Type = "CHANNEL_MESSAGE"
	job.Message.DeliveryRequest = bus.DeliveryWake
	if _, err := tx.ExecContext(ctx, `UPDATE notification_outbox SET state='managed-processing',attempt=?,available_at_ns=? WHERE message_id=? AND recipient_actor=? AND source='managed'`, job.Attempt, now.Add(30*time.Second).UnixNano(), job.Message.ID, job.RecipientActor); err != nil {
		return job, err
	}
	return job, tx.Commit()
}

func (s *Store) FinishManagedNotification(ctx context.Context, job bus.NotificationJob, disposition bus.NotificationDisposition, detail string) error {
	state := "done"
	now := s.now()
	available := now
	switch disposition {
	case bus.NotificationAccepted:
		state = "managed-accepted"
	case bus.NotificationRetry:
		if job.Attempt < 5 {
			state = "managed-pending"
			available = now.Add(time.Duration(job.Attempt*job.Attempt) * time.Second)
		}
	case bus.NotificationComplete:
	default:
		return bus.ErrInvalid
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var channel string
	if err := tx.QueryRowContext(ctx, `SELECT conversation_id FROM messages WHERE message_id=? AND conversation_id IS NOT NULL`, job.Message.ID).Scan(&channel); err != nil {
		return bus.ErrChannelDenied
	}
	// Error details may contain adapter output: retain a bounded private summary,
	// not raw output in a channel-wide event or any global condition.
	if len(detail) > 4096 {
		detail = detail[:4096]
	}
	if _, err := tx.ExecContext(ctx, `UPDATE notification_outbox SET state=?,available_at_ns=?,last_error=? WHERE message_id=? AND recipient_actor=? AND source='managed' AND state='managed-processing' AND attempt=?`, state, available.UnixNano(), detail, job.Message.ID, job.RecipientActor, job.Attempt); err != nil {
		return err
	}
	if _, err := s.channelEventTx(ctx, tx, channel, "attention.attempted", job.RecipientActor, job.Message.ID, map[string]interface{}{"state": state, "attempt": job.Attempt}); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) RecordManagedNotification(ctx context.Context, id string, attempt bus.NotificationAttempt) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var channel string
	if err := tx.QueryRowContext(ctx, `SELECT conversation_id FROM messages WHERE message_id=? AND conversation_id IS NOT NULL`, id).Scan(&channel); err != nil {
		return bus.ErrChannelDenied
	}
	if _, err := s.channelEventTx(ctx, tx, channel, "attention.adapter", attempt.Actor, id, map[string]interface{}{"result": attempt.Result}); err != nil {
		return err
	}
	return tx.Commit()
}
