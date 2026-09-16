package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"github.com/72olabs/holler/internal/bus"
	"time"
)

type ChannelDelivery struct {
	PolicyRevision int64              `json:"policy_revision"`
	Message        bus.ChannelMessage `json:"message"`
	State          bus.DeliveryState  `json:"state"`
	Attempt        int                `json:"attempt"`
	LeaseToken     string             `json:"lease_token,omitempty"`
	LeaseExpiresAt *time.Time         `json:"lease_expires_at,omitempty"`
}

// ChannelInbox never claims, marks read, or includes observer-only subscriptions.
func (s *Store) ChannelInbox(ctx context.Context, p bus.ConversationPrincipal, limit int) ([]ChannelDelivery, error) {
	if err := conversationPrincipal(p); err != nil {
		return nil, err
	}
	if limit <= 0 || limit > 100 {
		limit = 100
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if err := assertActorNotAdoptedTx(ctx, tx, p.Actor); err != nil {
		return nil, err
	}
	rows, err := tx.QueryContext(ctx, `SELECT d.message_id,d.state,d.attempt,d.lease_expires_at_ns FROM managed_deliveries d JOIN messages m ON m.message_id=d.message_id WHERE d.recipient_actor=? AND d.state IN ('queued','claimed') AND (m.expires_at_ns IS NULL OR m.expires_at_ns>?) AND EXISTS(SELECT 1 FROM channel_grants g WHERE g.channel_id=m.conversation_id AND g.actor=? AND g.can_post=1 AND g.revoked_seq IS NULL AND g.history_from<=m.channel_seq) ORDER BY m.created_at_ns,m.message_id LIMIT ?`, p.Actor, s.now().UnixNano(), p.Actor, limit)
	if err != nil {
		return nil, err
	}
	type row struct {
		id       string
		delivery ChannelDelivery
	}
	pending := []row{}
	for rows.Next() {
		var r row
		var expires sql.NullInt64
		if err := rows.Scan(&r.id, &r.delivery.State, &r.delivery.Attempt, &expires); err != nil {
			rows.Close()
			return nil, err
		}
		r.delivery.LeaseExpiresAt = timeFromNull(expires)
		pending = append(pending, r)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	out := []ChannelDelivery{}
	size := 0
	for _, r := range pending {
		r.delivery.Message, err = s.channelMessageTx(ctx, tx, p, r.id)
		if err != nil {
			return nil, err
		}
		size += len(r.delivery.Message.Body)
		if size > bus.MaxBodyBytes*3/2 && len(out) > 0 {
			break
		}
		out = append(out, r.delivery)
	}
	return out, nil
}

func (s *Store) channelDeliveryTx(ctx context.Context, tx *sql.Tx, p bus.ConversationPrincipal, id string) (bus.ChannelMessage, error) {
	if err := conversationPrincipal(p); err != nil {
		return bus.ChannelMessage{}, err
	}
	if err := assertActorNotAdoptedTx(ctx, tx, p.Actor); err != nil {
		return bus.ChannelMessage{}, err
	}
	m, err := s.channelMessageTx(ctx, tx, p, id)
	if err != nil {
		return m, err
	}
	var n int
	err = tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM managed_deliveries d WHERE d.message_id=? AND d.recipient_actor=? AND EXISTS(SELECT 1 FROM channel_grants WHERE channel_id=? AND actor=? AND can_post=1 AND revoked_seq IS NULL AND history_from<=?)`, id, p.Actor, m.ChannelID, p.Actor, m.Seq).Scan(&n)
	if err != nil {
		return m, err
	}
	if n != 1 {
		return m, bus.ErrChannelDenied
	}
	return m, nil
}

func (s *Store) ChannelClaim(ctx context.Context, p bus.ConversationPrincipal, id string, lease time.Duration) (ChannelDelivery, error) {
	var out ChannelDelivery
	if lease <= 0 || lease > 24*time.Hour {
		return out, &bus.ValidationError{Field: "lease", Problem: "must be between 0 and 24h"}
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return out, err
	}
	defer tx.Rollback()
	out.Message, err = s.channelDeliveryTx(ctx, tx, p, id)
	if err != nil {
		return out, err
	}
	c, err := channelTx(ctx, tx, p, out.Message.ChannelID)
	if err != nil {
		return out, err
	}
	out.PolicyRevision = c.Revision
	token, err := s.newID("lease")
	if err != nil {
		return out, err
	}
	now := s.now()
	expires := now.Add(lease)
	res, err := tx.ExecContext(ctx, `UPDATE managed_deliveries SET state='claimed',attempt=attempt+1,lease_token=?,lease_expires_at_ns=?,claimed_at_ns=?,acked_at_ns=NULL WHERE message_id=? AND recipient_actor=? AND (state='queued' OR (state='claimed' AND lease_expires_at_ns<=?))`, token, expires.UnixNano(), now.UnixNano(), id, p.Actor, now.UnixNano())
	if err != nil {
		return out, err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return out, bus.ErrNoMessage
	}
	if err := tx.QueryRowContext(ctx, `SELECT attempt FROM managed_deliveries WHERE message_id=? AND recipient_actor=?`, id, p.Actor).Scan(&out.Attempt); err != nil {
		return out, err
	}
	if _, err := s.channelEventTx(ctx, tx, out.Message.ChannelID, "delivery.claimed", p.Actor, id, nil); err != nil {
		return out, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE notification_outbox SET state='done',available_at_ns=? WHERE message_id=? AND recipient_actor=? AND source='managed'`, now.UnixNano(), id, p.Actor); err != nil {
		return out, err
	}
	if err := tx.Commit(); err != nil {
		return out, err
	}
	out.State = bus.DeliveryClaimed
	out.LeaseToken = token
	out.LeaseExpiresAt = &expires
	return out, nil
}

func (s *Store) ChannelDeliveryUpdate(ctx context.Context, p bus.ConversationPrincipal, id, token, action, reason string, lease time.Duration) (ChannelDelivery, error) {
	var out ChannelDelivery
	if token == "" || len(reason) > 4096 {
		return out, &bus.ValidationError{Field: "delivery", Problem: "lease token required; reason limited to 4096 bytes"}
	}
	if action != "ack" && action != "nack" && action != "dead_letter" && action != "extend" {
		return out, &bus.ValidationError{Field: "action", Problem: "expected ack, nack, dead_letter, or extend"}
	}
	if action == "extend" && (lease <= 0 || lease > 24*time.Hour) {
		return out, &bus.ValidationError{Field: "lease", Problem: "must be between 0 and 24h"}
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return out, err
	}
	defer tx.Rollback()
	out.Message, err = s.channelDeliveryTx(ctx, tx, p, id)
	if err != nil {
		return out, err
	}
	var stored, terminal sql.NullString
	var expires sql.NullInt64
	err = tx.QueryRowContext(ctx, `SELECT state,attempt,lease_token,terminal_lease_token,lease_expires_at_ns FROM managed_deliveries WHERE message_id=? AND recipient_actor=?`, id, p.Actor).Scan(&out.State, &out.Attempt, &stored, &terminal, &expires)
	if errors.Is(err, sql.ErrNoRows) {
		return out, bus.ErrChannelDenied
	}
	if err != nil {
		return out, err
	}
	now := s.now()
	kind := "delivery." + action
	if action == "extend" {
		if out.State != bus.DeliveryClaimed || !stored.Valid || stored.String != token {
			return out, bus.ErrLeaseTokenMismatch
		}
		if !expires.Valid || expires.Int64 <= now.UnixNano() {
			return out, bus.ErrLeaseExpired
		}
		until := now.Add(lease)
		out.LeaseExpiresAt = &until
		out.LeaseToken = token
		_, err = tx.ExecContext(ctx, `UPDATE managed_deliveries SET lease_expires_at_ns=? WHERE message_id=? AND recipient_actor=?`, until.UnixNano(), id, p.Actor)
	} else {
		done, e := finishDeliveryState(out.State, stored, terminal, token, action == "ack", action == "dead_letter")
		if done || e != nil {
			return out, e
		}
		next := bus.DeliveryQueued
		var terminalToken, acked interface{}
		if action == "ack" {
			next = bus.DeliveryAcked
			terminalToken = token
			acked = now.UnixNano()
		}
		if action == "dead_letter" {
			next = bus.DeliveryDeadLettered
			terminalToken = token
		}
		out.State = next
		_, err = tx.ExecContext(ctx, `UPDATE managed_deliveries SET state=?,lease_token=NULL,lease_expires_at_ns=NULL,terminal_lease_token=?,acked_at_ns=?,last_error=NULLIF(?,'') WHERE message_id=? AND recipient_actor=?`, next, terminalToken, acked, reason, id, p.Actor)
	}
	if err != nil {
		return out, err
	}
	if _, err := s.channelEventTx(ctx, tx, out.Message.ChannelID, kind, p.Actor, id, nil); err != nil {
		return out, err
	}
	if err := tx.Commit(); err != nil {
		return out, err
	}
	return out, nil
}

func (s *Store) ChannelResponseResolve(ctx context.Context, p bus.ConversationPrincipal, id, action, key string, revision int64) (bus.ChannelResponse, error) {
	var out bus.ChannelResponse
	if err := conversationPrincipal(p); err != nil {
		return out, err
	}
	if err := channelText("idempotency_key", key, 256); err != nil {
		return out, err
	}
	if action != "decline" && action != "withdraw" {
		return out, &bus.ValidationError{Field: "action", Problem: "expected decline or withdraw; answer using channel.post"}
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return out, err
	}
	defer tx.Rollback()
	err = tx.QueryRowContext(ctx, `SELECT request_id,channel_id,message_id,requester,respondent,state,revision,COALESCE(answer_id,'') FROM channel_responses WHERE request_id=?`, id).Scan(&out.ID, &out.ChannelID, &out.MessageID, &out.Requester, &out.Respondent, &out.State, &out.Revision, &out.AnswerID)
	if errors.Is(err, sql.ErrNoRows) {
		return out, bus.ErrChannelDenied
	}
	if err != nil {
		return out, err
	}
	if _, err := s.channelMessageTx(ctx, tx, p, out.MessageID); err != nil {
		return out, err
	}
	if (action == "decline" && out.Respondent != p.Actor) || (action == "withdraw" && out.Requester != p.Actor) {
		return out, bus.ErrResponseConflict
	}
	digest, _ := digestRequest([]interface{}{id, action, revision})
	var previous bus.ChannelResponse
	found, err := channelOperation(ctx, tx, p, "response", key, digest, &previous)
	if err != nil {
		return out, err
	}
	if found {
		return previous, nil
	}
	if out.State != "open" || out.Revision != revision {
		return out, bus.ErrResponseConflict
	}
	out.State = "declined"
	if action == "withdraw" {
		out.State = "withdrawn"
	}
	out.Revision++
	if _, err := tx.ExecContext(ctx, `UPDATE channel_responses SET state=?,revision=? WHERE request_id=?`, out.State, out.Revision, id); err != nil {
		return out, err
	}
	if _, err := s.channelEventTx(ctx, tx, out.ChannelID, "response."+out.State, p.Actor, out.MessageID, map[string]interface{}{"request_id": id}); err != nil {
		return out, err
	}
	if err := saveChannelOperation(ctx, tx, p, "response", key, digest, out); err != nil {
		return out, err
	}
	if err := tx.Commit(); err != nil {
		return out, err
	}
	return out, nil
}
