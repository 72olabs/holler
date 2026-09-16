package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"

	"github.com/72olabs/holler/internal/bus"
)

type SupervisionPreview struct {
	Agent    string           `json:"agent"`
	Human    string           `json:"human"`
	Revision int64            `json:"revision"`
	Channels map[string]int64 `json:"channel_revisions"`
}

func channelAdmin(p bus.ConversationPrincipal) error {
	if err := conversationPrincipal(p); err != nil {
		return err
	}
	if !p.Admin || (!p.Gateway && p.Actor != "operator") {
		return bus.ErrChannelDenied
	}
	return nil
}

// RegisterHuman is called only by authenticated administrative enrollment. It
// refuses to convert a preexisting legacy actor into a privileged human identity.
func (s *Store) RegisterHuman(ctx context.Context, p bus.ConversationPrincipal, actor string) error {
	if err := channelAdmin(p); err != nil {
		return err
	}
	if !strings.HasPrefix(actor, "human:") || strings.TrimSpace(actor) != actor {
		return bus.ErrChannelDenied
	}
	if err := channelText("human", actor, 128); err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var human, known int
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM human_actors WHERE actor=?),EXISTS(SELECT 1 FROM actor_names WHERE actor=?)`, actor, actor).Scan(&human, &known); err != nil {
		return err
	}
	if human == 1 {
		return nil
	}
	if known == 1 {
		return bus.ErrChannelDenied
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO actor_names(actor,first_seen_at_ns) VALUES(?,?)`, actor, s.now().UnixNano()); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO human_actors(actor,created_at_ns) VALUES(?,?)`, actor, s.now().UnixNano()); err != nil {
		return err
	}
	return tx.Commit()
}

func supervisionPreviewTx(ctx context.Context, tx *sql.Tx, agent string) (SupervisionPreview, error) {
	v := SupervisionPreview{Agent: agent, Channels: map[string]int64{}}
	var active int
	err := tx.QueryRowContext(ctx, `SELECT human,revision,active FROM supervision_links WHERE agent=?`, agent).Scan(&v.Human, &v.Revision, &active)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return v, err
	}
	if active == 0 {
		v.Human = ""
	}
	rows, err := tx.QueryContext(ctx, `SELECT c.channel_id,c.policy_revision FROM channels c JOIN channel_grants g ON g.channel_id=c.channel_id WHERE g.actor=? AND g.source='participant' AND g.revoked_seq IS NULL`, agent)
	if err != nil {
		return v, err
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		var revision int64
		if err := rows.Scan(&id, &revision); err != nil {
			return v, err
		}
		v.Channels[id] = revision
	}
	return v, rows.Err()
}

func (s *Store) SupervisionPreflight(ctx context.Context, p bus.ConversationPrincipal, agent string) (SupervisionPreview, error) {
	if err := channelAdmin(p); err != nil {
		return SupervisionPreview{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return SupervisionPreview{}, err
	}
	defer tx.Rollback()
	return supervisionPreviewTx(ctx, tx, agent)
}

func (s *Store) SupervisionChange(ctx context.Context, p bus.ConversationPrincipal, preview SupervisionPreview, human, key string) (SupervisionPreview, error) {
	if err := channelAdmin(p); err != nil {
		return SupervisionPreview{}, err
	}
	if err := channelText("idempotency_key", key, 256); err != nil {
		return SupervisionPreview{}, err
	}
	if bus.IsHumanActor(preview.Agent) {
		return SupervisionPreview{}, bus.ErrChannelDenied
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return SupervisionPreview{}, err
	}
	defer tx.Rollback()
	digest, _ := digestRequest([]interface{}{preview, human})
	var saved SupervisionPreview
	if found, err := channelOperation(ctx, tx, p, "supervise", key, digest, &saved); err != nil {
		return saved, err
	} else if found {
		return saved, nil
	}
	current, err := supervisionPreviewTx(ctx, tx, preview.Agent)
	if err != nil {
		return saved, err
	}
	a, _ := digestRequest(current)
	b, _ := digestRequest(preview)
	if a != b {
		return saved, bus.ErrAudienceChanged
	}
	var known int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM actor_names WHERE actor=?`, preview.Agent).Scan(&known); err != nil {
		return saved, err
	}
	if known != 1 {
		return saved, bus.ErrChannelDenied
	}
	if human != "" {
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM human_actors WHERE actor=?`, human).Scan(&known); err != nil {
			return saved, err
		}
		if known != 1 {
			return saved, bus.ErrChannelDenied
		}
	}
	active := 0
	if human != "" {
		active = 1
	}
	storedHuman := human
	if storedHuman == "" {
		storedHuman = current.Human
	}
	if storedHuman == "" {
		return saved, bus.ErrChannelDenied
	}
	scope := "admin"
	if p.Gateway {
		scope = "observe+admin"
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO supervision_links(agent,human,revision,active,established_by,established_scope,updated_at_ns) VALUES(?,?,?,?,?,?,?) ON CONFLICT(agent) DO UPDATE SET human=excluded.human,revision=excluded.revision,active=excluded.active,established_by=excluded.established_by,established_scope=excluded.established_scope,updated_at_ns=excluded.updated_at_ns`, preview.Agent, storedHuman, preview.Revision+1, active, p.Actor, scope, s.now().UnixNano())
	if err != nil {
		return saved, err
	}
	for id := range current.Channels {
		seq, err := s.channelEventTx(ctx, tx, id, "supervision.changed", p.Actor, "", map[string]interface{}{"agent": preview.Agent, "human": human, "scope": scope})
		if err != nil {
			return saved, err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE channel_grants SET revoked_seq=? WHERE channel_id=? AND source=? AND revoked_seq IS NULL`, seq, id, "supervisor:"+preview.Agent); err != nil {
			return saved, err
		}
		if human != "" {
			if _, err := tx.ExecContext(ctx, `INSERT INTO channel_grants(channel_id,actor,source,can_post,history_from,granted_seq) VALUES(?,?,?,0,?,?)`, id, human, "supervisor:"+preview.Agent, seq, seq); err != nil {
				return saved, err
			}
		}
		if _, err := tx.ExecContext(ctx, `UPDATE channels SET policy_revision=policy_revision+1 WHERE channel_id=?`, id); err != nil {
			return saved, err
		}
	}
	saved, err = supervisionPreviewTx(ctx, tx, preview.Agent)
	if err != nil {
		return saved, err
	}
	if err := saveChannelOperation(ctx, tx, p, "supervise", key, digest, saved); err != nil {
		return saved, err
	}
	if err := tx.Commit(); err != nil {
		return saved, err
	}
	return saved, nil
}

func (s *Store) ChannelMembershipChange(ctx context.Context, p bus.ConversationPrincipal, id, actor, action, key string, revision int64) (bus.Channel, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return bus.Channel{}, err
	}
	defer tx.Rollback()
	c, err := channelTx(ctx, tx, p, id)
	if err != nil {
		return c, err
	}
	if c.Kind == "dm" {
		return bus.Channel{}, bus.ErrImmutableAudience
	}
	if !c.CanManage {
		return bus.Channel{}, bus.ErrChannelDenied
	}
	if err := channelText("idempotency_key", key, 256); err != nil {
		return bus.Channel{}, err
	}
	digest, _ := digestRequest([]interface{}{id, actor, action, revision})
	var saved string
	if found, err := channelOperation(ctx, tx, p, "membership", key, digest, &saved); err != nil {
		return bus.Channel{}, err
	} else if found {
		return c, nil
	}
	if c.Revision != revision {
		return bus.Channel{}, bus.ErrAudienceChanged
	}
	if action != "add" && action != "remove" {
		return bus.Channel{}, &bus.ValidationError{Field: "action", Problem: "must be add or remove"}
	}
	if actor == c.Creator && action == "remove" {
		return bus.Channel{}, &bus.ValidationError{Field: "actor", Problem: "creator removal requires management transfer, which is not supported yet"}
	}
	var known int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM actor_names WHERE actor=?`, actor).Scan(&known); err != nil {
		return bus.Channel{}, err
	}
	if known != 1 {
		return bus.Channel{}, bus.ErrChannelDenied
	}
	if err := assertActorNotAdoptedTx(ctx, tx, actor); err != nil {
		return bus.Channel{}, err
	}
	if bus.IsHumanActor(actor) {
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM human_actors WHERE actor=?`, actor).Scan(&known); err != nil {
			return bus.Channel{}, err
		}
		if known != 1 {
			return bus.Channel{}, bus.ErrChannelDenied
		}
	}
	present := false
	for _, a := range c.Participants {
		present = present || actor == a
	}
	if (action == "add" && present) || (action == "remove" && !present) {
		return bus.Channel{}, bus.ErrAudienceChanged
	}
	if action == "add" && len(c.Participants) >= 32 {
		return bus.Channel{}, &bus.ValidationError{Field: "participants", Problem: "maximum 32"}
	}
	if action == "add" {
		if archived, err := s.actorArchivedTx(ctx, tx, actor); err != nil {
			return bus.Channel{}, err
		} else if archived {
			return bus.Channel{}, bus.ErrActorArchived
		}
	}
	seq, err := s.channelEventTx(ctx, tx, id, "membership."+action, p.Actor, "", map[string]string{"actor": actor})
	if err != nil {
		return bus.Channel{}, err
	}
	if action == "add" {
		if err := s.channelGrantTx(ctx, tx, id, actor, seq); err != nil {
			return bus.Channel{}, err
		}
	} else {
		if _, err := tx.ExecContext(ctx, `UPDATE channel_grants SET revoked_seq=? WHERE channel_id=? AND ((actor=? AND source='participant') OR source=?) AND revoked_seq IS NULL`, seq, id, actor, "supervisor:"+actor); err != nil {
			return bus.Channel{}, err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE managed_deliveries SET state='revoked',terminal_lease_token=lease_token,lease_token=NULL,lease_expires_at_ns=NULL,last_error='membership revoked' WHERE recipient_actor=? AND message_id IN (SELECT message_id FROM messages WHERE conversation_id=?) AND state IN ('queued','claimed')`, actor, id); err != nil {
			return bus.Channel{}, err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE notification_outbox SET state='done',last_error='membership revoked' WHERE source='managed' AND recipient_actor=? AND message_id IN (SELECT message_id FROM messages WHERE conversation_id=?)`, actor, id); err != nil {
			return bus.Channel{}, err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE channel_responses SET state='revoked',revision=revision+1 WHERE channel_id=? AND respondent=? AND state='open'`, id, actor); err != nil {
			return bus.Channel{}, err
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE channels SET policy_revision=policy_revision+1 WHERE channel_id=?`, id); err != nil {
		return bus.Channel{}, err
	}
	if err := saveChannelOperation(ctx, tx, p, "membership", key, digest, id); err != nil {
		return bus.Channel{}, err
	}
	c, err = channelTx(ctx, tx, p, id)
	if err != nil {
		return bus.Channel{}, err
	}
	if err := tx.Commit(); err != nil {
		return bus.Channel{}, err
	}
	return c, nil
}

func (s *Store) ChannelViewGet(ctx context.Context, p bus.ConversationPrincipal, id, thread string) (bus.ChannelViewState, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return bus.ChannelViewState{}, err
	}
	defer tx.Rollback()
	if _, err := channelTx(ctx, tx, p, id); err != nil {
		return bus.ChannelViewState{}, err
	}
	return channelViewTx(ctx, tx, p, id, thread)
}
func channelViewTx(ctx context.Context, tx *sql.Tx, p bus.ConversationPrincipal, id, thread string) (bus.ChannelViewState, error) {
	v := bus.ChannelViewState{ChannelID: id, ThreadID: thread}
	var raw []byte
	err := tx.QueryRowContext(ctx, `SELECT state FROM channel_views WHERE channel_id=? AND actor=? AND thread_id=?`, id, p.Actor, thread).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return v, nil
	}
	if err != nil {
		return v, err
	}
	err = json.Unmarshal(raw, &v)
	return v, err
}
func (s *Store) ChannelViewUpdate(ctx context.Context, p bus.ConversationPrincipal, v bus.ChannelViewState) (bus.ChannelViewState, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return v, err
	}
	defer tx.Rollback()
	c, err := channelTx(ctx, tx, p, v.ChannelID)
	if err != nil {
		return v, err
	}
	old, err := channelViewTx(ctx, tx, p, v.ChannelID, v.ThreadID)
	if err != nil {
		return v, err
	}
	if v.Revision != old.Revision {
		return v, bus.ErrAudienceChanged
	}
	if v.ReadThrough < old.ReadThrough || v.ReadThrough > c.LastSeq || v.ReadThrough < 0 {
		return v, &bus.ValidationError{Field: "read_through_seq", Problem: "must be monotonic and within history"}
	}
	for _, seq := range []*int64{&v.ReadThrough, v.UnreadFrom} {
		if seq == nil || *seq == 0 {
			continue
		}
		var n int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM messages WHERE conversation_id=? AND channel_seq=? AND channel_seq>=? AND (?='' OR thread_id=?)`, c.ID, *seq, c.HistoryFrom, v.ThreadID, v.ThreadID).Scan(&n); err != nil {
			return v, err
		}
		if n != 1 {
			return v, bus.ErrChannelDenied
		}
	}
	if v.SnoozedUntil != nil && !v.SnoozedUntil.After(s.now()) {
		return v, &bus.ValidationError{Field: "snoozed_until", Problem: "must be in the future or omitted"}
	}
	v.Revision++
	raw, err := json.Marshal(v)
	if err != nil {
		return v, err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO channel_views(channel_id,actor,thread_id,revision,state) VALUES(?,?,?,?,?) ON CONFLICT(channel_id,actor,thread_id) DO UPDATE SET revision=excluded.revision,state=excluded.state`, v.ChannelID, p.Actor, v.ThreadID, v.Revision, raw)
	if err != nil {
		return v, err
	}
	return v, tx.Commit()
}

func (s *Store) ChannelResponses(ctx context.Context, p bus.ConversationPrincipal, id string) ([]bus.ChannelResponse, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	c, err := channelTx(ctx, tx, p, id)
	if err != nil {
		return nil, err
	}
	rows, err := tx.QueryContext(ctx, `SELECT r.request_id,r.channel_id,r.message_id,r.requester,r.respondent,r.state,r.revision,COALESCE(r.answer_id,'') FROM channel_responses r JOIN messages m ON m.message_id=r.message_id WHERE r.channel_id=? AND m.channel_seq>=? AND (m.expires_at_ns IS NULL OR m.expires_at_ns>?) ORDER BY m.channel_seq DESC LIMIT 201`, id, c.HistoryFrom, s.now().UnixNano())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []bus.ChannelResponse{}
	for rows.Next() {
		var r bus.ChannelResponse
		if err := rows.Scan(&r.ID, &r.ChannelID, &r.MessageID, &r.Requester, &r.Respondent, &r.State, &r.Revision, &r.AnswerID); err != nil {
			return nil, err
		}
		result = append(result, r)
	}
	if len(result) > 200 {
		return nil, &bus.ValidationError{Field: "responses", Problem: "response list exceeds 200 requests"}
	}
	return result, rows.Err()
}
