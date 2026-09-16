package sqlite

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"time"

	"github.com/72olabs/holler/internal/bus"
)

func conversationPrincipal(p bus.ConversationPrincipal) error {
	if p.Actor == "" || p.Run == "" || (bus.IsHumanActor(p.Actor) != p.Gateway) {
		return bus.ErrChannelDenied
	}
	return nil
}

func digestRequest(v interface{}) (string, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	d := sha256.Sum256(raw)
	return hex.EncodeToString(d[:]), nil
}

func channelText(field, value string, max int) error {
	if strings.TrimSpace(value) == "" {
		return &bus.ValidationError{Field: field, Problem: "is required"}
	}
	return bus.ValidateTextIdentifier(field, value, max)
}

func channelTx(ctx context.Context, tx *sql.Tx, p bus.ConversationPrincipal, id string) (bus.Channel, error) {
	var c bus.Channel
	if err := conversationPrincipal(p); err != nil {
		return c, err
	}
	err := tx.QueryRowContext(ctx, `SELECT channel_id, project_id, kind, title, created_by, policy_revision, next_seq-1 FROM channels WHERE channel_id=?`, id).
		Scan(&c.ID, &c.ProjectID, &c.Kind, &c.Title, &c.Creator, &c.Revision, &c.LastSeq)
	if errors.Is(err, sql.ErrNoRows) {
		return c, bus.ErrChannelDenied
	}
	if err != nil {
		return c, err
	}
	var post int
	err = tx.QueryRowContext(ctx, `SELECT MIN(history_from), MAX(can_post) FROM channel_grants WHERE channel_id=? AND actor=? AND revoked_seq IS NULL HAVING COUNT(*)>0`, id, p.Actor).
		Scan(&c.HistoryFrom, &post)
	if errors.Is(err, sql.ErrNoRows) {
		return bus.Channel{}, bus.ErrChannelDenied
	}
	if err != nil {
		return bus.Channel{}, err
	}
	c.CanPost = post != 0
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM channel_responses q JOIN messages m ON m.message_id=q.message_id WHERE q.channel_id=? AND q.respondent=? AND q.state='open' AND m.channel_seq>=?)`, id, p.Actor, c.HistoryFrom).Scan(&c.NeedsResponse); err != nil {
		return bus.Channel{}, err
	}
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(channel_seq),0) FROM messages WHERE conversation_id=? AND channel_seq>=?`, id, c.HistoryFrom).Scan(&c.LastMessageSeq); err != nil {
		return bus.Channel{}, err
	}
	c.CanManage = c.CanPost && c.Creator == p.Actor && c.Kind == "named"
	c.Participants, c.Observers = []string{}, []string{}
	rows, err := tx.QueryContext(ctx, `SELECT actor, MAX(can_post) FROM channel_grants WHERE channel_id=? AND revoked_seq IS NULL GROUP BY actor ORDER BY actor`, id)
	if err != nil {
		return bus.Channel{}, err
	}
	defer rows.Close()
	for rows.Next() {
		var actor string
		if err := rows.Scan(&actor, &post); err != nil {
			return bus.Channel{}, err
		}
		if post != 0 {
			c.Participants = append(c.Participants, actor)
		} else {
			c.Observers = append(c.Observers, actor)
		}
	}
	return c, rows.Err()
}

func (s *Store) ChannelGet(ctx context.Context, p bus.ConversationPrincipal, id string) (bus.Channel, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return bus.Channel{}, err
	}
	defer tx.Rollback()
	return channelTx(ctx, tx, p, id)
}

func (s *Store) ChannelList(ctx context.Context, p bus.ConversationPrincipal) ([]bus.Channel, error) {
	if err := conversationPrincipal(p); err != nil {
		return nil, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT DISTINCT channel_id FROM channel_grants WHERE actor=? AND revoked_seq IS NULL ORDER BY channel_id LIMIT 201`, p.Actor)
	if err != nil {
		return nil, err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	if len(ids) > 200 {
		return nil, &bus.ValidationError{Field: "channels", Problem: "list exceeds the 200-channel local view limit; use channel.get for a known channel"}
	}
	result := []bus.Channel{}
	for _, id := range ids {
		c, err := channelTx(ctx, tx, p, id)
		if err != nil {
			return nil, err
		}
		v, err := channelViewTx(ctx, tx, p, id, "")
		if err != nil {
			return nil, err
		}
		c.View = &v
		result = append(result, c)
	}
	return result, nil
}

func channelOperation(ctx context.Context, tx *sql.Tx, p bus.ConversationPrincipal, op, key, digest string, result interface{}) (bool, error) {
	var old string
	var raw []byte
	err := tx.QueryRowContext(ctx, `SELECT request_digest,result FROM channel_operations WHERE actor=? AND operation=? AND idempotency_key=?`, p.Actor, op, key).Scan(&old, &raw)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if old != digest {
		return false, bus.ErrIdempotencyConflict
	}
	return true, json.Unmarshal(raw, result)
}

func saveChannelOperation(ctx context.Context, tx *sql.Tx, p bus.ConversationPrincipal, op, key, digest string, result interface{}) error {
	raw, err := json.Marshal(result)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO channel_operations(actor,operation,idempotency_key,request_digest,result) VALUES(?,?,?,?,?)`, p.Actor, op, key, digest, raw)
	return err
}

func normalizeChannelCreate(p bus.ConversationPrincipal, r bus.ChannelCreate) (bus.ChannelCreate, error) {
	if err := conversationPrincipal(p); err != nil {
		return r, err
	}
	if err := channelText("project_id", r.ProjectID, 128); err != nil {
		return r, err
	}
	if err := channelText("idempotency_key", r.IdempotencyKey, 256); err != nil {
		return r, err
	}
	if r.Kind != "dm" && r.Kind != "named" {
		return r, &bus.ValidationError{Field: "kind", Problem: "only dm and named private channels are supported"}
	}
	if len(r.Participants) < 2 || len(r.Participants) > 32 {
		return r, &bus.ValidationError{Field: "participants", Problem: "must contain 2 to 32 distinct actors"}
	}
	r.Participants = append([]string(nil), r.Participants...)
	sort.Strings(r.Participants)
	found := false
	for i, a := range r.Participants {
		if err := channelText("participant", a, 128); err != nil {
			return r, err
		}
		if strings.TrimSpace(a) != a || (i > 0 && a == r.Participants[i-1]) {
			return r, &bus.ValidationError{Field: "participants", Problem: "must use distinct canonical actor IDs"}
		}
		found = found || a == p.Actor
	}
	if !found {
		return r, bus.ErrChannelDenied
	}
	if r.Kind == "dm" {
		if len(r.Participants) != 2 {
			return r, bus.ErrImmutableAudience
		}
		r.Title = ""
	} else if err := channelText("title", r.Title, 256); err != nil {
		return r, err
	}
	return r, nil
}

func (s *Store) ChannelCreate(ctx context.Context, p bus.ConversationPrincipal, r bus.ChannelCreate) (bus.Channel, error) {
	r, err := normalizeChannelCreate(p, r)
	if err != nil {
		return bus.Channel{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return bus.Channel{}, err
	}
	defer tx.Rollback()
	c, err := s.createChannelTx(ctx, tx, p, r)
	if err != nil {
		return bus.Channel{}, err
	}
	if err := tx.Commit(); err != nil {
		return bus.Channel{}, err
	}
	return c, nil
}

func (s *Store) createChannelTx(ctx context.Context, tx *sql.Tx, p bus.ConversationPrincipal, r bus.ChannelCreate) (bus.Channel, error) {
	digest, err := digestRequest(r)
	if err != nil {
		return bus.Channel{}, err
	}
	var id string
	if found, err := channelOperation(ctx, tx, p, "create", r.IdempotencyKey, digest, &id); err != nil {
		return bus.Channel{}, err
	} else if found {
		return channelTx(ctx, tx, p, id)
	}
	var dmKey interface{}
	if r.Kind == "dm" {
		key, _ := digestRequest([]interface{}{r.ProjectID, r.Participants})
		dmKey = key
		err := tx.QueryRowContext(ctx, `SELECT channel_id FROM channels WHERE dm_key=?`, key).Scan(&id)
		if err == nil {
			c, err := channelTx(ctx, tx, p, id)
			if err != nil {
				return bus.Channel{}, err
			}
			if err := saveChannelOperation(ctx, tx, p, "create", r.IdempotencyKey, digest, id); err != nil {
				return bus.Channel{}, err
			}
			return c, nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return bus.Channel{}, err
		}
	}
	for _, a := range r.Participants {
		var known int
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM actor_names WHERE actor=?)`, a).Scan(&known); err != nil {
			return bus.Channel{}, err
		}
		if known == 0 {
			return bus.Channel{}, bus.ErrChannelDenied
		}
		if err := assertActorNotAdoptedTx(ctx, tx, a); err != nil {
			return bus.Channel{}, err
		}
		if archived, err := s.actorArchivedTx(ctx, tx, a); err != nil {
			return bus.Channel{}, err
		} else if archived {
			return bus.Channel{}, bus.ErrChannelDenied
		}
		if bus.IsHumanActor(a) {
			if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM human_actors WHERE actor=?`, a).Scan(&known); err != nil {
				return bus.Channel{}, err
			}
			if known != 1 {
				return bus.Channel{}, bus.ErrChannelDenied
			}
		}
	}
	id, err = s.newID("chn")
	if err != nil {
		return bus.Channel{}, err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO channels(channel_id,project_id,kind,title,created_by,dm_key,created_at_ns) VALUES(?,?,?,?,?,?,?)`, id, r.ProjectID, r.Kind, r.Title, p.Actor, dmKey, s.now().UnixNano())
	if err != nil {
		return bus.Channel{}, err
	}
	for _, a := range r.Participants {
		if err := s.channelGrantTx(ctx, tx, id, a, 0); err != nil {
			return bus.Channel{}, err
		}
	}
	if _, err := s.channelEventTx(ctx, tx, id, "channel.created", p.Actor, "", map[string]interface{}{"participants": r.Participants}); err != nil {
		return bus.Channel{}, err
	}
	if err := saveChannelOperation(ctx, tx, p, "create", r.IdempotencyKey, digest, id); err != nil {
		return bus.Channel{}, err
	}
	return channelTx(ctx, tx, p, id)
}

func (s *Store) channelGrantTx(ctx context.Context, tx *sql.Tx, id, actor string, floor int64) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO channel_grants(channel_id,actor,source,can_post,history_from,granted_seq) VALUES(?,?,'participant',1,?,?)`, id, actor, floor, floor)
	if err != nil {
		return err
	}
	var human string
	err = tx.QueryRowContext(ctx, `SELECT human FROM supervision_links WHERE agent=? AND active=1`, actor).Scan(&human)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO channel_grants(channel_id,actor,source,can_post,history_from,granted_seq) VALUES(?,?,?,0,?,?)`, id, human, "supervisor:"+actor, floor, floor)
	return err
}

func (s *Store) channelEventTx(ctx context.Context, tx *sql.Tx, id, kind, actor, message string, payload interface{}) (int64, error) {
	var seq int64
	if err := tx.QueryRowContext(ctx, `SELECT next_seq FROM channels WHERE channel_id=?`, id).Scan(&seq); err != nil {
		return 0, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE channels SET next_seq=next_seq+1 WHERE channel_id=?`, id); err != nil {
		return 0, err
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return 0, err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO channel_events(channel_id,seq,kind,actor,message_id,payload,created_at_ns) VALUES(?,?,?,?,?,?,?)`, id, seq, kind, actor, message, raw, s.now().UnixNano())
	return seq, err
}

func (s *Store) ChannelPost(ctx context.Context, p bus.ConversationPrincipal, r bus.ChannelPost) (bus.ChannelPostResult, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return bus.ChannelPostResult{}, err
	}
	defer tx.Rollback()
	result, err := s.channelPostTx(ctx, tx, p, r)
	if err != nil {
		return bus.ChannelPostResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return bus.ChannelPostResult{}, err
	}
	return result, nil
}

func (s *Store) channelPostTx(ctx context.Context, tx *sql.Tx, p bus.ConversationPrincipal, r bus.ChannelPost) (bus.ChannelPostResult, error) {
	var result bus.ChannelPostResult
	c, err := channelTx(ctx, tx, p, r.ChannelID)
	if err != nil {
		return result, err
	}
	if !c.CanPost {
		return result, bus.ErrChannelDenied
	}
	if err := channelText("idempotency_key", r.IdempotencyKey, 256); err != nil {
		return result, err
	}
	if len(r.Body) == 0 || len(r.Body) > bus.MaxBodyBytes || !json.Valid(r.Body) {
		return result, &bus.ValidationError{Field: "body", Problem: "must be valid JSON within the message limit"}
	}
	if len(r.References) > 16 {
		return result, &bus.ValidationError{Field: "references", Problem: "maximum 16"}
	}
	if err := assertActorNotAdoptedTx(ctx, tx, p.Actor); err != nil {
		return result, err
	}
	digest, err := digestRequest(r)
	if err != nil {
		return result, err
	}
	var priorID, priorDigest string
	err = tx.QueryRowContext(ctx, `SELECT m.message_id,COALESCE(r.request_digest,'') FROM messages m LEFT JOIN channel_message_requests r ON r.message_id=m.message_id WHERE m.from_actor=? AND m.idempotency_key=?`, p.Actor, r.IdempotencyKey).Scan(&priorID, &priorDigest)
	if err == nil {
		if digest != priorDigest {
			return result, bus.ErrIdempotencyConflict
		}
		result.Message, err = s.channelMessageTx(ctx, tx, p, priorID)
		result.Duplicate = true
		if err == nil {
			result.Recipients, err = managedRecipientsTx(ctx, tx, priorID)
		}
		return result, err
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return result, err
	}
	if r.ExpectedRevision != c.Revision {
		return result, bus.ErrAudienceChanged
	}
	if r.ResponseTo != "" {
		var questionID, threadID string
		err := tx.QueryRowContext(ctx, `SELECT q.message_id,m.thread_id FROM channel_responses q JOIN messages m ON m.message_id=q.message_id WHERE q.request_id=? AND q.channel_id=? AND q.respondent=? AND q.state='open' AND q.revision=?`, r.ResponseTo, c.ID, p.Actor, r.ExpectedResponseRevision).Scan(&questionID, &threadID)
		if errors.Is(err, sql.ErrNoRows) {
			return result, bus.ErrResponseConflict
		}
		if err != nil {
			return result, err
		}
		if r.ThreadID != "" && r.ThreadID != threadID {
			return result, bus.ErrResponseConflict
		}
		r.ThreadID = threadID
		if r.InReplyTo == "" {
			r.InReplyTo = questionID
		}
	}
	if r.InReplyTo != "" {
		parent, err := s.channelMessageTx(ctx, tx, p, r.InReplyTo)
		if err != nil {
			return result, err
		}
		if parent.ChannelID != c.ID || (r.ThreadID != "" && r.ThreadID != parent.ThreadID) {
			return result, &bus.ValidationError{Field: "in_reply_to", Problem: "parent must belong to the same channel and thread"}
		}
		r.ThreadID = parent.ThreadID
	}
	if r.ThreadID != "" {
		var root string
		if err := tx.QueryRowContext(ctx, `SELECT root_message_id FROM channel_threads WHERE channel_id=? AND thread_id=?`, c.ID, r.ThreadID).Scan(&root); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return result, bus.ErrChannelDenied
			}
			return result, err
		}
	}
	newThread := r.ThreadID == ""
	if newThread {
		r.ThreadID, err = s.newID("thr")
		if err != nil {
			return result, err
		}
	}
	member := map[string]bool{}
	if len(r.Attention) > 32 {
		return result, &bus.ValidationError{Field: "attention_targets", Problem: "maximum 32 distinct participants"}
	}
	escapedBody, err := json.Marshal(r.Body)
	if err != nil || len(escapedBody) > bus.MaxBodyBytes {
		return result, &bus.ValidationError{Field: "body", Problem: "encoded body exceeds transport limit"}
	}
	for _, a := range c.Participants {
		member[a] = true
	}
	attentionSeen := map[string]bool{}
	for _, a := range r.Attention {
		if attentionSeen[a] {
			return result, &bus.ValidationError{Field: "attention_targets", Problem: "duplicate participant"}
		}
		attentionSeen[a] = true
		if !member[a] || a == p.Actor {
			return result, bus.ErrChannelDenied
		}
	}
	if r.Respondent != "" && (!member[r.Respondent] || r.Respondent == p.Actor) {
		return result, bus.ErrChannelDenied
	}
	seen := map[string]bool{}
	for _, ref := range r.References {
		if ref.Relation != "discusses" && ref.Relation != "continued_from" {
			return result, bus.ErrShareAuthority
		}
		if seen[ref.SourceMessageID] {
			return result, &bus.ValidationError{Field: "references", Problem: "duplicate source"}
		}
		seen[ref.SourceMessageID] = true
		if _, err := s.channelMessageTx(ctx, tx, p, ref.SourceMessageID); err != nil {
			return result, err
		}
	}
	id, err := s.newID("msg")
	if err != nil {
		return result, err
	}
	seq, err := s.channelEventTx(ctx, tx, c.ID, "message.posted", p.Actor, id, nil)
	if err != nil {
		return result, err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO messages(message_id,schema_version,idempotency_key,project_id,channel_id,thread_id,from_actor,from_run,message_type,delivery_request,in_reply_to,body,created_at_ns,conversation_id,channel_seq) VALUES(?,2,?,?,'managed',?,?,?,'MESSAGE','non-blocking',NULLIF(?,''),?,?,?,?)`, id, r.IdempotencyKey, c.ProjectID, r.ThreadID, p.Actor, p.Run, r.InReplyTo, []byte(r.Body), s.now().UnixNano(), c.ID, seq)
	if err != nil {
		return result, err
	}
	if newThread {
		if _, err := tx.ExecContext(ctx, `INSERT INTO channel_threads(channel_id,thread_id,root_message_id) VALUES(?,?,?)`, c.ID, r.ThreadID, id); err != nil {
			return result, err
		}
	}
	for _, a := range c.Participants {
		if a == p.Actor {
			continue
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO managed_deliveries(message_id,recipient_actor) VALUES(?,?)`, id, a); err != nil {
			return result, err
		}
		result.Recipients = append(result.Recipients, a)
	}
	for _, ref := range r.References {
		if _, err := tx.ExecContext(ctx, `INSERT INTO channel_references(message_id,source_message_id,relation) VALUES(?,?,?)`, id, ref.SourceMessageID, ref.Relation); err != nil {
			return result, err
		}
	}
	// Managed notification dispatch is enabled separately after connector negotiation.
	// Persist requested attention without exposing managed IDs in legacy events.
	for _, a := range r.Attention {
		if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO notification_outbox(message_id,recipient_actor,state,available_at_ns,created_at_ns,source) VALUES(?,?,'managed-pending',?,?,'managed')`, id, a, s.now().UnixNano(), s.now().UnixNano()); err != nil {
			return result, err
		}
	}
	if r.Respondent != "" {
		rid, err := s.newID("req")
		if err != nil {
			return result, err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO channel_responses(request_id,channel_id,message_id,requester,respondent,state) VALUES(?,?,?,?,?,'open')`, rid, c.ID, id, p.Actor, r.Respondent); err != nil {
			return result, err
		}
	}
	if r.ResponseTo != "" {
		res, err := tx.ExecContext(ctx, `UPDATE channel_responses SET state='answered',answer_id=?,revision=revision+1 WHERE request_id=? AND channel_id=? AND respondent=? AND revision=? AND state='open'`, id, r.ResponseTo, c.ID, p.Actor, r.ExpectedResponseRevision)
		if err != nil {
			return result, err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return result, err
		}
		if n != 1 {
			return result, bus.ErrResponseConflict
		}
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO channel_message_requests(message_id,request_digest) VALUES(?,?)`, id, digest); err != nil {
		return result, err
	}
	result.Message, err = s.channelMessageTx(ctx, tx, p, id)
	return result, err
}

func managedRecipientsTx(ctx context.Context, tx *sql.Tx, id string) ([]string, error) {
	rows, err := tx.QueryContext(ctx, `SELECT recipient_actor FROM managed_deliveries WHERE message_id=? ORDER BY recipient_actor`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []string{}
	for rows.Next() {
		var a string
		if err := rows.Scan(&a); err != nil {
			return nil, err
		}
		result = append(result, a)
	}
	return result, rows.Err()
}

func (s *Store) ChannelMessageGet(ctx context.Context, p bus.ConversationPrincipal, id string) (bus.ChannelMessage, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return bus.ChannelMessage{}, err
	}
	defer tx.Rollback()
	return s.channelMessageTx(ctx, tx, p, id)
}

func (s *Store) channelMessageTx(ctx context.Context, tx *sql.Tx, p bus.ConversationPrincipal, id string) (bus.ChannelMessage, error) {
	var m bus.ChannelMessage
	var raw []byte
	var created int64
	if err := conversationPrincipal(p); err != nil {
		return m, err
	}
	err := tx.QueryRowContext(ctx, `SELECT message_id,conversation_id,thread_id,channel_seq,from_actor,from_run,COALESCE(in_reply_to,''),body,created_at_ns FROM messages WHERE message_id=? AND conversation_id IS NOT NULL AND (expires_at_ns IS NULL OR expires_at_ns>?)`, id, s.now().UnixNano()).Scan(&m.ID, &m.ChannelID, &m.ThreadID, &m.Seq, &m.FromActor, &m.FromRun, &m.InReplyTo, &raw, &created)
	if errors.Is(err, sql.ErrNoRows) {
		return bus.ChannelMessage{}, bus.ErrChannelDenied
	}
	if err != nil {
		return bus.ChannelMessage{}, err
	}
	c, err := channelTx(ctx, tx, p, m.ChannelID)
	if err != nil {
		return bus.ChannelMessage{}, err
	}
	if m.Seq < c.HistoryFrom {
		return bus.ChannelMessage{}, bus.ErrChannelDenied
	}
	m.Body = json.RawMessage(raw)
	m.CreatedAt = time.Unix(0, created).UTC()
	m.References = []bus.ChannelReference{}
	if m.InReplyTo != "" {
		var parentSeq int64
		if err := tx.QueryRowContext(ctx, `SELECT channel_seq FROM messages WHERE message_id=?`, m.InReplyTo).Scan(&parentSeq); err != nil {
			return bus.ChannelMessage{}, err
		}
		if parentSeq < c.HistoryFrom {
			m.InReplyTo = ""
		}
	}
	rows, err := tx.QueryContext(ctx, `SELECT r.source_message_id,r.relation,m.conversation_id,m.channel_seq,CASE WHEN m.expires_at_ns IS NULL OR m.expires_at_ns>? THEN 1 ELSE 0 END FROM channel_references r JOIN messages m ON m.message_id=r.source_message_id WHERE r.message_id=? ORDER BY r.source_message_id`, s.now().UnixNano(), id)
	if err != nil {
		return bus.ChannelMessage{}, err
	}
	type source struct {
		id, relation, channel string
		seq                   int64
		unexpired             int
	}
	var sources []source
	for rows.Next() {
		var r source
		if err := rows.Scan(&r.id, &r.relation, &r.channel, &r.seq, &r.unexpired); err != nil {
			rows.Close()
			return bus.ChannelMessage{}, err
		}
		sources = append(sources, r)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return bus.ChannelMessage{}, err
	}
	for _, r := range sources {
		ref := bus.ChannelReference{Relation: r.relation}
		if ch, err := channelTx(ctx, tx, p, r.channel); err == nil && r.seq >= ch.HistoryFrom && r.unexpired != 0 {
			ref.SourceMessageID = r.id
			ref.Available = true
		} else if err != nil && !errors.Is(err, bus.ErrChannelDenied) {
			return bus.ChannelMessage{}, err
		}
		m.References = append(m.References, ref)
	}
	err = tx.QueryRowContext(ctx, `SELECT request_id FROM channel_responses WHERE message_id=?`, id).Scan(&m.ResponseID)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return bus.ChannelMessage{}, err
	}
	return m, nil
}

// ChannelHistory returns an ACL-checked page. after/revision are an internal
// cursor contract; transports bind them to the authenticated principal.
func (s *Store) ChannelHistory(ctx context.Context, p bus.ConversationPrincipal, id, thread string, after, revision int64, limit int) (bus.ChannelPage, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return bus.ChannelPage{}, err
	}
	defer tx.Rollback()
	c, err := channelTx(ctx, tx, p, id)
	if err != nil {
		return bus.ChannelPage{}, err
	}
	if revision != 0 && revision != c.Revision {
		return bus.ChannelPage{}, bus.ErrAudienceChanged
	}
	if after < 0 {
		return bus.ChannelPage{}, &bus.ValidationError{Field: "after", Problem: "must be nonnegative"}
	}
	if limit <= 0 {
		limit = 50
	}
	if limit > 200 {
		limit = 200
	}
	rows, err := tx.QueryContext(ctx, `SELECT message_id FROM messages WHERE conversation_id=? AND channel_seq>? AND channel_seq>=? AND (?='' OR thread_id=?) AND (expires_at_ns IS NULL OR expires_at_ns>?) ORDER BY channel_seq LIMIT ?`, id, after, c.HistoryFrom, thread, thread, s.now().UnixNano(), limit)
	if err != nil {
		return bus.ChannelPage{}, err
	}
	var ids []string
	for rows.Next() {
		var mid string
		if err := rows.Scan(&mid); err != nil {
			rows.Close()
			return bus.ChannelPage{}, err
		}
		ids = append(ids, mid)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return bus.ChannelPage{}, err
	}
	page := bus.ChannelPage{Messages: []bus.ChannelMessage{}, PolicyRevision: c.Revision}
	size := 0
	for _, mid := range ids {
		m, err := s.channelMessageTx(ctx, tx, p, mid)
		if err != nil {
			return bus.ChannelPage{}, err
		}
		raw, err := json.Marshal(m)
		if err != nil {
			return bus.ChannelPage{}, err
		}
		if size+len(raw) > 1500000 && len(page.Messages) > 0 {
			break
		}
		size += len(raw)
		page.Messages = append(page.Messages, m)
	}
	// Only ChannelHistoryPage may issue a signed transport cursor.
	return page, nil
}
