package sqlite

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/72olabs/holler/internal/bus"
)

type ContinuationIntent struct {
	SourceMessageID string             `json:"source_message_id"`
	DestinationID   string             `json:"destination_channel_id,omitempty"`
	Create          *bus.ChannelCreate `json:"create,omitempty"`
	Body            json.RawMessage    `json:"body"`
	Attention       []string           `json:"attention_targets,omitempty"`
	Respondent      string             `json:"respondent,omitempty"`
}
type ContinuationPreview struct {
	Token        string    `json:"preflight_token"`
	Participants []string  `json:"participants"`
	Observers    []string  `json:"observers"`
	ContextMode  string    `json:"context_mode"`
	ExpiresAt    time.Time `json:"expires_at"`
}
type continuationToken struct {
	Actor               string             `json:"actor"`
	Expires             int64              `json:"expires"`
	Intent              ContinuationIntent `json:"intent"`
	SourceRevision      int64              `json:"source_revision"`
	DestinationRevision int64              `json:"destination_revision"`
	SupervisionDigest   string             `json:"supervision_digest"`
}

func (s *Store) signConversationToken(v interface{}) (string, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	mac := hmac.New(sha256.New, s.conversationSecret[:])
	mac.Write(raw)
	return base64.RawURLEncoding.EncodeToString(raw) + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil)), nil
}
func (s *Store) readConversationToken(token string, v interface{}) error {
	if len(token) > bus.MaxBodyBytes*2 {
		return bus.ErrChannelDenied
	}
	parts := strings.Split(token, ".")
	if len(parts) != 2 {
		return bus.ErrChannelDenied
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return bus.ErrChannelDenied
	}
	sig, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return bus.ErrChannelDenied
	}
	mac := hmac.New(sha256.New, s.conversationSecret[:])
	mac.Write(raw)
	if !hmac.Equal(sig, mac.Sum(nil)) {
		return bus.ErrChannelDenied
	}
	if err := json.Unmarshal(raw, v); err != nil {
		return bus.ErrChannelDenied
	}
	return nil
}

func supervisionDigestTx(ctx context.Context, tx *sql.Tx, participants []string) (string, []string, error) {
	type link struct {
		Agent, Human string
		Revision     int64
		Active       int
	}
	links := []link{}
	observers := []string{}
	seen := map[string]bool{}
	for _, a := range participants {
		l := link{Agent: a}
		err := tx.QueryRowContext(ctx, `SELECT human,revision,active FROM supervision_links WHERE agent=?`, a).Scan(&l.Human, &l.Revision, &l.Active)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return "", nil, err
		}
		links = append(links, l)
		if l.Active == 1 && !seen[l.Human] {
			observers = append(observers, l.Human)
			seen[l.Human] = true
		}
	}
	d, err := digestRequest(links)
	return d, observers, err
}

func (s *Store) ChannelContinuationPreflight(ctx context.Context, p bus.ConversationPrincipal, intent ContinuationIntent) (ContinuationPreview, error) {
	var preview ContinuationPreview
	if err := conversationPrincipal(p); err != nil {
		return preview, err
	}
	if len(intent.Body) == 0 || len(intent.Body) > bus.MaxBodyBytes/2 || !json.Valid(intent.Body) {
		return preview, &bus.ValidationError{Field: "body", Problem: "continuation body must be JSON within 512 KiB"}
	}
	encoded, err := json.Marshal(intent)
	if err != nil || len(encoded) > bus.MaxBodyBytes/2 {
		return preview, &bus.ValidationError{Field: "intent", Problem: "encoded continuation intent must fit within 512 KiB"}
	}
	if (intent.Create == nil) == (intent.DestinationID == "") {
		return preview, &bus.ValidationError{Field: "destination", Problem: "select an existing channel or a new conversation, not both"}
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return preview, err
	}
	defer tx.Rollback()
	source, err := s.channelMessageTx(ctx, tx, p, intent.SourceMessageID)
	if err != nil {
		return preview, err
	}
	sourceChannel, err := channelTx(ctx, tx, p, source.ChannelID)
	if err != nil {
		return preview, err
	}
	token := continuationToken{Actor: p.Actor, Intent: intent, SourceRevision: sourceChannel.Revision, Expires: s.now().Add(10 * time.Minute).Unix()}
	if intent.Create != nil {
		create := *intent.Create
		create.IdempotencyKey = "preflight"
		create, err = normalizeChannelCreate(p, create)
		if err != nil {
			return preview, err
		}
		token.Intent.Create = &create
		preview.Participants = create.Participants
		token.SupervisionDigest, preview.Observers, err = supervisionDigestTx(ctx, tx, create.Participants)
		if err != nil {
			return preview, err
		}
		if create.Kind == "dm" {
			pairKey, _ := digestRequest([]interface{}{create.ProjectID, create.Participants})
			var existing string
			err := tx.QueryRowContext(ctx, `SELECT channel_id FROM channels WHERE dm_key=?`, pairKey).Scan(&existing)
			if err == nil {
				c, err := channelTx(ctx, tx, p, existing)
				if err != nil {
					return preview, err
				}
				token.Intent.Create = nil
				token.Intent.DestinationID = existing
				token.DestinationRevision = c.Revision
				preview.Participants, preview.Observers = c.Participants, c.Observers
			} else if !errors.Is(err, sql.ErrNoRows) {
				return preview, err
			}
		}
	} else {
		c, err := channelTx(ctx, tx, p, intent.DestinationID)
		if err != nil {
			return preview, err
		}
		if !c.CanPost {
			return preview, bus.ErrChannelDenied
		}
		token.DestinationRevision = c.Revision
		preview.Participants = c.Participants
		preview.Observers = c.Observers
	}
	preview.Token, err = s.signConversationToken(token)
	preview.ContextMode = "reference_only"
	preview.ExpiresAt = time.Unix(token.Expires, 0).UTC()
	return preview, err
}

func (s *Store) ChannelContinuationCommit(ctx context.Context, p bus.ConversationPrincipal, token, key string) (bus.ChannelPostResult, error) {
	var result bus.ChannelPostResult
	if err := conversationPrincipal(p); err != nil {
		return result, err
	}
	if err := channelText("idempotency_key", key, 256); err != nil {
		return result, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return result, err
	}
	defer tx.Rollback()
	digest, _ := digestRequest(token)
	var savedID string
	if found, err := channelOperation(ctx, tx, p, "continue", key, digest, &savedID); err != nil {
		return result, err
	} else if found {
		result.Message, err = s.channelMessageTx(ctx, tx, p, savedID)
		if err != nil {
			return bus.ChannelPostResult{}, err
		}
		result.Recipients, err = managedRecipientsTx(ctx, tx, savedID)
		result.Duplicate = true
		return result, err
	}
	var data continuationToken
	if err := s.readConversationToken(token, &data); err != nil {
		return result, bus.ErrPreflightExpired
	}
	if data.Actor != p.Actor {
		return result, bus.ErrChannelDenied
	}
	if data.Expires <= s.now().Unix() {
		return result, bus.ErrPreflightExpired
	}
	source, err := s.channelMessageTx(ctx, tx, p, data.Intent.SourceMessageID)
	if err != nil {
		return result, err
	}
	sc, err := channelTx(ctx, tx, p, source.ChannelID)
	if err != nil {
		return result, err
	}
	if sc.Revision != data.SourceRevision {
		return result, bus.ErrAudienceChanged
	}
	var dest bus.Channel
	if data.Intent.Create != nil {
		create := *data.Intent.Create
		d, _, err := supervisionDigestTx(ctx, tx, create.Participants)
		if err != nil {
			return result, err
		}
		if d != data.SupervisionDigest {
			return result, bus.ErrAudienceChanged
		}
		creationKey, _ := digestRequest([]string{p.Actor, key})
		create.IdempotencyKey = "continuation-" + creationKey
		// An existing DM may have acquired a different observer audience. Refuse
		// reuse unless the preflight explicitly inspected that destination.
		if create.Kind == "dm" {
			pairKey, _ := digestRequest([]interface{}{create.ProjectID, create.Participants})
			var found string
			err := tx.QueryRowContext(ctx, `SELECT channel_id FROM channels WHERE dm_key=?`, pairKey).Scan(&found)
			if err == nil {
				return result, bus.ErrAudienceChanged
			}
			if !errors.Is(err, sql.ErrNoRows) {
				return result, err
			}
		}
		dest, err = s.createChannelTx(ctx, tx, p, create)
		if err != nil {
			return result, err
		}
	} else {
		dest, err = channelTx(ctx, tx, p, data.Intent.DestinationID)
		if err != nil {
			return result, err
		}
		if dest.Revision != data.DestinationRevision {
			return result, bus.ErrAudienceChanged
		}
	}
	result, err = s.channelPostTx(ctx, tx, p, bus.ChannelPost{ChannelID: dest.ID, ExpectedRevision: dest.Revision, IdempotencyKey: key, Body: data.Intent.Body, Attention: data.Intent.Attention, Respondent: data.Intent.Respondent, References: []bus.ChannelReference{{SourceMessageID: source.ID, Relation: "continued_from"}}})
	if err != nil {
		return result, err
	}
	if err := saveChannelOperation(ctx, tx, p, "continue", key, digest, result.Message.ID); err != nil {
		return result, err
	}
	if err := tx.Commit(); err != nil {
		return result, err
	}
	return result, nil
}

type historyToken struct {
	Actor, Channel, Thread string
	After, Revision        int64
}

func (s *Store) ChannelHistoryPage(ctx context.Context, p bus.ConversationPrincipal, id, thread, cursor string, limit int) (bus.ChannelPage, error) {
	t := historyToken{Actor: p.Actor, Channel: id, Thread: thread}
	if cursor != "" {
		if err := s.readConversationToken(cursor, &t); err != nil {
			return bus.ChannelPage{}, bus.ErrCursorExpired
		}
		if t.Actor != p.Actor || t.Channel != id || t.Thread != thread {
			return bus.ChannelPage{}, bus.ErrChannelDenied
		}
	}
	page, err := s.ChannelHistory(ctx, p, id, thread, t.After, t.Revision, limit)
	if err != nil {
		return page, err
	}
	page.NextCursor = ""
	if len(page.Messages) > 0 {
		t.After = page.Messages[len(page.Messages)-1].Seq
		t.Revision = page.PolicyRevision
		page.NextCursor, err = s.signConversationToken(t)
	}
	return page, err
}
