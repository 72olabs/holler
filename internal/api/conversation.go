package api

import (
	"context"
	"encoding/json"
	"github.com/72olabs/holler/internal/bus"
	"github.com/72olabs/holler/internal/store/sqlite"
	"reflect"
	"strings"
	"time"
)

// ConversationStore is deliberately separate from the frozen legacy Store.
// The implementation is optional and must be explicitly enabled by the daemon.
const ManagedConversationsCapability = "managed-conversations-v1"

type ConversationStore interface {
	ChannelCreate(context.Context, bus.ConversationPrincipal, bus.ChannelCreate) (bus.Channel, error)
	ChannelGet(context.Context, bus.ConversationPrincipal, string) (bus.Channel, error)
	ChannelList(context.Context, bus.ConversationPrincipal) ([]bus.Channel, error)
	ChannelPost(context.Context, bus.ConversationPrincipal, bus.ChannelPost) (bus.ChannelPostResult, error)
	ChannelMessageGet(context.Context, bus.ConversationPrincipal, string) (bus.ChannelMessage, error)
	ChannelHistoryPage(context.Context, bus.ConversationPrincipal, string, string, string, int) (bus.ChannelPage, error)
	ChannelMembershipChange(context.Context, bus.ConversationPrincipal, string, string, string, string, int64) (bus.Channel, error)
	ChannelContinuationPreflight(context.Context, bus.ConversationPrincipal, sqlite.ContinuationIntent) (sqlite.ContinuationPreview, error)
	ChannelContinuationCommit(context.Context, bus.ConversationPrincipal, string, string) (bus.ChannelPostResult, error)
	ChannelViewGet(context.Context, bus.ConversationPrincipal, string, string) (bus.ChannelViewState, error)
	ChannelViewUpdate(context.Context, bus.ConversationPrincipal, bus.ChannelViewState) (bus.ChannelViewState, error)
	ChannelResponses(context.Context, bus.ConversationPrincipal, string) ([]bus.ChannelResponse, error)
	ChannelResponseResolve(context.Context, bus.ConversationPrincipal, string, string, string, int64) (bus.ChannelResponse, error)
	ChannelInbox(context.Context, bus.ConversationPrincipal, int) ([]sqlite.ChannelDelivery, error)
	ChannelClaim(context.Context, bus.ConversationPrincipal, string, time.Duration) (sqlite.ChannelDelivery, error)
	ChannelDeliveryUpdate(context.Context, bus.ConversationPrincipal, string, string, string, string, time.Duration) (sqlite.ChannelDelivery, error)
}

type conversationRead struct {
	ChannelID string `json:"channel_id"`
	ThreadID  string `json:"thread_id,omitempty"`
	Cursor    string `json:"cursor,omitempty"`
	Limit     int    `json:"limit,omitempty"`
}
type conversationID struct {
	ChannelID string `json:"channel_id"`
}
type conversationMessageID struct {
	MessageID string `json:"message_id"`
}
type conversationMember struct {
	ChannelID string `json:"channel_id"`
	Actor     string `json:"actor"`
	Action    string `json:"action"`
	Key       string `json:"idempotency_key"`
	Revision  int64  `json:"expected_policy_revision"`
}
type conversationCommit struct {
	Token string `json:"preflight_token"`
	Key   string `json:"idempotency_key"`
}
type conversationView struct {
	ChannelID string `json:"channel_id"`
	ThreadID  string `json:"thread_id,omitempty"`
}
type conversationResolve struct {
	ID       string `json:"request_id"`
	Action   string `json:"action"`
	Key      string `json:"idempotency_key"`
	Revision int64  `json:"expected_revision"`
}
type conversationLimit struct {
	Limit int `json:"limit,omitempty"`
}
type conversationLease struct {
	ID      string `json:"message_id"`
	Token   string `json:"lease_token,omitempty"`
	Action  string `json:"action,omitempty"`
	Reason  string `json:"reason,omitempty"`
	Seconds int    `json:"lease_seconds,omitempty"`
}

type conversationOperation struct {
	mode        bus.CapabilityMode
	description string
	args        interface{}
}

var conversationOperations = map[string]conversationOperation{
	"channel.create":                 {bus.CapabilityWrite, "Create a 1:1 internal DM or named private channel using canonical actors. No historical import.", bus.ChannelCreate{}},
	"channel.list":                   {bus.CapabilityRead, "List currently authorized conversations, including declared read-only observers.", struct{}{}},
	"channel.get":                    {bus.CapabilityRead, "Inspect current audience, posting rights, and policy revision.", conversationID{}},
	"channel.post":                   {bus.CapabilityWrite, "Post into a channel with its current policy revision. Attention and designated response are independent.", bus.ChannelPost{}},
	"channel.message":                {bus.CapabilityRead, "Read one authorized message; inaccessible source references are redacted.", conversationMessageID{}},
	"channel.history":                {bus.CapabilityRead, "Read bounded history with a principal-bound cursor. Does not claim or mark read.", conversationRead{}},
	"channel.membership":             {bus.CapabilityWrite, "Creator-controlled join-forward admission/removal for named channels. DM participants cannot change.", conversationMember{}},
	"channel.continuation.preflight": {bus.CapabilityRead, "Preview a reference-only continuation and its audience without writing. Source context is not copied.", sqlite.ContinuationIntent{}},
	"channel.continuation.commit":    {bus.CapabilityWrite, "Atomically create/reuse destination and post the exact confirmed continuation; refresh if policy changed.", conversationCommit{}},
	"channel.view.get":               {bus.CapabilityRead, "Read your own read/unread, snooze, mute and archive state.", conversationView{}},
	"channel.view.update":            {bus.CapabilityWrite, "Update only your own view using its revision. Never acknowledges delivery or answers a question.", bus.ChannelViewState{}},
	"channel.responses":              {bus.CapabilityRead, "List designated response requests visible in this channel.", conversationID{}},
	"channel.response.resolve":       {bus.CapabilityWrite, "Decline your designated request or withdraw your own question. Answer with channel.post response_to.", conversationResolve{}},
	"channel.inbox":                  {bus.CapabilityRead, "Inspect managed deliveries addressed to your posting identity. Observers do not consume agent deliveries.", conversationLimit{}},
	"channel.claim":                  {bus.CapabilityWrite, "Claim a specific managed delivery. Process it and call channel.delivery with its lease token.", conversationMessageID{}},
	"channel.delivery":               {bus.CapabilityWrite, "Acknowledge, retry, dead-letter or extend your managed delivery lease.", conversationLease{}},
}

// schemaForConversation derives closed object shapes from the same request
// structs decoded below, preventing drift or accidental principal arguments.
func schemaForConversation(t reflect.Type) map[string]interface{} {
	if t == reflect.TypeOf(json.RawMessage{}) {
		return map[string]interface{}{}
	}
	if t == reflect.TypeOf(time.Time{}) {
		return map[string]interface{}{"type": "string", "format": "date-time"}
	}
	if t.Kind() == reflect.Pointer {
		return map[string]interface{}{"anyOf": []interface{}{schemaForConversation(t.Elem()), map[string]interface{}{"type": "null"}}}
	}
	switch t.Kind() {
	case reflect.Struct:
		properties := map[string]interface{}{}
		required := []string{}
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			tag := strings.Split(f.Tag.Get("json"), ",")
			if tag[0] == "-" {
				continue
			}
			name := tag[0]
			if name == "" {
				name = f.Name
			}
			properties[name] = schemaForConversation(f.Type)
			if len(tag) == 1 {
				required = append(required, name)
			}
		}
		return map[string]interface{}{"type": "object", "properties": properties, "additionalProperties": false, "required": required}
	case reflect.Slice:
		return map[string]interface{}{"type": "array", "items": schemaForConversation(t.Elem())}
	case reflect.Bool:
		return map[string]interface{}{"type": "boolean"}
	case reflect.Int, reflect.Int64:
		return map[string]interface{}{"type": "integer"}
	default:
		return map[string]interface{}{"type": "string"}
	}
}

func WithConversations() ServerOption {
	return func(s *Server) {
		if _, ok := s.store.(ConversationStore); !ok {
			s.capabilityErr = bus.ErrChannelCapability
			return
		}
		for name, op := range conversationOperations {
			name, op := name, op
			raw, _ := json.Marshal(schemaForConversation(reflect.TypeOf(op.args)))
			WithCapability(bus.CapabilityDescriptor{Name: name, Mode: op.mode, Since: "0.8.0", Description: op.description, InputSchema: raw}, func(ctx context.Context, store Store, id Identity, args json.RawMessage) (interface{}, error) {
				// Gateway/Admin can never be supplied on the Unix bridge.
				p := bus.ConversationPrincipal{Actor: id.Actor, Run: id.RunID}
				return InvokeConversation(ctx, store.(ConversationStore), p, name, args)
			})(s)
		}
	}
}

// InvokeConversation is shared by trusted transports. Never decode p from JSON.
func InvokeConversation(ctx context.Context, s ConversationStore, p bus.ConversationPrincipal, name string, raw json.RawMessage) (interface{}, error) {
	op, ok := conversationOperations[name]
	if !ok {
		return nil, bus.ErrChannelCapability
	}
	args := reflect.New(reflect.TypeOf(op.args))
	if !strings.HasPrefix(strings.TrimSpace(string(raw)), "{") {
		return nil, &bus.ValidationError{Field: "arguments", Problem: "must be an object"}
	}
	if err := decodeStrict(raw, args.Interface()); err != nil {
		return nil, &bus.ValidationError{Field: "arguments", Problem: err.Error()}
	}
	switch a := args.Elem().Interface().(type) {
	case bus.ChannelCreate:
		return s.ChannelCreate(ctx, p, a)
	case bus.ChannelPost:
		return s.ChannelPost(ctx, p, a)
	case conversationID:
		if name == "channel.responses" {
			return s.ChannelResponses(ctx, p, a.ChannelID)
		}
		return s.ChannelGet(ctx, p, a.ChannelID)
	case conversationMessageID:
		if name == "channel.claim" {
			return s.ChannelClaim(ctx, p, a.MessageID, 5*time.Minute)
		}
		return s.ChannelMessageGet(ctx, p, a.MessageID)
	case conversationRead:
		return s.ChannelHistoryPage(ctx, p, a.ChannelID, a.ThreadID, a.Cursor, a.Limit)
	case conversationMember:
		return s.ChannelMembershipChange(ctx, p, a.ChannelID, a.Actor, a.Action, a.Key, a.Revision)
	case sqlite.ContinuationIntent:
		return s.ChannelContinuationPreflight(ctx, p, a)
	case conversationCommit:
		return s.ChannelContinuationCommit(ctx, p, a.Token, a.Key)
	case conversationView:
		return s.ChannelViewGet(ctx, p, a.ChannelID, a.ThreadID)
	case bus.ChannelViewState:
		return s.ChannelViewUpdate(ctx, p, a)
	case conversationResolve:
		return s.ChannelResponseResolve(ctx, p, a.ID, a.Action, a.Key, a.Revision)
	case conversationLimit:
		return s.ChannelInbox(ctx, p, a.Limit)
	case conversationLease:
		return s.ChannelDeliveryUpdate(ctx, p, a.ID, a.Token, a.Action, a.Reason, time.Duration(a.Seconds)*time.Second)
	default:
		return s.ChannelList(ctx, p)
	}
}

func ConversationReadOnly(name string) bool {
	op, ok := conversationOperations[name]
	return ok && op.mode == bus.CapabilityRead
}
