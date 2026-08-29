package mcp

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/72olabs/holler/internal/bus"
)

const defaultProtocolVersion = "2024-11-05"

type Store interface {
	Send(context.Context, bus.SendRequest) (bus.SendResult, error)
	CheckInbox(context.Context, string, int) ([]bus.InboxItem, error)
	Claim(context.Context, string, string, time.Duration) (bus.Claim, error)
	Ack(context.Context, string, string, string) error
	Extend(context.Context, string, string, string, time.Duration) (bus.LeaseExtension, error)
	Nack(context.Context, string, string, string, string, bool) error
	HeartbeatRegistrations(context.Context, string, string, time.Duration) (int, error)
}

type Config struct {
	Actor     string
	RunID     string
	Role      string
	Peer      string
	ProjectID string
	ChannelID string
}

type Server struct {
	store  Store
	config Config
	now    func() time.Time
}

func New(store Store, config Config) (*Server, error) {
	config.Actor = strings.TrimSpace(config.Actor)
	config.RunID = strings.TrimSpace(config.RunID)
	config.Role = strings.TrimSpace(config.Role)
	config.Peer = strings.TrimSpace(config.Peer)
	config.ProjectID = strings.TrimSpace(config.ProjectID)
	config.ChannelID = strings.TrimSpace(config.ChannelID)
	if config.Actor == "" {
		return nil, &bus.ValidationError{Field: "actor", Problem: "is required"}
	}
	if config.RunID == "" {
		return nil, &bus.ValidationError{Field: "run_id", Problem: "is required"}
	}
	if config.ProjectID == "" {
		config.ProjectID = "default"
	}
	if config.ChannelID == "" {
		config.ChannelID = "direct"
	}
	server := &Server{store: store, config: config, now: time.Now}
	return server, nil
}

type request struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type response struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  interface{}     `json:"result,omitempty"`
	Error   *responseError  `json:"error,omitempty"`
}

type responseError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (s *Server) Run(ctx context.Context, input io.Reader, output io.Writer) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go s.heartbeat(runCtx)
	if closer, ok := input.(io.Closer); ok {
		go func() {
			<-runCtx.Done()
			_ = closer.Close()
		}()
	}
	scanner := bufio.NewScanner(input)
	// MCP requests can contain full tool schemas and message bodies.
	scanner.Buffer(make([]byte, 64*1024), bus.MaxBodyBytes+256*1024)
	encoder := json.NewEncoder(output)
	for scanner.Scan() {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		var req request
		if err := json.Unmarshal(line, &req); err != nil {
			if err := encoder.Encode(response{JSONRPC: "2.0", ID: json.RawMessage("null"), Error: &responseError{Code: -32700, Message: err.Error()}}); err != nil {
				return err
			}
			continue
		}
		result, notification, err := s.handle(ctx, req)
		if notification {
			continue
		}
		resp := response{JSONRPC: "2.0", ID: req.ID, Result: result}
		if err != nil {
			resp.Result = nil
			resp.Error = &responseError{Code: -32000, Message: err.Error()}
		}
		if err := encoder.Encode(resp); err != nil {
			return fmt.Errorf("write MCP response: %w", err)
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("read MCP request: %w", err)
	}
	return nil
}

func (s *Server) heartbeat(ctx context.Context) {
	const registrationLease = 5 * time.Minute
	_, _ = s.store.HeartbeatRegistrations(ctx, s.config.Actor, s.config.RunID, registrationLease)
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_, _ = s.store.HeartbeatRegistrations(ctx, s.config.Actor, s.config.RunID, registrationLease)
		}
	}
}

func (s *Server) handle(ctx context.Context, req request) (interface{}, bool, error) {
	switch req.Method {
	case "initialize":
		var params struct {
			ProtocolVersion string `json:"protocolVersion"`
		}
		if len(req.Params) > 0 {
			_ = json.Unmarshal(req.Params, &params)
		}
		if params.ProtocolVersion == "" {
			params.ProtocolVersion = defaultProtocolVersion
		}
		return map[string]interface{}{
			"protocolVersion": params.ProtocolVersion,
			"capabilities":    map[string]interface{}{"tools": map[string]bool{"listChanged": false}},
			"serverInfo":      map[string]string{"name": "holler", "version": "0.1.0"},
		}, false, nil
	case "tools/list":
		return map[string]interface{}{"tools": toolDefinitions()}, false, nil
	case "tools/call":
		var params struct {
			Name      string          `json:"name"`
			Arguments json.RawMessage `json:"arguments"`
		}
		cleanedParams, err := stripReservedToolMetadata(req.Params)
		if err != nil {
			return nil, false, err
		}
		if err := decodeStrict(cleanedParams, &params); err != nil {
			return nil, false, err
		}
		value, err := s.callTool(ctx, params.Name, params.Arguments)
		if err != nil {
			return nil, false, err
		}
		return toolResult(value), false, nil
	case "ping":
		return map[string]interface{}{}, false, nil
	default:
		if strings.HasPrefix(req.Method, "notifications/") {
			return nil, true, nil
		}
		return nil, false, fmt.Errorf("unsupported MCP method: %s", req.Method)
	}
}

func (s *Server) callTool(ctx context.Context, name string, raw json.RawMessage) (interface{}, error) {
	if len(raw) == 0 {
		raw = json.RawMessage(`{}`)
	}
	var err error
	raw, err = stripReservedToolMetadata(raw)
	if err != nil {
		return nil, err
	}
	switch name {
	case "bus_send":
		var args struct {
			To             string `json:"to"`
			Body           string `json:"body"`
			ThreadID       string `json:"thread_id"`
			ReplyTo        string `json:"reply_to"`
			IdempotencyKey string `json:"idempotency_key"`
		}
		if err := decodeStrict(raw, &args); err != nil {
			return nil, err
		}
		args.To = strings.TrimSpace(args.To)
		args.Body = strings.TrimSpace(args.Body)
		if args.To == "" {
			args.To = s.config.Peer
		}
		if args.To == "" || args.Body == "" || strings.TrimSpace(args.IdempotencyKey) == "" {
			return nil, &bus.ValidationError{Field: "bus_send", Problem: "recipient (or configured peer), body, and idempotency_key are required"}
		}
		if args.ThreadID == "" && args.ReplyTo == "" {
			args.ThreadID = deterministicThreadID(s.config.Actor, args.IdempotencyKey)
		}
		body, err := json.Marshal(map[string]string{"text": args.Body})
		if err != nil {
			return nil, err
		}
		result, err := s.store.Send(ctx, bus.SendRequest{
			IdempotencyKey:  args.IdempotencyKey,
			ProjectID:       s.config.ProjectID,
			ChannelID:       s.config.ChannelID,
			ThreadID:        args.ThreadID,
			FromActor:       s.config.Actor,
			FromRun:         s.config.RunID,
			FromRole:        s.config.Role,
			ToActors:        []string{args.To},
			Type:            "MESSAGE",
			DeliveryRequest: bus.DeliveryWake,
			InReplyTo:       args.ReplyTo,
			Body:            body,
		})
		if err != nil {
			return nil, err
		}
		view := messageView(result.Message, "", time.Time{}, 0)
		view["duplicate"] = result.Duplicate
		if result.NotificationState != "" {
			view["notification_state"] = result.NotificationState
		}
		return view, nil
	case "bus_check_inbox":
		var args struct {
			Limit int `json:"limit"`
		}
		if err := decodeStrict(raw, &args); err != nil {
			return nil, err
		}
		items, err := s.store.CheckInbox(ctx, s.config.Actor, args.Limit)
		if err != nil {
			return nil, err
		}
		return map[string]interface{}{"actor": s.config.Actor, "messages": items}, nil
	case "bus_claim":
		var args struct {
			MessageID    string `json:"message_id"`
			LeaseSeconds int    `json:"lease_seconds"`
		}
		if err := decodeStrict(raw, &args); err != nil {
			return nil, err
		}
		if args.LeaseSeconds == 0 {
			args.LeaseSeconds = 300
		}
		claim, err := s.store.Claim(ctx, s.config.Actor, args.MessageID, time.Duration(args.LeaseSeconds)*time.Second)
		if err != nil {
			return nil, err
		}
		return messageView(claim.Message, claim.LeaseToken, claim.LeaseExpiresAt, claim.Attempt), nil
	case "bus_inbox":
		var args struct {
			Limit        int `json:"limit"`
			LeaseSeconds int `json:"lease_seconds"`
		}
		if err := decodeStrict(raw, &args); err != nil {
			return nil, err
		}
		if args.Limit <= 0 || args.Limit > 100 {
			args.Limit = 20
		}
		if args.LeaseSeconds == 0 {
			args.LeaseSeconds = 300
		}
		items, err := s.store.CheckInbox(ctx, s.config.Actor, args.Limit)
		if err != nil {
			return nil, err
		}
		messages := make([]map[string]interface{}, 0, len(items))
		for _, item := range items {
			if !item.Available {
				continue
			}
			claim, err := s.store.Claim(ctx, s.config.Actor, item.MessageID, time.Duration(args.LeaseSeconds)*time.Second)
			if err != nil {
				if errors.Is(err, bus.ErrNoMessage) {
					continue
				}
				return nil, err
			}
			messages = append(messages, messageView(claim.Message, claim.LeaseToken, claim.LeaseExpiresAt, claim.Attempt))
		}
		return map[string]interface{}{"actor": s.config.Actor, "messages": messages}, nil
	case "bus_ack":
		var args struct {
			MessageID  string `json:"message_id"`
			LeaseToken string `json:"lease_token"`
		}
		if err := decodeStrict(raw, &args); err != nil {
			return nil, err
		}
		if err := s.store.Ack(ctx, s.config.Actor, args.MessageID, args.LeaseToken); err != nil {
			return nil, err
		}
		return map[string]bool{"acked": true}, nil
	case "bus_extend":
		var args struct {
			MessageID    string `json:"message_id"`
			LeaseToken   string `json:"lease_token"`
			LeaseSeconds int    `json:"lease_seconds"`
		}
		if err := decodeStrict(raw, &args); err != nil {
			return nil, err
		}
		if args.LeaseSeconds == 0 {
			args.LeaseSeconds = 300
		}
		return s.store.Extend(ctx, s.config.Actor, args.MessageID, args.LeaseToken, time.Duration(args.LeaseSeconds)*time.Second)
	case "bus_nack":
		var args struct {
			MessageID  string `json:"message_id"`
			LeaseToken string `json:"lease_token"`
			Reason     string `json:"reason"`
			Final      bool   `json:"final"`
		}
		if err := decodeStrict(raw, &args); err != nil {
			return nil, err
		}
		if err := s.store.Nack(ctx, s.config.Actor, args.MessageID, args.LeaseToken, args.Reason, args.Final); err != nil {
			return nil, err
		}
		return map[string]bool{"nacked": true}, nil
	case "bus_status":
		if err := decodeStrict(raw, &struct{}{}); err != nil {
			return nil, err
		}
		items, err := s.store.CheckInbox(ctx, s.config.Actor, 100)
		if err != nil {
			return nil, err
		}
		available := 0
		for _, item := range items {
			if item.Available {
				available++
			}
		}
		return map[string]interface{}{
			"actor": s.config.Actor, "run": s.config.RunID, "peer": s.config.Peer,
			"unread": len(items), "available": available, "counts_truncated": len(items) == 100,
		}, nil
	default:
		return nil, fmt.Errorf("unknown tool: %s", name)
	}
}

func messageView(message bus.Message, leaseToken string, leaseExpires time.Time, attempt int) map[string]interface{} {
	body := string(message.Body)
	var decoded struct {
		Text string `json:"text"`
	}
	if json.Unmarshal(message.Body, &decoded) == nil && decoded.Text != "" {
		body = decoded.Text
	}
	result := map[string]interface{}{
		"message_id": message.ID,
		"thread_id":  message.ThreadID,
		"from":       message.FromActor,
		"to":         message.ToActors,
		"body":       body,
		"reply_to":   message.InReplyTo,
		"created_at": message.CreatedAt,
	}
	if leaseToken != "" {
		result["lease_token"] = leaseToken
		result["lease_expires_at"] = leaseExpires
		result["attempt"] = attempt
	}
	return result
}

func toolResult(value interface{}) map[string]interface{} {
	encoded, _ := json.MarshalIndent(value, "", "  ")
	return map[string]interface{}{
		"content":           []map[string]string{{"type": "text", "text": string(encoded)}},
		"structuredContent": value,
	}
}

func decodeStrict(raw json.RawMessage, target interface{}) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var trailing interface{}
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("multiple JSON values are not allowed")
		}
		return err
	}
	return nil
}

func stripReservedToolMetadata(raw json.RawMessage) (json.RawMessage, error) {
	var object map[string]json.RawMessage
	if err := json.Unmarshal(raw, &object); err != nil {
		return nil, err
	}
	delete(object, "_meta")
	cleaned, err := json.Marshal(object)
	if err != nil {
		return nil, fmt.Errorf("encode tool arguments: %w", err)
	}
	return cleaned, nil
}

func deterministicThreadID(actor, idempotencyKey string) string {
	digest := sha256.Sum256([]byte(actor + "\x00" + idempotencyKey))
	return "thr_" + hex.EncodeToString(digest[:16])
}

func toolDefinitions() []map[string]interface{} {
	object := func(properties map[string]interface{}, required ...string) map[string]interface{} {
		schema := map[string]interface{}{
			"type": "object", "properties": properties, "additionalProperties": false,
		}
		if len(required) > 0 {
			schema["required"] = required
		}
		return schema
	}
	stringProperty := func(description string) map[string]interface{} {
		return map[string]interface{}{"type": "string", "description": description}
	}
	integerProperty := func(description string) map[string]interface{} {
		return map[string]interface{}{"type": "integer", "description": description}
	}
	return []map[string]interface{}{
		{
			"name": "bus_send", "description": "Send a durable message. Sender identity is fixed by the connector session.",
			"inputSchema": object(map[string]interface{}{
				"to": stringProperty("recipient actor"), "body": stringProperty("complete message"),
				"thread_id": stringProperty("thread for a new message"), "reply_to": stringProperty("message being answered"),
				"idempotency_key": stringProperty("stable key for safe retries"),
			}, "body", "idempotency_key"),
			"annotations": map[string]bool{"readOnlyHint": false, "idempotentHint": true},
		},
		{
			"name": "bus_check_inbox", "description": "Inspect unread delivery metadata without claiming it.",
			"inputSchema": object(map[string]interface{}{"limit": integerProperty("maximum messages")}),
			"annotations": map[string]bool{"readOnlyHint": true},
		},
		{
			"name": "bus_claim", "description": "Claim one message under a time-bounded lease and fetch its body.",
			"inputSchema": object(map[string]interface{}{
				"message_id":    stringProperty("specific message; oldest available when omitted"),
				"lease_seconds": integerProperty("lease duration, default 300"),
			}),
			"annotations": map[string]bool{"readOnlyHint": false},
		},
		{
			"name": "bus_inbox", "description": "Compatibility helper: claim all currently available messages; each must be acknowledged explicitly.",
			"inputSchema": object(map[string]interface{}{
				"limit": integerProperty("maximum messages"), "lease_seconds": integerProperty("lease duration, default 300"),
			}),
			"annotations": map[string]bool{"readOnlyHint": false},
		},
		{
			"name": "bus_ack", "description": "Acknowledge a processed message using its active lease token.",
			"inputSchema": object(map[string]interface{}{
				"message_id": stringProperty("message id"), "lease_token": stringProperty("active lease token"),
			}, "message_id", "lease_token"),
			"annotations": map[string]bool{"readOnlyHint": false, "idempotentHint": true},
		},
		{
			"name": "bus_extend", "description": "Renew an active message lease before long-running processing expires.",
			"inputSchema": object(map[string]interface{}{
				"message_id": stringProperty("message id"), "lease_token": stringProperty("active lease token"),
				"lease_seconds": integerProperty("new lease duration from now, default 300"),
			}, "message_id", "lease_token"),
			"annotations": map[string]bool{"readOnlyHint": false, "idempotentHint": true},
		},
		{
			"name": "bus_nack", "description": "Release or dead-letter a claimed message.",
			"inputSchema": object(map[string]interface{}{
				"message_id": stringProperty("message id"), "lease_token": stringProperty("active lease token"),
				"reason": stringProperty("failure reason"), "final": map[string]interface{}{"type": "boolean"},
			}, "message_id", "lease_token"),
			"annotations": map[string]bool{"readOnlyHint": false, "idempotentHint": false},
		},
		{
			"name": "bus_status", "description": "Show the connector-bound actor and inbox counts.",
			"inputSchema": object(map[string]interface{}{}),
			"annotations": map[string]bool{"readOnlyHint": true},
		},
	}
}

// ToolSurfaceHash identifies the complete MCP name, schema, description, and
// annotation surface. Connector authorization is tied to this value so a
// package cannot silently inherit approval after adding or changing tools.
func ToolSurfaceHash() string {
	payload, err := json.Marshal(toolDefinitions())
	if err != nil {
		panic(err)
	}
	digest := sha256.Sum256(payload)
	return "sha256:" + hex.EncodeToString(digest[:])
}

// ToolNames returns the stable tool names in advertised order.
func ToolNames() []string {
	definitions := toolDefinitions()
	names := make([]string, 0, len(definitions))
	for _, definition := range definitions {
		name, _ := definition["name"].(string)
		names = append(names, name)
	}
	return names
}
