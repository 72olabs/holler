package mcp

import (
	"context"
	"encoding/json"
	"github.com/72olabs/holler/internal/bus"
)

// This surface cannot select a capability, sender, recipient or channel. It
// consumes only the bound actor's delivery; other writes use the write bridge.
func (s *Server) consumeManaged(ctx context.Context, name string, raw json.RawMessage) (interface{}, error) {
	bridge, ok := s.store.(capabilityStore)
	if !ok {
		return nil, bus.ErrChannelCapability
	}
	if name == "holler_channel_inbox" {
		var a struct {
			Limit int `json:"limit"`
		}
		if err := decodeStrict(raw, &a); err != nil {
			return nil, err
		}
		args, _ := json.Marshal(a)
		return bridge.InvokeReadCapability(ctx, "channel.inbox", args)
	}
	if name == "holler_channel_claim" {
		var a struct {
			ID string `json:"message_id"`
		}
		if err := decodeStrict(raw, &a); err != nil {
			return nil, err
		}
		if a.ID == "" {
			return nil, bus.ErrInvalid
		}
		args, _ := json.Marshal(a)
		return bridge.InvokeWriteCapability(ctx, "channel.claim", args)
	}
	var id, token, reason string
	var seconds int
	action := ""
	switch name {
	case "holler_channel_ack":
		var a struct {
			ID    string `json:"message_id"`
			Token string `json:"lease_token"`
		}
		if err := decodeStrict(raw, &a); err != nil {
			return nil, err
		}
		id, token, action = a.ID, a.Token, "ack"
	case "holler_channel_extend":
		var a struct {
			ID      string `json:"message_id"`
			Token   string `json:"lease_token"`
			Seconds int    `json:"lease_seconds"`
		}
		if err := decodeStrict(raw, &a); err != nil {
			return nil, err
		}
		id, token, seconds, action = a.ID, a.Token, a.Seconds, "extend"
	case "holler_channel_nack":
		var a struct {
			ID     string `json:"message_id"`
			Token  string `json:"lease_token"`
			Reason string `json:"reason"`
			Final  bool   `json:"final"`
		}
		if err := decodeStrict(raw, &a); err != nil {
			return nil, err
		}
		id, token, reason, action = a.ID, a.Token, a.Reason, "nack"
		if a.Final {
			action = "dead_letter"
		}
	default:
		return nil, bus.ErrChannelCapability
	}
	if id == "" || token == "" {
		return nil, bus.ErrInvalid
	}
	args, _ := json.Marshal(map[string]interface{}{"message_id": id, "lease_token": token, "action": action, "reason": reason, "lease_seconds": seconds})
	return bridge.InvokeWriteCapability(ctx, "channel.delivery", args)
}

func channelToolDefinitions() []map[string]interface{} {
	str := map[string]interface{}{"type": "string"}
	integer := map[string]interface{}{"type": "integer", "minimum": 1, "maximum": 86400}
	tool := func(name, description string, props map[string]interface{}, required []string, read, idempotent bool) map[string]interface{} {
		return map[string]interface{}{"name": name, "description": description, "inputSchema": map[string]interface{}{"type": "object", "properties": props, "additionalProperties": false, "required": required}, "annotations": map[string]bool{"readOnlyHint": read, "idempotentHint": idempotent, "destructiveHint": false}}
	}
	return []map[string]interface{}{
		tool("holler_channel_inbox", "Inspect your own managed deliveries without claiming or marking read. Requires managed conversations to be enabled.", map[string]interface{}{"limit": map[string]interface{}{"type": "integer", "minimum": 1, "maximum": 100}}, []string{}, true, true),
		tool("holler_channel_claim", "Claim one managed message addressed to your bound identity for five minutes. Process it, then use holler_channel_ack. Observers cannot claim agent deliveries.", map[string]interface{}{"message_id": str}, []string{"message_id"}, false, false),
		tool("holler_channel_ack", "Acknowledge your processed managed delivery using its active lease token. Does not answer a response request or mark human history read.", map[string]interface{}{"message_id": str, "lease_token": str}, []string{"message_id", "lease_token"}, false, true),
		tool("holler_channel_extend", "Extend your own active managed delivery lease before it expires.", map[string]interface{}{"message_id": str, "lease_token": str, "lease_seconds": integer}, []string{"message_id", "lease_token", "lease_seconds"}, false, true),
		tool("holler_channel_nack", "Release your managed delivery for retry, or mark it dead-lettered with final=true. Reason stays recipient-private.", map[string]interface{}{"message_id": str, "lease_token": str, "reason": str, "final": map[string]interface{}{"type": "boolean"}}, []string{"message_id", "lease_token"}, false, false),
	}
}
