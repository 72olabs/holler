package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/72olabs/holler/internal/bus"
	"testing"
)

type consumeFake struct {
	Store
	lane, name string
	args       json.RawMessage
	err        error
}

func (f *consumeFake) ListCapabilities(context.Context) ([]bus.CapabilityDescriptor, error) {
	return nil, nil
}
func (f *consumeFake) InvokeReadCapability(_ context.Context, name string, args json.RawMessage) (json.RawMessage, error) {
	f.lane, f.name, f.args = "read", name, args
	return json.RawMessage(`{}`), f.err
}
func (f *consumeFake) InvokeWriteCapability(_ context.Context, name string, args json.RawMessage) (json.RawMessage, error) {
	f.lane, f.name, f.args = "write", name, args
	return json.RawMessage(`{}`), f.err
}

func TestManagedConsumeToolsHaveFixedLanesAndNoPrincipalSelectors(t *testing.T) {
	for _, tc := range []struct{ name, raw, cap, lane, action string }{
		{"holler_channel_inbox", `{"limit":10}`, "channel.inbox", "read", ""},
		{"holler_channel_claim", `{"message_id":"m"}`, "channel.claim", "write", ""},
		{"holler_channel_ack", `{"message_id":"m","lease_token":"t"}`, "channel.delivery", "write", "ack"},
		{"holler_channel_extend", `{"message_id":"m","lease_token":"t","lease_seconds":60}`, "channel.delivery", "write", "extend"},
		{"holler_channel_nack", `{"message_id":"m","lease_token":"t"}`, "channel.delivery", "write", "nack"},
		{"holler_channel_nack", `{"message_id":"m","lease_token":"t","final":true}`, "channel.delivery", "write", "dead_letter"},
	} {
		t.Run(tc.name+tc.action, func(t *testing.T) {
			f := &consumeFake{}
			s := &Server{store: f}
			if _, err := s.consumeManaged(context.Background(), tc.name, json.RawMessage(tc.raw)); err != nil {
				t.Fatal(err)
			}
			if f.name != tc.cap || f.lane != tc.lane {
				t.Fatalf("route %s %s", f.name, f.lane)
			}
			var got map[string]interface{}
			json.Unmarshal(f.args, &got)
			if tc.action != "" && got["action"] != tc.action {
				t.Fatal(got)
			}
			for _, field := range []string{"actor", "run_id", "gateway", "capability", "channel_id"} {
				if _, ok := got[field]; ok {
					t.Fatal("selector passed through")
				}
				f.name = ""
				raw := tc.raw[:len(tc.raw)-1] + `,"` + field + `":"forged"}`
				if _, err := s.consumeManaged(context.Background(), tc.name, json.RawMessage(raw)); err == nil || f.name != "" {
					t.Fatalf("accepted %s", field)
				}
			}
		})
	}
}

func TestManagedConsumeMissingArgumentsAndDisabledDaemon(t *testing.T) {
	f := &consumeFake{}
	s := &Server{store: f}
	for _, tc := range []struct{ name, raw string }{{"holler_channel_claim", `{}`}, {"holler_channel_ack", `{"message_id":"m"}`}, {"holler_channel_nack", `{"lease_token":"t"}`}} {
		if _, err := s.consumeManaged(context.Background(), tc.name, json.RawMessage(tc.raw)); !errors.Is(err, bus.ErrInvalid) {
			t.Fatalf("missing args: %v", err)
		}
	}
	f.err = bus.ErrChannelCapability
	if _, err := s.consumeManaged(context.Background(), "holler_channel_claim", json.RawMessage(`{"message_id":"m"}`)); !errors.Is(err, bus.ErrChannelCapability) {
		t.Fatal(err)
	}
}
