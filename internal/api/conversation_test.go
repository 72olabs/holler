package api_test

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/72olabs/holler/internal/api"
	"github.com/72olabs/holler/internal/bus"
	"testing"
)

func TestConversationCapabilitiesIdentityAndLanes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	_, socket := startServer(t, ctx, cancel, api.WithConversations())
	a := dial(t, socket, "a", "a-run")
	b := dial(t, socket, "b", "b-run")
	stranger := dial(t, socket, "stranger", "s-run")
	raw := json.RawMessage(`{"project_id":"test","kind":"dm","participants":["a","b"],"idempotency_key":"create"}`)
	if _, err := a.InvokeReadCapability(ctx, "channel.create", raw); err == nil {
		t.Fatal("write through read lane")
	}
	response, err := a.InvokeWriteCapability(ctx, "channel.create", raw)
	if err != nil {
		t.Fatal(err)
	}
	var c bus.Channel
	if err := json.Unmarshal(response, &c); err != nil {
		t.Fatal(err)
	}
	post := bus.ChannelPost{ChannelID: c.ID, ExpectedRevision: c.Revision, IdempotencyKey: "message", Body: json.RawMessage(`"private"`)}
	raw, _ = json.Marshal(post)
	response, err = a.InvokeWriteCapability(ctx, "channel.post", raw)
	if err != nil {
		t.Fatal(err)
	}
	var sent bus.ChannelPostResult
	if err := json.Unmarshal(response, &sent); err != nil {
		t.Fatal(err)
	}
	if sent.Message.FromActor != "a" || sent.Message.FromRun != "a-run" {
		t.Fatalf("principal: %+v", sent)
	}
	raw, _ = json.Marshal(map[string]string{"message_id": sent.Message.ID})
	if _, err := stranger.InvokeReadCapability(ctx, "channel.message", raw); !errors.Is(err, bus.ErrChannelDenied) {
		t.Fatalf("stranger: %v", err)
	}
	if _, err := b.InvokeReadCapability(ctx, "channel.message", raw); err != nil {
		t.Fatal(err)
	}
	if _, err := a.InvokeWriteCapability(ctx, "channel.post", json.RawMessage(`{"gateway":true,"actor":"human:owner"}`)); err == nil {
		t.Fatal("wire principal fields accepted")
	}
	items, err := b.CheckInbox(ctx, "b", 10)
	if err != nil || len(items) != 0 {
		t.Fatalf("legacy leak: %v %v", items, err)
	}
}

func TestConversationCapabilitiesRequireEnablement(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	_, socket := startServer(t, ctx, cancel)
	a := dial(t, socket, "a", "r")
	caps, err := a.ListCapabilities(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range caps {
		if c.Name == "channel.create" {
			t.Fatal("unenabled capability")
		}
	}
}
