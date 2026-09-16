package api_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/72olabs/holler/internal/api"
	"github.com/72olabs/holler/internal/bus"
)

func TestArchivePreflightAccessAcrossTransports(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	_, socket := startServer(t, ctx, cancel)
	sender := dial(t, socket, "sender", "sender-run")
	victim := dial(t, socket, "victim", "victim-run")
	stranger := dial(t, socket, "stranger", "stranger-run")
	operator := dial(t, socket, "operator", "operator-run")
	const secret = "private-preview-not-for-stranger"
	_, err := sender.Send(ctx, bus.SendRequest{
		IdempotencyKey: "private-preview", ProjectID: "test", ChannelID: "direct",
		ToActors: []string{"victim"}, Type: "MESSAGE", Body: json.RawMessage(`{"text":"` + secret + `"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, bridge := range []bool{false, true} {
		call := func(client *api.Client, actor string) (bus.ActorArchivePreflight, error) {
			if !bridge {
				return client.ArchivePreflight(ctx, actor, 10)
			}
			args, _ := json.Marshal(map[string]interface{}{"actor": actor, "limit": 10})
			raw, err := client.InvokeReadCapability(ctx, "actor.archive_preflight", args)
			var result bus.ActorArchivePreflight
			if err == nil {
				err = json.Unmarshal(raw, &result)
			}
			return result, err
		}
		for _, target := range []string{"victim", "does-not-exist", " operator "} {
			result, err := call(stranger, target)
			if !errors.Is(err, bus.ErrNotFound) || len(result.Unread) != 0 || result.Actor != "" {
				t.Fatalf("bridge=%v unauthorized target=%q: result=%+v err=%v", bridge, target, result, err)
			}
			if strings.Contains(err.Error(), secret) || strings.Contains(err.Error(), target) {
				t.Fatalf("unauthorized error leaks target or body: %v", err)
			}
		}
		for _, client := range []*api.Client{victim, operator} {
			result, err := call(client, " victim ")
			if err != nil || len(result.Unread) != 1 || !strings.Contains(result.Unread[0].BodyPreview, secret) {
				t.Fatalf("bridge=%v authorized: result=%+v err=%v", bridge, result, err)
			}
		}
	}
}
