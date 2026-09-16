package daemon_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/72olabs/holler/internal/api"
	"github.com/72olabs/holler/internal/daemon"
)

type readyMessageWriter chan []byte

func (w readyMessageWriter) Write(p []byte) (int, error) {
	w <- append([]byte(nil), p...)
	return len(p), nil
}

func TestAgentConversationsDoNotEnableHumanGateway(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		name := "default"
		if enabled {
			name = "agent-conversations"
		}
		t.Run(name, func(t *testing.T) {
			directory, err := os.MkdirTemp("/tmp", "holler-agent-only-")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = os.RemoveAll(directory) })
			ctx, cancel := context.WithCancel(context.Background())
			ready := make(readyMessageWriter, 1)
			done := make(chan error, 1)
			socket := filepath.Join(directory, "holler.sock")
			t.Cleanup(func() {
				cancel()
				select {
				case err := <-done:
					if err != nil {
						t.Errorf("daemon shutdown: %v", err)
					}
				case <-time.After(3 * time.Second):
					t.Error("daemon did not stop")
				}
			})
			go func() {
				done <- daemon.Run(ctx, daemon.Config{
					DatabasePath: filepath.Join(directory, "holler.sqlite3"),
					SocketPath:   socket, Conversations: enabled,
					// The CLI leaves this nil without --human-listen.
					HumanGateway: nil,
				}, ready)
			}()
			select {
			case raw := <-ready:
				var report map[string]any
				if err := json.Unmarshal(raw, &report); err != nil {
					t.Fatal(err)
				}
				// Run reports a bound gateway URL whenever it opens the HTTP
				// listener. Agent-only startup must leave that listener absent.
				if got, present := report["human_gateway"]; !present || got != "" {
					t.Fatalf("unexpected human gateway: %v (present %v)", got, present)
				}
				if report["conversations"] != enabled {
					t.Fatalf("conversation state: %v", report)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("daemon readiness timed out")
			}
			client, err := api.Dial(ctx, socket, api.Identity{Actor: "test-agent", RunID: "agent-only", Client: "test"})
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			if err := client.Ping(ctx); err != nil {
				t.Fatal(err)
			}
		})
	}
}
