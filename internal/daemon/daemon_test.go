package daemon_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/72olabs/holler/internal/api"
	"github.com/72olabs/holler/internal/bus"
	"github.com/72olabs/holler/internal/daemon"
)

func TestDaemonOwnsDatabaseAndSecuresSocket(t *testing.T) {
	socketDirectory, err := os.MkdirTemp("/tmp", "holler-daemon-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(socketDirectory) })
	socket := filepath.Join(socketDirectory, "holler.sock")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	var ready bytes.Buffer
	go func() {
		done <- daemon.Run(ctx, daemon.Config{
			DatabasePath: filepath.Join(t.TempDir(), "holler.sqlite3"), SocketPath: socket,
		}, &ready)
	}()
	waitForSocket(t, socket)
	info, err := os.Stat(socket)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("socket mode = %o, want 600", info.Mode().Perm())
	}
	client, err := api.Dial(context.Background(), socket, api.Identity{
		Actor: "operator", RunID: "test-run", Client: "test",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Ping(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Keep an established client connected while shutting down. The daemon must
	// close active connections rather than waiting forever for clients to leave.
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("daemon: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("daemon did not stop")
	}
	_ = client.Close()
	if _, err := os.Stat(socket); !os.IsNotExist(err) {
		t.Fatalf("socket remains after shutdown: %v", err)
	}
}

func TestDaemonRejectsInvalidSocketBeforeOpeningDatabase(t *testing.T) {
	directory := t.TempDir()
	database := filepath.Join(directory, "holler.sqlite3")
	socket := filepath.Join(directory, strings.Repeat("x", 180)+".sock")
	err := daemon.Run(context.Background(), daemon.Config{
		DatabasePath: database,
		SocketPath:   socket,
	}, nil)
	if err == nil {
		t.Fatal("daemon accepted an overlong Unix socket path")
	}
	if _, statErr := os.Stat(database); !os.IsNotExist(statErr) {
		t.Fatalf("database was opened before socket validation: %v", statErr)
	}
}

func TestDaemonRecoversAStaleSocketButRejectsALiveOne(t *testing.T) {
	directory, err := os.MkdirTemp("/tmp", "ab-stale-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	socket := filepath.Join(directory, "holler.sock")
	stale, err := net.ListenUnix("unix", &net.UnixAddr{Name: socket, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	stale.SetUnlinkOnClose(false)
	if err := stale.Close(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- daemon.Run(ctx, daemon.Config{DatabasePath: filepath.Join(directory, "holler.sqlite3"), SocketPath: socket}, nil)
	}()
	deadline := time.Now().Add(2 * time.Second)
	for {
		probe, dialErr := net.DialTimeout("unix", socket, 50*time.Millisecond)
		if dialErr == nil {
			_ = probe.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("daemon did not replace stale socket: %v", dialErr)
		}
		time.Sleep(10 * time.Millisecond)
	}
	client, err := api.Dial(context.Background(), socket, api.Identity{
		Actor: "operator", RunID: "stale-socket-test", Client: "test",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Ping(context.Background()); err != nil {
		t.Fatal(err)
	}
	_ = client.Close()
	if err := daemon.Run(context.Background(), daemon.Config{DatabasePath: filepath.Join(directory, "other.sqlite3"), SocketPath: socket}, nil); err == nil || !strings.Contains(err.Error(), "already listening") {
		t.Fatalf("second daemon error = %v", err)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestDaemonDeliversClaudeAttentionThroughDurableOutbox(t *testing.T) {
	directory, err := os.MkdirTemp("/tmp", "ab-claude-attention-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	socket := filepath.Join(directory, "holler.sock")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- daemon.Run(ctx, daemon.Config{
			DatabasePath: filepath.Join(directory, "holler.sqlite3"), SocketPath: socket,
		}, nil)
	}()
	waitForSocket(t, socket)
	t.Cleanup(func() {
		cancel()
		select {
		case daemonErr := <-done:
			if daemonErr != nil {
				t.Errorf("daemon: %v", daemonErr)
			}
		case <-time.After(2 * time.Second):
			t.Error("daemon did not stop")
		}
	})

	claude, err := api.Dial(ctx, socket, api.Identity{Actor: "claude", RunID: "claude-run", Client: "test-monitor"})
	if err != nil {
		t.Fatal(err)
	}
	defer claude.Close()
	if _, err := claude.RegisterSession(ctx, bus.RegistrationRequest{
		Actor: "claude", RunID: "claude-run", Harness: "claude", SessionID: "session-1",
		AttentionMode: "hook-long-poll", DeliveryHandle: "session-1", ProjectID: "experiment", Lease: time.Hour,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := claude.MonitorAttach(ctx, "claude", "claude-run", "session-1", "hook-long-poll", 5*time.Minute); err != nil {
		t.Fatal(err)
	}

	notices := make(chan bus.AttentionNotice, 1)
	waitErrors := make(chan error, 1)
	go func() {
		notice, waitErr := claude.WaitAttention(ctx, "claude", "claude-run", "session-1", "hook-long-poll", 5*time.Second)
		notices <- notice
		waitErrors <- waitErr
	}()
	time.Sleep(20 * time.Millisecond)
	sender, err := api.Dial(ctx, socket, api.Identity{Actor: "codex", RunID: "codex-run", Client: "test-sender"})
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close()
	sent, err := sender.Send(ctx, bus.SendRequest{
		IdempotencyKey: "claude-attention", ProjectID: "experiment", ChannelID: "direct",
		ThreadID: "thread-1", ToActors: []string{"claude"}, Type: "QUESTION",
		DeliveryRequest: bus.DeliveryWake, Body: json.RawMessage(`{"text":"secret body"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case notice := <-notices:
		if waitErr := <-waitErrors; waitErr != nil {
			t.Fatal(waitErr)
		}
		if notice.MessageID != sent.Message.ID || notice.FromActor != "codex" || notice.ThreadID != "thread-1" {
			t.Fatalf("notice = %+v", notice)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Claude attention was not delivered")
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		events, listErr := claude.ListEvents(ctx, "experiment", "operational", 0, 100)
		if listErr != nil {
			t.Fatal(listErr)
		}
		accepted := false
		for _, event := range events {
			if event.Kind == "delivery.notification" && event.MessageID == sent.Message.ID && strings.Contains(string(event.Payload), `"result":"accepted"`) && strings.Contains(string(event.Payload), `"detail":"hook-long-poll"`) {
				accepted = true
				break
			}
		}
		if accepted {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("accepted notification event not recorded: %+v", events)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func waitForSocket(t *testing.T, socket string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(socket); err == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("socket did not appear: %s", socket)
}
