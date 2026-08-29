package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/72olabs/holler/internal/api"
	"github.com/72olabs/holler/internal/attention"
	"github.com/72olabs/holler/internal/bus"
	"github.com/72olabs/holler/internal/daemon"
	store "github.com/72olabs/holler/internal/store/sqlite"
)

func TestUnixAPIMessageLifecycleAndSessionIdentity(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	db, socket := startServer(t, ctx, cancel)
	_ = db
	alice := dial(t, socket, "alice", "alice-run")
	bob := dial(t, socket, "bob", "bob-run")

	sent, err := alice.Send(ctx, bus.SendRequest{
		IdempotencyKey: "api-1", ProjectID: "experiment", ChannelID: "direct",
		FromActor: "mallory", FromRun: "spoofed-run", ToActors: []string{"bob"},
		Type: "QUESTION", DeliveryRequest: bus.DeliveryWake,
		Body: json.RawMessage(`{"text":"Which policy?"}`),
	})
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	if sent.Message.FromActor != "alice" || sent.Message.FromRun != "alice-run" {
		t.Fatalf("server identity = %s/%s", sent.Message.FromActor, sent.Message.FromRun)
	}
	items, err := bob.CheckInbox(ctx, "bob", 10)
	if err != nil || len(items) != 1 {
		t.Fatalf("inbox = %+v, err = %v", items, err)
	}
	claim, err := bob.Claim(ctx, "bob", sent.Message.ID, time.Minute)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := bob.Ack(ctx, "bob", sent.Message.ID, claim.LeaseToken); err != nil {
		t.Fatalf("ack: %v", err)
	}
	items, err = bob.CheckInbox(ctx, "bob", 10)
	if err != nil || len(items) != 0 {
		t.Fatalf("inbox after ack = %+v, err = %v", items, err)
	}
	if _, err := bob.CheckInbox(ctx, "mallory", 10); err == nil {
		t.Fatal("client accepted actor different from bound session")
	}
}

func TestUnixAPIConnectorRegistrationAndEvents(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	_, socket := startServer(t, ctx, cancel)
	client := dial(t, socket, "codex", "codex-run")
	registration, err := client.RegisterSession(ctx, bus.RegistrationRequest{
		Actor: "codex", RunID: "codex-run", Harness: "codex", SessionID: "thread-1",
		DeliveryHandle: "thread-1", ProjectID: "experiment", Lease: time.Hour,
	})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	if registration.Actor != "codex" || registration.RunID != "codex-run" {
		t.Fatalf("registration = %+v", registration)
	}
	registrations, err := client.LiveRegistrations(ctx, "codex")
	if err != nil || len(registrations) != 1 {
		t.Fatalf("live registrations = %+v, err = %v", registrations, err)
	}
	if registrations[0].DeliveryHandle != "" {
		t.Fatalf("external API exposed delivery handle %q", registrations[0].DeliveryHandle)
	}
	other := dial(t, socket, "other", "other-run")
	defer other.Close()
	if _, err := other.LiveRegistrations(ctx, "codex"); err == nil {
		t.Fatal("cross-actor registration lookup succeeded")
	}
	events, err := client.ListEvents(ctx, "experiment", "operational", 0, 10)
	if err != nil || len(events) != 1 || events[0].Kind != "session.registered" {
		t.Fatalf("events = %+v, err = %v", events, err)
	}
}

func TestUnixAPIWaitAttentionRequiresAndTargetsLiveClaudeSession(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	broker := attention.NewBroker()
	_, socket := startServer(t, ctx, cancel, api.WithAttentionBroker(broker))
	client := dial(t, socket, "claude", "claude-run")
	if _, err := client.WaitAttention(ctx, "claude", "claude-run", "missing", "hook-long-poll", 50*time.Millisecond); !errors.Is(err, bus.ErrRegistrationExpired) {
		t.Fatalf("unregistered wait error = %v", err)
	}
	registration, err := client.RegisterSession(ctx, bus.RegistrationRequest{
		Actor: "claude", RunID: "claude-run", Harness: "claude", SessionID: "session-1",
		AttentionMode: "hook-long-poll", DeliveryHandle: "session-1", ProjectID: "experiment", Lease: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.MonitorAttach(ctx, "claude", "claude-run", "session-1", "hook-long-poll", 5*time.Minute); err != nil {
		t.Fatal(err)
	}
	type waitResult struct {
		notice bus.AttentionNotice
		err    error
	}
	result := make(chan waitResult, 1)
	go func() {
		notice, waitErr := client.WaitAttention(ctx, "claude", "claude-run", "session-1", "hook-long-poll", time.Second)
		result <- waitResult{notice: notice, err: waitErr}
	}()
	message := bus.Message{
		ID: "msg-reference", ThreadID: "thread-1", FromActor: "codex", Type: "QUESTION",
		DeliveryRequest: bus.DeliveryWake, Body: json.RawMessage(`{"text":"must not cross attention path"}`),
	}
	deadline := time.Now().Add(time.Second)
	for {
		adapter, accepted := broker.Notify(registration, message)
		if accepted {
			if adapter != "hook-long-poll" {
				t.Fatalf("adapter = %q", adapter)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("API waiter did not register")
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case delivered := <-result:
		if delivered.err != nil || delivered.notice.MessageID != message.ID || delivered.notice.FromActor != "codex" {
			t.Fatalf("wait result = %+v", delivered)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("attention wait did not return")
	}
}

type cancellationBroker struct {
	waiting  chan struct{}
	canceled chan struct{}
}

func (b *cancellationBroker) Attach(_ string, _ string, _ string, transition func() error) error {
	if transition != nil {
		return transition()
	}
	return nil
}
func (b *cancellationBroker) Cancel(string, string, string, error) {}
func (b *cancellationBroker) Wait(ctx context.Context, _, _, _, _ string) (bus.AttentionNotice, error) {
	close(b.waiting)
	<-ctx.Done()
	close(b.canceled)
	return bus.AttentionNotice{}, ctx.Err()
}

func TestUnixAPIClientDisconnectCancelsParkedWaiter(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	broker := &cancellationBroker{waiting: make(chan struct{}), canceled: make(chan struct{})}
	_, socket := startServer(t, ctx, cancel, api.WithAttentionBroker(broker))
	client := dial(t, socket, "claude", "claude-run")
	defer client.Close()
	if _, err := client.RegisterSession(ctx, bus.RegistrationRequest{
		Actor: "claude", RunID: "claude-run", Harness: "claude", SessionID: "session-1",
		AttentionMode: "hook-long-poll", DeliveryHandle: "session-1", ProjectID: "experiment", Lease: time.Hour,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.MonitorAttach(ctx, "claude", "claude-run", "session-1", "hook-long-poll", 5*time.Minute); err != nil {
		t.Fatal(err)
	}
	waitDone := make(chan error, 1)
	go func() {
		_, err := client.WaitAttention(context.Background(), "claude", "claude-run", "session-1", "hook-long-poll", 20*time.Second)
		waitDone <- err
	}()
	select {
	case <-broker.waiting:
	case <-time.After(time.Second):
		t.Fatal("waiter did not park")
	}
	client.Interrupt()
	select {
	case <-broker.canceled:
	case <-time.After(time.Second):
		t.Fatal("server did not cancel waiter after client disconnect")
	}
	select {
	case err := <-waitDone:
		if err == nil {
			t.Fatal("interrupted wait returned nil")
		}
	case <-time.After(time.Second):
		t.Fatal("client wait remained blocked")
	}
}

func TestUnixAPIWaitAttentionRejectsUnsupportedAdapter(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	broker := attention.NewBroker()
	_, socket := startServer(t, ctx, cancel, api.WithAttentionBroker(broker))
	client := dial(t, socket, "claude", "claude-run")
	defer client.Close()
	if _, err := client.RegisterSession(ctx, bus.RegistrationRequest{
		Actor: "claude", RunID: "claude-run", Harness: "claude", SessionID: "session-1",
		AttentionMode: "hook-long-poll", DeliveryHandle: "session-1", ProjectID: "experiment", Lease: time.Hour,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.WaitAttention(ctx, "claude", "claude-run", "session-1", "unsupported", 50*time.Millisecond); !errors.Is(err, bus.ErrInvalid) {
		t.Fatalf("unsupported adapter wait error = %v", err)
	}
}

func TestMonitorAttachRearmsOnlyOnPresenceTransition(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	broker := attention.NewBroker()
	db, socket := startServer(t, ctx, cancel, api.WithAttentionBroker(broker))
	client := dial(t, socket, "reviewer", "run-1")
	defer client.Close()
	if _, err := client.RegisterSession(ctx, bus.RegistrationRequest{
		Actor: "reviewer", RunID: "run-1", Harness: "claude", AttentionMode: "hook-long-poll",
		SessionID: "session-1", DeliveryHandle: "session-1", ProjectID: "experiment", Lease: time.Hour,
	}); err != nil {
		t.Fatal(err)
	}
	sent, err := db.Send(ctx, bus.SendRequest{
		IdempotencyKey: "presence-transition-rearm", ProjectID: "experiment", ChannelID: "direct",
		FromActor: "sender", FromRun: "sender-run", ToActors: []string{"reviewer"}, Type: "MESSAGE",
		DeliveryRequest: bus.DeliveryWake, Body: json.RawMessage(`{"text":"wake"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	job, err := db.ClaimNotification(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.FinishNotification(ctx, job, bus.NotificationAccepted, "accepted"); err != nil {
		t.Fatal(err)
	}

	if _, err := client.MonitorAttach(ctx, "reviewer", "run-1", "session-1", "hook-long-poll", 5*time.Minute); err != nil {
		t.Fatal(err)
	}
	rearmed, err := db.ClaimNotification(ctx)
	if err != nil || rearmed.Message.ID != sent.Message.ID || rearmed.Attempt != 2 {
		t.Fatalf("first presence rearm = %+v, err=%v", rearmed, err)
	}
	if err := db.FinishNotification(ctx, rearmed, bus.NotificationAccepted, "accepted again"); err != nil {
		t.Fatal(err)
	}
	if _, err := client.MonitorAttach(ctx, "reviewer", "run-1", "session-1", "hook-long-poll", 5*time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ClaimNotification(ctx); !errors.Is(err, bus.ErrNoMessage) {
		t.Fatalf("same-presence heartbeat rearmed notification: %v", err)
	}

	successor := dial(t, socket, "reviewer", "run-2")
	defer successor.Close()
	if _, err := successor.RegisterSession(ctx, bus.RegistrationRequest{
		Actor: "reviewer", RunID: "run-2", Harness: "claude", AttentionMode: "hook-long-poll",
		SessionID: "session-2", DeliveryHandle: "session-2", ProjectID: "experiment", Lease: time.Hour,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := successor.MonitorAttach(ctx, "reviewer", "run-2", "session-2", "hook-long-poll", 5*time.Minute); err != nil {
		t.Fatal(err)
	}
	rearmed, err = db.ClaimNotification(ctx)
	if err != nil || rearmed.Attempt != 3 {
		t.Fatalf("successor presence rearm = %+v, err=%v", rearmed, err)
	}
}

func TestUnixAPIEnqueuesPostCommitNotificationOnce(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	db, socket := startServer(t, ctx, cancel)
	client := dial(t, socket, "sender", "sender-run")
	request := bus.SendRequest{
		IdempotencyKey: "universal-wake", ProjectID: "experiment", ChannelID: "direct",
		ToActors: []string{"recipient"}, Type: "MESSAGE", DeliveryRequest: bus.DeliveryWake,
		Body: json.RawMessage(`{"text":"wake through API"}`),
	}
	result, err := client.Send(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if result.NotificationState != "pending" {
		t.Fatalf("result=%+v", result)
	}
	job, err := db.ClaimNotification(ctx)
	if err != nil || job.Message.ID != result.Message.ID || job.RecipientActor != "recipient" {
		t.Fatalf("job=%+v err=%v", job, err)
	}
	if err := db.FinishNotification(ctx, job, bus.NotificationComplete, ""); err != nil {
		t.Fatal(err)
	}
	result, err = client.Send(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Duplicate {
		t.Fatalf("idempotent replay result=%+v", result)
	}
	if _, err := db.ClaimNotification(ctx); !errors.Is(err, bus.ErrNoMessage) {
		t.Fatalf("duplicate enqueued another notification: %v", err)
	}
}

func TestClientReconnectsAfterDaemonRestart(t *testing.T) {
	directory, err := os.MkdirTemp("/tmp", "ab-reconnect-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	database := filepath.Join(directory, "holler.sqlite3")
	socket := filepath.Join(directory, "holler.sock")
	start := func() (context.CancelFunc, <-chan error) {
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		readyReader, readyWriter := io.Pipe()
		go func() {
			done <- daemon.Run(ctx, daemon.Config{DatabasePath: database, SocketPath: socket}, readyWriter)
			_ = readyWriter.Close()
		}()
		ready := make(chan error, 1)
		go func() {
			var result map[string]interface{}
			ready <- json.NewDecoder(readyReader).Decode(&result)
			_ = readyReader.Close()
		}()
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			select {
			case err := <-done:
				t.Fatalf("daemon exited before readiness: %v", err)
			case err := <-ready:
				if err != nil {
					t.Fatalf("read daemon readiness: %v", err)
				}
				return cancel, done
			default:
			}
			time.Sleep(10 * time.Millisecond)
		}
		cancel()
		t.Fatal("daemon did not become ready")
		return cancel, done
	}
	cancel, done := start()
	client := dial(t, socket, "alice", "run-1")
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	cancel, done = start()
	if err := client.Ping(context.Background()); err != nil {
		t.Fatalf("ping after restart: %v", err)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestClaimResponseHasFrameHeadroomAboveMaximumBody(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	_, socket := startServer(t, ctx, cancel)
	alice := dial(t, socket, "alice", "run-a")
	bob := dial(t, socket, "bob", "run-b")
	body, err := json.Marshal(map[string]string{"text": string(bytes.Repeat([]byte("x"), bus.MaxBodyBytes-2048))})
	if err != nil {
		t.Fatal(err)
	}
	sent, err := alice.Send(ctx, bus.SendRequest{IdempotencyKey: "large", ProjectID: "frames", ChannelID: "direct", ToActors: []string{"bob"}, Type: "MESSAGE", Body: body})
	if err != nil {
		t.Fatalf("send large body: %v", err)
	}
	claim, err := bob.Claim(ctx, "bob", sent.Message.ID, time.Minute)
	if err != nil {
		t.Fatalf("claim large body: %v", err)
	}
	if !bytes.Equal(claim.Message.Body, body) {
		t.Fatal("large claim body changed")
	}
}

func startServer(t *testing.T, ctx context.Context, cancel context.CancelFunc, options ...api.ServerOption) (*store.Store, string) {
	t.Helper()
	directory := t.TempDir()
	db, err := store.Open(ctx, filepath.Join(directory, "holler.sqlite3"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	socketDirectory, err := os.MkdirTemp("/tmp", "holler-api-")
	if err != nil {
		t.Fatalf("socket temp directory: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(socketDirectory) })
	listener, err := net.Listen("unix", filepath.Join(socketDirectory, "holler.sock"))
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- api.NewServer(db, options...).Serve(ctx, listener) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("serve: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Error("server did not stop")
		}
		_ = db.Close()
	})
	return db, listener.Addr().String()
}

func dial(t *testing.T, socket, actor, run string) *api.Client {
	t.Helper()
	client, err := api.Dial(context.Background(), socket, api.Identity{Actor: actor, RunID: run, Client: "test"})
	if err != nil {
		t.Fatalf("dial %s: %v", actor, err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}
