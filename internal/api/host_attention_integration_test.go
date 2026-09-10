package api_test

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/72olabs/holler/internal/api"
	"github.com/72olabs/holler/internal/attention"
	"github.com/72olabs/holler/internal/bus"
	store "github.com/72olabs/holler/internal/store/sqlite"
)

func TestHostAttentionAdmitsExactParentAndProjectsMessageID(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	broker := attention.NewBroker()
	evidence := api.HarnessProcessIdentity{
		Handle:  "hin_host_test",
		Harness: api.ProcessIdentity{PID: 67533, StartFingerprint: "claude-start"},
		Host:    api.ProcessIdentity{PID: 8784, StartFingerprint: "t3-server-start"},
	}
	_, socket := startServer(t, ctx, cancel,
		api.WithAttentionBroker(broker),
		api.WithHarnessProcessResolver(func(_ net.Conn, _ string) (api.HarnessProcessIdentity, error) {
			return evidence, nil
		}),
		api.WithPeerProcessResolver(func(net.Conn) (api.ProcessIdentity, error) {
			return evidence.Host, nil
		}),
		api.WithProcessStartResolver(func(int) (string, error) {
			return evidence.Harness.StartFingerprint, nil
		}),
	)
	client, registration := registerHostInjectedClaude(t, ctx, socket)
	defer client.Close()

	host, err := api.DialHostAttention(ctx, socket, evidence.Harness.PID)
	if err != nil {
		t.Fatal(err)
	}
	if !broker.Attached(registration.Actor, registration.RunID, registration.SessionID) {
		t.Fatal("exact host was admitted without attaching attention")
	}
	if duplicate, duplicateErr := api.DialHostAttention(ctx, socket, evidence.Harness.PID); !errors.Is(duplicateErr, bus.ErrAttentionWaiterBusy) {
		if duplicate != nil {
			duplicate.Close()
		}
		t.Fatalf("duplicate host error = %v", duplicateErr)
	}
	type waitResult struct {
		notice api.HostAttentionNotice
		err    error
	}
	wake := make(chan waitResult, 1)
	go func() {
		notice, waitErr := host.Wait(ctx, time.Second)
		wake <- waitResult{notice: notice, err: waitErr}
	}()
	message := bus.Message{
		ID: "msg_host_reference", ThreadID: "hostile-thread", FromActor: "hostile-sender",
		Type: "hostile-type", DeliveryRequest: bus.DeliveryWake,
		Body: json.RawMessage(`{"text":"must not cross host boundary"}`),
	}
	deadline := time.Now().Add(time.Second)
	for {
		if adapter, accepted := broker.Notify(registration, message); accepted {
			if adapter != "host-injected" {
				t.Fatalf("adapter = %q", adapter)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("host waiter did not park")
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case result := <-wake:
		if result.err != nil || result.notice != (api.HostAttentionNotice{MessageID: message.ID}) {
			t.Fatalf("host wake = %+v", result)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("host wake did not return")
	}

	sent, err := client.Send(ctx, bus.SendRequest{
		IdempotencyKey: "host-ready-receipt", ProjectID: "test", ChannelID: "direct",
		ToActors: []string{registration.Actor},
		Type:     "MESSAGE", DeliveryRequest: bus.DeliveryWake, Body: json.RawMessage(`{"text":"wake"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(sent.DeliveryReceipts) != 1 || sent.DeliveryReceipts[0].AttentionAttachment != "attached" {
		t.Fatalf("host readiness receipt = %+v", sent.DeliveryReceipts)
	}

	if err := host.Close(); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(time.Second)
	for broker.Attached(registration.Actor, registration.RunID, registration.SessionID) {
		if time.Now().After(deadline) {
			t.Fatal("host disconnect left attention attached")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestHostAttentionRejectsEveryProcessExceptExactParent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	evidence := api.HarnessProcessIdentity{
		Handle:  "hin_host_reject",
		Harness: api.ProcessIdentity{PID: 67533, StartFingerprint: "claude-start"},
		Host:    api.ProcessIdentity{PID: 8784, StartFingerprint: "t3-server-start"},
	}
	var peer atomic.Value
	peer.Store(api.ProcessIdentity{PID: 8784, StartFingerprint: "t3-server-start"})
	var harnessStart atomic.Value
	harnessStart.Store(evidence.Harness.StartFingerprint)
	_, socket := startServer(t, ctx, cancel,
		api.WithAttentionBroker(attention.NewBroker()),
		api.WithHarnessProcessResolver(func(_ net.Conn, _ string) (api.HarnessProcessIdentity, error) {
			return evidence, nil
		}),
		api.WithPeerProcessResolver(func(net.Conn) (api.ProcessIdentity, error) {
			return peer.Load().(api.ProcessIdentity), nil
		}),
		api.WithProcessStartResolver(func(int) (string, error) {
			return harnessStart.Load().(string), nil
		}),
	)
	client, _ := registerHostInjectedClaude(t, ctx, socket)
	defer client.Close()
	harnessStart.Store("reused-claude-pid-start")
	if host, err := api.DialHostAttention(ctx, socket, evidence.Harness.PID); err == nil {
		host.Close()
		t.Fatal("reused Claude PID was admitted")
	}
	harnessStart.Store(evidence.Harness.StartFingerprint)

	for _, test := range []struct {
		name string
		peer api.ProcessIdentity
	}{
		{name: "claude", peer: evidence.Harness},
		{name: "model bash", peer: api.ProcessIdentity{PID: 68000, StartFingerprint: "bash-start"}},
		{name: "sibling waiter", peer: api.ProcessIdentity{PID: 8790, StartFingerprint: "waiter-start"}},
		{name: "neighboring codex", peer: api.ProcessIdentity{PID: 34363, StartFingerprint: "codex-start"}},
		{name: "grandparent", peer: api.ProcessIdentity{PID: 8754, StartFingerprint: "t3-app-start"}},
		{name: "reused pid", peer: api.ProcessIdentity{PID: 8784, StartFingerprint: "new-start"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			peer.Store(test.peer)
			host, err := api.DialHostAttention(ctx, socket, evidence.Harness.PID)
			if host != nil {
				host.Close()
			}
			if err == nil || !strings.Contains(err.Error(), "not admitted") {
				t.Fatalf("peer %+v error = %v", test.peer, err)
			}
		})
	}
}

func TestHostAttentionDisconnectCancelsParkedWait(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	evidence := api.HarnessProcessIdentity{
		Handle:  "hin_host_disconnect",
		Harness: api.ProcessIdentity{PID: 67535, StartFingerprint: "claude-disconnect-start"},
		Host:    api.ProcessIdentity{PID: 8784, StartFingerprint: "t3-server-start"},
	}
	broker := &cancellationBroker{waiting: make(chan struct{}), canceled: make(chan struct{})}
	_, socket := startServer(t, ctx, cancel,
		api.WithAttentionBroker(broker),
		api.WithHarnessProcessResolver(func(net.Conn, string) (api.HarnessProcessIdentity, error) {
			return evidence, nil
		}),
		api.WithPeerProcessResolver(func(net.Conn) (api.ProcessIdentity, error) {
			return evidence.Host, nil
		}),
		api.WithProcessStartResolver(func(int) (string, error) {
			return evidence.Harness.StartFingerprint, nil
		}),
	)
	client, _ := registerHostInjectedClaude(t, ctx, socket)
	defer client.Close()
	host, err := api.DialHostAttention(ctx, socket, evidence.Harness.PID)
	if err != nil {
		t.Fatal(err)
	}
	waitDone := make(chan error, 1)
	go func() {
		_, waitErr := host.Wait(context.Background(), 20*time.Second)
		waitDone <- waitErr
	}()
	select {
	case <-broker.waiting:
	case <-time.After(time.Second):
		t.Fatal("host waiter did not park")
	}
	if err := host.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-broker.canceled:
	case <-time.After(time.Second):
		t.Fatal("host disconnect did not cancel parked broker wait")
	}
	select {
	case err := <-waitDone:
		if err == nil {
			t.Fatal("disconnected host wait returned nil")
		}
	case <-time.After(time.Second):
		t.Fatal("disconnected host client remained blocked")
	}
}

func TestLiveHostProcessRebindFallsBackToStartupOnly(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	evidence := api.HarnessProcessIdentity{
		Handle:  "hin_host_live_rebind",
		Harness: api.ProcessIdentity{PID: 67537, StartFingerprint: "claude-live-start"},
		Host:    api.ProcessIdentity{PID: 8784, StartFingerprint: "t3-server-start"},
	}
	_, socket := startServer(t, ctx, cancel,
		api.WithAttentionBroker(attention.NewBroker()),
		api.WithHarnessProcessResolver(func(net.Conn, string) (api.HarnessProcessIdentity, error) {
			return evidence, nil
		}),
		api.WithPeerProcessResolver(func(net.Conn) (api.ProcessIdentity, error) {
			return evidence.Host, nil
		}),
		api.WithProcessStartResolver(func(int) (string, error) {
			return evidence.Harness.StartFingerprint, nil
		}),
	)
	client, _ := registerHostInjectedClaude(t, ctx, socket)
	defer client.Close()
	host, err := api.DialHostAttention(ctx, socket, evidence.Harness.PID)
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	replacement, err := client.RegisterSession(ctx, bus.RegistrationRequest{
		Actor: client.Identity().Actor, RunID: client.Identity().RunID,
		Harness: "claude", AttentionMode: "host-injected", SessionID: "session-2",
		ProjectID: "test", Lease: time.Hour,
	})
	if err != nil {
		t.Fatalf("live rebind should degrade, not fail: %v", err)
	}
	if replacement.AttentionMode != "startup-only" {
		t.Fatalf("live rebind mode = %q", replacement.AttentionMode)
	}
	conditions, err := client.ListConditions(ctx, false, 10)
	if err != nil || len(conditions) != 1 || conditions[0].ReasonCode != "host_attention_binding_in_use" {
		t.Fatalf("host fallback conditions = %+v, err=%v", conditions, err)
	}
}

func TestCrossSessionLaunchTagRaceCannotRedirectHostBinding(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	hostProcess := api.ProcessIdentity{PID: 8784, StartFingerprint: "t3-server-start"}
	claudeA := api.HarnessProcessIdentity{
		Handle: "hin_claude_a", Harness: api.ProcessIdentity{PID: 67540, StartFingerprint: "claude-a-start"}, Host: hostProcess,
	}
	claudeB := api.HarnessProcessIdentity{
		Handle: "hin_claude_b", Harness: api.ProcessIdentity{PID: 67541, StartFingerprint: "claude-b-start"}, Host: hostProcess,
	}
	var resolved atomic.Value
	resolved.Store(claudeA)
	_, socket := startServer(t, ctx, cancel,
		api.WithAttentionBroker(attention.NewBroker()),
		api.WithHarnessProcessResolver(func(net.Conn, string) (api.HarnessProcessIdentity, error) {
			return resolved.Load().(api.HarnessProcessIdentity), nil
		}),
		api.WithPeerProcessResolver(func(net.Conn) (api.ProcessIdentity, error) {
			return hostProcess, nil
		}),
		api.WithProcessStartResolver(func(pid int) (string, error) {
			switch pid {
			case claudeA.Harness.PID:
				return claudeA.Harness.StartFingerprint, nil
			case claudeB.Harness.PID:
				return claudeB.Harness.StartFingerprint, nil
			default:
				return "", errors.New("unknown process")
			}
		}),
	)
	attacker, err := api.Dial(ctx, socket, api.Identity{
		Actor: "claude-a", RunID: "run-a", Client: "model-descendant", Harness: "claude",
		NameMode: bus.NameModeAllocate, ProjectID: "test",
		ContinuityHandles: []string{"process:claude:run-a", "launch:claude:tag-b"},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer attacker.Close()
	if _, err := attacker.RegisterSession(ctx, bus.RegistrationRequest{
		Actor: attacker.Identity().Actor, RunID: attacker.Identity().RunID,
		Harness: "claude", AttentionMode: "host-injected", SessionID: "session-a",
		ProjectID: "test", Lease: time.Hour,
	}); err != nil {
		t.Fatal(err)
	}

	resolved.Store(claudeB)
	realB, err := api.Dial(ctx, socket, api.Identity{
		Actor: "claude-b", RunID: "run-b", Client: "session-start", Harness: "claude",
		NameMode: bus.NameModeAllocate, ProjectID: "test",
		ContinuityHandles: []string{"process:claude:run-b", "launch:claude:tag-b"},
	})
	if err != nil {
		t.Fatalf("real B hello: %v", err)
	}
	defer realB.Close()
	registrationB, err := realB.RegisterSession(ctx, bus.RegistrationRequest{
		Actor: realB.Identity().Actor, RunID: realB.Identity().RunID,
		Harness: "claude", AttentionMode: "host-injected", SessionID: "session-b",
		ProjectID: "test", Lease: time.Hour,
	})
	if err != nil {
		t.Fatalf("real B registration: %v", err)
	}
	hostB, err := api.DialHostAttention(ctx, socket, claudeB.Harness.PID)
	if err != nil {
		t.Fatalf("B host did not attach by B's process identity: %v", err)
	}
	if registrationB.Actor == attacker.Identity().Actor || registrationB.SessionID != "session-b" {
		hostB.Close()
		t.Fatalf("B registration redirected to A: %+v", registrationB)
	}
	hostB.Close()
	hostA, err := api.DialHostAttention(ctx, socket, claudeA.Harness.PID)
	if err != nil {
		t.Fatalf("A's daemon-derived process binding was lost: %v", err)
	}
	hostA.Close()
}

func TestHostInjectedRegistrationFailsClosedWithoutProcessProof(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	_, socket := startServer(t, ctx, cancel, api.WithHarnessInstanceResolver(func(net.Conn, string) (string, error) {
		return "hin_hash_only", nil
	}))
	client, err := api.Dial(ctx, socket, api.Identity{
		Actor: "reviewer", RunID: "reviewer-run", Client: "mcp", Harness: "claude",
		NameMode: bus.NameModeAllocate, ProjectID: "test",
		ContinuityHandles: []string{"process:claude:reviewer-run", "launch:claude:t3-test"},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	registration, err := client.RegisterSession(ctx, bus.RegistrationRequest{
		Actor: client.Identity().Actor, RunID: client.Identity().RunID,
		Harness: "claude", AttentionMode: "host-injected", SessionID: "session-1",
		DeliveryHandle: "session-1", ProjectID: "test", Lease: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	if registration.AttentionMode != "startup-only" {
		t.Fatalf("registration mode = %q, want startup-only", registration.AttentionMode)
	}
	conditions, err := client.ListConditions(ctx, false, 10)
	if err != nil || len(conditions) != 1 || conditions[0].ReasonCode != "host_attention_process_proof_unavailable" {
		t.Fatalf("process-proof fallback conditions = %+v, err=%v", conditions, err)
	}
}

func TestHostAttentionBindingSurvivesDaemonRestart(t *testing.T) {
	evidence := api.HarnessProcessIdentity{
		Handle:  "hin_host_restart",
		Harness: api.ProcessIdentity{PID: 67534, StartFingerprint: "claude-restart-start"},
		Host:    api.ProcessIdentity{PID: 8784, StartFingerprint: "t3-server-start"},
	}
	databasePath := filepath.Join(t.TempDir(), "holler.sqlite3")
	socketDirectory, err := os.MkdirTemp("/tmp", "holler-host-restart-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(socketDirectory)
	socketPath := filepath.Join(socketDirectory, "holler.sock")

	firstBroker := attention.NewBroker()
	first := startRestartableHostServer(t, databasePath, socketPath, firstBroker, evidence)
	client, registration := registerHostInjectedClaude(t, context.Background(), socketPath)
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	first.stop(t)

	secondBroker := attention.NewBroker()
	second := startRestartableHostServer(t, databasePath, socketPath, secondBroker, evidence)
	defer second.stop(t)
	host, err := api.DialHostAttention(context.Background(), socketPath, evidence.Harness.PID)
	if err != nil {
		t.Fatalf("reattach after daemon restart: %v", err)
	}
	defer host.Close()
	wake := make(chan api.HostAttentionNotice, 1)
	go func() {
		notice, _ := host.Wait(context.Background(), time.Second)
		wake <- notice
	}()
	message := bus.Message{ID: "msg_after_restart", DeliveryRequest: bus.DeliveryWake}
	deadline := time.Now().Add(time.Second)
	for {
		if _, accepted := secondBroker.Notify(registration, message); accepted {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("restarted host waiter did not park")
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case notice := <-wake:
		if notice.MessageID != message.ID {
			t.Fatalf("restart notice = %+v", notice)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("restart wake did not return")
	}
}

func TestHostAttentionReconnectRearmsAcceptedNotice(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	broker := attention.NewBroker()
	evidence := api.HarnessProcessIdentity{
		Handle:  "hin_host_rearm",
		Harness: api.ProcessIdentity{PID: 67536, StartFingerprint: "claude-rearm-start"},
		Host:    api.ProcessIdentity{PID: 8784, StartFingerprint: "t3-server-start"},
	}
	db, socket := startServer(t, ctx, cancel,
		api.WithAttentionBroker(broker),
		api.WithHarnessProcessResolver(func(net.Conn, string) (api.HarnessProcessIdentity, error) {
			return evidence, nil
		}),
		api.WithPeerProcessResolver(func(net.Conn) (api.ProcessIdentity, error) {
			return evidence.Host, nil
		}),
		api.WithProcessStartResolver(func(int) (string, error) {
			return evidence.Harness.StartFingerprint, nil
		}),
	)
	client, registration := registerHostInjectedClaude(t, ctx, socket)
	defer client.Close()
	sent, err := db.Send(ctx, bus.SendRequest{
		IdempotencyKey: "host-rearm", ProjectID: "test", ChannelID: "direct",
		FromActor: "sender", FromRun: "sender-run", ToActors: []string{registration.Actor},
		Type: "MESSAGE", DeliveryRequest: bus.DeliveryWake, Body: json.RawMessage(`{"text":"wake"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	job, err := db.ClaimNotification(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.FinishNotification(ctx, job, bus.NotificationAccepted, "host connection died after acceptance"); err != nil {
		t.Fatal(err)
	}

	host, err := api.DialHostAttention(ctx, socket, evidence.Harness.PID)
	if err != nil {
		t.Fatal(err)
	}
	rearmed, err := db.ClaimNotification(ctx)
	if err != nil || rearmed.Message.ID != sent.Message.ID || rearmed.Attempt != 2 {
		host.Close()
		t.Fatalf("first host attach rearm = %+v, err=%v", rearmed, err)
	}
	if err := db.FinishNotification(ctx, rearmed, bus.NotificationAccepted, "accepted again"); err != nil {
		host.Close()
		t.Fatal(err)
	}
	if err := host.Close(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for broker.Attached(registration.Actor, registration.RunID, registration.SessionID) {
		if time.Now().After(deadline) {
			t.Fatal("host did not detach before reconnect")
		}
		time.Sleep(time.Millisecond)
	}
	host, err = api.DialHostAttention(ctx, socket, evidence.Harness.PID)
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	rearmed, err = db.ClaimNotification(ctx)
	if err != nil || rearmed.Message.ID != sent.Message.ID || rearmed.Attempt != 3 {
		t.Fatalf("host reconnect rearm = %+v, err=%v", rearmed, err)
	}
}

type restartableHostServer struct {
	cancel context.CancelFunc
	done   chan error
	db     *store.Store
}

func startRestartableHostServer(t *testing.T, databasePath, socketPath string, broker *attention.Broker, evidence api.HarnessProcessIdentity) *restartableHostServer {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	db, err := store.Open(ctx, databasePath)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	_ = os.Remove(socketPath)
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		db.Close()
		cancel()
		t.Fatal(err)
	}
	done := make(chan error, 1)
	server := api.NewServer(db,
		api.WithAttentionBroker(broker),
		api.WithHarnessProcessResolver(func(net.Conn, string) (api.HarnessProcessIdentity, error) {
			return evidence, nil
		}),
		api.WithPeerProcessResolver(func(net.Conn) (api.ProcessIdentity, error) {
			return evidence.Host, nil
		}),
		api.WithProcessStartResolver(func(int) (string, error) {
			return evidence.Harness.StartFingerprint, nil
		}),
	)
	go func() { done <- server.Serve(ctx, listener) }()
	return &restartableHostServer{cancel: cancel, done: done, db: db}
}

func (server *restartableHostServer) stop(t *testing.T) {
	t.Helper()
	server.cancel()
	select {
	case err := <-server.done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server did not stop")
	}
	if err := server.db.Close(); err != nil {
		t.Fatal(err)
	}
}

func registerHostInjectedClaude(t *testing.T, ctx context.Context, socket string) (*api.Client, bus.Registration) {
	t.Helper()
	client, err := api.Dial(ctx, socket, api.Identity{
		Actor: "reviewer", RunID: "reviewer-run", Client: "mcp", Harness: "claude",
		NameMode: bus.NameModeAllocate, ProjectID: "test",
		ContinuityHandles: []string{"process:claude:reviewer-run", "launch:claude:t3-test"},
	})
	if err != nil {
		t.Fatal(err)
	}
	registration, err := client.RegisterSession(ctx, bus.RegistrationRequest{
		Actor: client.Identity().Actor, RunID: client.Identity().RunID,
		Harness: "claude", AttentionMode: "host-injected", SessionID: "session-1",
		DeliveryHandle: "session-1", ProjectID: "test", Lease: time.Hour,
	})
	if err != nil {
		client.Close()
		t.Fatal(err)
	}
	if registration.AttentionMode != "host-injected" {
		client.Close()
		t.Fatalf("registration = %+v", registration)
	}
	return client, registration
}
