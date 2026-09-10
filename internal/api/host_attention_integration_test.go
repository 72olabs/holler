package api_test

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/72olabs/holler/internal/api"
	"github.com/72olabs/holler/internal/attention"
	"github.com/72olabs/holler/internal/bus"
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
	)
	client, registration := registerHostInjectedClaude(t, ctx, socket)
	defer client.Close()

	host, err := api.DialHostAttention(ctx, socket, "launch:claude:t3-test")
	if err != nil {
		t.Fatal(err)
	}
	if !broker.Attached(registration.Actor, registration.RunID, registration.SessionID) {
		t.Fatal("exact host was admitted without attaching attention")
	}
	if duplicate, duplicateErr := api.DialHostAttention(ctx, socket, "launch:claude:t3-test"); !errors.Is(duplicateErr, bus.ErrAttentionWaiterBusy) {
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
	_, socket := startServer(t, ctx, cancel,
		api.WithAttentionBroker(attention.NewBroker()),
		api.WithHarnessProcessResolver(func(_ net.Conn, _ string) (api.HarnessProcessIdentity, error) {
			return evidence, nil
		}),
		api.WithPeerProcessResolver(func(net.Conn) (api.ProcessIdentity, error) {
			return peer.Load().(api.ProcessIdentity), nil
		}),
	)
	client, _ := registerHostInjectedClaude(t, ctx, socket)
	defer client.Close()

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
			host, err := api.DialHostAttention(ctx, socket, "launch:claude:t3-test")
			if host != nil {
				host.Close()
			}
			if err == nil || !strings.Contains(err.Error(), "not admitted") {
				t.Fatalf("peer %+v error = %v", test.peer, err)
			}
		})
	}
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
