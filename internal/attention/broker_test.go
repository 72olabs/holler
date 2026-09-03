package attention

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/72olabs/holler/internal/bus"
)

func TestBrokerRoutesReferenceToExactSession(t *testing.T) {
	broker := NewBroker()
	registration := bus.Registration{Actor: "claude", RunID: "run-1", SessionID: "session-1", AttentionMode: "hook-long-poll"}
	if err := broker.Attach(registration.Actor, registration.RunID, registration.SessionID, nil); err != nil {
		t.Fatal(err)
	}
	result := make(chan bus.AttentionNotice, 1)
	errorsSeen := make(chan error, 1)
	go func() {
		notice, err := broker.Wait(context.Background(), "claude", "run-1", "session-1", "hook-long-poll")
		result <- notice
		errorsSeen <- err
	}()
	waitForWaiter(t, broker, registration)
	message := bus.Message{
		ID: "msg-1", ThreadID: "thread-1", FromActor: "codex", Type: "QUESTION",
		DeliveryRequest: bus.DeliveryWake,
	}
	if adapter, accepted := broker.Notify(registration, message); !accepted || adapter != "hook-long-poll" {
		t.Fatal("exact session did not accept attention notice")
	}
	if _, accepted := broker.Notify(registration, bus.Message{ID: "msg-raced"}); accepted {
		t.Fatal("second notice was accepted into a waiter already consumed by the first notice")
	}
	select {
	case notice := <-result:
		if notice.MessageID != message.ID || notice.FromActor != "codex" || notice.ThreadID != "thread-1" {
			t.Fatalf("notice = %+v", notice)
		}
		if err := <-errorsSeen; err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("waiter did not receive notice")
	}
}

func TestBrokerRejectsWrongOrDuplicateSessionAndCancels(t *testing.T) {
	broker := NewBroker()
	if err := broker.Attach("claude", "run-1", "session-1", nil); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := broker.Wait(ctx, "claude", "run-1", "session-1", "hook-long-poll")
		done <- err
	}()
	registration := bus.Registration{Actor: "claude", RunID: "run-1", SessionID: "session-1", AttentionMode: "hook-long-poll"}
	waitForWaiter(t, broker, registration)
	if _, err := broker.Wait(ctx, "claude", "run-1", "session-1", "other-adapter"); !errors.Is(err, bus.ErrAttentionWaiterBusy) {
		t.Fatalf("duplicate waiter error = %v", err)
	}
	wrong := registration
	wrong.SessionID = "session-2"
	if _, accepted := broker.Notify(wrong, bus.Message{ID: "msg-wrong"}); accepted {
		t.Fatal("wrong session accepted notice")
	}
	wrongMode := registration
	wrongMode.AttentionMode = "other-adapter"
	if _, accepted := broker.Notify(wrongMode, bus.Message{ID: "msg-wrong-mode"}); accepted {
		t.Fatal("registration with mismatched attention mode accepted notice")
	}
	broker.Cancel("claude", "run-1", "session-1", bus.ErrSessionEnded)
	select {
	case err := <-done:
		if !errors.Is(err, bus.ErrSessionEnded) {
			t.Fatalf("cancel error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancel did not release waiter")
	}
}

func TestBrokerSupersedesOldPresenceAndRejectsItsWake(t *testing.T) {
	broker := NewBroker()
	old := bus.Registration{Actor: "claude", RunID: "old-run", SessionID: "old-session", AttentionMode: "hook-long-poll"}
	newer := bus.Registration{Actor: "claude", RunID: "new-run", SessionID: "new-session", AttentionMode: "hook-long-poll"}
	if err := broker.Attach(old.Actor, old.RunID, old.SessionID, nil); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := broker.Wait(context.Background(), old.Actor, old.RunID, old.SessionID, old.AttentionMode)
		done <- err
	}()
	waitForWaiter(t, broker, old)
	if err := broker.Attach(newer.Actor, newer.RunID, newer.SessionID, nil); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, bus.ErrPresenceSuperseded) {
			t.Fatalf("old waiter outcome = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("old waiter was not superseded")
	}
	if _, accepted := broker.Notify(old, bus.Message{ID: "stale"}); accepted {
		t.Fatal("superseded presence accepted a wake")
	}
}

func TestBrokerRunsTransitionOnlyWhenPresenceChanges(t *testing.T) {
	broker := NewBroker()
	transitions := 0
	transition := func() error {
		transitions++
		return nil
	}
	if err := broker.Attach("claude", "run-1", "session-1", transition); err != nil {
		t.Fatal(err)
	}
	if err := broker.Attach("claude", "run-1", "session-1", transition); err != nil {
		t.Fatal(err)
	}
	if transitions != 1 {
		t.Fatalf("same-presence heartbeat transitions = %d, want 1", transitions)
	}
	if err := broker.Attach("claude", "run-2", "session-2", transition); err != nil {
		t.Fatal(err)
	}
	if transitions != 2 {
		t.Fatalf("successor presence transitions = %d, want 2", transitions)
	}

	want := errors.New("rearm failed")
	if err := broker.Attach("claude", "run-3", "session-3", func() error { return want }); !errors.Is(err, want) {
		t.Fatalf("failed transition = %v, want %v", err, want)
	}
	broker.mu.Lock()
	current := broker.current["claude"]
	broker.mu.Unlock()
	if current.runID != "run-2" || current.sessionID != "session-2" {
		t.Fatalf("failed transition changed current presence: %+v", current)
	}
}

func TestBrokerConnectionContextCancellationRemovesWaiter(t *testing.T) {
	broker := NewBroker()
	registration := bus.Registration{Actor: "claude", RunID: "run-1", SessionID: "session-1", AttentionMode: "hook-long-poll"}
	if err := broker.Attach(registration.Actor, registration.RunID, registration.SessionID, nil); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := broker.Wait(ctx, registration.Actor, registration.RunID, registration.SessionID, registration.AttentionMode)
		done <- err
	}()
	waitForWaiter(t, broker, registration)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("wait outcome = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled waiter did not return")
	}
	broker.mu.Lock()
	defer broker.mu.Unlock()
	if len(broker.waiters) != 0 {
		t.Fatalf("orphan waiters = %d", len(broker.waiters))
	}
}

func waitForWaiter(t *testing.T, broker *Broker, registration bus.Registration) {
	t.Helper()
	key := sessionKey{actor: registration.Actor, runID: registration.RunID, sessionID: registration.SessionID}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		broker.mu.Lock()
		_, ready := broker.waiters[key]
		broker.mu.Unlock()
		if ready {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("waiter was not ready")
}
