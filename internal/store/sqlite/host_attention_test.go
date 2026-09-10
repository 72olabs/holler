package sqlite_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/72olabs/holler/internal/bus"
)

func TestHostAttentionBindingIsProcessKeyedAndAtomic(t *testing.T) {
	ctx := context.Background()
	db, _ := openTestStore(t)
	binding, err := db.BindActor(ctx, bus.ActorBindRequest{
		RequestedActor: "reviewer", RunID: "run-1", NameMode: bus.NameModeAllocate,
		ContinuityHandles: []string{"process:claude:run-1", "instance:hin_host_store"}, ProjectID: "test",
	})
	if err != nil {
		t.Fatal(err)
	}
	process := bus.HostAttentionBinding{
		HarnessHandle: "instance:hin_host_store",
		Harness:       bus.ProcessIdentity{PID: 67550, StartFingerprint: "claude-start"},
		Host:          bus.ProcessIdentity{PID: 8784, StartFingerprint: "t3-start"},
		Actor:         binding.Actor, RunID: binding.AssignedRunID, SessionID: "session-1",
	}
	registration, err := db.RegisterSession(ctx, bus.RegistrationRequest{
		Actor: binding.Actor, RunID: binding.AssignedRunID, Harness: "claude",
		AttentionMode: "host-injected", SessionID: "session-1", ProjectID: "test",
		Lease: time.Hour, HostAttention: &process,
	})
	if err != nil {
		t.Fatal(err)
	}
	stored, err := db.HostAttentionBindingByPID(ctx, process.Harness.PID)
	if err != nil || stored != process {
		t.Fatalf("stored binding = %+v, err=%v", stored, err)
	}

	rebind := process
	rebind.SessionID = "session-2"
	if _, err := db.RegisterSession(ctx, bus.RegistrationRequest{
		Actor: binding.Actor, RunID: binding.AssignedRunID, Harness: "claude",
		AttentionMode: "host-injected", SessionID: "session-2", ProjectID: "test",
		Lease: time.Hour, HostAttention: &rebind,
	}); !errors.Is(err, bus.ErrInvalid) {
		t.Fatalf("process rebind error = %v", err)
	}
	live, err := db.LiveRegistrations(ctx, binding.Actor)
	if err != nil || len(live) != 1 || live[0].SessionID != registration.SessionID {
		t.Fatalf("rebind was not atomic: registrations=%+v err=%v", live, err)
	}

	mismatched := process
	mismatched.Actor = "other"
	mismatched.RunID = "other-run"
	mismatched.SessionID = "other-session"
	if _, err := db.RegisterSession(ctx, bus.RegistrationRequest{
		Actor: "other", RunID: "other-run", Harness: "claude", AttentionMode: "host-injected",
		SessionID: "other-session", ProjectID: "test", Lease: time.Hour, HostAttention: &mismatched,
	}); !errors.Is(err, bus.ErrInvalid) {
		t.Fatalf("harness actor/run mismatch error = %v", err)
	}

	if err := db.ExpireRegistration(ctx, binding.Actor, binding.AssignedRunID, registration.SessionID, "test"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.HostAttentionBindingByPID(ctx, process.Harness.PID); !errors.Is(err, bus.ErrNotFound) {
		t.Fatalf("expired binding lookup error = %v", err)
	}
}
