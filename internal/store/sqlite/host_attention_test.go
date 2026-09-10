package sqlite_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/72olabs/holler/internal/bus"
	store "github.com/72olabs/holler/internal/store/sqlite"
)

func TestHostAttentionBindingIsProcessKeyedAndAtomic(t *testing.T) {
	ctx := context.Background()
	clock := &fakeClock{now: time.Date(2026, 9, 10, 9, 0, 0, 0, time.UTC)}
	db, _ := openTestStore(t, store.WithClock(clock.Now))
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
	_, err = db.RegisterSession(ctx, bus.RegistrationRequest{
		Actor: binding.Actor, RunID: binding.AssignedRunID, Harness: "claude",
		AttentionMode: "host-injected", SessionID: "session-1", ProjectID: "test",
		Lease: time.Hour, HostAttention: &process,
	})
	if err != nil {
		t.Fatal(err)
	}
	stored, err := db.HostAttentionBinding(ctx, process.Harness.PID, process.Harness.StartFingerprint)
	if err != nil || stored != process {
		t.Fatalf("stored binding = %+v, err=%v", stored, err)
	}
	if err := db.MarkHostAttentionAdmitted(ctx, process); err != nil {
		t.Fatal(err)
	}
	stored, err = db.HostAttentionBinding(ctx, process.Harness.PID, process.Harness.StartFingerprint)
	if err != nil || !stored.Admitted {
		t.Fatalf("admitted binding = %+v, err=%v", stored, err)
	}
	liveAfterAdmission, err := db.LiveRegistrations(ctx, binding.Actor)
	if err != nil || len(liveAfterAdmission) != 1 || !liveAfterAdmission[0].HostAttentionAdmitted {
		t.Fatalf("admitted registration = %+v, err=%v", liveAfterAdmission, err)
	}

	rebind := process
	rebind.SessionID = "session-2"
	replacement, err := db.RegisterSession(ctx, bus.RegistrationRequest{
		Actor: binding.Actor, RunID: binding.AssignedRunID, Harness: "claude",
		AttentionMode: "host-injected", SessionID: "session-2", ProjectID: "test",
		Lease: time.Hour, HostAttention: &rebind,
	})
	if err != nil || replacement.SessionID != "session-2" {
		t.Fatalf("same-process /clear rebind = %+v, err=%v", replacement, err)
	}
	live, err := db.LiveRegistrations(ctx, binding.Actor)
	if err != nil || len(live) != 1 || live[0].SessionID != replacement.SessionID {
		t.Fatalf("rebind did not supersede old registration atomically: registrations=%+v err=%v", live, err)
	}
	if live[0].HostAttentionAdmitted {
		t.Fatal("/clear replacement inherited prior host admission")
	}

	mismatched := process
	mismatched.Actor = "other"
	mismatched.RunID = "other-run"
	mismatched.SessionID = "other-session"
	if _, err := db.RegisterSession(ctx, bus.RegistrationRequest{
		Actor: "other", RunID: "other-run", Harness: "claude", AttentionMode: "host-injected",
		SessionID: "other-session", ProjectID: "test", Lease: time.Hour, HostAttention: &mismatched,
	}); !errors.Is(err, bus.ErrHostAttentionUnavailable) {
		t.Fatalf("harness actor/run mismatch error = %v", err)
	}

	if err := db.ExpireRegistration(ctx, binding.Actor, binding.AssignedRunID, replacement.SessionID, "test"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.HostAttentionBinding(ctx, process.Harness.PID, process.Harness.StartFingerprint); !errors.Is(err, bus.ErrNotFound) {
		t.Fatalf("expired binding lookup error = %v", err)
	}

	afterEnd := process
	afterEnd.SessionID = "session-3"
	if _, err := db.RegisterSession(ctx, bus.RegistrationRequest{
		Actor: binding.Actor, RunID: binding.AssignedRunID, Harness: "claude",
		AttentionMode: "host-injected", SessionID: afterEnd.SessionID, ProjectID: "test",
		Lease: time.Millisecond, HostAttention: &afterEnd,
	}); err != nil {
		t.Fatalf("rebind after ended registration: %v", err)
	}
	clock.Advance(2 * time.Millisecond)
	afterLapse := process
	afterLapse.SessionID = "session-4"
	if _, err := db.RegisterSession(ctx, bus.RegistrationRequest{
		Actor: binding.Actor, RunID: binding.AssignedRunID, Harness: "claude",
		AttentionMode: "host-injected", SessionID: afterLapse.SessionID, ProjectID: "test",
		Lease: time.Hour, HostAttention: &afterLapse,
	}); err != nil {
		t.Fatalf("rebind after lapsed registration: %v", err)
	}
}
