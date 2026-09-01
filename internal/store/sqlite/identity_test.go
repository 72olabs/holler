package sqlite_test

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/72olabs/holler/internal/bus"
	store "github.com/72olabs/holler/internal/store/sqlite"
)

func TestAllocateActorIsAtomicAndContinuitySafe(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 8, 31, 15, 0, 0, 0, time.UTC)
	failEvent := true
	id := 0
	db, _ := openTestStore(t,
		store.WithClock(func() time.Time { return now }),
		store.WithIDGenerator(func(prefix string) (string, error) {
			if failEvent {
				failEvent = false
				return "", errors.New("injected event failure")
			}
			id++
			return fmt.Sprintf("%s_%d", prefix, id), nil
		}),
	)
	request := bus.ActorBindRequest{
		RequestedActor: "coupon-reviewer", RunID: "run-1", NameMode: bus.NameModeAllocate,
		ContinuityHandles: []string{"process:codex:run-1", "session:codex:session-1"}, ProjectID: "coupon",
	}
	if _, err := db.BindActor(ctx, request); err == nil || !strings.Contains(err.Error(), "injected event failure") {
		t.Fatalf("first allocation error = %v", err)
	}
	first, err := db.BindActor(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if first.Actor != "coupon-reviewer" || !first.Minted || first.ContinuityReclaimed {
		t.Fatalf("first allocation = %+v", first)
	}
	retry, err := db.BindActor(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if retry.Actor != first.Actor || retry.Minted || !retry.ContinuityReclaimed {
		t.Fatalf("retry allocation = %+v", retry)
	}
	second, err := db.BindActor(ctx, bus.ActorBindRequest{
		RequestedActor: "coupon-reviewer", RunID: "run-2", NameMode: bus.NameModeAllocate,
		ContinuityHandles: []string{"process:codex:run-2", "session:codex:session-2"}, ProjectID: "coupon",
	})
	if err != nil {
		t.Fatal(err)
	}
	if second.Actor != "coupon-reviewer-2" || !second.Minted {
		t.Fatalf("second allocation = %+v", second)
	}
	events, err := db.ListEvents(ctx, "coupon", "durable", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	mints := 0
	for _, event := range events {
		if event.Kind == "actor.minted" {
			mints++
		}
	}
	if mints != 2 {
		t.Fatalf("mint events = %d, want 2", mints)
	}
}

func TestContinuityReclaimSupersedesOldPresence(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 8, 31, 15, 0, 0, 0, time.UTC)
	db, _ := openTestStore(t, store.WithClock(func() time.Time { return now }))
	first, err := db.BindActor(ctx, bus.ActorBindRequest{
		RequestedActor: "reviewer", RunID: "run-1", NameMode: bus.NameModeAllocate,
		ContinuityHandles: []string{"session:claude:session-1", "process:claude:run-1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.RegisterSession(ctx, bus.RegistrationRequest{
		Actor: first.Actor, RunID: "run-1", Harness: "claude", SessionID: "session-1", Lease: time.Hour,
	}); err != nil {
		t.Fatal(err)
	}
	reclaimed, err := db.BindActor(ctx, bus.ActorBindRequest{
		RequestedActor: "reviewer", RunID: "run-2", NameMode: bus.NameModeAllocate,
		ContinuityHandles: []string{"session:claude:session-1", "process:claude:run-2"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if reclaimed.Actor != first.Actor || !reclaimed.ContinuityReclaimed || len(reclaimed.SupersededPresences) != 1 {
		t.Fatalf("reclaimed binding = %+v", reclaimed)
	}
	if live, err := db.LiveRegistrations(ctx, first.Actor); err != nil || len(live) != 0 {
		t.Fatalf("old live registrations = %+v, err = %v", live, err)
	}
}

func TestExactActorRefusesLiveCollisionUnlessTakeover(t *testing.T) {
	ctx := context.Background()
	db, _ := openTestStore(t)
	if _, err := db.RegisterSession(ctx, bus.RegistrationRequest{
		Actor: "reviewer", RunID: "run-1", Harness: "test", SessionID: "session-1", Lease: time.Hour,
	}); err != nil {
		t.Fatal(err)
	}
	request := bus.ActorBindRequest{RequestedActor: "reviewer", RunID: "run-2", NameMode: bus.NameModeExact}
	if _, err := db.BindActor(ctx, request); !errors.Is(err, bus.ErrActorLive) {
		t.Fatalf("exact collision error = %v", err)
	}
	request.Takeover = true
	result, err := db.BindActor(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if result.Actor != "reviewer" || len(result.SupersededPresences) != 1 {
		t.Fatalf("takeover = %+v", result)
	}
}

func TestContinuityBindingSurvivesStoreRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "holler.sqlite3")
	db, err := store.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	first, err := db.BindActor(ctx, bus.ActorBindRequest{
		RequestedActor: "worker", RunID: "run-1", NameMode: bus.NameModeAllocate,
		ContinuityHandles: []string{"launch:codex:tab-7"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = store.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	reclaimed, err := db.BindActor(ctx, bus.ActorBindRequest{
		RequestedActor: "worker", RunID: "run-2", NameMode: bus.NameModeAllocate,
		ContinuityHandles: []string{"launch:codex:tab-7"},
	})
	if err != nil || reclaimed.Actor != first.Actor || !reclaimed.ContinuityReclaimed {
		t.Fatalf("reclaimed after restart = %+v, err = %v", reclaimed, err)
	}
}

func TestSupersededRunCannotReclaimAcrossRestart(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 8, 31, 15, 0, 0, 0, time.UTC)
	path := filepath.Join(t.TempDir(), "holler.sqlite3")
	open := func() *store.Store {
		db, err := store.Open(ctx, path, store.WithClock(func() time.Time { return now }))
		if err != nil {
			t.Fatal(err)
		}
		return db
	}

	db := open()
	first, err := db.BindActor(ctx, bus.ActorBindRequest{
		RequestedActor: "reviewer", RunID: "run-a", NameMode: bus.NameModeAllocate,
		ContinuityHandles: []string{"process:codex:run-a", "session:codex:session-a"}, ProjectID: "coupon",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.RegisterSession(ctx, bus.RegistrationRequest{
		Actor: first.Actor, RunID: "run-a", Harness: "codex", SessionID: "session-a",
		ProjectID: "coupon", Lease: time.Hour,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.BindActor(ctx, bus.ActorBindRequest{
		RequestedActor: first.Actor, RunID: "run-b", NameMode: bus.NameModeExact,
		ProjectID: "coupon", Takeover: true,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.RegisterSession(ctx, bus.RegistrationRequest{
		Actor: first.Actor, RunID: "run-b", Harness: "codex", SessionID: "session-b",
		ProjectID: "coupon", Lease: time.Hour,
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db = open()
	defer db.Close()
	_, err = db.BindActor(ctx, bus.ActorBindRequest{
		RequestedActor: "reviewer", RunID: "run-a", NameMode: bus.NameModeAllocate,
		ContinuityHandles: []string{"process:codex:run-a", "session:codex:session-a"}, ProjectID: "coupon",
	})
	if !errors.Is(err, bus.ErrBindingStale) {
		t.Fatalf("superseded reclaim error = %v", err)
	}
	live, err := db.LiveRegistrations(ctx, first.Actor)
	if err != nil || len(live) != 1 || live[0].RunID != "run-b" {
		t.Fatalf("live registrations after stale reclaim = %+v, err = %v", live, err)
	}
}

func TestEndedRunStillReclaimsAfterRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "holler.sqlite3")
	db, err := store.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	first, err := db.BindActor(ctx, bus.ActorBindRequest{
		RequestedActor: "reviewer", RunID: "run-a", NameMode: bus.NameModeAllocate,
		ContinuityHandles: []string{"session:claude:session-a"}, ProjectID: "coupon",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.RegisterSession(ctx, bus.RegistrationRequest{
		Actor: first.Actor, RunID: "run-a", Harness: "claude", SessionID: "session-a",
		ProjectID: "coupon", Lease: time.Hour,
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.ExpireRegistration(ctx, first.Actor, "run-a", "session-a", "SessionEnd"); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = store.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	reclaimed, err := db.BindActor(ctx, bus.ActorBindRequest{
		RequestedActor: "reviewer", RunID: "run-b", NameMode: bus.NameModeAllocate,
		ContinuityHandles: []string{"session:claude:session-a"}, ProjectID: "coupon",
	})
	if err != nil || reclaimed.Actor != first.Actor || !reclaimed.ContinuityReclaimed {
		t.Fatalf("ended binding reclaim = %+v, err = %v", reclaimed, err)
	}
}

func TestSessionContinuityReconcilesMCPFirstResumeWithoutPhantomActor(t *testing.T) {
	ctx := context.Background()
	db, _ := openTestStore(t)
	first, err := db.BindActor(ctx, bus.ActorBindRequest{
		RequestedActor: "reviewer", RunID: "run-1", NameMode: bus.NameModeAllocate,
		ContinuityHandles: []string{"process:codex:run-1", "session:codex:session-1"}, ProjectID: "coupon",
	})
	if err != nil || first.Actor != "reviewer" || !first.Minted || first.Provisional {
		t.Fatalf("first binding = %+v, err = %v", first, err)
	}
	reservation, err := db.BindActor(ctx, bus.ActorBindRequest{
		RequestedActor: "reviewer", RunID: "run-2", NameMode: bus.NameModeAllocate,
		ContinuityHandles: []string{"process:codex:run-2"}, ProjectID: "coupon",
	})
	if err != nil || reservation.Actor != "reviewer-2" || reservation.Minted || !reservation.Provisional {
		t.Fatalf("process-only reservation = %+v, err = %v", reservation, err)
	}
	directory, err := db.Who(ctx, 10)
	if err != nil || len(directory.Actors) != 1 || directory.Actors[0].Actor != "reviewer" {
		t.Fatalf("directory exposed provisional reservation = %+v, err = %v", directory, err)
	}
	reclaimed, err := db.BindActor(ctx, bus.ActorBindRequest{
		RequestedActor: "reviewer", RunID: "run-2", NameMode: bus.NameModeAllocate,
		ContinuityHandles: []string{"process:codex:run-2", "session:codex:session-1"}, ProjectID: "coupon",
	})
	if err != nil || reclaimed.Actor != "reviewer" || !reclaimed.ContinuityReclaimed || reclaimed.Minted || reclaimed.Provisional {
		t.Fatalf("reconciled binding = %+v, err = %v", reclaimed, err)
	}
	current, err := db.CurrentActorForContinuity(ctx, []string{"process:codex:run-2"})
	if err != nil || current != "reviewer" {
		t.Fatalf("reconciled process handle = %q, err = %v", current, err)
	}
	second, err := db.BindActor(ctx, bus.ActorBindRequest{
		RequestedActor: "reviewer", RunID: "run-3", NameMode: bus.NameModeAllocate,
		ContinuityHandles: []string{"launch:codex:worker-3"}, ProjectID: "coupon",
	})
	if err != nil || second.Actor != "reviewer-2" || !second.Minted {
		t.Fatalf("released suffix allocation = %+v, err = %v", second, err)
	}
	events, err := db.ListEvents(ctx, "coupon", "durable", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	mints := 0
	for _, event := range events {
		if event.Kind == "actor.minted" {
			mints++
		}
	}
	if mints != 2 {
		t.Fatalf("mint events = %d, want only the two visible actors", mints)
	}
}

func TestDaemonRestartDropsUnusedProcessOnlyReservation(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "holler.sqlite3")
	db, err := store.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	reservation, err := db.BindActor(ctx, bus.ActorBindRequest{
		RequestedActor: "worker", RunID: "run-1", NameMode: bus.NameModeAllocate,
		ContinuityHandles: []string{"process:codex:run-1"}, ProjectID: "test",
	})
	if err != nil || reservation.Actor != "worker" || !reservation.Provisional {
		t.Fatalf("reservation = %+v, err = %v", reservation, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = store.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	active, err := db.BindActor(ctx, bus.ActorBindRequest{
		RequestedActor: "worker", RunID: "run-2", NameMode: bus.NameModeAllocate,
		ContinuityHandles: []string{"launch:codex:worker-2"}, ProjectID: "test",
	})
	if err != nil || active.Actor != "worker" || !active.Minted {
		t.Fatalf("allocation after restart = %+v, err = %v", active, err)
	}
}
