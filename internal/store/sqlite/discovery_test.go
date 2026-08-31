package sqlite_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/72olabs/holler/internal/bus"
	store "github.com/72olabs/holler/internal/store/sqlite"
)

func TestActorProfileHistoryAndDirectory(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	db, _ := openTestStore(t, store.WithClock(func() time.Time { return now }))

	if _, err := db.RegisterSession(ctx, bus.RegistrationRequest{
		Actor: "reviewer", RunID: "review-run-1", Harness: "test", SessionID: "session-1",
		DeliveryHandle: "private-handle", ProjectID: "coupon", WorkingDir: "/workspace/coupon",
		Lease: 5 * time.Minute,
	}); err != nil {
		t.Fatal(err)
	}
	profileCtx := bus.WithCaller(ctx, bus.Caller{Actor: "reviewer", RunID: "review-run-1", Client: "test"})
	first, err := db.SetActorProfile(profileCtx, "reviewer", "review-run-1", "coupon", bus.ActorProfileRequest{
		RoleText: "Reviews the coupon plugin", Accepts: []string{"REVIEW_REQUEST", "QUESTION", "REVIEW_REQUEST"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !first.Updated || first.Profile.Revision != 1 || len(first.Profile.Accepts) != 2 {
		t.Fatalf("first profile = %+v", first)
	}
	duplicate, err := db.SetActorProfile(profileCtx, "reviewer", "review-run-1", "coupon", bus.ActorProfileRequest{
		RoleText: "Reviews the coupon plugin", Accepts: []string{"REVIEW_REQUEST", "QUESTION"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if duplicate.Updated || duplicate.Profile.Revision != 1 {
		t.Fatalf("duplicate profile = %+v", duplicate)
	}
	now = now.Add(time.Minute)
	second, err := db.SetActorProfile(profileCtx, "reviewer", "review-run-1", "coupon", bus.ActorProfileRequest{
		RoleText: "Reviews coupon correctness and regressions", Accepts: []string{"REVIEW_REQUEST"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !second.Updated || second.Profile.Revision != 2 {
		t.Fatalf("second profile = %+v", second)
	}

	request := testRequest()
	request.ProjectID = "coupon"
	request.ToActors = []string{"reviewer"}
	if _, err := db.Send(ctx, request); err != nil {
		t.Fatal(err)
	}
	if err := db.ExpireRegistration(ctx, "reviewer", "review-run-1", "session-1", "test_end"); err != nil {
		t.Fatal(err)
	}
	directory, err := db.Who(ctx, 100)
	if err != nil {
		t.Fatal(err)
	}
	if directory.MetadataTrust != "untrusted" || directory.Truncated {
		t.Fatalf("directory envelope = %+v", directory)
	}
	reviewer := directoryActor(t, directory, "reviewer")
	if reviewer.State != "ended" || reviewer.UnclaimedMessages != 1 {
		t.Fatalf("reviewer directory entry = %+v", reviewer)
	}
	if reviewer.Profile == nil || reviewer.Profile.Revision != 2 || reviewer.Profile.RoleText != "Reviews coupon correctness and regressions" {
		t.Fatalf("reviewer profile = %+v", reviewer.Profile)
	}
	if len(reviewer.Sessions) != 1 || reviewer.Sessions[0].WorkingDir != "/workspace/coupon" || reviewer.Sessions[0].State != "ended" {
		t.Fatalf("reviewer sessions = %+v", reviewer.Sessions)
	}

	events, err := db.ListEvents(ctx, "coupon", "durable", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	profileEvents := 0
	for _, event := range events {
		if event.Kind != "actor.profile_updated" {
			continue
		}
		profileEvents++
		var payload map[string]interface{}
		if err := json.Unmarshal(event.Payload, &payload); err != nil || payload["role_text"] == nil {
			t.Fatalf("profile event payload = %s, err = %v", event.Payload, err)
		}
	}
	if profileEvents != 2 {
		t.Fatalf("profile event count = %d, want 2", profileEvents)
	}
}

func TestWhoDistinguishesLiveAndLapsedSessions(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	db, _ := openTestStore(t, store.WithClock(func() time.Time { return now }))
	for _, registration := range []bus.RegistrationRequest{
		{Actor: "live-agent", RunID: "live-run", Harness: "test", SessionID: "live-session", Lease: 10 * time.Minute},
		{Actor: "lapsed-agent", RunID: "lapsed-run", Harness: "test", SessionID: "lapsed-session", Lease: time.Minute},
	} {
		if _, err := db.RegisterSession(ctx, registration); err != nil {
			t.Fatal(err)
		}
	}
	now = now.Add(2 * time.Minute)
	directory, err := db.Who(ctx, 100)
	if err != nil {
		t.Fatal(err)
	}
	if got := directoryActor(t, directory, "live-agent").State; got != "live" {
		t.Fatalf("live-agent state = %q", got)
	}
	if got := directoryActor(t, directory, "lapsed-agent").State; got != "lapsed" {
		t.Fatalf("lapsed-agent state = %q", got)
	}
}

func directoryActor(t *testing.T, directory bus.ActorDirectory, actor string) bus.ActorDirectoryEntry {
	t.Helper()
	for _, entry := range directory.Actors {
		if entry.Actor == actor {
			return entry
		}
	}
	t.Fatalf("actor %q not found in %+v", actor, directory.Actors)
	return bus.ActorDirectoryEntry{}
}
