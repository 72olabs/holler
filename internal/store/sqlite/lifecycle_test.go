package sqlite_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/72olabs/holler/internal/bus"
	store "github.com/72olabs/holler/internal/store/sqlite"
)

func TestActorArchiveRequiresPreflightPreservesUnreadAndRestores(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 9, 3, 10, 0, 0, 0, time.UTC)}
	db, _ := openTestStore(t, store.WithClock(clock.Now))
	ctx := context.Background()
	if err := db.ReserveActorName(ctx, "reviewer-a7f3c2"); err != nil {
		t.Fatal(err)
	}
	if err := db.ReserveActorName(ctx, "sender-b81d90"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.SetAlias(ctx, bus.AliasSetRequest{
		Alias: "project-reviewer", Actor: "reviewer-a7f3c2", UpdatedByActor: "operator",
		UpdatedByRun: "operator-run", ProjectID: "project", IdempotencyKey: "set-reviewer",
	}); err != nil {
		t.Fatal(err)
	}
	request := testRequest()
	request.FromActor = "sender-b81d90"
	request.IdempotencyKey = "archive-unread"
	request.ToActors = []string{"reviewer-a7f3c2"}
	request.Body = []byte(`{"text":"untrusted instructions"}`)
	if _, err := db.Send(ctx, request); err != nil {
		t.Fatal(err)
	}
	blocked, err := db.ArchivePreflight(ctx, "reviewer-a7f3c2", 10)
	if err != nil || blocked.OperatorEligible || len(blocked.Aliases) != 1 || len(blocked.Unread) != 1 || !blocked.Unread[0].PreviewUntrusted {
		t.Fatalf("blocked preflight = %+v, err=%v", blocked, err)
	}
	if _, err := db.RemoveAlias(ctx, bus.AliasRemoveRequest{
		Alias: "project-reviewer", UpdatedByActor: "operator", UpdatedByRun: "operator-run",
		ProjectID: "project", IdempotencyKey: "remove-reviewer",
	}); err != nil {
		t.Fatal(err)
	}
	preflight, err := db.ArchivePreflight(ctx, "reviewer-a7f3c2", 10)
	if err != nil || !preflight.OperatorEligible || preflight.AutomaticEligible || len(preflight.Unread) != 1 ||
		!strings.Contains(preflight.Unread[0].BodyPreview, "untrusted instructions") {
		t.Fatalf("archive preflight = %+v, err=%v", preflight, err)
	}
	if _, err := db.ArchiveActor(ctx, "reviewer-a7f3c2", "operator", false); err == nil {
		t.Fatal("archive without unread authorization succeeded")
	}
	archived, err := db.ArchiveActor(ctx, "reviewer-a7f3c2", "operator", true)
	if err != nil || !archived.Archived || !archived.WithUnread {
		t.Fatalf("archive result = %+v, err=%v", archived, err)
	}
	conditions, err := db.ListConditions(ctx, false, 10)
	if err != nil || len(conditions) != 1 || conditions[0].Kind != "stale_unread" ||
		conditions[0].State != bus.ConditionActiveAcknowledged {
		t.Fatalf("archived unread condition = %+v, err=%v", conditions, err)
	}
	directory, err := db.Who(ctx, 100)
	if err != nil {
		t.Fatal(err)
	}
	for _, actor := range directory.Actors {
		if actor.Actor == "reviewer-a7f3c2" {
			t.Fatalf("archived actor remained in default directory: %+v", actor)
		}
	}
	all, err := db.WhoIncludingArchived(ctx, 100)
	if err != nil {
		t.Fatal(err)
	}
	foundArchived := false
	for _, actor := range all.Actors {
		if actor.Actor == "reviewer-a7f3c2" && actor.State == "archived" {
			foundArchived = true
		}
	}
	if !foundArchived {
		t.Fatalf("archived actor missing from full directory: %+v", all)
	}
	request.IdempotencyKey = "send-after-archive"
	if _, err := db.Send(ctx, request); !errors.Is(err, bus.ErrActorArchived) {
		t.Fatalf("send to archived actor error = %v", err)
	}
	if _, err := db.RestoreActor(ctx, "reviewer-a7f3c2", "operator"); err != nil {
		t.Fatal(err)
	}
	conditions, err = db.ListConditions(ctx, false, 10)
	if err != nil || len(conditions) != 1 || conditions[0].State != bus.ConditionActiveAcknowledged {
		t.Fatalf("restored unread condition = %+v, err=%v", conditions, err)
	}
	request.IdempotencyKey = "send-after-restore"
	if _, err := db.Send(ctx, request); err != nil {
		t.Fatalf("send after restore: %v", err)
	}
}

func TestOperatorLeaseRevocationRequeuesAndFencesLateAck(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 9, 3, 10, 0, 0, 0, time.UTC)}
	db, _ := openTestStore(t, store.WithClock(clock.Now))
	ctx := context.Background()
	request := testRequest()
	request.IdempotencyKey = "revocable-claim"
	result, err := db.Send(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	claim, err := db.Claim(ctx, "reviewer", result.Message.ID, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Minute)
	if err := db.RevokeDeliveryLease(ctx, "reviewer", result.Message.ID, 30*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := db.Ack(ctx, "reviewer", result.Message.ID, claim.LeaseToken); !errors.Is(err, bus.ErrDeliveryTerminal) {
		t.Fatalf("late ack error = %v", err)
	}
	reclaimed, err := db.Claim(ctx, "reviewer", result.Message.ID, time.Minute)
	if err != nil || reclaimed.Attempt != claim.Attempt+1 {
		t.Fatalf("reclaimed = %+v, err=%v", reclaimed, err)
	}
}

func TestAutomaticArchiveRequiresNoContinuityOrMail(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	db, _ := openTestStore(t, store.WithClock(clock.Now))
	ctx := context.Background()
	if err := db.ReserveActorName(ctx, "idle-worker"); err != nil {
		t.Fatal(err)
	}
	if err := db.ReserveActorName(ctx, "recent-worker"); err != nil {
		t.Fatal(err)
	}
	clock.Advance(31 * 24 * time.Hour)
	if _, err := db.Send(ctx, bus.SendRequest{
		FromActor: "recent-worker", FromRun: "recent-run", IdempotencyKey: "recent-activity",
		ProjectID: "test", ChannelID: "direct", ToActors: []string{"recipient"},
		Type: "MESSAGE", DeliveryRequest: bus.DeliveryNonBlocking, Body: []byte(`{"text":"still active"}`),
	}); err != nil {
		t.Fatal(err)
	}
	archived, err := db.ArchiveEligibleActors(ctx, 30*24*time.Hour)
	if err != nil || len(archived) != 1 || archived[0] != "idle-worker" {
		t.Fatalf("auto archived = %+v, err=%v", archived, err)
	}
}
