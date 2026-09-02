package sqlite_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/72olabs/holler/internal/bus"
	store "github.com/72olabs/holler/internal/store/sqlite"
)

func TestOperatorConditionCoalescesPresentationAndRecurrence(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 9, 3, 10, 0, 0, 0, time.UTC)}
	db, _ := openTestStore(t, store.WithClock(clock.Now))
	ctx := context.Background()
	observation := bus.ConditionObservation{
		Kind: "attention_unavailable", Subject: "reviewer-a7f3c2", ReasonCode: "startup_only_selected",
		Summary: "Automatic wake is unavailable", Details: json.RawMessage(`{"mode":"startup-only"}`),
	}
	first, err := db.ObserveCondition(ctx, observation)
	if err != nil || first.Generation != 1 || first.State != bus.ConditionActiveVisible {
		t.Fatalf("first condition = %+v, err = %v", first, err)
	}
	acknowledged, err := db.AcknowledgeCondition(ctx, first.Kind, first.Subject, first.Generation)
	if err != nil || acknowledged.State != bus.ConditionActiveAcknowledged {
		t.Fatalf("acknowledged = %+v, err = %v", acknowledged, err)
	}
	clock.Advance(time.Minute)
	coalesced, err := db.ObserveCondition(ctx, observation)
	if err != nil || coalesced.Generation != 1 || coalesced.State != bus.ConditionActiveAcknowledged {
		t.Fatalf("coalesced = %+v, err = %v", coalesced, err)
	}
	if err := db.ResolveCondition(ctx, first.Kind, first.Subject); err != nil {
		t.Fatal(err)
	}
	resolved, err := db.ListConditions(ctx, true, 10)
	if err != nil || len(resolved) != 1 || resolved[0].State != bus.ConditionResolved || resolved[0].AcknowledgedAt == nil {
		t.Fatalf("resolved condition lost acknowledgement history: %+v, err = %v", resolved, err)
	}
	clock.Advance(time.Minute)
	recurred, err := db.ObserveCondition(ctx, observation)
	if err != nil || recurred.Generation != 2 || recurred.State != bus.ConditionActiveVisible || recurred.AcknowledgedAt != nil {
		t.Fatalf("recurred = %+v, err = %v", recurred, err)
	}
}

func TestOperatorConditionSnoozeExpiresAndPresentationIsLeased(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 9, 3, 10, 0, 0, 0, time.UTC)}
	db, _ := openTestStore(t, store.WithClock(clock.Now))
	ctx := context.Background()
	condition, err := db.ObserveCondition(ctx, bus.ConditionObservation{
		Kind: "identity_conflict", Subject: "hin_example", ReasonCode: "contradictory_binding_evidence",
		Summary: "Conflicting connector identity evidence was rejected",
	})
	if err != nil {
		t.Fatal(err)
	}
	if claimed, err := db.ClaimConditionPresentation(ctx, condition.Kind, condition.Subject, condition.Generation, "codex/run-a", time.Minute); err != nil || !claimed {
		t.Fatalf("first presentation claim = %v, %v", claimed, err)
	}
	if claimed, err := db.ClaimConditionPresentation(ctx, condition.Kind, condition.Subject, condition.Generation, "claude/run-b", time.Minute); err != nil || claimed {
		t.Fatalf("competing presentation claim = %v, %v", claimed, err)
	}
	snoozed, err := db.SnoozeCondition(ctx, condition.Kind, condition.Subject, condition.Generation, clock.Now().Add(2*time.Minute))
	if err != nil || snoozed.State != bus.ConditionActiveSnoozed {
		t.Fatalf("snoozed = %+v, err = %v", snoozed, err)
	}
	clock.Advance(3 * time.Minute)
	conditions, err := db.ListConditions(ctx, false, 10)
	if err != nil || len(conditions) != 1 || conditions[0].State != bus.ConditionActiveVisible {
		t.Fatalf("conditions after snooze = %+v, err = %v", conditions, err)
	}
}

func TestStaleUnreadConditionsIgnoreNonBlockingAndResolveOnClaim(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 9, 3, 10, 0, 0, 0, time.UTC)}
	db, _ := openTestStore(t, store.WithClock(clock.Now))
	ctx := context.Background()
	wake := testRequest()
	wake.IdempotencyKey = "wake-stale"
	wake.ToActors = []string{"reviewer"}
	if _, err := db.Send(ctx, wake); err != nil {
		t.Fatal(err)
	}
	nonBlocking := testRequest()
	nonBlocking.IdempotencyKey = "nonblocking-stale"
	nonBlocking.ToActors = []string{"background"}
	nonBlocking.DeliveryRequest = bus.DeliveryNonBlocking
	if _, err := db.Send(ctx, nonBlocking); err != nil {
		t.Fatal(err)
	}
	clock.Advance(16 * time.Minute)
	if err := db.ReconcileStaleUnreadConditions(ctx, 15*time.Minute); err != nil {
		t.Fatal(err)
	}
	conditions, err := db.ListConditions(ctx, false, 10)
	if err != nil || len(conditions) != 1 || conditions[0].Kind != "stale_unread" || conditions[0].Subject != "reviewer" {
		t.Fatalf("stale conditions = %+v, err = %v", conditions, err)
	}
	if _, err := db.Claim(ctx, "reviewer", "", time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := db.ReconcileStaleUnreadConditions(ctx, 15*time.Minute); err != nil {
		t.Fatal(err)
	}
	active, err := db.ListConditions(ctx, false, 10)
	if err != nil || len(active) != 0 {
		t.Fatalf("active after claim = %+v, err = %v", active, err)
	}
	all, err := db.ListConditions(ctx, true, 10)
	if err != nil || len(all) != 1 || all[0].State != bus.ConditionResolved {
		t.Fatalf("condition history = %+v, err = %v", all, err)
	}
}
