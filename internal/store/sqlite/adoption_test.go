package sqlite_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/72olabs/holler/internal/bus"
)

func TestActorAdoptionPreservesOriginalRecipientAcrossDeliveryLifecycle(t *testing.T) {
	ctx := context.Background()
	db, _ := openTestStore(t)
	registerLiveActor(t, db, "replacement", "replacement-run")
	sent, err := db.Send(ctx, bus.SendRequest{
		IdempotencyKey: "orphan-before-adopt", ProjectID: "coupon", ChannelID: "direct",
		FromActor: "sender", FromRun: "sender-run", ToActors: []string{"reviewer-old"},
		Type: "REVIEW_REQUEST", DeliveryRequest: bus.DeliveryWake, Body: json.RawMessage(`{"text":"review this"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := db.AdoptActor(ctx, bus.AdoptRequest{
		SourceActor: "reviewer-old", AdoptingActor: "replacement", AdoptingRun: "replacement-run",
		ProjectID: "coupon", IdempotencyKey: "adopt-reviewer-old-v1",
	})
	if err != nil || result.Transferred != 1 || result.Deduplicated != 0 || result.DuplicateRequest {
		t.Fatalf("adoption = %+v, err = %v", result, err)
	}
	if oldInbox, err := db.CheckInbox(ctx, "reviewer-old", 10); err != nil || len(oldInbox) != 0 {
		t.Fatalf("old inbox = %+v, err = %v", oldInbox, err)
	}
	inbox, err := db.CheckInbox(ctx, "replacement", 10)
	if err != nil || len(inbox) != 1 || inbox[0].RecipientActor != "replacement" || inbox[0].OriginalRecipientActor != "reviewer-old" {
		t.Fatalf("adopted inbox = %+v, err = %v", inbox, err)
	}
	claim, err := db.Claim(ctx, "replacement", sent.Message.ID, time.Minute)
	if err != nil || claim.RecipientActor != "replacement" || claim.OriginalRecipientActor != "reviewer-old" || claim.Message.ToActors[0] != "reviewer-old" {
		t.Fatalf("adopted claim = %+v, err = %v", claim, err)
	}
	if _, err := db.Extend(ctx, "replacement", sent.Message.ID, claim.LeaseToken, 2*time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := db.Ack(ctx, "replacement", sent.Message.ID, claim.LeaseToken); err != nil {
		t.Fatal(err)
	}
	retry, err := db.AdoptActor(ctx, bus.AdoptRequest{
		SourceActor: "reviewer-old", AdoptingActor: "replacement", AdoptingRun: "new-run-is-okay",
		ProjectID: "coupon", IdempotencyKey: "adopt-reviewer-old-v1",
	})
	if err != nil || !retry.DuplicateRequest || retry.Transferred != 1 {
		t.Fatalf("idempotent adoption = %+v, err = %v", retry, err)
	}
}

func TestActorAdoptionRoutesFutureMessagesAndDeduplicatesDirectRecipient(t *testing.T) {
	ctx := context.Background()
	db, _ := openTestStore(t)
	registerLiveActor(t, db, "replacement", "replacement-run")
	if _, err := db.Send(ctx, bus.SendRequest{
		IdempotencyKey: "seed", ProjectID: "coupon", ChannelID: "direct", FromActor: "sender", FromRun: "run",
		ToActors: []string{"reviewer-old"}, Type: "MESSAGE", Body: json.RawMessage(`{"text":"seed"}`),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.AdoptActor(ctx, bus.AdoptRequest{
		SourceActor: "reviewer-old", AdoptingActor: "replacement", AdoptingRun: "replacement-run",
		ProjectID: "coupon", IdempotencyKey: "adopt-once",
	}); err != nil {
		t.Fatal(err)
	}
	sent, err := db.Send(ctx, bus.SendRequest{
		IdempotencyKey: "future-duplicate", ProjectID: "coupon", ChannelID: "direct",
		FromActor: "sender", FromRun: "run", ToActors: []string{"reviewer-old", "replacement"},
		Type: "MESSAGE", DeliveryRequest: bus.DeliveryWake, Body: json.RawMessage(`{"text":"future"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	inbox, err := db.CheckInbox(ctx, "replacement", 10)
	if err != nil {
		t.Fatal(err)
	}
	seen := 0
	for _, item := range inbox {
		if item.MessageID == sent.Message.ID {
			seen++
			if item.OriginalRecipientActor != "" {
				t.Fatalf("direct delivery should win duplicate provenance: %+v", item)
			}
		}
	}
	if seen != 1 {
		t.Fatalf("future duplicate appeared %d times in %+v", seen, inbox)
	}
	job, err := db.ClaimNotification(ctx)
	if err != nil || job.Message.ID != sent.Message.ID || job.RecipientActor != "replacement" {
		t.Fatalf("adopted notification = %+v, err = %v", job, err)
	}
}

func TestActorAdoptionRearmsTerminalSourceWakeForLiveReplacement(t *testing.T) {
	ctx := context.Background()
	db, _ := openTestStore(t)
	registerLiveActor(t, db, "replacement", "replacement-run")
	sent, err := db.Send(ctx, bus.SendRequest{
		IdempotencyKey: "terminal-source-wake", ProjectID: "test", ChannelID: "direct",
		FromActor: "sender", FromRun: "sender-run", ToActors: []string{"reviewer-old"}, Type: "MESSAGE",
		DeliveryRequest: bus.DeliveryWake, Body: json.RawMessage(`{"text":"wake replacement"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	job, err := db.ClaimNotification(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.FinishNotification(ctx, job, bus.NotificationComplete, "source offline"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.AdoptActor(ctx, bus.AdoptRequest{
		SourceActor: "reviewer-old", AdoptingActor: "replacement", AdoptingRun: "replacement-run",
		ProjectID: "test", IdempotencyKey: "rearm-source-wake",
	}); err != nil {
		t.Fatal(err)
	}
	rearmed, err := db.ClaimNotification(ctx)
	if err != nil || rearmed.Message.ID != sent.Message.ID || rearmed.RecipientActor != "replacement" {
		t.Fatalf("rearmed notification = %+v, err = %v", rearmed, err)
	}
}

func TestActorAdoptionEnforcesLivenessClaimsAndOneWinner(t *testing.T) {
	ctx := context.Background()
	db, _ := openTestStore(t)
	registerLiveActor(t, db, "replacement-a", "run-a")
	registerLiveActor(t, db, "replacement-b", "run-b")
	registerLiveActor(t, db, "source-live", "source-run")
	for index, source := range []string{"source-live", "source-busy", "source-race"} {
		if _, err := db.Send(ctx, bus.SendRequest{
			IdempotencyKey: "seed-" + source, ProjectID: "test", ChannelID: "direct",
			FromActor: "sender", FromRun: "run", ToActors: []string{source}, Type: "MESSAGE",
			Body: json.RawMessage(`{"text":"work"}`),
		}); err != nil {
			t.Fatal(index, err)
		}
	}
	if _, err := db.AdoptActor(ctx, bus.AdoptRequest{
		SourceActor: "source-live", AdoptingActor: "replacement-a", AdoptingRun: "run-a",
		ProjectID: "test", IdempotencyKey: "live-refused",
	}); !errors.Is(err, bus.ErrActorLive) {
		t.Fatalf("live source error = %v", err)
	}
	busyClaim, err := db.Claim(ctx, "source-busy", "", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.AdoptActor(ctx, bus.AdoptRequest{
		SourceActor: "source-busy", AdoptingActor: "replacement-a", AdoptingRun: "run-a",
		ProjectID: "test", IdempotencyKey: "busy-refused",
	}); !errors.Is(err, bus.ErrAdoptionBusy) {
		t.Fatalf("active claim error = %v", err)
	}
	if err := db.Nack(ctx, "source-busy", busyClaim.Message.ID, busyClaim.LeaseToken, "release", false); err != nil {
		t.Fatal(err)
	}

	start := make(chan struct{})
	results := make(chan error, 2)
	var wait sync.WaitGroup
	for _, target := range []string{"replacement-a", "replacement-b"} {
		target := target
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			_, adoptErr := db.AdoptActor(ctx, bus.AdoptRequest{
				SourceActor: "source-race", AdoptingActor: target, AdoptingRun: "run-" + target,
				ProjectID: "test", IdempotencyKey: "race-" + target,
			})
			results <- adoptErr
		}()
	}
	close(start)
	wait.Wait()
	close(results)
	successes, conflicts := 0, 0
	for err := range results {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, bus.ErrAdoptionConflict):
			conflicts++
		default:
			t.Fatalf("unexpected adoption race error: %v", err)
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("race outcomes: successes=%d conflicts=%d", successes, conflicts)
	}
}

func TestActorAdoptionRejectsChainsThatWouldOrphanForwardedMessages(t *testing.T) {
	ctx := context.Background()
	db, _ := openTestStore(t)
	registerLiveActor(t, db, "middle", "middle-run")
	registerLiveActor(t, db, "replacement", "replacement-run")
	for _, source := range []string{"old", "middle", "other"} {
		if _, err := db.Send(ctx, bus.SendRequest{
			IdempotencyKey: "seed-" + source, ProjectID: "test", ChannelID: "direct",
			FromActor: "sender", FromRun: "sender-run", ToActors: []string{source}, Type: "MESSAGE",
			Body: json.RawMessage(`{"text":"work"}`),
		}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.AdoptActor(ctx, bus.AdoptRequest{
		SourceActor: "old", AdoptingActor: "middle", AdoptingRun: "middle-run",
		ProjectID: "test", IdempotencyKey: "old-to-middle",
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.ExpireRegistration(ctx, "middle", "middle-run", "middle-session", "test chain"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.AdoptActor(ctx, bus.AdoptRequest{
		SourceActor: "middle", AdoptingActor: "replacement", AdoptingRun: "replacement-run",
		ProjectID: "test", IdempotencyKey: "middle-to-replacement",
	}); !errors.Is(err, bus.ErrInvalid) {
		t.Fatalf("source adoption chain error = %v", err)
	}
	registerLiveActor(t, db, "old", "old-new-run")
	if _, err := db.AdoptActor(ctx, bus.AdoptRequest{
		SourceActor: "other", AdoptingActor: "old", AdoptingRun: "old-new-run",
		ProjectID: "test", IdempotencyKey: "other-to-old",
	}); !errors.Is(err, bus.ErrInvalid) {
		t.Fatalf("target adoption chain error = %v", err)
	}
}

func registerLiveActor(t *testing.T, db interface {
	RegisterSession(context.Context, bus.RegistrationRequest) (bus.Registration, error)
}, actor, runID string) {
	t.Helper()
	if _, err := db.RegisterSession(context.Background(), bus.RegistrationRequest{
		Actor: actor, RunID: runID, Harness: "test", SessionID: actor + "-session",
		ProjectID: "test", Lease: time.Hour,
	}); err != nil {
		t.Fatal(err)
	}
}
