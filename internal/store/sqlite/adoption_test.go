package sqlite_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/72olabs/holler/internal/bus"
	store "github.com/72olabs/holler/internal/store/sqlite"
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
	for _, target := range []struct{ actor, run string }{{"replacement-a", "run-a"}, {"replacement-b", "run-b"}} {
		target := target
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			_, adoptErr := db.AdoptActor(ctx, bus.AdoptRequest{
				SourceActor: "source-race", AdoptingActor: target.actor, AdoptingRun: target.run,
				ProjectID: "test", IdempotencyKey: "race-" + target.actor,
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

func TestActorAdoptionRequiresTheExactAdoptingRunToBeLive(t *testing.T) {
	ctx := context.Background()
	db, _ := openTestStore(t)
	registerLiveActor(t, db, "replacement", "live-run")
	if _, err := db.Send(ctx, bus.SendRequest{
		IdempotencyKey: "exact-run-seed", ProjectID: "test", ChannelID: "direct",
		FromActor: "sender", FromRun: "sender-run", ToActors: []string{"orphan"},
		Type: "MESSAGE", Body: json.RawMessage(`{"text":"work"}`),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.AdoptActor(ctx, bus.AdoptRequest{
		SourceActor: "orphan", AdoptingActor: "replacement", AdoptingRun: "stale-run",
		ProjectID: "test", IdempotencyKey: "wrong-run",
	}); !errors.Is(err, bus.ErrRunNotLive) {
		t.Fatalf("wrong adopting run error = %v", err)
	}
	if inbox, err := db.CheckInbox(ctx, "orphan", 10); err != nil || len(inbox) != 1 {
		t.Fatalf("source inbox changed after rejected adoption: %+v, err = %v", inbox, err)
	}
}

func TestAdoptedSourceCannotReviveExpiredPresenceOrAuthorData(t *testing.T) {
	ctx := context.Background()
	clock := &fakeClock{now: time.Date(2026, 8, 31, 18, 0, 0, 0, time.UTC)}
	db, _ := openTestStore(t, store.WithClock(clock.Now))
	if _, err := db.RegisterSession(ctx, bus.RegistrationRequest{
		Actor: "reviewer-old", RunID: "old-run", Harness: "claude", AttentionMode: "hook-long-poll",
		SessionID: "old-session", ProjectID: "coupon", Lease: time.Minute,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Send(ctx, bus.SendRequest{
		IdempotencyKey: "expired-presence-seed", ProjectID: "coupon", ChannelID: "direct",
		FromActor: "sender", FromRun: "sender-run", ToActors: []string{"reviewer-old"},
		Type: "MESSAGE", Body: json.RawMessage(`{"text":"handoff"}`),
	}); err != nil {
		t.Fatal(err)
	}
	clock.Advance(2 * time.Minute)
	registerLiveActor(t, db, "replacement", "replacement-run")
	if _, err := db.AdoptActor(ctx, bus.AdoptRequest{
		SourceActor: "reviewer-old", AdoptingActor: "replacement", AdoptingRun: "replacement-run",
		ProjectID: "coupon", IdempotencyKey: "expired-presence-adoption",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.HeartbeatRegistrations(ctx, "reviewer-old", "old-run", time.Hour); !errors.Is(err, bus.ErrActorAdopted) {
		t.Fatalf("adopted source heartbeat error = %v", err)
	}
	if _, err := db.AttachMonitor(ctx, "reviewer-old", "old-run", "old-session", "claude", "hook-long-poll", time.Hour); !errors.Is(err, bus.ErrActorAdopted) {
		t.Fatalf("adopted source monitor attach error = %v", err)
	}
	if live, err := db.LiveRegistrations(ctx, "reviewer-old"); err != nil || len(live) != 0 {
		t.Fatalf("retired source presence revived: %+v, err = %v", live, err)
	}
	if _, err := db.Send(ctx, bus.SendRequest{
		IdempotencyKey: "retired-send", ProjectID: "coupon", ChannelID: "direct",
		FromActor: "reviewer-old", FromRun: "old-run", ToActors: []string{"peer"},
		Type: "MESSAGE", Body: json.RawMessage(`{"text":"must fail"}`),
	}); !errors.Is(err, bus.ErrActorAdopted) {
		t.Fatalf("adopted source send error = %v", err)
	}
	if _, err := db.SetActorProfile(ctx, "reviewer-old", "old-run", "coupon", bus.ActorProfileRequest{
		RoleText: "retired actor",
	}); !errors.Is(err, bus.ErrActorAdopted) {
		t.Fatalf("adopted source profile error = %v", err)
	}
	if err := db.RecordHydration(ctx, "coupon", "reviewer-old", "old-run", "claude", "old-session", 0); !errors.Is(err, bus.ErrActorAdopted) {
		t.Fatalf("adopted source hydration error = %v", err)
	}
}

func TestAdoptedSourceIdentityIsFencedAndContinuityMovesToFreshActor(t *testing.T) {
	ctx := context.Background()
	db, _ := openTestStore(t)
	handles := []string{"process:codex:old-run", "session:codex:old-session"}
	source, err := db.BindActor(ctx, bus.ActorBindRequest{
		RequestedActor: "reviewer-old", RunID: "old-run", NameMode: bus.NameModeAllocate,
		ContinuityHandles: handles, ProjectID: "coupon",
	})
	if err != nil {
		t.Fatalf("source binding = %+v, err = %v", source, err)
	}
	assertOpaqueActor(t, source.Actor, "reviewer-old")
	if _, err := db.RegisterSession(ctx, bus.RegistrationRequest{
		Actor: source.Actor, RunID: "old-run", Harness: "codex", SessionID: "old-session",
		ProjectID: "coupon", Lease: time.Hour,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Send(ctx, bus.SendRequest{
		IdempotencyKey: "fenced-source-seed", ProjectID: "coupon", ChannelID: "direct",
		FromActor: "sender", FromRun: "sender-run", ToActors: []string{source.Actor},
		Type: "MESSAGE", Body: json.RawMessage(`{"text":"handoff"}`),
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.ExpireRegistration(ctx, source.Actor, "old-run", "old-session", "crashed"); err != nil {
		t.Fatal(err)
	}
	registerLiveActor(t, db, "replacement", "replacement-run")
	if _, err := db.AdoptActor(ctx, bus.AdoptRequest{
		SourceActor: source.Actor, AdoptingActor: "replacement", AdoptingRun: "replacement-run",
		ProjectID: "coupon", IdempotencyKey: "terminal-handoff",
	}); err != nil {
		t.Fatal(err)
	}

	if _, err := db.BindActor(ctx, bus.ActorBindRequest{
		RequestedActor: source.Actor, RunID: "exact-reuse", NameMode: bus.NameModeExact,
	}); !errors.Is(err, bus.ErrActorAdopted) {
		t.Fatalf("exact adopted-source bind error = %v", err)
	}
	if _, err := db.RegisterSession(ctx, bus.RegistrationRequest{
		Actor: source.Actor, RunID: "legacy-reuse", Harness: "test", SessionID: "legacy-reuse",
		Lease: time.Hour,
	}); !errors.Is(err, bus.ErrActorAdopted) {
		t.Fatalf("adopted-source registration error = %v", err)
	}

	freshHandles := []string{"process:codex:fresh-run", "session:codex:old-session"}
	fresh, err := db.BindActor(ctx, bus.ActorBindRequest{
		RequestedActor: source.Actor, RunID: "fresh-run", NameMode: bus.NameModeAllocate,
		ContinuityHandles: freshHandles, ProjectID: "coupon",
	})
	if err != nil {
		t.Fatal(err)
	}
	assertOpaqueActor(t, fresh.Actor, "reviewer-old")
	if fresh.Actor == source.Actor || fresh.AdoptedPredecessor != source.Actor ||
		fresh.ContinuityReclaimed || !fresh.Minted || fresh.Provisional {
		t.Fatalf("fresh binding after adoption = %+v", fresh)
	}
	if current, err := db.CurrentActorForContinuity(ctx, freshHandles); err != nil || current != fresh.Actor {
		t.Fatalf("repointed continuity actor = %q, err = %v", current, err)
	}
	if _, err := db.RegisterSession(ctx, bus.RegistrationRequest{
		Actor: fresh.Actor, RunID: "fresh-run", Harness: "codex", SessionID: "old-session",
		ProjectID: "coupon", Lease: time.Hour,
	}); err != nil {
		t.Fatal(err)
	}
	if inbox, err := db.CheckInbox(ctx, fresh.Actor, 10); err != nil || len(inbox) != 0 {
		t.Fatalf("fresh actor inherited adopted inbox: %+v, err = %v", inbox, err)
	}
	if inbox, err := db.CheckInbox(ctx, "replacement", 10); err != nil || len(inbox) != 1 {
		t.Fatalf("adopter lost source inbox: %+v, err = %v", inbox, err)
	}

	if err := db.ExpireRegistration(ctx, "replacement", "replacement-run", "replacement-session", "restart"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.BindActor(ctx, bus.ActorBindRequest{
		RequestedActor: "replacement", RunID: "replacement-resume", NameMode: bus.NameModeExact,
	}); err != nil {
		t.Fatalf("adopter name reuse bind: %v", err)
	}
	if _, err := db.RegisterSession(ctx, bus.RegistrationRequest{
		Actor: "replacement", RunID: "replacement-resume", Harness: "test", SessionID: "replacement-resume",
		ProjectID: "coupon", Lease: time.Hour,
	}); err != nil {
		t.Fatalf("adopter name reuse registration: %v", err)
	}
	if inbox, err := db.CheckInbox(ctx, "replacement", 10); err != nil || len(inbox) != 1 || inbox[0].OriginalRecipientActor != source.Actor {
		t.Fatalf("resumed adopter inbox = %+v, err = %v", inbox, err)
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
