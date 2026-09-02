package sqlite_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/72olabs/holler/internal/bus"
	store "github.com/72olabs/holler/internal/store/sqlite"
)

func TestConcurrentAliasRepointsSerializeWithHistory(t *testing.T) {
	ctx := context.Background()
	db, err := store.Open(ctx, filepath.Join(t.TempDir(), "holler.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	registerAliasActor(t, db, "claude", "claude-run")
	registerAliasActor(t, db, "claude-2", "claude-2-run")
	registerAliasActor(t, db, "claude-3", "claude-3-run")
	if _, err := db.SetAlias(ctx, aliasSet("reviewer", "claude", "initial")); err != nil {
		t.Fatal(err)
	}

	start := make(chan struct{})
	type outcome struct {
		result bus.AliasMutationResult
		err    error
	}
	outcomes := make(chan outcome, 2)
	for index, actor := range []string{"claude-2", "claude-3"} {
		index, actor := index, actor
		go func() {
			<-start
			result, err := db.SetAlias(ctx, aliasSet("reviewer", actor, "concurrent-"+string(rune('a'+index))))
			outcomes <- outcome{result: result, err: err}
		}()
	}
	close(start)
	seenRevisions := map[int64]bool{}
	for range 2 {
		outcome := <-outcomes
		if outcome.err != nil {
			t.Fatal(outcome.err)
		}
		seenRevisions[outcome.result.Alias.Revision] = true
	}
	if !seenRevisions[2] || !seenRevisions[3] {
		t.Fatalf("serialized revisions = %+v", seenRevisions)
	}
	resolved, err := db.ResolveAlias(ctx, "reviewer")
	if err != nil || resolved.Revision != 3 || (resolved.Actor != "claude-2" && resolved.Actor != "claude-3") {
		t.Fatalf("final alias = %+v, err = %v", resolved, err)
	}
}

func TestClaimAliasIfAbsentHasOneWinnerAndDurableLoser(t *testing.T) {
	ctx := context.Background()
	db, err := store.Open(ctx, filepath.Join(t.TempDir(), "holler.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	registerAliasActor(t, db, "claude-a7f3c2", "claude-a-run")
	registerAliasActor(t, db, "claude-b81d90", "claude-b-run")

	type outcome struct {
		request bus.AliasClaimRequest
		result  bus.AliasClaimResult
		err     error
	}
	start := make(chan struct{})
	outcomes := make(chan outcome, 2)
	for index, actor := range []string{"claude-a7f3c2", "claude-b81d90"} {
		request := bus.AliasClaimRequest{
			Alias: "coupon-claude", Actor: actor, PolicyID: "setup:default-workstream-alias",
			Harness:        "claude",
			UpdatedByActor: actor, UpdatedByRun: fmt.Sprintf("run-%d", index), ProjectID: "coupon",
			IdempotencyKey: fmt.Sprintf("claim-%d", index),
		}
		go func() {
			<-start
			result, err := db.ClaimAliasIfAbsent(ctx, request)
			outcomes <- outcome{request: request, result: result, err: err}
		}()
	}
	close(start)
	results := []outcome{<-outcomes, <-outcomes}
	claimed := 0
	var winner string
	for _, result := range results {
		if result.err != nil {
			t.Fatal(result.err)
		}
		if result.result.Claimed {
			claimed++
		}
		if winner == "" {
			winner = result.result.Alias.Actor
		} else if result.result.Alias.Actor != winner {
			t.Fatalf("claim outcomes disagree: %+v", results)
		}
	}
	if claimed != 1 {
		t.Fatalf("claimed outcomes = %d, want one: %+v", claimed, results)
	}

	var loser outcome
	for _, result := range results {
		if !result.result.Claimed {
			loser = result
		}
	}
	retry, err := db.ClaimAliasIfAbsent(ctx, loser.request)
	if err != nil || retry.Claimed || !retry.DuplicateRequest || retry.Alias.Actor != winner {
		t.Fatalf("durable loser retry = %+v, err = %v", retry, err)
	}
	changed := loser.request
	changed.Actor = winner
	if _, err := db.ClaimAliasIfAbsent(ctx, changed); !errors.Is(err, bus.ErrIdempotencyConflict) {
		t.Fatalf("changed loser retry error = %v", err)
	}
}

func TestConcurrentAliasRepointAndSendStampOneAtomicTarget(t *testing.T) {
	ctx := context.Background()
	db, err := store.Open(ctx, filepath.Join(t.TempDir(), "holler.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	registerAliasActor(t, db, "claude", "claude-run")
	registerAliasActor(t, db, "claude-2", "claude-2-run")

	for iteration := range 50 {
		alias := fmt.Sprintf("reviewer-%d", iteration)
		if _, err := db.SetAlias(ctx, aliasSet(alias, "claude", fmt.Sprintf("initial-%d", iteration))); err != nil {
			t.Fatal(err)
		}

		start := make(chan struct{})
		repointed := make(chan error, 1)
		sent := make(chan struct {
			result bus.SendResult
			err    error
		}, 1)
		go func() {
			<-start
			_, err := db.SetAlias(ctx, aliasSet(alias, "claude-2", fmt.Sprintf("repoint-%d", iteration)))
			repointed <- err
		}()
		go func() {
			<-start
			result, err := db.Send(ctx, bus.SendRequest{
				IdempotencyKey: fmt.Sprintf("send-%d", iteration), ProjectID: "default", ChannelID: "direct",
				FromActor: "codex", FromRun: "codex-run", ToActors: []string{alias},
				Type: "MESSAGE", DeliveryRequest: bus.DeliveryNonBlocking, Body: json.RawMessage(`{"text":"review"}`),
			})
			sent <- struct {
				result bus.SendResult
				err    error
			}{result: result, err: err}
		}()
		close(start)

		if err := <-repointed; err != nil {
			t.Fatalf("iteration %d repoint: %v", iteration, err)
		}
		outcome := <-sent
		if outcome.err != nil {
			t.Fatalf("iteration %d send: %v", iteration, outcome.err)
		}
		if len(outcome.result.Message.ToActors) != 1 ||
			(outcome.result.Message.ToActors[0] != "claude" && outcome.result.Message.ToActors[0] != "claude-2") {
			t.Fatalf("iteration %d stamped recipients = %v", iteration, outcome.result.Message.ToActors)
		}
		if len(outcome.result.Message.RequestedToActors) != 1 || outcome.result.Message.RequestedToActors[0] != alias {
			t.Fatalf("iteration %d requested recipients = %v", iteration, outcome.result.Message.RequestedToActors)
		}
		for _, actor := range []string{"claude", "claude-2"} {
			items, err := db.CheckInbox(ctx, actor, 100)
			if err != nil {
				t.Fatal(err)
			}
			found := 0
			for _, item := range items {
				if item.MessageID == outcome.result.Message.ID {
					found++
				}
			}
			if want := outcome.result.Message.ToActors[0] == actor; (found == 1) != want {
				t.Fatalf("iteration %d actor %s delivery count = %d, stamped target = %s",
					iteration, actor, found, outcome.result.Message.ToActors[0])
			}
		}
	}
}

func TestAliasResolvesBeforeImmutableRecipientStamp(t *testing.T) {
	ctx := context.Background()
	db, err := store.Open(ctx, filepath.Join(t.TempDir(), "holler.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	registerAliasActor(t, db, "claude", "claude-run")

	set, err := db.SetAlias(ctx, bus.AliasSetRequest{
		Alias: "skillbank", Actor: "claude", UpdatedByActor: "operator", UpdatedByRun: "operator-run",
		ProjectID: "default", IdempotencyKey: "alias-skillbank-v1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if set.Alias.Actor != "claude" || set.Alias.Revision != 1 || set.DuplicateRequest {
		t.Fatalf("set alias = %+v", set)
	}
	if err := db.ExpireRegistration(ctx, "claude", "claude-run", "claude-run-session", "offline-target-test"); err != nil {
		t.Fatal(err)
	}

	sent, err := db.Send(ctx, bus.SendRequest{
		IdempotencyKey: "send-through-alias", ProjectID: "default", ChannelID: "direct",
		FromActor: "codex", FromRun: "codex-run", ToActors: []string{"skillbank"},
		Type: "MESSAGE", DeliveryRequest: bus.DeliveryNonBlocking, Body: json.RawMessage(`{"text":"review"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(sent.Message.ToActors) != 1 || sent.Message.ToActors[0] != "claude" {
		t.Fatalf("stamped recipients = %v", sent.Message.ToActors)
	}
	items, err := db.CheckInbox(ctx, "claude", 10)
	if err != nil || len(items) != 1 || items[0].MessageID != sent.Message.ID {
		t.Fatalf("canonical inbox = %+v, err = %v", items, err)
	}
	if items, err := db.CheckInbox(ctx, "skillbank", 10); err != nil || len(items) != 0 {
		t.Fatalf("alias must not own an inbox: %+v, err = %v", items, err)
	}
	events, err := db.ListEvents(ctx, "default", "durable", 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	var provenance struct {
		AliasResolution map[string]string `json:"alias_resolution"`
		Recipients      []string          `json:"recipients"`
	}
	if len(events) != 2 || events[1].Kind != "message.sent" || json.Unmarshal(events[1].Payload, &provenance) != nil ||
		provenance.AliasResolution["skillbank"] != "claude" || len(provenance.Recipients) != 1 || provenance.Recipients[0] != "claude" {
		t.Fatalf("message provenance = events:%+v payload:%+v", events, provenance)
	}
}

func TestAliasRepointDoesNotRetargetIdempotentSendRetry(t *testing.T) {
	ctx := context.Background()
	db, err := store.Open(ctx, filepath.Join(t.TempDir(), "holler.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	registerAliasActor(t, db, "claude", "claude-run")
	registerAliasActor(t, db, "claude-2", "claude-2-run")

	firstSet, err := db.SetAlias(ctx, aliasSet("skillbank", "claude", "set-1"))
	if err != nil {
		t.Fatal(err)
	}
	request := bus.SendRequest{
		IdempotencyKey: "stable-send", ProjectID: "default", ChannelID: "direct",
		FromActor: "codex", FromRun: "codex-run", ToActors: []string{"skillbank"},
		Type: "MESSAGE", DeliveryRequest: bus.DeliveryNonBlocking, Body: json.RawMessage(`{"text":"review"}`),
	}
	firstSend, err := db.Send(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	secondSet, err := db.SetAlias(ctx, aliasSet("skillbank", "claude-2", "set-2"))
	if err != nil {
		t.Fatal(err)
	}
	if firstSet.Alias.Revision != 1 || secondSet.Alias.Revision != 2 {
		t.Fatalf("alias revisions = %d, %d", firstSet.Alias.Revision, secondSet.Alias.Revision)
	}
	retry, err := db.Send(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if !retry.Duplicate || retry.Message.ID != firstSend.Message.ID || retry.Message.ToActors[0] != "claude" {
		t.Fatalf("retry = %+v, first = %+v", retry, firstSend)
	}
	secondSend := request
	secondSend.IdempotencyKey = "new-send"
	second, err := db.Send(ctx, secondSend)
	if err != nil {
		t.Fatal(err)
	}
	if second.Message.ToActors[0] != "claude-2" {
		t.Fatalf("new send recipient = %v", second.Message.ToActors)
	}
}

func TestAliasMutationIsIdempotentAndCollisionSafe(t *testing.T) {
	ctx := context.Background()
	db, err := store.Open(ctx, filepath.Join(t.TempDir(), "holler.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	registerAliasActor(t, db, "claude", "claude-run")

	request := aliasSet("skillbank", "claude", "set-once")
	first, err := db.SetAlias(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	retry, err := db.SetAlias(ctx, request)
	if err != nil || !retry.DuplicateRequest || retry.Alias.Revision != first.Alias.Revision {
		t.Fatalf("idempotent retry = %+v, err = %v", retry, err)
	}
	resumed := request
	resumed.UpdatedByRun = "operator-run-2"
	if retry, err := db.SetAlias(ctx, resumed); err != nil || !retry.DuplicateRequest || retry.Alias.UpdatedByRun != "operator-run" {
		t.Fatalf("cross-run idempotent retry = %+v, err = %v", retry, err)
	}
	conflict := request
	conflict.Actor = "somewhere-else"
	if _, err := db.SetAlias(ctx, conflict); !errors.Is(err, bus.ErrIdempotencyConflict) {
		t.Fatalf("changed retry error = %v", err)
	}
	if _, err := db.SetAlias(ctx, aliasSet("claude", "claude", "actor-shadow")); !errors.Is(err, bus.ErrAliasConflict) {
		t.Fatalf("actor shadow error = %v", err)
	}
	if _, err := db.SetAlias(ctx, aliasSet("operator", "claude", "operator-shadow")); !errors.Is(err, bus.ErrAliasConflict) {
		t.Fatalf("operator shadow error = %v", err)
	}
	if _, err := db.Send(ctx, bus.SendRequest{
		IdempotencyKey: "typed-actor-shadow", ProjectID: "default", ChannelID: "direct",
		FromActor: "sender", FromRun: "run", Destinations: []bus.Route{{Kind: bus.RouteActor, Value: "skillbank"}},
		Type: "MESSAGE", Body: json.RawMessage(`{}`),
	}); !errors.Is(err, bus.ErrAliasConflict) {
		t.Fatalf("typed actor shadow error = %v", err)
	}
	if _, err := db.SetAlias(ctx, aliasSet("ghost", "missing", "unknown-target")); !errors.Is(err, bus.ErrAliasTargetUnknown) {
		t.Fatalf("unknown target error = %v", err)
	}
	if _, err := db.BindActor(ctx, bus.ActorBindRequest{
		RequestedActor: "skillbank", RunID: "collision-run", NameMode: bus.NameModeExact, ProjectID: "default",
	}); !errors.Is(err, bus.ErrAliasConflict) {
		t.Fatalf("exact actor collision error = %v", err)
	}
	removed, err := db.RemoveAlias(ctx, bus.AliasRemoveRequest{
		Alias: "skillbank", UpdatedByActor: "operator", UpdatedByRun: "operator-run",
		ProjectID: "default", IdempotencyKey: "remove-once",
	})
	if err != nil || !removed.Removed || removed.Alias.Revision != 2 || removed.Alias.Actor != "claude" {
		t.Fatalf("remove alias = %+v, err = %v", removed, err)
	}
	recreated, err := db.SetAlias(ctx, aliasSet("skillbank", "claude", "recreate"))
	if err != nil || recreated.Alias.Revision != 3 {
		t.Fatalf("recreate alias = %+v, err = %v", recreated, err)
	}
}

func TestAllocatedActorSkipsReservedAlias(t *testing.T) {
	ctx := context.Background()
	db, err := store.Open(ctx, filepath.Join(t.TempDir(), "holler.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	registerAliasActor(t, db, "reviewer", "reviewer-run")
	if _, err := db.SetAlias(ctx, aliasSet("claude", "reviewer", "reserve-claude")); err != nil {
		t.Fatal(err)
	}
	allocation, err := db.BindActor(ctx, bus.ActorBindRequest{
		RequestedActor: "claude", RunID: "claude-run", NameMode: bus.NameModeAllocate,
		ProjectID: "default", ContinuityHandles: []string{"session:allocated"},
	})
	if err != nil {
		t.Fatal(err)
	}
	assertOpaqueActor(t, allocation.Actor, "claude")
}

func TestTypedRoutesAndReplyProvenance(t *testing.T) {
	ctx := context.Background()
	db, err := store.Open(ctx, filepath.Join(t.TempDir(), "holler.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	registerAliasActor(t, db, "claude-a7f3c2", "claude-run")
	registerAliasActor(t, db, "claude-b81d90", "claude-2-run")
	if _, err := db.SetAlias(ctx, aliasSet("architect-claude", "claude-a7f3c2", "alias-v1")); err != nil {
		t.Fatal(err)
	}

	question, err := db.Send(ctx, bus.SendRequest{
		IdempotencyKey: "question", ProjectID: "default", ChannelID: "direct", ThreadID: "routing-thread",
		FromActor: "codex-a1b2c3", FromRun: "codex-run",
		Destinations: []bus.Route{{Kind: bus.RouteAlias, Value: "architect-claude"}},
		Type:         "MESSAGE", Body: json.RawMessage(`{"text":"review"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := question.Message.ToActors; len(got) != 1 || got[0] != "claude-a7f3c2" {
		t.Fatalf("canonical recipients = %v", got)
	}
	if got := question.Message.RequestedRoutes; len(got) != 1 || got[0] != (bus.Route{Kind: bus.RouteAlias, Value: "architect-claude"}) {
		t.Fatalf("requested routes = %+v", got)
	}

	if _, err := db.SetAlias(ctx, aliasSet("architect-claude", "claude-b81d90", "alias-v2")); err != nil {
		t.Fatal(err)
	}
	reply, err := db.Send(ctx, bus.SendRequest{
		IdempotencyKey: "reply", ProjectID: "default", ChannelID: "direct",
		FromActor: "claude-a7f3c2", FromRun: "claude-run", InReplyTo: question.Message.ID,
		Type: "MESSAGE", Body: json.RawMessage(`{"text":"done"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := reply.Message.ToActors; len(got) != 1 || got[0] != "codex-a1b2c3" {
		t.Fatalf("reply recipients = %v", got)
	}
	if got := reply.Message.RequestedRoutes; len(got) != 1 || got[0] != (bus.Route{Kind: bus.RouteReply, Value: question.Message.ID}) {
		t.Fatalf("reply provenance = %+v", got)
	}

	_, err = db.Send(ctx, bus.SendRequest{
		IdempotencyKey: "unsafe-legacy-reply", ProjectID: "default", ChannelID: "direct",
		FromActor: "claude-a7f3c2", FromRun: "claude-run", ToActors: []string{"architect-claude"},
		InReplyTo: question.Message.ID, Type: "MESSAGE", Body: json.RawMessage(`{"text":"wrong"}`),
	})
	if !errors.Is(err, bus.ErrInvalid) {
		t.Fatalf("legacy reply through repointed alias error = %v", err)
	}
}

func TestRemovedAliasTombstoneBlocksLegacyFallbackAndActorReuse(t *testing.T) {
	ctx := context.Background()
	db, err := store.Open(ctx, filepath.Join(t.TempDir(), "holler.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	registerAliasActor(t, db, "claude-a7f3c2", "claude-run")
	if _, err := db.SetAlias(ctx, aliasSet("architect-claude", "claude-a7f3c2", "alias-set")); err != nil {
		t.Fatal(err)
	}
	if _, err := db.RemoveAlias(ctx, bus.AliasRemoveRequest{
		Alias: "architect-claude", UpdatedByActor: "operator", UpdatedByRun: "operator-run",
		ProjectID: "default", IdempotencyKey: "alias-remove",
	}); err != nil {
		t.Fatal(err)
	}

	for name, request := range map[string]bus.SendRequest{
		"legacy": {
			IdempotencyKey: "legacy", ProjectID: "default", ChannelID: "direct", FromActor: "sender", FromRun: "run",
			ToActors: []string{"architect-claude"}, Type: "MESSAGE", Body: json.RawMessage(`{}`),
		},
		"typed": {
			IdempotencyKey: "typed", ProjectID: "default", ChannelID: "direct", FromActor: "sender", FromRun: "run",
			Destinations: []bus.Route{{Kind: bus.RouteAlias, Value: "architect-claude"}}, Type: "MESSAGE", Body: json.RawMessage(`{}`),
		},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := db.Send(ctx, request); !errors.Is(err, bus.ErrAliasTombstoned) {
				t.Fatalf("send error = %v", err)
			}
		})
	}
	if _, err := db.BindActor(ctx, bus.ActorBindRequest{
		RequestedActor: "architect-claude", RunID: "collision-run", NameMode: bus.NameModeExact, ProjectID: "default",
	}); !errors.Is(err, bus.ErrAliasConflict) {
		t.Fatalf("retired alias actor collision error = %v", err)
	}
	if _, err := db.Send(ctx, bus.SendRequest{
		IdempotencyKey: "retired-typed-actor-shadow", ProjectID: "default", ChannelID: "direct",
		FromActor: "sender", FromRun: "run", Destinations: []bus.Route{{Kind: bus.RouteActor, Value: "architect-claude"}},
		Type: "MESSAGE", Body: json.RawMessage(`{}`),
	}); !errors.Is(err, bus.ErrAliasConflict) {
		t.Fatalf("retired typed actor shadow error = %v", err)
	}
}

func TestAliasPreflightShowsBothSidesAndWholeActorImpact(t *testing.T) {
	ctx := context.Background()
	db, _ := openTestStore(t)
	for _, actor := range []string{"reviewer-old", "reviewer-new"} {
		if err := db.ReserveActorName(ctx, actor); err != nil {
			t.Fatal(err)
		}
	}
	for index, mapping := range []struct{ alias, actor string }{
		{"reviewer", "reviewer-old"}, {"legacy-reviewer", "reviewer-old"}, {"new-reviewer", "reviewer-new"},
	} {
		if _, err := db.SetAlias(ctx, bus.AliasSetRequest{
			Alias: mapping.alias, Actor: mapping.actor, UpdatedByActor: "operator", UpdatedByRun: "run",
			ProjectID: "default", IdempotencyKey: fmt.Sprintf("preflight-%d", index),
		}); err != nil {
			t.Fatal(err)
		}
	}
	preflight, err := db.AliasPreflight(ctx, "reviewer", "reviewer-new")
	if err != nil {
		t.Fatal(err)
	}
	if preflight.CurrentTarget != "reviewer-old" || !preflight.WholeActorAdoption ||
		!reflect.DeepEqual(preflight.AliasesOnPredecessor, []string{"legacy-reviewer", "reviewer"}) ||
		!reflect.DeepEqual(preflight.AliasesOnProposed, []string{"new-reviewer"}) || preflight.Predecessor == nil {
		t.Fatalf("alias preflight = %+v", preflight)
	}
}

func aliasSet(alias, actor, key string) bus.AliasSetRequest {
	return bus.AliasSetRequest{
		Alias: alias, Actor: actor, UpdatedByActor: "operator", UpdatedByRun: "operator-run",
		ProjectID: "default", IdempotencyKey: key,
	}
}

func registerAliasActor(t *testing.T, db *store.Store, actor, run string) {
	t.Helper()
	if _, err := db.RegisterSession(context.Background(), bus.RegistrationRequest{
		Actor: actor, RunID: run, Harness: "test", SessionID: run + "-session",
		ProjectID: "default", Lease: time.Hour,
	}); err != nil {
		t.Fatal(err)
	}
}
