package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/72olabs/holler/internal/api"
	"github.com/72olabs/holler/internal/attention"
	"github.com/72olabs/holler/internal/bus"
	"github.com/72olabs/holler/internal/daemon"
	store "github.com/72olabs/holler/internal/store/sqlite"
)

func TestUnixAPIMessageLifecycleAndSessionIdentity(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	db, socket := startServer(t, ctx, cancel)
	_ = db
	alice := dial(t, socket, "alice", "alice-run")
	bob := dial(t, socket, "bob", "bob-run")

	sent, err := alice.Send(ctx, bus.SendRequest{
		IdempotencyKey: "api-1", ProjectID: "experiment", ChannelID: "direct",
		FromActor: "mallory", FromRun: "spoofed-run", ToActors: []string{"bob"},
		Type: "QUESTION", DeliveryRequest: bus.DeliveryWake,
		Body: json.RawMessage(`{"text":"Which policy?"}`),
	})
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	if sent.Message.FromActor != "alice" || sent.Message.FromRun != "alice-run" {
		t.Fatalf("server identity = %s/%s", sent.Message.FromActor, sent.Message.FromRun)
	}
	items, err := bob.CheckInbox(ctx, "bob", 10)
	if err != nil || len(items) != 1 {
		t.Fatalf("inbox = %+v, err = %v", items, err)
	}
	claim, err := bob.Claim(ctx, "bob", sent.Message.ID, time.Minute)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := bob.Ack(ctx, "bob", sent.Message.ID, claim.LeaseToken); err != nil {
		t.Fatalf("ack: %v", err)
	}
	items, err = bob.CheckInbox(ctx, "bob", 10)
	if err != nil || len(items) != 0 {
		t.Fatalf("inbox after ack = %+v, err = %v", items, err)
	}
	if _, err := bob.CheckInbox(ctx, "mallory", 10); err == nil {
		t.Fatal("client accepted actor different from bound session")
	}
}

func TestUnixAPIAliasMutationResolutionAndActorCollision(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	_, socket := startServer(t, ctx, cancel)
	claude := dial(t, socket, "claude", "claude-run")
	if _, err := claude.RegisterSession(ctx, bus.RegistrationRequest{
		Actor: "claude", RunID: "claude-run", Harness: "test", SessionID: "claude-session",
		ProjectID: "default", Lease: time.Hour,
	}); err != nil {
		t.Fatal(err)
	}
	operator := dial(t, socket, "operator", "operator-run")
	hidden := dial(t, socket, "hidden-live-actor", "hidden-run")
	defer hidden.Close()
	if _, err := operator.SetAlias(ctx, bus.AliasSetRequest{
		Alias: "hidden-live-actor", Actor: "claude", ProjectID: "default", IdempotencyKey: "hidden-collision",
	}); !errors.Is(err, bus.ErrAliasConflict) {
		t.Fatalf("connected actor collision error = %v", err)
	}
	alias, err := operator.SetAlias(ctx, bus.AliasSetRequest{
		Alias: "skillbank", Actor: "claude", ProjectID: "default", IdempotencyKey: "api-alias-set",
	})
	if err != nil || alias.Alias.Actor != "claude" {
		t.Fatalf("set alias = %+v, err = %v", alias, err)
	}
	resolved, err := operator.ResolveAlias(ctx, "skillbank")
	if err != nil || resolved.Actor != "claude" {
		t.Fatalf("resolve alias = %+v, err = %v", resolved, err)
	}
	sender := dial(t, socket, "codex", "codex-run")
	sent, err := sender.Send(ctx, bus.SendRequest{
		IdempotencyKey: "api-alias-send", ProjectID: "default", ChannelID: "direct",
		ToActors: []string{"skillbank"}, Type: "MESSAGE", Body: json.RawMessage(`{"text":"review"}`),
	})
	if err != nil || len(sent.Message.ToActors) != 1 || sent.Message.ToActors[0] != "claude" {
		t.Fatalf("alias send = %+v, err = %v", sent, err)
	}
	if _, err := api.Dial(ctx, socket, api.Identity{Actor: "skillbank", RunID: "shadow-run", Client: "test"}); !errors.Is(err, bus.ErrAliasConflict) {
		t.Fatalf("alias actor collision error = %v", err)
	}
}

func TestUnixAPIAdoptsInactiveInboxWithConnectionBoundIdentity(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	_, socket := startServer(t, ctx, cancel)
	sender := dial(t, socket, "sender", "sender-run")
	replacement := dial(t, socket, "replacement", "replacement-run")
	if _, err := replacement.RegisterSession(ctx, bus.RegistrationRequest{
		Actor: "replacement", RunID: "replacement-run", Harness: "test", SessionID: "replacement-session",
		ProjectID: "coupon", Lease: time.Hour,
	}); err != nil {
		t.Fatal(err)
	}
	sent, err := sender.Send(ctx, bus.SendRequest{
		IdempotencyKey: "api-orphan", ProjectID: "coupon", ChannelID: "direct",
		ToActors: []string{"reviewer-old"}, Type: "REVIEW_REQUEST", Body: json.RawMessage(`{"text":"review"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	adopted, err := replacement.AdoptActor(ctx, bus.AdoptRequest{
		SourceActor: "reviewer-old", AdoptingActor: "spoofed", AdoptingRun: "spoofed-run",
		ProjectID: "coupon", IdempotencyKey: "api-adopt-once",
	})
	if err != nil {
		t.Fatal(err)
	}
	if adopted.AdoptingActor != "replacement" || adopted.AdoptingRun != "replacement-run" || adopted.Transferred != 1 {
		t.Fatalf("adoption = %+v", adopted)
	}
	items, err := replacement.CheckInbox(ctx, "replacement", 10)
	if err != nil || len(items) != 1 || items[0].MessageID != sent.Message.ID || items[0].OriginalRecipientActor != "reviewer-old" {
		t.Fatalf("adopted inbox = %+v, err = %v", items, err)
	}
	claim, err := replacement.Claim(ctx, "replacement", sent.Message.ID, time.Minute)
	if err != nil || claim.OriginalRecipientActor != "reviewer-old" {
		t.Fatalf("adopted claim = %+v, err = %v", claim, err)
	}
}

func TestUnixAPIAdoptionFencesSourceAndReportsFreshContinuityIdentity(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	db, socket := startServer(t, ctx, cancel, api.WithAttentionBroker(attention.NewBroker()))
	handles := []string{"process:claude:source-run", "session:claude:source-session"}
	source, err := api.Dial(ctx, socket, api.Identity{
		Actor: "reviewer", RunID: "source-run", Client: "test", NameMode: bus.NameModeAllocate,
		ContinuityHandles: handles, ProjectID: "coupon",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	if _, err := source.RegisterSession(ctx, bus.RegistrationRequest{
		Actor: "reviewer", RunID: "source-run", Harness: "claude", AttentionMode: "hook-long-poll",
		SessionID: "source-session", ProjectID: "coupon", Lease: 10 * time.Millisecond,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := source.Send(ctx, bus.SendRequest{
		IdempotencyKey: "api-fence-seed", ProjectID: "coupon", ChannelID: "direct",
		ToActors: []string{"reviewer"}, Type: "MESSAGE", Body: json.RawMessage(`{"text":"handoff"}`),
	}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	target := dial(t, socket, "replacement", "target-run")
	defer target.Close()
	if _, err := target.RegisterSession(ctx, bus.RegistrationRequest{
		Actor: "replacement", RunID: "target-run", Harness: "test", SessionID: "target-session",
		ProjectID: "coupon", Lease: time.Hour,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := target.AdoptActor(ctx, bus.AdoptRequest{
		SourceActor: "reviewer", ProjectID: "coupon", IdempotencyKey: "api-terminal-handoff",
	}); err != nil {
		t.Fatal(err)
	}
	if items, err := source.CheckInbox(ctx, "reviewer", 10); err != nil || len(items) != 0 {
		t.Fatalf("stale source inbox = %+v, err = %v", items, err)
	}
	if _, err := source.Claim(ctx, "reviewer", "", time.Minute); !errors.Is(err, bus.ErrNoMessage) {
		t.Fatalf("stale source claim error = %v", err)
	}
	if _, err := source.HeartbeatRegistrations(ctx, "reviewer", "source-run", time.Hour); !errors.Is(err, bus.ErrActorAdopted) {
		t.Fatalf("stale source heartbeat error = %v", err)
	}
	if _, err := source.MonitorAttach(ctx, "reviewer", "source-run", "source-session", "hook-long-poll", time.Hour); !errors.Is(err, bus.ErrActorAdopted) {
		t.Fatalf("stale source monitor attach error = %v", err)
	}
	if _, err := source.Send(ctx, bus.SendRequest{
		IdempotencyKey: "retired-authorship", ProjectID: "coupon", ChannelID: "direct",
		ToActors: []string{"peer"}, Type: "MESSAGE", Body: json.RawMessage(`{"text":"must fail"}`),
	}); !errors.Is(err, bus.ErrActorAdopted) {
		t.Fatalf("stale source send error = %v", err)
	}
	if _, err := source.SetActorProfile(ctx, "reviewer", "source-run", "coupon", bus.ActorProfileRequest{
		RoleText: "retired actor",
	}); !errors.Is(err, bus.ErrActorAdopted) {
		t.Fatalf("stale source profile error = %v", err)
	}
	if err := source.RecordHydration(ctx, "coupon", "reviewer", "source-run", "claude", "source-session", 0); !errors.Is(err, bus.ErrActorAdopted) {
		t.Fatalf("stale source hydration error = %v", err)
	}
	legacy, err := api.Dial(ctx, socket, api.Identity{
		Actor: "reviewer", RunID: "source-run", Client: "legacy-test",
	})
	if err != nil {
		t.Fatalf("legacy diagnostic connection: %v", err)
	}
	defer legacy.Close()
	if _, err := legacy.Who(ctx, 10); err != nil {
		t.Fatalf("legacy retired-source diagnostic: %v", err)
	}
	if err := legacy.ExpireRegistration(ctx, "reviewer", "source-run", "source-session", "post-adoption-session-end"); err != nil {
		t.Fatalf("legacy retired-source cleanup: %v", err)
	}
	events, err := db.ListEvents(ctx, "coupon", "operational", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	foundCleanup := false
	for _, event := range events {
		if event.Kind == "session.stale" && event.ActorID == "reviewer" {
			foundCleanup = true
		}
	}
	if !foundCleanup {
		t.Fatalf("retired source cleanup event missing: %+v", events)
	}
	if exact, err := api.Dial(ctx, socket, api.Identity{
		Actor: "reviewer", RunID: "exact-reuse", Client: "test", NameMode: bus.NameModeExact,
		ProjectID: "coupon",
	}); !errors.Is(err, bus.ErrActorAdopted) {
		if exact != nil {
			_ = exact.Close()
		}
		t.Fatalf("exact adopted-source API error = %v", err)
	}
	fresh, err := api.Dial(ctx, socket, api.Identity{
		Actor: "reviewer", RunID: "fresh-run", Client: "test", NameMode: bus.NameModeAllocate,
		ContinuityHandles: []string{"process:claude:fresh-run", "session:claude:source-session"}, ProjectID: "coupon",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Close()
	if identity := fresh.Identity(); identity.Actor != "reviewer-2" || identity.AdoptedPredecessor != "reviewer" || identity.Provisional {
		t.Fatalf("fresh API identity = %+v", identity)
	}
}

func TestUnixAPIAdoptionRejectsDifferentRunUnderLiveActor(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	_, socket := startServer(t, ctx, cancel)
	live := dial(t, socket, "replacement", "live-run")
	defer live.Close()
	if _, err := live.RegisterSession(ctx, bus.RegistrationRequest{
		Actor: "replacement", RunID: "live-run", Harness: "test", SessionID: "live-session",
		Lease: time.Hour,
	}); err != nil {
		t.Fatal(err)
	}
	sender := dial(t, socket, "sender", "sender-run")
	defer sender.Close()
	if _, err := sender.Send(ctx, bus.SendRequest{
		IdempotencyKey: "api-run-liveness", ProjectID: "test", ChannelID: "direct",
		ToActors: []string{"orphan"}, Type: "MESSAGE", Body: json.RawMessage(`{"text":"work"}`),
	}); err != nil {
		t.Fatal(err)
	}
	stale := dial(t, socket, "replacement", "stale-run")
	defer stale.Close()
	if _, err := stale.AdoptActor(ctx, bus.AdoptRequest{
		SourceActor: "orphan", ProjectID: "test", IdempotencyKey: "wrong-run",
	}); !errors.Is(err, bus.ErrRunNotLive) {
		t.Fatalf("wrong adopting run API error = %v", err)
	}
}

func TestUnixAPIConnectorRegistrationAndEvents(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	_, socket := startServer(t, ctx, cancel)
	client := dial(t, socket, "codex", "codex-run")
	registration, err := client.RegisterSession(ctx, bus.RegistrationRequest{
		Actor: "codex", RunID: "codex-run", Harness: "codex", SessionID: "thread-1",
		DeliveryHandle: "thread-1", ProjectID: "experiment", Lease: time.Hour,
	})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	if registration.Actor != "codex" || registration.RunID != "codex-run" {
		t.Fatalf("registration = %+v", registration)
	}
	registrations, err := client.LiveRegistrations(ctx, "codex")
	if err != nil || len(registrations) != 1 {
		t.Fatalf("live registrations = %+v, err = %v", registrations, err)
	}
	if registrations[0].DeliveryHandle != "" {
		t.Fatalf("external API exposed delivery handle %q", registrations[0].DeliveryHandle)
	}
	other := dial(t, socket, "other", "other-run")
	defer other.Close()
	if _, err := other.LiveRegistrations(ctx, "codex"); err == nil {
		t.Fatal("cross-actor registration lookup succeeded")
	}
	events, err := client.ListEvents(ctx, "experiment", "operational", 0, 10)
	if err != nil || len(events) != 1 || events[0].Kind != "session.registered" {
		t.Fatalf("events = %+v, err = %v", events, err)
	}
}

func TestUnixAPIWaitAttentionRequiresAndTargetsLiveClaudeSession(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	broker := attention.NewBroker()
	_, socket := startServer(t, ctx, cancel, api.WithAttentionBroker(broker))
	client := dial(t, socket, "claude", "claude-run")
	if _, err := client.WaitAttention(ctx, "claude", "claude-run", "missing", "hook-long-poll", 50*time.Millisecond); !errors.Is(err, bus.ErrRegistrationExpired) {
		t.Fatalf("unregistered wait error = %v", err)
	}
	registration, err := client.RegisterSession(ctx, bus.RegistrationRequest{
		Actor: "claude", RunID: "claude-run", Harness: "claude", SessionID: "session-1",
		AttentionMode: "hook-long-poll", DeliveryHandle: "session-1", ProjectID: "experiment", Lease: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.MonitorAttach(ctx, "claude", "claude-run", "session-1", "hook-long-poll", 5*time.Minute); err != nil {
		t.Fatal(err)
	}
	type waitResult struct {
		notice bus.AttentionNotice
		err    error
	}
	result := make(chan waitResult, 1)
	go func() {
		notice, waitErr := client.WaitAttention(ctx, "claude", "claude-run", "session-1", "hook-long-poll", time.Second)
		result <- waitResult{notice: notice, err: waitErr}
	}()
	message := bus.Message{
		ID: "msg-reference", ThreadID: "thread-1", FromActor: "codex", Type: "QUESTION",
		DeliveryRequest: bus.DeliveryWake, Body: json.RawMessage(`{"text":"must not cross attention path"}`),
	}
	deadline := time.Now().Add(time.Second)
	for {
		adapter, accepted := broker.Notify(registration, message)
		if accepted {
			if adapter != "hook-long-poll" {
				t.Fatalf("adapter = %q", adapter)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("API waiter did not register")
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case delivered := <-result:
		if delivered.err != nil || delivered.notice.MessageID != message.ID || delivered.notice.FromActor != "codex" {
			t.Fatalf("wait result = %+v", delivered)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("attention wait did not return")
	}
}

type cancellationBroker struct {
	waiting  chan struct{}
	canceled chan struct{}
}

func (b *cancellationBroker) Attach(_ string, _ string, _ string, transition func() error) error {
	if transition != nil {
		return transition()
	}
	return nil
}
func (b *cancellationBroker) Cancel(string, string, string, error) {}
func (b *cancellationBroker) Wait(ctx context.Context, _, _, _, _ string) (bus.AttentionNotice, error) {
	close(b.waiting)
	<-ctx.Done()
	close(b.canceled)
	return bus.AttentionNotice{}, ctx.Err()
}

func TestUnixAPIClientDisconnectCancelsParkedWaiter(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	broker := &cancellationBroker{waiting: make(chan struct{}), canceled: make(chan struct{})}
	_, socket := startServer(t, ctx, cancel, api.WithAttentionBroker(broker))
	client := dial(t, socket, "claude", "claude-run")
	defer client.Close()
	if _, err := client.RegisterSession(ctx, bus.RegistrationRequest{
		Actor: "claude", RunID: "claude-run", Harness: "claude", SessionID: "session-1",
		AttentionMode: "hook-long-poll", DeliveryHandle: "session-1", ProjectID: "experiment", Lease: time.Hour,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.MonitorAttach(ctx, "claude", "claude-run", "session-1", "hook-long-poll", 5*time.Minute); err != nil {
		t.Fatal(err)
	}
	waitDone := make(chan error, 1)
	go func() {
		_, err := client.WaitAttention(context.Background(), "claude", "claude-run", "session-1", "hook-long-poll", 20*time.Second)
		waitDone <- err
	}()
	select {
	case <-broker.waiting:
	case <-time.After(time.Second):
		t.Fatal("waiter did not park")
	}
	client.Interrupt()
	select {
	case <-broker.canceled:
	case <-time.After(time.Second):
		t.Fatal("server did not cancel waiter after client disconnect")
	}
	select {
	case err := <-waitDone:
		if err == nil {
			t.Fatal("interrupted wait returned nil")
		}
	case <-time.After(time.Second):
		t.Fatal("client wait remained blocked")
	}
}

func TestUnixAPIWaitAttentionRejectsUnsupportedAdapter(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	broker := attention.NewBroker()
	_, socket := startServer(t, ctx, cancel, api.WithAttentionBroker(broker))
	client := dial(t, socket, "claude", "claude-run")
	defer client.Close()
	if _, err := client.RegisterSession(ctx, bus.RegistrationRequest{
		Actor: "claude", RunID: "claude-run", Harness: "claude", SessionID: "session-1",
		AttentionMode: "hook-long-poll", DeliveryHandle: "session-1", ProjectID: "experiment", Lease: time.Hour,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.WaitAttention(ctx, "claude", "claude-run", "session-1", "unsupported", 50*time.Millisecond); !errors.Is(err, bus.ErrInvalid) {
		t.Fatalf("unsupported adapter wait error = %v", err)
	}
}

func TestUnixAPIAttentionUnavailableIsTypedAndTerminal(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	_, socket := startServer(t, ctx, cancel)
	client := dial(t, socket, "claude", "claude-run")
	defer client.Close()

	if _, err := client.WaitAttention(ctx, "claude", "claude-run", "session-1", "hook-long-poll", 50*time.Millisecond); !errors.Is(err, bus.ErrAttentionUnavailable) || err.Error() != bus.ErrAttentionUnavailable.Error() {
		t.Fatalf("wait attention error = %v, want unduplicated ErrAttentionUnavailable", err)
	}
	if _, err := client.MonitorAttach(ctx, "claude", "claude-run", "session-1", "hook-long-poll", 5*time.Minute); !errors.Is(err, bus.ErrAttentionUnavailable) || err.Error() != bus.ErrAttentionUnavailable.Error() {
		t.Fatalf("monitor attach error = %v, want unduplicated ErrAttentionUnavailable", err)
	}
}

func TestMonitorAttachRearmsOnlyOnPresenceTransition(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	broker := attention.NewBroker()
	db, socket := startServer(t, ctx, cancel, api.WithAttentionBroker(broker))
	client := dial(t, socket, "reviewer", "run-1")
	defer client.Close()
	if _, err := client.RegisterSession(ctx, bus.RegistrationRequest{
		Actor: "reviewer", RunID: "run-1", Harness: "claude", AttentionMode: "hook-long-poll",
		SessionID: "session-1", DeliveryHandle: "session-1", ProjectID: "experiment", Lease: time.Hour,
	}); err != nil {
		t.Fatal(err)
	}
	sent, err := db.Send(ctx, bus.SendRequest{
		IdempotencyKey: "presence-transition-rearm", ProjectID: "experiment", ChannelID: "direct",
		FromActor: "sender", FromRun: "sender-run", ToActors: []string{"reviewer"}, Type: "MESSAGE",
		DeliveryRequest: bus.DeliveryWake, Body: json.RawMessage(`{"text":"wake"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	job, err := db.ClaimNotification(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.FinishNotification(ctx, job, bus.NotificationAccepted, "accepted"); err != nil {
		t.Fatal(err)
	}

	if _, err := client.MonitorAttach(ctx, "reviewer", "run-1", "session-1", "hook-long-poll", 5*time.Minute); err != nil {
		t.Fatal(err)
	}
	rearmed, err := db.ClaimNotification(ctx)
	if err != nil || rearmed.Message.ID != sent.Message.ID || rearmed.Attempt != 2 {
		t.Fatalf("first presence rearm = %+v, err=%v", rearmed, err)
	}
	if err := db.FinishNotification(ctx, rearmed, bus.NotificationAccepted, "accepted again"); err != nil {
		t.Fatal(err)
	}
	if _, err := client.MonitorAttach(ctx, "reviewer", "run-1", "session-1", "hook-long-poll", 5*time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ClaimNotification(ctx); !errors.Is(err, bus.ErrNoMessage) {
		t.Fatalf("same-presence heartbeat rearmed notification: %v", err)
	}

	successor := dial(t, socket, "reviewer", "run-2")
	defer successor.Close()
	if _, err := successor.RegisterSession(ctx, bus.RegistrationRequest{
		Actor: "reviewer", RunID: "run-2", Harness: "claude", AttentionMode: "hook-long-poll",
		SessionID: "session-2", DeliveryHandle: "session-2", ProjectID: "experiment", Lease: time.Hour,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := successor.MonitorAttach(ctx, "reviewer", "run-2", "session-2", "hook-long-poll", 5*time.Minute); err != nil {
		t.Fatal(err)
	}
	rearmed, err = db.ClaimNotification(ctx)
	if err != nil || rearmed.Attempt != 3 {
		t.Fatalf("successor presence rearm = %+v, err=%v", rearmed, err)
	}
}

func TestUnixAPIEnqueuesPostCommitNotificationOnce(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	db, socket := startServer(t, ctx, cancel)
	client := dial(t, socket, "sender", "sender-run")
	request := bus.SendRequest{
		IdempotencyKey: "universal-wake", ProjectID: "experiment", ChannelID: "direct",
		ToActors: []string{"recipient"}, Type: "MESSAGE", DeliveryRequest: bus.DeliveryWake,
		Body: json.RawMessage(`{"text":"wake through API"}`),
	}
	result, err := client.Send(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if result.NotificationState != "pending" {
		t.Fatalf("result=%+v", result)
	}
	job, err := db.ClaimNotification(ctx)
	if err != nil || job.Message.ID != result.Message.ID || job.RecipientActor != "recipient" {
		t.Fatalf("job=%+v err=%v", job, err)
	}
	if err := db.FinishNotification(ctx, job, bus.NotificationComplete, ""); err != nil {
		t.Fatal(err)
	}
	result, err = client.Send(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Duplicate {
		t.Fatalf("idempotent replay result=%+v", result)
	}
	if _, err := db.ClaimNotification(ctx); !errors.Is(err, bus.ErrNoMessage) {
		t.Fatalf("duplicate enqueued another notification: %v", err)
	}
}

func TestClientReconnectsAfterDaemonRestart(t *testing.T) {
	directory, err := os.MkdirTemp("/tmp", "ab-reconnect-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	database := filepath.Join(directory, "holler.sqlite3")
	socket := filepath.Join(directory, "holler.sock")
	start := func() (context.CancelFunc, <-chan error) {
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		readyReader, readyWriter := io.Pipe()
		go func() {
			done <- daemon.Run(ctx, daemon.Config{DatabasePath: database, SocketPath: socket}, readyWriter)
			_ = readyWriter.Close()
		}()
		ready := make(chan error, 1)
		go func() {
			var result map[string]interface{}
			ready <- json.NewDecoder(readyReader).Decode(&result)
			_ = readyReader.Close()
		}()
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			select {
			case err := <-done:
				t.Fatalf("daemon exited before readiness: %v", err)
			case err := <-ready:
				if err != nil {
					t.Fatalf("read daemon readiness: %v", err)
				}
				return cancel, done
			default:
			}
			time.Sleep(10 * time.Millisecond)
		}
		cancel()
		t.Fatal("daemon did not become ready")
		return cancel, done
	}
	cancel, done := start()
	client := dial(t, socket, "alice", "run-1")
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	cancel, done = start()
	if err := client.Ping(context.Background()); err != nil {
		t.Fatalf("ping after restart: %v", err)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestClaimResponseHasFrameHeadroomAboveMaximumBody(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	_, socket := startServer(t, ctx, cancel)
	alice := dial(t, socket, "alice", "run-a")
	bob := dial(t, socket, "bob", "run-b")
	body, err := json.Marshal(map[string]string{"text": string(bytes.Repeat([]byte("x"), bus.MaxBodyBytes-2048))})
	if err != nil {
		t.Fatal(err)
	}
	sent, err := alice.Send(ctx, bus.SendRequest{IdempotencyKey: "large", ProjectID: "frames", ChannelID: "direct", ToActors: []string{"bob"}, Type: "MESSAGE", Body: body})
	if err != nil {
		t.Fatalf("send large body: %v", err)
	}
	claim, err := bob.Claim(ctx, "bob", sent.Message.ID, time.Minute)
	if err != nil {
		t.Fatalf("claim large body: %v", err)
	}
	if !bytes.Equal(claim.Message.Body, body) {
		t.Fatal("large claim body changed")
	}
}

func TestAPIProfileAndWhoUseConnectionBoundIdentity(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	_, socket := startServer(t, ctx, cancel)
	alice := dial(t, socket, "alice", "alice-run-1")
	if _, err := alice.RegisterSession(ctx, bus.RegistrationRequest{
		Actor: "alice", RunID: "alice-run-1", Harness: "test", SessionID: "alice-session",
		ProjectID: "coupon", WorkingDir: "/workspace/coupon", Lease: time.Hour,
	}); err != nil {
		t.Fatal(err)
	}
	profile, err := alice.SetActorProfile(ctx, "alice", "alice-run-1", "coupon", bus.ActorProfileRequest{
		RoleText: "Implements coupon changes", Accepts: []string{"QUESTION"},
	})
	if err != nil || !profile.Updated || profile.Profile.Actor != "alice" {
		t.Fatalf("profile = %+v, err = %v", profile, err)
	}
	if _, err := alice.SetActorProfile(ctx, "mallory", "alice-run-1", "coupon", bus.ActorProfileRequest{RoleText: "spoof"}); err == nil {
		t.Fatal("profile accepted an actor that differs from the connection identity")
	}
	directory, err := alice.Who(ctx, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(directory.Actors) != 1 || directory.Actors[0].Actor != "alice" || directory.Actors[0].Profile == nil {
		t.Fatalf("directory = %+v", directory)
	}
	if got := directory.Actors[0].Sessions[0].WorkingDir; got != "/workspace/coupon" {
		t.Fatalf("working directory = %q", got)
	}
}

func TestUnixAPIAllocatesAndReclaimsActorsBeforeStampingRequests(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	_, socket := startServer(t, ctx, cancel)
	dialAllocated := func(runID, handle string) *api.Client {
		t.Helper()
		client, err := api.Dial(ctx, socket, api.Identity{
			Actor: "reviewer", RunID: runID, Client: "allocation-test",
			NameMode: bus.NameModeAllocate, ContinuityHandles: []string{handle}, ProjectID: "coupon",
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = client.Close() })
		return client
	}

	first := dialAllocated("run-1", "launch:test:first")
	second := dialAllocated("run-2", "launch:test:second")
	if got := first.Identity().Actor; got != "reviewer" {
		t.Fatalf("first actor = %q", got)
	}
	if got := second.Identity().Actor; got != "reviewer-2" {
		t.Fatalf("second actor = %q", got)
	}
	registration, err := second.RegisterSession(ctx, bus.RegistrationRequest{
		Actor: "reviewer-2", RunID: "run-2", Harness: "test", SessionID: "session-2", ProjectID: "coupon", Lease: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	if registration.Actor != "reviewer-2" || registration.RunID != "run-2" {
		t.Fatalf("server-stamped registration = %+v", registration)
	}
	sent, err := second.Send(ctx, bus.SendRequest{
		IdempotencyKey: "allocated-send", ProjectID: "coupon", ChannelID: "direct",
		ToActors: []string{"reviewer"}, Type: "MESSAGE", Body: json.RawMessage(`{"text":"ready"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if sent.Message.FromActor != "reviewer-2" || sent.Message.FromRun != "run-2" {
		t.Fatalf("server-stamped send = %+v", sent.Message)
	}

	reclaimed := dialAllocated("run-3", "launch:test:second")
	if got := reclaimed.Identity().Actor; got != "reviewer-2" {
		t.Fatalf("reclaimed actor = %q", got)
	}
}

func TestUnixAPIExactModeRefusesLiveActorWithoutTakeover(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	_, socket := startServer(t, ctx, cancel)
	first, err := api.Dial(ctx, socket, api.Identity{Actor: "reviewer", RunID: "run-1", Client: "test", NameMode: bus.NameModeExact})
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	if _, err := first.RegisterSession(ctx, bus.RegistrationRequest{
		Actor: "reviewer", RunID: "run-1", Harness: "test", SessionID: "session-1", ProjectID: "test", Lease: time.Hour,
	}); err != nil {
		t.Fatal(err)
	}
	if collided, err := api.Dial(ctx, socket, api.Identity{Actor: "reviewer", RunID: "run-2", Client: "test", NameMode: bus.NameModeExact}); !errors.Is(err, bus.ErrActorLive) {
		if collided != nil {
			_ = collided.Close()
		}
		t.Fatalf("exact collision error = %v", err)
	}
	takeover, err := api.Dial(ctx, socket, api.Identity{
		Actor: "reviewer", RunID: "run-2", Client: "test", NameMode: bus.NameModeExact, Takeover: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer takeover.Close()
	if registrations, err := takeover.LiveRegistrations(ctx, "reviewer"); err != nil || len(registrations) != 0 {
		t.Fatalf("registrations after takeover = %+v, err = %v", registrations, err)
	}
}

func TestUnixAPISupersededRunGetsTerminalStaleBindingError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	_, socket := startServer(t, ctx, cancel)
	continuity := []string{"process:codex:run-a", "session:codex:session-a"}
	first, err := api.Dial(ctx, socket, api.Identity{
		Actor: "reviewer", RunID: "run-a", Client: "test", NameMode: bus.NameModeAllocate,
		ContinuityHandles: continuity, ProjectID: "coupon",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	if _, err := first.RegisterSession(ctx, bus.RegistrationRequest{
		Actor: "reviewer", RunID: "run-a", Harness: "codex", SessionID: "session-a",
		ProjectID: "coupon", Lease: time.Hour,
	}); err != nil {
		t.Fatal(err)
	}
	winner, err := api.Dial(ctx, socket, api.Identity{
		Actor: "reviewer", RunID: "run-b", Client: "test", NameMode: bus.NameModeExact,
		ProjectID: "coupon", Takeover: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer winner.Close()
	if _, err := winner.RegisterSession(ctx, bus.RegistrationRequest{
		Actor: "reviewer", RunID: "run-b", Harness: "codex", SessionID: "session-b",
		ProjectID: "coupon", Lease: time.Hour,
	}); err != nil {
		t.Fatal(err)
	}

	stale, err := api.Dial(ctx, socket, api.Identity{
		Actor: "reviewer", RunID: "run-a", Client: "test", NameMode: bus.NameModeAllocate,
		ContinuityHandles: continuity, ProjectID: "coupon",
	})
	if stale != nil {
		_ = stale.Close()
	}
	if !errors.Is(err, bus.ErrBindingStale) {
		t.Fatalf("stale binding API error = %v", err)
	}
	live, err := winner.LiveRegistrations(ctx, "reviewer")
	if err != nil || len(live) != 1 || live[0].RunID != "run-b" {
		t.Fatalf("winner registrations after stale hello = %+v, err = %v", live, err)
	}
}

func TestUnixAPIProvisionalConnectionFollowsSessionResumeReconciliation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	_, socket := startServer(t, ctx, cancel)
	first, err := api.Dial(ctx, socket, api.Identity{
		Actor: "reviewer", RunID: "run-1", Client: "hook", NameMode: bus.NameModeAllocate,
		ContinuityHandles: []string{"process:codex:run-1", "session:codex:session-1"}, ProjectID: "coupon",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	provisional, err := api.Dial(ctx, socket, api.Identity{
		Actor: "reviewer", RunID: "run-2", Client: "mcp", NameMode: bus.NameModeAllocate,
		ContinuityHandles: []string{"process:codex:run-2"}, ProjectID: "coupon",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer provisional.Close()
	if identity := provisional.Identity(); identity.Actor != "reviewer-2" || !identity.Provisional {
		t.Fatalf("provisional identity = %+v", identity)
	}
	hook, err := api.Dial(ctx, socket, api.Identity{
		Actor: "reviewer", RunID: "run-2", Client: "hook", NameMode: bus.NameModeAllocate,
		ContinuityHandles: []string{"process:codex:run-2", "session:codex:session-1"}, ProjectID: "coupon",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer hook.Close()
	if hook.Identity().Actor != "reviewer" {
		t.Fatalf("hook reclaimed %q", hook.Identity().Actor)
	}
	if _, err := provisional.Who(ctx, 10); err != nil {
		t.Fatalf("provisional client did not reconnect after reconciliation: %v", err)
	}
	if identity := provisional.Identity(); identity.Actor != "reviewer" || identity.Provisional {
		t.Fatalf("reconciled client identity = %+v", identity)
	}
	sent, err := provisional.Send(ctx, bus.SendRequest{
		IdempotencyKey: "resume-reconciled", ProjectID: "coupon", ChannelID: "direct",
		ToActors: []string{"peer"}, Type: "MESSAGE", Body: json.RawMessage(`{"text":"resumed"}`),
	})
	if err != nil || sent.Message.FromActor != "reviewer" {
		t.Fatalf("reconciled send = %+v, err = %v", sent, err)
	}
}

func TestUnixAPIReleasesUnusedProvisionalActorOnDisconnect(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	db, socket := startServer(t, ctx, cancel)
	provisional, err := api.Dial(ctx, socket, api.Identity{
		Actor: "worker", RunID: "run-1", Client: "mcp", NameMode: bus.NameModeAllocate,
		ContinuityHandles: []string{"process:codex:run-1"}, ProjectID: "test",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := provisional.Close(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		_, bindingErr := db.CurrentActorForContinuity(ctx, []string{"process:codex:run-1"})
		if errors.Is(bindingErr, bus.ErrNotFound) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("provisional actor was not released: %v", bindingErr)
		}
		time.Sleep(10 * time.Millisecond)
	}
	active, err := api.Dial(ctx, socket, api.Identity{
		Actor: "worker", RunID: "run-2", Client: "launcher", NameMode: bus.NameModeAllocate,
		ContinuityHandles: []string{"launch:codex:worker-2"}, ProjectID: "test",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer active.Close()
	if active.Identity().Actor != "worker" {
		t.Fatalf("actor after provisional disconnect = %q", active.Identity().Actor)
	}
}

func startServer(t *testing.T, ctx context.Context, cancel context.CancelFunc, options ...api.ServerOption) (*store.Store, string) {
	t.Helper()
	directory := t.TempDir()
	db, err := store.Open(ctx, filepath.Join(directory, "holler.sqlite3"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	socketDirectory, err := os.MkdirTemp("/tmp", "holler-api-")
	if err != nil {
		t.Fatalf("socket temp directory: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(socketDirectory) })
	listener, err := net.Listen("unix", filepath.Join(socketDirectory, "holler.sock"))
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- api.NewServer(db, options...).Serve(ctx, listener) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("serve: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Error("server did not stop")
		}
		_ = db.Close()
	})
	return db, listener.Addr().String()
}

func dial(t *testing.T, socket, actor, run string) *api.Client {
	t.Helper()
	client, err := api.Dial(context.Background(), socket, api.Identity{Actor: actor, RunID: run, Client: "test"})
	if err != nil {
		t.Fatalf("dial %s: %v", actor, err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}
