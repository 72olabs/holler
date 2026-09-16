package sqlite

import (
	"encoding/json"
	"errors"
	"github.com/72olabs/holler/internal/bus"
	"testing"
	"time"
)

func TestManagedNotificationSourceNegotiationAndRevocation(t *testing.T) {
	s, ctx := channelFixture(t)
	c, err := s.ChannelCreate(ctx, agent("a"), bus.ChannelCreate{ProjectID: "test", Kind: "named", Title: "attention", Participants: []string{"a", "b", "c"}, IdempotencyKey: "create"})
	if err != nil {
		t.Fatal(err)
	}
	sent, err := s.ChannelPost(ctx, agent("a"), bus.ChannelPost{ChannelID: c.ID, ExpectedRevision: c.Revision, IdempotencyKey: "msg", Body: json.RawMessage(`"secret body"`), Attention: []string{"b"}, Respondent: "c"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.ClaimNotification(ctx); !errors.Is(err, bus.ErrNoMessage) {
		t.Fatalf("legacy worker claimed managed: %v", err)
	}
	job, err := s.ClaimManagedNotification(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if job.RecipientActor != "b" || len(job.Message.Body) != 0 || job.Message.FromActor != "" || job.Message.SchemaVersion != 2 {
		t.Fatalf("unsafe or wrong-target wake: %+v", job)
	}
	reg := bus.Registration{Actor: "b", RunID: "b-run", Harness: "codex"}
	if allowed, err := s.ManagedNotificationAllowed(ctx, reg, sent.Message.ID); err != nil || allowed {
		t.Fatalf("unnegotiated wake: %v %v", allowed, err)
	}
	if err := s.EnableChannelAttention(ctx, agent("b"), false); err != nil {
		t.Fatal(err)
	}
	if allowed, err := s.ManagedNotificationAllowed(ctx, reg, sent.Message.ID); err != nil || !allowed {
		t.Fatalf("negotiated wake: %v %v", allowed, err)
	}
	reg.Harness = "claude"
	reg.AttentionMode = "hook-long-poll"
	if allowed, err := s.ManagedNotificationAllowed(ctx, reg, sent.Message.ID); err != nil || allowed {
		t.Fatalf("old monitor wake: %v %v", allowed, err)
	}
	if err := s.EnableChannelAttention(ctx, agent("b"), true); err != nil {
		t.Fatal(err)
	}
	if allowed, err := s.ManagedNotificationAllowed(ctx, reg, sent.Message.ID); err != nil || !allowed {
		t.Fatalf("new monitor wake: %v %v", allowed, err)
	}
	reg.AttentionMode = "host-injected"
	if allowed, err := s.ManagedNotificationAllowed(ctx, reg, sent.Message.ID); err != nil || allowed {
		t.Fatalf("frozen host wake: %v %v", allowed, err)
	}
	if err := s.FinishManagedNotification(ctx, job, bus.NotificationAccepted, "private adapter detail"); err != nil {
		t.Fatal(err)
	}
	events, err := s.ListEvents(bus.WithCaller(ctx, bus.Caller{Actor: "operator"}), "test", "operational", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range events {
		if e.MessageID == sent.Message.ID {
			t.Fatal("managed attention in global events")
		}
	}
	claim, err := s.ChannelClaim(ctx, agent("b"), sent.Message.ID, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.ChannelDeliveryUpdate(ctx, agent("b"), sent.Message.ID, claim.LeaseToken, "nack", "recipient-private reason", 0); err != nil {
		t.Fatal(err)
	}
	var leaks int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM channel_events WHERE CAST(payload AS TEXT) LIKE '%recipient-private%' OR CAST(payload AS TEXT) LIKE '%private adapter%'`).Scan(&leaks); err != nil || leaks != 0 {
		t.Fatalf("private detail leaked: %d %v", leaks, err)
	}
	if _, err := s.ChannelMembershipChange(ctx, agent("a"), c.ID, "b", "remove", "remove", c.Revision); err != nil {
		t.Fatal(err)
	}
	reg.Harness = "codex"
	if allowed, err := s.ManagedNotificationAllowed(ctx, reg, sent.Message.ID); err != nil || allowed {
		t.Fatalf("revoked wake: %v %v", allowed, err)
	}
	// Only b was notified, even though c was assigned the response.
	var jobs int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM notification_outbox WHERE message_id=?`, sent.Message.ID).Scan(&jobs); err != nil || jobs != 1 {
		t.Fatalf("attention conflated with response: %d %v", jobs, err)
	}
}

func TestManagedAcceptedRearmIsBoundedAndRequiresLiveReadiness(t *testing.T) {
	s, ctx := channelFixture(t)
	c := createDM(t, s, ctx, "a", "b", "dm")
	_, err := s.ChannelPost(ctx, agent("a"), bus.ChannelPost{ChannelID: c.ID, ExpectedRevision: c.Revision, IdempotencyKey: "notify", Body: json.RawMessage(`"hello"`), Attention: []string{"b"}})
	if err != nil {
		t.Fatal(err)
	}
	job, err := s.ClaimManagedNotification(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.FinishManagedNotification(ctx, job, bus.NotificationAccepted, ""); err != nil {
		t.Fatal(err)
	}
	stale := func() {
		t.Helper()
		if _, err := s.db.Exec(`UPDATE notification_outbox SET available_at_ns=? WHERE message_id=?`, s.now().Add(-time.Hour).UnixNano(), job.Message.ID); err != nil {
			t.Fatal(err)
		}
	}
	stale()
	if err := s.RearmStaleManagedNotifications(ctx, time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ClaimManagedNotification(ctx); !errors.Is(err, bus.ErrNoMessage) {
		t.Fatal("rearmed without live ready recipient")
	}
	if _, err := s.RegisterSession(ctx, bus.RegistrationRequest{Actor: "b", RunID: "b-run", Harness: "codex", AttentionMode: "native-queue", SessionID: "session", DeliveryHandle: "session", ProjectID: "test", Lease: time.Hour}); err != nil {
		t.Fatal(err)
	}
	if err := s.EnableChannelAttention(ctx, agent("b"), false); err != nil {
		t.Fatal(err)
	}
	if err := s.RearmStaleManagedNotifications(ctx, time.Minute); err != nil {
		t.Fatal(err)
	}
	retry, err := s.ClaimManagedNotification(ctx)
	if err != nil || retry.Attempt != 2 {
		t.Fatalf("rearm: %+v %v", retry, err)
	}
	if err := s.FinishManagedNotification(ctx, retry, bus.NotificationAccepted, ""); err != nil {
		t.Fatal(err)
	}
	stale()
	if err := s.ResetChannelAttention(ctx, agent("b"), false); err != nil {
		t.Fatal(err)
	}
	if err := s.RearmStaleManagedNotifications(ctx, time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ClaimManagedNotification(ctx); !errors.Is(err, bus.ErrNoMessage) {
		t.Fatal("downgrade rearmed")
	}
	if err := s.EnableChannelAttention(ctx, agent("b"), false); err != nil {
		t.Fatal(err)
	}
	if err := s.ExpireRegistration(ctx, "b", "b-run", "session", "test end"); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM channel_attention_clients WHERE actor='b' AND run_id='b-run'`).Scan(&n); err != nil || n != 0 {
		t.Fatal("session-end readiness retained")
	}
}
