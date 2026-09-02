package sqlite_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/72olabs/holler/internal/bus"
	store "github.com/72olabs/holler/internal/store/sqlite"
)

func TestSendIsIdempotentAndDetectsConflict(t *testing.T) {
	db, _ := openTestStore(t)
	ctx := context.Background()
	req := testRequest()
	req.ToActors = []string{"reviewer", "implementer", "reviewer"}

	first, err := db.Send(ctx, req)
	if err != nil {
		t.Fatalf("first send: %v", err)
	}
	if first.Duplicate {
		t.Fatal("first send reported duplicate")
	}
	if got, want := first.Message.ToActors, []string{"implementer", "reviewer"}; !sameStrings(got, want) {
		t.Fatalf("normalized recipients = %v, want %v", got, want)
	}

	second, err := db.Send(ctx, req)
	if err != nil {
		t.Fatalf("duplicate send: %v", err)
	}
	if !second.Duplicate || second.Message.ID != first.Message.ID {
		t.Fatalf("duplicate result = %+v, first id = %s", second, first.Message.ID)
	}

	changed := req
	changed.Body = json.RawMessage(`{"text":"different"}`)
	if _, err := db.Send(ctx, changed); !errors.Is(err, bus.ErrIdempotencyConflict) {
		t.Fatalf("changed duplicate error = %v, want idempotency conflict", err)
	}

	durable, err := db.ListEvents(ctx, "test-project", "durable", 0, 10)
	if err != nil {
		t.Fatalf("list durable events: %v", err)
	}
	if len(durable) != 1 || durable[0].Kind != "message.sent" || durable[0].Position != 1 {
		t.Fatalf("durable events = %+v", durable)
	}
	operational, err := db.ListEvents(ctx, "test-project", "operational", 0, 10)
	if err != nil {
		t.Fatalf("list operational events: %v", err)
	}
	if len(operational) != 2 || operational[0].Position != 1 || operational[1].Position != 2 {
		t.Fatalf("operational events = %+v", operational)
	}
}

func TestClaimAndAckHaveSeparateLifecycle(t *testing.T) {
	db, _ := openTestStore(t)
	ctx := context.Background()
	sent, err := db.Send(ctx, testRequest())
	if err != nil {
		t.Fatalf("send: %v", err)
	}

	inbox, err := db.CheckInbox(ctx, "reviewer", 10)
	if err != nil {
		t.Fatalf("check inbox: %v", err)
	}
	if len(inbox) != 1 || inbox[0].State != bus.DeliveryQueued || !inbox[0].Available {
		t.Fatalf("queued inbox = %+v", inbox)
	}

	claim, err := db.Claim(ctx, "reviewer", sent.Message.ID, 30*time.Second)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if claim.Message.ID != sent.Message.ID || claim.Attempt != 1 || claim.LeaseToken == "" {
		t.Fatalf("claim = %+v", claim)
	}
	inbox, err = db.CheckInbox(ctx, "reviewer", 10)
	if err != nil {
		t.Fatalf("check claimed inbox: %v", err)
	}
	if len(inbox) != 1 || inbox[0].State != bus.DeliveryClaimed || inbox[0].Available {
		t.Fatalf("claimed inbox = %+v", inbox)
	}

	if err := db.Ack(ctx, "reviewer", sent.Message.ID, claim.LeaseToken); err != nil {
		t.Fatalf("ack: %v", err)
	}
	inbox, err = db.CheckInbox(ctx, "reviewer", 10)
	if err != nil {
		t.Fatalf("check acked inbox: %v", err)
	}
	if len(inbox) != 0 {
		t.Fatalf("acked inbox = %+v, want empty", inbox)
	}
	if err := db.Ack(ctx, "reviewer", sent.Message.ID, claim.LeaseToken); err != nil {
		t.Fatalf("idempotent second ack = %v", err)
	}
}

func TestExpiredClaimCanBeRecoveredByNewRun(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC)}
	db, _ := openTestStore(t, store.WithClock(clock.Now))
	ctx := context.Background()
	sent, err := db.Send(ctx, testRequest())
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	first, err := db.Claim(ctx, "reviewer", sent.Message.ID, time.Second)
	if err != nil {
		t.Fatalf("first claim: %v", err)
	}

	clock.Advance(2 * time.Second)
	inbox, err := db.CheckInbox(ctx, "reviewer", 10)
	if err != nil {
		t.Fatalf("check expired claim: %v", err)
	}
	if len(inbox) != 1 || !inbox[0].Available {
		t.Fatalf("expired claim inbox = %+v", inbox)
	}
	second, err := db.Claim(ctx, "reviewer", sent.Message.ID, time.Minute)
	if err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	if second.Attempt != 2 || second.LeaseToken == first.LeaseToken {
		t.Fatalf("recovered claim = %+v; first = %+v", second, first)
	}
	if err := db.Ack(ctx, "reviewer", sent.Message.ID, first.LeaseToken); !errors.Is(err, bus.ErrLeaseTokenMismatch) {
		t.Fatalf("stale ack error = %v, want token mismatch", err)
	}
	if err := db.Ack(ctx, "reviewer", sent.Message.ID, second.LeaseToken); err != nil {
		t.Fatalf("recovered ack: %v", err)
	}
}

func TestLeaseCanBeExtendedAndExpiredAckIsSafeUntilReissue(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC)}
	db, _ := openTestStore(t, store.WithClock(clock.Now))
	ctx := context.Background()
	sent, err := db.Send(ctx, testRequest())
	if err != nil {
		t.Fatal(err)
	}
	claim, err := db.Claim(ctx, "reviewer", sent.Message.ID, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	extended, err := db.Extend(ctx, "reviewer", sent.Message.ID, claim.LeaseToken, time.Minute)
	if err != nil {
		t.Fatalf("extend active lease: %v", err)
	}
	if !extended.LeaseExpiresAt.Equal(clock.Now().Add(time.Minute)) {
		t.Fatalf("extended expiry = %v", extended.LeaseExpiresAt)
	}
	clock.Advance(2 * time.Minute)
	if _, err := db.Extend(ctx, "reviewer", sent.Message.ID, claim.LeaseToken, time.Minute); !errors.Is(err, bus.ErrLeaseExpired) {
		t.Fatalf("extend expired lease error = %v", err)
	}
	if err := db.Ack(ctx, "reviewer", sent.Message.ID, claim.LeaseToken); err != nil {
		t.Fatalf("ack expired but unreissued lease: %v", err)
	}
	if err := db.Ack(ctx, "reviewer", sent.Message.ID, claim.LeaseToken); err != nil {
		t.Fatalf("idempotent ack retry: %v", err)
	}
}

func TestHeartbeatRenewsOnlyNewestRegistrationAndRevivesPassiveExpiry(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC)}
	db, _ := openTestStore(t, store.WithClock(clock.Now))
	ctx := context.Background()
	register := func(session string) {
		t.Helper()
		_, err := db.RegisterSession(ctx, bus.RegistrationRequest{
			Actor: "codex", RunID: "run-1", Harness: "codex", SessionID: session,
			DeliveryHandle: session, ProjectID: "experiment", Lease: time.Minute,
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	register("older")
	clock.Advance(time.Second)
	register("newer")
	clock.Advance(2 * time.Minute)

	renewed, err := db.HeartbeatRegistrations(ctx, "codex", "run-1", 5*time.Minute)
	if err != nil || renewed != 1 {
		t.Fatalf("heartbeat renewed=%d err=%v", renewed, err)
	}
	live, err := db.LiveRegistrations(ctx, "codex")
	if err != nil || len(live) != 1 || live[0].SessionID != "newer" {
		t.Fatalf("live registrations after heartbeat = %+v, err=%v", live, err)
	}

	if err := db.ExpireRegistration(ctx, "codex", "run-1", "newer", "SessionEnd"); err != nil {
		t.Fatal(err)
	}
	renewed, err = db.HeartbeatRegistrations(ctx, "codex", "run-1", 5*time.Minute)
	if err != nil || renewed != 0 {
		t.Fatalf("heartbeat resurrected an explicitly ended session: renewed=%d err=%v", renewed, err)
	}

	register("expired-before-end")
	clock.Advance(2 * time.Minute)
	if err := db.ExpireRegistration(ctx, "codex", "run-1", "expired-before-end", "SessionEnd"); err != nil {
		t.Fatal(err)
	}
	renewed, err = db.HeartbeatRegistrations(ctx, "codex", "run-1", 5*time.Minute)
	if err != nil || renewed != 0 {
		t.Fatalf("heartbeat resurrected a passively expired then explicitly ended session: renewed=%d err=%v", renewed, err)
	}
}

func TestMonitorAttachRecoversAcrossShortAndLongDaemonOutages(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC)}
	db, _ := openTestStore(t, store.WithClock(clock.Now))
	ctx := context.Background()
	register := func(session string) {
		t.Helper()
		if _, err := db.RegisterSession(ctx, bus.RegistrationRequest{
			Actor: "claude", RunID: "run-1", Harness: "claude", AttentionMode: "hook-long-poll",
			SessionID: session, DeliveryHandle: session, ProjectID: "experiment", Lease: 5 * time.Minute,
		}); err != nil {
			t.Fatal(err)
		}
	}
	register("short-outage")
	clock.Advance(4 * time.Minute)
	if _, err := db.AttachMonitor(ctx, "claude", "run-1", "short-outage", "claude", "hook-long-poll", 5*time.Minute); err != nil {
		t.Fatalf("attach after short outage: %v", err)
	}
	register("long-outage")
	clock.Advance(6 * time.Minute)
	if _, err := db.AttachMonitor(ctx, "claude", "run-1", "long-outage", "claude", "hook-long-poll", 5*time.Minute); err != nil {
		t.Fatalf("attach after passive expiry: %v", err)
	}
	if _, err := db.AttachMonitor(ctx, "claude", "run-1", "short-outage", "claude", "hook-long-poll", 5*time.Minute); !errors.Is(err, bus.ErrPresenceSuperseded) {
		t.Fatalf("older presence reclaimed attention: %v", err)
	}
	live, err := db.LiveRegistrations(ctx, "claude")
	if err != nil || len(live) != 1 || live[0].SessionID != "long-outage" {
		t.Fatalf("live registrations after recovery = %+v, err=%v", live, err)
	}
	if err := db.ExpireRegistration(ctx, "claude", "run-1", "long-outage", "SessionEnd"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.AttachMonitor(ctx, "claude", "run-1", "long-outage", "claude", "hook-long-poll", 5*time.Minute); !errors.Is(err, bus.ErrSessionEnded) {
		t.Fatalf("explicitly ended attach = %v", err)
	}
	if _, err := db.AttachMonitor(ctx, "claude", "run-1", "missing", "claude", "hook-long-poll", 5*time.Minute); !errors.Is(err, bus.ErrRegistrationExpired) {
		t.Fatalf("missing attach = %v", err)
	}
}

func TestMonitorAttachUsesRegistrationOrderWhenClockTies(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC)}
	db, _ := openTestStore(t, store.WithClock(clock.Now))
	ctx := context.Background()
	for _, session := range []string{"older", "newer"} {
		if _, err := db.RegisterSession(ctx, bus.RegistrationRequest{
			Actor: "claude", RunID: session + "-run", Harness: "claude", AttentionMode: "hook-long-poll",
			SessionID: session, DeliveryHandle: session, ProjectID: "experiment", Lease: 5 * time.Minute,
		}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.AttachMonitor(ctx, "claude", "older-run", "older", "claude", "hook-long-poll", 5*time.Minute); !errors.Is(err, bus.ErrPresenceSuperseded) {
		t.Fatalf("older tied registration attached: %v", err)
	}
	if _, err := db.AttachMonitor(ctx, "claude", "newer-run", "newer", "claude", "hook-long-poll", 5*time.Minute); err != nil {
		t.Fatalf("newer tied registration did not attach: %v", err)
	}
}

func TestMonitorHeartbeatDoesNotRearmAcceptedUnclaimedNotification(t *testing.T) {
	db, _ := openTestStore(t)
	ctx := context.Background()
	if _, err := db.RegisterSession(ctx, bus.RegistrationRequest{
		Actor: "reviewer", RunID: "reviewer-run", Harness: "claude", AttentionMode: "hook-long-poll",
		SessionID: "reviewer-session", DeliveryHandle: "reviewer-session", ProjectID: "test-project", Lease: 5 * time.Minute,
	}); err != nil {
		t.Fatal(err)
	}
	sent, err := db.Send(ctx, testRequest())
	if err != nil {
		t.Fatal(err)
	}
	job, err := db.ClaimNotification(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.FinishNotification(ctx, job, bus.NotificationAccepted, "waiter accepted"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ClaimNotification(ctx); !errors.Is(err, bus.ErrNoMessage) {
		t.Fatalf("accepted notification remained claimable: %v", err)
	}
	if _, err := db.AttachMonitor(ctx, "reviewer", "reviewer-run", "reviewer-session", "claude", "hook-long-poll", 5*time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ClaimNotification(ctx); !errors.Is(err, bus.ErrNoMessage) {
		t.Fatalf("same-presence heartbeat rearmed notification: %v", err)
	}
	if err := db.RearmAcceptedNotifications(ctx, "reviewer"); err != nil {
		t.Fatal(err)
	}
	rearmed, err := db.ClaimNotification(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if rearmed.Message.ID != sent.Message.ID || rearmed.Attempt != 2 {
		t.Fatalf("rearmed job = %+v, want message %s attempt 2", rearmed, sent.Message.ID)
	}
}

func TestExpiredMessageIsNeverClaimedForNotification(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC)}
	db, _ := openTestStore(t, store.WithClock(clock.Now))
	ctx := context.Background()
	expires := clock.Now().Add(time.Second)
	request := testRequest()
	request.ExpiresAt = &expires
	if _, err := db.Send(ctx, request); err != nil {
		t.Fatal(err)
	}
	clock.Advance(2 * time.Second)
	if _, err := db.ClaimNotification(ctx); !errors.Is(err, bus.ErrNoMessage) {
		t.Fatalf("expired notification claim = %v, want no message", err)
	}
}

func TestAcceptedNotificationRearmIsBounded(t *testing.T) {
	db, _ := openTestStore(t)
	ctx := context.Background()
	if _, err := db.Send(ctx, testRequest()); err != nil {
		t.Fatal(err)
	}
	for attempt := 1; attempt <= 5; attempt++ {
		job, err := db.ClaimNotification(ctx)
		if err != nil {
			t.Fatalf("claim attempt %d: %v", attempt, err)
		}
		if err := db.FinishNotification(ctx, job, bus.NotificationAccepted, "waiter accepted"); err != nil {
			t.Fatal(err)
		}
		if err := db.RearmAcceptedNotifications(ctx, job.RecipientActor); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.ClaimNotification(ctx); !errors.Is(err, bus.ErrNoMessage) {
		t.Fatalf("accepted notification exceeded rearm bound: %v", err)
	}
}

func TestNotificationOutboxRecordsAbandonmentAfterBoundedRetries(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC)}
	db, _ := openTestStore(t, store.WithClock(clock.Now))
	ctx := context.Background()
	if _, err := db.Send(ctx, testRequest()); err != nil {
		t.Fatal(err)
	}
	for attempt := 1; attempt <= 5; attempt++ {
		job, err := db.ClaimNotification(ctx)
		if err != nil {
			t.Fatalf("claim notification attempt %d: %v", attempt, err)
		}
		if job.Attempt != attempt {
			t.Fatalf("job attempt=%d, want=%d", job.Attempt, attempt)
		}
		if err := db.FinishNotification(ctx, job, bus.NotificationRetry, "adapter unavailable"); err != nil {
			t.Fatal(err)
		}
		clock.Advance(time.Duration(attempt*attempt) * time.Second)
	}
	if _, err := db.ClaimNotification(ctx); !errors.Is(err, bus.ErrNoMessage) {
		t.Fatalf("exhausted notification remained claimable: %v", err)
	}
	events, err := db.ListEvents(ctx, "test-project", "operational", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, event := range events {
		if event.Kind == "delivery.notification_abandoned" {
			found = true
		}
	}
	if !found {
		t.Fatalf("abandonment event missing: %+v", events)
	}
}

func TestOpenRetriesTransientExclusiveLockDuringDaemonRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "restart.sqlite3")
	first, err := store.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	type openResult struct {
		store *store.Store
		err   error
	}
	done := make(chan openResult, 1)
	go func() {
		second, openErr := store.Open(ctx, path)
		done <- openResult{store: second, err: openErr}
	}()
	select {
	case result := <-done:
		if result.store != nil {
			_ = result.store.Close()
		}
		t.Fatalf("second open completed before first store closed: %v", result.err)
	case <-time.After(150 * time.Millisecond):
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case result := <-done:
		if result.err != nil {
			t.Fatalf("restart open did not recover from transient lock: %v", result.err)
		}
		if err := result.store.Close(); err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("restart open did not complete after lock release")
	}
}

func TestConcurrentProcessOpenSerializesMigrations(t *testing.T) {
	path := filepath.Join(t.TempDir(), "concurrent.sqlite3")
	runConcurrentOpenProcesses(t, path, "success", 8)

	raw, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	var migrationRows int
	if err := raw.QueryRow(`SELECT COUNT(*) FROM schema_migrations`).Scan(&migrationRows); err != nil {
		raw.Close()
		t.Fatal(err)
	}
	if migrationRows != 1 {
		raw.Close()
		t.Fatalf("schema migration rows = %d, want 1", migrationRows)
	}
	var coldAppliedAt int64
	if err := raw.QueryRow(`SELECT applied_at_ns FROM schema_migrations`).Scan(&coldAppliedAt); err != nil {
		raw.Close()
		t.Fatal(err)
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopening an already-current database exercises the warm path under the
	// same multi-process contention as a daemon restart or concurrent CLI use.
	runConcurrentOpenProcesses(t, path, "success", 8)

	raw, err = sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	var warmAppliedAt int64
	if err := raw.QueryRow(`SELECT applied_at_ns FROM schema_migrations`).Scan(&warmAppliedAt); err != nil {
		raw.Close()
		t.Fatal(err)
	}
	if warmAppliedAt != coldAppliedAt {
		raw.Close()
		t.Fatalf("warm open rewrote migration timestamp: got %d, want %d", warmAppliedAt, coldAppliedAt)
	}
	if _, err := raw.Exec(`INSERT INTO schema_migrations(version, applied_at_ns) VALUES (99, 1)`); err != nil {
		raw.Close()
		t.Fatal(err)
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}
	runConcurrentOpenProcesses(t, path, "newer", 8)
}

func TestConcurrentProcessOpenHelper(t *testing.T) {
	if os.Getenv("HOLLER_SQLITE_OPEN_HELPER") != "1" {
		t.Skip("helper process")
	}
	gate := os.Getenv("HOLLER_SQLITE_OPEN_GATE")
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Stat(gate); err == nil {
			break
		} else if !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
		if !time.Now().Before(deadline) {
			t.Fatal("timed out waiting for concurrent-open gate")
		}
		time.Sleep(time.Millisecond)
	}

	db, err := store.Open(context.Background(), os.Getenv("HOLLER_SQLITE_OPEN_PATH"))
	switch os.Getenv("HOLLER_SQLITE_OPEN_EXPECT") {
	case "success":
		if err != nil {
			t.Fatalf("concurrent open: %v", err)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	case "newer":
		if err == nil {
			db.Close()
			t.Fatal("opened database with a newer schema version")
		}
		if !strings.Contains(err.Error(), "newer than supported") {
			t.Fatalf("newer schema error = %v", err)
		}
	default:
		t.Fatalf("unknown helper expectation %q", os.Getenv("HOLLER_SQLITE_OPEN_EXPECT"))
	}
}

func runConcurrentOpenProcesses(t *testing.T, path, expectation string, count int) {
	t.Helper()
	gate := filepath.Join(t.TempDir(), "open.gate")
	type child struct {
		command *exec.Cmd
		output  bytes.Buffer
	}
	children := make([]child, count)
	for index := range children {
		command := exec.Command(os.Args[0], "-test.run=^TestConcurrentProcessOpenHelper$", "-test.count=1", "-test.timeout=60s")
		command.Env = append(os.Environ(),
			"HOLLER_SQLITE_OPEN_HELPER=1",
			"HOLLER_SQLITE_OPEN_GATE="+gate,
			"HOLLER_SQLITE_OPEN_PATH="+path,
			"HOLLER_SQLITE_OPEN_EXPECT="+expectation,
		)
		command.Stdout = &children[index].output
		command.Stderr = &children[index].output
		children[index].command = command
		if err := command.Start(); err != nil {
			t.Fatalf("start helper %d: %v", index, err)
		}
	}
	if err := os.WriteFile(gate, []byte("go"), 0o600); err != nil {
		t.Fatal(err)
	}
	for index := range children {
		if err := children[index].command.Wait(); err != nil {
			t.Errorf("helper %d (%s): %v\n%s", index, expectation, err, children[index].output.String())
		}
	}
}

func TestRecipientClaimCompletesNotificationOutbox(t *testing.T) {
	db, _ := openTestStore(t)
	ctx := context.Background()
	sent, err := db.Send(ctx, testRequest())
	if err != nil {
		t.Fatal(err)
	}
	job, err := db.ClaimNotification(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Claim(ctx, "reviewer", sent.Message.ID, time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := db.FinishNotification(ctx, job, bus.NotificationAccepted, "accepted; awaiting recipient claim"); err != nil {
		t.Fatalf("finish racing with claim: %v", err)
	}
	if _, err := db.ClaimNotification(ctx); !errors.Is(err, bus.ErrNoMessage) {
		t.Fatalf("claimed delivery left wake job active: %v", err)
	}
}

func TestAcceptedNotificationWaitsForClaimWithoutDuplicateWake(t *testing.T) {
	db, _ := openTestStore(t)
	ctx := context.Background()
	sent, err := db.Send(ctx, testRequest())
	if err != nil {
		t.Fatal(err)
	}
	job, err := db.ClaimNotification(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.FinishNotification(ctx, job, bus.NotificationAccepted, "native-queue"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ClaimNotification(ctx); !errors.Is(err, bus.ErrNoMessage) {
		t.Fatalf("accepted notification was reinjected: %v", err)
	}
	if _, err := db.Claim(ctx, "reviewer", sent.Message.ID, time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ClaimNotification(ctx); !errors.Is(err, bus.ErrNoMessage) {
		t.Fatalf("recipient claim left accepted wake active: %v", err)
	}
}

func TestNackCanRequeueOrDeadLetter(t *testing.T) {
	db, _ := openTestStore(t)
	ctx := context.Background()
	sent, err := db.Send(ctx, testRequest())
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	first, err := db.Claim(ctx, "reviewer", sent.Message.ID, time.Minute)
	if err != nil {
		t.Fatalf("first claim: %v", err)
	}
	if err := db.Nack(ctx, "reviewer", sent.Message.ID, first.LeaseToken, "temporary", false); err != nil {
		t.Fatalf("requeue nack: %v", err)
	}
	second, err := db.Claim(ctx, "reviewer", sent.Message.ID, time.Minute)
	if err != nil {
		t.Fatalf("second claim: %v", err)
	}
	if second.Attempt != 2 {
		t.Fatalf("second attempt = %d, want 2", second.Attempt)
	}
	if err := db.Nack(ctx, "reviewer", sent.Message.ID, second.LeaseToken, "poison", true); err != nil {
		t.Fatalf("terminal nack: %v", err)
	}
	inbox, err := db.CheckInbox(ctx, "reviewer", 10)
	if err != nil {
		t.Fatalf("check dead-lettered inbox: %v", err)
	}
	if len(inbox) != 0 {
		t.Fatalf("dead-lettered inbox = %+v, want empty", inbox)
	}
}

func TestConcurrentClaimsHaveOneWinner(t *testing.T) {
	db, _ := openTestStore(t)
	ctx := context.Background()
	if _, err := db.Send(ctx, testRequest()); err != nil {
		t.Fatalf("send: %v", err)
	}

	const contenders = 8
	start := make(chan struct{})
	results := make(chan error, contenders)
	var wait sync.WaitGroup
	for i := 0; i < contenders; i++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			_, err := db.Claim(ctx, "reviewer", "", time.Minute)
			results <- err
		}()
	}
	close(start)
	wait.Wait()
	close(results)

	winners := 0
	losers := 0
	for err := range results {
		switch {
		case err == nil:
			winners++
		case errors.Is(err, bus.ErrNoMessage):
			losers++
		default:
			t.Fatalf("unexpected claim error: %v", err)
		}
	}
	if winners != 1 || losers != contenders-1 {
		t.Fatalf("winners=%d losers=%d", winners, losers)
	}
}

func TestQueuedDeliverySurvivesStoreRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "holler.sqlite3")
	db, err := store.Open(ctx, path)
	if err != nil {
		t.Fatalf("open first store: %v", err)
	}
	sent, err := db.Send(ctx, testRequest())
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close first store: %v", err)
	}

	db, err = store.Open(ctx, path)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	claim, err := db.Claim(ctx, "reviewer", "", time.Minute)
	if err != nil {
		t.Fatalf("claim after restart: %v", err)
	}
	if claim.Message.ID != sent.Message.ID {
		t.Fatalf("claimed %s, want %s", claim.Message.ID, sent.Message.ID)
	}
}

func TestStoreRefusesNewerSchemaVersion(t *testing.T) {
	db, path := openTestStore(t)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	raw, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Exec(`INSERT INTO schema_migrations(version, applied_at_ns) VALUES (99, 1)`); err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Exec(`DROP TABLE notification_outbox`); err != nil {
		t.Fatal(err)
	}
	_ = raw.Close()
	if reopened, err := store.Open(context.Background(), path); err == nil {
		_ = reopened.Close()
		t.Fatal("opened database with a newer schema version")
	}
	raw, err = sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	var outboxTables int
	if err := raw.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'notification_outbox'`).Scan(&outboxTables); err != nil {
		t.Fatal(err)
	}
	if outboxTables != 0 {
		t.Fatal("newer database was mutated before compatibility refusal")
	}
}

func TestMigrationV5AddsSupersessionWithoutLosingRegistration(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "holler.sqlite3")
	db, err := store.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.RegisterSession(ctx, bus.RegistrationRequest{
		Actor: "claude", RunID: "run-1", Harness: "claude", AttentionMode: "hook-long-poll",
		SessionID: "session-1", DeliveryHandle: "session-1", ProjectID: "migration", Lease: 5 * time.Minute,
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	raw, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Exec(`ALTER TABLE registrations DROP COLUMN attention_superseded_at_ns`); err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Exec(`DELETE FROM schema_migrations`); err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Exec(`INSERT OR IGNORE INTO schema_migrations(version, applied_at_ns) VALUES (5, 1)`); err != nil {
		t.Fatal(err)
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = store.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.AttachMonitor(ctx, "claude", "run-1", "session-1", "claude", "hook-long-poll", 5*time.Minute); err != nil {
		t.Fatalf("attach migrated v5 registration: %v", err)
	}
}

func TestMigrationV6AddsDiscoverySchemaWithoutLosingRegistration(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "holler.sqlite3")
	db, err := store.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.RegisterSession(ctx, bus.RegistrationRequest{
		Actor: "codex", RunID: "run-1", Harness: "codex", SessionID: "session-1",
		ProjectID: "migration", WorkingDir: "/workspace/coupon", Lease: time.Hour,
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	raw, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{
		`DROP TABLE actor_profiles`,
		`ALTER TABLE registrations DROP COLUMN working_directory`,
		`DELETE FROM schema_migrations`,
		`INSERT INTO schema_migrations(version, applied_at_ns) VALUES (6, 1)`,
	} {
		if _, err := raw.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = store.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	registrations, err := db.LiveRegistrations(ctx, "codex")
	if err != nil || len(registrations) != 1 || registrations[0].WorkingDir != "" {
		t.Fatalf("migrated registrations = %+v, err = %v", registrations, err)
	}
	profile, err := db.SetActorProfile(ctx, "codex", "run-1", "migration", bus.ActorProfileRequest{RoleText: "Migration reviewer"})
	if err != nil || !profile.Updated {
		t.Fatalf("profile after migration = %+v, err = %v", profile, err)
	}
}

func TestMigrationV7AddsActorBindingsWithoutLosingProfiles(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "holler.sqlite3")
	db, err := store.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.SetActorProfile(ctx, "codex", "run-1", "migration", bus.ActorProfileRequest{RoleText: "Migration reviewer"}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	raw, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{
		`DROP TABLE continuity_bindings`,
		`DROP TABLE actor_allocations`,
		`DELETE FROM schema_migrations`,
		`INSERT INTO schema_migrations(version, applied_at_ns) VALUES (7, 1)`,
	} {
		if _, err := raw.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = store.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	binding, err := db.BindActor(ctx, bus.ActorBindRequest{
		RequestedActor: "codex", RunID: "run-2", NameMode: bus.NameModeAllocate,
		ContinuityHandles: []string{"launch:codex:migration"}, ProjectID: "migration",
	})
	if err != nil {
		t.Fatalf("binding after migration = %+v, err = %v", binding, err)
	}
	assertOpaqueActor(t, binding.Actor, "codex")
	directory, err := db.Who(ctx, 10)
	if err != nil || len(directory.Actors) != 2 {
		t.Fatalf("directory after migration = %+v, err = %v", directory, err)
	}
}

func TestMigrationV8AddsActorAdoptionsWithoutLosingMessages(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "holler.sqlite3")
	db, err := store.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	sent, err := db.Send(ctx, testRequest())
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	raw, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{
		`DROP TABLE actor_adoptions`,
		`DELETE FROM schema_migrations`,
		`INSERT INTO schema_migrations(version, applied_at_ns) VALUES (8, 1)`,
	} {
		if _, err := raw.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = store.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.RegisterSession(ctx, bus.RegistrationRequest{
		Actor: "replacement", RunID: "replacement-run", Harness: "test", SessionID: "replacement-session",
		ProjectID: "migration", Lease: time.Hour,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.AdoptActor(ctx, bus.AdoptRequest{
		SourceActor: "reviewer", AdoptingActor: "replacement", AdoptingRun: "replacement-run",
		ProjectID: "migration", IdempotencyKey: "migration-adopt",
	}); err != nil {
		t.Fatal(err)
	}
	items, err := db.CheckInbox(ctx, "replacement", 10)
	if err != nil || len(items) != 1 || items[0].MessageID != sent.Message.ID || items[0].OriginalRecipientActor != "reviewer" {
		t.Fatalf("migrated adopted inbox = %+v, err = %v", items, err)
	}
}

func TestMigrationV9AddsAliasesWithoutLosingMessages(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "holler.sqlite3")
	db, err := store.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	sent, err := db.Send(ctx, testRequest())
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	raw, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{
		`DROP TABLE actor_alias_history`,
		`DROP TABLE actor_aliases`,
		`DROP TABLE actor_names`,
		`ALTER TABLE messages DROP COLUMN requested_recipients_json`,
		`DELETE FROM schema_migrations`,
		`INSERT INTO schema_migrations(version, applied_at_ns) VALUES (9, 1)`,
	} {
		if _, err := raw.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = store.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	retry, err := db.Send(ctx, testRequest())
	if err != nil || !retry.Duplicate || retry.Message.ID != sent.Message.ID {
		t.Fatalf("legacy send retry = %+v, err = %v", retry, err)
	}
	if _, err := db.SetAlias(ctx, bus.AliasSetRequest{
		Alias: "review", Actor: "reviewer", UpdatedByActor: "operator", UpdatedByRun: "operator-run",
		ProjectID: "migration", IdempotencyKey: "migration-alias",
	}); err != nil {
		t.Fatalf("set alias after migration: %v", err)
	}
}

func TestMigrationV11AddsIdentityConditionsAndLifecycleWithoutLosingMessages(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "holler.sqlite3")
	db, err := store.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	sent, err := db.Send(ctx, testRequest())
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	raw, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{
		`DROP TABLE harness_instance_bindings`,
		`DROP TABLE operator_conditions`,
		`DROP TABLE actor_lifecycle`,
		`DELETE FROM schema_migrations`,
		`INSERT INTO schema_migrations(version, applied_at_ns) VALUES (11, 1)`,
	} {
		if _, err := raw.Exec(statement); err != nil {
			raw.Close()
			t.Fatal(err)
		}
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = store.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	items, err := db.CheckInbox(ctx, "reviewer", 10)
	if err != nil || len(items) != 1 || items[0].MessageID != sent.Message.ID {
		t.Fatalf("migrated inbox = %+v, err = %v", items, err)
	}
	binding, err := db.BindActor(ctx, bus.ActorBindRequest{
		RequestedActor: "worker", RunID: "run-1", NameMode: bus.NameModeAllocate,
		ContinuityHandles: []string{"process:codex:run-1", "instance:hin_migration", "launch:codex:migration"},
		ProjectID:         "migration",
	})
	if err != nil || binding.Actor == "" || binding.AssignedRunID != "run-1" {
		t.Fatalf("migrated harness binding = %+v, err = %v", binding, err)
	}
	condition, err := db.ObserveCondition(ctx, bus.ConditionObservation{
		Kind: "attention_unavailable", Subject: binding.Actor, ReasonCode: "migration_probe", Summary: "migration probe",
	})
	if err != nil || condition.Generation != 1 {
		t.Fatalf("migrated condition = %+v, err = %v", condition, err)
	}
	preflight, err := db.ArchivePreflight(ctx, "reviewer", 10)
	if err != nil || preflight.Actor != "reviewer" || len(preflight.Unread) != 1 {
		t.Fatalf("migrated lifecycle preflight = %+v, err = %v", preflight, err)
	}
}

func TestStoreRepairsUnversionedLegacyColumns(t *testing.T) {
	db, path := openTestStore(t)
	if _, err := db.RegisterSession(context.Background(), bus.RegistrationRequest{
		Actor: "legacy", RunID: "legacy-run", Harness: "claude", SessionID: "legacy-session",
		ProjectID: "migration", WorkingDir: "/workspace/legacy", Lease: time.Hour,
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	raw, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{
		`DELETE FROM schema_migrations`,
		`ALTER TABLE deliveries DROP COLUMN terminal_lease_token`,
		`ALTER TABLE registrations DROP COLUMN ended_at_ns`,
		`ALTER TABLE registrations DROP COLUMN registered_at_ns`,
		`ALTER TABLE registrations DROP COLUMN attention_mode`,
		`ALTER TABLE registrations DROP COLUMN working_directory`,
	} {
		if _, err := raw.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	_ = raw.Close()
	reopened, err := store.Open(context.Background(), path)
	if err != nil {
		t.Fatalf("repair unversioned schema: %v", err)
	}
	defer reopened.Close()
	directory, err := reopened.Who(context.Background(), 100)
	if err != nil {
		t.Fatalf("read directory after schema repair: %v", err)
	}
	legacy := directoryActor(t, directory, "legacy")
	if len(legacy.Sessions) != 1 || legacy.Sessions[0].StartedAt.IsZero() || legacy.Sessions[0].WorkingDir != "" {
		t.Fatalf("legacy directory entry after schema repair = %+v", legacy)
	}
}

func openTestStore(t *testing.T, options ...store.Option) (*store.Store, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "holler.sqlite3")
	db, err := store.Open(context.Background(), path, options...)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db, path
}

func testRequest() bus.SendRequest {
	return bus.SendRequest{
		IdempotencyKey:  "request-1",
		ProjectID:       "test-project",
		ChannelID:       "coordination",
		ThreadID:        "thread-1",
		FromActor:       "implementer",
		FromRun:         "run-1",
		FromRole:        "implementer",
		ToActors:        []string{"reviewer"},
		Type:            "QUESTION",
		DeliveryRequest: bus.DeliveryWake,
		Body:            json.RawMessage(`{"text":"Which retry policy applies?"}`),
	}
}

func sameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(duration time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(duration)
}
