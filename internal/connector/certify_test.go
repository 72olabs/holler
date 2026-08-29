package connector_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/72olabs/holler/internal/bus"
	"github.com/72olabs/holler/internal/connector"
)

func TestCertificationRequiresCorrelatedRealClientEvidence(t *testing.T) {
	const actor = "codex"
	const runID = "codex-run-1"
	ctx := bus.WithCaller(context.Background(), bus.Caller{Actor: actor, RunID: runID, Client: "bus-hook/0.1", BuildID: "test@clean", DaemonBuildID: "test@clean"})
	db := openStore(t)
	if _, err := db.RegisterSession(ctx, bus.RegistrationRequest{
		Actor: actor, RunID: runID, Harness: "codex", SessionID: "thread-1",
		DeliveryHandle: "thread-1", ProjectID: "experiment", Lease: time.Hour,
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.RecordHydration(ctx, "experiment", actor, runID, "codex", "thread-1", 1); err != nil {
		t.Fatal(err)
	}
	inbound := send(t, db, "observer", actor, "certification input").Message
	mcpCtx := bus.WithCaller(context.Background(), bus.Caller{Actor: actor, RunID: runID, Client: "bus-mcp/0.1", BuildID: "test@clean", DaemonBuildID: "test@clean"})
	claim, err := db.Claim(mcpCtx, actor, inbound.ID, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Ack(mcpCtx, actor, inbound.ID, claim.LeaseToken); err != nil {
		t.Fatal(err)
	}
	body := []byte(`{"text":"certification output"}`)
	output, err := db.Send(mcpCtx, bus.SendRequest{IdempotencyKey: "cert-output", ProjectID: "experiment", ChannelID: "direct", ThreadID: "thread-1", FromActor: actor, FromRun: runID, ToActors: []string{"observer"}, Type: "MESSAGE", DeliveryRequest: bus.DeliveryWake, Body: body})
	if err != nil {
		t.Fatal(err)
	}
	notifyCtx := bus.WithCaller(context.Background(), bus.Caller{Client: "hollerd/0.1", BuildID: "test@clean", DaemonBuildID: "test@clean"})
	if err := db.RecordNotification(notifyCtx, "experiment", output.Message.ID, bus.NotificationAttempt{
		Actor: actor, RunID: runID, SessionID: "thread-1", Harness: "codex", Result: "accepted", Detail: connector.AttentionNativeQueue,
	}); err != nil {
		t.Fatal(err)
	}

	report, err := connector.Certify(ctx, db, connector.CertificationConfig{
		Harness: "codex", Profile: "live-review", ProjectID: "experiment", Actor: actor, RunID: runID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if report.Ready || !hasCheck(reportAsDoctor(report), "canary.notification", connector.CheckFail) {
		t.Fatalf("notification for a different message produced READY: %+v", report)
	}
	if err := db.RecordNotification(notifyCtx, "experiment", inbound.ID, bus.NotificationAttempt{
		Actor: actor, RunID: runID, SessionID: "thread-1", Harness: "codex", Result: "accepted", Detail: connector.AttentionNativeQueue,
	}); err != nil {
		t.Fatal(err)
	}
	report, err = connector.Certify(ctx, db, connector.CertificationConfig{
		Harness: "codex", Profile: "live-review", ProjectID: "experiment", Actor: actor, RunID: runID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !report.Ready || report.State != connector.StateReady {
		t.Fatalf("certification = %+v", report)
	}

	staleWindow, err := connector.Certify(ctx, db, connector.CertificationConfig{
		Harness: "codex", Profile: "live-review", ProjectID: "experiment", Actor: actor, RunID: runID,
		AfterDurable: report.ObservedDurable, AfterOperational: report.ObservedOperational,
	})
	if err != nil {
		t.Fatal(err)
	}
	if staleWindow.Ready || staleWindow.State != connector.StateConfigured {
		t.Fatalf("historical evidence leaked across event cursors: %+v", staleWindow)
	}
}

func TestLiveCertificationReportsDegradedWhenOnlyWakeEvidenceIsMissing(t *testing.T) {
	const actor = "claude"
	const runID = "claude-run-1"
	ctx := bus.WithCaller(context.Background(), bus.Caller{Actor: actor, RunID: runID, Client: "bus-hook/0.1", BuildID: "test@clean", DaemonBuildID: "test@clean"})
	db := openStore(t)
	if _, err := db.RegisterSession(ctx, bus.RegistrationRequest{
		Actor: actor, RunID: runID, Harness: "claude", SessionID: "session-1",
		DeliveryHandle: "session-1", ProjectID: "experiment", Lease: time.Hour,
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.RecordHydration(ctx, "experiment", actor, runID, "claude", "session-1", 1); err != nil {
		t.Fatal(err)
	}
	inbound := send(t, db, "observer", actor, "certification input").Message
	mcpCtx := bus.WithCaller(context.Background(), bus.Caller{Actor: actor, RunID: runID, Client: "bus-mcp/0.1", BuildID: "test@clean", DaemonBuildID: "test@clean"})
	claim, err := db.Claim(mcpCtx, actor, inbound.ID, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Ack(mcpCtx, actor, inbound.ID, claim.LeaseToken); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Send(mcpCtx, bus.SendRequest{IdempotencyKey: "claude-cert-output", ProjectID: "experiment", ChannelID: "direct", ThreadID: "thread-1", FromActor: actor, FromRun: runID, ToActors: []string{"observer"}, Type: "MESSAGE", DeliveryRequest: bus.DeliveryWake, Body: []byte(`{"text":"certification output"}`)}); err != nil {
		t.Fatal(err)
	}

	report, err := connector.Certify(ctx, db, connector.CertificationConfig{
		Harness: "claude", Profile: "live-review", ProjectID: "experiment", Actor: actor, RunID: runID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if report.Ready || report.State != connector.StateDegraded ||
		!hasCheck(reportAsDoctor(report), "canary.notification", connector.CheckFail) {
		t.Fatalf("certification = %+v", report)
	}
}

func TestCertificationRejectsCLIAndMonitorEvidence(t *testing.T) {
	const actor, runID = "codex", "codex-run-cli"
	hookCtx := bus.WithCaller(context.Background(), bus.Caller{Actor: actor, RunID: runID, Client: "bus-hook/0.1", BuildID: "test@clean", DaemonBuildID: "test@clean"})
	db := openStore(t)
	_, _ = db.RegisterSession(hookCtx, bus.RegistrationRequest{Actor: actor, RunID: runID, Harness: "codex", SessionID: "thread-cli", DeliveryHandle: "thread-cli", ProjectID: "experiment", Lease: time.Hour})
	_ = db.RecordHydration(hookCtx, "experiment", actor, runID, "codex", "thread-cli", 0)
	cliCtx := bus.WithCaller(context.Background(), bus.Caller{Actor: actor, RunID: runID, Client: "bus-cli/0.1", BuildID: "test@clean", DaemonBuildID: "test@clean"})
	inbound := send(t, db, "observer", actor, "cli evidence").Message
	claim, _ := db.Claim(cliCtx, actor, inbound.ID, time.Minute)
	_ = db.Ack(cliCtx, actor, inbound.ID, claim.LeaseToken)
	_, _ = db.Send(cliCtx, bus.SendRequest{IdempotencyKey: "cli-output", ProjectID: "experiment", ChannelID: "direct", FromActor: actor, FromRun: runID, ToActors: []string{"observer"}, Type: "MESSAGE", DeliveryRequest: bus.DeliveryWake, Body: []byte(`{"text":"cli"}`)})
	notifyCtx := bus.WithCaller(context.Background(), bus.Caller{Client: "hollerd/0.1", BuildID: "test@clean", DaemonBuildID: "test@clean"})
	_ = db.RecordNotification(notifyCtx, "experiment", inbound.ID, bus.NotificationAttempt{Actor: actor, RunID: runID, SessionID: "thread-cli", Harness: "codex", Result: "monitor-notified"})
	report, err := connector.Certify(context.Background(), db, connector.CertificationConfig{Harness: "codex", Profile: "live-review", ProjectID: "experiment", Actor: actor, RunID: runID})
	if err != nil {
		t.Fatal(err)
	}
	if report.Ready || report.State == connector.StateReady {
		t.Fatalf("CLI/monitor evidence produced READY: %+v", report)
	}
}

func TestLiveCertificationRejectsStartupOnlyForEveryHarness(t *testing.T) {
	for _, harness := range []string{"codex", "claude", "opencode"} {
		t.Run(harness, func(t *testing.T) {
			_, err := connector.Certify(context.Background(), fixedEventReader{}, connector.CertificationConfig{
				Harness: harness, Profile: "live-review", ProjectID: "experiment", Actor: harness,
				RunID: harness + "-run", AttentionMode: connector.AttentionStartupOnly,
			})
			if err == nil || !strings.Contains(err.Error(), "startup-only") {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

func TestCertificationRejectsDirtyMCPBuildEvidence(t *testing.T) {
	const actor, runID = "codex", "codex-run-dirty"
	hookCtx := bus.WithCaller(context.Background(), bus.Caller{
		Actor: actor, RunID: runID, Client: "bus-hook/0.1",
		BuildID: "test@clean", DaemonBuildID: "test@clean",
	})
	db := openStore(t)
	_, _ = db.RegisterSession(hookCtx, bus.RegistrationRequest{
		Actor: actor, RunID: runID, Harness: "codex", SessionID: "thread-dirty",
		DeliveryHandle: "thread-dirty", ProjectID: "experiment", Lease: time.Hour,
	})
	_ = db.RecordHydration(hookCtx, "experiment", actor, runID, "codex", "thread-dirty", 0)

	inbound := send(t, db, "observer", actor, "dirty build evidence").Message
	dirtyMCP := bus.WithCaller(context.Background(), bus.Caller{
		Actor: actor, RunID: runID, Client: "bus-mcp/0.1",
		BuildID: "test@clean+dirty", DaemonBuildID: "test@clean",
	})
	claim, err := db.Claim(dirtyMCP, actor, inbound.ID, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Ack(dirtyMCP, actor, inbound.ID, claim.LeaseToken); err != nil {
		t.Fatal(err)
	}
	_, err = db.Send(dirtyMCP, bus.SendRequest{
		IdempotencyKey: "dirty-output", ProjectID: "experiment", ChannelID: "direct",
		FromActor: actor, FromRun: runID, ToActors: []string{"observer"}, Type: "MESSAGE",
		DeliveryRequest: bus.DeliveryWake, Body: []byte(`{"text":"dirty"}`),
	})
	if err != nil {
		t.Fatal(err)
	}

	report, err := connector.Certify(context.Background(), db, connector.CertificationConfig{
		Harness: "codex", Profile: "async-peer", ProjectID: "experiment", Actor: actor, RunID: runID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if report.Ready || !hasCheck(reportAsDoctor(report), "canary.build_identity", connector.CheckFail) {
		t.Fatalf("dirty MCP build produced READY: %+v", report)
	}
}

func TestCertificationExplainsMissingLegacyClientBuild(t *testing.T) {
	const actor, runID = "codex", "legacy-client-run"
	ctx := bus.WithCaller(context.Background(), bus.Caller{
		Actor: actor, RunID: runID, Client: "bus-hook/0.1", DaemonBuildID: "daemon@clean",
	})
	db := openStore(t)
	_, err := db.RegisterSession(ctx, bus.RegistrationRequest{
		Actor: actor, RunID: runID, Harness: "codex", SessionID: "legacy-session",
		DeliveryHandle: "legacy-session", ProjectID: "experiment", Lease: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	report, err := connector.Certify(context.Background(), db, connector.CertificationConfig{
		Harness: "codex", Profile: "async-peer", ProjectID: "experiment", Actor: actor, RunID: runID,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, check := range report.Checks {
		if check.ID == "canary.build_identity" {
			if !strings.Contains(check.Summary, "missing the self-reported client build") || check.Evidence["client_build_seen"] != "false" {
				t.Fatalf("legacy build explanation = %+v", check)
			}
			return
		}
	}
	t.Fatal("build identity check missing")
}

func TestCertificationAcceptsAnExactlyTenThousandEventWindow(t *testing.T) {
	reader := fixedEventReader{count: 10000}
	if _, err := connector.Certify(context.Background(), reader, connector.CertificationConfig{
		Harness: "codex", Profile: "async-peer", ProjectID: "experiment", Actor: "codex", RunID: "run-1",
	}); err != nil {
		t.Fatalf("exactly 10000 events: %v", err)
	}
}

type fixedEventReader struct{ count int64 }

func (reader fixedEventReader) ListEvents(_ context.Context, project, stream string, after int64, limit int) ([]bus.Event, error) {
	if stream != "durable" || after >= reader.count {
		return nil, nil
	}
	remaining := int(reader.count - after)
	if remaining > limit {
		remaining = limit
	}
	events := make([]bus.Event, remaining)
	for index := range events {
		events[index] = bus.Event{PartitionID: project, Stream: stream, Position: after + int64(index) + 1}
	}
	return events, nil
}

func reportAsDoctor(report connector.CertificationReport) connector.DoctorReport {
	return connector.DoctorReport{Checks: report.Checks}
}
