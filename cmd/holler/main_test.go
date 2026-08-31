package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/72olabs/holler/internal/api"
	"github.com/72olabs/holler/internal/attention"
	"github.com/72olabs/holler/internal/bus"
	"github.com/72olabs/holler/internal/connector"
	"github.com/72olabs/holler/internal/daemon"
	store "github.com/72olabs/holler/internal/store/sqlite"
)

func TestCLISendInboxClaimAck(t *testing.T) {
	ctx := context.Background()
	socket := startAPIServer(t)

	send := invoke(t, ctx, "send",
		"--socket", socket,
		"--actor", "implementer",
		"--run", "run-1",
		"--to", "reviewer",
		"--idempotency-key", "cli-1",
		"--type", "QUESTION",
		"--body", `{"text":"Which retry policy?"}`,
	)
	var sent bus.SendResult
	decode(t, send, &sent)
	if sent.Message.ID == "" {
		t.Fatal("send returned no message id")
	}

	inbox := invoke(t, ctx, "inbox", "--socket", socket, "--actor", "reviewer")
	var items []bus.InboxItem
	decode(t, inbox, &items)
	if len(items) != 1 || items[0].MessageID != sent.Message.ID {
		t.Fatalf("inbox = %+v", items)
	}

	claimed := invoke(t, ctx, "claim", "--socket", socket, "--actor", "reviewer")
	var claim bus.Claim
	decode(t, claimed, &claim)
	if claim.LeaseToken == "" || claim.Message.ID != sent.Message.ID {
		t.Fatalf("claim = %+v", claim)
	}

	invoke(t, ctx, "ack",
		"--socket", socket,
		"--actor", "reviewer",
		"--message", sent.Message.ID,
		"--lease-token", claim.LeaseToken,
	)
	inbox = invoke(t, ctx, "inbox", "--socket", socket, "--actor", "reviewer")
	decode(t, inbox, &items)
	if len(items) != 0 {
		t.Fatalf("inbox after ack = %+v", items)
	}
}

func TestCLIProfileAndWho(t *testing.T) {
	ctx := context.Background()
	socket := startAPIServer(t)
	profileRaw := invoke(t, ctx, "profile",
		"--socket", socket,
		"--actor", "coupon-reviewer",
		"--run", "review-run-1",
		"--project", "coupon",
		"--role", "Reviews coupon correctness",
		"--accepts", "REVIEW_REQUEST,QUESTION",
	)
	var profile bus.ActorProfileResult
	decode(t, profileRaw, &profile)
	if !profile.Updated || profile.Profile.Actor != "coupon-reviewer" {
		t.Fatalf("profile = %+v", profile)
	}
	whoRaw := invoke(t, ctx, "who", "--socket", socket)
	var directory bus.ActorDirectory
	decode(t, whoRaw, &directory)
	if len(directory.Actors) != 1 || directory.Actors[0].Actor != "coupon-reviewer" || directory.Actors[0].Profile == nil {
		t.Fatalf("directory = %+v", directory)
	}
}

func TestCLIMCPUsesConnectorBoundEnvironment(t *testing.T) {
	socket := startAPIServer(t)
	t.Setenv("HOLLER_SOCKET", socket)
	t.Setenv("HOLLER_ACTOR", "codex")
	t.Setenv("HOLLER_RUN", "codex-run-1")
	t.Setenv("HOLLER_PEER", "claude")
	input := strings.NewReader(
		`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05"}}` + "\n" +
			`{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"bus_status","arguments":{}}}` + "\n",
	)
	var stdout, stderr bytes.Buffer
	if exit := run(context.Background(), []string{"mcp"}, input, &stdout, &stderr); exit != 0 {
		t.Fatalf("mcp exit=%d stderr=%s", exit, stderr.String())
	}
	decoder := json.NewDecoder(&stdout)
	var initialized, status map[string]interface{}
	if err := decoder.Decode(&initialized); err != nil {
		t.Fatalf("decode initialize: %v", err)
	}
	if err := decoder.Decode(&status); err != nil {
		t.Fatalf("decode status: %v", err)
	}
	structured := status["result"].(map[string]interface{})["structuredContent"].(map[string]interface{})
	if structured["actor"] != "codex" || structured["peer"] != "claude" {
		t.Fatalf("connector identity = %+v", structured)
	}
}

func TestPlainClaudePluginCommandsSharePersistedBindingAndProcessRun(t *testing.T) {
	socket := startAPIServer(t)
	configPath := filepath.Join(t.TempDir(), "claude.json")
	config := fmt.Sprintf(`{
  "schema_version": 1,
  "attention_mode": "hook-long-poll",
  "actor": "claude-plain",
  "peer": "codex-plain",
  "project": "experiment",
  "channel": "direct",
  "socket": %q
}`, socket)
	if err := os.WriteFile(configPath, []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HOLLER_CONNECTOR_CONFIG", configPath)
	for _, name := range []string{"HOLLER_SOCKET", "HOLLER_ACTOR", "HOLLER_RUN", "HOLLER_PEER", "HOLLER_PROJECT", "HOLLER_CHANNEL"} {
		t.Setenv(name, "")
	}

	var hookOut, hookErr bytes.Buffer
	if exit := run(context.Background(), []string{"hook", "--harness", "claude"},
		strings.NewReader(`{"session_id":"claude-session-plain"}`), &hookOut, &hookErr); exit != 0 {
		t.Fatalf("hook exit=%d stderr=%s", exit, hookErr.String())
	}

	input := strings.NewReader(
		`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05"}}` + "\n" +
			`{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"bus_status","arguments":{}}}` + "\n",
	)
	var mcpOut, mcpErr bytes.Buffer
	if exit := run(context.Background(), []string{"mcp", "--harness", "claude"}, input, &mcpOut, &mcpErr); exit != 0 {
		t.Fatalf("mcp exit=%d stderr=%s", exit, mcpErr.String())
	}
	decoder := json.NewDecoder(&mcpOut)
	var initialized, status map[string]interface{}
	if err := decoder.Decode(&initialized); err != nil {
		t.Fatal(err)
	}
	if err := decoder.Decode(&status); err != nil {
		t.Fatal(err)
	}
	structured := status["result"].(map[string]interface{})["structuredContent"].(map[string]interface{})
	runID, _ := structured["run"].(string)
	if structured["actor"] != "claude-plain" || structured["peer"] != "codex-plain" || runID == "" {
		t.Fatalf("status=%+v", structured)
	}
	client, err := api.Dial(context.Background(), socket, api.Identity{Actor: "claude-plain", RunID: runID, Client: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	registrations, err := client.LiveRegistrations(context.Background(), "claude-plain")
	if err != nil || len(registrations) != 1 || registrations[0].RunID != runID {
		t.Fatalf("registrations=%+v status_run=%q err=%v", registrations, runID, err)
	}
}

func TestCLIHookDegradesWithoutBlockingHarnessStartup(t *testing.T) {
	var stdout, stderr bytes.Buffer
	exit := run(context.Background(), []string{
		"hook", "--socket", filepath.Join(t.TempDir(), "missing.sock"),
		"--actor", "codex", "--run", "run-1", "--harness", "codex",
	}, strings.NewReader(`{"session_id":"thread-1"}`), &stdout, &stderr)
	if exit != 0 {
		t.Fatalf("hook exit=%d stderr=%s", exit, stderr.String())
	}
	var output struct {
		HookSpecificOutput struct {
			AdditionalContext string `json:"additionalContext"`
		} `json:"hookSpecificOutput"`
	}
	decode(t, stdout.Bytes(), &output)
	if !strings.Contains(output.HookSpecificOutput.AdditionalContext, "DEGRADED") ||
		!strings.Contains(stderr.String(), "registration failed") && !strings.Contains(stderr.String(), "connect") {
		t.Fatalf("stdout=%s stderr=%s", stdout.String(), stderr.String())
	}
}

func TestCLIClaudeMonitorExitsTwoWithReferenceOnlyWake(t *testing.T) {
	broker := attention.NewBroker()
	socket := startAPIServerWithOptions(t, api.WithAttentionBroker(broker))
	t.Setenv("HOLLER_CLAUDE_ATTENTION", connector.AttentionHookLongPoll)
	t.Setenv("HOLLER_SOCKET", socket)
	t.Setenv("HOLLER_ACTOR", "claude")
	t.Setenv("HOLLER_RUN", "claude-run")
	client, err := api.Dial(context.Background(), socket, api.Identity{Actor: "claude", RunID: "claude-run", Client: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	registration, err := client.RegisterSession(context.Background(), bus.RegistrationRequest{
		Actor: "claude", RunID: "claude-run", Harness: "claude", SessionID: "session-1",
		DeliveryHandle: "session-1", ProjectID: "experiment", Lease: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	type monitorResult struct {
		exit   int
		stderr string
	}
	done := make(chan monitorResult, 1)
	stderrWriter, finishStderr := startPipeCapture(t)
	go func() {
		var stdout bytes.Buffer
		exit := run(context.Background(), []string{
			"monitor", "--harness", "claude", "--wait", "1s",
		}, strings.NewReader(`{"session_id":"session-1"}`), &stdout, stderrWriter)
		_ = stderrWriter.Close()
		done <- monitorResult{exit: exit, stderr: finishStderr()}
	}()
	message := bus.Message{
		ID: "msg-reference", ThreadID: "thread-1", FromActor: "codex", Type: "QUESTION",
		DeliveryRequest: bus.DeliveryWake, Body: json.RawMessage(`{"text":"secret body"}`),
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		adapter, accepted := broker.Notify(registration, message)
		if accepted {
			if adapter != "hook-long-poll" {
				t.Fatalf("adapter = %q", adapter)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("monitor did not park")
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case result := <-done:
		if result.exit != 2 || !strings.Contains(result.stderr, message.ID) || strings.Contains(result.stderr, "secret body") {
			t.Fatalf("monitor result = %+v", result)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("monitor did not exit for asyncRewake")
	}
}

func TestCLIClaudeMonitorReconcilesDurableInboxBeforeParking(t *testing.T) {
	socket := startAPIServerWithOptions(t, api.WithAttentionBroker(attention.NewBroker()))
	t.Setenv("HOLLER_CLAUDE_ATTENTION", connector.AttentionHookLongPoll)
	t.Setenv("HOLLER_SOCKET", socket)
	t.Setenv("HOLLER_ACTOR", "claude")
	t.Setenv("HOLLER_RUN", "claude-run")
	client, err := api.Dial(context.Background(), socket, api.Identity{Actor: "claude", RunID: "claude-run", Client: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if _, err := client.RegisterSession(context.Background(), bus.RegistrationRequest{
		Actor: "claude", RunID: "claude-run", Harness: "claude", AttentionMode: connector.AttentionHookLongPoll,
		SessionID: "session-1", DeliveryHandle: "session-1", ProjectID: "experiment", Lease: time.Hour,
	}); err != nil {
		t.Fatal(err)
	}
	sentRaw := invoke(t, context.Background(), "send", "--socket", socket, "--actor", "observer", "--run", "observer-run",
		"--to", "claude", "--project", "experiment", "--idempotency-key", "preexisting-wake", "--type", "QUESTION",
		"--body", `{"text":"secret body"}`)
	var sent bus.SendResult
	decode(t, sentRaw, &sent)
	var stdout bytes.Buffer
	stderrWriter, finishStderr := startPipeCapture(t)
	exit := run(context.Background(), []string{"monitor", "--harness", "claude", "--wait", "1s"},
		strings.NewReader(`{"session_id":"session-1"}`), &stdout, stderrWriter)
	_ = stderrWriter.Close()
	stderr := finishStderr()
	if exit != 2 || !strings.Contains(stderr, sent.Message.ID) || strings.Contains(stderr, "secret body") {
		t.Fatalf("exit=%d stderr=%q", exit, stderr)
	}
}

func TestCLIClaudeMonitorRejectsMissingIdentityWithoutRetrying(t *testing.T) {
	t.Setenv("HOLLER_CLAUDE_ATTENTION", connector.AttentionHookLongPoll)
	t.Setenv("HOLLER_CONNECTOR_CONFIG", filepath.Join(t.TempDir(), "missing.json"))
	t.Setenv("HOLLER_ACTOR", "")
	t.Setenv("HOLLER_RUN", "")
	var stdout, stderr bytes.Buffer
	exit := run(context.Background(), []string{"monitor", "--harness", "claude"},
		strings.NewReader(`{"session_id":"session-1"}`), &stdout, &stderr)
	if exit != 1 || !strings.Contains(stderr.String(), "configuration is invalid") {
		t.Fatalf("exit = %d, stderr = %q", exit, stderr.String())
	}
}

func TestCLIClaudeMonitorIsDisabledByAnotherSelectedAdapter(t *testing.T) {
	t.Setenv("HOLLER_CLAUDE_ATTENTION", connector.AttentionStartupOnly)
	t.Setenv("HOLLER_CONNECTOR_CONFIG", filepath.Join(t.TempDir(), "missing.json"))
	t.Setenv("HOLLER_ACTOR", "")
	t.Setenv("HOLLER_RUN", "")
	var stdout, stderr bytes.Buffer
	exit := run(context.Background(), []string{"monitor", "--harness", "claude"},
		strings.NewReader(`{"session_id":"session-1"}`), &stdout, &stderr)
	if exit != 0 || stderr.Len() != 0 {
		t.Fatalf("exit=%d stderr=%q", exit, stderr.String())
	}
}

func TestCLIClaudeMonitorFailsVisiblyWithoutResultChannel(t *testing.T) {
	t.Setenv("HOLLER_CLAUDE_ATTENTION", connector.AttentionHookLongPoll)
	t.Setenv("HOLLER_SOCKET", filepath.Join(t.TempDir(), "holler.sock"))
	t.Setenv("HOLLER_ACTOR", "claude")
	t.Setenv("HOLLER_RUN", "claude-run")
	var stdout, stderr bytes.Buffer
	exit := run(context.Background(), []string{"monitor", "--harness", "claude"},
		strings.NewReader(`{"session_id":"session-1"}`), &stdout, &stderr)
	if exit != 1 || !strings.Contains(stderr.String(), "did not provide a watchable result channel") {
		t.Fatalf("exit=%d stderr=%q", exit, stderr.String())
	}
}

func TestCLIClaudeMonitorReportsTerminalPresenceOutcomes(t *testing.T) {
	for _, test := range []struct {
		name, want string
		prepare    func(t *testing.T, client *api.Client)
	}{
		{
			name: "session-ended", want: `session "session-1" has ended`,
			prepare: func(t *testing.T, client *api.Client) {
				if err := client.ExpireRegistration(context.Background(), "claude", "claude-run", "session-1", "SessionEnd"); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "presence-superseded", want: `actor "claude" run "claude-run" was superseded`,
			prepare: func(t *testing.T, client *api.Client) {
				if _, err := client.RegisterSession(context.Background(), bus.RegistrationRequest{
					Actor: "claude", RunID: "newer-run", Harness: "claude", AttentionMode: connector.AttentionHookLongPoll,
					SessionID: "session-2", DeliveryHandle: "session-2", ProjectID: "experiment", Lease: time.Hour,
				}); err != nil {
					t.Fatal(err)
				}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			socket := startAPIServerWithOptions(t, api.WithAttentionBroker(attention.NewBroker()))
			client, err := api.Dial(context.Background(), socket, api.Identity{Actor: "claude", RunID: "claude-run", Client: "test"})
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			if _, err := client.RegisterSession(context.Background(), bus.RegistrationRequest{
				Actor: "claude", RunID: "claude-run", Harness: "claude", AttentionMode: connector.AttentionHookLongPoll,
				SessionID: "session-1", DeliveryHandle: "session-1", ProjectID: "experiment", Lease: time.Hour,
			}); err != nil {
				t.Fatal(err)
			}
			test.prepare(t, client)
			t.Setenv("HOLLER_CLAUDE_ATTENTION", connector.AttentionHookLongPoll)
			t.Setenv("HOLLER_SOCKET", socket)
			t.Setenv("HOLLER_ACTOR", "claude")
			t.Setenv("HOLLER_RUN", "claude-run")
			var stdout bytes.Buffer
			stderrWriter, finishStderr := startPipeCapture(t)
			exit := run(context.Background(), []string{"monitor", "--harness", "claude", "--startup-grace", "50ms"},
				strings.NewReader(`{"session_id":"session-1"}`), &stdout, stderrWriter)
			_ = stderrWriter.Close()
			stderr := finishStderr()
			if exit != 0 || !strings.Contains(stderr, test.want) {
				t.Fatalf("exit=%d stderr=%q", exit, stderr)
			}
		})
	}
}

func TestClaudeMonitorReconnectsAcrossShortAndLongDaemonOutages(t *testing.T) {
	for _, outage := range []time.Duration{4 * time.Minute, 6 * time.Minute} {
		t.Run(outage.String(), func(t *testing.T) {
			type monitorResult struct {
				exit   int
				stderr string
			}
			directory := t.TempDir()
			database := filepath.Join(directory, "holler.sqlite3")
			socketDirectory, err := os.MkdirTemp("/tmp", "ab-outage-")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = os.RemoveAll(socketDirectory) })
			socket := filepath.Join(socketDirectory, "holler.sock")
			clock := &daemonTestClock{now: time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)}
			startDaemon := func() (context.CancelFunc, <-chan error) {
				ctx, cancel := context.WithCancel(context.Background())
				done := make(chan error, 1)
				go func() {
					done <- daemon.Run(ctx, daemon.Config{DatabasePath: database, SocketPath: socket, Clock: clock.Now}, nil)
				}()
				deadline := time.Now().Add(3 * time.Second)
				for time.Now().Before(deadline) {
					client, err := api.Dial(context.Background(), socket, api.Identity{Actor: "probe", RunID: "probe-run", Client: "test"})
					if err == nil {
						_ = client.Close()
						return cancel, done
					}
					time.Sleep(10 * time.Millisecond)
				}
				t.Fatal("daemon did not become ready")
				return cancel, done
			}
			stopDaemon := func(cancel context.CancelFunc, done <-chan error) {
				cancel()
				select {
				case err := <-done:
					if err != nil {
						t.Fatalf("daemon stop: %v", err)
					}
				case <-time.After(3 * time.Second):
					t.Fatal("daemon did not stop")
				}
			}

			cancelDaemon, daemonDone := startDaemon()
			registrationClient, err := api.Dial(context.Background(), socket, api.Identity{Actor: "claude", RunID: "claude-run", Client: "test"})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := registrationClient.RegisterSession(context.Background(), bus.RegistrationRequest{
				Actor: "claude", RunID: "claude-run", Harness: "claude", AttentionMode: connector.AttentionHookLongPoll,
				SessionID: "session-1", DeliveryHandle: "session-1", ProjectID: "outage", Lease: 5 * time.Minute,
			}); err != nil {
				t.Fatal(err)
			}
			_ = registrationClient.Close()
			t.Setenv("HOLLER_CLAUDE_ATTENTION", connector.AttentionHookLongPoll)
			t.Setenv("HOLLER_SOCKET", socket)
			t.Setenv("HOLLER_ACTOR", "claude")
			t.Setenv("HOLLER_RUN", "claude-run")
			monitorDone := make(chan monitorResult, 1)
			stderrReader, stderrWriter, err := os.Pipe()
			if err != nil {
				t.Fatal(err)
			}
			go func() {
				var stdout, stderr bytes.Buffer
				captureDone := make(chan error, 1)
				go func() {
					_, copyErr := io.Copy(&stderr, stderrReader)
					captureDone <- copyErr
				}()
				exit := run(context.Background(), []string{"monitor", "--harness", "claude", "--wait", "200ms", "--startup-grace", "200ms"},
					strings.NewReader(`{"session_id":"session-1"}`), &stdout, stderrWriter)
				_ = stderrWriter.Close()
				_ = <-captureDone
				_ = stderrReader.Close()
				monitorDone <- monitorResult{exit: exit, stderr: stderr.String()}
			}()
			time.Sleep(300 * time.Millisecond)
			stopDaemon(cancelDaemon, daemonDone)
			clock.Advance(outage)
			cancelDaemon, daemonDone = startDaemon()
			defer stopDaemon(cancelDaemon, daemonDone)

			sender, err := api.Dial(context.Background(), socket, api.Identity{Actor: "sender", RunID: "sender-run", Client: "test"})
			if err != nil {
				t.Fatal(err)
			}
			sent, err := sender.Send(context.Background(), bus.SendRequest{
				IdempotencyKey: "wake-" + outage.String(), ProjectID: "outage", ChannelID: "direct",
				ToActors: []string{"claude"}, Type: "MESSAGE", DeliveryRequest: bus.DeliveryWake,
				Body: json.RawMessage(`{"text":"reconnected"}`),
			})
			_ = sender.Close()
			if err != nil {
				t.Fatal(err)
			}
			select {
			case result := <-monitorDone:
				if result.exit != 2 || !strings.Contains(result.stderr, sent.Message.ID) {
					t.Fatalf("monitor result after %s outage = %+v", outage, result)
				}
			case <-time.After(5 * time.Second):
				t.Fatalf("monitor did not reconnect after %s outage", outage)
			}
		})
	}
}

func TestResultChannelContextCancelsOnSocketClosure(t *testing.T) {
	descriptors, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	resultWriter := os.NewFile(uintptr(descriptors[0]), "result-writer")
	peer := os.NewFile(uintptr(descriptors[1]), "result-peer")
	defer resultWriter.Close()
	ctx, cancel, watched := resultChannelContext(context.Background(), resultWriter)
	defer cancel()
	if !watched {
		t.Fatal("socket result channel was not watched")
	}
	if err := peer.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("result-channel closure did not cancel monitor context")
	}
}

type daemonTestClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *daemonTestClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *daemonTestClock) Advance(duration time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(duration)
}

func TestCLIConnectorManifestReportsFrozenSurface(t *testing.T) {
	raw := invoke(t, context.Background(), "connector", "manifest", "--harness", "codex")
	var manifest struct {
		Harness         string `json:"harness"`
		ToolSurfaceHash string `json:"tool_surface_hash"`
		PackageHash     string `json:"package_hash"`
	}
	decode(t, raw, &manifest)
	if manifest.Harness != "codex" || !strings.HasPrefix(manifest.ToolSurfaceHash, "sha256:") ||
		!strings.HasPrefix(manifest.PackageHash, "sha256:") {
		t.Fatalf("manifest = %+v", manifest)
	}
}

func TestCLIProductSetupDryRunUsesHarnessDefaults(t *testing.T) {
	repo := filepath.Clean(filepath.Join("..", ".."))
	marketplace := filepath.Join(repo, "connectors", "marketplace")
	for _, test := range []struct {
		harness, actor, peer, attention string
	}{
		{harness: "claude", actor: "claude", peer: "codex", attention: connector.AttentionHookLongPoll},
		{harness: "codex", actor: "codex", peer: "claude", attention: connector.AttentionNativeQueue},
	} {
		t.Run(test.harness, func(t *testing.T) {
			home := t.TempDir()
			t.Setenv("HOME", home)
			t.Setenv("CODEX_HOME", filepath.Join(home, ".codex"))
			t.Setenv("CLAUDE_CONFIG_DIR", filepath.Join(home, ".claude"))
			var stdout, stderr bytes.Buffer
			exit := run(context.Background(), []string{
				"setup", test.harness, "--dry-run", "--marketplace", marketplace,
				"--client-binary", "/usr/bin/true", "--daemon-binary", "/usr/bin/true",
			}, strings.NewReader(""), &stdout, &stderr)
			if exit != 0 {
				t.Fatalf("exit=%d stderr=%s", exit, stderr.String())
			}
			var result productSetupResult
			decode(t, stdout.Bytes(), &result)
			wantService := "launchd-user"
			if runtime.GOOS == "linux" {
				wantService = "systemd-user"
			}
			if result.Applied || result.Harness != test.harness || result.Connector.AttentionMode != test.attention ||
				result.Daemon.Kind != wantService {
				t.Fatalf("result=%+v", result)
			}
			selection, err := os.ReadFile(result.Connector.ConnectorConfigPath)
			if !os.IsNotExist(err) {
				t.Fatalf("dry run wrote connector selection %s: %s", selection, err)
			}
		})
	}
}

func TestCLIProductSetupConfirmationDefaultsToNo(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("CLAUDE_CONFIG_DIR", filepath.Join(home, ".claude"))
	marketplace := filepath.Join("..", "..", "connectors", "marketplace")
	var stdout, stderr bytes.Buffer
	exit := run(context.Background(), []string{
		"setup", "claude", "--marketplace", marketplace,
		"--client-binary", "/usr/bin/true", "--daemon-binary", "/usr/bin/true",
	}, strings.NewReader("\n"), &stdout, &stderr)
	if exit != 1 || stdout.Len() != 0 || !strings.Contains(stderr.String(), "cancelled") {
		t.Fatalf("exit=%d stdout=%s stderr=%s", exit, stdout.String(), stderr.String())
	}
	if _, err := os.Stat(filepath.Join(home, ".holler", "connectors", "claude.json")); !os.IsNotExist(err) {
		t.Fatalf("cancelled setup wrote connector config: %v", err)
	}
}

func TestCLIProductSetupRecoversFromInvalidConnectorSelection(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("CLAUDE_CONFIG_DIR", filepath.Join(home, ".claude"))
	selection := filepath.Join(home, ".holler", "connectors", "claude.json")
	if err := os.MkdirAll(filepath.Dir(selection), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(selection, []byte(`{"schema_version":99,"future":true}`), 0o600); err != nil {
		t.Fatal(err)
	}
	marketplace := filepath.Join("..", "..", "connectors", "marketplace")
	var stdout, stderr bytes.Buffer
	exit := run(context.Background(), []string{
		"setup", "claude", "--dry-run", "--marketplace", marketplace,
		"--client-binary", "/usr/bin/true", "--daemon-binary", "/usr/bin/true",
	}, strings.NewReader(""), &stdout, &stderr)
	if exit != 0 || !strings.Contains(stderr.String(), "will be replaced only after confirmation") {
		t.Fatalf("exit=%d stdout=%s stderr=%s", exit, stdout.String(), stderr.String())
	}
	if raw, err := os.ReadFile(selection); err != nil || string(raw) != `{"schema_version":99,"future":true}` {
		t.Fatalf("dry run changed invalid selection: %q err=%v", raw, err)
	}
}

func TestCLIProductRemovalDryRunPreservesSharedDaemonWhenPeerConfigured(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("CLAUDE_CONFIG_DIR", filepath.Join(home, ".claude"))
	peerSelection := filepath.Join(home, ".holler", "connectors", "codex.json")
	if err := os.MkdirAll(filepath.Dir(peerSelection), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(peerSelection, []byte(`{}`), 0o600); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	exit := run(context.Background(), []string{
		"setup", "claude", "--remove", "--dry-run",
		"--client-binary", "/usr/bin/true", "--daemon-binary", "/usr/bin/true",
	}, strings.NewReader(""), &stdout, &stderr)
	if exit != 0 {
		t.Fatalf("exit=%d stdout=%s stderr=%s", exit, stdout.String(), stderr.String())
	}
	var result productSetupResult
	decode(t, stdout.Bytes(), &result)
	actions := strings.Join(result.Daemon.Actions, "\n")
	if !strings.Contains(actions, "preserve the shared Holler daemon") || strings.Contains(actions, "stop the per-user") {
		t.Fatalf("daemon plan=%+v", result.Daemon)
	}
}

func TestCLIProductSetupRequiresYesWhenInputIsEOF(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("CLAUDE_CONFIG_DIR", filepath.Join(home, ".claude"))
	marketplace := filepath.Join("..", "..", "connectors", "marketplace")
	var stdout, stderr bytes.Buffer
	exit := run(context.Background(), []string{
		"setup", "claude", "--marketplace", marketplace,
		"--client-binary", "/usr/bin/true", "--daemon-binary", "/usr/bin/true",
	}, strings.NewReader(""), &stdout, &stderr)
	if exit != 1 || !strings.Contains(stderr.String(), "pass --yes") {
		t.Fatalf("exit=%d stdout=%s stderr=%s", exit, stdout.String(), stderr.String())
	}
}

func invoke(t *testing.T, ctx context.Context, args ...string) []byte {
	t.Helper()
	var stdout, stderr bytes.Buffer
	if exit := run(ctx, args, bytes.NewReader(nil), &stdout, &stderr); exit != 0 {
		t.Fatalf("run(%v) exit=%d stderr=%s", args, exit, stderr.String())
	}
	return stdout.Bytes()
}

func decode(t *testing.T, raw []byte, target interface{}) {
	t.Helper()
	if err := json.Unmarshal(raw, target); err != nil {
		t.Fatalf("decode %s: %v", raw, err)
	}
}

func startPipeCapture(t *testing.T) (*os.File, func() string) {
	t.Helper()
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	var captured bytes.Buffer
	done := make(chan error, 1)
	go func() {
		_, copyErr := io.Copy(&captured, reader)
		done <- copyErr
	}()
	return writer, func() string {
		t.Helper()
		if err := <-done; err != nil {
			t.Errorf("capture stderr: %v", err)
		}
		_ = reader.Close()
		return captured.String()
	}
}

func startAPIServer(t *testing.T) string {
	return startAPIServerWithOptions(t)
}

func startAPIServerWithOptions(t *testing.T, options ...api.ServerOption) string {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	db, err := store.Open(ctx, filepath.Join(t.TempDir(), "holler.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	socketDirectory, err := os.MkdirTemp("/tmp", "holler-main-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(socketDirectory) })
	listener, err := net.Listen("unix", filepath.Join(socketDirectory, "holler.sock"))
	if err != nil {
		t.Fatal(err)
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
	return listener.Addr().String()
}
