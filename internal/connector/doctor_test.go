package connector_test

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/72olabs/holler/internal/api"
	"github.com/72olabs/holler/internal/connector"
)

func TestDoctorProvesAuthorizationButNeverClaimsReadiness(t *testing.T) {
	repo := repositoryRoot(t)
	socket := startDoctorAPI(t)
	for _, harness := range []string{"codex", "claude"} {
		t.Run(harness, func(t *testing.T) {
			report, err := connector.Doctor(context.Background(), doctorConfig(repo, socket, harness),
				connector.WithDoctorLookPath(func(name string) (string, error) { return "/test/" + name, nil }),
				connector.WithDoctorCommandRunner(healthyDoctorRunner(repo, harness)),
			)
			if err != nil {
				t.Fatal(err)
			}
			if report.State != connector.StateConfigured || report.Ready {
				t.Fatalf("state=%s ready=%v checks=%+v", report.State, report.Ready, report.Checks)
			}
			if !hasCheck(report, "authorization.runtime_trust", connector.CheckWarn) {
				t.Fatalf("doctor omitted explicit runtime hook-trust uncertainty: %+v", report.Checks)
			}
			if !hasCheck(report, "daemon.protocol", connector.CheckPass) {
				t.Fatalf("doctor omitted API protocol evidence: %+v", report.Checks)
			}
		})
	}
}

func TestDoctorClassifiesFailuresDeterministically(t *testing.T) {
	repo := repositoryRoot(t)
	socket := startDoctorAPI(t)
	tests := []struct {
		name      string
		mutate    func(*connector.DoctorConfig)
		runner    connector.CommandRunner
		wantState connector.ReadinessState
		wantCheck string
	}{
		{
			name: "old client", runner: versionedDoctorRunner(repo, "codex", "0.148.0", true),
			wantState: connector.StateIncompatible, wantCheck: "client.version",
		},
		{
			name:      "wrong project root",
			mutate:    func(config *connector.DoctorConfig) { config.ProjectRoot = filepath.Join(repo, "internal") },
			runner:    versionedDoctorRunner(repo, "codex", "0.149.1", true),
			wantState: connector.StateDiscovered, wantCheck: "project.repository",
		},
		{
			name:      "missing authority",
			mutate:    func(config *connector.DoctorConfig) { config.PolicyPath = filepath.Join(t.TempDir(), "missing.toml") },
			runner:    versionedDoctorRunner(repo, "codex", "0.149.1", true),
			wantState: connector.StateAuthorizationRequired, wantCheck: "authorization.tool_policy",
		},
		{
			name:      "daemon unavailable",
			mutate:    func(config *connector.DoctorConfig) { config.SocketPath = filepath.Join(t.TempDir(), "missing.sock") },
			runner:    versionedDoctorRunner(repo, "codex", "0.149.1", true),
			wantState: connector.StateDegraded, wantCheck: "daemon.reachable",
		},
		{
			name:      "wake adapter unavailable",
			mutate:    func(config *connector.DoctorConfig) { config.Profile = "live-review" },
			runner:    versionedDoctorRunner(repo, "codex", "0.149.1", false),
			wantState: connector.StateDegraded, wantCheck: "notification.adapter",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := doctorConfig(repo, socket, "codex")
			if test.mutate != nil {
				test.mutate(&config)
			}
			report, err := connector.Doctor(context.Background(), config,
				connector.WithDoctorLookPath(func(name string) (string, error) { return "/test/" + name, nil }),
				connector.WithDoctorCommandRunner(test.runner),
			)
			if err != nil {
				t.Fatal(err)
			}
			if report.State != test.wantState || !hasCheck(report, test.wantCheck, connector.CheckFail) {
				t.Fatalf("state=%s, want=%s; checks=%+v", report.State, test.wantState, report.Checks)
			}
		})
	}
}

func TestPackageHashRejectsSymlinkedAssets(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(t.TempDir(), "outside")
	if err := os.WriteFile(target, []byte("outside package"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, filepath.Join(root, "asset")); err != nil {
		t.Fatal(err)
	}
	if _, err := connector.PackageHash(root, []string{"asset"}); err == nil || !strings.Contains(err.Error(), "not a regular file") {
		t.Fatalf("PackageHash symlink error = %v", err)
	}
}

func TestDoctorRejectsClaudeServerLevelDeny(t *testing.T) {
	repo := repositoryRoot(t)
	socket := startDoctorAPI(t)
	policy := filepath.Join(t.TempDir(), "claude-deny.json")
	raw := `{
  "permissions": {
    "allow": [
      "mcp__plugin_holler_holler__bus_send",
      "mcp__plugin_holler_holler__bus_check_inbox",
      "mcp__plugin_holler_holler__bus_claim",
      "mcp__plugin_holler_holler__bus_inbox",
      "mcp__plugin_holler_holler__bus_ack",
      "mcp__plugin_holler_holler__bus_extend",
      "mcp__plugin_holler_holler__bus_nack",
      "mcp__plugin_holler_holler__bus_status"
    ],
    "deny": ["mcp__plugin_holler_holler"]
  }
}`
	if err := os.WriteFile(policy, []byte(raw), 0o600); err != nil {
		t.Fatal(err)
	}
	config := doctorConfig(repo, socket, "claude")
	config.PolicyPath = policy
	report, err := connector.Doctor(context.Background(), config,
		connector.WithDoctorLookPath(func(name string) (string, error) { return "/test/" + name, nil }),
		connector.WithDoctorCommandRunner(healthyDoctorRunner(repo, "claude")),
	)
	if err != nil {
		t.Fatal(err)
	}
	if report.State != connector.StateAuthorizationRequired || !hasCheck(report, "authorization.tool_policy", connector.CheckFail) {
		t.Fatalf("server deny was not detected: state=%s checks=%+v", report.State, report.Checks)
	}
}

func TestDoctorRequiresExplicitClaudeAdoptionApproval(t *testing.T) {
	repo := repositoryRoot(t)
	socket := startDoctorAPI(t)
	policy := filepath.Join(t.TempDir(), "claude-overbroad.json")
	raw, err := os.ReadFile(filepath.Join(repo, "connectors", "policies", "claude-live-review.json"))
	if err != nil {
		t.Fatal(err)
	}
	var settings map[string]interface{}
	if err := json.Unmarshal(raw, &settings); err != nil {
		t.Fatal(err)
	}
	permissions := settings["permissions"].(map[string]interface{})
	asked := permissions["ask"].([]interface{})
	adopt := asked[0]
	permissions["ask"] = []interface{}{}
	permissions["allow"] = append(permissions["allow"].([]interface{}), adopt)
	raw, err = json.Marshal(settings)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(policy, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	config := doctorConfig(repo, socket, "claude")
	config.PolicyPath = policy
	report, err := connector.Doctor(context.Background(), config,
		connector.WithDoctorLookPath(func(name string) (string, error) { return "/test/" + name, nil }),
		connector.WithDoctorCommandRunner(healthyDoctorRunner(repo, "claude")),
	)
	if err != nil {
		t.Fatal(err)
	}
	if report.State != connector.StateAuthorizationRequired || !hasCheck(report, "authorization.tool_policy", connector.CheckFail) {
		t.Fatalf("overbroad adoption permission was accepted: state=%s checks=%+v", report.State, report.Checks)
	}
}

func TestDoctorEvaluatesSelectedClaudeAttentionMode(t *testing.T) {
	repo := repositoryRoot(t)
	socket := startDoctorAPI(t)
	for _, test := range []struct {
		mode      string
		wantState connector.ReadinessState
		wantID    string
		want      connector.CheckStatus
	}{
		{connector.AttentionHookLongPoll, connector.StateConfigured, "notification.adapter", connector.CheckPass},
		{connector.AttentionStartupOnly, connector.StateDegraded, "notification.adapter", connector.CheckFail},
	} {
		t.Run(test.mode, func(t *testing.T) {
			config := doctorConfig(repo, socket, "claude")
			config.Profile = "live-review"
			config.AttentionMode = test.mode
			report, err := connector.Doctor(context.Background(), config,
				connector.WithDoctorLookPath(func(name string) (string, error) { return "/test/" + name, nil }),
				connector.WithDoctorCommandRunner(healthyDoctorRunner(repo, "claude")),
			)
			if err != nil {
				t.Fatal(err)
			}
			if report.State != test.wantState || !hasCheck(report, test.wantID, test.want) {
				t.Fatalf("state=%s checks=%+v", report.State, report.Checks)
			}
		})
	}
}

func TestDoctorValidatesInstalledOpenCodeProfileAndNativePrompt(t *testing.T) {
	repo := repositoryRoot(t)
	socket := startDoctorAPI(t)
	directory := t.TempDir()
	installRoot := filepath.Join(directory, "opencode", "holler")
	profile := filepath.Join(installRoot, "opencode.json")
	selection := filepath.Join(directory, "connectors", "opencode.json")
	_, err := connector.SetupOpenCode(context.Background(), connector.OpenCodeSetupConfig{
		AttentionMode: connector.AttentionNativePrompt, Actor: "opencode-live", Peer: "codex-live",
		Project: "holler", ProjectRoot: repo,
		PackageSource: filepath.Join(repo, "connectors", "marketplace", "plugins", "opencode-holler"),
		PackageRoot:   installRoot, ProfilePath: profile, ConnectorConfig: selection,
		HollerBinary: "/test/holler", Apply: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	report, err := connector.Doctor(context.Background(), connector.DoctorConfig{
		Harness: "opencode", Profile: "live-review", ProjectRoot: repo, PluginRoot: installRoot,
		PolicyPath: profile, SocketPath: socket, Actor: "opencode-live", RunID: "opencode-doctor",
		AttentionMode: connector.AttentionNativePrompt,
	},
		connector.WithDoctorLookPath(func(name string) (string, error) { return "/test/" + name, nil }),
		connector.WithDoctorCommandRunner(versionedDoctorRunner(repo, "opencode", "1.18.4", true)),
	)
	if err != nil {
		t.Fatal(err)
	}
	if report.State != connector.StateConfigured || report.Ready ||
		!hasCheck(report, "authorization.tool_policy", connector.CheckPass) ||
		!hasCheck(report, "notification.adapter", connector.CheckPass) {
		t.Fatalf("state=%s ready=%v checks=%+v", report.State, report.Ready, report.Checks)
	}
	raw, err := os.ReadFile(profile)
	if err != nil {
		t.Fatal(err)
	}
	var settings map[string]interface{}
	if err := json.Unmarshal(raw, &settings); err != nil {
		t.Fatal(err)
	}
	settings["permission"].(map[string]interface{})["holler_holler_adopt"] = "allow"
	raw, err = json.Marshal(settings)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(profile, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	report, err = connector.Doctor(context.Background(), connector.DoctorConfig{
		Harness: "opencode", Profile: "live-review", ProjectRoot: repo, PluginRoot: installRoot,
		PolicyPath: profile, SocketPath: socket, Actor: "opencode-live", RunID: "opencode-doctor",
		AttentionMode: connector.AttentionNativePrompt,
	},
		connector.WithDoctorLookPath(func(name string) (string, error) { return "/test/" + name, nil }),
		connector.WithDoctorCommandRunner(versionedDoctorRunner(repo, "opencode", "1.18.4", true)),
	)
	if err != nil {
		t.Fatal(err)
	}
	if report.State != connector.StateAuthorizationRequired || !hasCheck(report, "authorization.tool_policy", connector.CheckFail) {
		t.Fatalf("overbroad OpenCode adoption permission was accepted: state=%s checks=%+v", report.State, report.Checks)
	}
}

func doctorConfig(repo, socket, harness string) connector.DoctorConfig {
	plugin := filepath.Join(repo, "connectors", "marketplace", "plugins", "holler")
	policy := filepath.Join(repo, "connectors", "policies", "codex-live-review.toml")
	if harness == "claude" {
		plugin = filepath.Join(repo, "connectors", "marketplace", "plugins", "claude-holler")
		policy = filepath.Join(repo, "connectors", "policies", "claude-live-review.json")
	}
	return connector.DoctorConfig{
		Harness: harness, Profile: "async-peer", ProjectRoot: repo, PluginRoot: plugin,
		PolicyPath: policy, SocketPath: socket, Actor: harness, RunID: harness + "-doctor",
	}
}

func healthyDoctorRunner(repo, harness string) connector.CommandRunner {
	version := "0.149.1"
	if harness == "claude" {
		version = "2.1.247"
	}
	return versionedDoctorRunner(repo, harness, version, true)
}

func versionedDoctorRunner(repo, harness, version string, queueAvailable bool) connector.CommandRunner {
	return func(_ context.Context, name string, args ...string) (string, string, int, error) {
		joined := strings.Join(args, " ")
		switch {
		case strings.HasSuffix(name, harness) && joined == "--version":
			return harness + " " + version, "", 0, nil
		case name == "git" && strings.Contains(joined, "rev-parse --show-toplevel"):
			if strings.Contains(joined, filepath.Join(repo, "internal")) {
				return repo + "\n", "", 0, nil
			}
			return repo + "\n", "", 0, nil
		case strings.HasSuffix(name, "codex") && joined == "queue --help" && queueAvailable:
			return "queue help", "", 0, nil
		default:
			return "", "unsupported test command", 1, nil
		}
	}
}

func hasCheck(report connector.DoctorReport, id string, status connector.CheckStatus) bool {
	for _, check := range report.Checks {
		if check.ID == id && check.Status == status {
			return true
		}
	}
	return false
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	return root
}

func startDoctorAPI(t *testing.T) string {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	db := openStore(t)
	directory, err := os.MkdirTemp("/tmp", "ab-doc-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	listener, err := net.Listen("unix", filepath.Join(directory, "holler.sock"))
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- api.NewServer(db).Serve(ctx, listener) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("serve: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Error("API server did not stop")
		}
	})
	return listener.Addr().String()
}
