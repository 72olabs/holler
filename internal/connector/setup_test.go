package connector_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/72olabs/holler/internal/buildinfo"
	"github.com/72olabs/holler/internal/connector"
	"github.com/pelletier/go-toml/v2"
)

func TestClaudeSetupDryRunMakesNoChanges(t *testing.T) {
	directory := t.TempDir()
	settings := filepath.Join(directory, "settings.json")
	configPath := filepath.Join(directory, "claude.json")
	called := false
	plan, err := connector.SetupClaude(context.Background(), connector.ClaudeSetupConfig{
		AttentionMode: connector.AttentionHookLongPoll, Actor: "claude", Marketplace: directory,
		ClaudeSettings: settings, ConnectorConfig: configPath, HollerBinary: "/usr/bin/true",
		RuntimeBinaryPath: filepath.Join(directory, "bin-path"),
	}, connector.WithSetupCommandRunner(func(context.Context, string, ...string) (string, string, int, error) {
		called = true
		return "", "", 0, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	if plan.Applied || called {
		t.Fatalf("plan=%+v called=%v", plan, called)
	}
	if _, err := os.Stat(settings); !os.IsNotExist(err) {
		t.Fatalf("dry run wrote settings: %v", err)
	}
}

func TestClaudeSetupApplyPreservesSettingsAndWritesSelection(t *testing.T) {
	directory := t.TempDir()
	settings := filepath.Join(directory, "settings.json")
	configPath := filepath.Join(directory, "claude.json")
	if err := os.WriteFile(settings, []byte(`{"theme":"dark","permissions":{"allow":["Read"]}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	var commands []string
	runner := func(_ context.Context, name string, args ...string) (string, string, int, error) {
		commands = append(commands, name+" "+strings.Join(args, " "))
		if strings.Join(args, " ") == "plugin list --json" || strings.Join(args, " ") == "plugin marketplace list --json" {
			return `[]`, "", 0, nil
		}
		return "", "", 0, nil
	}
	plan, err := connector.SetupClaude(context.Background(), connector.ClaudeSetupConfig{
		AttentionMode: connector.AttentionHookLongPoll,
		Actor:         "claude-review", Peer: "codex", Project: "holler", Channel: "direct",
		Marketplace: directory, ClaudeSettings: settings, ConnectorConfig: configPath, Apply: true,
		HollerBinary: "/usr/bin/true", RuntimeBinaryPath: filepath.Join(directory, "bin-path"),
	}, connector.WithSetupCommandRunner(runner))
	if err != nil {
		t.Fatal(err)
	}
	if !plan.Applied || len(plan.Backups) != 1 || len(commands) != 4 {
		t.Fatalf("plan=%+v commands=%+v", plan, commands)
	}
	raw, err := os.ReadFile(settings)
	if err != nil {
		t.Fatal(err)
	}
	var merged map[string]interface{}
	if err := json.Unmarshal(raw, &merged); err != nil {
		t.Fatal(err)
	}
	if merged["theme"] != "dark" || !strings.Contains(string(raw), "bus_inbox") || !strings.Contains(string(raw), "hook-long-poll") {
		t.Fatalf("settings = %s", raw)
	}
	loaded, err := connector.LoadClaudeConnectorConfig(configPath)
	if err != nil || loaded.AttentionMode != connector.AttentionHookLongPoll || loaded.Actor != "claude-review" {
		t.Fatalf("config=%+v err=%v", loaded, err)
	}
}

func TestConfigWritesPreserveSymlinkModeAndFirstBackup(t *testing.T) {
	directory := t.TempDir()
	target := filepath.Join(directory, "managed", "settings.json")
	if err := os.MkdirAll(filepath.Dir(target), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(target, []byte(`{"theme":"dark"}`), 0o640); err != nil {
		t.Fatal(err)
	}
	intermediate := filepath.Join(directory, "managed-settings.json")
	if err := os.Symlink(filepath.Join("managed", "settings.json"), intermediate); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(directory, "settings.json")
	if err := os.Symlink(filepath.Base(intermediate), link); err != nil {
		t.Fatal(err)
	}
	config := connector.ClaudeSetupConfig{
		Actor: "claude", ClaudeSettings: link, ConnectorConfig: filepath.Join(directory, "claude.json"),
		HollerBinary: "/usr/bin/true", RuntimeBinaryPath: filepath.Join(directory, "bin-path"), Apply: true,
	}
	runner := connector.WithSetupCommandRunner(func(_ context.Context, _ string, args ...string) (string, string, int, error) {
		if strings.Join(args, " ") == "plugin list --json" {
			return `[]`, "", 0, nil
		}
		return "", "", 0, nil
	})
	if _, err := connector.SetupClaude(context.Background(), config, runner); err != nil {
		t.Fatal(err)
	}
	if info, err := os.Lstat(link); err != nil {
		t.Fatalf("settings symlink missing: %v", err)
	} else if info.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("settings symlink was replaced: mode=%v", info.Mode())
	}
	if info, err := os.Lstat(intermediate); err != nil {
		t.Fatalf("intermediate settings symlink missing: %v", err)
	} else if info.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("intermediate settings symlink was replaced: mode=%v", info.Mode())
	}
	if info, err := os.Stat(target); err != nil {
		t.Fatalf("settings target missing: %v", err)
	} else if info.Mode().Perm() != 0o640 {
		t.Fatalf("target mode=%v", info.Mode())
	}
	backup, err := os.ReadFile(link + ".bak")
	if err != nil || string(backup) != `{"theme":"dark"}` {
		t.Fatalf("backup=%q err=%v", backup, err)
	}
	if err := os.WriteFile(target, []byte(`{"theme":"user-change"}`), 0o640); err != nil {
		t.Fatal(err)
	}
	if _, err := connector.SetupClaude(context.Background(), config, runner); err != nil {
		t.Fatal(err)
	}
	backup, err = os.ReadFile(link + ".bak")
	if err != nil || string(backup) != `{"theme":"dark"}` {
		t.Fatalf("first backup was overwritten: %q err=%v", backup, err)
	}
}

func TestClaudeRemovalUninstallsManagedStateAndPreservesOtherSettings(t *testing.T) {
	directory := t.TempDir()
	settings := filepath.Join(directory, "settings.json")
	selection := filepath.Join(directory, "claude.json")
	manifest, _ := connector.Manifest("claude")
	managedTool := manifest.ClaudeToolPrefix + manifest.Tools[0].Name
	raw := `{"theme":"dark","permissions":{"allow":["Read",` + strconv.Quote(managedTool) + `]},"pluginConfigs":{` +
		strconv.Quote(connector.DefaultClaudePluginID) + `:{"options":{"actor":"claude"}}}}`
	if err := os.WriteFile(settings, []byte(raw), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(selection, []byte(`{}`), 0o600); err != nil {
		t.Fatal(err)
	}
	var commands []string
	runner := connector.WithSetupCommandRunner(func(_ context.Context, command string, args ...string) (string, string, int, error) {
		joined := strings.Join(args, " ")
		commands = append(commands, command+" "+joined)
		switch joined {
		case "plugin list --json":
			return `[{"id":"` + connector.DefaultClaudePluginID + `","enabled":true}]`, "", 0, nil
		case "plugin marketplace list --json":
			return `[{"name":"holler"}]`, "", 0, nil
		default:
			return "", "", 0, nil
		}
	})
	plan, err := connector.RemoveClaude(context.Background(), connector.ClaudeSetupConfig{
		PluginID: connector.DefaultClaudePluginID, ClaudeSettings: settings, ConnectorConfig: selection,
		ClaudeBinary: "/usr/bin/true", Apply: true,
	}, runner)
	if err != nil {
		t.Fatal(err)
	}
	if !plan.Applied || len(commands) != 4 {
		t.Fatalf("plan=%+v commands=%v", plan, commands)
	}
	updated, err := os.ReadFile(settings)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(updated), `"Read"`) || strings.Contains(string(updated), managedTool) || strings.Contains(string(updated), "pluginConfigs") {
		t.Fatalf("settings after removal=%s", updated)
	}
	if _, err := os.Stat(selection); !os.IsNotExist(err) {
		t.Fatalf("connector selection remained: %v", err)
	}
}

func TestCodexRemovalStripsManagedPolicyAndOwnedFiles(t *testing.T) {
	directory := t.TempDir()
	userConfig := filepath.Join(directory, "config.toml")
	policy := filepath.Join(directory, "holler.config.toml")
	selection := filepath.Join(directory, "codex.json")
	managed := "model = \"gpt-5.6-sol\"\n\n# BEGIN holler managed Codex MCP policy\n[plugins.test]\nenabled = true\n# END holler managed Codex MCP policy\n"
	for path, value := range map[string]string{userConfig: managed, policy: "generated", selection: `{}`} {
		if err := os.WriteFile(path, []byte(value), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	runner := connector.WithSetupCommandRunner(func(_ context.Context, _ string, args ...string) (string, string, int, error) {
		if strings.Join(args, " ") == "plugin marketplace list --json" {
			return `{"marketplaces":[{"name":"holler"}]}`, "", 0, nil
		}
		return "", "", 0, nil
	})
	_, err := connector.RemoveCodex(context.Background(), connector.CodexSetupConfig{
		PluginID: connector.DefaultCodexPluginID, Profile: "holler", PolicyPath: policy,
		UserConfigPath: userConfig, GlobalPolicy: true, ConnectorConfig: selection,
		CodexBinary: "/usr/bin/true", Apply: true,
	}, runner)
	if err != nil {
		t.Fatal(err)
	}
	updated, err := os.ReadFile(userConfig)
	if err != nil || string(updated) != "model = \"gpt-5.6-sol\"\n" {
		t.Fatalf("user config=%q err=%v", updated, err)
	}
	for _, path := range []string{policy, selection} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("generated file remained at %s: %v", path, err)
		}
	}
}

func TestHarnessRemovalCleansLocalStateWhenClientBinaryIsGone(t *testing.T) {
	t.Run("claude", func(t *testing.T) {
		directory := t.TempDir()
		settings := filepath.Join(directory, "settings.json")
		selection := filepath.Join(directory, "claude.json")
		manifest, _ := connector.Manifest("claude")
		raw := `{"permissions":{"allow":[` + strconv.Quote(manifest.ClaudeToolPrefix+manifest.Tools[0].Name) + `]},"pluginConfigs":{` +
			strconv.Quote(connector.DefaultClaudePluginID) + `:{}}}`
		for path, value := range map[string]string{settings: raw, selection: `{}`} {
			if err := os.WriteFile(path, []byte(value), 0o600); err != nil {
				t.Fatal(err)
			}
		}
		called := false
		_, err := connector.RemoveClaude(context.Background(), connector.ClaudeSetupConfig{
			PluginID: connector.DefaultClaudePluginID, ClaudeSettings: settings, ConnectorConfig: selection,
			ClaudeBinary: "missing-claude", Apply: true,
		}, connector.WithSetupLookPath(func(string) (string, error) { return "", os.ErrNotExist }),
			connector.WithSetupCommandRunner(func(context.Context, string, ...string) (string, string, int, error) {
				called = true
				return "", "", 0, nil
			}))
		if err != nil || called {
			t.Fatalf("err=%v called=%v", err, called)
		}
		updated, err := os.ReadFile(settings)
		if err != nil || strings.TrimSpace(string(updated)) != "{}" {
			t.Fatalf("settings=%q err=%v", updated, err)
		}
		if _, err := os.Stat(selection); !os.IsNotExist(err) {
			t.Fatalf("connector selection remained: %v", err)
		}
	})

	t.Run("codex", func(t *testing.T) {
		directory := t.TempDir()
		userConfig := filepath.Join(directory, "config.toml")
		policy := filepath.Join(directory, "holler.config.toml")
		selection := filepath.Join(directory, "codex.json")
		managed := "model = \"gpt-5.6-sol\"\n\n# BEGIN holler managed Codex MCP policy\n[plugins.test]\nenabled = true\n# END holler managed Codex MCP policy\n"
		for path, value := range map[string]string{userConfig: managed, policy: "generated", selection: `{}`} {
			if err := os.WriteFile(path, []byte(value), 0o600); err != nil {
				t.Fatal(err)
			}
		}
		called := false
		_, err := connector.RemoveCodex(context.Background(), connector.CodexSetupConfig{
			PluginID: connector.DefaultCodexPluginID, Profile: "holler", PolicyPath: policy,
			UserConfigPath: userConfig, GlobalPolicy: true, ConnectorConfig: selection,
			CodexBinary: "missing-codex", Apply: true,
		}, connector.WithSetupLookPath(func(string) (string, error) { return "", os.ErrNotExist }),
			connector.WithSetupCommandRunner(func(context.Context, string, ...string) (string, string, int, error) {
				called = true
				return "", "", 0, nil
			}))
		if err != nil || called {
			t.Fatalf("err=%v called=%v", err, called)
		}
		updated, err := os.ReadFile(userConfig)
		if err != nil || string(updated) != "model = \"gpt-5.6-sol\"\n" {
			t.Fatalf("user config=%q err=%v", updated, err)
		}
		for _, path := range []string{policy, selection} {
			if _, err := os.Stat(path); !os.IsNotExist(err) {
				t.Fatalf("generated file remained at %s: %v", path, err)
			}
		}
	})
}

func TestClaudeSetupRepairsDisabledInstalledPlugin(t *testing.T) {
	directory := t.TempDir()
	var commands []string
	runner := func(_ context.Context, command string, args ...string) (string, string, int, error) {
		joined := strings.Join(args, " ")
		commands = append(commands, command+" "+joined)
		switch joined {
		case "plugin marketplace list --json":
			return `[{"path":` + strconv.Quote(directory) + `}]`, "", 0, nil
		case "plugin list --json":
			return `[{"id":"` + connector.DefaultClaudePluginID + `","enabled":false}]`, "", 0, nil
		default:
			return "", "", 0, nil
		}
	}
	_, err := connector.SetupClaude(context.Background(), connector.ClaudeSetupConfig{
		Actor: "claude", Marketplace: directory, ClaudeSettings: filepath.Join(directory, "settings.json"),
		ConnectorConfig: filepath.Join(directory, "claude.json"), HollerBinary: "/usr/bin/true",
		RuntimeBinaryPath: filepath.Join(directory, "bin-path"), Apply: true,
	}, connector.WithSetupCommandRunner(runner))
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(commands, "\n")
	if !strings.Contains(joined, "plugin update "+connector.DefaultClaudePluginID+" --scope user --yes") ||
		!strings.Contains(joined, "plugin enable "+connector.DefaultClaudePluginID+" --scope user") ||
		strings.Contains(joined, "marketplace add") {
		t.Fatalf("commands=%s", joined)
	}
}

func TestClaudeSetupReplacesMarketplaceSourceAfterPackageUpgrade(t *testing.T) {
	directory := t.TempDir()
	newSource := filepath.Join(directory, "new-marketplace")
	var commands []string
	runner := func(_ context.Context, command string, args ...string) (string, string, int, error) {
		joined := strings.Join(args, " ")
		commands = append(commands, command+" "+joined)
		switch joined {
		case "plugin marketplace list --json":
			return `[{"name":"holler","path":"/old/cellar/holler"}]`, "", 0, nil
		case "plugin list --json":
			return `[{"id":"` + connector.DefaultClaudePluginID + `","enabled":true}]`, "", 0, nil
		default:
			return "", "", 0, nil
		}
	}
	_, err := connector.SetupClaude(context.Background(), connector.ClaudeSetupConfig{
		Actor: "claude", Marketplace: newSource, ClaudeSettings: filepath.Join(directory, "settings.json"),
		ConnectorConfig: filepath.Join(directory, "claude.json"), HollerBinary: "/usr/bin/true",
		RuntimeBinaryPath: filepath.Join(directory, "bin-path"), Apply: true,
	}, connector.WithSetupCommandRunner(runner))
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(commands, "\n")
	remove := "plugin marketplace remove holler --scope user"
	add := "plugin marketplace add " + newSource + " --scope user"
	if !strings.Contains(joined, remove) || !strings.Contains(joined, add) || strings.Index(joined, remove) > strings.Index(joined, add) {
		t.Fatalf("commands=%s", joined)
	}
}

func TestCodexSetupDryRunAndApplyWriteDedicatedProfile(t *testing.T) {
	directory := t.TempDir()
	policy := filepath.Join(directory, "holler.config.toml")
	configPath := filepath.Join(directory, "codex.json")
	called := false
	base := connector.CodexSetupConfig{
		AttentionMode: connector.AttentionNativeQueue, Actor: "codex-live", Peer: "claude-review",
		Project: "holler", ProjectRoot: directory, Profile: "holler", PolicyPath: policy,
		ConnectorConfig: configPath, HollerBinary: "/usr/bin/true", CodexBinary: "/usr/bin/true",
		RuntimeBinaryPath: filepath.Join(directory, "bin-path"),
	}
	plan, err := connector.SetupCodex(context.Background(), base, connector.WithSetupCommandRunner(
		func(context.Context, string, ...string) (string, string, int, error) {
			called = true
			return "", "", 0, nil
		}))
	if err != nil || plan.Applied || called {
		t.Fatalf("plan=%+v called=%v err=%v", plan, called, err)
	}
	if _, err := os.Stat(policy); !os.IsNotExist(err) {
		t.Fatalf("dry run wrote policy: %v", err)
	}

	base.Apply = true
	var commands []string
	plan, err = connector.SetupCodex(context.Background(), base, connector.WithSetupCommandRunner(
		func(_ context.Context, command string, args ...string) (string, string, int, error) {
			commands = append(commands, command+" "+strings.Join(args, " "))
			return "", "", 0, nil
		}))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(policy)
	if err != nil {
		t.Fatal(err)
	}
	if len(commands) != 1 || commands[0] != "/usr/bin/true plugin add "+connector.DefaultCodexPluginID ||
		!strings.Contains(string(raw), "default_tools_approval_mode = \"approve\"") ||
		!strings.Contains(string(raw), "bus_inbox") {
		t.Fatalf("commands=%v policy=%s", commands, raw)
	}
	var parsed map[string]interface{}
	if err := toml.Unmarshal(raw, &parsed); err != nil {
		t.Fatalf("generated policy is invalid TOML: %v", err)
	}
	loaded, err := connector.LoadCodexConnectorConfig(configPath)
	if err != nil || loaded.Actor != "codex-live" || loaded.Profile != "holler" || loaded.ProjectRoot != directory {
		t.Fatalf("config=%+v err=%v", loaded, err)
	}
}

func TestCodexProductSetupMergesUserPolicyWithoutRewritingExistingConfig(t *testing.T) {
	directory := t.TempDir()
	policy := filepath.Join(directory, "holler.config.toml")
	userConfig := filepath.Join(directory, "config.toml")
	configPath := filepath.Join(directory, "codex.json")
	original := "# user's model choice\nmodel = \"gpt-5.6-sol\" # keep this comment\n"
	if err := os.WriteFile(userConfig, []byte(original), 0o600); err != nil {
		t.Fatal(err)
	}
	base := connector.CodexSetupConfig{
		AttentionMode: connector.AttentionNativeQueue, Actor: "codex", Peer: "claude",
		Project: "default", ProjectRoot: directory, Profile: "holler", PolicyPath: policy,
		UserConfigPath: userConfig, GlobalPolicy: true, ConnectorConfig: configPath,
		HollerBinary: "/usr/bin/true", CodexBinary: "/usr/bin/true", Apply: true,
		RuntimeBinaryPath: filepath.Join(directory, "bin-path"),
	}
	runner := connector.WithSetupCommandRunner(func(context.Context, string, ...string) (string, string, int, error) {
		return "", "", 0, nil
	})
	plan, err := connector.SetupCodex(context.Background(), base, runner)
	if err != nil {
		t.Fatal(err)
	}
	if plan.UserConfigPath != userConfig || len(plan.Backups) != 1 || plan.Backups[0] != userConfig+".bak" {
		t.Fatalf("plan=%+v", plan)
	}
	raw, err := os.ReadFile(userConfig)
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	if !strings.Contains(text, original) || !strings.Contains(text, "BEGIN holler managed Codex MCP policy") ||
		!strings.Contains(text, "bus_inbox") || !strings.Contains(text, "approval_mode = \"approve\"") {
		t.Fatalf("user config=%s", raw)
	}
	if _, err := connector.SetupCodex(context.Background(), base, runner); err != nil {
		t.Fatal(err)
	}
	second, err := os.ReadFile(userConfig)
	if err != nil {
		t.Fatal(err)
	}
	if string(second) != text {
		t.Fatalf("idempotent setup changed config\nfirst=%s\nsecond=%s", text, second)
	}
}

func TestCodexProductSetupRejectsConflictingUserPolicyDuringPreflight(t *testing.T) {
	directory := t.TempDir()
	userConfig := filepath.Join(directory, "config.toml")
	conflict := `[plugins."holler@holler".mcp_servers.holler]
enabled = true
default_tools_approval_mode = "prompt"
enabled_tools = ["bus_status"]
`
	if err := os.WriteFile(userConfig, []byte(conflict), 0o600); err != nil {
		t.Fatal(err)
	}
	called := false
	_, err := connector.SetupCodex(context.Background(), connector.CodexSetupConfig{
		Actor: "codex", ProjectRoot: directory, UserConfigPath: userConfig, GlobalPolicy: true,
		PolicyPath: filepath.Join(directory, "holler.config.toml"), ConnectorConfig: filepath.Join(directory, "codex.json"),
		HollerBinary: "/usr/bin/true", CodexBinary: "/usr/bin/true", Apply: true,
		RuntimeBinaryPath: filepath.Join(directory, "bin-path"),
	}, connector.WithSetupCommandRunner(func(context.Context, string, ...string) (string, string, int, error) {
		called = true
		return "", "", 0, nil
	}))
	if err == nil || !strings.Contains(err.Error(), "conflicting Holler MCP policy") || called {
		t.Fatalf("err=%v called=%v", err, called)
	}
	if _, statErr := os.Stat(filepath.Join(directory, "holler.config.toml")); !os.IsNotExist(statErr) {
		t.Fatalf("preflight wrote policy: %v", statErr)
	}
}

func TestCodexSetupReplacesMarketplaceSourceAfterPackageUpgrade(t *testing.T) {
	directory := t.TempDir()
	newSource := filepath.Join(directory, "new-marketplace")
	var commands []string
	runner := func(_ context.Context, command string, args ...string) (string, string, int, error) {
		joined := strings.Join(args, " ")
		commands = append(commands, command+" "+joined)
		if joined == "plugin marketplace list --json" {
			return `{"marketplaces":[{"name":"holler","root":"/old/cellar/holler"}]}`, "", 0, nil
		}
		return "", "", 0, nil
	}
	_, err := connector.SetupCodex(context.Background(), connector.CodexSetupConfig{
		Actor: "codex", ProjectRoot: directory, Marketplace: newSource,
		PolicyPath: filepath.Join(directory, "holler.config.toml"), ConnectorConfig: filepath.Join(directory, "codex.json"),
		HollerBinary: "/usr/bin/true", CodexBinary: "/usr/bin/true", Apply: true,
		RuntimeBinaryPath: filepath.Join(directory, "bin-path"),
	}, connector.WithSetupCommandRunner(runner))
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(commands, "\n")
	remove := "plugin marketplace remove holler"
	add := "plugin marketplace add " + newSource
	if !strings.Contains(joined, remove) || !strings.Contains(joined, add) || strings.Index(joined, remove) > strings.Index(joined, add) {
		t.Fatalf("commands=%s", joined)
	}
}

func TestDaemonServiceSetupIsIdempotentForLaunchd(t *testing.T) {
	directory := t.TempDir()
	loaded := false
	var commands []string
	runner := func(_ context.Context, command string, args ...string) (string, string, int, error) {
		joined := command + " " + strings.Join(args, " ")
		commands = append(commands, joined)
		if len(args) > 0 && args[0] == "print" {
			if loaded {
				return "state = running\npid = 123", "", 0, nil
			}
			return "", "not found", 1, nil
		}
		if len(args) > 0 && args[0] == "bootstrap" {
			loaded = true
		}
		return "", "", 0, nil
	}
	config := connector.DaemonServiceConfig{
		DaemonBinary: "/usr/bin/true", HollerBinary: "/usr/bin/true", Home: directory,
		Socket: filepath.Join(directory, ".holler", "holler.sock"), GOOS: "darwin", Apply: true,
	}
	probe := connector.WithServiceDaemonProbe(func(context.Context, string) (connector.DaemonProcessInfo, bool, error) {
		if !loaded {
			return connector.DaemonProcessInfo{}, false, nil
		}
		return connector.DaemonProcessInfo{PID: 123, BuildID: buildinfo.Current().ID()}, true, nil
	})
	plan, err := connector.SetupDaemonService(context.Background(), config, connector.WithServiceCommandRunner(runner), probe)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(plan.Path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), "/usr/bin/true") || !strings.Contains(string(raw), "<key>KeepAlive</key>\n  <true/>") ||
		strings.Contains(string(raw), "SuccessfulExit") || !strings.Contains(string(raw), "<integer>5</integer>") ||
		len(commands) != 3 || !strings.Contains(commands[1], "bootstrap") {
		t.Fatalf("plan=%+v commands=%v plist=%s", plan, commands, raw)
	}
	commands = nil
	second, err := connector.SetupDaemonService(context.Background(), config, connector.WithServiceCommandRunner(runner), probe)
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Backups) != 0 || len(commands) != 1 || !strings.Contains(commands[0], "launchctl print") {
		t.Fatalf("plan=%+v commands=%v", second, commands)
	}
}

func TestDaemonServiceSetupRetriesTransientLaunchdBootstrapFailure(t *testing.T) {
	directory := t.TempDir()
	servicePath := filepath.Join(directory, "Library", "LaunchAgents", "com.72olabs.hollerd.plist")
	if err := os.MkdirAll(filepath.Dir(servicePath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(servicePath, []byte("old service"), 0o600); err != nil {
		t.Fatal(err)
	}

	loaded := true
	bootstrapAttempts := 0
	runner := func(_ context.Context, command string, args ...string) (string, string, int, error) {
		if command != "launchctl" || len(args) == 0 {
			return "", "", 0, nil
		}
		switch args[0] {
		case "print":
			if loaded {
				return "state = running\npid = 123", "", 0, nil
			}
			return "", "not found", 1, nil
		case "bootout":
			loaded = false
			return "", "", 0, nil
		case "bootstrap":
			bootstrapAttempts++
			if bootstrapAttempts == 1 {
				return "", "Bootstrap failed: 5: Input/output error", 5, nil
			}
			loaded = true
		}
		return "", "", 0, nil
	}
	probe := connector.WithServiceDaemonProbe(func(context.Context, string) (connector.DaemonProcessInfo, bool, error) {
		if !loaded {
			return connector.DaemonProcessInfo{}, false, nil
		}
		buildID := "old-build"
		if bootstrapAttempts > 1 {
			buildID = buildinfo.Current().ID()
		}
		return connector.DaemonProcessInfo{PID: 123, BuildID: buildID}, true, nil
	})

	plan, err := connector.SetupDaemonService(context.Background(), connector.DaemonServiceConfig{
		DaemonBinary: "/usr/bin/true", HollerBinary: "/usr/bin/true", Home: directory,
		Socket: filepath.Join(directory, ".holler", "holler.sock"), GOOS: "darwin", Apply: true,
	}, connector.WithServiceCommandRunner(runner), probe)
	if err != nil {
		t.Fatal(err)
	}
	if bootstrapAttempts != 2 {
		t.Fatalf("bootstrap attempts=%d, want 2", bootstrapAttempts)
	}
	if len(plan.Backups) != 1 {
		t.Fatalf("backups=%v, want one replaced service backup", plan.Backups)
	}
}

func TestDaemonServiceSetupKickstartsLoadedStoppedLaunchdService(t *testing.T) {
	directory := t.TempDir()
	loaded, running := false, false
	var commands []string
	runner := func(_ context.Context, command string, args ...string) (string, string, int, error) {
		joined := command + " " + strings.Join(args, " ")
		commands = append(commands, joined)
		if len(args) > 0 && args[0] == "print" {
			if !loaded {
				return "", "not found", 1, nil
			}
			if running {
				return "state = running\npid = 123", "", 0, nil
			}
			return "state = not running", "", 0, nil
		}
		if len(args) > 0 && args[0] == "bootstrap" {
			loaded, running = true, true
		}
		if len(args) > 0 && args[0] == "kickstart" {
			running = true
		}
		return "", "", 0, nil
	}
	config := connector.DaemonServiceConfig{
		DaemonBinary: "/usr/bin/true", HollerBinary: "/usr/bin/true", Home: directory,
		Socket: filepath.Join(directory, ".holler", "holler.sock"), GOOS: "darwin", Apply: true,
	}
	probe := connector.WithServiceDaemonProbe(func(context.Context, string) (connector.DaemonProcessInfo, bool, error) {
		if !running {
			return connector.DaemonProcessInfo{}, false, nil
		}
		return connector.DaemonProcessInfo{PID: 123, BuildID: buildinfo.Current().ID()}, true, nil
	})
	if _, err := connector.SetupDaemonService(context.Background(), config, connector.WithServiceCommandRunner(runner), probe); err != nil {
		t.Fatal(err)
	}
	running = false
	commands = nil
	if _, err := connector.SetupDaemonService(context.Background(), config, connector.WithServiceCommandRunner(runner), probe); err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(commands, "\n")
	if !strings.Contains(joined, "launchctl kickstart -k") || strings.Contains(joined, "launchctl bootstrap") {
		t.Fatalf("commands=%v", commands)
	}
}

func TestDaemonServiceSetupUpgradesManagedDaemonWithoutPIDReporting(t *testing.T) {
	directory := t.TempDir()
	loaded, legacy := false, false
	var commands []string
	runner := func(_ context.Context, command string, args ...string) (string, string, int, error) {
		joined := command + " " + strings.Join(args, " ")
		commands = append(commands, joined)
		if len(args) > 0 && args[0] == "print" {
			if !loaded {
				return "", "not found", 1, nil
			}
			return "state = running\npid = 123", "", 0, nil
		}
		if len(args) > 0 && args[0] == "bootstrap" {
			loaded = true
		}
		if len(args) > 0 && args[0] == "kickstart" {
			legacy = false
		}
		return "", "", 0, nil
	}
	config := connector.DaemonServiceConfig{
		DaemonBinary: "/usr/bin/true", HollerBinary: "/usr/bin/true", Home: directory,
		Socket: filepath.Join(directory, ".holler", "holler.sock"), GOOS: "darwin", Apply: true,
	}
	probe := connector.WithServiceDaemonProbe(func(context.Context, string) (connector.DaemonProcessInfo, bool, error) {
		if !loaded {
			return connector.DaemonProcessInfo{}, false, nil
		}
		if legacy {
			return connector.DaemonProcessInfo{PID: 0, BuildID: "legacy"}, true, nil
		}
		return connector.DaemonProcessInfo{PID: 123, BuildID: buildinfo.Current().ID()}, true, nil
	})
	if _, err := connector.SetupDaemonService(context.Background(), config, connector.WithServiceCommandRunner(runner), probe); err != nil {
		t.Fatal(err)
	}
	legacy = true
	commands = nil
	if _, err := connector.SetupDaemonService(context.Background(), config, connector.WithServiceCommandRunner(runner), probe); err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(commands, "\n")
	if !strings.Contains(joined, "launchctl kickstart -k") || strings.Contains(joined, "unmanaged") {
		t.Fatalf("commands=%v", commands)
	}
}

func TestDaemonServiceSetupRejectsForeignSocketOwnerBeforeWriting(t *testing.T) {
	directory := t.TempDir()
	config := connector.DaemonServiceConfig{
		DaemonBinary: "/usr/bin/true", HollerBinary: "/usr/bin/true", Home: directory,
		Socket: filepath.Join(directory, ".holler", "holler.sock"), GOOS: "darwin", Apply: true,
	}
	runner := connector.WithServiceCommandRunner(func(context.Context, string, ...string) (string, string, int, error) {
		return "", "not found", 1, nil
	})
	probe := connector.WithServiceDaemonProbe(func(context.Context, string) (connector.DaemonProcessInfo, bool, error) {
		return connector.DaemonProcessInfo{PID: 999, BuildID: "other"}, true, nil
	})
	plan, err := connector.SetupDaemonService(context.Background(), config, runner, probe)
	if err == nil || !strings.Contains(err.Error(), "unmanaged hollerd pid 999") {
		t.Fatalf("plan=%+v err=%v", plan, err)
	}
	if _, statErr := os.Stat(filepath.Join(directory, "Library", "LaunchAgents", "com.72olabs.hollerd.plist")); !os.IsNotExist(statErr) {
		t.Fatalf("collision wrote service file: %v", statErr)
	}
}

func TestDaemonServiceRemovalStopsServiceAndPreservesData(t *testing.T) {
	directory := t.TempDir()
	servicePath := filepath.Join(directory, "Library", "LaunchAgents", "com.72olabs.hollerd.plist")
	database := filepath.Join(directory, ".holler", "holler.sqlite3")
	if err := os.MkdirAll(filepath.Dir(servicePath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(database), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(servicePath, []byte("plist"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(database, []byte("durable"), 0o600); err != nil {
		t.Fatal(err)
	}
	var commands []string
	runner := connector.WithServiceCommandRunner(func(_ context.Context, command string, args ...string) (string, string, int, error) {
		commands = append(commands, command+" "+strings.Join(args, " "))
		if len(args) > 0 && args[0] == "print" {
			return "state = running\npid = 123", "", 0, nil
		}
		return "", "", 0, nil
	})
	plan, err := connector.RemoveDaemonService(context.Background(), connector.DaemonServiceConfig{
		Home: directory, GOOS: "darwin", Database: database, Apply: true,
	}, runner)
	if err != nil {
		t.Fatal(err)
	}
	if !plan.Applied || len(commands) != 2 || !strings.Contains(commands[1], "bootout") {
		t.Fatalf("plan=%+v commands=%v", plan, commands)
	}
	if _, err := os.Stat(servicePath); !os.IsNotExist(err) {
		t.Fatalf("service file remained: %v", err)
	}
	if raw, err := os.ReadFile(database); err != nil || string(raw) != "durable" {
		t.Fatalf("database=%q err=%v", raw, err)
	}
}

func TestDiscoverMarketplaceFromDevelopmentBinary(t *testing.T) {
	repo := repositoryRoot(t)
	binary := filepath.Join(repo, ".build", "holler")
	for _, harness := range []string{"claude", "codex"} {
		root, err := connector.DiscoverMarketplace(harness, "", binary)
		if err != nil {
			t.Fatal(err)
		}
		if root != filepath.Join(repo, "connectors", "marketplace") {
			t.Fatalf("%s marketplace=%q", harness, root)
		}
	}
}

func TestDiscoverMarketplacePrefersStableSymlinkPrefix(t *testing.T) {
	directory := t.TempDir()
	stableBin := filepath.Join(directory, "prefix", "bin")
	cellarBin := filepath.Join(directory, "Cellar", "holler", "0.1.0", "bin")
	for _, path := range []string{stableBin, cellarBin} {
		if err := os.MkdirAll(path, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	cellarExecutable := filepath.Join(cellarBin, "holler")
	if err := os.WriteFile(cellarExecutable, []byte("binary"), 0o700); err != nil {
		t.Fatal(err)
	}
	stableExecutable := filepath.Join(stableBin, "holler")
	if err := os.Symlink(cellarExecutable, stableExecutable); err != nil {
		t.Fatal(err)
	}
	stableMarketplace := filepath.Join(directory, "prefix", "share", "holler", "marketplace")
	cellarMarketplace := filepath.Join(directory, "Cellar", "holler", "0.1.0", "share", "holler", "marketplace")
	for _, root := range []string{stableMarketplace, cellarMarketplace} {
		if err := os.MkdirAll(filepath.Join(root, ".claude-plugin"), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, ".claude-plugin", "marketplace.json"), []byte(`{}`), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	root, err := connector.DiscoverMarketplace("claude", "", stableExecutable)
	if err != nil {
		t.Fatal(err)
	}
	if root != stableMarketplace {
		t.Fatalf("marketplace=%q want stable %q", root, stableMarketplace)
	}
}

func TestOpenCodeSetupDryRunAndApplyInstallIsolatedPackage(t *testing.T) {
	repo := repositoryRoot(t)
	directory := t.TempDir()
	source := filepath.Join(repo, "connectors", "marketplace", "plugins", "opencode-holler")
	installRoot := filepath.Join(directory, "opencode", "holler")
	profile := filepath.Join(installRoot, "opencode.json")
	selection := filepath.Join(directory, "holler", "opencode.json")
	base := connector.OpenCodeSetupConfig{
		AttentionMode: connector.AttentionNativePrompt, Actor: "opencode-live", Peer: "codex-live",
		Project: "holler", ProjectRoot: repo, PackageSource: source, PackageRoot: installRoot,
		ProfilePath: profile, ConnectorConfig: selection, HollerBinary: "/bin/holler",
	}
	plan, err := connector.SetupOpenCode(context.Background(), base)
	if err != nil || plan.Applied {
		t.Fatalf("plan=%+v err=%v", plan, err)
	}
	if _, err := os.Stat(profile); !os.IsNotExist(err) {
		t.Fatalf("dry run wrote profile: %v", err)
	}

	base.Apply = true
	plan, err = connector.SetupOpenCode(context.Background(), base)
	if err != nil {
		t.Fatal(err)
	}
	if !plan.Applied || plan.PackageRoot != installRoot || plan.OpenCodeConfigPath != profile {
		t.Fatalf("plan=%+v", plan)
	}
	raw, err := os.ReadFile(profile)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `"holler_bus_inbox": "allow"`) ||
		!strings.Contains(string(raw), filepath.Join(installRoot, "scripts", "holler")) {
		t.Fatalf("profile=%s", raw)
	}
	launcher, err := os.Stat(filepath.Join(installRoot, "scripts", "holler"))
	if err != nil || launcher.Mode()&0o111 == 0 {
		t.Fatalf("launcher mode=%v err=%v", launcher, err)
	}
	loaded, err := connector.LoadOpenCodeConnectorConfig(selection)
	if err != nil || loaded.Actor != "opencode-live" || loaded.PackageRoot != installRoot || loaded.ServerHostname != "127.0.0.1" {
		t.Fatalf("config=%+v err=%v", loaded, err)
	}
}
