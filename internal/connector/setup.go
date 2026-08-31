package connector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
)

const DefaultClaudePluginID = "holler@holler"

type ClaudeSetupConfig struct {
	AttentionMode     string
	NameMode          string
	Actor             string
	Role              string
	Peer              string
	Project           string
	Channel           string
	Socket            string
	PluginID          string
	Marketplace       string
	Scope             string
	ConnectorConfig   string
	ClaudeSettings    string
	ClaudeBinary      string
	HollerBinary      string
	RuntimeBinaryPath string
	Apply             bool
}

type SetupPlan struct {
	SchemaVersion       int      `json:"schema_version"`
	Harness             string   `json:"harness"`
	Applied             bool     `json:"applied"`
	AttentionMode       string   `json:"attention_mode"`
	NameMode            string   `json:"name_mode,omitempty"`
	PluginID            string   `json:"plugin_id"`
	ConnectorConfigPath string   `json:"connector_config_path"`
	ClaudeSettingsPath  string   `json:"claude_settings_path,omitempty"`
	PolicyPath          string   `json:"policy_path,omitempty"`
	UserConfigPath      string   `json:"user_config_path,omitempty"`
	Profile             string   `json:"profile,omitempty"`
	PackageRoot         string   `json:"package_root,omitempty"`
	OpenCodeConfigPath  string   `json:"opencode_config_path,omitempty"`
	Backups             []string `json:"backups,omitempty"`
	Actions             []string `json:"actions"`
	LaunchCommand       []string `json:"launch_command"`
	DoctorCommand       []string `json:"doctor_command"`
	GuidedActions       []string `json:"guided_actions,omitempty"`
	RuntimeBinaryPath   string   `json:"runtime_binary_path,omitempty"`
}

type setupRuntime struct {
	run      CommandRunner
	readFile func(string) ([]byte, error)
	lookPath func(string) (string, error)
}

type SetupOption func(*setupRuntime)

func WithSetupCommandRunner(runner CommandRunner) SetupOption {
	return func(runtime *setupRuntime) { runtime.run = runner }
}

func WithSetupLookPath(lookPath func(string) (string, error)) SetupOption {
	return func(runtime *setupRuntime) { runtime.lookPath = lookPath }
}

func SetupClaude(ctx context.Context, config ClaudeSetupConfig, options ...SetupOption) (SetupPlan, error) {
	runtime := &setupRuntime{run: runCommand, readFile: os.ReadFile, lookPath: exec.LookPath}
	for _, option := range options {
		option(runtime)
	}
	config = setupDefaults(config)
	var err error
	config.HollerBinary, err = stableExecutablePath(config.HollerBinary, runtime.lookPath)
	if err != nil {
		return SetupPlan{}, fmt.Errorf("resolve holler executable: %w", err)
	}
	if err := validateSetup(config); err != nil {
		return SetupPlan{}, err
	}
	plan := SetupPlan{
		SchemaVersion: 1, Harness: "claude", Applied: config.Apply,
		AttentionMode: config.AttentionMode, NameMode: config.NameMode,
		PluginID: config.PluginID, ConnectorConfigPath: config.ConnectorConfig,
		ClaudeSettingsPath: config.ClaudeSettings,
		Actions: []string{
			"register the Holler Claude marketplace",
			"install or update the Claude plugin",
			"merge the frozen Holler MCP allowlist and plugin options into Claude user settings",
			"write the Holler Claude connector selection",
			"record the absolute holler executable for plugin processes with a minimal PATH",
		},
		LaunchCommand:     []string{"holler", "connector", "launch", "--harness", "claude"},
		DoctorCommand:     []string{"holler", "connector", "doctor", "--harness", "claude", "--profile", "live-review", "--attention", config.AttentionMode, "--actor", config.Actor},
		RuntimeBinaryPath: config.RuntimeBinaryPath,
	}
	plan.GuidedActions = append(plan.GuidedActions,
		"After setup, plain Claude sessions load the persisted connector binding and derive one immutable run from the Claude process. Use connector launch only for explicit per-session identity overrides.",
		"Give concurrently running Claude sessions distinct actor names unless you intentionally want them to compete as workers for one inbox.",
	)
	if !config.Apply {
		return plan, nil
	}

	if strings.TrimSpace(config.Marketplace) != "" {
		marketplace := marketplaceName(config.PluginID)
		registered, stale, err := claudeMarketplaceRegistration(ctx, runtime.run, config.ClaudeBinary, config.Marketplace, marketplace)
		if err != nil {
			return plan, err
		}
		if stale {
			if err := runSetupCommand(ctx, runtime.run, config.ClaudeBinary, "plugin", "marketplace", "remove", marketplace, "--scope", config.Scope); err != nil {
				return plan, err
			}
		}
		if !registered {
			if err := runSetupCommand(ctx, runtime.run, config.ClaudeBinary, "plugin", "marketplace", "add", config.Marketplace, "--scope", config.Scope); err != nil {
				return plan, err
			}
		}
	}
	installed, enabled, err := claudePluginInstalled(ctx, runtime.run, config.ClaudeBinary, config.PluginID)
	if err != nil {
		return plan, err
	}
	if installed {
		if err := runSetupCommand(ctx, runtime.run, config.ClaudeBinary, "plugin", "update", config.PluginID, "--scope", config.Scope, "--yes"); err != nil {
			return plan, err
		}
		if !enabled {
			if err := runSetupCommand(ctx, runtime.run, config.ClaudeBinary, "plugin", "enable", config.PluginID, "--scope", config.Scope); err != nil {
				return plan, err
			}
		}
	} else {
		installArgs := []string{"plugin", "install", config.PluginID, "--scope", config.Scope, "--yes"}
		for _, option := range []struct{ name, value string }{
			{"actor", config.Actor}, {"role", config.Role}, {"peer", config.Peer},
			{"project", config.Project}, {"channel", config.Channel}, {"socket", config.Socket},
			{"attention_mode", config.AttentionMode}, {"name_mode", config.NameMode}, {"binary", config.HollerBinary},
		} {
			if option.value != "" {
				installArgs = append(installArgs, "--config", option.name+"="+option.value)
			}
		}
		if err := runSetupCommand(ctx, runtime.run, config.ClaudeBinary, installArgs...); err != nil {
			return plan, err
		}
	}

	settingsBackup, err := mergeClaudeSettings(runtime.readFile, config)
	if err != nil {
		return plan, err
	}
	if settingsBackup != "" {
		plan.Backups = append(plan.Backups, settingsBackup)
	}
	connectorBackup, err := writeJSONConfig(config.ConnectorConfig, ClaudeConnectorConfig{
		SchemaVersion: 1, AttentionMode: config.AttentionMode, NameMode: config.NameMode,
		PluginID: config.PluginID, Actor: config.Actor, Role: config.Role, Peer: config.Peer,
		Project: config.Project, Channel: config.Channel, Socket: config.Socket, HollerBinary: config.HollerBinary,
	})
	if err != nil {
		return plan, err
	}
	if connectorBackup != "" {
		plan.Backups = append(plan.Backups, connectorBackup)
	}
	binaryBackup, err := writeConfigBytes(plan.RuntimeBinaryPath, []byte(config.HollerBinary+"\n"))
	if err != nil {
		return plan, err
	}
	if binaryBackup != "" {
		plan.Backups = append(plan.Backups, binaryBackup)
	}
	return plan, nil
}

func claudeMarketplaceRegistration(ctx context.Context, runner CommandRunner, command, source, wantedName string) (bool, bool, error) {
	stdout, stderr, exitCode, err := runner(ctx, command, "plugin", "marketplace", "list", "--json")
	if err != nil || exitCode != 0 {
		return false, false, fmt.Errorf("list Claude marketplaces: %s", strings.TrimSpace(firstNonEmpty(stderr, errorText(err))))
	}
	var marketplaces []struct {
		Name            string `json:"name"`
		Path            string `json:"path"`
		InstallLocation string `json:"installLocation"`
	}
	if err := json.Unmarshal([]byte(stdout), &marketplaces); err != nil {
		return false, false, fmt.Errorf("decode Claude marketplace list: %w", err)
	}
	wanted := strings.TrimSpace(source)
	wantedAbsolute, _ := filepath.Abs(wanted)
	nameConflict := false
	for _, marketplace := range marketplaces {
		for _, candidate := range []string{marketplace.Path, marketplace.InstallLocation} {
			if candidate == wanted || (wantedAbsolute != "" && filepath.Clean(candidate) == filepath.Clean(wantedAbsolute)) {
				return true, false, nil
			}
		}
		nameConflict = nameConflict || marketplace.Name == wantedName
	}
	return false, nameConflict, nil
}

func marketplaceName(pluginID string) string {
	if index := strings.LastIndex(pluginID, "@"); index >= 0 {
		return strings.TrimSpace(pluginID[index+1:])
	}
	return ""
}

func setupDefaults(config ClaudeSetupConfig) ClaudeSetupConfig {
	if strings.TrimSpace(config.HollerBinary) == "" {
		config.HollerBinary, _ = os.Executable()
	}
	if strings.TrimSpace(config.RuntimeBinaryPath) == "" {
		config.RuntimeBinaryPath = DefaultRuntimeBinaryPath()
	}
	config.AttentionMode = strings.TrimSpace(config.AttentionMode)
	config.NameMode = strings.TrimSpace(config.NameMode)
	if config.AttentionMode == "" {
		config.AttentionMode = AttentionHookLongPoll
	}
	if strings.TrimSpace(config.PluginID) == "" {
		config.PluginID = DefaultClaudePluginID
	}
	if strings.TrimSpace(config.Scope) == "" {
		config.Scope = "user"
	}
	if strings.TrimSpace(config.Project) == "" {
		config.Project = "default"
	}
	if strings.TrimSpace(config.Channel) == "" {
		config.Channel = "direct"
	}
	if strings.TrimSpace(config.ClaudeBinary) == "" {
		config.ClaudeBinary = "claude"
	}
	if strings.TrimSpace(config.ConnectorConfig) == "" {
		config.ConnectorConfig = DefaultClaudeConnectorConfigPath()
	}
	if strings.TrimSpace(config.ClaudeSettings) == "" {
		base := strings.TrimSpace(os.Getenv("CLAUDE_CONFIG_DIR"))
		if base == "" {
			if home, err := os.UserHomeDir(); err == nil {
				base = filepath.Join(home, ".claude")
			}
		}
		config.ClaudeSettings = filepath.Join(base, "settings.json")
	}
	return config
}

func DefaultRuntimeBinaryPath() string {
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return filepath.Join(".holler", "bin-path")
	}
	return filepath.Join(home, ".holler", "bin-path")
}

func stableExecutablePath(command string, lookPath func(string) (string, error)) (string, error) {
	command = strings.TrimSpace(command)
	if command == "" {
		return "", fmt.Errorf("path is required")
	}
	if !filepath.IsAbs(command) {
		resolved, err := lookPath(command)
		if err != nil {
			return "", err
		}
		command = resolved
	}
	absolute, err := filepath.Abs(command)
	if err != nil {
		return "", err
	}
	info, err := os.Stat(absolute)
	if err != nil {
		return "", err
	}
	if info.IsDir() || info.Mode()&0o111 == 0 {
		return "", fmt.Errorf("%s is not an executable file", absolute)
	}
	return filepath.Clean(absolute), nil
}

func validateSetup(config ClaudeSetupConfig) error {
	if err := ValidateClaudeAttentionMode(config.AttentionMode); err != nil {
		return err
	}
	if err := ValidateNameMode(config.NameMode); err != nil {
		return err
	}
	if strings.TrimSpace(config.Actor) == "" {
		return fmt.Errorf("actor is required")
	}
	if strings.ContainsAny(config.PluginID, "\"\\\r\n") || marketplaceName(config.PluginID) == "" || strings.HasPrefix(config.PluginID, "@") {
		return fmt.Errorf("Claude plugin ID must be a safe plugin@marketplace identifier")
	}
	if config.Scope != "user" && config.Scope != "project" && config.Scope != "local" {
		return fmt.Errorf("scope must be user, project, or local")
	}
	return nil
}

func runSetupCommand(ctx context.Context, runner CommandRunner, command string, args ...string) error {
	_, stderr, exitCode, err := runner(ctx, command, args...)
	if err != nil || exitCode != 0 {
		detail := strings.TrimSpace(stderr)
		if detail == "" && err != nil {
			detail = err.Error()
		}
		return fmt.Errorf("%s %s failed: %s", command, strings.Join(args, " "), detail)
	}
	return nil
}

func claudePluginInstalled(ctx context.Context, runner CommandRunner, command, pluginID string) (bool, bool, error) {
	stdout, stderr, exitCode, err := runner(ctx, command, "plugin", "list", "--json")
	if err != nil || exitCode != 0 {
		return false, false, fmt.Errorf("list Claude plugins: %s", strings.TrimSpace(firstNonEmpty(stderr, errorText(err))))
	}
	var plugins []struct {
		ID      string `json:"id"`
		Enabled bool   `json:"enabled"`
	}
	if err := json.Unmarshal([]byte(stdout), &plugins); err != nil {
		return false, false, fmt.Errorf("decode Claude plugin list: %w", err)
	}
	for _, plugin := range plugins {
		if plugin.ID == pluginID {
			return true, plugin.Enabled, nil
		}
	}
	return false, false, nil
}

func mergeClaudeSettings(readFile func(string) ([]byte, error), config ClaudeSetupConfig) (string, error) {
	settings := map[string]interface{}{}
	raw, err := readFile(config.ClaudeSettings)
	if err == nil {
		if err := json.Unmarshal(raw, &settings); err != nil {
			return "", fmt.Errorf("decode Claude settings: %w", err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	permissions := objectAt(settings, "permissions")
	allow := stringSliceAt(permissions, "allow")
	allowed := make(map[string]struct{}, len(allow))
	for _, value := range allow {
		allowed[value] = struct{}{}
	}
	manifest, _ := Manifest("claude")
	for _, tool := range manifest.Tools {
		allowed[manifest.ClaudeToolPrefix+tool.Name] = struct{}{}
	}
	allow = allow[:0]
	for value := range allowed {
		allow = append(allow, value)
	}
	sort.Strings(allow)
	permissions["allow"] = allow
	settings["permissions"] = permissions

	pluginConfigs := objectAt(settings, "pluginConfigs")
	plugin := objectAt(pluginConfigs, config.PluginID)
	options := objectAt(plugin, "options")
	options["actor"] = config.Actor
	options["role"] = config.Role
	options["peer"] = config.Peer
	options["project"] = config.Project
	options["channel"] = config.Channel
	options["socket"] = config.Socket
	options["attention_mode"] = config.AttentionMode
	if config.NameMode != "" {
		options["name_mode"] = config.NameMode
	} else {
		delete(options, "name_mode")
	}
	options["binary"] = config.HollerBinary
	plugin["options"] = options
	pluginConfigs[config.PluginID] = plugin
	settings["pluginConfigs"] = pluginConfigs
	return writeJSONConfig(config.ClaudeSettings, settings)
}

func objectAt(parent map[string]interface{}, key string) map[string]interface{} {
	if value, ok := parent[key].(map[string]interface{}); ok {
		return value
	}
	return map[string]interface{}{}
}

func stringSliceAt(parent map[string]interface{}, key string) []string {
	values, _ := parent[key].([]interface{})
	result := make([]string, 0, len(values))
	for _, value := range values {
		if text, ok := value.(string); ok {
			result = append(result, text)
		}
	}
	if typed, ok := parent[key].([]string); ok {
		result = append(result, typed...)
	}
	return result
}

func writeJSONConfig(path string, value interface{}) (string, error) {
	raw, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return "", err
	}
	raw = append(raw, '\n')
	return writeConfigBytes(path, raw)
}

func writeConfigBytes(path string, raw []byte) (string, error) {
	logicalPath := path
	path, err := configWriteTarget(path)
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return "", err
	}
	backup := ""
	mode := os.FileMode(0o600)
	if previous, err := os.ReadFile(path); err == nil {
		if string(previous) == string(raw) {
			return "", nil
		}
		if info, statErr := os.Stat(path); statErr == nil {
			mode = info.Mode().Perm()
		}
		created, err := writeFirstBackup(logicalPath+".bak", previous)
		if err != nil {
			return "", err
		}
		if created {
			backup = logicalPath + ".bak"
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	temporary, err := os.CreateTemp(filepath.Dir(path), ".holler-config-*")
	if err != nil {
		return "", err
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(mode); err != nil {
		_ = temporary.Close()
		return "", err
	}
	if _, err := temporary.Write(raw); err != nil {
		_ = temporary.Close()
		return "", err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return "", err
	}
	if err := temporary.Close(); err != nil {
		return "", err
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return "", err
	}
	return backup, nil
}

func configWriteTarget(path string) (string, error) {
	path = filepath.Clean(path)
	seen := make(map[string]bool)
	for {
		if seen[path] {
			return "", fmt.Errorf("configuration symlink cycle at %s", path)
		}
		seen[path] = true
		info, err := os.Lstat(path)
		if errors.Is(err, os.ErrNotExist) {
			return path, nil
		}
		if err != nil {
			return "", err
		}
		if info.Mode()&os.ModeSymlink == 0 {
			return path, nil
		}
		target, err := os.Readlink(path)
		if err != nil {
			return "", err
		}
		if !filepath.IsAbs(target) {
			target = filepath.Join(filepath.Dir(path), target)
		}
		path = filepath.Clean(target)
	}
}

func writeFirstBackup(path string, raw []byte) (bool, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return false, err
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if errors.Is(err, os.ErrExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if _, err := file.Write(raw); err != nil {
		_ = file.Close()
		return false, err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return false, err
	}
	if err := file.Close(); err != nil {
		return false, err
	}
	return true, nil
}

func errorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
