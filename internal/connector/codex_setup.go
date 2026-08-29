package connector

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/pelletier/go-toml/v2"
)

type CodexSetupConfig struct {
	AttentionMode     string
	Actor             string
	Role              string
	Peer              string
	Project           string
	ProjectRoot       string
	Channel           string
	Socket            string
	PluginID          string
	Marketplace       string
	Profile           string
	PolicyPath        string
	UserConfigPath    string
	GlobalPolicy      bool
	ConnectorConfig   string
	CodexHome         string
	CodexBinary       string
	HollerBinary      string
	RuntimeBinaryPath string
	Apply             bool
}

func SetupCodex(ctx context.Context, config CodexSetupConfig, options ...SetupOption) (SetupPlan, error) {
	runtime := &setupRuntime{run: runCommand, readFile: os.ReadFile, lookPath: exec.LookPath}
	for _, option := range options {
		option(runtime)
	}
	config = codexSetupDefaults(config)
	var err error
	config.HollerBinary, err = stableExecutablePath(config.HollerBinary, runtime.lookPath)
	if err != nil {
		return SetupPlan{}, fmt.Errorf("resolve holler executable: %w", err)
	}
	config.CodexBinary, err = stableExecutablePath(config.CodexBinary, runtime.lookPath)
	if err != nil {
		return SetupPlan{}, fmt.Errorf("resolve Codex executable: %w", err)
	}
	if err := validateCodexSetup(config); err != nil {
		return SetupPlan{}, err
	}
	if config.GlobalPolicy {
		if _, _, err := plannedCodexUserConfig(runtime.readFile, config.UserConfigPath, config.PluginID); err != nil {
			return SetupPlan{}, err
		}
	}
	plan := SetupPlan{
		SchemaVersion: 1, Harness: "codex", Applied: config.Apply,
		AttentionMode: config.AttentionMode, PluginID: config.PluginID,
		ConnectorConfigPath: config.ConnectorConfig, PolicyPath: config.PolicyPath, Profile: config.Profile,
		RuntimeBinaryPath: config.RuntimeBinaryPath,
		Actions: []string{
			"register the Holler Codex marketplace when a source is provided",
			"install or update the Codex plugin",
			"write a dedicated least-privilege Codex profile for the frozen Holler MCP surface",
			"write the Holler Codex connector selection",
			"record absolute holler and Codex executable paths for background and plugin processes",
		},
		LaunchCommand: []string{"holler", "connector", "launch", "--harness", "codex"},
		DoctorCommand: []string{"holler", "connector", "doctor", "--harness", "codex", "--profile", profileForAttention(config.AttentionMode), "--policy", config.PolicyPath, "--attention", config.AttentionMode, "--actor", config.Actor},
		GuidedActions: []string{
			"Review the generated Codex profile before use; it authorizes only the frozen Holler MCP tool surface.",
			"On first launch, inspect the Holler SessionStart and SessionEnd hooks when Codex asks for trust, then approve the exact installed package hash.",
			"Do not use --dangerously-bypass-hook-trust for normal sessions; it is reserved for disposable certification canaries after package validation.",
			"After hook trust and MCP policy admission, plain Codex sessions load the persisted connector binding and derive one immutable run from the Codex process. Use connector launch for the dedicated least-privilege profile or explicit identity overrides.",
			"Give concurrently running Codex sessions distinct actor names unless you intentionally want competing workers for one inbox.",
		},
	}
	if config.GlobalPolicy {
		plan.UserConfigPath = config.UserConfigPath
		plan.Actions = append(plan.Actions, "merge the same frozen MCP allowlist into Codex user config so plain codex sessions do not prompt per Holler tool")
		plan.DoctorCommand = []string{"holler", "connector", "doctor", "--harness", "codex", "--profile", profileForAttention(config.AttentionMode), "--policy", config.UserConfigPath, "--attention", config.AttentionMode, "--actor", config.Actor}
		plan.GuidedActions[0] = "Review the Holler section merged into Codex user config; it authorizes only the frozen Holler MCP tool surface."
	}
	manifest, _ := Manifest("codex")
	plan.GuidedActions[1] += " Expected package content hash: " + manifest.PackageHash + "."
	if config.AttentionMode == AttentionStartupOnly {
		plan.GuidedActions = append(plan.GuidedActions, "Startup-only preserves durable inbox hydration but cannot satisfy the live-review profile because it has no live wake path.")
	}
	if !config.Apply {
		return plan, nil
	}
	if strings.TrimSpace(config.Marketplace) != "" {
		marketplace := marketplaceName(config.PluginID)
		registered, stale, err := codexMarketplaceRegistration(ctx, runtime.run, config.CodexBinary, config.Marketplace, marketplace)
		if err != nil {
			return plan, err
		}
		if stale {
			if err := runSetupCommand(ctx, runtime.run, config.CodexBinary, "plugin", "marketplace", "remove", marketplace); err != nil {
				return plan, err
			}
		}
		if !registered {
			if err := runSetupCommand(ctx, runtime.run, config.CodexBinary, "plugin", "marketplace", "add", config.Marketplace); err != nil {
				return plan, err
			}
		}
	}
	// `plugin add` is intentionally used for both first install and update. Codex
	// resolves the current marketplace snapshot and refreshes the cached package.
	if err := runSetupCommand(ctx, runtime.run, config.CodexBinary, "plugin", "add", config.PluginID); err != nil {
		return plan, err
	}
	policyBackup, err := writeConfigBytes(config.PolicyPath, []byte(codexPolicy(config.PluginID)))
	if err != nil {
		return plan, err
	}
	if policyBackup != "" {
		plan.Backups = append(plan.Backups, policyBackup)
	}
	if config.GlobalPolicy {
		userConfigBackup, err := mergeCodexUserConfig(runtime.readFile, config.UserConfigPath, config.PluginID)
		if err != nil {
			return plan, err
		}
		if userConfigBackup != "" {
			plan.Backups = append(plan.Backups, userConfigBackup)
		}
	}
	connectorBackup, err := writeJSONConfig(config.ConnectorConfig, CodexConnectorConfig{
		SchemaVersion: 1, AttentionMode: config.AttentionMode, PluginID: config.PluginID,
		Profile: config.Profile, Actor: config.Actor, Role: config.Role, Peer: config.Peer,
		Project: config.Project, ProjectRoot: config.ProjectRoot, Channel: config.Channel, Socket: config.Socket,
		HollerBinary: config.HollerBinary, ClientBinary: config.CodexBinary,
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

func codexMarketplaceRegistration(ctx context.Context, runner CommandRunner, binary, source, wantedName string) (bool, bool, error) {
	stdout, stderr, exitCode, err := runner(ctx, binary, "plugin", "marketplace", "list", "--json")
	if err != nil || exitCode != 0 {
		return false, false, fmt.Errorf("list Codex marketplaces: %s", strings.TrimSpace(firstNonEmpty(stderr, errorText(err))))
	}
	var result struct {
		Marketplaces []struct {
			Name   string `json:"name"`
			Root   string `json:"root"`
			Source struct {
				Source string `json:"source"`
			} `json:"marketplaceSource"`
		} `json:"marketplaces"`
	}
	if err := json.Unmarshal([]byte(stdout), &result); err != nil {
		return false, false, fmt.Errorf("decode Codex marketplace list: %w", err)
	}
	wanted := strings.TrimSpace(source)
	wantedAbs, _ := filepath.Abs(wanted)
	nameConflict := false
	for _, marketplace := range result.Marketplaces {
		for _, candidate := range []string{marketplace.Root, marketplace.Source.Source} {
			if candidate == wanted || (wantedAbs != "" && filepath.Clean(candidate) == filepath.Clean(wantedAbs)) {
				return true, false, nil
			}
		}
		nameConflict = nameConflict || marketplace.Name == wantedName
	}
	return false, nameConflict, nil
}

func codexSetupDefaults(config CodexSetupConfig) CodexSetupConfig {
	if strings.TrimSpace(config.HollerBinary) == "" {
		config.HollerBinary, _ = os.Executable()
	}
	if strings.TrimSpace(config.RuntimeBinaryPath) == "" {
		config.RuntimeBinaryPath = DefaultRuntimeBinaryPath()
	}
	if strings.TrimSpace(config.AttentionMode) == "" {
		config.AttentionMode = AttentionNativeQueue
	}
	if strings.TrimSpace(config.PluginID) == "" {
		config.PluginID = DefaultCodexPluginID
	}
	if strings.TrimSpace(config.Profile) == "" {
		config.Profile = "holler"
	}
	if strings.TrimSpace(config.Project) == "" {
		config.Project = "default"
	}
	if strings.TrimSpace(config.Channel) == "" {
		config.Channel = "direct"
	}
	if strings.TrimSpace(config.CodexBinary) == "" {
		config.CodexBinary = "codex"
	}
	if strings.TrimSpace(config.ConnectorConfig) == "" {
		config.ConnectorConfig = DefaultCodexConnectorConfigPath()
	}
	if strings.TrimSpace(config.ProjectRoot) == "" {
		config.ProjectRoot, _ = os.Getwd()
	}
	if strings.TrimSpace(config.CodexHome) == "" {
		config.CodexHome = strings.TrimSpace(os.Getenv("CODEX_HOME"))
		if config.CodexHome == "" {
			if home, err := os.UserHomeDir(); err == nil {
				config.CodexHome = filepath.Join(home, ".codex")
			}
		}
	}
	if strings.TrimSpace(config.PolicyPath) == "" {
		config.PolicyPath = filepath.Join(config.CodexHome, config.Profile+".config.toml")
	}
	if strings.TrimSpace(config.UserConfigPath) == "" {
		config.UserConfigPath = filepath.Join(config.CodexHome, "config.toml")
	}
	return config
}

func validateCodexSetup(config CodexSetupConfig) error {
	if err := ValidateCodexAttentionMode(config.AttentionMode); err != nil {
		return err
	}
	if strings.TrimSpace(config.Actor) == "" {
		return fmt.Errorf("actor is required")
	}
	if !safeConfigName(config.Profile) {
		return fmt.Errorf("Codex profile must contain only letters, digits, dot, underscore, or hyphen")
	}
	if strings.ContainsAny(config.PluginID, "\"\\\r\n") || marketplaceName(config.PluginID) == "" || strings.HasPrefix(config.PluginID, "@") {
		return fmt.Errorf("Codex plugin ID must be a safe plugin@marketplace identifier")
	}
	if strings.TrimSpace(config.PolicyPath) == "" || strings.TrimSpace(config.ConnectorConfig) == "" ||
		(config.GlobalPolicy && strings.TrimSpace(config.UserConfigPath) == "") {
		return fmt.Errorf("policy, user config, and connector config paths are required")
	}
	return nil
}

func safeConfigName(value string) bool {
	if value == "" {
		return false
	}
	for _, char := range value {
		if (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') ||
			(char >= '0' && char <= '9') || char == '.' || char == '_' || char == '-' {
			continue
		}
		return false
	}
	return true
}

func profileForAttention(attention string) string {
	if attention == AttentionStartupOnly {
		return "async-peer"
	}
	return "live-review"
}

func codexPolicy(pluginID string) string {
	manifest, _ := Manifest("codex")
	var builder strings.Builder
	fmt.Fprintf(&builder, "# Generated by holler connector setup. Review before use.\n")
	fmt.Fprintf(&builder, "# Frozen tool surface: %s\n", manifest.ToolSurfaceHash)
	fmt.Fprintf(&builder, "[plugins.%q.mcp_servers.%s]\n", pluginID, manifest.MCPServerName)
	builder.WriteString("enabled = true\nrequired = true\ndefault_tools_approval_mode = \"approve\"\nenabled_tools = [\n")
	for _, tool := range manifest.Tools {
		fmt.Fprintf(&builder, "  %q,\n", tool.Name)
	}
	builder.WriteString("]\n\n")
	for _, tool := range manifest.Tools {
		fmt.Fprintf(&builder, "[plugins.%q.mcp_servers.%s.tools.%s]\napproval_mode = \"approve\"\n", pluginID, manifest.MCPServerName, tool.Name)
	}
	return builder.String()
}

const (
	codexManagedPolicyStart = "# BEGIN holler managed Codex MCP policy"
	codexManagedPolicyEnd   = "# END holler managed Codex MCP policy"
)

func mergeCodexUserConfig(readFile func(string) ([]byte, error), path, pluginID string) (string, error) {
	output, changed, err := plannedCodexUserConfig(readFile, path, pluginID)
	if err != nil || !changed {
		return "", err
	}
	return writeConfigBytes(path, output)
}

func plannedCodexUserConfig(readFile func(string) ([]byte, error), path, pluginID string) ([]byte, bool, error) {
	raw, err := readFile(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, false, err
	}
	if errors.Is(err, os.ErrNotExist) {
		raw = nil
	}
	base, hadManagedBlock, err := stripManagedCodexPolicy(raw)
	if err != nil {
		return nil, false, err
	}
	if len(strings.TrimSpace(string(base))) > 0 {
		var settings map[string]interface{}
		if err := toml.Unmarshal(base, &settings); err != nil {
			return nil, false, fmt.Errorf("decode Codex user config: %w", err)
		}
		if codexPluginPolicyExists(settings, pluginID) {
			if codexPluginPolicyAuthorizes(settings, pluginID) {
				if !hadManagedBlock {
					return raw, false, nil
				}
				return ensureTrailingNewline(base), true, nil
			}
			return nil, false, fmt.Errorf("Codex user config already contains a conflicting Holler MCP policy; review %s and reconcile it before rerunning setup", path)
		}
	}
	var output strings.Builder
	output.Write(ensureTrailingNewline(base))
	if output.Len() > 0 {
		output.WriteByte('\n')
	}
	output.WriteString(codexManagedPolicyStart)
	output.WriteByte('\n')
	output.WriteString(codexPolicy(pluginID))
	output.WriteString(codexManagedPolicyEnd)
	output.WriteByte('\n')
	merged := []byte(output.String())
	return merged, !bytes.Equal(raw, merged), nil
}

func stripManagedCodexPolicy(raw []byte) ([]byte, bool, error) {
	text := string(raw)
	start := strings.Index(text, codexManagedPolicyStart)
	end := strings.Index(text, codexManagedPolicyEnd)
	if start < 0 && end < 0 {
		return raw, false, nil
	}
	if start < 0 || end < start {
		return nil, false, fmt.Errorf("Codex user config contains an incomplete Holler managed policy block")
	}
	end += len(codexManagedPolicyEnd)
	for end < len(text) && (text[end] == '\r' || text[end] == '\n') {
		end++
	}
	return []byte(strings.TrimRight(text[:start], "\r\n") + text[end:]), true, nil
}

func ensureTrailingNewline(raw []byte) []byte {
	if len(raw) == 0 || raw[len(raw)-1] == '\n' {
		return raw
	}
	return append(append([]byte(nil), raw...), '\n')
}

func codexPluginPolicyExists(settings map[string]interface{}, pluginID string) bool {
	plugins, ok := settings["plugins"].(map[string]interface{})
	if !ok {
		return false
	}
	plugin, ok := plugins[pluginID].(map[string]interface{})
	if !ok {
		return false
	}
	servers, ok := plugin["mcp_servers"].(map[string]interface{})
	if !ok {
		return false
	}
	manifest, _ := Manifest("codex")
	_, ok = servers[manifest.MCPServerName]
	return ok
}

func codexPluginPolicyAuthorizes(settings map[string]interface{}, pluginID string) bool {
	manifest, _ := Manifest("codex")
	plugins, _ := settings["plugins"].(map[string]interface{})
	plugin, _ := plugins[pluginID].(map[string]interface{})
	servers, _ := plugin["mcp_servers"].(map[string]interface{})
	server, _ := servers[manifest.MCPServerName].(map[string]interface{})
	if enabled, ok := server["enabled"].(bool); !ok || !enabled {
		return false
	}
	enabledTools := make(map[string]bool)
	for _, name := range stringSliceAt(server, "enabled_tools") {
		enabledTools[name] = true
	}
	defaultApproval, _ := server["default_tools_approval_mode"].(string)
	tools, _ := server["tools"].(map[string]interface{})
	for _, tool := range manifest.Tools {
		if !enabledTools[tool.Name] {
			return false
		}
		approval := defaultApproval
		if configured, ok := tools[tool.Name].(map[string]interface{}); ok {
			if value, ok := configured["approval_mode"].(string); ok {
				approval = value
			}
		}
		if approval != "approve" {
			return false
		}
	}
	return true
}
