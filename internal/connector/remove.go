package connector

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
)

func RemoveClaude(ctx context.Context, config ClaudeSetupConfig, options ...SetupOption) (SetupPlan, error) {
	runtime := &setupRuntime{run: runCommand, readFile: os.ReadFile, lookPath: exec.LookPath}
	for _, option := range options {
		option(runtime)
	}
	config = setupDefaults(config)
	plan := SetupPlan{
		SchemaVersion: 1, Harness: "claude", Applied: config.Apply, PluginID: config.PluginID,
		ConnectorConfigPath: config.ConnectorConfig, ClaudeSettingsPath: config.ClaudeSettings,
		RuntimeBinaryPath: config.RuntimeBinaryPath,
		Actions: []string{
			"uninstall the Holler Claude plugin and marketplace registration",
			"remove the Holler MCP permissions and plugin options from Claude settings",
			"remove the Claude connector selection",
		},
	}
	if !config.Apply {
		return plan, nil
	}
	claudeBinary, binaryErr := stableExecutablePath(config.ClaudeBinary, runtime.lookPath)
	if binaryErr == nil {
		installed, _, err := claudePluginInstalled(ctx, runtime.run, claudeBinary, config.PluginID)
		if err != nil {
			return plan, err
		}
		if installed {
			if err := runSetupCommand(ctx, runtime.run, claudeBinary, "plugin", "uninstall", config.PluginID, "--scope", config.Scope, "--yes"); err != nil {
				return plan, err
			}
		}
		registered, err := claudeMarketplaceNamed(ctx, runtime.run, claudeBinary, marketplaceName(config.PluginID))
		if err != nil {
			return plan, err
		}
		if registered {
			if err := runSetupCommand(ctx, runtime.run, claudeBinary, "plugin", "marketplace", "remove", marketplaceName(config.PluginID), "--scope", config.Scope); err != nil {
				return plan, err
			}
		}
	}
	backup, err := removeClaudeSettings(runtime.readFile, config.ClaudeSettings, config.PluginID)
	if err != nil {
		return plan, err
	}
	if backup != "" {
		plan.Backups = append(plan.Backups, backup)
	}
	if err := removeGeneratedFile(config.ConnectorConfig); err != nil {
		return plan, err
	}
	return plan, nil
}

func RemoveCodex(ctx context.Context, config CodexSetupConfig, options ...SetupOption) (SetupPlan, error) {
	runtime := &setupRuntime{run: runCommand, readFile: os.ReadFile, lookPath: exec.LookPath}
	for _, option := range options {
		option(runtime)
	}
	config = codexSetupDefaults(config)
	plan := SetupPlan{
		SchemaVersion: 1, Harness: "codex", Applied: config.Apply, PluginID: config.PluginID,
		ConnectorConfigPath: config.ConnectorConfig, PolicyPath: config.PolicyPath,
		UserConfigPath: config.UserConfigPath, Profile: config.Profile, RuntimeBinaryPath: config.RuntimeBinaryPath,
		Actions: []string{
			"remove the Holler Codex plugin and marketplace registration",
			"remove the managed Holler MCP policy from Codex configuration",
			"remove the dedicated profile and Codex connector selection",
		},
	}
	if !config.Apply {
		return plan, nil
	}
	codexBinary, binaryErr := stableExecutablePath(config.CodexBinary, runtime.lookPath)
	if binaryErr == nil {
		if err := runSetupCommand(ctx, runtime.run, codexBinary, "plugin", "remove", config.PluginID); err != nil && !commandReportsMissing(err) {
			return plan, err
		}
		registered, err := codexMarketplaceNamed(ctx, runtime.run, codexBinary, marketplaceName(config.PluginID))
		if err != nil {
			return plan, err
		}
		if registered {
			if err := runSetupCommand(ctx, runtime.run, codexBinary, "plugin", "marketplace", "remove", marketplaceName(config.PluginID)); err != nil {
				return plan, err
			}
		}
	}
	if config.GlobalPolicy {
		backup, err := removeCodexManagedPolicy(runtime.readFile, config.UserConfigPath)
		if err != nil {
			return plan, err
		}
		if backup != "" {
			plan.Backups = append(plan.Backups, backup)
		}
	}
	if err := removeGeneratedFile(config.PolicyPath); err != nil {
		return plan, err
	}
	if err := removeGeneratedFile(config.ConnectorConfig); err != nil {
		return plan, err
	}
	return plan, nil
}

func removeClaudeSettings(readFile func(string) ([]byte, error), path, pluginID string) (string, error) {
	raw, err := readFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	var settings map[string]interface{}
	if err := json.Unmarshal(raw, &settings); err != nil {
		return "", fmt.Errorf("decode Claude settings: %w", err)
	}
	changed := false
	manifest, _ := Manifest("claude")
	managedTools := make(map[string]bool, len(manifest.Tools))
	for _, tool := range manifest.Tools {
		managedTools[manifest.ClaudeToolPrefix+tool.Name] = true
	}
	if permissions, ok := settings["permissions"].(map[string]interface{}); ok {
		for _, key := range []string{"allow", "ask"} {
			if values, ok := permissions[key].([]interface{}); ok {
				filtered := make([]interface{}, 0, len(values))
				for _, value := range values {
					text, isString := value.(string)
					if isString && managedTools[text] {
						changed = true
						continue
					}
					filtered = append(filtered, value)
				}
				if len(filtered) == 0 {
					delete(permissions, key)
				} else {
					permissions[key] = filtered
				}
			}
		}
		if len(permissions) == 0 {
			delete(settings, "permissions")
		}
	}
	if pluginConfigs, ok := settings["pluginConfigs"].(map[string]interface{}); ok {
		if _, exists := pluginConfigs[pluginID]; exists {
			delete(pluginConfigs, pluginID)
			changed = true
		}
		if len(pluginConfigs) == 0 {
			delete(settings, "pluginConfigs")
		}
	}
	if !changed {
		return "", nil
	}
	return writeJSONConfig(path, settings)
}

func removeCodexManagedPolicy(readFile func(string) ([]byte, error), path string) (string, error) {
	raw, err := readFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	base, present, err := stripManagedCodexPolicy(raw)
	if err != nil || !present {
		return "", err
	}
	base = ensureTrailingNewline(bytes.TrimRight(base, "\r\n"))
	return writeConfigBytes(path, base)
}

func removeGeneratedFile(path string) error {
	if strings.TrimSpace(path) == "" {
		return nil
	}
	target, err := configWriteTarget(path)
	if err != nil {
		return err
	}
	err = os.Remove(target)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

func claudeMarketplaceNamed(ctx context.Context, runner CommandRunner, binary, name string) (bool, error) {
	stdout, stderr, exitCode, err := runner(ctx, binary, "plugin", "marketplace", "list", "--json")
	if err != nil || exitCode != 0 {
		return false, fmt.Errorf("list Claude marketplaces: %s", strings.TrimSpace(firstNonEmpty(stderr, errorText(err))))
	}
	var entries []struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal([]byte(stdout), &entries); err != nil {
		return false, fmt.Errorf("decode Claude marketplace list: %w", err)
	}
	for _, entry := range entries {
		if entry.Name == name {
			return true, nil
		}
	}
	return false, nil
}

func codexMarketplaceNamed(ctx context.Context, runner CommandRunner, binary, name string) (bool, error) {
	stdout, stderr, exitCode, err := runner(ctx, binary, "plugin", "marketplace", "list", "--json")
	if err != nil || exitCode != 0 {
		return false, fmt.Errorf("list Codex marketplaces: %s", strings.TrimSpace(firstNonEmpty(stderr, errorText(err))))
	}
	var result struct {
		Marketplaces []struct {
			Name string `json:"name"`
		} `json:"marketplaces"`
	}
	if err := json.Unmarshal([]byte(stdout), &result); err != nil {
		return false, fmt.Errorf("decode Codex marketplace list: %w", err)
	}
	for _, entry := range result.Marketplaces {
		if entry.Name == name {
			return true, nil
		}
	}
	return false, nil
}

func commandReportsMissing(err error) bool {
	text := strings.ToLower(err.Error())
	return strings.Contains(text, "not installed") || strings.Contains(text, "not found")
}
