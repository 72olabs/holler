package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
)

type OpenCodeSetupConfig struct {
	AttentionMode   string
	NameMode        string
	Actor           string
	Role            string
	Peer            string
	Project         string
	ProjectRoot     string
	Channel         string
	Socket          string
	PluginID        string
	PackageSource   string
	PackageRoot     string
	ProfilePath     string
	ConnectorConfig string
	ServerHostname  string
	ServerPort      int
	ServerUsername  string
	HollerBinary    string
	Apply           bool
}

func SetupOpenCode(_ context.Context, config OpenCodeSetupConfig, _ ...SetupOption) (SetupPlan, error) {
	config = openCodeSetupDefaults(config)
	if err := validateOpenCodeSetup(config); err != nil {
		return SetupPlan{}, err
	}
	manifest, _ := Manifest("opencode")
	plan := SetupPlan{
		SchemaVersion: 1, Harness: "opencode", Applied: config.Apply,
		AttentionMode: config.AttentionMode, NameMode: config.NameMode, PluginID: config.PluginID,
		ConnectorConfigPath: config.ConnectorConfig, PolicyPath: config.ProfilePath,
		PackageRoot: config.PackageRoot, OpenCodeConfigPath: config.ProfilePath,
		Actions: []string{
			"validate and install the versioned Holler OpenCode package",
			"write a connector-owned OpenCode profile with the frozen MCP surface and exact permissions",
			"write the Holler OpenCode connector selection",
		},
		LaunchCommand: []string{"holler", "connector", "launch", "--harness", "opencode"},
		DoctorCommand: []string{"holler", "connector", "doctor", "--harness", "opencode", "--profile", profileForAttention(config.AttentionMode), "--plugin", config.PackageRoot, "--policy", config.ProfilePath, "--attention", config.AttentionMode, "--actor", config.Actor},
		GuidedActions: []string{
			"Install OpenCode " + manifest.MinimumClient + " or newer, then rerun doctor; this source build has not yet completed real-client certification.",
			"The launcher binds OpenCode to a loopback-only random port and generates new HTTP Basic credentials for every run.",
			"Give concurrently running OpenCode sessions distinct actor names unless you intentionally want competing workers for one inbox.",
		},
	}
	if config.AttentionMode == AttentionStartupOnly {
		plan.GuidedActions = append(plan.GuidedActions, "Startup-only preserves durable inbox hydration but cannot satisfy live-review because it has no live wake path.")
	}
	if !config.Apply {
		return plan, nil
	}
	if err := validateOpenCodePackageSource(config.PackageSource, manifest); err != nil {
		return plan, err
	}
	for _, asset := range append([]string{"connector.json"}, manifest.RequiredAssets...) {
		backup, err := copySetupAsset(filepath.Join(config.PackageSource, filepath.FromSlash(asset)), filepath.Join(config.PackageRoot, filepath.FromSlash(asset)))
		if err != nil {
			return plan, err
		}
		if backup != "" {
			plan.Backups = append(plan.Backups, backup)
		}
	}
	profileBackup, err := writeJSONConfig(config.ProfilePath, openCodeProfile(config, manifest))
	if err != nil {
		return plan, err
	}
	if profileBackup != "" {
		plan.Backups = append(plan.Backups, profileBackup)
	}
	connectorBackup, err := writeJSONConfig(config.ConnectorConfig, OpenCodeConnectorConfig{
		SchemaVersion: 1, AttentionMode: config.AttentionMode, NameMode: config.NameMode, PluginID: config.PluginID,
		Actor: config.Actor, Role: config.Role, Peer: config.Peer, Project: config.Project,
		ProjectRoot: config.ProjectRoot, Channel: config.Channel, Socket: config.Socket,
		PackageRoot: config.PackageRoot, ProfilePath: config.ProfilePath,
		ServerHostname: config.ServerHostname, ServerPort: config.ServerPort, ServerUsername: config.ServerUsername,
	})
	if err != nil {
		return plan, err
	}
	if connectorBackup != "" {
		plan.Backups = append(plan.Backups, connectorBackup)
	}
	return plan, nil
}

func openCodeSetupDefaults(config OpenCodeSetupConfig) OpenCodeSetupConfig {
	config.NameMode = strings.TrimSpace(config.NameMode)
	if strings.TrimSpace(config.AttentionMode) == "" {
		config.AttentionMode = AttentionNativePrompt
	}
	if strings.TrimSpace(config.PluginID) == "" {
		config.PluginID = DefaultOpenCodePluginID
	}
	if strings.TrimSpace(config.Project) == "" {
		config.Project = "default"
	}
	if strings.TrimSpace(config.Channel) == "" {
		config.Channel = "direct"
	}
	if strings.TrimSpace(config.ServerHostname) == "" {
		config.ServerHostname = "127.0.0.1"
	}
	if strings.TrimSpace(config.ServerUsername) == "" {
		config.ServerUsername = "holler"
	}
	if strings.TrimSpace(config.ProjectRoot) == "" {
		config.ProjectRoot, _ = os.Getwd()
	}
	if strings.TrimSpace(config.ConnectorConfig) == "" {
		config.ConnectorConfig = DefaultOpenCodeConnectorConfigPath()
	}
	if strings.TrimSpace(config.PackageRoot) == "" {
		config.PackageRoot = filepath.Join(DefaultOpenCodeConfigHome(), "holler")
	}
	if strings.TrimSpace(config.ProfilePath) == "" {
		config.ProfilePath = filepath.Join(config.PackageRoot, "opencode.json")
	}
	for _, target := range []*string{&config.ProjectRoot, &config.PackageSource, &config.PackageRoot, &config.ProfilePath, &config.ConnectorConfig} {
		if strings.TrimSpace(*target) == "" {
			continue
		}
		if absolute, err := filepath.Abs(*target); err == nil {
			*target = absolute
		}
	}
	return config
}

func validateOpenCodeSetup(config OpenCodeSetupConfig) error {
	if err := ValidateOpenCodeAttentionMode(config.AttentionMode); err != nil {
		return err
	}
	if err := ValidateNameMode(config.NameMode); err != nil {
		return err
	}
	if strings.TrimSpace(config.Actor) == "" {
		return fmt.Errorf("actor is required")
	}
	if strings.TrimSpace(config.HollerBinary) == "" {
		return fmt.Errorf("holler binary path is required")
	}
	if strings.TrimSpace(config.PackageSource) == "" {
		return fmt.Errorf("OpenCode package source is required")
	}
	return validateOpenCodeConnectorConfig(OpenCodeConnectorConfig{
		AttentionMode: config.AttentionMode, PackageRoot: config.PackageRoot, ProfilePath: config.ProfilePath,
		ServerHostname: config.ServerHostname, ServerPort: config.ServerPort,
	})
}

func validateOpenCodePackageSource(root string, expected CapabilityManifest) error {
	raw, err := os.ReadFile(filepath.Join(root, "connector.json"))
	if err != nil {
		return fmt.Errorf("read OpenCode package manifest: %w", err)
	}
	var actual CapabilityManifest
	if err := json.Unmarshal(raw, &actual); err != nil {
		return fmt.Errorf("decode OpenCode package manifest: %w", err)
	}
	if !reflect.DeepEqual(actual, expected) {
		return fmt.Errorf("OpenCode package manifest does not match this holler binary")
	}
	digest, err := PackageHash(root, expected.RequiredAssets)
	if err != nil {
		return err
	}
	if digest != expected.PackageHash {
		return fmt.Errorf("OpenCode package hash %s does not match expected %s", digest, expected.PackageHash)
	}
	return nil
}

func copySetupAsset(source, target string) (string, error) {
	raw, err := os.ReadFile(source)
	if err != nil {
		return "", err
	}
	info, err := os.Stat(source)
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(filepath.Dir(target), 0o700); err != nil {
		return "", err
	}
	backup := ""
	if previous, err := os.ReadFile(target); err == nil {
		backup = target + ".bak"
		if err := os.WriteFile(backup, previous, info.Mode().Perm()); err != nil {
			return "", err
		}
	} else if !os.IsNotExist(err) {
		return "", err
	}
	temporary, err := os.CreateTemp(filepath.Dir(target), ".holler-package-*")
	if err != nil {
		return "", err
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(info.Mode().Perm()); err != nil {
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
	// Package assets are connector-owned executables. Rename the regular-file
	// temporary over target so a pre-existing symlink is replaced, never followed.
	if err := os.Rename(temporaryPath, target); err != nil {
		return "", err
	}
	return backup, nil
}

func openCodeProfile(config OpenCodeSetupConfig, manifest CapabilityManifest) map[string]interface{} {
	permissions := map[string]interface{}{}
	for _, tool := range manifest.Tools {
		mode := "allow"
		if tool.RequiresExplicitApproval {
			mode = "ask"
		}
		permissions[manifest.MCPServerName+"_"+tool.Name] = mode
	}
	environment := map[string]interface{}{}
	for _, name := range []string{"HOLLER_BIN", "HOLLER_SOCKET", "HOLLER_ACTOR", "HOLLER_RUN", "HOLLER_ROLE", "HOLLER_PEER", "HOLLER_PROJECT", "HOLLER_CHANNEL", "HOLLER_NAME_MODE", "HOLLER_LAUNCH_TAG", "HOLLER_TAKEOVER"} {
		environment[name] = "{env:" + name + "}"
	}
	return map[string]interface{}{
		"$schema": "https://opencode.ai/config.json",
		"mcp": map[string]interface{}{
			manifest.MCPServerName: map[string]interface{}{
				"type": "local", "command": []string{filepath.Join(config.PackageRoot, "scripts", "holler"), "mcp"},
				"enabled": true, "environment": environment,
			},
		},
		"permission": permissions,
	}
}
