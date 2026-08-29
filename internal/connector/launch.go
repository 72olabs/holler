package connector

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
	"time"
)

type ClaudeLaunchConfig struct {
	ConnectorConfig ClaudeConnectorConfig
	HollerBinary    string
	ClaudeBinary    string
	ConnectorPath   string
	RunID           string
	ExtraArgs       []string
}

type CodexLaunchConfig struct {
	ConnectorConfig CodexConnectorConfig
	HollerBinary    string
	CodexBinary     string
	ConnectorPath   string
	RunID           string
	ExtraArgs       []string
}

type OpenCodeLaunchConfig struct {
	ConnectorConfig OpenCodeConnectorConfig
	HollerBinary    string
	OpenCodeBinary  string
	ConnectorPath   string
	RunID           string
	ServerPassword  string
	ExtraArgs       []string
}

type LaunchSpec struct {
	Command string            `json:"command"`
	Args    []string          `json:"args"`
	Env     map[string]string `json:"env"`
	RunID   string            `json:"run_id"`
}

func BuildClaudeLaunch(config ClaudeLaunchConfig) (LaunchSpec, error) {
	connectorConfig := config.ConnectorConfig
	if err := ValidateClaudeAttentionMode(connectorConfig.AttentionMode); err != nil {
		return LaunchSpec{}, err
	}
	if strings.TrimSpace(connectorConfig.Actor) == "" {
		return LaunchSpec{}, fmt.Errorf("Claude connector actor is not configured; run holler connector setup")
	}
	if strings.TrimSpace(config.HollerBinary) == "" {
		return LaunchSpec{}, fmt.Errorf("holler binary path is required")
	}
	if strings.TrimSpace(config.ClaudeBinary) == "" {
		config.ClaudeBinary = "claude"
	}
	if strings.TrimSpace(connectorConfig.PluginID) == "" {
		connectorConfig.PluginID = DefaultClaudePluginID
	}
	if strings.TrimSpace(connectorConfig.Project) == "" {
		connectorConfig.Project = "default"
	}
	if strings.TrimSpace(connectorConfig.Channel) == "" {
		connectorConfig.Channel = "direct"
	}
	if strings.TrimSpace(config.RunID) == "" {
		runID, err := newRunID(connectorConfig.Actor)
		if err != nil {
			return LaunchSpec{}, err
		}
		config.RunID = runID
	}
	args := append([]string(nil), config.ExtraArgs...)
	environment := map[string]string{
		"HOLLER_BIN":              config.HollerBinary,
		"HOLLER_ACTOR":            connectorConfig.Actor,
		"HOLLER_RUN":              config.RunID,
		"HOLLER_ROLE":             connectorConfig.Role,
		"HOLLER_PEER":             connectorConfig.Peer,
		"HOLLER_PROJECT":          connectorConfig.Project,
		"HOLLER_CHANNEL":          connectorConfig.Channel,
		"HOLLER_CLAUDE_ATTENTION": connectorConfig.AttentionMode,
	}
	if connectorConfig.Socket != "" {
		environment["HOLLER_SOCKET"] = connectorConfig.Socket
	}
	if config.ConnectorPath != "" {
		environment["HOLLER_CONNECTOR_CONFIG"] = config.ConnectorPath
	}
	return LaunchSpec{Command: config.ClaudeBinary, Args: args, Env: environment, RunID: config.RunID}, nil
}

func BuildCodexLaunch(config CodexLaunchConfig) (LaunchSpec, error) {
	connectorConfig := config.ConnectorConfig
	if err := ValidateCodexAttentionMode(connectorConfig.AttentionMode); err != nil {
		return LaunchSpec{}, err
	}
	if strings.TrimSpace(connectorConfig.Actor) == "" {
		return LaunchSpec{}, fmt.Errorf("Codex connector actor is not configured; run holler connector setup")
	}
	if strings.TrimSpace(connectorConfig.Profile) == "" {
		return LaunchSpec{}, fmt.Errorf("Codex connector profile is not configured; run holler connector setup")
	}
	if strings.TrimSpace(config.HollerBinary) == "" {
		return LaunchSpec{}, fmt.Errorf("holler binary path is required")
	}
	if strings.TrimSpace(config.CodexBinary) == "" {
		config.CodexBinary = "codex"
	}
	if strings.TrimSpace(connectorConfig.PluginID) == "" {
		connectorConfig.PluginID = DefaultCodexPluginID
	}
	if strings.TrimSpace(connectorConfig.Project) == "" {
		connectorConfig.Project = "default"
	}
	if strings.TrimSpace(connectorConfig.Channel) == "" {
		connectorConfig.Channel = "direct"
	}
	if strings.TrimSpace(config.RunID) == "" {
		runID, err := newRunID(connectorConfig.Actor)
		if err != nil {
			return LaunchSpec{}, err
		}
		config.RunID = runID
	}
	for _, arg := range config.ExtraArgs {
		switch {
		case arg == "-p", strings.HasPrefix(arg, "-p="), len(arg) > 2 && strings.HasPrefix(arg, "-p"):
			return LaunchSpec{}, fmt.Errorf("Codex profile and project root are controlled by the Holler connector")
		case arg == "-C", strings.HasPrefix(arg, "-C="), len(arg) > 2 && strings.HasPrefix(arg, "-C"):
			return LaunchSpec{}, fmt.Errorf("Codex profile and project root are controlled by the Holler connector")
		case controlledLongOption(arg, "--profile"), controlledLongOption(arg, "--cd"):
			return LaunchSpec{}, fmt.Errorf("Codex profile and project root are controlled by the Holler connector")
		case controlledLongOption(arg, "--dangerously-bypass-hook-trust"):
			return LaunchSpec{}, fmt.Errorf("the production connector launcher does not bypass Codex hook trust; review and trust the installed hook")
		}
	}
	args := []string{"-p", connectorConfig.Profile}
	if strings.TrimSpace(connectorConfig.ProjectRoot) != "" {
		args = append(args, "-C", connectorConfig.ProjectRoot)
	}
	args = append(args, config.ExtraArgs...)
	environment := map[string]string{
		"HOLLER_BIN":             config.HollerBinary,
		"HOLLER_ACTOR":           connectorConfig.Actor,
		"HOLLER_RUN":             config.RunID,
		"HOLLER_ROLE":            connectorConfig.Role,
		"HOLLER_PEER":            connectorConfig.Peer,
		"HOLLER_PROJECT":         connectorConfig.Project,
		"HOLLER_CHANNEL":         connectorConfig.Channel,
		"HOLLER_CODEX_ATTENTION": connectorConfig.AttentionMode,
	}
	if connectorConfig.Socket != "" {
		environment["HOLLER_SOCKET"] = connectorConfig.Socket
	}
	if config.ConnectorPath != "" {
		environment["HOLLER_CODEX_CONNECTOR_CONFIG"] = config.ConnectorPath
	}
	return LaunchSpec{Command: config.CodexBinary, Args: args, Env: environment, RunID: config.RunID}, nil
}

func BuildOpenCodeLaunch(config OpenCodeLaunchConfig) (LaunchSpec, error) {
	connectorConfig := config.ConnectorConfig
	if err := validateOpenCodeConnectorConfig(connectorConfig); err != nil {
		return LaunchSpec{}, err
	}
	if strings.TrimSpace(connectorConfig.Actor) == "" {
		return LaunchSpec{}, fmt.Errorf("OpenCode connector actor is not configured; run holler connector setup")
	}
	if strings.TrimSpace(config.HollerBinary) == "" {
		return LaunchSpec{}, fmt.Errorf("holler binary path is required")
	}
	if strings.TrimSpace(config.OpenCodeBinary) == "" {
		config.OpenCodeBinary = "opencode"
	}
	if strings.TrimSpace(connectorConfig.Project) == "" {
		connectorConfig.Project = "default"
	}
	if strings.TrimSpace(connectorConfig.Channel) == "" {
		connectorConfig.Channel = "direct"
	}
	if strings.TrimSpace(connectorConfig.ServerUsername) == "" {
		connectorConfig.ServerUsername = "holler"
	}
	if strings.TrimSpace(config.RunID) == "" {
		runID, err := newRunID(connectorConfig.Actor)
		if err != nil {
			return LaunchSpec{}, err
		}
		config.RunID = runID
	}
	if connectorConfig.AttentionMode == AttentionNativePrompt && config.ServerPassword == "" {
		secret := make([]byte, 24)
		if _, err := rand.Read(secret); err != nil {
			return LaunchSpec{}, fmt.Errorf("generate OpenCode server password: %w", err)
		}
		config.ServerPassword = hex.EncodeToString(secret)
	}
	for _, arg := range config.ExtraArgs {
		if arg == "--hostname" || arg == "--port" || strings.HasPrefix(arg, "--hostname=") || strings.HasPrefix(arg, "--port=") {
			return LaunchSpec{}, fmt.Errorf("OpenCode server binding is controlled by the Holler connector")
		}
		if controlledLongOption(arg, "--mini") {
			return LaunchSpec{}, fmt.Errorf("OpenCode mini mode cannot expose the HTTP attention endpoint required by the Holler connector")
		}
	}
	args := make([]string, 0, len(config.ExtraArgs)+5)
	if connectorConfig.AttentionMode == AttentionNativePrompt {
		// An explicit --port 0 asks OpenCode to let the OS bind a free port
		// atomically. The plugin receives the resulting serverUrl from the
		// host and passes that exact URL to the lifecycle hook.
		args = append(args, "--hostname", connectorConfig.ServerHostname, "--port", "0")
	}
	if strings.TrimSpace(connectorConfig.ProjectRoot) != "" {
		args = append(args, connectorConfig.ProjectRoot)
	}
	args = append(args, config.ExtraArgs...)
	environment := map[string]string{
		"HOLLER_BIN":                       config.HollerBinary,
		"HOLLER_ACTOR":                     connectorConfig.Actor,
		"HOLLER_RUN":                       config.RunID,
		"HOLLER_ROLE":                      connectorConfig.Role,
		"HOLLER_PEER":                      connectorConfig.Peer,
		"HOLLER_PROJECT":                   connectorConfig.Project,
		"HOLLER_CHANNEL":                   connectorConfig.Channel,
		"HOLLER_OPENCODE_ATTENTION":        connectorConfig.AttentionMode,
		"HOLLER_OPENCODE_CONNECTOR_CONFIG": config.ConnectorPath,
		"OPENCODE_CONFIG":                  connectorConfig.ProfilePath,
		"OPENCODE_CONFIG_DIR":              connectorConfig.PackageRoot,
	}
	if connectorConfig.AttentionMode == AttentionNativePrompt {
		environment["OPENCODE_SERVER_USERNAME"] = connectorConfig.ServerUsername
		environment["OPENCODE_SERVER_PASSWORD"] = config.ServerPassword
	}
	if connectorConfig.Socket != "" {
		environment["HOLLER_SOCKET"] = connectorConfig.Socket
	}
	return LaunchSpec{Command: config.OpenCodeBinary, Args: args, Env: environment, RunID: config.RunID}, nil
}

func controlledLongOption(argument, name string) bool {
	return argument == name || strings.HasPrefix(argument, name+"=")
}

func newRunID(actor string) (string, error) {
	random := make([]byte, 8)
	if _, err := rand.Read(random); err != nil {
		return "", fmt.Errorf("generate run identity: %w", err)
	}
	return fmt.Sprintf("%s-%d-%s", actor, time.Now().UTC().Unix(), hex.EncodeToString(random)), nil
}
