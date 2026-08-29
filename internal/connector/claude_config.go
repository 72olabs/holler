package connector

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const (
	AttentionHookLongPoll = "hook-long-poll"
	AttentionStartupOnly  = "startup-only"
)

type ClaudeConnectorConfig struct {
	SchemaVersion int    `json:"schema_version"`
	AttentionMode string `json:"attention_mode"`
	// LegacyChannelActivation is retained only so existing research-era configuration
	// files continue to decode. The shipping Claude connector does not use it.
	LegacyChannelActivation string `json:"channel_activation,omitempty"`
	PluginID                string `json:"plugin_id,omitempty"`
	Actor                   string `json:"actor,omitempty"`
	Role                    string `json:"role,omitempty"`
	Peer                    string `json:"peer,omitempty"`
	Project                 string `json:"project,omitempty"`
	Channel                 string `json:"channel,omitempty"`
	Socket                  string `json:"socket,omitempty"`
	HollerBinary            string `json:"holler_binary,omitempty"`
}

func ValidateClaudeAttentionMode(mode string) error {
	switch strings.TrimSpace(mode) {
	case AttentionHookLongPoll, AttentionStartupOnly:
		return nil
	default:
		return fmt.Errorf("unsupported Claude attention mode %q (expected hook-long-poll or startup-only)", mode)
	}
}

func DefaultClaudeConnectorConfigPath() string {
	if configured := strings.TrimSpace(os.Getenv("HOLLER_CONNECTOR_CONFIG")); configured != "" {
		return configured
	}
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return filepath.Join(".holler", "connectors", "claude.json")
	}
	return filepath.Join(home, ".holler", "connectors", "claude.json")
}

func LoadClaudeConnectorConfig(path string) (ClaudeConnectorConfig, error) {
	if strings.TrimSpace(path) == "" {
		path = DefaultClaudeConnectorConfigPath()
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return ClaudeConnectorConfig{}, err
	}
	var config ClaudeConnectorConfig
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&config); err != nil {
		return ClaudeConnectorConfig{}, fmt.Errorf("decode Claude connector config: %w", err)
	}
	if config.SchemaVersion != 1 {
		return ClaudeConnectorConfig{}, fmt.Errorf("unsupported Claude connector config schema %d", config.SchemaVersion)
	}
	if err := ValidateClaudeAttentionMode(config.AttentionMode); err != nil {
		return ClaudeConnectorConfig{}, err
	}
	return config, nil
}

func ResolveClaudeAttentionMode() (string, error) {
	for _, name := range []string{"HOLLER_CLAUDE_ATTENTION", "CLAUDE_PLUGIN_OPTION_ATTENTION_MODE"} {
		if value := strings.TrimSpace(os.Getenv(name)); value != "" {
			if err := ValidateClaudeAttentionMode(value); err != nil {
				return "", err
			}
			return value, nil
		}
	}
	config, err := LoadClaudeConnectorConfig("")
	if err == nil {
		return config.AttentionMode, nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return AttentionHookLongPoll, nil
	}
	return "", err
}
