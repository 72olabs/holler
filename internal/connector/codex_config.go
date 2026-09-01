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
	AttentionNativeQueue = "native-queue"
	DefaultCodexPluginID = "holler@holler"
)

type CodexConnectorConfig struct {
	SchemaVersion int    `json:"schema_version"`
	AttentionMode string `json:"attention_mode"`
	PluginID      string `json:"plugin_id,omitempty"`
	Profile       string `json:"profile"`
	Actor         string `json:"actor,omitempty"`
	NameMode      string `json:"name_mode,omitempty"`
	Role          string `json:"role,omitempty"`
	Peer          string `json:"peer,omitempty"`
	Project       string `json:"project,omitempty"`
	ProjectRoot   string `json:"project_root,omitempty"`
	Channel       string `json:"channel,omitempty"`
	Socket        string `json:"socket,omitempty"`
	HollerBinary  string `json:"holler_binary,omitempty"`
	ClientBinary  string `json:"client_binary,omitempty"`
}

func ValidateCodexAttentionMode(mode string) error {
	switch strings.TrimSpace(mode) {
	case AttentionNativeQueue, AttentionStartupOnly:
		return nil
	default:
		return fmt.Errorf("unsupported Codex attention mode %q (expected native-queue or startup-only)", mode)
	}
}

func DefaultCodexConnectorConfigPath() string {
	if configured := strings.TrimSpace(os.Getenv("HOLLER_CODEX_CONNECTOR_CONFIG")); configured != "" {
		return configured
	}
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return filepath.Join(".holler", "connectors", "codex.json")
	}
	return filepath.Join(home, ".holler", "connectors", "codex.json")
}

func LoadCodexConnectorConfig(path string) (CodexConnectorConfig, error) {
	if strings.TrimSpace(path) == "" {
		path = DefaultCodexConnectorConfigPath()
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return CodexConnectorConfig{}, err
	}
	var config CodexConnectorConfig
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&config); err != nil {
		return CodexConnectorConfig{}, fmt.Errorf("decode Codex connector config: %w", err)
	}
	if config.SchemaVersion != 1 {
		return CodexConnectorConfig{}, fmt.Errorf("unsupported Codex connector config schema %d", config.SchemaVersion)
	}
	if err := ValidateCodexAttentionMode(config.AttentionMode); err != nil {
		return CodexConnectorConfig{}, err
	}
	if strings.TrimSpace(config.Profile) == "" {
		return CodexConnectorConfig{}, fmt.Errorf("Codex connector profile is required")
	}
	if err := ValidateNameMode(config.NameMode); err != nil {
		return CodexConnectorConfig{}, err
	}
	config.NameMode = strings.TrimSpace(config.NameMode)
	return config, nil
}

func ResolveCodexAttentionMode() (string, error) {
	if value := strings.TrimSpace(os.Getenv("HOLLER_CODEX_ATTENTION")); value != "" {
		if err := ValidateCodexAttentionMode(value); err != nil {
			return "", err
		}
		return value, nil
	}
	config, err := LoadCodexConnectorConfig("")
	if err == nil {
		return config.AttentionMode, nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return AttentionNativeQueue, nil
	}
	return "", err
}
