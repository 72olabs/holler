package connector

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
)

const (
	AttentionNativePrompt   = "native-prompt"
	DefaultOpenCodePluginID = "holler"
)

type OpenCodeConnectorConfig struct {
	SchemaVersion  int    `json:"schema_version"`
	AttentionMode  string `json:"attention_mode"`
	PluginID       string `json:"plugin_id,omitempty"`
	Actor          string `json:"actor,omitempty"`
	NameMode       string `json:"name_mode,omitempty"`
	Role           string `json:"role,omitempty"`
	Peer           string `json:"peer,omitempty"`
	Project        string `json:"project,omitempty"`
	ProjectRoot    string `json:"project_root,omitempty"`
	Channel        string `json:"channel,omitempty"`
	Socket         string `json:"socket,omitempty"`
	PackageRoot    string `json:"package_root"`
	ProfilePath    string `json:"profile_path"`
	ServerHostname string `json:"server_hostname"`
	ServerPort     int    `json:"server_port,omitempty"`
	ServerUsername string `json:"server_username,omitempty"`
}

func ValidateOpenCodeAttentionMode(mode string) error {
	switch strings.TrimSpace(mode) {
	case AttentionNativePrompt, AttentionStartupOnly:
		return nil
	default:
		return fmt.Errorf("unsupported OpenCode attention mode %q (expected native-prompt or startup-only)", mode)
	}
}

func DefaultOpenCodeConnectorConfigPath() string {
	if configured := strings.TrimSpace(os.Getenv("HOLLER_OPENCODE_CONNECTOR_CONFIG")); configured != "" {
		return configured
	}
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return filepath.Join(".holler", "connectors", "opencode.json")
	}
	return filepath.Join(home, ".holler", "connectors", "opencode.json")
}

func DefaultOpenCodeConfigHome() string {
	if configured := strings.TrimSpace(os.Getenv("XDG_CONFIG_HOME")); configured != "" {
		return filepath.Join(configured, "opencode")
	}
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return filepath.Join(".config", "opencode")
	}
	return filepath.Join(home, ".config", "opencode")
}

func LoadOpenCodeConnectorConfig(path string) (OpenCodeConnectorConfig, error) {
	if strings.TrimSpace(path) == "" {
		path = DefaultOpenCodeConnectorConfigPath()
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return OpenCodeConnectorConfig{}, err
	}
	var config OpenCodeConnectorConfig
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&config); err != nil {
		return OpenCodeConnectorConfig{}, fmt.Errorf("decode OpenCode connector config: %w", err)
	}
	if config.SchemaVersion != 1 {
		return OpenCodeConnectorConfig{}, fmt.Errorf("unsupported OpenCode connector config schema %d", config.SchemaVersion)
	}
	if err := validateOpenCodeConnectorConfig(config); err != nil {
		return OpenCodeConnectorConfig{}, err
	}
	config.NameMode = strings.TrimSpace(config.NameMode)
	return config, nil
}

func ResolveOpenCodeAttentionMode() (string, error) {
	if value := strings.TrimSpace(os.Getenv("HOLLER_OPENCODE_ATTENTION")); value != "" {
		if err := ValidateOpenCodeAttentionMode(value); err != nil {
			return "", err
		}
		return value, nil
	}
	config, err := LoadOpenCodeConnectorConfig("")
	if err == nil {
		return config.AttentionMode, nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return AttentionNativePrompt, nil
	}
	return "", err
}

func validateOpenCodeConnectorConfig(config OpenCodeConnectorConfig) error {
	if err := ValidateOpenCodeAttentionMode(config.AttentionMode); err != nil {
		return err
	}
	if err := ValidateNameMode(config.NameMode); err != nil {
		return err
	}
	if strings.TrimSpace(config.PackageRoot) == "" || strings.TrimSpace(config.ProfilePath) == "" {
		return fmt.Errorf("OpenCode package root and profile path are required")
	}
	host := strings.TrimSpace(config.ServerHostname)
	if host == "" {
		return fmt.Errorf("OpenCode server hostname is required")
	}
	if host != "localhost" {
		ip := net.ParseIP(host)
		if ip == nil || !ip.IsLoopback() {
			return fmt.Errorf("OpenCode server hostname must be loopback")
		}
	}
	if config.ServerPort != 0 {
		return fmt.Errorf("OpenCode server port must be zero so the OS can bind an available loopback port atomically")
	}
	return nil
}
