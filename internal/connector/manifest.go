package connector

import (
	"errors"
	"fmt"
	"strings"

	"github.com/72olabs/holler/internal/api"
	"github.com/72olabs/holler/internal/mcp"
)

const ConnectorVersion = "0.1.0"

type ReadinessState string

const (
	StateDiscovered            ReadinessState = "DISCOVERED"
	StateInstalled             ReadinessState = "INSTALLED"
	StateConfigured            ReadinessState = "CONFIGURED"
	StateRegistered            ReadinessState = "REGISTERED"
	StateReady                 ReadinessState = "READY"
	StateDegraded              ReadinessState = "DEGRADED"
	StateStale                 ReadinessState = "STALE"
	StateIncompatible          ReadinessState = "INCOMPATIBLE"
	StateAuthorizationRequired ReadinessState = "AUTHORIZATION_REQUIRED"
)

type CapabilityProfile struct {
	Name         string `json:"name"`
	RequiresWake bool   `json:"requires_wake"`
	Description  string `json:"description"`
}

type ToolPermission struct {
	Name  string `json:"name"`
	Class string `json:"class"`
}

type CapabilityManifest struct {
	SchemaVersion        int                 `json:"schema_version"`
	Connector            string              `json:"connector"`
	ConnectorVersion     string              `json:"connector_version"`
	ProtocolVersion      int                 `json:"protocol_version"`
	Harness              string              `json:"harness"`
	PluginName           string              `json:"plugin_name"`
	PluginID             string              `json:"plugin_id"`
	ClientCommand        string              `json:"client_command"`
	MinimumClient        string              `json:"minimum_client_version"`
	TestedClient         string              `json:"tested_client_version"`
	LifecycleEvents      []string            `json:"lifecycle_events"`
	NotificationMode     string              `json:"notification_mode"`
	NotificationFallback string              `json:"notification_fallback"`
	AttentionModes       []string            `json:"attention_modes,omitempty"`
	RequiredAssets       []string            `json:"required_assets"`
	PackageHash          string              `json:"package_hash"`
	ToolSurfaceHash      string              `json:"tool_surface_hash"`
	MCPServerName        string              `json:"mcp_server_name"`
	ClaudeToolPrefix     string              `json:"claude_tool_prefix,omitempty"`
	Tools                []ToolPermission    `json:"tools"`
	Profiles             []CapabilityProfile `json:"profiles"`
}

var commonTools = []ToolPermission{
	{Name: "bus_send", Class: "idempotent-write"},
	{Name: "bus_check_inbox", Class: "read-only"},
	{Name: "bus_claim", Class: "leased-write"},
	{Name: "bus_inbox", Class: "leased-write"},
	{Name: "bus_ack", Class: "idempotent-write"},
	{Name: "bus_extend", Class: "idempotent-write"},
	{Name: "bus_nack", Class: "leased-write"},
	{Name: "bus_status", Class: "read-only"},
	{Name: "holler_profile", Class: "idempotent-write"},
	{Name: "holler_who", Class: "read-only"},
}

var commonProfiles = []CapabilityProfile{
	{Name: "async-peer", RequiresWake: false, Description: "Durable messaging with visible polling fallback."},
	{Name: "live-review", RequiresWake: true, Description: "Durable messaging plus a certified live attention path."},
}

func Manifest(harness string) (CapabilityManifest, error) {
	harness = strings.ToLower(strings.TrimSpace(harness))
	base := CapabilityManifest{
		SchemaVersion: 1, Connector: "holler", ConnectorVersion: ConnectorVersion,
		ProtocolVersion: api.ProtocolVersion, Harness: harness, PluginName: "holler",
		MCPServerName:   "holler",
		ToolSurfaceHash: mcp.ToolSurfaceHash(), Tools: append([]ToolPermission(nil), commonTools...),
		Profiles: append([]CapabilityProfile(nil), commonProfiles...),
	}
	switch harness {
	case "codex":
		base.PluginID = "holler@holler"
		base.ClientCommand = "codex"
		base.MinimumClient = "0.149.1"
		base.TestedClient = "0.150.1"
		base.LifecycleEvents = []string{"startup", "resume", "clear", "compact", "end"}
		base.NotificationMode = "native-queue"
		base.NotificationFallback = "polling"
		base.AttentionModes = []string{AttentionNativeQueue, AttentionStartupOnly}
		base.RequiredAssets = []string{
			".codex-plugin/plugin.json", ".mcp.json", "hooks/hooks.json",
			"scripts/holler", "skills/holler/SKILL.md",
			"skills/holler-setup/SKILL.md",
		}
		base.PackageHash = "sha256:6c7d26878fee77f926c245cb65f0839c84724b6aaed18b87f66caf2fd575be06"
	case "claude":
		base.PluginID = DefaultClaudePluginID
		base.ClientCommand = "claude"
		base.MinimumClient = "2.1.247"
		base.TestedClient = "2.1.251"
		base.LifecycleEvents = []string{"startup", "resume", "clear", "compact", "fork", "stop", "stop_failure", "end"}
		base.NotificationMode = AttentionHookLongPoll
		base.NotificationFallback = "startup-only"
		base.AttentionModes = []string{AttentionHookLongPoll, AttentionStartupOnly}
		base.ClaudeToolPrefix = "mcp__plugin_" + base.PluginName + "_" + base.MCPServerName + "__"
		base.RequiredAssets = []string{
			".claude-plugin/plugin.json", ".mcp.json", "hooks/hooks.json",
			"scripts/holler", "skills/holler/SKILL.md",
			"skills/holler-setup/SKILL.md",
		}
		base.PackageHash = "sha256:88ba5e93422c6000c173327653da35a5e5962ab6a5502727877ad8fd9c2f9bee"
	case "opencode":
		base.PluginID = DefaultOpenCodePluginID
		base.ClientCommand = "opencode"
		base.MinimumClient = "1.1.1"
		base.TestedClient = "pending-live-certification"
		base.LifecycleEvents = []string{"created", "first-message", "compact", "deleted"}
		base.NotificationMode = AttentionNativePrompt
		base.NotificationFallback = AttentionStartupOnly
		base.AttentionModes = []string{AttentionNativePrompt, AttentionStartupOnly}
		base.RequiredAssets = []string{
			"plugins/holler.js", "scripts/holler",
			"skills/holler/SKILL.md", "skills/holler-setup/SKILL.md",
		}
		base.PackageHash = "sha256:f6d8cb9174dc7085f9c654beebcf3d70f0c0f0cec096ab614289f28e444a62d9"
	default:
		return CapabilityManifest{}, fmt.Errorf("unsupported harness %q: %w", harness, errors.New("expected codex, claude, or opencode"))
	}
	return base, nil
}

func (manifest CapabilityManifest) Profile(name string) (CapabilityProfile, bool) {
	for _, profile := range manifest.Profiles {
		if profile.Name == name {
			return profile, true
		}
	}
	return CapabilityProfile{}, false
}
