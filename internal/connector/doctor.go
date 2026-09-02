package connector

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	pathpkg "path"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/72olabs/holler/internal/api"
	"github.com/72olabs/holler/internal/buildinfo"
	"github.com/pelletier/go-toml/v2"
)

type CheckStatus string

const (
	CheckPass CheckStatus = "PASS"
	CheckWarn CheckStatus = "WARN"
	CheckFail CheckStatus = "FAIL"
)

type DiagnosticCheck struct {
	ID          string            `json:"id"`
	Layer       string            `json:"layer"`
	Status      CheckStatus       `json:"status"`
	Summary     string            `json:"summary"`
	Evidence    map[string]string `json:"evidence,omitempty"`
	Remediation string            `json:"remediation,omitempty"`
}

type DoctorReport struct {
	SchemaVersion    int               `json:"schema_version"`
	Harness          string            `json:"harness"`
	Profile          string            `json:"profile"`
	State            ReadinessState    `json:"state"`
	Ready            bool              `json:"ready"`
	ConnectorVersion string            `json:"connector_version"`
	ProtocolVersion  int               `json:"protocol_version"`
	ToolSurfaceHash  string            `json:"tool_surface_hash"`
	ProjectRoot      string            `json:"project_root"`
	PluginRoot       string            `json:"plugin_root,omitempty"`
	Checks           []DiagnosticCheck `json:"checks"`
}

type DoctorConfig struct {
	Harness       string
	Profile       string
	ProjectRoot   string
	PluginRoot    string
	PolicyPath    string
	SocketPath    string
	Actor         string
	RunID         string
	ClientBinary  string
	AttentionMode string
}

type doctorRuntime struct {
	run      CommandRunner
	lookPath func(string) (string, error)
	readFile func(string) ([]byte, error)
	stat     func(string) (os.FileInfo, error)
	getwd    func() (string, error)
	homeDir  func() (string, error)
	getenv   func(string) string
}

type DoctorOption func(*doctorRuntime)

func WithDoctorCommandRunner(runner CommandRunner) DoctorOption {
	return func(runtime *doctorRuntime) { runtime.run = runner }
}

func WithDoctorLookPath(lookPath func(string) (string, error)) DoctorOption {
	return func(runtime *doctorRuntime) { runtime.lookPath = lookPath }
}

func Doctor(ctx context.Context, config DoctorConfig, options ...DoctorOption) (DoctorReport, error) {
	manifest, err := Manifest(config.Harness)
	if err != nil {
		return DoctorReport{}, err
	}
	if strings.TrimSpace(config.Profile) == "" {
		config.Profile = "async-peer"
	}
	profile, ok := manifest.Profile(config.Profile)
	if !ok {
		return DoctorReport{}, fmt.Errorf("unsupported connector profile %q", config.Profile)
	}
	runtime := &doctorRuntime{
		run: runCommand, lookPath: exec.LookPath, readFile: os.ReadFile, stat: os.Stat,
		getwd: os.Getwd, homeDir: os.UserHomeDir, getenv: os.Getenv,
	}
	for _, option := range options {
		option(runtime)
	}
	report := DoctorReport{
		SchemaVersion: 1, Harness: manifest.Harness, Profile: profile.Name,
		State: StateDiscovered, ConnectorVersion: manifest.ConnectorVersion,
		ProtocolVersion: manifest.ProtocolVersion, ToolSurfaceHash: manifest.ToolSurfaceHash,
	}

	clientOK := checkClient(ctx, runtime, &config, manifest, &report)
	projectOK := checkProject(ctx, runtime, &config, &report)
	packageState := checkPackage(ctx, runtime, &config, manifest, &report)
	policyOK := checkPolicy(runtime, &config, manifest, &report)
	wakeOK := checkWake(ctx, runtime, config, manifest, profile, &report)
	daemonOK := checkDaemon(ctx, config, &report)

	switch {
	case !clientOK:
		report.State = StateIncompatible
	case !projectOK || packageState == packageMissing:
		report.State = StateDiscovered
	case packageState == packageChanged || !policyOK:
		report.State = StateAuthorizationRequired
	case !daemonOK || !wakeOK:
		report.State = StateDegraded
	default:
		report.State = StateConfigured
	}
	report.Ready = false // READY requires a live real-client certification, never static inspection.
	return report, nil
}

func checkClient(ctx context.Context, runtime *doctorRuntime, config *DoctorConfig, manifest CapabilityManifest, report *DoctorReport) bool {
	command := strings.TrimSpace(config.ClientBinary)
	if command == "" {
		command = manifest.ClientCommand
	}
	path, err := runtime.lookPath(command)
	if err != nil {
		report.add(CheckFail, "client.executable", "discovery", "harness executable was not found", nil,
			"install "+manifest.ClientCommand+" or pass --client-binary")
		return false
	}
	config.ClientBinary = path
	stdout, stderr, exitCode, runErr := runtime.run(ctx, path, "--version")
	versionText := strings.TrimSpace(firstNonEmpty(stdout, stderr))
	version := extractVersion(versionText)
	if runErr != nil || exitCode != 0 || version == "" {
		report.add(CheckFail, "client.version", "discovery", "could not determine harness version",
			map[string]string{"path": path, "output": versionText}, "run "+manifest.ClientCommand+" --version")
		return false
	}
	if compareVersions(version, manifest.MinimumClient) < 0 {
		report.add(CheckFail, "client.version", "discovery", "harness version is below the supported minimum",
			map[string]string{"path": path, "version": version, "minimum": manifest.MinimumClient},
			"upgrade "+manifest.ClientCommand+" to "+manifest.MinimumClient+" or newer")
		return false
	}
	report.add(CheckPass, "client.version", "discovery", "supported harness version found",
		map[string]string{"path": path, "version": version, "minimum": manifest.MinimumClient, "tested": manifest.TestedClient}, "")
	return true
}

func checkProject(ctx context.Context, runtime *doctorRuntime, config *DoctorConfig, report *DoctorReport) bool {
	root := strings.TrimSpace(config.ProjectRoot)
	if root == "" {
		value, err := runtime.getwd()
		if err != nil {
			report.add(CheckFail, "project.root", "discovery", "could not resolve the current directory", nil, "pass --project explicitly")
			return false
		}
		root = value
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		report.add(CheckFail, "project.root", "discovery", "project path is invalid", map[string]string{"path": root}, "pass an absolute project directory")
		return false
	}
	info, err := runtime.stat(abs)
	if err != nil || !info.IsDir() {
		report.add(CheckFail, "project.root", "discovery", "project directory does not exist", map[string]string{"path": abs}, "create the directory or pass --project")
		return false
	}
	if resolved, err := filepath.EvalSymlinks(abs); err == nil {
		abs = resolved
	}
	report.ProjectRoot = abs
	stdout, stderr, exitCode, runErr := runtime.run(ctx, "git", "-C", abs, "rev-parse", "--show-toplevel")
	gitRoot := strings.TrimSpace(stdout)
	if runErr != nil || exitCode != 0 || gitRoot == "" {
		report.add(CheckFail, "project.repository", "discovery", "project is not an independently discoverable Git root",
			map[string]string{"path": abs, "detail": strings.TrimSpace(stderr)}, "initialize the intended workspace as a Git repository")
		return false
	}
	resolvedGitRoot, _ := filepath.Abs(gitRoot)
	if resolved, err := filepath.EvalSymlinks(resolvedGitRoot); err == nil {
		resolvedGitRoot = resolved
	}
	if filepath.Clean(resolvedGitRoot) != filepath.Clean(abs) {
		report.add(CheckFail, "project.repository", "discovery", "harness would discover a different repository root",
			map[string]string{"requested": abs, "discovered": resolvedGitRoot}, "pass the discovered root or initialize the intended workspace as its own repository")
		return false
	}
	report.add(CheckPass, "project.repository", "discovery", "effective repository root matches the requested project",
		map[string]string{"root": abs}, "")
	return true
}

type packageCheck int

const (
	packageMissing packageCheck = iota
	packageChanged
	packageOK
)

func checkPackage(ctx context.Context, runtime *doctorRuntime, config *DoctorConfig, expected CapabilityManifest, report *DoctorReport) packageCheck {
	root := strings.TrimSpace(config.PluginRoot)
	source := "explicit"
	if root == "" {
		var err error
		root, err = discoverPluginRoot(ctx, runtime, config, expected)
		if err != nil {
			report.add(CheckFail, "package.installed", "installation", "connector plugin is not installed or enabled",
				map[string]string{"plugin": expected.PluginID, "detail": err.Error()}, "install and enable the connector plugin, then rerun doctor")
			return packageMissing
		}
		source = "installed"
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		return packageMissing
	}
	report.PluginRoot = abs
	manifestPath := filepath.Join(abs, "connector.json")
	raw, err := runtime.readFile(manifestPath)
	if err != nil {
		report.add(CheckFail, "package.manifest", "installation", "connector capability manifest is missing",
			map[string]string{"path": manifestPath}, "reinstall the connector package")
		return packageChanged
	}
	var installed CapabilityManifest
	if err := json.Unmarshal(raw, &installed); err != nil {
		report.add(CheckFail, "package.manifest", "installation", "connector capability manifest is invalid JSON",
			map[string]string{"path": manifestPath, "detail": err.Error()}, "reinstall the connector package")
		return packageChanged
	}
	if !reflect.DeepEqual(installed, expected) {
		report.add(CheckFail, "package.manifest", "installation", "connector manifest does not match this binary",
			map[string]string{"path": manifestPath, "expected_tool_surface": expected.ToolSurfaceHash, "installed_tool_surface": installed.ToolSurfaceHash},
			"install the connector package built for holler "+ConnectorVersion)
		return packageChanged
	}
	digest, err := PackageHash(abs, expected.RequiredAssets)
	if err != nil {
		report.add(CheckFail, "package.assets", "installation", "a required connector asset is missing or unreadable",
			map[string]string{"root": abs, "detail": err.Error()}, "reinstall the connector package")
		return packageChanged
	}
	if digest != expected.PackageHash {
		report.add(CheckFail, "package.assets", "installation", "connector package content hash does not match its manifest",
			map[string]string{"root": abs, "expected": expected.PackageHash, "actual": digest}, "reinstall and reauthorize the connector package")
		return packageChanged
	}
	launcher, err := runtime.stat(filepath.Join(abs, "scripts", "holler"))
	if err != nil || launcher.Mode()&0o111 == 0 {
		report.add(CheckFail, "package.launcher", "installation", "connector launcher is not executable",
			map[string]string{"path": filepath.Join(abs, "scripts", "holler")}, "reinstall the connector package with executable permissions preserved")
		return packageChanged
	}
	report.add(CheckPass, "package.assets", "installation", "connector manifest and required assets match",
		map[string]string{"root": abs, "source": source, "package_hash": digest, "tool_surface_hash": expected.ToolSurfaceHash}, "")
	return packageOK
}

func checkPolicy(runtime *doctorRuntime, config *DoctorConfig, manifest CapabilityManifest, report *DoctorReport) bool {
	path := strings.TrimSpace(config.PolicyPath)
	if path == "" {
		if manifest.Harness == "codex" {
			if codexHome := strings.TrimSpace(runtime.getenv("CODEX_HOME")); codexHome != "" {
				path = filepath.Join(codexHome, "config.toml")
			}
		} else if manifest.Harness == "claude" && strings.TrimSpace(runtime.getenv("CLAUDE_CONFIG_DIR")) != "" {
			claudeHome := strings.TrimSpace(runtime.getenv("CLAUDE_CONFIG_DIR"))
			path = filepath.Join(claudeHome, "settings.json")
		} else if manifest.Harness == "opencode" && strings.TrimSpace(runtime.getenv("XDG_CONFIG_HOME")) != "" {
			path = filepath.Join(strings.TrimSpace(runtime.getenv("XDG_CONFIG_HOME")), "opencode", "holler", "opencode.json")
		}
		if path == "" {
			home, err := runtime.homeDir()
			if err == nil {
				if manifest.Harness == "codex" {
					path = filepath.Join(home, ".codex", "config.toml")
				} else if manifest.Harness == "claude" {
					path = filepath.Join(home, ".claude", "settings.json")
				} else {
					path = filepath.Join(home, ".config", "opencode", "holler", "opencode.json")
				}
			}
		}
	}
	raw, err := runtime.readFile(path)
	if err != nil {
		report.add(CheckFail, "authorization.tool_policy", "authorization", "operator tool policy is missing",
			map[string]string{"path": path}, "install the least-privilege policy example in an operator-controlled config layer")
		return false
	}
	var missing []string
	if manifest.Harness == "codex" {
		var settings struct {
			Plugins map[string]struct {
				MCPServers map[string]struct {
					Enabled                  bool     `toml:"enabled"`
					Required                 bool     `toml:"required"`
					DefaultToolsApprovalMode string   `toml:"default_tools_approval_mode"`
					EnabledTools             []string `toml:"enabled_tools"`
					Tools                    map[string]struct {
						ApprovalMode string `toml:"approval_mode"`
					} `toml:"tools"`
				} `toml:"mcp_servers"`
			} `toml:"plugins"`
		}
		if err := toml.Unmarshal(raw, &settings); err != nil {
			report.add(CheckFail, "authorization.tool_policy", "authorization", "Codex policy is invalid TOML",
				map[string]string{"path": path, "detail": err.Error()}, "fix the operator-controlled config file")
			return false
		}
		plugin, ok := settings.Plugins[manifest.PluginID]
		server, serverOK := plugin.MCPServers[manifest.MCPServerName]
		if !ok || !serverOK {
			missing = append(missing, "plugins."+manifest.PluginID+".mcp_servers."+manifest.MCPServerName)
		} else {
			if !server.Enabled {
				missing = append(missing, "enabled=true")
			}
			if !server.Required {
				missing = append(missing, "required=true")
			}
			if server.DefaultToolsApprovalMode != "approve" {
				missing = append(missing, "default_tools_approval_mode=approve")
			}
			enabled := make(map[string]struct{}, len(server.EnabledTools))
			for _, name := range server.EnabledTools {
				enabled[name] = struct{}{}
			}
			for _, tool := range manifest.Tools {
				if _, present := enabled[tool.Name]; !present {
					missing = append(missing, tool.Name)
				}
				expected := "approve"
				if tool.RequiresExplicitApproval {
					expected = "prompt"
				}
				if server.Tools[tool.Name].ApprovalMode != expected {
					missing = append(missing, "tools."+tool.Name+".approval_mode="+expected)
				}
			}
		}
	} else if manifest.Harness == "claude" {
		var settings struct {
			Permissions struct {
				Allow []string `json:"allow"`
				Ask   []string `json:"ask"`
				Deny  []string `json:"deny"`
			} `json:"permissions"`
		}
		if err := json.Unmarshal(raw, &settings); err != nil {
			report.add(CheckFail, "authorization.tool_policy", "authorization", "Claude policy is invalid JSON",
				map[string]string{"path": path, "detail": err.Error()}, "fix the operator-controlled settings file")
			return false
		}
		allowed := make(map[string]struct{}, len(settings.Permissions.Allow))
		for _, name := range settings.Permissions.Allow {
			allowed[name] = struct{}{}
		}
		asked := make(map[string]struct{}, len(settings.Permissions.Ask))
		for _, name := range settings.Permissions.Ask {
			asked[name] = struct{}{}
		}
		for _, tool := range manifest.Tools {
			name := manifest.ClaudeToolPrefix + tool.Name
			serverName := strings.TrimSuffix(manifest.ClaudeToolPrefix, "__")
			if tool.RequiresExplicitApproval {
				if _, ok := asked[name]; !ok {
					missing = append(missing, "permissions.ask:"+name)
				}
				if _, ok := allowed[name]; ok {
					missing = append(missing, "permissions.allow:"+name+" (must require explicit approval)")
				}
			} else if _, ok := allowed[name]; !ok {
				missing = append(missing, "permissions.allow:"+name)
			}
			for _, rule := range settings.Permissions.Deny {
				toolMatched, _ := pathpkg.Match(rule, name)
				serverMatched, _ := pathpkg.Match(rule, serverName)
				if rule == name || rule == serverName || toolMatched || serverMatched {
					missing = append(missing, name+" (denied by "+rule+")")
				}
			}
		}
	} else {
		var settings struct {
			MCP map[string]struct {
				Type    string   `json:"type"`
				Command []string `json:"command"`
				Enabled bool     `json:"enabled"`
			} `json:"mcp"`
			Permission map[string]interface{} `json:"permission"`
		}
		if err := json.Unmarshal(raw, &settings); err != nil {
			report.add(CheckFail, "authorization.tool_policy", "authorization", "OpenCode policy is invalid JSON",
				map[string]string{"path": path, "detail": err.Error()}, "fix the connector-owned OpenCode config file")
			return false
		}
		server, ok := settings.MCP[manifest.MCPServerName]
		if !ok {
			missing = append(missing, "mcp."+manifest.MCPServerName)
		} else {
			if server.Type != "local" {
				missing = append(missing, "mcp."+manifest.MCPServerName+".type=local")
			}
			if !server.Enabled {
				missing = append(missing, "mcp."+manifest.MCPServerName+".enabled=true")
			}
			if len(server.Command) < 2 || server.Command[len(server.Command)-1] != "mcp" {
				missing = append(missing, "mcp."+manifest.MCPServerName+".command")
			}
		}
		for _, tool := range manifest.Tools {
			name := manifest.MCPServerName + "_" + tool.Name
			want := "allow"
			if tool.RequiresExplicitApproval {
				want = "ask"
			}
			if value, ok := settings.Permission[name]; !ok || value != want {
				missing = append(missing, "permission."+name+"="+want)
			}
		}
	}
	if len(missing) > 0 {
		report.add(CheckFail, "authorization.tool_policy", "authorization", "operator policy does not authorize the frozen MCP surface",
			map[string]string{"path": path, "missing": strings.Join(missing, ", "), "tool_surface_hash": manifest.ToolSurfaceHash},
			"review and install the matching least-privilege policy; do not grant a changed surface implicitly")
		return false
	}
	report.add(CheckPass, "authorization.tool_policy", "authorization", "operator policy covers the frozen MCP surface",
		map[string]string{"path": path, "tool_surface_hash": manifest.ToolSurfaceHash}, "")
	report.add(CheckWarn, "authorization.runtime_trust", "authorization", "static inspection cannot prove runtime lifecycle activation or attention admission",
		map[string]string{"harness": manifest.Harness}, "complete real-client certification; plugin installation alone does not prove lifecycle or attention behavior")
	return true
}

func checkWake(ctx context.Context, runtime *doctorRuntime, config DoctorConfig, manifest CapabilityManifest, profile CapabilityProfile, report *DoctorReport) bool {
	if !profile.RequiresWake {
		report.add(CheckPass, "notification.profile", "wakeup", "profile permits visible polling fallback",
			map[string]string{"mode": manifest.NotificationMode, "fallback": manifest.NotificationFallback}, "")
		return true
	}
	if manifest.Harness == "codex" {
		mode := strings.TrimSpace(config.AttentionMode)
		if mode == "" {
			var err error
			mode, err = ResolveCodexAttentionMode()
			if err != nil {
				report.add(CheckFail, "notification.selection", "wakeup", "Codex attention selection is invalid",
					map[string]string{"detail": err.Error()}, "rerun connector setup with native-queue or startup-only")
				return false
			}
		}
		if !containsString(manifest.AttentionModes, mode) {
			report.add(CheckFail, "notification.adapter", "wakeup", "selected Codex attention adapter is not packaged",
				map[string]string{"selected": mode, "available": strings.Join(manifest.AttentionModes, ",")},
				"rerun connector setup with a packaged adapter")
			return false
		}
		if mode == AttentionStartupOnly {
			report.add(CheckFail, "notification.adapter", "wakeup", "startup-only cannot satisfy a live-review profile",
				map[string]string{"selected": mode, "fallback": manifest.NotificationFallback},
				"choose native-queue or use the async-peer profile")
			return false
		}
		_, stderr, exitCode, err := runtime.run(ctx, config.ClientBinary, "queue", "--help")
		if err != nil || exitCode != 0 {
			report.add(CheckFail, "notification.adapter", "wakeup", "Codex queue adapter is unavailable",
				map[string]string{"detail": strings.TrimSpace(stderr)}, "use async-peer or install a Codex release with queue support")
			return false
		}
	} else if manifest.Harness == "claude" {
		mode := strings.TrimSpace(config.AttentionMode)
		if mode == "" {
			var err error
			mode, err = ResolveClaudeAttentionMode()
			if err != nil {
				report.add(CheckFail, "notification.selection", "wakeup", "Claude attention selection is invalid",
					map[string]string{"detail": err.Error()}, "rerun connector setup with a supported attention mode")
				return false
			}
		}
		if !containsString(manifest.AttentionModes, mode) {
			report.add(CheckFail, "notification.adapter", "wakeup", "selected Claude attention adapter is not packaged",
				map[string]string{"selected": mode, "available": strings.Join(manifest.AttentionModes, ",")},
				"install the matching connector package or choose a packaged adapter")
			return false
		}
		if mode == AttentionStartupOnly {
			report.add(CheckFail, "notification.adapter", "wakeup", "startup-only cannot satisfy a live-review profile",
				map[string]string{"selected": mode, "fallback": manifest.NotificationFallback},
				"choose hook-long-poll or use the async-peer profile")
			return false
		}
		report.add(CheckPass, "notification.adapter", "wakeup", "selected Claude notification adapter is packaged",
			map[string]string{"selected": mode, "available": strings.Join(manifest.AttentionModes, ","), "fallback": manifest.NotificationFallback}, "")
		return true
	} else {
		mode := strings.TrimSpace(config.AttentionMode)
		if mode == "" {
			var err error
			mode, err = ResolveOpenCodeAttentionMode()
			if err != nil {
				report.add(CheckFail, "notification.selection", "wakeup", "OpenCode attention selection is invalid",
					map[string]string{"detail": err.Error()}, "rerun connector setup with native-prompt or startup-only")
				return false
			}
		}
		if !containsString(manifest.AttentionModes, mode) {
			report.add(CheckFail, "notification.adapter", "wakeup", "selected OpenCode attention adapter is not packaged",
				map[string]string{"selected": mode, "available": strings.Join(manifest.AttentionModes, ",")},
				"rerun connector setup with a packaged adapter")
			return false
		}
		if mode == AttentionStartupOnly {
			report.add(CheckFail, "notification.adapter", "wakeup", "startup-only cannot satisfy a live-review profile",
				map[string]string{"selected": mode, "fallback": manifest.NotificationFallback},
				"choose native-prompt or use the async-peer profile")
			return false
		}
		report.add(CheckPass, "notification.adapter", "wakeup", "OpenCode native prompt adapter is packaged",
			map[string]string{"selected": mode, "endpoint": "prompt_async", "fallback": manifest.NotificationFallback}, "")
		return true
	}
	report.add(CheckPass, "notification.adapter", "wakeup", "required notification adapter is present",
		map[string]string{"mode": manifest.NotificationMode, "available": strings.Join(manifest.AttentionModes, ","), "fallback": manifest.NotificationFallback}, "")
	return true
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func checkDaemon(ctx context.Context, config DoctorConfig, report *DoctorReport) bool {
	actor := firstNonEmpty(config.Actor, "connector-doctor")
	runID := firstNonEmpty(config.RunID, "connector-doctor-"+config.Harness)
	client, err := api.Dial(ctx, config.SocketPath, api.Identity{Actor: actor, RunID: runID, Client: "connector-doctor/" + ConnectorVersion, Build: buildinfo.Current()})
	if err != nil {
		report.add(CheckFail, "daemon.reachable", "runtime", "hollerd is not reachable",
			map[string]string{"socket": config.SocketPath, "detail": err.Error()}, "start hollerd and verify the socket path and OS permissions")
		return false
	}
	defer client.Close()
	if err := client.Ping(ctx); err != nil {
		report.add(CheckFail, "daemon.protocol", "runtime", "hollerd protocol handshake failed",
			map[string]string{"socket": config.SocketPath, "detail": err.Error()}, "upgrade hollerd and the connector together")
		return false
	}
	conditions, conditionErr := client.ListConditions(ctx, false, 100)
	if conditionErr != nil {
		report.add(CheckWarn, "daemon.operator_conditions", "runtime", "operator conditions could not be inspected",
			map[string]string{"detail": conditionErr.Error()}, "run holler conditions list")
	} else if len(conditions) > 0 {
		labels := make([]string, 0, len(conditions))
		for _, condition := range conditions {
			labels = append(labels, condition.Kind+"/"+condition.Subject+"/"+string(condition.State))
		}
		report.add(CheckWarn, "daemon.operator_conditions", "runtime", "active operator conditions require attention",
			map[string]string{"count": strconv.Itoa(len(conditions)), "conditions": strings.Join(labels, ",")},
			"run holler conditions list for full reason and remediation context")
	} else {
		report.add(CheckPass, "daemon.operator_conditions", "runtime", "no active operator conditions", nil, "")
	}
	build := client.ServerBuild()
	buildEvidence := map[string]string{"id": build.ID(), "commit": build.Commit, "dirty": strconv.FormatBool(build.Dirty)}
	if build.Commit == "" || build.Commit == "unknown" || build.Dirty {
		report.add(CheckWarn, "daemon.build_identity", "runtime", "daemon build is not a clean attributable artifact",
			buildEvidence, "build from a clean commit before certification")
	} else {
		report.add(CheckPass, "daemon.build_identity", "runtime", "daemon reports a clean attributable build", buildEvidence, "")
	}
	report.add(CheckPass, "daemon.protocol", "runtime", "hollerd accepted the versioned API handshake",
		map[string]string{"socket": config.SocketPath, "protocol": strconv.Itoa(api.ProtocolVersion)}, "")
	return true
}

func discoverPluginRoot(ctx context.Context, runtime *doctorRuntime, config *DoctorConfig, manifest CapabilityManifest) (string, error) {
	if manifest.Harness == "opencode" {
		path := strings.TrimSpace(runtime.getenv("HOLLER_OPENCODE_CONNECTOR_CONFIG"))
		if path == "" {
			home, err := runtime.homeDir()
			if err != nil {
				return "", fmt.Errorf("resolve OpenCode connector config: %w", err)
			}
			path = filepath.Join(home, ".holler", "connectors", "opencode.json")
		}
		raw, err := runtime.readFile(path)
		if err != nil {
			return "", fmt.Errorf("read OpenCode connector config: %w", err)
		}
		var installed OpenCodeConnectorConfig
		if err := json.Unmarshal(raw, &installed); err != nil {
			return "", fmt.Errorf("decode OpenCode connector config: %w", err)
		}
		if strings.TrimSpace(installed.PackageRoot) == "" {
			return "", errors.New("OpenCode connector config has no package root")
		}
		config.PolicyPath = firstNonEmpty(config.PolicyPath, installed.ProfilePath)
		return installed.PackageRoot, nil
	}
	stdout, stderr, exitCode, err := runtime.run(ctx, manifest.ClientCommand, "plugin", "list", "--json")
	if err != nil || exitCode != 0 {
		return "", fmt.Errorf("plugin list failed: %s", strings.TrimSpace(stderr))
	}
	if manifest.Harness == "codex" {
		var response struct {
			Installed []struct {
				PluginID        string `json:"pluginId"`
				MarketplaceName string `json:"marketplaceName"`
				Version         string `json:"version"`
				Enabled         bool   `json:"enabled"`
			} `json:"installed"`
		}
		if err := json.Unmarshal([]byte(stdout), &response); err != nil {
			return "", err
		}
		for _, plugin := range response.Installed {
			if plugin.PluginID == manifest.PluginID && plugin.Enabled {
				codexHome := strings.TrimSpace(runtime.getenv("CODEX_HOME"))
				if codexHome == "" {
					home, err := runtime.homeDir()
					if err != nil {
						return "", fmt.Errorf("resolve Codex home: %w", err)
					}
					codexHome = filepath.Join(home, ".codex")
				}
				root := filepath.Join(codexHome, "plugins", "cache", plugin.MarketplaceName, manifest.PluginName, plugin.Version)
				if _, err := runtime.stat(root); err != nil {
					return "", fmt.Errorf("effective Codex plugin cache is missing: %s", root)
				}
				return root, nil
			}
		}
	} else {
		var plugins []struct {
			ID          string `json:"id"`
			Enabled     bool   `json:"enabled"`
			InstallPath string `json:"installPath"`
		}
		if err := json.Unmarshal([]byte(stdout), &plugins); err != nil {
			return "", err
		}
		for _, plugin := range plugins {
			if (plugin.ID == manifest.PluginID || strings.HasPrefix(plugin.ID, manifest.PluginName+"@")) && plugin.Enabled {
				return plugin.InstallPath, nil
			}
		}
	}
	return "", errors.New("matching enabled plugin not found")
}

func PackageHash(root string, assets []string) (string, error) {
	root, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	paths := append([]string(nil), assets...)
	sort.Strings(paths)
	digest := sha256.New()
	for _, relative := range paths {
		clean := filepath.Clean(relative)
		if clean == "." || filepath.IsAbs(clean) || strings.HasPrefix(clean, ".."+string(filepath.Separator)) || clean == ".." {
			return "", fmt.Errorf("asset path escapes plugin root: %s", relative)
		}
		path := filepath.Join(root, clean)
		info, err := os.Lstat(path)
		if err != nil {
			return "", fmt.Errorf("inspect %s: %w", relative, err)
		}
		if !info.Mode().IsRegular() {
			return "", fmt.Errorf("asset %s is not a regular file", relative)
		}
		body, err := os.ReadFile(path)
		if err != nil {
			return "", fmt.Errorf("read %s: %w", relative, err)
		}
		digest.Write([]byte(filepath.ToSlash(clean)))
		digest.Write([]byte{0})
		digest.Write(body)
		digest.Write([]byte{0})
	}
	return "sha256:" + hex.EncodeToString(digest.Sum(nil)), nil
}

func (report *DoctorReport) add(status CheckStatus, id, layer, summary string, evidence map[string]string, remediation string) {
	report.Checks = append(report.Checks, DiagnosticCheck{
		ID: id, Layer: layer, Status: status, Summary: summary, Evidence: evidence, Remediation: remediation,
	})
}

var versionPattern = regexp.MustCompile(`\d+(?:\.\d+)+`)

func extractVersion(value string) string { return versionPattern.FindString(value) }

func compareVersions(left, right string) int {
	leftParts := strings.Split(left, ".")
	rightParts := strings.Split(right, ".")
	count := len(leftParts)
	if len(rightParts) > count {
		count = len(rightParts)
	}
	for index := 0; index < count; index++ {
		var leftValue, rightValue int
		if index < len(leftParts) {
			leftValue, _ = strconv.Atoi(leftParts[index])
		}
		if index < len(rightParts) {
			rightValue, _ = strconv.Atoi(rightParts[index])
		}
		if leftValue < rightValue {
			return -1
		}
		if leftValue > rightValue {
			return 1
		}
	}
	return 0
}
