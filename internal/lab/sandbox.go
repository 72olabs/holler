package lab

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type Sandbox struct {
	OutputDir     string
	Root          string
	Home          string
	RuntimeDir    string
	EvidenceDir   string
	SocketPath    string
	DatabasePath  string
	ClaudeHome    string
	CodexHome     string
	OpenCodeHome  string
	RuntimeRecord string
	Environment   []string
}

type EnvironmentManifest struct {
	SchemaVersion int               `json:"schema_version"`
	CreatedAt     time.Time         `json:"created_at"`
	SandboxRoot   string            `json:"sandbox_root"`
	Values        map[string]string `json:"values"`
	HostPaths     []string          `json:"host_paths"`
	Isolated      bool              `json:"isolated"`
}

func NewSandbox(outputDir string) (Sandbox, EnvironmentManifest, error) {
	if strings.TrimSpace(outputDir) == "" {
		workingDirectory, err := os.Getwd()
		if err != nil {
			return Sandbox{}, EnvironmentManifest{}, err
		}
		outputDir = filepath.Join(workingDirectory, ".runs", "lab", time.Now().UTC().Format("20060102T150405.000000000Z"))
	}
	absolute, err := filepath.Abs(outputDir)
	if err != nil {
		return Sandbox{}, EnvironmentManifest{}, err
	}
	if entries, readErr := os.ReadDir(absolute); readErr == nil && len(entries) > 0 {
		return Sandbox{}, EnvironmentManifest{}, fmt.Errorf("lab output %s is not empty; choose a fresh directory", absolute)
	} else if readErr != nil && !os.IsNotExist(readErr) {
		return Sandbox{}, EnvironmentManifest{}, readErr
	}
	hostPaths := resolvedHostPaths()
	for _, hostPath := range hostPaths {
		if pathContains(hostPath, absolute) || pathContains(absolute, hostPath) {
			return Sandbox{}, EnvironmentManifest{}, fmt.Errorf("lab output %s overlaps live host path %s", absolute, hostPath)
		}
	}
	root, err := os.MkdirTemp("", "holler-lab-")
	if err != nil {
		return Sandbox{}, EnvironmentManifest{}, err
	}
	if err := os.Chmod(root, 0o700); err != nil {
		_ = os.RemoveAll(root)
		return Sandbox{}, EnvironmentManifest{}, err
	}
	sandbox := Sandbox{
		OutputDir:   absolute,
		Root:        root,
		EvidenceDir: filepath.Join(absolute, "evidence"),
	}
	sandbox.Home = filepath.Join(sandbox.Root, "home")
	sandbox.RuntimeDir = filepath.Join(sandbox.Root, "runtime")
	sandbox.SocketPath = filepath.Join(sandbox.RuntimeDir, "holler.sock")
	sandbox.DatabasePath = filepath.Join(sandbox.RuntimeDir, "holler.sqlite3")
	sandbox.ClaudeHome = filepath.Join(sandbox.Root, "claude-home")
	sandbox.CodexHome = filepath.Join(sandbox.Root, "codex-home")
	sandbox.OpenCodeHome = filepath.Join(sandbox.Root, "opencode-home")
	sandbox.RuntimeRecord = filepath.Join(sandbox.RuntimeDir, "bin-path")

	for _, hostPath := range hostPaths {
		if pathContains(hostPath, sandbox.Root) || pathContains(sandbox.Root, hostPath) {
			_ = os.RemoveAll(sandbox.Root)
			return Sandbox{}, EnvironmentManifest{}, fmt.Errorf("sandbox %s overlaps live host path %s", sandbox.Root, hostPath)
		}
	}
	for _, directory := range []string{sandbox.Home, sandbox.RuntimeDir, sandbox.EvidenceDir, sandbox.ClaudeHome, sandbox.CodexHome, sandbox.OpenCodeHome} {
		if err := os.MkdirAll(directory, 0o700); err != nil {
			_ = os.RemoveAll(sandbox.Root)
			return Sandbox{}, EnvironmentManifest{}, err
		}
	}
	values := map[string]string{
		"HOME":                sandbox.Home,
		"HOLLER_SOCKET":       sandbox.SocketPath,
		"HOLLER_RUNTIME_PATH": sandbox.RuntimeRecord,
		"CODEX_HOME":          sandbox.CodexHome,
		"CLAUDE_CONFIG_DIR":   sandbox.ClaudeHome,
		"OPENCODE_CONFIG":     filepath.Join(sandbox.OpenCodeHome, "opencode.json"),
		"OPENCODE_CONFIG_DIR": sandbox.OpenCodeHome,
		"XDG_CONFIG_HOME":     filepath.Join(sandbox.Root, "xdg-config"),
		"XDG_DATA_HOME":       filepath.Join(sandbox.Root, "xdg-data"),
		"XDG_STATE_HOME":      filepath.Join(sandbox.Root, "xdg-state"),
		"XDG_CACHE_HOME":      filepath.Join(sandbox.Root, "xdg-cache"),
	}
	sandbox.Environment = isolatedEnvironment(values)
	return sandbox, EnvironmentManifest{
		SchemaVersion: 1, CreatedAt: time.Now().UTC(), SandboxRoot: sandbox.Root,
		Values: values, HostPaths: hostPaths, Isolated: true,
	}, nil
}

func resolvedHostPaths() []string {
	paths := make([]string, 0, 5)
	if home, err := os.UserHomeDir(); err == nil && home != "" {
		paths = append(paths, filepath.Join(home, ".holler"), filepath.Join(home, ".codex"),
			filepath.Join(home, ".claude"), filepath.Join(home, ".config", "opencode"))
	}
	for _, name := range []string{"CODEX_HOME", "CLAUDE_CONFIG_DIR", "OPENCODE_CONFIG_DIR"} {
		if value := strings.TrimSpace(os.Getenv(name)); value != "" {
			if absolute, err := filepath.Abs(value); err == nil {
				paths = append(paths, absolute)
			}
		}
	}
	return uniqueStrings(paths)
}

func isolatedEnvironment(overrides map[string]string) []string {
	blocked := map[string]bool{
		"HOME": true, "HOLLER_SOCKET": true, "HOLLER_RUNTIME_PATH": true,
		"CODEX_HOME": true, "CLAUDE_CONFIG_DIR": true, "OPENCODE_CONFIG": true,
		"OPENCODE_CONFIG_DIR": true, "XDG_CONFIG_HOME": true, "XDG_DATA_HOME": true,
		"XDG_STATE_HOME": true, "XDG_CACHE_HOME": true,
	}
	environment := make([]string, 0, len(os.Environ())+len(overrides))
	for _, value := range os.Environ() {
		name, _, _ := strings.Cut(value, "=")
		if !blocked[name] {
			environment = append(environment, value)
		}
	}
	for name, value := range overrides {
		environment = append(environment, name+"="+value)
	}
	return environment
}

func pathContains(parent, child string) bool {
	relative, err := filepath.Rel(filepath.Clean(parent), filepath.Clean(child))
	return err == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))
}

func uniqueStrings(values []string) []string {
	seen := make(map[string]bool, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		value = filepath.Clean(value)
		if value != "." && !seen[value] {
			seen[value] = true
			result = append(result, value)
		}
	}
	return result
}
