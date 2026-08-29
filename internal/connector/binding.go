package connector

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
)

// RuntimeBinding is the identity and routing state shared by a harness's MCP
// server and lifecycle hooks. Explicit launcher values win; a normally started
// harness falls back to the connector selection written by setup.
type RuntimeBinding struct {
	Actor   string
	RunID   string
	Role    string
	Peer    string
	Project string
	Channel string
	Socket  string
}

func ResolveRuntimeBinding(harness string, binding RuntimeBinding) (RuntimeBinding, error) {
	harness = strings.ToLower(strings.TrimSpace(harness))
	if harness == "" {
		return binding, nil
	}
	var configured RuntimeBinding
	var err error
	switch harness {
	case "claude":
		var config ClaudeConnectorConfig
		config, err = LoadClaudeConnectorConfig("")
		configured = RuntimeBinding{
			Actor: config.Actor, Role: config.Role, Peer: config.Peer, Project: config.Project,
			Channel: config.Channel, Socket: config.Socket,
		}
	case "codex":
		var config CodexConnectorConfig
		config, err = LoadCodexConnectorConfig("")
		configured = RuntimeBinding{
			Actor: config.Actor, Role: config.Role, Peer: config.Peer, Project: config.Project,
			Channel: config.Channel, Socket: config.Socket,
		}
	case "opencode":
		var config OpenCodeConnectorConfig
		config, err = LoadOpenCodeConnectorConfig("")
		configured = RuntimeBinding{
			Actor: config.Actor, Role: config.Role, Peer: config.Peer, Project: config.Project,
			Channel: config.Channel, Socket: config.Socket,
		}
	default:
		return RuntimeBinding{}, fmt.Errorf("unsupported runtime binding harness %q", harness)
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return RuntimeBinding{}, err
	}
	binding.Actor = firstNonEmpty(binding.Actor, configured.Actor)
	binding.Role = firstNonEmpty(binding.Role, configured.Role)
	binding.Peer = firstNonEmpty(binding.Peer, configured.Peer)
	binding.Project = firstNonEmpty(binding.Project, configured.Project, "default")
	binding.Channel = firstNonEmpty(binding.Channel, configured.Channel, "direct")
	binding.Socket = firstNonEmpty(binding.Socket, configured.Socket)
	if strings.TrimSpace(binding.Actor) == "" {
		return RuntimeBinding{}, fmt.Errorf("%s connector actor is not configured; run holler connector setup", harness)
	}
	if strings.TrimSpace(binding.RunID) == "" {
		binding.RunID = processRunID(binding.Actor, harness)
	}
	return binding, nil
}

// processRunID gives plugin processes launched by the same harness process the
// same run without requiring users to start the harness through our launcher.
// A launcher-supplied HOLLER_RUN always takes precedence. The process start
// fingerprint prevents normal PID reuse from aliasing a recent execution.
func processRunID(actor, harness string) string {
	pid, fingerprint := harnessProcessIdentity(harness)
	digest := sha256.Sum256([]byte(harness + "\x00" + strconv.Itoa(pid) + "\x00" + fingerprint))
	return actor + "-auto-" + hex.EncodeToString(digest[:8])
}

func harnessProcessIdentity(harness string) (int, string) {
	immediate := os.Getppid()
	selected := immediate
	pid := immediate
	for depth := 0; depth < 32 && pid > 1; depth++ {
		parent, command, ok := processParentAndCommand(pid)
		if !ok {
			break
		}
		lower := strings.ToLower(filepath.Base(command))
		if strings.Contains(lower, harness) && !strings.Contains(lower, "holler") {
			selected = pid
			break
		}
		pid = parent
	}
	return selected, processStartFingerprint(selected)
}

func processParentAndCommand(pid int) (int, string, bool) {
	output, err := exec.Command("/bin/ps", "-o", "ppid=", "-o", "comm=", "-p", strconv.Itoa(pid)).Output()
	if err != nil {
		return 0, "", false
	}
	fields := strings.Fields(string(output))
	if len(fields) < 2 {
		return 0, "", false
	}
	parent, err := strconv.Atoi(fields[0])
	if err != nil {
		return 0, "", false
	}
	return parent, strings.Join(fields[1:], " "), true
}

func processStartFingerprint(pid int) string {
	output, err := exec.Command("/bin/ps", "-o", "lstart=", "-p", strconv.Itoa(pid)).Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(output))
}
