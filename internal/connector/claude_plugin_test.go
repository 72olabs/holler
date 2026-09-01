package connector_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestClaudePluginWrapperFailsOpenWithoutHollerBinary(t *testing.T) {
	root := repositoryRoot(t)
	wrapper := filepath.Join(root, "connectors", "marketplace", "plugins", "claude-holler", "scripts", "holler")
	command := exec.Command("/bin/sh", wrapper, "monitor", "--harness", "claude")
	command.Env = []string{"PATH=/usr/bin:/bin", "HOME=" + t.TempDir()}
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("wrapper blocked Claude Stop without Holler: %v: %s", err, output)
	}
	if !strings.Contains(string(output), "holler executable not found") {
		t.Fatalf("wrapper did not report degraded integration: %s", output)
	}
}

func TestClaudePluginWrapperSkipsMonitorForPrintMode(t *testing.T) {
	root := repositoryRoot(t)
	wrapper := filepath.Join(root, "connectors", "marketplace", "plugins", "claude-holler", "scripts", "holler")
	for _, entrypoint := range []string{"sdk-cli", "sdk-ts", "sdk-py"} {
		t.Run(entrypoint, func(t *testing.T) {
			command := exec.Command("/bin/sh", wrapper, "monitor", "--harness", "claude")
			command.Env = []string{
				"PATH=/usr/bin:/bin",
				"HOME=" + t.TempDir(),
				"HOLLER_BIN=/usr/bin/false",
				"CLAUDE_CODE_ENTRYPOINT=" + entrypoint,
			}
			if output, err := command.CombinedOutput(); err != nil {
				t.Fatalf("noninteractive monitor did not exit successfully: %v: %s", err, output)
			}
		})
	}

	interactive := exec.Command("/bin/sh", wrapper, "monitor", "--harness", "claude")
	interactive.Env = []string{
		"PATH=/usr/bin:/bin",
		"HOME=" + t.TempDir(),
		"HOLLER_BIN=/usr/bin/false",
		"CLAUDE_CODE_ENTRYPOINT=cli",
	}
	if output, err := interactive.CombinedOutput(); err == nil {
		t.Fatalf("interactive monitor was incorrectly skipped: %s", output)
	}
}

func TestClaudePluginWrapperKeepsLifecycleHookForPrintMode(t *testing.T) {
	root := repositoryRoot(t)
	wrapper := filepath.Join(root, "connectors", "marketplace", "plugins", "claude-holler", "scripts", "holler")
	command := exec.Command("/bin/sh", wrapper, "hook", "--harness", "claude")
	command.Env = []string{
		"PATH=/usr/bin:/bin",
		"HOME=" + t.TempDir(),
		"HOLLER_BIN=/bin/echo",
		"CLAUDE_CODE_ENTRYPOINT=sdk-cli",
	}
	output, err := command.CombinedOutput()
	if err != nil || !strings.Contains(string(output), "hook --harness claude") {
		t.Fatalf("print-mode lifecycle hook was skipped: %v: %s", err, output)
	}
}

func TestPluginWrappersUseSetupRecordedBinaryWithMinimalPath(t *testing.T) {
	root := repositoryRoot(t)
	state := t.TempDir()
	if err := os.WriteFile(filepath.Join(state, "bin-path"), []byte("/bin/echo\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, wrapper := range []string{
		filepath.Join(root, "connectors", "marketplace", "plugins", "holler", "scripts", "holler"),
		filepath.Join(root, "connectors", "marketplace", "plugins", "claude-holler", "scripts", "holler"),
	} {
		command := exec.Command("/bin/sh", wrapper, "mcp", "--harness", "codex")
		command.Env = []string{"PATH=/usr/bin:/bin", "HOME=" + t.TempDir(), "HOLLER_HOME=" + state}
		output, err := command.CombinedOutput()
		if err != nil || !strings.Contains(string(output), "mcp --harness codex") {
			t.Fatalf("wrapper=%s err=%v output=%s", wrapper, err, output)
		}
	}
}

func TestCodexPluginLifecycleFailsOpenButMCPFailsClosedWithoutRuntime(t *testing.T) {
	root := repositoryRoot(t)
	wrapper := filepath.Join(root, "connectors", "marketplace", "plugins", "holler", "scripts", "holler")
	for _, commandName := range []string{"hook", "session-end", "monitor"} {
		command := exec.Command("/bin/sh", wrapper, commandName)
		command.Env = []string{"PATH=/usr/bin:/bin", "HOME=" + t.TempDir()}
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("%s blocked lifecycle: %v: %s", commandName, err, output)
		}
	}
	command := exec.Command("/bin/sh", wrapper, "mcp")
	command.Env = []string{"PATH=/usr/bin:/bin", "HOME=" + t.TempDir()}
	if output, err := command.CombinedOutput(); err == nil {
		t.Fatalf("MCP falsely reported success without runtime: %s", output)
	}
}
