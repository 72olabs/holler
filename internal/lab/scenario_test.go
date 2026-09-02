package lab

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestBuiltInScenariosValidate(t *testing.T) {
	names, err := BuiltInScenarioNames()
	if err != nil {
		t.Fatal(err)
	}
	if len(names) < 2 {
		t.Fatalf("built-in scenarios = %v", names)
	}
	for _, name := range names {
		t.Run(name, func(t *testing.T) {
			scenario, _, err := LoadScenario(name, "")
			if err != nil {
				t.Fatal(err)
			}
			if scenario.Name != name {
				t.Fatalf("scenario name = %q, want %q", scenario.Name, name)
			}
		})
	}
}

func TestScenarioDecoderRejectsUnknownFields(t *testing.T) {
	path := filepath.Join(t.TempDir(), "invalid.yaml")
	if err := os.WriteFile(path, []byte("name: invalid\nparticipants:\n  - id: a\n    actor: a\n    harness: test\nunknown: true\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := LoadScenario("", path); err == nil || !strings.Contains(err.Error(), "field unknown not found") {
		t.Fatalf("decode error = %v", err)
	}
}

func TestSandboxRedirectsAllHarnessState(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("CODEX_HOME", filepath.Join(home, ".codex"))
	t.Setenv("CLAUDE_CONFIG_DIR", filepath.Join(home, ".claude"))
	sandbox, manifest, err := NewSandbox(filepath.Join(t.TempDir(), "run"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(sandbox.OutputDir) })
	if !manifest.Isolated || manifest.Values["HOLLER_SOCKET"] != sandbox.SocketPath ||
		manifest.Values["HOLLER_RUNTIME_PATH"] != sandbox.RuntimeRecord {
		t.Fatalf("manifest = %+v", manifest)
	}
	for name, path := range manifest.Values {
		if name == "HOME" || strings.Contains(name, "HOME") || strings.HasPrefix(name, "HOLLER_") || strings.HasPrefix(name, "OPENCODE_") {
			if !pathContains(sandbox.Root, path) {
				t.Fatalf("%s escaped sandbox: %s", name, path)
			}
		}
	}
}

func TestSandboxRefusesLiveHostOverlap(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	output := filepath.Join(home, ".holler", "lab-run")
	if _, _, err := NewSandbox(output); err == nil || !strings.Contains(err.Error(), "overlaps live host path") {
		t.Fatalf("overlap error = %v", err)
	}
}

func TestSandboxRefusesNonEmptyEvidenceDirectory(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	output := filepath.Join(t.TempDir(), "existing")
	if err := os.MkdirAll(output, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(output, "stale.json"), []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := NewSandbox(output); err == nil || !strings.Contains(err.Error(), "is not empty") {
		t.Fatalf("non-empty output error = %v", err)
	}
}

func TestSafeKeyRemovesPathSyntax(t *testing.T) {
	if got := safeKey("/tmp/run:one"); got != "runone" {
		t.Fatalf("safeKey = %q", got)
	}
}
