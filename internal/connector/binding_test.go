package connector_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/72olabs/holler/internal/connector"
)

func TestRuntimeBindingHydratesPlainClaudeSessionAndKeepsStableProcessRun(t *testing.T) {
	path := filepath.Join(t.TempDir(), "claude.json")
	if err := os.WriteFile(path, []byte(`{
  "schema_version": 1,
  "attention_mode": "hook-long-poll",
  "actor": "claude-plain",
  "role": "reviewer",
  "peer": "codex-plain",
  "project": "holler",
  "channel": "direct",
  "socket": "/tmp/holler-test.sock"
}`), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HOLLER_CONNECTOR_CONFIG", path)
	first, err := connector.ResolveRuntimeBinding("claude", connector.RuntimeBinding{})
	if err != nil {
		t.Fatal(err)
	}
	second, err := connector.ResolveRuntimeBinding("claude", connector.RuntimeBinding{})
	if err != nil {
		t.Fatal(err)
	}
	if first.Actor != "claude-plain" || first.Peer != "codex-plain" || first.Project != "holler" ||
		first.Socket != "/tmp/holler-test.sock" || first.RunID == "" || first.RunID != second.RunID {
		t.Fatalf("first=%+v second=%+v", first, second)
	}
}

func TestRuntimeBindingPreservesLauncherOverrides(t *testing.T) {
	path := filepath.Join(t.TempDir(), "codex.json")
	if err := os.WriteFile(path, []byte(`{
  "schema_version": 1,
  "attention_mode": "native-queue",
  "profile": "holler",
  "actor": "configured",
  "peer": "configured-peer",
  "project": "configured-project"
}`), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HOLLER_CODEX_CONNECTOR_CONFIG", path)
	binding, err := connector.ResolveRuntimeBinding("codex", connector.RuntimeBinding{
		Actor: "launched", RunID: "immutable-run", Peer: "launched-peer", Project: "launched-project",
	})
	if err != nil {
		t.Fatal(err)
	}
	if binding.Actor != "launched" || binding.RunID != "immutable-run" ||
		binding.Peer != "launched-peer" || binding.Project != "launched-project" {
		t.Fatalf("binding=%+v", binding)
	}
}
