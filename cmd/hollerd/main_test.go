package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestResolveCodexBinaryPrecedenceAndSetupFallback(t *testing.T) {
	path := filepath.Join(t.TempDir(), "codex.json")
	if err := os.WriteFile(path, []byte(`{
  "schema_version": 1,
  "attention_mode": "native-queue",
  "profile": "holler",
  "client_binary": "/setup/codex"
}`), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HOLLER_CODEX_CONNECTOR_CONFIG", path)
	t.Setenv("HOLLER_CODEX_BIN", "")
	if got := resolveCodexBinary(""); got != "/setup/codex" {
		t.Fatalf("setup fallback=%q", got)
	}
	t.Setenv("HOLLER_CODEX_BIN", "/env/codex")
	if got := resolveCodexBinary(""); got != "/env/codex" {
		t.Fatalf("environment override=%q", got)
	}
	if got := resolveCodexBinary("/flag/codex"); got != "/flag/codex" {
		t.Fatalf("flag override=%q", got)
	}
}
