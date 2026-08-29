package connector_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/72olabs/holler/internal/connector"
)

func TestResolveCodexAttentionModeUsesEnvironmentConfigAndNativeDefault(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing.json")
	t.Setenv("HOLLER_CODEX_CONNECTOR_CONFIG", missing)
	t.Setenv("HOLLER_CODEX_ATTENTION", "")
	mode, err := connector.ResolveCodexAttentionMode()
	if err != nil || mode != connector.AttentionNativeQueue {
		t.Fatalf("default mode=%q err=%v", mode, err)
	}
	path := filepath.Join(t.TempDir(), "codex.json")
	if err := os.WriteFile(path, []byte(`{"schema_version":1,"attention_mode":"startup-only","profile":"holler"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HOLLER_CODEX_CONNECTOR_CONFIG", path)
	mode, err = connector.ResolveCodexAttentionMode()
	if err != nil || mode != connector.AttentionStartupOnly {
		t.Fatalf("config mode=%q err=%v", mode, err)
	}
	t.Setenv("HOLLER_CODEX_ATTENTION", connector.AttentionNativeQueue)
	mode, err = connector.ResolveCodexAttentionMode()
	if err != nil || mode != connector.AttentionNativeQueue {
		t.Fatalf("environment mode=%q err=%v", mode, err)
	}
}
