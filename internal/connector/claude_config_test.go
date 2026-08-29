package connector_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/72olabs/holler/internal/connector"
)

func TestResolveClaudeAttentionModeUsesExplicitPrecedenceAndCompatibleDefault(t *testing.T) {
	t.Setenv("HOLLER_CONNECTOR_CONFIG", filepath.Join(t.TempDir(), "missing.json"))
	t.Setenv("HOLLER_CLAUDE_ATTENTION", "")
	t.Setenv("CLAUDE_PLUGIN_OPTION_ATTENTION_MODE", "")
	mode, err := connector.ResolveClaudeAttentionMode()
	if err != nil || mode != connector.AttentionHookLongPoll {
		t.Fatalf("default mode=%q err=%v", mode, err)
	}

	path := filepath.Join(t.TempDir(), "claude.json")
	if err := os.WriteFile(path, []byte(`{"schema_version":1,"attention_mode":"startup-only"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HOLLER_CONNECTOR_CONFIG", path)
	mode, err = connector.ResolveClaudeAttentionMode()
	if err != nil || mode != connector.AttentionStartupOnly {
		t.Fatalf("config mode=%q err=%v", mode, err)
	}
	t.Setenv("CLAUDE_PLUGIN_OPTION_ATTENTION_MODE", connector.AttentionStartupOnly)
	mode, err = connector.ResolveClaudeAttentionMode()
	if err != nil || mode != connector.AttentionStartupOnly {
		t.Fatalf("plugin mode=%q err=%v", mode, err)
	}
	t.Setenv("HOLLER_CLAUDE_ATTENTION", connector.AttentionHookLongPoll)
	mode, err = connector.ResolveClaudeAttentionMode()
	if err != nil || mode != connector.AttentionHookLongPoll {
		t.Fatalf("environment mode=%q err=%v", mode, err)
	}
}
