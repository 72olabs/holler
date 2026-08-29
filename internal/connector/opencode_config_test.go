package connector_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/72olabs/holler/internal/connector"
)

func TestResolveOpenCodeAttentionModeUsesEnvironmentConfigAndNativeDefault(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing.json")
	t.Setenv("HOLLER_OPENCODE_CONNECTOR_CONFIG", missing)
	t.Setenv("HOLLER_OPENCODE_ATTENTION", "")
	mode, err := connector.ResolveOpenCodeAttentionMode()
	if err != nil || mode != connector.AttentionNativePrompt {
		t.Fatalf("default mode=%q err=%v", mode, err)
	}

	path := filepath.Join(t.TempDir(), "opencode.json")
	raw, err := json.Marshal(connector.OpenCodeConnectorConfig{
		SchemaVersion: 1, AttentionMode: connector.AttentionStartupOnly,
		PackageRoot: "/config/opencode/holler", ProfilePath: "/config/opencode/holler/opencode.json",
		ServerHostname: "127.0.0.1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HOLLER_OPENCODE_CONNECTOR_CONFIG", path)
	mode, err = connector.ResolveOpenCodeAttentionMode()
	if err != nil || mode != connector.AttentionStartupOnly {
		t.Fatalf("config mode=%q err=%v", mode, err)
	}
	t.Setenv("HOLLER_OPENCODE_ATTENTION", connector.AttentionNativePrompt)
	mode, err = connector.ResolveOpenCodeAttentionMode()
	if err != nil || mode != connector.AttentionNativePrompt {
		t.Fatalf("environment mode=%q err=%v", mode, err)
	}
}

func TestLoadOpenCodeConfigRejectsNonLoopbackServer(t *testing.T) {
	path := filepath.Join(t.TempDir(), "opencode.json")
	raw := `{"schema_version":1,"attention_mode":"native-prompt","package_root":"/tmp/package","profile_path":"/tmp/profile","server_hostname":"example.com"}`
	if err := os.WriteFile(path, []byte(raw), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := connector.LoadOpenCodeConnectorConfig(path); err == nil {
		t.Fatal("non-loopback server was accepted")
	}
}
