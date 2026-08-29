package connector_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/72olabs/holler/internal/connector"
)

func TestConnectorPackagesMatchFrozenManifests(t *testing.T) {
	tests := []struct {
		harness string
		path    string
	}{
		{harness: "codex", path: filepath.Join("..", "..", "connectors", "marketplace", "plugins", "holler")},
		{harness: "claude", path: filepath.Join("..", "..", "connectors", "marketplace", "plugins", "claude-holler")},
		{harness: "opencode", path: filepath.Join("..", "..", "connectors", "marketplace", "plugins", "opencode-holler")},
	}
	for _, test := range tests {
		t.Run(test.harness, func(t *testing.T) {
			manifest, err := connector.Manifest(test.harness)
			if err != nil {
				t.Fatal(err)
			}
			digest, err := connector.PackageHash(test.path, manifest.RequiredAssets)
			if err != nil {
				t.Fatal(err)
			}
			if digest != manifest.PackageHash {
				t.Fatalf("package hash = %q, want %q", digest, manifest.PackageHash)
			}
			raw, err := os.ReadFile(filepath.Join(test.path, "connector.json"))
			if err != nil {
				t.Fatal(err)
			}
			var packaged connector.CapabilityManifest
			if err := json.Unmarshal(raw, &packaged); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(packaged, manifest) {
				t.Fatalf("packaged manifest does not match binary:\npackaged=%+v\nbinary=%+v", packaged, manifest)
			}
		})
	}
}
