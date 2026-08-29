package connector

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func DiscoverMarketplace(harness, explicit, hollerBinary string) (string, error) {
	if value := strings.TrimSpace(explicit); value != "" {
		if strings.Contains(value, "://") {
			return value, nil
		}
		return validateMarketplaceRoot(harness, value)
	}
	if value := strings.TrimSpace(os.Getenv("HOLLER_MARKETPLACE")); value != "" {
		return validateMarketplaceRoot(harness, value)
	}
	candidates := make([]string, 0, 12)
	if executable := strings.TrimSpace(hollerBinary); executable != "" {
		// Prefer the invoked path before resolving symlinks. Homebrew keeps a
		// stable prefix/bin symlink while versioned Cellar directories are
		// deleted during cleanup.
		candidates = appendMarketplaceBinaryCandidates(candidates, executable)
		if resolved, err := filepath.EvalSymlinks(executable); err == nil && filepath.Clean(resolved) != filepath.Clean(executable) {
			candidates = appendMarketplaceBinaryCandidates(candidates, resolved)
		}
	}
	if workingDirectory, err := os.Getwd(); err == nil {
		for directory := workingDirectory; ; directory = filepath.Dir(directory) {
			candidates = append(candidates,
				filepath.Join(directory, "marketplace"),
				filepath.Join(directory, "connectors", "marketplace"),
			)
			parent := filepath.Dir(directory)
			if parent == directory {
				break
			}
		}
	}
	candidates = append(candidates,
		"/opt/homebrew/share/holler/marketplace",
		"/usr/local/share/holler/marketplace",
		"/usr/share/holler/marketplace",
	)
	seen := map[string]bool{}
	for _, candidate := range candidates {
		absolute, err := filepath.Abs(candidate)
		if err != nil || seen[absolute] {
			continue
		}
		seen[absolute] = true
		if root, err := validateMarketplaceRoot(harness, absolute); err == nil {
			return root, nil
		}
	}
	return "", fmt.Errorf("Holler %s plugin marketplace was not installed; reinstall holler with its share/holler/marketplace assets or pass --marketplace", harness)
}

func appendMarketplaceBinaryCandidates(candidates []string, executable string) []string {
	binaryDirectory := filepath.Dir(executable)
	return append(candidates,
		filepath.Join(binaryDirectory, "..", "share", "holler", "marketplace"),
		filepath.Join(binaryDirectory, "..", "share", "holler"),
		filepath.Join(binaryDirectory, "..", "connectors", "marketplace"),
	)
}

func validateMarketplaceRoot(harness, root string) (string, error) {
	absolute, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	var manifest string
	switch harness {
	case "codex":
		manifest = filepath.Join(absolute, ".agents", "plugins", "marketplace.json")
	case "claude":
		manifest = filepath.Join(absolute, ".claude-plugin", "marketplace.json")
	default:
		return "", fmt.Errorf("marketplace discovery does not support harness %q", harness)
	}
	info, err := os.Stat(manifest)
	if err != nil || !info.Mode().IsRegular() {
		return "", fmt.Errorf("%s marketplace manifest is missing at %s", harness, manifest)
	}
	return filepath.Clean(absolute), nil
}
