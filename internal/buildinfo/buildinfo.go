package buildinfo

import (
	"runtime/debug"
	"strings"
)

// These values are overridden by release builds with -ldflags. The debug
// module fallback keeps local builds attributable without requiring a custom
// build command.
var (
	Version = "dev"
	Commit  = ""
	Dirty   = ""
	BuiltAt = ""
)

type Info struct {
	Version string `json:"version"`
	Commit  string `json:"commit"`
	Dirty   bool   `json:"dirty"`
	BuiltAt string `json:"built_at,omitempty"`
}

func Current() Info {
	info := Info{
		Version: strings.TrimSpace(Version),
		Commit:  strings.TrimSpace(Commit),
		Dirty:   strings.EqualFold(strings.TrimSpace(Dirty), "true") || strings.TrimSpace(Dirty) == "1",
		BuiltAt: strings.TrimSpace(BuiltAt),
	}
	if build, ok := debug.ReadBuildInfo(); ok {
		if info.Version == "" || info.Version == "dev" {
			info.Version = build.Main.Version
		}
		for _, setting := range build.Settings {
			switch setting.Key {
			case "vcs.revision":
				if info.Commit == "" {
					info.Commit = setting.Value
				}
			case "vcs.modified":
				if Dirty == "" {
					info.Dirty = setting.Value == "true"
				}
			}
		}
	}
	if info.Version == "" {
		info.Version = "dev"
	}
	if info.Commit == "" {
		info.Commit = "unknown"
	}
	return info
}

func (info Info) ID() string {
	id := info.Version + "@" + info.Commit
	if info.Dirty {
		id += "+dirty"
	}
	return id
}
