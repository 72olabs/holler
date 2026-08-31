package connector_test

import (
	"reflect"
	"testing"

	"github.com/72olabs/holler/internal/connector"
)

func TestBuildClaudeLaunchBindsConfiguredAdapter(t *testing.T) {
	base := connector.ClaudeConnectorConfig{
		SchemaVersion: 1, Actor: "claude-review", Peer: "codex", Project: "holler",
		Channel: "direct", PluginID: connector.DefaultClaudePluginID,
	}
	tests := []string{connector.AttentionHookLongPoll, connector.AttentionStartupOnly}
	for _, test := range tests {
		t.Run(test, func(t *testing.T) {
			config := base
			config.AttentionMode = test
			spec, err := connector.BuildClaudeLaunch(connector.ClaudeLaunchConfig{
				ConnectorConfig: config, HollerBinary: "/bin/holler", RunID: "run-1",
				ExtraArgs: []string{"--name", "reviewer"},
			})
			if err != nil {
				t.Fatal(err)
			}
			want := []string{"--name", "reviewer"}
			if !reflect.DeepEqual(spec.Args, want) || spec.Env["HOLLER_CLAUDE_ATTENTION"] != test || spec.Env["HOLLER_RUN"] != "run-1" {
				t.Fatalf("spec = %+v, want args %v", spec, want)
			}
		})
	}
}

func TestBuildLaunchExportsExplicitNamingLifecycle(t *testing.T) {
	spec, err := connector.BuildClaudeLaunch(connector.ClaudeLaunchConfig{
		ConnectorConfig: connector.ClaudeConnectorConfig{
			AttentionMode: connector.AttentionHookLongPoll, Actor: "reviewer", NameMode: "allocate",
		},
		HollerBinary: "/bin/holler", RunID: "run-1", LaunchTag: "tab-7",
	})
	if err != nil {
		t.Fatal(err)
	}
	if spec.Env["HOLLER_NAME_MODE"] != "allocate" || spec.Env["HOLLER_LAUNCH_TAG"] != "tab-7" || spec.Env["HOLLER_TAKEOVER"] != "" {
		t.Fatalf("allocation environment = %+v", spec.Env)
	}
	exact, err := connector.BuildClaudeLaunch(connector.ClaudeLaunchConfig{
		ConnectorConfig: connector.ClaudeConnectorConfig{
			AttentionMode: connector.AttentionHookLongPoll, Actor: "reviewer", NameMode: "exact",
		},
		HollerBinary: "/bin/holler", RunID: "run-2", Takeover: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if exact.Env["HOLLER_TAKEOVER"] != "true" {
		t.Fatalf("takeover environment = %+v", exact.Env)
	}
	for _, config := range []connector.ClaudeLaunchConfig{
		{ConnectorConfig: connector.ClaudeConnectorConfig{AttentionMode: connector.AttentionHookLongPoll, Actor: "reviewer", NameMode: "allocate"}, HollerBinary: "/bin/holler", Takeover: true},
		{ConnectorConfig: connector.ClaudeConnectorConfig{AttentionMode: connector.AttentionHookLongPoll, Actor: "reviewer", NameMode: ""}, HollerBinary: "/bin/holler", Takeover: true},
	} {
		if _, err := connector.BuildClaudeLaunch(config); err == nil {
			t.Fatalf("invalid naming lifecycle was accepted: %+v", config)
		}
	}
}

func TestBuildCodexLaunchBindsProfileProjectAndAttention(t *testing.T) {
	spec, err := connector.BuildCodexLaunch(connector.CodexLaunchConfig{
		ConnectorConfig: connector.CodexConnectorConfig{
			AttentionMode: connector.AttentionNativeQueue, Actor: "codex-live", Peer: "claude-review",
			Project: "holler", ProjectRoot: "/work/holler", Channel: "direct", Profile: "holler",
		},
		HollerBinary: "/bin/holler", CodexBinary: "codex-test", ConnectorPath: "/config/codex.json",
		RunID: "run-1", ExtraArgs: []string{"--search"},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"-p", "holler", "-C", "/work/holler", "--search"}
	if spec.Command != "codex-test" || !reflect.DeepEqual(spec.Args, want) ||
		spec.Env["HOLLER_CODEX_ATTENTION"] != connector.AttentionNativeQueue ||
		spec.Env["HOLLER_CODEX_CONNECTOR_CONFIG"] != "/config/codex.json" {
		t.Fatalf("spec = %+v", spec)
	}
}

func TestBuildCodexLaunchRejectsTrustBypassAndConflictingBindings(t *testing.T) {
	base := connector.CodexLaunchConfig{
		ConnectorConfig: connector.CodexConnectorConfig{
			AttentionMode: connector.AttentionNativeQueue, Actor: "codex", Profile: "holler",
		},
		HollerBinary: "/bin/holler",
	}
	for _, arg := range []string{
		"--dangerously-bypass-hook-trust", "--dangerously-bypass-hook-trust=true",
		"--profile", "--profile=other", "-pother", "-p=other", "-C", "-C/other", "--cd=/other",
	} {
		config := base
		config.ExtraArgs = []string{arg}
		if _, err := connector.BuildCodexLaunch(config); err == nil {
			t.Fatalf("argument %q was accepted", arg)
		}
	}
}

func TestBuildOpenCodeLaunchBindsIsolatedConfigAndNativePromptServer(t *testing.T) {
	spec, err := connector.BuildOpenCodeLaunch(connector.OpenCodeLaunchConfig{
		ConnectorConfig: connector.OpenCodeConnectorConfig{
			AttentionMode: connector.AttentionNativePrompt, Actor: "opencode-live", Peer: "codex-live",
			Project: "holler", ProjectRoot: "/work/holler", Channel: "direct",
			PackageRoot: "/config/opencode/holler", ProfilePath: "/config/opencode/holler/opencode.json",
			ServerHostname: "127.0.0.1", ServerPort: 0, ServerUsername: "holler",
		},
		HollerBinary: "/bin/holler", OpenCodeBinary: "opencode-test",
		ConnectorPath: "/config/holler/opencode.json", ServerPassword: "secret", RunID: "run-1",
		ExtraArgs: []string{"--model", "test/model"},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"--hostname", "127.0.0.1", "--port", "0", "/work/holler", "--model", "test/model"}
	if spec.Command != "opencode-test" || !reflect.DeepEqual(spec.Args, want) ||
		spec.Env["OPENCODE_CONFIG_DIR"] != "/config/opencode/holler" ||
		spec.Env["HOLLER_OPENCODE_SERVER"] != "" ||
		spec.Env["OPENCODE_SERVER_PASSWORD"] != "secret" ||
		spec.Env["HOLLER_OPENCODE_ATTENTION"] != connector.AttentionNativePrompt {
		t.Fatalf("spec = %+v", spec)
	}
}

func TestBuildOpenCodeStartupOnlyDoesNotExposeAttentionServer(t *testing.T) {
	spec, err := connector.BuildOpenCodeLaunch(connector.OpenCodeLaunchConfig{
		ConnectorConfig: connector.OpenCodeConnectorConfig{
			AttentionMode: connector.AttentionStartupOnly, Actor: "opencode-live",
			PackageRoot: "/config/opencode", ProfilePath: "/config/opencode/opencode.json",
			ServerHostname: "127.0.0.1", ServerPort: 0,
		},
		HollerBinary: "/bin/holler", RunID: "run-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(spec.Args) != 0 || spec.Env["OPENCODE_SERVER_USERNAME"] != "" || spec.Env["OPENCODE_SERVER_PASSWORD"] != "" {
		t.Fatalf("startup-only spec = %+v", spec)
	}
}

func TestBuildOpenCodeLaunchRejectsConflictingServerBinding(t *testing.T) {
	base := connector.OpenCodeLaunchConfig{
		ConnectorConfig: connector.OpenCodeConnectorConfig{
			AttentionMode: connector.AttentionNativePrompt, Actor: "opencode",
			PackageRoot: "/config/opencode", ProfilePath: "/config/opencode/opencode.json",
			ServerHostname: "localhost", ServerPort: 0,
		},
		HollerBinary: "/bin/holler",
	}
	for _, arg := range []string{"--hostname", "--port", "--hostname=localhost", "--port=4096", "--mini", "--mini=true"} {
		config := base
		config.ExtraArgs = []string{arg}
		if _, err := connector.BuildOpenCodeLaunch(config); err == nil {
			t.Fatalf("argument %q was accepted", arg)
		}
	}
}
