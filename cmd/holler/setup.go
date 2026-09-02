package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/72olabs/holler/internal/api"
	"github.com/72olabs/holler/internal/bus"
	"github.com/72olabs/holler/internal/connector"
)

type productSetupResult struct {
	SchemaVersion int                         `json:"schema_version"`
	Harness       string                      `json:"harness"`
	Applied       bool                        `json:"applied"`
	Connector     connector.SetupPlan         `json:"connector"`
	Daemon        connector.DaemonServicePlan `json:"daemon"`
	NextSteps     []string                    `json:"next_steps"`
}

func runProductSetup(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		fmt.Fprintln(stderr, "usage: holler setup claude|codex [--yes|--remove]")
		return flag.ErrHelp
	}
	harness := strings.TrimSpace(args[0])
	if harness != "claude" && harness != "codex" {
		return fmt.Errorf("setup supports claude or codex, got %q", harness)
	}

	existingAttention, existingNameMode, existingActor, existingRole, existingPeer := "", "", "", "", ""
	existingProject, existingRoot, existingChannel, existingSocket := "", "", "", ""
	existingPluginID, existingProfile, existingClient := "", "", ""
	hasExistingConfig := false
	if harness == "claude" {
		if configured, err := connector.LoadClaudeConnectorConfig(""); err == nil {
			hasExistingConfig = true
			existingAttention, existingActor, existingRole, existingPeer = configured.AttentionMode, configured.Actor, configured.Role, configured.Peer
			existingNameMode = configured.NameMode
			existingProject, existingChannel, existingSocket = configured.Project, configured.Channel, configured.Socket
			existingPluginID = configured.PluginID
		} else if !errors.Is(err, os.ErrNotExist) {
			if isConnectorConfigReadError(err) {
				return err
			}
			fmt.Fprintf(stderr, "warning: existing Claude connector selection is invalid and will be replaced only after confirmation; its first backup is preserved: %v\n", err)
		}
	} else if configured, err := connector.LoadCodexConnectorConfig(""); err == nil {
		hasExistingConfig = true
		existingAttention, existingActor, existingRole, existingPeer = configured.AttentionMode, configured.Actor, configured.Role, configured.Peer
		existingNameMode = configured.NameMode
		existingProject, existingRoot, existingChannel, existingSocket = configured.Project, configured.ProjectRoot, configured.Channel, configured.Socket
		existingPluginID, existingProfile, existingClient = configured.PluginID, configured.Profile, configured.ClientBinary
	} else if !errors.Is(err, os.ErrNotExist) {
		if isConnectorConfigReadError(err) {
			return err
		}
		fmt.Fprintf(stderr, "warning: existing Codex connector selection is invalid and will be replaced only after confirmation; its first backup is preserved: %v\n", err)
	}
	defaultAttention := connector.AttentionHookLongPoll
	defaultActor, defaultPeer := "claude", "codex"
	if harness == "codex" {
		defaultAttention = connector.AttentionNativeQueue
		defaultActor, defaultPeer = "codex", "claude"
	}
	workingDirectory, _ := os.Getwd()
	defaultNameMode := existingNameMode
	if !hasExistingConfig {
		defaultNameMode = string(bus.NameModeAllocate)
	}

	flags := commandFlags("setup "+harness, stderr)
	attention := flags.String("attention", firstNonEmptyString(existingAttention, defaultAttention), "harness attention mode")
	nameMode := flags.String("name-mode", defaultNameMode, "actor naming: exact or allocate; new installations default to allocate")
	actor := flags.String("actor", firstNonEmptyString(existingActor, defaultActor), "durable inbox identity")
	role := flags.String("role", firstNonEmptyString(existingRole, "assistant"), "actor role")
	peer := flags.String("peer", firstNonEmptyString(existingPeer, defaultPeer), "default peer actor")
	project := flags.String("project", firstNonEmptyString(existingProject, "default"), "Holler project/partition")
	projectRoot := flags.String("project-root", firstNonEmptyString(existingRoot, workingDirectory), "Codex working tree used by the optional connector launcher")
	channel := flags.String("channel", firstNonEmptyString(existingChannel, "direct"), "Holler channel")
	socket := flags.String("socket", firstNonEmptyString(existingSocket, defaultSocketPath()), "hollerd Unix socket")
	marketplace := flags.String("marketplace", "", "plugin marketplace source; auto-discovered when omitted")
	pluginID := flags.String("plugin-id", existingPluginID, "plugin@marketplace identifier")
	profile := flags.String("profile", firstNonEmptyString(existingProfile, "holler"), "dedicated Codex profile name")
	clientBinary := flags.String("client-binary", existingClient, "Claude or Codex executable")
	daemonBinary := flags.String("daemon-binary", "", "hollerd executable; defaults next to holler")
	connectorConfig := flags.String("config", "", "connector selection path")
	claudeSettings := flags.String("settings", "", "Claude user settings path")
	codexHome := flags.String("codex-home", "", "Codex configuration directory")
	codexUserConfig := flags.String("user-config", "", "Codex user config.toml path")
	scope := flags.String("scope", "user", "Claude plugin installation scope")
	yes := flags.Bool("yes", false, "apply without the confirmation prompt")
	dryRun := flags.Bool("dry-run", false, "print the plan without making changes")
	remove := flags.Bool("remove", false, "remove this harness connector; the shared daemon is removed after the last harness")
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected setup arguments: %s", strings.Join(flags.Args(), " "))
	}

	executable, err := os.Executable()
	if err != nil {
		return err
	}
	executable = invokedExecutable(executable)
	input := harnessSetupInput{
		Attention: *attention, NameMode: *nameMode, Actor: *actor, Role: *role, Peer: *peer, Project: *project,
		ProjectRoot: *projectRoot, Channel: *channel, Socket: *socket,
		PluginID: *pluginID, Profile: *profile, ClientBinary: *clientBinary, ConnectorConfig: *connectorConfig,
		ClaudeSettings: *claudeSettings, CodexHome: *codexHome, CodexUserConfig: *codexUserConfig,
		Scope: *scope, HollerBinary: executable,
	}
	if *remove {
		return runProductRemoval(ctx, harness, input, *daemonBinary, *yes, *dryRun, stdin, stdout, stderr)
	}
	marketplaceSource, err := connector.DiscoverMarketplace(harness, *marketplace, executable)
	if err != nil {
		return err
	}
	input.Marketplace = marketplaceSource
	connectorPlan, err := buildHarnessSetupPlan(ctx, harness, input)
	if err != nil {
		return err
	}
	daemonPlan, err := connector.SetupDaemonService(ctx, connector.DaemonServiceConfig{
		HollerBinary: executable, DaemonBinary: *daemonBinary, Socket: *socket,
	})
	if err != nil {
		return err
	}
	result := productSetupResult{
		SchemaVersion: 1, Harness: harness, Connector: connectorPlan, Daemon: daemonPlan,
		NextSteps: productSetupNextSteps(harness),
	}
	if *dryRun {
		return writeJSON(stdout, result)
	}
	if !*yes {
		printProductSetupSummary(stderr, result, marketplaceSource)
		approved, err := readSetupConfirmation(stdin, stderr)
		if err != nil {
			return err
		}
		if !approved {
			return errors.New("Holler setup cancelled; no changes were made")
		}
	}

	daemonPlan, err = connector.SetupDaemonService(ctx, connector.DaemonServiceConfig{
		HollerBinary: executable, DaemonBinary: *daemonBinary, Socket: *socket, Apply: true,
	})
	if err != nil {
		return err
	}
	if err := waitForSetupDaemon(ctx, *socket, *actor); err != nil {
		return err
	}
	input.Apply = true
	connectorPlan, err = buildHarnessSetupPlan(ctx, harness, input)
	if err != nil {
		return err
	}
	result.Applied, result.Connector, result.Daemon = true, connectorPlan, daemonPlan
	return writeJSON(stdout, result)
}

func invokedExecutable(fallback string) string {
	candidate := strings.TrimSpace(os.Args[0])
	if candidate == "" {
		return fallback
	}
	if !filepath.IsAbs(candidate) {
		resolved, err := exec.LookPath(candidate)
		if err != nil {
			return fallback
		}
		candidate = resolved
	}
	absolute, err := filepath.Abs(candidate)
	if err != nil {
		return fallback
	}
	info, err := os.Stat(absolute)
	if err != nil || info.IsDir() || info.Mode()&0o111 == 0 {
		return fallback
	}
	return filepath.Clean(absolute)
}

func isConnectorConfigReadError(err error) bool {
	var pathError *os.PathError
	return errors.As(err, &pathError)
}

func runProductRemoval(ctx context.Context, harness string, input harnessSetupInput, daemonBinary string,
	yes, dryRun bool, stdin io.Reader, stdout, stderr io.Writer) error {
	removeDaemon := !otherHarnessConfigured(harness)
	connectorPlan, err := buildHarnessRemovalPlan(ctx, harness, input)
	if err != nil {
		return err
	}
	daemonPlan, err := connector.RemoveDaemonService(ctx, connector.DaemonServiceConfig{DaemonBinary: daemonBinary, Socket: input.Socket})
	if err != nil {
		return err
	}
	if !removeDaemon {
		daemonPlan.Actions = []string{"preserve the shared Holler daemon because another harness connector remains configured", "preserve the durable database and logs"}
	}
	result := productSetupResult{SchemaVersion: 1, Harness: harness, Connector: connectorPlan, Daemon: daemonPlan}
	if dryRun {
		return writeJSON(stdout, result)
	}
	if !yes {
		fmt.Fprintf(stderr, "Holler will remove the %s connector and its managed harness configuration.\n", harness)
		fmt.Fprintln(stderr, "The shared daemon is removed only if no other Claude or Codex connector remains; durable messages and logs are preserved.")
		approved, err := readSetupConfirmation(stdin, stderr)
		if err != nil {
			return err
		}
		if !approved {
			return errors.New("Holler removal cancelled; no changes were made")
		}
	}
	input.Apply = true
	connectorPlan, err = buildHarnessRemovalPlan(ctx, harness, input)
	if err != nil {
		return err
	}
	if removeDaemon {
		daemonPlan, err = connector.RemoveDaemonService(ctx, connector.DaemonServiceConfig{
			DaemonBinary: daemonBinary, Socket: input.Socket, Apply: true,
		})
		if err != nil {
			return err
		}
		if err := os.Remove(connector.DefaultRuntimeBinaryPath()); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	result.Applied, result.Connector, result.Daemon = true, connectorPlan, daemonPlan
	return writeJSON(stdout, result)
}

type harnessSetupInput struct {
	Attention, NameMode, Actor, Role, Peer, Project, ProjectRoot, Channel, Socket string
	Marketplace, PluginID, Profile, ClientBinary, ConnectorConfig                 string
	ClaudeSettings, CodexHome, CodexUserConfig, Scope, HollerBinary               string
	Apply                                                                         bool
}

func buildHarnessSetupPlan(ctx context.Context, harness string, input harnessSetupInput) (connector.SetupPlan, error) {
	if harness == "claude" {
		return connector.SetupClaude(ctx, connector.ClaudeSetupConfig{
			AttentionMode: input.Attention, NameMode: input.NameMode, Actor: input.Actor, Role: input.Role, Peer: input.Peer,
			Project: input.Project, Channel: input.Channel, Socket: input.Socket, PluginID: input.PluginID,
			Marketplace: input.Marketplace, Scope: input.Scope, ConnectorConfig: input.ConnectorConfig,
			ClaudeSettings: input.ClaudeSettings, ClaudeBinary: input.ClientBinary,
			HollerBinary: input.HollerBinary, Apply: input.Apply,
		})
	}
	return connector.SetupCodex(ctx, connector.CodexSetupConfig{
		AttentionMode: input.Attention, NameMode: input.NameMode, Actor: input.Actor, Role: input.Role, Peer: input.Peer,
		Project: input.Project, ProjectRoot: input.ProjectRoot, Channel: input.Channel, Socket: input.Socket,
		PluginID: input.PluginID, Marketplace: input.Marketplace, Profile: input.Profile,
		ConnectorConfig: input.ConnectorConfig, CodexHome: input.CodexHome, UserConfigPath: input.CodexUserConfig,
		CodexBinary: input.ClientBinary, HollerBinary: input.HollerBinary, GlobalPolicy: true, Apply: input.Apply,
	})
}

func buildHarnessRemovalPlan(ctx context.Context, harness string, input harnessSetupInput) (connector.SetupPlan, error) {
	if harness == "claude" {
		return connector.RemoveClaude(ctx, connector.ClaudeSetupConfig{
			PluginID: input.PluginID, Scope: input.Scope, ConnectorConfig: input.ConnectorConfig,
			ClaudeSettings: input.ClaudeSettings, ClaudeBinary: input.ClientBinary,
			RuntimeBinaryPath: connector.DefaultRuntimeBinaryPath(), Apply: input.Apply,
		})
	}
	return connector.RemoveCodex(ctx, connector.CodexSetupConfig{
		PluginID: input.PluginID, Profile: input.Profile, ConnectorConfig: input.ConnectorConfig,
		CodexHome: input.CodexHome, UserConfigPath: input.CodexUserConfig,
		CodexBinary: input.ClientBinary, RuntimeBinaryPath: connector.DefaultRuntimeBinaryPath(),
		GlobalPolicy: true, Apply: input.Apply,
	})
}

func otherHarnessConfigured(removed string) bool {
	path := connector.DefaultClaudeConnectorConfigPath()
	if removed == "claude" {
		path = connector.DefaultCodexConnectorConfigPath()
	}
	_, err := os.Stat(path)
	return err == nil
}

func printProductSetupSummary(writer io.Writer, result productSetupResult, marketplace string) {
	fmt.Fprintf(writer, "Holler will configure %s for normal `%s` launches:\n", result.Harness, result.Harness)
	fmt.Fprintf(writer, "  - install or refresh %s\n", result.Connector.PluginID)
	fmt.Fprintf(writer, "  - write connector identity %s with %s attention\n", result.Connector.ConnectorConfigPath, result.Connector.AttentionMode)
	if result.Connector.NameMode != "" {
		fmt.Fprintf(writer, "  - use %s actor naming\n", result.Connector.NameMode)
	}
	if result.Connector.UserConfigPath != "" {
		fmt.Fprintf(writer, "  - merge the frozen Holler MCP allowlist into %s\n", result.Connector.UserConfigPath)
	} else if result.Connector.ClaudeSettingsPath != "" {
		fmt.Fprintf(writer, "  - merge the frozen Holler MCP allowlist into %s\n", result.Connector.ClaudeSettingsPath)
	}
	fmt.Fprintf(writer, "  - install and start %s at %s\n", result.Daemon.Kind, result.Daemon.Path)
	fmt.Fprintf(writer, "  - use plugin marketplace %s\n", marketplace)
	if result.Harness == "codex" {
		fmt.Fprintln(writer, "  - leave Codex hook trust to Codex's explicit first-launch review")
	}
}

func readSetupConfirmation(reader io.Reader, writer io.Writer) (bool, error) {
	fmt.Fprint(writer, "Continue? [y/N] ")
	line, err := bufio.NewReader(reader).ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return false, err
	}
	if errors.Is(err, io.EOF) && strings.TrimSpace(line) == "" {
		return false, errors.New("setup requires confirmation on a terminal; pass --yes for non-interactive use")
	}
	switch strings.ToLower(strings.TrimSpace(line)) {
	case "y", "yes":
		return true, nil
	default:
		return false, nil
	}
}

func waitForSetupDaemon(ctx context.Context, socket, actor string) error {
	deadline := time.Now().Add(5 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		client, err := api.Dial(ctx, socket, api.Identity{Actor: actor, RunID: "setup-health", Client: "setup/" + connector.ConnectorVersion})
		if err == nil {
			return client.Close()
		}
		lastErr = err
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return fmt.Errorf("hollerd did not become ready at %s: %w", socket, lastErr)
}

func productSetupNextSteps(harness string) []string {
	if harness == "codex" {
		return []string{
			"Start Codex normally with `codex`.",
			"On the first turn after plugin installation or update, review and trust the exact Holler SessionStart and SessionEnd hooks when Codex prompts. Later sessions reuse that trust.",
			"Codex native-queue attention becomes addressable after the session's first submitted turn.",
		}
	}
	return []string{"Start Claude normally with `claude`; Holler hook-long-poll attention will arm automatically."}
}
