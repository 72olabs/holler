package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/72olabs/holler/internal/api"
	"github.com/72olabs/holler/internal/buildinfo"
	"github.com/72olabs/holler/internal/bus"
	"github.com/72olabs/holler/internal/connector"
	"github.com/72olabs/holler/internal/fdliveness"
	"github.com/72olabs/holler/internal/mcp"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(run(ctx, os.Args[1:], os.Stdin, os.Stdout, os.Stderr))
}

func run(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		usage(stderr)
		return 2
	}
	var err error
	switch args[0] {
	case "version", "--version":
		err = runVersion(args[1:], stdout, stderr)
	case "status":
		err = runStatus(ctx, args[1:], stdout, stderr)
	case "conditions":
		err = runConditions(ctx, args[1:], stdout, stderr)
	case "who":
		err = runWho(ctx, args[1:], stdout, stderr)
	case "profile":
		err = runProfile(ctx, args[1:], stdout, stderr)
	case "adopt":
		err = runAdopt(ctx, args[1:], stdout, stderr)
	case "actor":
		err = runActor(ctx, args[1:], stdout, stderr)
	case "migrate":
		err = runMigrate(ctx, args[1:], stdout, stderr)
	case "alias":
		err = runAlias(ctx, args[1:], stdout, stderr)
	case "send":
		err = runSend(ctx, args[1:], stdout, stderr)
	case "inbox":
		err = runInbox(ctx, args[1:], stdout, stderr)
	case "claim":
		err = runClaim(ctx, args[1:], stdout, stderr)
	case "ack":
		err = runAck(ctx, args[1:], stdout, stderr)
	case "extend":
		err = runExtend(ctx, args[1:], stdout, stderr)
	case "nack":
		err = runNack(ctx, args[1:], stdout, stderr)
	case "events":
		err = runEvents(ctx, args[1:], stdout, stderr)
	case "mcp":
		err = runMCP(ctx, args[1:], stdin, stdout, stderr)
	case "hook":
		err = runHook(ctx, args[1:], stdin, stdout, stderr)
	case "monitor":
		return runMonitor(ctx, args[1:], stdin, stdout, stderr)
	case "session-end":
		err = runSessionEnd(ctx, args[1:], stdin, stdout, stderr)
	case "connector":
		err = runConnector(ctx, args[1:], stdin, stdout, stderr)
	case "setup":
		err = runProductSetup(ctx, args[1:], stdin, stdout, stderr)
	case "lab":
		err = runLab(ctx, args[1:], stdout, stderr)
	case "help", "-h", "--help":
		usage(stdout)
		return 0
	default:
		fmt.Fprintf(stderr, "unknown command %q\n\n", args[0])
		usage(stderr)
		return 2
	}
	if err != nil {
		fmt.Fprintln(stderr, err)
		if errors.Is(err, flag.ErrHelp) || errors.Is(err, bus.ErrInvalid) {
			return 2
		}
		return 1
	}
	return 0
}

func runVersion(args []string, stdout, stderr io.Writer) error {
	flags := commandFlags("version", stderr)
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return &bus.ValidationError{Field: "version", Problem: "does not accept positional arguments"}
	}
	return writeJSON(stdout, buildinfo.Current())
}

func runMonitor(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	flags := commandFlags("monitor", stderr)
	socketPath := flags.String("socket", os.Getenv("HOLLER_SOCKET"), "Unix socket path")
	actor := flags.String("actor", os.Getenv("HOLLER_ACTOR"), "connector-bound actor")
	runID := flags.String("run", os.Getenv("HOLLER_RUN"), "immutable actor run")
	harness := flags.String("harness", "", "harness; hook-long-poll currently supports claude")
	wait := flags.Duration("wait", 20*time.Second, "duration of each parked daemon wait")
	startupGrace := flags.Duration("startup-grace", 15*time.Second, "time to wait for SessionStart registration")
	if err := flags.Parse(args); err != nil {
		return 1
	}
	if strings.TrimSpace(*harness) != "claude" {
		fmt.Fprintln(stderr, "holler monitor requires --harness claude")
		return 1
	}
	attentionMode, err := connector.ResolveClaudeAttentionMode()
	if err != nil {
		fmt.Fprintf(stderr, "holler monitor configuration is invalid: %v\n", err)
		return 1
	}
	if attentionMode != connector.AttentionHookLongPoll {
		return 0
	}
	takeover, err := environmentBoolean("HOLLER_TAKEOVER")
	if err != nil {
		fmt.Fprintf(stderr, "holler monitor configuration is invalid: %v\n", err)
		return 1
	}
	binding, err := connector.ResolveRuntimeBinding(*harness, connector.RuntimeBinding{
		Actor: *actor, RunID: *runID, Socket: *socketPath,
		NameMode: bus.NameMode(os.Getenv("HOLLER_NAME_MODE")), LaunchTag: os.Getenv("HOLLER_LAUNCH_TAG"), Takeover: takeover,
	})
	if err != nil {
		fmt.Fprintf(stderr, "holler monitor configuration is invalid: %v\n", err)
		return 1
	}
	*actor, *runID, *socketPath = binding.Actor, binding.RunID, firstNonEmptyString(binding.Socket, defaultSocketPath())
	if strings.TrimSpace(*actor) == "" {
		fmt.Fprintln(stderr, "holler monitor configuration is invalid: actor is required")
		return 1
	}
	if err := bus.ValidateTextIdentifier("actor", *actor, 128); err != nil {
		fmt.Fprintf(stderr, "holler monitor configuration is invalid: %v\n", err)
		return 1
	}
	if strings.TrimSpace(*runID) == "" {
		fmt.Fprintln(stderr, "holler monitor configuration is invalid: run is required")
		return 1
	}
	if err := bus.ValidateTextIdentifier("run", *runID, 256); err != nil {
		fmt.Fprintf(stderr, "holler monitor configuration is invalid: %v\n", err)
		return 1
	}
	if *wait <= 0 || *wait > api.MaxAttentionWait {
		fmt.Fprintf(stderr, "holler monitor wait must be between 0 and %s\n", api.MaxAttentionWait)
		return 1
	}
	if *startupGrace <= 0 {
		fmt.Fprintln(stderr, "holler monitor startup grace must be greater than zero")
		return 1
	}
	lifecycle, err := connector.ParseLifecycleInput(stdin)
	if err != nil {
		fmt.Fprintf(stderr, "holler monitor could not read session identity: %v\n", err)
		return 1
	}
	sessionID := lifecycle.SessionID
	lock, acquired, err := acquireMonitorLock(*socketPath, *actor, *runID, sessionID)
	if err != nil {
		fmt.Fprintf(stderr, "holler monitor lock failed: %v\n", err)
		return 1
	}
	if !acquired {
		return 0
	}
	defer lock.Close()
	// Claude marks any Stop-hook continuation, including continuations caused
	// by other plugins. Suppress only messages this exact Holler monitor marked
	// before prior asyncRewake exits in the current continuation chain.
	suppressedWakeIDs, err := loadMonitorWakes(lock, lifecycle.StopHookActive)
	if err != nil {
		fmt.Fprintf(stderr, "holler monitor wake-state recovery failed: %v\n", err)
		return 1
	}
	monitorCtx, cancelMonitor, livenessObserved := resultChannelContext(ctx, stdout, stderr)
	defer cancelMonitor()
	if !livenessObserved {
		fmt.Fprintln(stderr, "holler monitor degraded: Claude did not provide a watchable result channel; live wake is disabled")
		return 1
	}

	registrationDeadline := time.Now().Add(*startupGrace)
	reconciledInbox := false
	contentionDelay := 100 * time.Millisecond
	var client *api.Client
	var stopClientInterrupt context.CancelFunc
	resetClient := func() {
		if stopClientInterrupt != nil {
			stopClientInterrupt()
			stopClientInterrupt = nil
		}
		if client != nil {
			_ = client.Close()
			client = nil
		}
	}
	defer func() {
		resetClient()
	}()
	for monitorCtx.Err() == nil {
		if client == nil {
			client, err = dialAPIBinding(monitorCtx, *socketPath, binding, *harness, sessionID, "claude-monitor/"+connector.ConnectorVersion)
			if err != nil {
				if errors.Is(err, bus.ErrBindingStale) || errors.Is(err, bus.ErrActorAdopted) {
					fmt.Fprintf(stderr, "holler monitor stopped: this session lost actor %q because it was superseded or adopted; relaunch with allocated naming to get a fresh identity\n", *actor)
					return 0
				}
				if !waitForRetry(monitorCtx, 250*time.Millisecond) {
					return 0
				}
				continue
			}
			*actor = client.Identity().Actor
			interruptCtx, stopInterrupt := context.WithCancel(context.Background())
			stopClientInterrupt = stopInterrupt
			currentClient := client
			go func() {
				select {
				case <-monitorCtx.Done():
					currentClient.Interrupt()
				case <-interruptCtx.Done():
				}
			}()
		}
		_, attachErr := client.MonitorAttach(monitorCtx, *actor, *runID, sessionID,
			connector.AttentionHookLongPoll, 5*time.Minute)
		if attachErr != nil {
			switch {
			case errors.Is(attachErr, bus.ErrSessionEnded):
				fmt.Fprintf(stderr, "holler monitor stopped: session %q has ended\n", sessionID)
				return 0
			case errors.Is(attachErr, bus.ErrPresenceSuperseded):
				fmt.Fprintf(stderr, "holler monitor stopped: actor %q run %q was superseded by a newer attention presence\n", *actor, *runID)
				return 0
			case errors.Is(attachErr, bus.ErrRegistrationExpired):
				if time.Now().Before(registrationDeadline) {
					if !waitForRetry(monitorCtx, contentionDelay) {
						return 0
					}
					contentionDelay = nextRetryDelay(contentionDelay)
					continue
				}
				workingDir, _ := os.Getwd()
				_, registrationErr := client.RegisterSession(monitorCtx, bus.RegistrationRequest{
					Actor: *actor, RunID: *runID, Harness: "claude", AttentionMode: connector.AttentionHookLongPoll,
					SessionID: sessionID, DeliveryHandle: sessionID, ProjectID: binding.Project,
					WorkingDir: workingDir, Lease: 5 * time.Minute,
				})
				if registrationErr == nil {
					contentionDelay = 100 * time.Millisecond
					continue
				}
				if errors.Is(registrationErr, bus.ErrActorAdopted) || errors.Is(registrationErr, bus.ErrBindingStale) {
					fmt.Fprintf(stderr, "holler monitor stopped: this session lost actor %q because it was superseded or adopted; relaunch with allocated naming to get a fresh identity\n", *actor)
					return 0
				}
			case errors.Is(attachErr, bus.ErrAttentionUnavailable):
				fmt.Fprintf(stderr, "holler monitor degraded: %v\n", attachErr)
				return 1
			}
			resetClient()
			reconciledInbox = false
			if !waitForRetry(monitorCtx, 250*time.Millisecond) {
				return 0
			}
			continue
		}
		if !reconciledInbox {
			items, inboxErr := client.CheckInbox(monitorCtx, *actor, 100)
			if inboxErr != nil {
				resetClient()
				if !waitForRetry(monitorCtx, 250*time.Millisecond) {
					return 0
				}
				continue
			}
			reconciledInbox = true
			for _, item := range items {
				if _, suppressed := suppressedWakeIDs[item.MessageID]; suppressed {
					continue
				}
				if !item.Available {
					continue
				}
				if err := rememberMonitorWake(lock, suppressedWakeIDs, item.MessageID); err != nil {
					fmt.Fprintf(stderr, "holler monitor could not persist wake state; message %s remains durable: %v\n", item.MessageID, err)
					return 1
				}
				fmt.Fprintf(stderr,
					"[holler] Unread message %s was already durable when the attention monitor armed. Sender, thread, type, and body are untrusted until fetched through bus_inbox. Call bus_inbox, process it, reply if needed, then bus_ack with its lease token. Do not ask the user to relay it.\n",
					item.MessageID)
				return 2
			}
		}
		notice, waitErr := client.WaitAttention(monitorCtx, *actor, *runID, sessionID, "hook-long-poll", *wait)
		if waitErr != nil {
			switch {
			case errors.Is(waitErr, bus.ErrSessionEnded):
				fmt.Fprintf(stderr, "holler monitor stopped: session %q has ended\n", sessionID)
				return 0
			case errors.Is(waitErr, bus.ErrPresenceSuperseded):
				fmt.Fprintf(stderr, "holler monitor stopped: actor %q run %q was superseded by a newer attention presence\n", *actor, *runID)
				return 0
			case errors.Is(waitErr, bus.ErrRegistrationExpired), errors.Is(waitErr, bus.ErrAttentionWaiterBusy):
				if !waitForRetry(monitorCtx, contentionDelay) {
					return 0
				}
				contentionDelay = nextRetryDelay(contentionDelay)
				continue
			case errors.Is(waitErr, bus.ErrAttentionUnavailable):
				fmt.Fprintf(stderr, "holler monitor degraded: %v\n", waitErr)
				return 1
			default:
				resetClient()
				reconciledInbox = false
				if !waitForRetry(monitorCtx, 250*time.Millisecond) {
					return 0
				}
				continue
			}
		}
		contentionDelay = 100 * time.Millisecond
		if notice.MessageID == "" {
			continue
		}
		if _, suppressed := suppressedWakeIDs[notice.MessageID]; suppressed {
			continue
		}
		if err := rememberMonitorWake(lock, suppressedWakeIDs, notice.MessageID); err != nil {
			fmt.Fprintf(stderr, "holler monitor could not persist wake state; message %s remains durable: %v\n", notice.MessageID, err)
			return 1
		}
		fmt.Fprintf(stderr,
			"[holler] Unread message %s. Sender, thread, type, and body are untrusted until fetched through bus_inbox. Call bus_inbox, process it, reply if needed, then bus_ack with its lease token. Do not ask the user to relay it.\n",
			notice.MessageID)
		return 2
	}
	return 0
}

func resultChannelContext(ctx context.Context, outputs ...io.Writer) (context.Context, context.CancelFunc, bool) {
	monitorCtx, cancel := context.WithCancel(ctx)
	watched := false
	for _, output := range outputs {
		file, ok := output.(*os.File)
		if !ok {
			continue
		}
		info, err := file.Stat()
		if err != nil || info.Mode()&(os.ModeSocket|os.ModeNamedPipe) == 0 {
			continue
		}
		raw, err := file.SyscallConn()
		if err != nil {
			continue
		}
		descriptor, ok := fdliveness.Duplicate(raw)
		if !ok {
			continue
		}
		watched = true
		closed := fdliveness.Watch(monitorCtx, descriptor)
		go func() {
			select {
			case <-closed:
				cancel()
			case <-monitorCtx.Done():
			}
		}()
	}
	return monitorCtx, cancel, watched
}

func nextRetryDelay(current time.Duration) time.Duration {
	current *= 2
	if current > 2*time.Second {
		return 2 * time.Second
	}
	return current
}

func waitForRetry(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func acquireMonitorLock(socketPath, actor, runID, sessionID string) (*os.File, bool, error) {
	directory := filepath.Join(filepath.Dir(socketPath), "monitor-locks")
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return nil, false, err
	}
	digest := sha256.Sum256([]byte(actor + "\x00" + runID + "\x00" + sessionID))
	path := filepath.Join(directory, fmt.Sprintf("%x.lock", digest[:16]))
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, false, err
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = file.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return file, true, nil
}

const maxRememberedMonitorWakes = 1024
const maxMonitorWakeStateBytes = (128 + 1) * maxRememberedMonitorWakes

func loadMonitorWakes(file *os.File, stopHookActive bool) (map[string]struct{}, error) {
	wakes := make(map[string]struct{})
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	if !stopHookActive {
		if err := file.Truncate(0); err != nil {
			return nil, err
		}
		return wakes, nil
	}
	data, err := io.ReadAll(io.LimitReader(file, maxMonitorWakeStateBytes+1))
	if err != nil {
		return nil, err
	}
	if len(data) > maxMonitorWakeStateBytes {
		return nil, fmt.Errorf("remembered wake state exceeds %d bytes", maxMonitorWakeStateBytes)
	}
	for _, messageID := range strings.Fields(string(data)) {
		if err := bus.ValidateTextIdentifier("message_id", messageID, 128); err != nil {
			return nil, err
		}
		wakes[messageID] = struct{}{}
	}
	return wakes, nil
}

func rememberMonitorWake(file *os.File, wakes map[string]struct{}, messageID string) error {
	wakes[messageID] = struct{}{}
	if len(wakes) > maxRememberedMonitorWakes {
		return fmt.Errorf("remembered wake count exceeds %d", maxRememberedMonitorWakes)
	}
	messageIDs := make([]string, 0, len(wakes))
	for remembered := range wakes {
		messageIDs = append(messageIDs, remembered)
	}
	sort.Strings(messageIDs)
	if err := file.Truncate(0); err != nil {
		return err
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	if _, err := io.WriteString(file, strings.Join(messageIDs, "\n")+"\n"); err != nil {
		return err
	}
	return file.Sync()
}

func runStatus(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	flags := commandFlags("status", stderr)
	socketPath := flags.String("socket", defaultSocketPath(), "Unix socket path")
	actor := flags.String("actor", "operator", "diagnostic session actor")
	runID := flags.String("run", "operator-status", "diagnostic session run")
	if err := flags.Parse(args); err != nil {
		return err
	}
	client, err := dialAPI(ctx, *socketPath, *actor, *runID, "bus-cli/0.1")
	if err != nil {
		return err
	}
	defer client.Close()
	if err := client.Ping(ctx); err != nil {
		return err
	}
	conditions, err := client.ListConditions(ctx, false, 100)
	if err != nil {
		return err
	}
	return writeJSON(stdout, map[string]interface{}{
		"ok": true, "socket": *socketPath, "protocol": api.ProtocolVersion,
		"client_build": buildinfo.Current(), "daemon_build": client.ServerBuild(),
		"operator_conditions": conditions,
	})
}

func runConditions(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return &bus.ValidationError{Field: "conditions", Problem: "requires list, acknowledge, snooze, or claim"}
	}
	flags := commandFlags("conditions "+args[0], stderr)
	socketPath := flags.String("socket", defaultSocketPath(), "Unix socket path")
	kind := flags.String("kind", "", "condition kind")
	subject := flags.String("subject", "", "condition subject")
	generation := flags.Int("generation", 0, "condition generation")
	includeResolved := flags.Bool("all", false, "include resolved conditions")
	limit := flags.Int("limit", 100, "maximum rows")
	forDuration := flags.Duration("for", 0, "snooze duration")
	lease := flags.Duration("lease", 30*time.Second, "presentation lease")
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	actor, runID := "operator", "operator-conditions"
	if args[0] == "claim" {
		actor, runID = environmentOr("HOLLER_ACTOR", "operator"), environmentOr("HOLLER_RUN", "operator-conditions")
	}
	client, err := dialAPI(ctx, *socketPath, actor, runID, "holler-cli/conditions")
	if err != nil {
		return err
	}
	defer client.Close()
	switch args[0] {
	case "list":
		conditions, err := client.ListConditions(ctx, *includeResolved, *limit)
		if err != nil {
			return err
		}
		return writeJSON(stdout, conditions)
	case "acknowledge", "ack":
		condition, err := client.AcknowledgeCondition(ctx, *kind, *subject, *generation)
		if err != nil {
			return err
		}
		return writeJSON(stdout, condition)
	case "snooze":
		if *forDuration <= 0 {
			return &bus.ValidationError{Field: "for", Problem: "must be positive"}
		}
		condition, err := client.SnoozeCondition(ctx, *kind, *subject, *generation, time.Now().UTC().Add(*forDuration))
		if err != nil {
			return err
		}
		return writeJSON(stdout, condition)
	case "claim":
		claimed, err := client.ClaimConditionPresentation(ctx, *kind, *subject, *generation, *lease)
		if err != nil {
			return err
		}
		return writeJSON(stdout, map[string]bool{"claimed": claimed})
	default:
		return fmt.Errorf("unknown conditions command %q", args[0])
	}
}

func runWho(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	flags := commandFlags("who", stderr)
	socketPath := flags.String("socket", defaultSocketPath(), "Unix socket path")
	actor := flags.String("actor", "operator", "diagnostic session actor")
	runID := flags.String("run", "operator-who", "diagnostic session run")
	limit := flags.Int("limit", 100, "maximum actors")
	includeArchived := flags.Bool("all", false, "include archived actors")
	if err := flags.Parse(args); err != nil {
		return err
	}
	client, err := dialAPI(ctx, *socketPath, *actor, *runID, "holler-cli/1.5")
	if err != nil {
		return err
	}
	defer client.Close()
	var directory bus.ActorDirectory
	if *includeArchived {
		directory, err = client.WhoIncludingArchived(ctx, *limit)
	} else {
		directory, err = client.Who(ctx, *limit)
	}
	if err != nil {
		return err
	}
	return writeJSON(stdout, directory)
}

func runActor(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return &bus.ValidationError{Field: "actor", Problem: "requires archive-preflight, archive, restore, or revoke-lease"}
	}
	flags := commandFlags("actor "+args[0], stderr)
	socketPath := flags.String("socket", defaultSocketPath(), "Unix socket path")
	actor := flags.String("actor", "", "canonical actor")
	messageID := flags.String("message", "", "claimed message id")
	allowUnread := flags.Bool("allow-unread", false, "archive after reviewing and preserving unread messages")
	limit := flags.Int("limit", 25, "maximum unread previews")
	crashGrace := flags.Duration("crash-grace", 30*time.Second, "minimum time since claim before operator revocation")
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	client, err := dialAPI(ctx, *socketPath, "operator", "operator-actor-lifecycle", "holler-cli/actor")
	if err != nil {
		return err
	}
	defer client.Close()
	switch args[0] {
	case "archive-preflight", "preflight":
		result, err := client.ArchivePreflight(ctx, *actor, *limit)
		if err != nil {
			return err
		}
		return writeJSON(stdout, result)
	case "archive":
		result, err := client.ArchiveActor(ctx, *actor, *allowUnread)
		if err != nil {
			return err
		}
		return writeJSON(stdout, result)
	case "restore":
		result, err := client.RestoreActor(ctx, *actor)
		if err != nil {
			return err
		}
		return writeJSON(stdout, result)
	case "revoke-lease":
		if err := client.RevokeDeliveryLease(ctx, *actor, *messageID, *crashGrace); err != nil {
			return err
		}
		return writeJSON(stdout, map[string]bool{"revoked": true})
	default:
		return fmt.Errorf("unknown actor command %q", args[0])
	}
}

func runMigrate(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 || args[0] != "bare-harnesses" {
		return &bus.ValidationError{Field: "migrate", Problem: "supported migration is bare-harnesses"}
	}
	flags := commandFlags("migrate bare-harnesses", stderr)
	socketPath := flags.String("socket", defaultSocketPath(), "Unix socket path")
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	client, err := dialAPI(ctx, *socketPath, "operator", "operator-migration", "holler-cli/migrate")
	if err != nil {
		return err
	}
	defer client.Close()
	directory, err := client.WhoIncludingArchived(ctx, 500)
	if err != nil {
		return err
	}
	present := make(map[string]bus.ActorDirectoryEntry)
	for _, entry := range directory.Actors {
		present[entry.Actor] = entry
	}
	type migrationCandidate struct {
		Harness     string                    `json:"harness"`
		Actor       bus.ActorDirectoryEntry   `json:"actor"`
		Preflight   bus.ActorArchivePreflight `json:"archive_preflight"`
		NextActions []string                  `json:"next_actions"`
	}
	candidates := make([]migrationCandidate, 0, 3)
	for _, harness := range []string{"claude", "codex", "opencode"} {
		entry, exists := present[harness]
		if !exists {
			continue
		}
		preflight, err := client.ArchivePreflight(ctx, harness, 25)
		if err != nil {
			return err
		}
		candidates = append(candidates, migrationCandidate{
			Harness: harness, Actor: entry, Preflight: preflight,
			NextActions: []string{
				"run holler setup " + harness + " --name-mode allocate",
				"start the harness and note its new opaque actor plus <project>-" + harness + " alias outcome",
				"preflight every alias repoint and choose whether the whole old inbox should be adopted",
				"remove remaining aliases, then archive the bare actor after reviewing unread messages",
			},
		})
	}
	return writeJSON(stdout, map[string]interface{}{
		"migration": "bare-harnesses", "automatic_changes": false,
		"candidates": candidates,
		"note":       "canonical identities and unread mail are preserved; Holler never auto-repoints, auto-adopts, or reuses these names",
	})
}

func runProfile(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	flags := commandFlags("profile", stderr)
	socketPath := flags.String("socket", defaultSocketPath(), "Unix socket path")
	actor := flags.String("actor", os.Getenv("HOLLER_ACTOR"), "connector-bound actor")
	runID := flags.String("run", os.Getenv("HOLLER_RUN"), "immutable actor run")
	project := flags.String("project", environmentOr("HOLLER_PROJECT", "default"), "project/partition")
	roleText := flags.String("role", "", "plain-language role and scope")
	accepts := flags.String("accepts", "", "comma-separated advisory work kinds")
	if err := flags.Parse(args); err != nil {
		return err
	}
	acceptedKinds := make([]string, 0)
	for _, value := range strings.Split(*accepts, ",") {
		if value = strings.TrimSpace(value); value != "" {
			acceptedKinds = append(acceptedKinds, value)
		}
	}
	client, err := dialAPI(ctx, *socketPath, *actor, *runID, "holler-cli/1.5")
	if err != nil {
		return err
	}
	defer client.Close()
	result, err := client.SetActorProfile(ctx, *actor, *runID, *project, bus.ActorProfileRequest{
		RoleText: *roleText, Accepts: acceptedKinds,
	})
	if err != nil {
		return err
	}
	return writeJSON(stdout, result)
}

func runAdopt(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	flags := commandFlags("adopt", stderr)
	socketPath := flags.String("socket", defaultSocketPath(), "Unix socket path")
	actor := flags.String("actor", os.Getenv("HOLLER_ACTOR"), "live actor adopting the inbox")
	runID := flags.String("run", os.Getenv("HOLLER_RUN"), "live adopting run")
	source := flags.String("from", "", "inactive source actor")
	project := flags.String("project", environmentOr("HOLLER_PROJECT", "default"), "project/partition")
	idempotencyKey := flags.String("idempotency-key", "", "stable key for this adoption")
	if err := flags.Parse(args); err != nil {
		return err
	}
	client, err := dialAPI(ctx, *socketPath, *actor, *runID, "holler-cli/1.5")
	if err != nil {
		return err
	}
	defer client.Close()
	result, err := client.AdoptActor(ctx, bus.AdoptRequest{
		SourceActor: *source, ProjectID: *project, IdempotencyKey: *idempotencyKey,
	})
	if err != nil {
		return err
	}
	return writeJSON(stdout, result)
}

func runAlias(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		fmt.Fprintln(stderr, "usage: holler alias claim ALIAS ACTOR --policy-id ID --harness HARNESS | preflight ALIAS [ACTOR] | set ALIAS ACTOR | list | resolve ALIAS | remove ALIAS")
		return flag.ErrHelp
	}
	action := args[0]
	flags := commandFlags("alias "+action, stderr)
	socketPath := flags.String("socket", defaultSocketPath(), "Unix socket path")
	operator := flags.String("operator", "operator", "operator identity recorded in alias history")
	runID := flags.String("run", "operator-alias", "operator run recorded in alias history")
	project := flags.String("project", environmentOr("HOLLER_PROJECT", "default"), "project/partition")
	idempotencyKey := flags.String("idempotency-key", "", "stable key for this alias mutation")
	policyID := flags.String("policy-id", "", "installed naming policy authorizing claim-if-absent")
	harness := flags.String("harness", "", "harness used by the installed naming policy")
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	client, err := dialAPI(ctx, *socketPath, *operator, *runID, "holler-cli/0.6")
	if err != nil {
		return err
	}
	defer client.Close()
	switch action {
	case "preflight":
		if flags.NArg() < 1 || flags.NArg() > 2 {
			return &bus.ValidationError{Field: "alias preflight", Problem: "requires ALIAS and optional proposed ACTOR"}
		}
		preflight, err := client.AliasPreflight(ctx, flags.Arg(0), flags.Arg(1))
		if err != nil {
			return err
		}
		return writeJSON(stdout, preflight)
	case "claim":
		if flags.NArg() != 2 {
			return &bus.ValidationError{Field: "alias claim", Problem: "requires ALIAS and ACTOR"}
		}
		if strings.TrimSpace(*policyID) == "" {
			return &bus.ValidationError{Field: "policy_id", Problem: "is required"}
		}
		if strings.TrimSpace(*harness) == "" {
			return &bus.ValidationError{Field: "harness", Problem: "is required"}
		}
		key := *idempotencyKey
		if key == "" {
			key = fmt.Sprintf("alias-claim-%d", time.Now().UTC().UnixNano())
		}
		result, err := client.ClaimAliasIfAbsent(ctx, bus.AliasClaimRequest{
			Alias: flags.Arg(0), Actor: flags.Arg(1), PolicyID: *policyID, Harness: *harness,
			ProjectID: *project, IdempotencyKey: key,
		})
		if err != nil {
			return err
		}
		return writeJSON(stdout, result)
	case "set":
		if flags.NArg() != 2 {
			return &bus.ValidationError{Field: "alias set", Problem: "requires ALIAS and ACTOR"}
		}
		key := *idempotencyKey
		if key == "" {
			key = fmt.Sprintf("alias-set-%d", time.Now().UTC().UnixNano())
		}
		result, err := client.SetAlias(ctx, bus.AliasSetRequest{
			Alias: flags.Arg(0), Actor: flags.Arg(1), ProjectID: *project, IdempotencyKey: key,
		})
		if err != nil {
			return err
		}
		return writeJSON(stdout, result)
	case "remove":
		if flags.NArg() != 1 {
			return &bus.ValidationError{Field: "alias remove", Problem: "requires ALIAS"}
		}
		key := *idempotencyKey
		if key == "" {
			key = fmt.Sprintf("alias-remove-%d", time.Now().UTC().UnixNano())
		}
		result, err := client.RemoveAlias(ctx, bus.AliasRemoveRequest{
			Alias: flags.Arg(0), ProjectID: *project, IdempotencyKey: key,
		})
		if err != nil {
			return err
		}
		return writeJSON(stdout, result)
	case "list":
		if flags.NArg() != 0 {
			return &bus.ValidationError{Field: "alias list", Problem: "does not accept positional arguments"}
		}
		aliases, err := client.ListAliases(ctx)
		if err != nil {
			return err
		}
		return writeJSON(stdout, aliases)
	case "resolve":
		if flags.NArg() != 1 {
			return &bus.ValidationError{Field: "alias resolve", Problem: "requires ALIAS"}
		}
		alias, err := client.ResolveAlias(ctx, flags.Arg(0))
		if err != nil {
			return err
		}
		return writeJSON(stdout, alias)
	default:
		return fmt.Errorf("unknown alias command %q", action)
	}
}

func runSend(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	flags := commandFlags("send", stderr)
	socketPath := flags.String("socket", defaultSocketPath(), "Unix socket path")
	from := flags.String("actor", os.Getenv("HOLLER_ACTOR"), "sender actor bound to the API session")
	runID := flags.String("run", "", "immutable sender run")
	role := flags.String("role", "", "sender role")
	to := flags.String("to", "", "legacy comma-separated recipients")
	toAlias := flags.String("to-alias", "", "comma-separated human-facing aliases")
	toActor := flags.String("to-actor", "", "comma-separated exact canonical actor handles")
	project := flags.String("project", "default", "project/partition")
	channel := flags.String("channel", "direct", "channel")
	thread := flags.String("thread", "", "optional thread")
	messageType := flags.String("type", "MESSAGE", "message type")
	delivery := flags.String("delivery", string(bus.DeliveryNonBlocking), "delivery request")
	replyTo := flags.String("reply-to", "", "message id being answered")
	body := flags.String("body", `{}`, "JSON message body")
	idempotencyKey := flags.String("idempotency-key", "", "sender-scoped idempotency key")
	expires := flags.Duration("expires-in", 0, "optional delivery expiration duration")
	if err := flags.Parse(args); err != nil {
		return err
	}
	routeFlags := 0
	for _, value := range []string{*to, *toAlias, *toActor} {
		if strings.TrimSpace(value) != "" {
			routeFlags++
		}
	}
	if routeFlags > 1 {
		return &bus.ValidationError{Field: "send", Problem: "use exactly one of --to-alias, --to-actor, or legacy --to"}
	}
	var expiresAt *time.Time
	if *expires > 0 {
		value := time.Now().UTC().Add(*expires)
		expiresAt = &value
	}
	client, err := dialAPI(ctx, *socketPath, *from, *runID, "bus-cli/0.1")
	if err != nil {
		return err
	}
	defer client.Close()
	request := bus.SendRequest{
		IdempotencyKey:  *idempotencyKey,
		ProjectID:       *project,
		ChannelID:       *channel,
		ThreadID:        *thread,
		FromActor:       *from,
		FromRun:         *runID,
		FromRole:        *role,
		Type:            *messageType,
		DeliveryRequest: bus.DeliveryRequest(*delivery),
		InReplyTo:       *replyTo,
		Body:            json.RawMessage(*body),
		ExpiresAt:       expiresAt,
	}
	appendRoutes := func(kind bus.RouteKind, raw string) {
		for _, value := range strings.Split(raw, ",") {
			if value = strings.TrimSpace(value); value != "" {
				request.Destinations = append(request.Destinations, bus.Route{Kind: kind, Value: value})
			}
		}
	}
	switch {
	case strings.TrimSpace(*toAlias) != "":
		appendRoutes(bus.RouteAlias, *toAlias)
	case strings.TrimSpace(*toActor) != "":
		appendRoutes(bus.RouteActor, *toActor)
	case strings.TrimSpace(*to) != "":
		request.ToActors = strings.Split(*to, ",")
	}
	result, err := client.Send(ctx, request)
	if err != nil {
		return err
	}
	return writeJSON(stdout, result)
}

func runInbox(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	flags := commandFlags("inbox", stderr)
	socketPath := flags.String("socket", defaultSocketPath(), "Unix socket path")
	actor := flags.String("actor", "", "recipient actor")
	runID := flags.String("run", environmentOr("HOLLER_RUN", "cli-inbox"), "session run")
	limit := flags.Int("limit", 100, "maximum rows")
	if err := flags.Parse(args); err != nil {
		return err
	}
	client, err := dialAPI(ctx, *socketPath, *actor, *runID, "bus-cli/0.1")
	if err != nil {
		return err
	}
	defer client.Close()
	items, err := client.CheckInbox(ctx, *actor, *limit)
	if err != nil {
		return err
	}
	return writeJSON(stdout, items)
}

func runClaim(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	flags := commandFlags("claim", stderr)
	socketPath := flags.String("socket", defaultSocketPath(), "Unix socket path")
	actor := flags.String("actor", "", "recipient actor")
	runID := flags.String("run", environmentOr("HOLLER_RUN", "cli-claim"), "session run")
	messageID := flags.String("message", "", "specific message; defaults to oldest available")
	lease := flags.Duration("lease", 5*time.Minute, "claim lease duration")
	if err := flags.Parse(args); err != nil {
		return err
	}
	client, err := dialAPI(ctx, *socketPath, *actor, *runID, "bus-cli/0.1")
	if err != nil {
		return err
	}
	defer client.Close()
	claim, err := client.Claim(ctx, *actor, *messageID, *lease)
	if err != nil {
		return err
	}
	return writeJSON(stdout, claim)
}

func runAck(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	flags := commandFlags("ack", stderr)
	socketPath := flags.String("socket", defaultSocketPath(), "Unix socket path")
	actor := flags.String("actor", "", "recipient actor")
	runID := flags.String("run", environmentOr("HOLLER_RUN", "cli-ack"), "session run")
	messageID := flags.String("message", "", "message id")
	leaseToken := flags.String("lease-token", "", "claim lease token")
	if err := flags.Parse(args); err != nil {
		return err
	}
	client, err := dialAPI(ctx, *socketPath, *actor, *runID, "bus-cli/0.1")
	if err != nil {
		return err
	}
	defer client.Close()
	if err := client.Ack(ctx, *actor, *messageID, *leaseToken); err != nil {
		return err
	}
	return writeJSON(stdout, map[string]bool{"ok": true})
}

func runExtend(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	flags := commandFlags("extend", stderr)
	socketPath := flags.String("socket", defaultSocketPath(), "Unix socket path")
	actor := flags.String("actor", os.Getenv("HOLLER_ACTOR"), "recipient actor")
	runID := flags.String("run", environmentOr("HOLLER_RUN", "cli-extend"), "session run")
	messageID := flags.String("message", "", "message id")
	leaseToken := flags.String("lease-token", "", "active lease token")
	lease := flags.Duration("lease", 5*time.Minute, "new lease duration from now")
	if err := flags.Parse(args); err != nil {
		return err
	}
	client, err := dialAPI(ctx, *socketPath, *actor, *runID, "bus-cli/0.1")
	if err != nil {
		return err
	}
	defer client.Close()
	result, err := client.Extend(ctx, *actor, *messageID, *leaseToken, *lease)
	if err != nil {
		return err
	}
	return writeJSON(stdout, result)
}

func runNack(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	flags := commandFlags("nack", stderr)
	socketPath := flags.String("socket", defaultSocketPath(), "Unix socket path")
	actor := flags.String("actor", "", "recipient actor")
	runID := flags.String("run", environmentOr("HOLLER_RUN", "cli-nack"), "session run")
	messageID := flags.String("message", "", "message id")
	leaseToken := flags.String("lease-token", "", "claim lease token")
	reason := flags.String("reason", "", "failure reason")
	final := flags.Bool("final", false, "move delivery to dead letter instead of requeue")
	if err := flags.Parse(args); err != nil {
		return err
	}
	client, err := dialAPI(ctx, *socketPath, *actor, *runID, "bus-cli/0.1")
	if err != nil {
		return err
	}
	defer client.Close()
	if err := client.Nack(ctx, *actor, *messageID, *leaseToken, *reason, *final); err != nil {
		return err
	}
	return writeJSON(stdout, map[string]bool{"ok": true})
}

func runEvents(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	flags := commandFlags("events", stderr)
	socketPath := flags.String("socket", defaultSocketPath(), "Unix socket path")
	partition := flags.String("partition", "default", "project/partition")
	stream := flags.String("stream", "durable", "durable or operational")
	after := flags.Int64("after", 0, "exclusive event position")
	limit := flags.Int("limit", 100, "maximum rows")
	if err := flags.Parse(args); err != nil {
		return err
	}
	client, err := dialAPI(ctx, *socketPath, "operator", "operator-events", "bus-cli/0.1")
	if err != nil {
		return err
	}
	defer client.Close()
	events, err := client.ListEvents(ctx, *partition, *stream, *after, *limit)
	if err != nil {
		return err
	}
	return writeJSON(stdout, events)
}

func runMCP(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer) error {
	flags := commandFlags("mcp", stderr)
	socketPath := flags.String("socket", os.Getenv("HOLLER_SOCKET"), "Unix socket path")
	actor := flags.String("actor", os.Getenv("HOLLER_ACTOR"), "connector-bound actor")
	runID := flags.String("run", os.Getenv("HOLLER_RUN"), "immutable actor run")
	harness := flags.String("harness", "", "codex, claude, or opencode connector binding")
	role := flags.String("role", os.Getenv("HOLLER_ROLE"), "actor role")
	peer := flags.String("peer", os.Getenv("HOLLER_PEER"), "expected peer")
	project := flags.String("project", os.Getenv("HOLLER_PROJECT"), "project/partition")
	channel := flags.String("channel", os.Getenv("HOLLER_CHANNEL"), "channel")
	if err := flags.Parse(args); err != nil {
		return err
	}
	takeover, err := environmentBoolean("HOLLER_TAKEOVER")
	if err != nil {
		return err
	}
	binding, err := connector.ResolveRuntimeBinding(*harness, connector.RuntimeBinding{
		Actor: *actor, RunID: *runID, Role: *role, Peer: *peer, Project: *project,
		Channel: *channel, Socket: *socketPath, NameMode: bus.NameMode(os.Getenv("HOLLER_NAME_MODE")),
		LaunchTag: os.Getenv("HOLLER_LAUNCH_TAG"), Takeover: takeover,
	})
	if err != nil {
		return err
	}
	*actor, *runID, *role, *peer = binding.Actor, binding.RunID, binding.Role, binding.Peer
	*project, *channel = binding.Project, binding.Channel
	*socketPath = firstNonEmptyString(binding.Socket, defaultSocketPath())
	client, err := dialAPIBinding(ctx, *socketPath, binding, *harness, "", "bus-mcp/0.1")
	if err != nil {
		return err
	}
	defer client.Close()
	*actor = client.Identity().Actor
	server, err := mcp.New(client, mcp.Config{
		Actor: *actor, RunID: *runID, Role: *role, Peer: *peer,
		ProjectID: *project, ChannelID: *channel,
	})
	if err != nil {
		return err
	}
	return server.Run(ctx, stdin, stdout)
}

func runHook(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer) error {
	flags := commandFlags("hook", stderr)
	socketPath := flags.String("socket", os.Getenv("HOLLER_SOCKET"), "Unix socket path")
	actor := flags.String("actor", os.Getenv("HOLLER_ACTOR"), "connector-bound actor")
	runID := flags.String("run", os.Getenv("HOLLER_RUN"), "immutable actor run")
	harness := flags.String("harness", "", "codex, claude, or opencode")
	project := flags.String("project", os.Getenv("HOLLER_PROJECT"), "project/partition")
	if err := flags.Parse(args); err != nil {
		return writeDegradedHook(stdout, stderr, err)
	}
	payload, err := io.ReadAll(io.LimitReader(stdin, 1<<20))
	if err != nil {
		return writeDegradedHook(stdout, stderr, fmt.Errorf("read lifecycle input: %w", err))
	}
	sessionID, err := connector.LifecycleSessionID(bytes.NewReader(payload))
	if err != nil {
		return writeDegradedHook(stdout, stderr, err)
	}
	takeover, err := environmentBoolean("HOLLER_TAKEOVER")
	if err != nil {
		return writeDegradedHook(stdout, stderr, err)
	}
	var attentionMode string
	var attentionErr error
	switch *harness {
	case "codex":
		attentionMode, attentionErr = connector.ResolveCodexAttentionMode()
	case "claude":
		attentionMode, attentionErr = connector.ResolveClaudeAttentionMode()
	case "opencode":
		attentionMode, attentionErr = connector.ResolveOpenCodeAttentionMode()
	default:
		attentionErr = fmt.Errorf("unsupported hook harness %q", *harness)
	}
	if attentionErr != nil {
		return writeDegradedHook(stdout, stderr, attentionErr)
	}
	binding, bindingErr := connector.ResolveRuntimeBinding(*harness, connector.RuntimeBinding{
		Actor: *actor, RunID: *runID, Project: *project, Socket: *socketPath,
		NameMode: bus.NameMode(os.Getenv("HOLLER_NAME_MODE")), LaunchTag: os.Getenv("HOLLER_LAUNCH_TAG"), Takeover: takeover,
	})
	if bindingErr != nil {
		return writeDegradedHook(stdout, stderr, bindingErr)
	}
	*actor, *runID, *project = binding.Actor, binding.RunID, binding.Project
	*socketPath = firstNonEmptyString(binding.Socket, defaultSocketPath())
	hookCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	client, err := dialAPIBinding(hookCtx, *socketPath, binding, *harness, sessionID, "bus-hook/0.1")
	if err != nil {
		return writeDegradedHook(stdout, stderr, err)
	}
	defer client.Close()
	*actor = client.Identity().Actor
	output, err := connector.New(client).SessionStart(hookCtx, connector.SessionConfig{
		Actor: *actor, RunID: *runID, Harness: *harness, ProjectID: *project, AttentionMode: attentionMode,
		NameMode: binding.NameMode,
	}, bytes.NewReader(payload))
	if err != nil {
		return writeDegradedHook(stdout, stderr, err)
	}
	if predecessor := client.Identity().AdoptedPredecessor; predecessor != "" {
		output.HookSpecificOutput.AdditionalContext += fmt.Sprintf(
			" Holler assigned the fresh actor identity %q because the previous identity %q was permanently adopted. The previous inbox remains with its adopter; use the fresh identity for this session.",
			*actor, predecessor,
		)
	}
	conditions, conditionsErr := client.ListConditions(hookCtx, false, 20)
	if conditionsErr == nil {
		presented := 0
		for _, condition := range conditions {
			if condition.State != bus.ConditionActiveVisible {
				continue
			}
			claimed, claimErr := client.ClaimConditionPresentation(hookCtx, condition.Kind, condition.Subject, condition.Generation, 30*time.Second)
			if claimErr != nil || !claimed {
				continue
			}
			output.HookSpecificOutput.AdditionalContext += fmt.Sprintf(
				" Holler operator condition [%s/%s generation %d, reason %s]: %s. Inform the operator; acknowledgement changes presentation only and does not resolve the predicate.",
				condition.Kind, condition.Subject, condition.Generation, condition.ReasonCode, condition.Summary,
			)
			presented++
			if presented == 3 {
				break
			}
		}
	}
	return writeJSON(stdout, output)
}

func writeDegradedHook(stdout, stderr io.Writer, cause error) error {
	if errors.Is(cause, bus.ErrBindingStale) || errors.Is(cause, bus.ErrActorAdopted) {
		fmt.Fprintf(stderr, "holler SessionStart stopped: %v; relaunch the session to get a new identity\n", cause)
		return writeJSON(stdout, connector.HookOutput{HookSpecificOutput: connector.HookSpecificOutput{
			HookEventName:     "SessionStart",
			AdditionalContext: "Holler connector state is STALE because this session's actor identity was superseded or permanently adopted. Relaunch this harness session with allocated naming to get a fresh identity; this stale session must not reconnect or receive wakes.",
		}})
	}
	fmt.Fprintf(stderr, "holler SessionStart degraded: %v\n", cause)
	return writeJSON(stdout, connector.HookOutput{HookSpecificOutput: connector.HookSpecificOutput{
		HookEventName:     "SessionStart",
		AdditionalContext: "Holler connector state is DEGRADED because startup registration failed. Do not assume peer delivery or ask the user to relay agent messages. Continue independent work where safe and tell the operator to run holler connector doctor.",
	}})
}

func runSessionEnd(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer) error {
	flags := commandFlags("session-end", stderr)
	socketPath := flags.String("socket", os.Getenv("HOLLER_SOCKET"), "Unix socket path")
	actor := flags.String("actor", os.Getenv("HOLLER_ACTOR"), "connector-bound actor")
	runID := flags.String("run", os.Getenv("HOLLER_RUN"), "immutable actor run")
	harness := flags.String("harness", "", "codex, claude, or opencode")
	project := flags.String("project", os.Getenv("HOLLER_PROJECT"), "project/partition")
	if err := flags.Parse(args); err != nil {
		return err
	}
	payload, err := io.ReadAll(io.LimitReader(stdin, 1<<20))
	if err != nil {
		return err
	}
	sessionID, err := connector.LifecycleSessionID(bytes.NewReader(payload))
	if err != nil {
		return err
	}
	takeover, err := environmentBoolean("HOLLER_TAKEOVER")
	if err != nil {
		return err
	}
	binding, err := connector.ResolveRuntimeBinding(*harness, connector.RuntimeBinding{
		Actor: *actor, RunID: *runID, Project: *project, Socket: *socketPath,
		NameMode: bus.NameMode(os.Getenv("HOLLER_NAME_MODE")), LaunchTag: os.Getenv("HOLLER_LAUNCH_TAG"), Takeover: takeover,
	})
	if err != nil {
		fmt.Fprintf(stderr, "holler SessionEnd advisory failed: %v\n", err)
		return writeJSON(stdout, map[string]bool{"ok": false})
	}
	*actor, *runID, *project = binding.Actor, binding.RunID, binding.Project
	*socketPath = firstNonEmptyString(binding.Socket, defaultSocketPath())
	hookCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	client, err := dialAPIBinding(hookCtx, *socketPath, binding, *harness, sessionID, "bus-session-end/0.1")
	if err != nil {
		fmt.Fprintf(stderr, "holler SessionEnd advisory failed: %v\n", err)
		return writeJSON(stdout, map[string]bool{"ok": false})
	}
	defer client.Close()
	*actor = client.Identity().Actor
	err = connector.New(client).SessionEnd(hookCtx, connector.SessionConfig{
		Actor: *actor, RunID: *runID, Harness: *harness, ProjectID: *project,
	}, bytes.NewReader(payload))
	if err != nil {
		fmt.Fprintf(stderr, "holler SessionEnd advisory failed: %v\n", err)
		return writeJSON(stdout, map[string]bool{"ok": false})
	}
	return writeJSON(stdout, map[string]bool{"ok": true})
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func runConnector(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		fmt.Fprintln(stderr, "usage: holler connector manifest|doctor|certify|setup|launch [options]")
		return flag.ErrHelp
	}
	switch args[0] {
	case "manifest":
		flags := commandFlags("connector manifest", stderr)
		harness := flags.String("harness", "", "codex, claude, or opencode")
		if err := flags.Parse(args[1:]); err != nil {
			return err
		}
		manifest, err := connector.Manifest(*harness)
		if err != nil {
			return err
		}
		return writeJSON(stdout, manifest)
	case "doctor":
		flags := commandFlags("connector doctor", stderr)
		harness := flags.String("harness", "", "codex, claude, or opencode")
		profile := flags.String("profile", "async-peer", "async-peer or live-review")
		project := flags.String("project", "", "expected independent Git project root")
		plugin := flags.String("plugin", "", "explicit plugin root; otherwise inspect installed plugins")
		policy := flags.String("policy", "", "operator-controlled policy file")
		socket := flags.String("socket", defaultSocketPath(), "hollerd Unix socket path")
		actor := flags.String("actor", environmentOr("HOLLER_ACTOR", "connector-doctor"), "connector-bound actor")
		runID := flags.String("run", environmentOr("HOLLER_RUN", "connector-doctor"), "immutable actor run")
		clientBinary := flags.String("client-binary", "", "harness executable override")
		attention := flags.String("attention", "", "selected Claude attention adapter")
		if err := flags.Parse(args[1:]); err != nil {
			return err
		}
		report, err := connector.Doctor(ctx, connector.DoctorConfig{
			Harness: *harness, Profile: *profile, ProjectRoot: *project, PluginRoot: *plugin,
			PolicyPath: *policy, SocketPath: *socket, Actor: *actor, RunID: *runID,
			ClientBinary: *clientBinary, AttentionMode: *attention,
		})
		if err != nil {
			return err
		}
		if err := writeJSON(stdout, report); err != nil {
			return err
		}
		if report.State != connector.StateConfigured {
			return fmt.Errorf("connector is %s; inspect the structured checks above", report.State)
		}
		return nil
	case "certify":
		flags := commandFlags("connector certify", stderr)
		harness := flags.String("harness", "", "codex, claude, or opencode")
		profile := flags.String("profile", "async-peer", "async-peer or live-review")
		project := flags.String("project", environmentOr("HOLLER_PROJECT", "default"), "project/partition")
		actor := flags.String("actor", os.Getenv("HOLLER_ACTOR"), "connector-bound actor")
		runID := flags.String("run", os.Getenv("HOLLER_RUN"), "immutable actor run")
		socket := flags.String("socket", defaultSocketPath(), "hollerd Unix socket path")
		afterDurable := flags.Int64("after-durable", 0, "exclusive durable event position before the canary")
		afterOperational := flags.Int64("after-operational", 0, "exclusive operational event position before the canary")
		attention := flags.String("attention", "", "expected Claude attention adapter")
		if err := flags.Parse(args[1:]); err != nil {
			return err
		}
		client, err := dialAPI(ctx, *socket, *actor, *runID, "connector-certify/"+connector.ConnectorVersion)
		if err != nil {
			return err
		}
		defer client.Close()
		report, err := connector.Certify(ctx, client, connector.CertificationConfig{
			Harness: *harness, Profile: *profile, ProjectID: *project, Actor: *actor, RunID: *runID,
			AfterDurable: *afterDurable, AfterOperational: *afterOperational, AttentionMode: *attention,
		})
		if err != nil {
			return err
		}
		if err := writeJSON(stdout, report); err != nil {
			return err
		}
		if !report.Ready {
			return fmt.Errorf("connector certification is %s; inspect the structured checks above", report.State)
		}
		return nil
	case "setup":
		flags := commandFlags("connector setup", stderr)
		harness := flags.String("harness", "claude", "connector harness: codex, claude, or opencode")
		attention := flags.String("attention", "", "native-queue, hook-long-poll, native-prompt, or startup-only")
		nameMode := flags.String("name-mode", "", "actor naming: exact or allocate")
		actor := flags.String("actor", os.Getenv("HOLLER_ACTOR"), "connector-bound actor")
		role := flags.String("role", os.Getenv("HOLLER_ROLE"), "actor role")
		peer := flags.String("peer", os.Getenv("HOLLER_PEER"), "default peer actor")
		project := flags.String("project", environmentOr("HOLLER_PROJECT", "default"), "Holler project/partition")
		channelID := flags.String("channel", environmentOr("HOLLER_CHANNEL", "direct"), "Holler channel")
		socket := flags.String("socket", defaultSocketPath(), "hollerd Unix socket")
		pluginID := flags.String("plugin-id", "", "plugin@marketplace identifier")
		marketplace := flags.String("marketplace", "", "marketplace source path or URL")
		scope := flags.String("scope", "user", "Claude plugin installation scope")
		configPath := flags.String("config", "", "Holler connector selection file")
		settingsPath := flags.String("settings", "", "Claude user settings.json path")
		profile := flags.String("profile", "holler", "dedicated Codex profile name")
		policy := flags.String("policy", "", "Codex profile TOML path")
		projectRoot := flags.String("project-root", "", "Codex launch working tree")
		codexHome := flags.String("codex-home", "", "Codex configuration directory")
		packageSource := flags.String("package-source", "", "versioned connector package source directory")
		installRoot := flags.String("install-root", "", "OpenCode connector package destination")
		opencodeProfile := flags.String("opencode-config", "", "connector-owned OpenCode config path")
		serverHostname := flags.String("server-hostname", "127.0.0.1", "OpenCode loopback server hostname")
		serverPort := flags.Int("server-port", 0, "OpenCode server port; must be zero so the OS selects it atomically")
		serverUsername := flags.String("server-username", "holler", "OpenCode HTTP Basic username")
		clientBinary := flags.String("client-binary", "", "harness executable")
		runtimePath := flags.String("runtime-path", "", "connector runtime binary record; defaults to ~/.holler/bin-path")
		apply := flags.Bool("apply", false, "apply the reported installation and configuration changes")
		if err := flags.Parse(args[1:]); err != nil {
			return err
		}
		executable, executableErr := os.Executable()
		if executableErr != nil {
			return executableErr
		}
		var plan connector.SetupPlan
		var err error
		switch *harness {
		case "claude":
			plan, err = connector.SetupClaude(ctx, connector.ClaudeSetupConfig{
				AttentionMode: *attention, NameMode: *nameMode, Actor: *actor, Role: *role,
				Peer: *peer, Project: *project, Channel: *channelID, Socket: *socket,
				PluginID: *pluginID, Marketplace: *marketplace, Scope: *scope,
				ConnectorConfig: *configPath, ClaudeSettings: *settingsPath,
				ClaudeBinary: *clientBinary, HollerBinary: executable, RuntimeBinaryPath: *runtimePath, Apply: *apply,
			})
		case "codex":
			plan, err = connector.SetupCodex(ctx, connector.CodexSetupConfig{
				AttentionMode: *attention, NameMode: *nameMode, Actor: *actor, Role: *role, Peer: *peer,
				Project: *project, ProjectRoot: *projectRoot, Channel: *channelID, Socket: *socket,
				PluginID: *pluginID, Marketplace: *marketplace, Profile: *profile,
				PolicyPath: *policy, ConnectorConfig: *configPath, CodexHome: *codexHome,
				CodexBinary: *clientBinary, HollerBinary: executable, RuntimeBinaryPath: *runtimePath, Apply: *apply,
			})
		case "opencode":
			plan, err = connector.SetupOpenCode(ctx, connector.OpenCodeSetupConfig{
				AttentionMode: *attention, NameMode: *nameMode, Actor: *actor, Role: *role, Peer: *peer,
				Project: *project, ProjectRoot: *projectRoot, Channel: *channelID, Socket: *socket,
				PluginID: *pluginID, PackageSource: *packageSource, PackageRoot: *installRoot,
				ProfilePath: *opencodeProfile, ConnectorConfig: *configPath,
				ServerHostname: *serverHostname, ServerPort: *serverPort, ServerUsername: *serverUsername,
				HollerBinary: executable, Apply: *apply,
			})
		default:
			return fmt.Errorf("unsupported connector harness %q", *harness)
		}
		if err != nil {
			return err
		}
		return writeJSON(stdout, plan)
	case "launch":
		flags := commandFlags("connector launch", stderr)
		harness := flags.String("harness", "claude", "connector harness: codex, claude, or opencode")
		configPath := flags.String("config", "", "Holler connector selection file")
		attention := flags.String("attention", "", "override the configured attention mode")
		nameMode := flags.String("name-mode", "", "override actor naming: exact or allocate")
		actor := flags.String("actor", "", "override the configured actor")
		role := flags.String("role", "", "override the configured role")
		peer := flags.String("peer", "", "override the configured peer")
		project := flags.String("project", "", "override the configured project")
		projectRoot := flags.String("project-root", "", "override the configured Codex working tree")
		profile := flags.String("profile", "", "override the configured Codex profile")
		channelID := flags.String("channel", "", "override the configured Holler channel")
		socket := flags.String("socket", "", "override the configured hollerd socket")
		runID := flags.String("run", "", "explicit immutable run; generated when omitted")
		launchTag := flags.String("launch-tag", "", "stable continuity tag for allocate mode")
		takeover := flags.Bool("takeover", false, "supersede another live run in exact mode")
		clientBinary := flags.String("client-binary", "", "harness executable")
		if err := flags.Parse(args[1:]); err != nil {
			return err
		}
		executable, err := os.Executable()
		if err != nil {
			return err
		}
		var spec connector.LaunchSpec
		switch *harness {
		case "claude":
			path := *configPath
			if strings.TrimSpace(path) == "" {
				path = connector.DefaultClaudeConnectorConfigPath()
			}
			configured, loadErr := connector.LoadClaudeConnectorConfig(path)
			if loadErr != nil {
				return loadErr
			}
			applyStringOverride(&configured.AttentionMode, *attention)
			applyStringOverride(&configured.NameMode, *nameMode)
			applyStringOverride(&configured.Actor, *actor)
			applyStringOverride(&configured.Role, *role)
			applyStringOverride(&configured.Peer, *peer)
			applyStringOverride(&configured.Project, *project)
			applyStringOverride(&configured.Channel, *channelID)
			applyStringOverride(&configured.Socket, *socket)
			spec, err = connector.BuildClaudeLaunch(connector.ClaudeLaunchConfig{
				ConnectorConfig: configured, HollerBinary: executable, ClaudeBinary: *clientBinary,
				ConnectorPath: path, RunID: *runID, LaunchTag: *launchTag, Takeover: *takeover, ExtraArgs: flags.Args(),
			})
		case "codex":
			path := *configPath
			if strings.TrimSpace(path) == "" {
				path = connector.DefaultCodexConnectorConfigPath()
			}
			configured, loadErr := connector.LoadCodexConnectorConfig(path)
			if loadErr != nil {
				return loadErr
			}
			applyStringOverride(&configured.AttentionMode, *attention)
			applyStringOverride(&configured.NameMode, *nameMode)
			applyStringOverride(&configured.Actor, *actor)
			applyStringOverride(&configured.Role, *role)
			applyStringOverride(&configured.Peer, *peer)
			applyStringOverride(&configured.Project, *project)
			applyStringOverride(&configured.ProjectRoot, *projectRoot)
			applyStringOverride(&configured.Profile, *profile)
			applyStringOverride(&configured.Channel, *channelID)
			applyStringOverride(&configured.Socket, *socket)
			codexBinary := firstNonEmptyString(*clientBinary, configured.ClientBinary)
			spec, err = connector.BuildCodexLaunch(connector.CodexLaunchConfig{
				ConnectorConfig: configured, HollerBinary: executable, CodexBinary: codexBinary,
				ConnectorPath: path, RunID: *runID, LaunchTag: *launchTag, Takeover: *takeover, ExtraArgs: flags.Args(),
			})
		case "opencode":
			path := *configPath
			if strings.TrimSpace(path) == "" {
				path = connector.DefaultOpenCodeConnectorConfigPath()
			}
			configured, loadErr := connector.LoadOpenCodeConnectorConfig(path)
			if loadErr != nil {
				return loadErr
			}
			applyStringOverride(&configured.AttentionMode, *attention)
			applyStringOverride(&configured.NameMode, *nameMode)
			applyStringOverride(&configured.Actor, *actor)
			applyStringOverride(&configured.Role, *role)
			applyStringOverride(&configured.Peer, *peer)
			applyStringOverride(&configured.Project, *project)
			applyStringOverride(&configured.ProjectRoot, *projectRoot)
			applyStringOverride(&configured.Channel, *channelID)
			applyStringOverride(&configured.Socket, *socket)
			spec, err = connector.BuildOpenCodeLaunch(connector.OpenCodeLaunchConfig{
				ConnectorConfig: configured, HollerBinary: executable, OpenCodeBinary: *clientBinary,
				ConnectorPath: path, RunID: *runID, LaunchTag: *launchTag, Takeover: *takeover, ExtraArgs: flags.Args(),
			})
		default:
			return fmt.Errorf("unsupported connector harness %q", *harness)
		}
		if err != nil {
			return err
		}
		command := exec.CommandContext(ctx, spec.Command, spec.Args...)
		command.Env = mergedEnvironment(spec.Env)
		command.Stdin, command.Stdout, command.Stderr = stdin, stdout, stderr
		return command.Run()
	case "help", "-h", "--help":
		fmt.Fprintln(stdout, "usage: holler connector manifest|doctor|certify|setup|launch [options]")
		return nil
	default:
		return fmt.Errorf("unknown connector command %q", args[0])
	}
}

func applyStringOverride(target *string, value string) {
	if strings.TrimSpace(value) != "" {
		*target = value
	}
}

func mergedEnvironment(overrides map[string]string) []string {
	values := make(map[string]string)
	for _, entry := range os.Environ() {
		if index := strings.IndexByte(entry, '='); index > 0 {
			values[entry[:index]] = entry[index+1:]
		}
	}
	for key, value := range overrides {
		values[key] = value
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]string, 0, len(keys))
	for _, key := range keys {
		result = append(result, key+"="+values[key])
	}
	return result
}

func dialAPI(ctx context.Context, socketPath, actor, runID, clientName string) (*api.Client, error) {
	if strings.TrimSpace(socketPath) == "" {
		return nil, &bus.ValidationError{Field: "socket", Problem: "is required"}
	}
	return api.Dial(ctx, socketPath, api.Identity{Actor: actor, RunID: runID, Client: clientName, Build: buildinfo.Current()})
}

func dialAPIBinding(ctx context.Context, socketPath string, binding connector.RuntimeBinding, harness, sessionID, clientName string) (*api.Client, error) {
	if strings.TrimSpace(socketPath) == "" {
		return nil, &bus.ValidationError{Field: "socket", Problem: "is required"}
	}
	return api.Dial(ctx, socketPath, api.Identity{
		Actor: binding.Actor, RunID: binding.RunID, Client: clientName, Build: buildinfo.Current(),
		NameMode: binding.NameMode, ContinuityHandles: binding.ContinuityHandles(harness, sessionID),
		ProjectID: binding.Project, Harness: harness, Takeover: binding.Takeover,
	})
}

func environmentBoolean(name string) (bool, error) {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return false, nil
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return false, fmt.Errorf("%s must be a boolean: %w", name, err)
	}
	return parsed, nil
}

func commandFlags(name string, stderr io.Writer) *flag.FlagSet {
	flags := flag.NewFlagSet(name, flag.ContinueOnError)
	flags.SetOutput(stderr)
	return flags
}

func writeJSON(writer io.Writer, value interface{}) error {
	encoder := json.NewEncoder(writer)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}

func environmentOr(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func defaultSocketPath() string {
	if value := strings.TrimSpace(os.Getenv("HOLLER_SOCKET")); value != "" {
		return value
	}
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return filepath.Join(".holler", "holler.sock")
	}
	return filepath.Join(home, ".holler", "holler.sock")
}

func usage(writer io.Writer) {
	fmt.Fprintln(writer, `holler: local agent communication CLI

Usage:
  holler version
  holler --version
  holler setup claude [--yes|--remove]
  holler setup codex [--yes|--remove]
  holler lab list
  holler lab run [--all|--scenario NAME|--file PATH] [--output DIR]
  holler status [--socket PATH]
  holler conditions list [--all] | ack|snooze|claim --kind KIND --subject SUBJECT --generation N
  holler who [--socket PATH] [--limit 100] [--all]
  holler actor archive-preflight|archive|restore --actor ACTOR
  holler actor revoke-lease --actor ACTOR --message ID [--crash-grace 30s]
  holler migrate bare-harnesses
  holler profile --actor ACTOR --run RUN --role TEXT [--accepts KIND,KIND]
  holler adopt --actor LIVE_ACTOR --run RUN --from INACTIVE_ACTOR --idempotency-key KEY
  holler alias claim ALIAS ACTOR --policy-id ID --harness HARNESS | preflight ALIAS [ACTOR] | set ALIAS ACTOR | list | resolve ALIAS | remove ALIAS
  holler send   --socket PATH --actor ACTOR --run RUN --to ACTOR[,ACTOR] --idempotency-key KEY [options]
  holler inbox  --socket PATH --actor ACTOR
  holler claim  --socket PATH --actor ACTOR [--message ID] [--lease 5m]
  holler ack    --socket PATH --actor ACTOR --message ID --lease-token TOKEN
  holler extend --socket PATH --actor ACTOR --message ID --lease-token TOKEN [--lease 5m]
  holler nack   --socket PATH --actor ACTOR --message ID --lease-token TOKEN [--reason TEXT] [--final]
  holler events --socket PATH [--partition default] [--stream durable] [--after POSITION]
  holler mcp    --socket PATH --actor ACTOR --run RUN
  holler hook   --socket PATH --actor ACTOR --run RUN --harness codex|claude|opencode
  holler monitor --socket PATH --actor ACTOR --run RUN --harness claude
  holler session-end --socket PATH --actor ACTOR --run RUN --harness codex|claude|opencode
  holler connector manifest --harness codex|claude|opencode
  holler connector doctor --harness codex|claude|opencode [--profile async-peer|live-review]
  holler connector certify --harness codex|claude|opencode --actor ACTOR --run RUN [event cursors]
  holler connector setup --harness codex|claude|opencode --attention MODE --actor ACTOR [--apply]
  holler connector launch --harness codex|claude|opencode -- [harness arguments]

The separate hollerd process is the only SQLite owner. Every CLI, MCP, and hook operation uses
the versioned framed-JSON API over its Unix socket.`)
}
