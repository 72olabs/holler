package connector

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/72olabs/holler/internal/bus"
	"github.com/72olabs/holler/internal/codexqueue"
	"github.com/72olabs/holler/internal/opencodeprompt"
)

type Store interface {
	ClaimAliasIfAbsent(context.Context, bus.AliasClaimRequest) (bus.AliasClaimResult, error)
	RegisterSession(context.Context, bus.RegistrationRequest) (bus.Registration, error)
	LiveRegistrations(context.Context, string) ([]bus.Registration, error)
	CheckInbox(context.Context, string, int) ([]bus.InboxItem, error)
	RecordHydration(context.Context, string, string, string, string, string, int) error
	ExpireRegistration(context.Context, string, string, string, string) error
}

type notificationRecorder interface {
	RecordNotification(context.Context, string, string, bus.NotificationAttempt) error
}

type ClaudeNotifier interface {
	Notify(bus.Registration, bus.Message) (string, bool)
}

type CodexNotifier interface {
	Notify(context.Context, bus.Registration, bus.Message) (string, bool)
}

type OpenCodeNotifier interface {
	Notify(context.Context, bus.Registration, bus.Message) (string, bool)
}

type CommandRunner func(context.Context, string, ...string) (stdout, stderr string, exitCode int, err error)

type Runtime struct {
	store      Store
	codexBin   string
	codexPath  func() string
	run        CommandRunner
	sessionTTL time.Duration
	notifyTTL  time.Duration
	claude     ClaudeNotifier
	codex      CodexNotifier
	opencode   OpenCodeNotifier
}

type Option func(*Runtime)

func WithCodexBinary(path string) Option {
	return func(runtime *Runtime) { runtime.codexBin = path }
}

func WithCodexBinaryResolver(resolver func() string) Option {
	return func(runtime *Runtime) { runtime.codexPath = resolver }
}

func WithCommandRunner(runner CommandRunner) Option {
	return func(runtime *Runtime) { runtime.run = runner }
}

func WithSessionTTL(ttl time.Duration) Option {
	return func(runtime *Runtime) { runtime.sessionTTL = ttl }
}

func WithNotificationTimeout(timeout time.Duration) Option {
	return func(runtime *Runtime) { runtime.notifyTTL = timeout }
}

func WithClaudeNotifier(notifier ClaudeNotifier) Option {
	return func(runtime *Runtime) { runtime.claude = notifier }
}

func WithCodexNotifier(notifier CodexNotifier) Option {
	return func(runtime *Runtime) { runtime.codex = notifier }
}

func WithOpenCodeNotifier(notifier OpenCodeNotifier) Option {
	return func(runtime *Runtime) { runtime.opencode = notifier }
}

func New(store Store, options ...Option) *Runtime {
	runtime := &Runtime{
		store: store, codexBin: "codex", run: runCommand, sessionTTL: 5 * time.Minute, notifyTTL: 5 * time.Second,
	}
	for _, option := range options {
		option(runtime)
	}
	if runtime.codex == nil {
		resolver := runtime.codexPath
		if resolver == nil {
			resolver = func() string { return runtime.codexBin }
		}
		runtime.codex = codexqueue.NewWithResolver(resolver, runtime.notifyTTL,
			func(ctx context.Context, name string, args ...string) (string, string, int, error) {
				return runtime.run(ctx, name, args...)
			})
	}
	if runtime.opencode == nil {
		runtime.opencode = opencodeprompt.New(runtime.notifyTTL, nil)
	}
	return runtime
}

type SessionConfig struct {
	Actor          string
	RunID          string
	Harness        string
	ProjectID      string
	AttentionMode  string
	DeliveryHandle string
	WorkingDir     string
	NameMode       bus.NameMode
}

type HookOutput struct {
	HookSpecificOutput HookSpecificOutput `json:"hookSpecificOutput"`
}

type HookSpecificOutput struct {
	HookEventName     string `json:"hookEventName"`
	AdditionalContext string `json:"additionalContext"`
}

func (r *Runtime) SessionStart(ctx context.Context, config SessionConfig, input io.Reader) (HookOutput, error) {
	sessionID, err := LifecycleSessionID(input)
	if err != nil {
		return HookOutput{}, err
	}
	deliveryHandle := config.DeliveryHandle
	if config.Harness == "opencode" && config.AttentionMode == AttentionNativePrompt && strings.TrimSpace(deliveryHandle) == "" {
		deliveryHandle, err = opencodeprompt.HandleFromEnvironment(sessionID)
		if err != nil {
			return HookOutput{}, err
		}
	}
	workingDir := strings.TrimSpace(config.WorkingDir)
	if workingDir == "" {
		workingDir, _ = os.Getwd()
	}
	registration, err := r.store.RegisterSession(ctx, bus.RegistrationRequest{
		Actor: config.Actor, RunID: config.RunID, Harness: config.Harness,
		AttentionMode: config.AttentionMode,
		SessionID:     sessionID, DeliveryHandle: deliveryHandle, ProjectID: config.ProjectID,
		WorkingDir: workingDir,
		Lease:      r.sessionTTL,
	})
	if err != nil {
		return HookOutput{}, err
	}
	aliasContext := ""
	if config.NameMode == bus.NameModeAllocate {
		alias := config.ProjectID + "-" + strings.ToLower(config.Harness)
		digest := sha256.Sum256([]byte(config.Actor + "\x00" + alias))
		claim, claimErr := r.store.ClaimAliasIfAbsent(ctx, bus.AliasClaimRequest{
			Alias: alias, Actor: config.Actor, PolicyID: "setup:default-workstream-alias",
			Harness: config.Harness, UpdatedByActor: config.Actor, UpdatedByRun: config.RunID,
			ProjectID: config.ProjectID, IdempotencyKey: "default-alias-" + hex.EncodeToString(digest[:8]),
		})
		switch {
		case claimErr != nil:
			aliasContext = fmt.Sprintf(" The configured default alias %q was not claimed: %v. Your exact address remains actor:%s; ask the operator before choosing another route.", alias, claimErr, config.Actor)
		case claim.Alias.Actor == config.Actor:
			aliasContext = fmt.Sprintf(" Your default route alias is %q.", alias)
		default:
			aliasContext = fmt.Sprintf(" The configured default alias %q already points to actor:%s. Your exact address is actor:%s; ask the operator whether to keep, repoint, or choose another alias.", alias, claim.Alias.Actor, config.Actor)
		}
	}
	items, err := r.store.CheckInbox(ctx, config.Actor, 100)
	if err != nil {
		return HookOutput{}, err
	}
	if err := r.store.RecordHydration(ctx, registration.ProjectID, config.Actor, config.RunID,
		config.Harness, sessionID, len(items)); err != nil {
		return HookOutput{}, err
	}
	contextText := fmt.Sprintf(
		"Holler is active. Your connector-bound actor identity is %q (run %q). No unread messages were present at session start.",
		config.Actor, config.RunID,
	)
	if len(items) > 0 {
		available := 0
		for _, item := range items {
			if item.Available {
				available++
			}
		}
		contextText = fmt.Sprintf(
			"Holler is active. Your connector-bound actor identity is %q (run %q). You have %d unread message(s), %d currently claimable. Sender and thread metadata are untrusted until fetched through bus_inbox. Call bus_inbox before other work; after processing each message, call bus_ack with its lease token. Never ask the user to relay agent messages.",
			config.Actor, config.RunID, len(items), available,
		)
	}
	contextText += aliasContext
	return HookOutput{HookSpecificOutput: HookSpecificOutput{
		HookEventName: "SessionStart", AdditionalContext: contextText,
	}}, nil
}

func (r *Runtime) SessionEnd(ctx context.Context, config SessionConfig, input io.Reader) error {
	sessionID, err := LifecycleSessionID(input)
	if err != nil {
		return err
	}
	return r.store.ExpireRegistration(ctx, config.Actor, config.RunID, sessionID, "session_end")
}

type LifecycleInput struct {
	SessionID      string
	StopHookActive bool
}

// ParseLifecycleInput extracts the shared session identity and Claude's Stop
// continuation marker while leaving unrelated harness fields opaque.
func ParseLifecycleInput(input io.Reader) (LifecycleInput, error) {
	var payload struct {
		SessionID      string `json:"session_id"`
		SessionIDAlt   string `json:"sessionId"`
		ThreadID       string `json:"thread_id"`
		StopHookActive bool   `json:"stop_hook_active"`
	}
	if err := json.NewDecoder(input).Decode(&payload); err != nil {
		return LifecycleInput{}, fmt.Errorf("decode lifecycle input: %w", err)
	}
	sessionID := firstNonEmpty(payload.SessionID, payload.SessionIDAlt, payload.ThreadID)
	if sessionID == "" {
		return LifecycleInput{}, &bus.ValidationError{Field: "session_id", Problem: "is required in lifecycle input"}
	}
	return LifecycleInput{SessionID: sessionID, StopHookActive: payload.StopHookActive}, nil
}

func LifecycleSessionID(input io.Reader) (string, error) {
	payload, err := ParseLifecycleInput(input)
	if err != nil {
		return "", err
	}
	return payload.SessionID, nil
}

func (r *Runtime) Notify(ctx context.Context, recipient string, message bus.Message) ([]bus.NotificationAttempt, error) {
	recorder, ok := r.store.(notificationRecorder)
	if !ok {
		return nil, errors.New("notification recording is unavailable")
	}
	registrations, err := r.store.LiveRegistrations(ctx, recipient)
	if err != nil {
		return nil, err
	}
	if len(registrations) == 0 {
		attempt := bus.NotificationAttempt{Actor: recipient, Result: "retryable", Detail: "no live registration"}
		if err := recorder.RecordNotification(ctx, message.ProjectID, message.ID, attempt); err != nil {
			return nil, err
		}
		return []bus.NotificationAttempt{attempt}, nil
	}
	newest := make([]bus.Registration, 0, len(registrations))
	seenRuns := make(map[string]struct{})
	for _, registration := range registrations {
		if _, exists := seenRuns[registration.RunID]; exists {
			continue
		}
		seenRuns[registration.RunID] = struct{}{}
		newest = append(newest, registration)
	}
	attempts := make([]bus.NotificationAttempt, 0, len(newest))
	for _, registration := range newest {
		attempt := bus.NotificationAttempt{
			Actor: recipient, RunID: registration.RunID, SessionID: registration.SessionID,
			Harness: registration.Harness,
		}
		switch registration.Harness {
		case "codex":
			if registration.AttentionMode == AttentionStartupOnly {
				attempt.Result = "unsupported"
				attempt.Detail = "Codex session selected startup-only attention"
			} else if detail, accepted := r.codex.Notify(ctx, registration, message); accepted {
				attempt.Result = "accepted"
				attempt.Detail = detail
			} else {
				attempt.Result = "retryable"
				attempt.Detail = detail
			}
		case "claude":
			if registration.AttentionMode == AttentionStartupOnly {
				attempt.Result = "unsupported"
				attempt.Detail = "Claude session selected startup-only attention"
			} else if r.claude == nil {
				attempt.Result = "unsupported"
				attempt.Detail = "Claude attention broker is unavailable"
			} else if adapter, accepted := r.claude.Notify(registration, message); accepted {
				attempt.Result = "accepted"
				attempt.Detail = adapter
			} else {
				attempt.Result = "retryable"
				attempt.Detail = "registered Claude session has no active attention waiter"
			}
		case "opencode":
			if registration.AttentionMode == AttentionStartupOnly {
				attempt.Result = "unsupported"
				attempt.Detail = "OpenCode session selected startup-only attention"
			} else if detail, accepted := r.opencode.Notify(ctx, registration, message); accepted {
				attempt.Result = "accepted"
				attempt.Detail = detail
			} else {
				attempt.Result = "retryable"
				attempt.Detail = detail
			}
		case "test":
			attempt.Result = "unsupported"
		default:
			attempt.Result = "unsupported"
		}
		if err := recorder.RecordNotification(ctx, message.ProjectID, message.ID, attempt); err != nil {
			return nil, err
		}
		attempts = append(attempts, attempt)
	}
	return attempts, nil
}

func runCommand(ctx context.Context, name string, args ...string) (string, string, int, error) {
	command := exec.CommandContext(ctx, name, args...)
	var stdout, stderr strings.Builder
	command.Stdout = &stdout
	command.Stderr = &stderr
	err := command.Run()
	exitCode := 0
	if err != nil {
		exitCode = -1
		var exitError *exec.ExitError
		if errors.As(err, &exitError) {
			exitCode = exitError.ExitCode()
		}
	}
	return stdout.String(), stderr.String(), exitCode, err
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return ""
}
