package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/72olabs/holler/internal/api"
	"github.com/72olabs/holler/internal/attention"
	"github.com/72olabs/holler/internal/buildinfo"
	"github.com/72olabs/holler/internal/bus"
	"github.com/72olabs/holler/internal/connector"
	store "github.com/72olabs/holler/internal/store/sqlite"
)

type Config struct {
	DatabasePath            string
	SocketPath              string
	CodexBinary             string
	CodexBinaryResolver     func() string
	NotificationTimeout     time.Duration
	Clock                   func() time.Time
	HarnessInstanceResolver api.HarnessInstanceResolver
	StaleUnreadAfter        time.Duration
	ArchiveAfter            time.Duration
}

func Run(ctx context.Context, config Config, ready io.Writer) error {
	config.DatabasePath = strings.TrimSpace(config.DatabasePath)
	config.SocketPath = strings.TrimSpace(config.SocketPath)
	if config.DatabasePath == "" {
		return &bus.ValidationError{Field: "db", Problem: "is required"}
	}
	if config.SocketPath == "" {
		return &bus.ValidationError{Field: "socket", Problem: "is required"}
	}
	if err := os.MkdirAll(filepath.Dir(config.SocketPath), 0o700); err != nil {
		return fmt.Errorf("create socket directory: %w", err)
	}
	if info, err := os.Lstat(config.SocketPath); err == nil {
		if info.Mode()&os.ModeSocket == 0 {
			return fmt.Errorf("socket path exists and is not a Unix socket: %s", config.SocketPath)
		}
		probe, dialErr := net.DialTimeout("unix", config.SocketPath, 250*time.Millisecond)
		if dialErr == nil {
			_ = probe.Close()
			return fmt.Errorf("hollerd is already listening at %s", config.SocketPath)
		}
		if !errors.Is(dialErr, syscall.ECONNREFUSED) && !errors.Is(dialErr, os.ErrNotExist) {
			return fmt.Errorf("cannot verify existing socket %s is stale: %w", config.SocketPath, dialErr)
		}
		if err := os.Remove(config.SocketPath); err != nil {
			return fmt.Errorf("remove stale socket %s: %w", config.SocketPath, err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect socket path: %w", err)
	}
	listener, err := net.Listen("unix", config.SocketPath)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", config.SocketPath, err)
	}
	defer listener.Close()
	defer os.Remove(config.SocketPath)
	if err := os.Chmod(config.SocketPath, 0o600); err != nil {
		return fmt.Errorf("secure socket: %w", err)
	}
	storeOptions := make([]store.Option, 0, 1)
	if config.Clock != nil {
		storeOptions = append(storeOptions, store.WithClock(config.Clock))
	}
	db, err := store.Open(ctx, config.DatabasePath, storeOptions...)
	if err != nil {
		return err
	}
	codexBinary := strings.TrimSpace(config.CodexBinary)
	if codexBinary == "" {
		codexBinary = "codex"
	}
	if ready != nil {
		if err := json.NewEncoder(ready).Encode(map[string]interface{}{
			"ok": true, "database": config.DatabasePath, "socket": config.SocketPath,
			"protocol": api.ProtocolVersion, "codex_binary": codexBinary,
		}); err != nil {
			_ = db.Close()
			return err
		}
	}
	notificationTimeout := config.NotificationTimeout
	if notificationTimeout <= 0 {
		notificationTimeout = 5 * time.Second
	}
	attentionBroker := attention.NewBroker()
	options := []connector.Option{connector.WithCodexBinary(codexBinary),
		connector.WithNotificationTimeout(notificationTimeout), connector.WithClaudeNotifier(attentionBroker)}
	if config.CodexBinaryResolver != nil {
		options = append(options, connector.WithCodexBinaryResolver(config.CodexBinaryResolver))
	}
	notifier := connector.New(db, options...)
	workerCtx, cancelWorker := context.WithCancel(ctx)
	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		runNotificationWorker(workerCtx, db, notifier, config.StaleUnreadAfter, config.ArchiveAfter)
	}()
	serverOptions := []api.ServerOption{api.WithAttentionBroker(attentionBroker)}
	if config.HarnessInstanceResolver != nil {
		serverOptions = append(serverOptions, api.WithHarnessInstanceResolver(config.HarnessInstanceResolver))
	}
	serveErr := api.NewServer(db, serverOptions...).Serve(ctx, listener)
	cancelWorker()
	<-workerDone
	closeErr := db.Close()
	if serveErr != nil {
		if closeErr != nil {
			return fmt.Errorf("%w; close database: %v", serveErr, closeErr)
		}
		return serveErr
	}
	return closeErr
}

type notificationQueue interface {
	ClaimNotification(context.Context) (bus.NotificationJob, error)
	FinishNotification(context.Context, bus.NotificationJob, bus.NotificationDisposition, string) error
}

type conditionReconciler interface {
	ReconcileStaleUnreadConditions(context.Context, time.Duration) error
}

type conditionObserver interface {
	ObserveCondition(context.Context, bus.ConditionObservation) (bus.OperatorCondition, error)
	ResolveCondition(context.Context, string, string) error
}

type lifecycleArchiver interface {
	ArchiveEligibleActors(context.Context, time.Duration) ([]string, error)
}

func runNotificationWorker(ctx context.Context, queue notificationQueue, notifier *connector.Runtime, staleAfter, archiveAfter time.Duration) {
	if staleAfter <= 0 {
		staleAfter = 15 * time.Minute
	}
	if archiveAfter <= 0 {
		archiveAfter = 30 * 24 * time.Hour
	}
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	reconcileTicker := time.NewTicker(30 * time.Second)
	defer reconcileTicker.Stop()
	if reconciler, ok := queue.(conditionReconciler); ok {
		_ = reconciler.ReconcileStaleUnreadConditions(ctx, staleAfter)
	}
	if archiver, ok := queue.(lifecycleArchiver); ok {
		_, _ = archiver.ArchiveEligibleActors(ctx, archiveAfter)
	}
	for {
		job, err := queue.ClaimNotification(ctx)
		if err == nil {
			buildID := buildinfo.Current().ID()
			notifyCtx := bus.WithCaller(ctx, bus.Caller{Client: "hollerd/0.1", BuildID: buildID, DaemonBuildID: buildID})
			attempts, notifyErr := notifier.Notify(notifyCtx, job.RecipientActor, job.Message)
			disposition, detail := notificationOutcome(attempts, notifyErr)
			_ = queue.FinishNotification(ctx, job, disposition, detail)
			if observer, ok := queue.(conditionObserver); ok {
				observeNotificationCondition(ctx, observer, job.RecipientActor, attempts)
			}
			continue
		}
		// No work and transient store errors both retry on the next tick. The
		// durable row remains the source of truth.
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-reconcileTicker.C:
			if reconciler, ok := queue.(conditionReconciler); ok {
				_ = reconciler.ReconcileStaleUnreadConditions(ctx, staleAfter)
			}
			if archiver, ok := queue.(lifecycleArchiver); ok {
				_, _ = archiver.ArchiveEligibleActors(ctx, archiveAfter)
			}
		}
	}
}

func observeNotificationCondition(ctx context.Context, observer conditionObserver, actor string, attempts []bus.NotificationAttempt) {
	for _, attempt := range attempts {
		if attempt.Result == "accepted" {
			_ = observer.ResolveCondition(ctx, "attention_unavailable", actor)
			return
		}
	}
	for _, attempt := range attempts {
		if attempt.Result != "unsupported" {
			continue
		}
		reason := "harness_wake_unsupported"
		detailLower := strings.ToLower(attempt.Detail)
		switch {
		case strings.Contains(detailLower, "startup-only"):
			reason = "startup_only_selected"
		case strings.Contains(detailLower, "unavailable") || strings.Contains(detailLower, "missing"):
			reason = "host_attention_adapter_missing"
		}
		details, _ := json.Marshal(map[string]interface{}{
			"actor": actor, "harness": attempt.Harness, "session_id": attempt.SessionID, "detail": attempt.Detail,
		})
		_, _ = observer.ObserveCondition(ctx, bus.ConditionObservation{
			Kind: "attention_unavailable", Subject: actor, ReasonCode: reason,
			Summary: "Automatic wake is unavailable for " + actor, Details: details,
		})
		return
	}
}

func notificationOutcome(attempts []bus.NotificationAttempt, notifyErr error) (bus.NotificationDisposition, string) {
	if notifyErr != nil {
		return bus.NotificationRetry, notifyErr.Error()
	}
	disposition := bus.NotificationComplete
	detail := ""
	for _, attempt := range attempts {
		if attempt.Result == "accepted" {
			return bus.NotificationAccepted, "accepted; awaiting recipient claim"
		}
		if attempt.Result == "retryable" {
			disposition = bus.NotificationRetry
			if detail == "" {
				detail = attempt.Detail
			}
		}
	}
	return disposition, detail
}
