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
	DatabasePath        string
	SocketPath          string
	CodexBinary         string
	CodexBinaryResolver func() string
	NotificationTimeout time.Duration
	Clock               func() time.Time
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
	defer db.Close()
	codexBinary := strings.TrimSpace(config.CodexBinary)
	if codexBinary == "" {
		codexBinary = "codex"
	}
	if ready != nil {
		if err := json.NewEncoder(ready).Encode(map[string]interface{}{
			"ok": true, "database": config.DatabasePath, "socket": config.SocketPath,
			"protocol": api.ProtocolVersion, "codex_binary": codexBinary,
		}); err != nil {
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
	go runNotificationWorker(ctx, db, notifier)
	return api.NewServer(db, api.WithAttentionBroker(attentionBroker)).Serve(ctx, listener)
}

type notificationQueue interface {
	ClaimNotification(context.Context) (bus.NotificationJob, error)
	FinishNotification(context.Context, bus.NotificationJob, bus.NotificationDisposition, string) error
}

func runNotificationWorker(ctx context.Context, queue notificationQueue, notifier *connector.Runtime) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		job, err := queue.ClaimNotification(ctx)
		if err == nil {
			buildID := buildinfo.Current().ID()
			notifyCtx := bus.WithCaller(ctx, bus.Caller{Client: "hollerd/0.1", BuildID: buildID, DaemonBuildID: buildID})
			attempts, notifyErr := notifier.Notify(notifyCtx, job.RecipientActor, job.Message)
			disposition, detail := notificationOutcome(attempts, notifyErr)
			_ = queue.FinishNotification(ctx, job, disposition, detail)
			continue
		}
		// No work and transient store errors both retry on the next tick. The
		// durable row remains the source of truth.
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
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
