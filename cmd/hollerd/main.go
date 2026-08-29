package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/72olabs/holler/internal/connector"
	"github.com/72olabs/holler/internal/daemon"
)

func main() {
	flags := flag.NewFlagSet("hollerd", flag.ExitOnError)
	dbPath := flags.String("db", defaultDatabasePath(), "SQLite database path")
	socketPath := flags.String("socket", defaultSocketPath(), "Unix socket path")
	codexBinary := flags.String("codex-binary", "", "Codex executable used by native queue notifications; defaults to the setup-recorded absolute path")
	notificationTimeout := flags.Duration("notification-timeout", 5*time.Second, "maximum duration of one harness notification attempt")
	flags.Parse(os.Args[1:])
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	resolvedCodexBinary := resolveCodexBinary(*codexBinary)
	if err := daemon.Run(ctx, daemon.Config{
		DatabasePath: *dbPath, SocketPath: *socketPath, CodexBinary: resolvedCodexBinary,
		CodexBinaryResolver: func() string { return resolveCodexBinary(*codexBinary) },
		NotificationTimeout: *notificationTimeout,
	}, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func resolveCodexBinary(explicit string) string {
	if value := strings.TrimSpace(explicit); value != "" {
		return value
	}
	if value := strings.TrimSpace(os.Getenv("HOLLER_CODEX_BIN")); value != "" {
		return value
	}
	if config, err := connector.LoadCodexConnectorConfig(""); err == nil {
		if value := strings.TrimSpace(config.ClientBinary); value != "" {
			return value
		}
	}
	if value, err := exec.LookPath("codex"); err == nil {
		return value
	}
	return "codex"
}

func defaultHome() string {
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return ".holler"
	}
	return filepath.Join(home, ".holler")
}

func defaultDatabasePath() string { return filepath.Join(defaultHome(), "holler.sqlite3") }
func defaultSocketPath() string {
	if value := strings.TrimSpace(os.Getenv("HOLLER_SOCKET")); value != "" {
		return value
	}
	return filepath.Join(defaultHome(), "holler.sock")
}
