package connector

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"html"
	"net"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/72olabs/holler/internal/api"
	"github.com/72olabs/holler/internal/buildinfo"
)

const defaultDaemonServiceLabel = "com.72olabs.hollerd"

type DaemonServiceConfig struct {
	HollerBinary string
	DaemonBinary string
	Socket       string
	Database     string
	Home         string
	GOOS         string
	Apply        bool
}

type DaemonServicePlan struct {
	SchemaVersion int      `json:"schema_version"`
	Applied       bool     `json:"applied"`
	Kind          string   `json:"kind"`
	Path          string   `json:"path"`
	Label         string   `json:"label"`
	DaemonBinary  string   `json:"daemon_binary"`
	Socket        string   `json:"socket"`
	Database      string   `json:"database"`
	Actions       []string `json:"actions"`
	Backups       []string `json:"backups,omitempty"`
}

type serviceRuntime struct {
	run      CommandRunner
	lookPath func(string) (string, error)
	probe    func(context.Context, string) (DaemonProcessInfo, bool, error)
}

type DaemonProcessInfo struct {
	PID     int
	BuildID string
}

type installedService struct {
	PID    int
	Target string
	Loaded bool
}

type ServiceOption func(*serviceRuntime)

func WithServiceCommandRunner(runner CommandRunner) ServiceOption {
	return func(runtime *serviceRuntime) { runtime.run = runner }
}

func WithServiceLookPath(lookPath func(string) (string, error)) ServiceOption {
	return func(runtime *serviceRuntime) { runtime.lookPath = lookPath }
}

func WithServiceDaemonProbe(probe func(context.Context, string) (DaemonProcessInfo, bool, error)) ServiceOption {
	return func(runtime *serviceRuntime) { runtime.probe = probe }
}

func SetupDaemonService(ctx context.Context, config DaemonServiceConfig, options ...ServiceOption) (DaemonServicePlan, error) {
	serviceRuntime := &serviceRuntime{run: runCommand, lookPath: exec.LookPath, probe: probeDaemonProcess}
	for _, option := range options {
		option(serviceRuntime)
	}
	if strings.TrimSpace(config.GOOS) == "" {
		config.GOOS = runtime.GOOS
	}
	if strings.TrimSpace(config.Home) == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return DaemonServicePlan{}, err
		}
		config.Home = home
	}
	if strings.TrimSpace(config.Socket) == "" {
		config.Socket = filepath.Join(config.Home, ".holler", "holler.sock")
	}
	if strings.TrimSpace(config.Database) == "" {
		config.Database = filepath.Join(filepath.Dir(config.Socket), "holler.sqlite3")
	}
	var err error
	config.DaemonBinary, err = resolveDaemonExecutable(config, serviceRuntime.lookPath)
	if err != nil {
		return DaemonServicePlan{}, err
	}

	plan := DaemonServicePlan{
		SchemaVersion: 1, Applied: config.Apply, Label: defaultDaemonServiceLabel,
		DaemonBinary: config.DaemonBinary, Socket: config.Socket, Database: config.Database,
		Actions: []string{"install a per-user Holler daemon service", "start or restart hollerd", "keep hollerd running across terminal and harness restarts"},
	}
	var contents []byte
	switch config.GOOS {
	case "darwin":
		plan.Kind = "launchd-user"
		plan.Path = filepath.Join(config.Home, "Library", "LaunchAgents", defaultDaemonServiceLabel+".plist")
		contents = []byte(renderLaunchAgent(plan, filepath.Join(config.Home, ".holler", "logs")))
	case "linux":
		plan.Kind = "systemd-user"
		plan.Label = "hollerd.service"
		plan.Path = filepath.Join(config.Home, ".config", "systemd", "user", plan.Label)
		contents = []byte(renderSystemdUserService(plan))
	default:
		return DaemonServicePlan{}, fmt.Errorf("automatic hollerd service installation is not supported on %s", config.GOOS)
	}
	if !config.Apply {
		return plan, nil
	}
	service, err := installedServiceState(ctx, config.GOOS, plan, serviceRuntime.run)
	if err != nil {
		return plan, err
	}
	process, connected, err := serviceRuntime.probe(ctx, config.Socket)
	if err != nil {
		return plan, err
	}
	// Daemons from before PID reporting decode pid as zero. When the service
	// manager has a running PID, treat that as a managed legacy daemon and
	// restart it into the current build instead of falsely calling it foreign.
	legacyManaged := connected && service.PID > 0 && process.PID == 0
	if connected && !legacyManaged && (service.PID == 0 || process.PID != service.PID) {
		return plan, fmt.Errorf("refusing to install %s: %s is already served by unmanaged hollerd pid %d; stop that daemon or choose a different --socket", plan.Kind, config.Socket, process.PID)
	}
	managedHealthy := connected && service.PID > 0 && process.PID == service.PID && process.BuildID == buildinfo.Current().ID()
	if err := os.MkdirAll(filepath.Dir(config.Database), 0o700); err != nil {
		return plan, err
	}
	changed := true
	if previous, readErr := os.ReadFile(plan.Path); readErr == nil {
		changed = !bytes.Equal(previous, contents)
	} else if !os.IsNotExist(readErr) {
		return plan, readErr
	}
	backup, err := writeConfigBytes(plan.Path, contents)
	if err != nil {
		return plan, err
	}
	if backup != "" {
		plan.Backups = append(plan.Backups, backup)
	}
	if config.GOOS == "darwin" {
		if err := os.MkdirAll(filepath.Join(config.Home, ".holler", "logs"), 0o700); err != nil {
			return plan, err
		}
		target := service.Target
		loaded := service.Loaded
		if loaded && changed {
			if err := runSetupCommand(ctx, serviceRuntime.run, "launchctl", "bootout", target); err != nil {
				return plan, err
			}
			loaded = false
		}
		if !loaded {
			uid, err := currentUID()
			if err != nil {
				return plan, err
			}
			if err := runSetupCommand(ctx, serviceRuntime.run, "launchctl", "bootstrap", "gui/"+uid, plan.Path); err != nil {
				return plan, err
			}
		} else if !managedHealthy {
			if err := runSetupCommand(ctx, serviceRuntime.run, "launchctl", "kickstart", "-k", target); err != nil {
				return plan, err
			}
		} else {
			return plan, nil
		}
		if err := waitForOwnedDaemon(ctx, config.Socket, func(ctx context.Context) (int, string, error) {
			return launchdServicePID(ctx, serviceRuntime.run, target)
		}, serviceRuntime.probe); err != nil {
			return plan, err
		}
		return plan, nil
	}
	if managedHealthy && !changed {
		return plan, nil
	}
	if err := runSetupCommand(ctx, serviceRuntime.run, "systemctl", "--user", "daemon-reload"); err != nil {
		return plan, err
	}
	if err := runSetupCommand(ctx, serviceRuntime.run, "systemctl", "--user", "enable", "--now", plan.Label); err != nil {
		return plan, err
	}
	if service.PID > 0 {
		if err := runSetupCommand(ctx, serviceRuntime.run, "systemctl", "--user", "restart", plan.Label); err != nil {
			return plan, err
		}
	}
	if err := runSetupCommand(ctx, serviceRuntime.run, "systemctl", "--user", "is-active", "--quiet", plan.Label); err != nil {
		return plan, err
	}
	if err := waitForOwnedDaemon(ctx, config.Socket, func(ctx context.Context) (int, string, error) {
		return systemdServicePID(ctx, serviceRuntime.run, plan.Label)
	}, serviceRuntime.probe); err != nil {
		return plan, err
	}
	return plan, nil
}

// RemoveDaemonService stops and removes the setup-owned per-user service. It
// intentionally preserves the database and logs so uninstalling a harness
// never destroys durable messages or diagnostic evidence.
func RemoveDaemonService(ctx context.Context, config DaemonServiceConfig, options ...ServiceOption) (DaemonServicePlan, error) {
	serviceRuntime := &serviceRuntime{run: runCommand, lookPath: exec.LookPath, probe: probeDaemonProcess}
	for _, option := range options {
		option(serviceRuntime)
	}
	if strings.TrimSpace(config.GOOS) == "" {
		config.GOOS = runtime.GOOS
	}
	if strings.TrimSpace(config.Home) == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return DaemonServicePlan{}, err
		}
		config.Home = home
	}
	if strings.TrimSpace(config.Socket) == "" {
		config.Socket = filepath.Join(config.Home, ".holler", "holler.sock")
	}
	plan := DaemonServicePlan{
		SchemaVersion: 1, Applied: config.Apply, Label: defaultDaemonServiceLabel, Socket: config.Socket,
		Actions: []string{"stop the per-user Holler daemon service", "remove the setup-owned service definition", "preserve the durable database and logs"},
	}
	switch config.GOOS {
	case "darwin":
		plan.Kind = "launchd-user"
		plan.Path = filepath.Join(config.Home, "Library", "LaunchAgents", defaultDaemonServiceLabel+".plist")
	case "linux":
		plan.Kind = "systemd-user"
		plan.Label = "hollerd.service"
		plan.Path = filepath.Join(config.Home, ".config", "systemd", "user", plan.Label)
	default:
		return DaemonServicePlan{}, fmt.Errorf("automatic hollerd service removal is not supported on %s", config.GOOS)
	}
	if !config.Apply {
		return plan, nil
	}
	if config.GOOS == "darwin" {
		uid, err := currentUID()
		if err != nil {
			return plan, err
		}
		target := "gui/" + uid + "/" + defaultDaemonServiceLabel
		_, _, exitCode, _ := serviceRuntime.run(ctx, "launchctl", "print", target)
		if exitCode == 0 {
			if err := runSetupCommand(ctx, serviceRuntime.run, "launchctl", "bootout", target); err != nil {
				return plan, err
			}
		}
	} else if _, err := os.Stat(plan.Path); err == nil {
		if err := runSetupCommand(ctx, serviceRuntime.run, "systemctl", "--user", "disable", "--now", plan.Label); err != nil {
			return plan, err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return plan, err
	}
	if err := os.Remove(plan.Path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return plan, err
	}
	if config.GOOS == "linux" {
		if err := runSetupCommand(ctx, serviceRuntime.run, "systemctl", "--user", "daemon-reload"); err != nil {
			return plan, err
		}
	}
	return plan, nil
}

func installedServiceState(ctx context.Context, goos string, plan DaemonServicePlan, runner CommandRunner) (installedService, error) {
	if goos == "darwin" {
		uid, err := currentUID()
		if err != nil {
			return installedService{}, err
		}
		target := "gui/" + uid + "/" + defaultDaemonServiceLabel
		pid, loaded, _, err := launchdServiceState(ctx, runner, target)
		return installedService{PID: pid, Target: target, Loaded: loaded}, err
	}
	pid, loaded, _, err := systemdServiceState(ctx, runner, plan.Label)
	return installedService{PID: pid, Target: plan.Label, Loaded: loaded}, err
}

func launchdServicePID(ctx context.Context, runner CommandRunner, target string) (int, string, error) {
	pid, _, detail, err := launchdServiceState(ctx, runner, target)
	return pid, detail, err
}

func launchdServiceState(ctx context.Context, runner CommandRunner, target string) (int, bool, string, error) {
	stdout, stderr, exitCode, err := runner(ctx, "launchctl", "print", target)
	if exitCode != 0 {
		return 0, false, strings.TrimSpace(firstNonEmpty(stderr, stdout, errorText(err))), nil
	}
	return parseServicePID(stdout), true, strings.TrimSpace(stdout), nil
}

func systemdServicePID(ctx context.Context, runner CommandRunner, label string) (int, string, error) {
	pid, _, detail, err := systemdServiceState(ctx, runner, label)
	return pid, detail, err
}

func systemdServiceState(ctx context.Context, runner CommandRunner, label string) (int, bool, string, error) {
	stdout, stderr, exitCode, err := runner(ctx, "systemctl", "--user", "show", "--property", "MainPID", "--value", label)
	if exitCode != 0 {
		return 0, false, strings.TrimSpace(firstNonEmpty(stderr, stdout, errorText(err))), nil
	}
	pid, _ := strconv.Atoi(strings.TrimSpace(stdout))
	return pid, true, strings.TrimSpace(stdout), nil
}

func parseServicePID(output string) int {
	fields := strings.Fields(output)
	for index := 0; index+2 < len(fields); index++ {
		if fields[index] == "pid" && fields[index+1] == "=" {
			pid, _ := strconv.Atoi(strings.Trim(fields[index+2], ";"))
			return pid
		}
	}
	return 0
}

func waitForOwnedDaemon(ctx context.Context, socket string,
	servicePID func(context.Context) (int, string, error),
	probe func(context.Context, string) (DaemonProcessInfo, bool, error)) error {
	const ownershipDeadline = 15 * time.Second
	deadline := time.Now().Add(ownershipDeadline)
	var detail string
	for time.Now().Before(deadline) {
		pid, serviceDetail, err := servicePID(ctx)
		detail = serviceDetail
		if err == nil && pid > 0 {
			process, connected, probeErr := probe(ctx, socket)
			if probeErr == nil && connected && process.PID == pid && process.BuildID == buildinfo.Current().ID() {
				return nil
			}
			if probeErr != nil {
				detail = probeErr.Error()
			} else if connected {
				detail = fmt.Sprintf("service pid %d but socket reports pid %d build %s", pid, process.PID, process.BuildID)
			}
		}
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return fmt.Errorf("daemon service did not own %s within %s: %s", socket, ownershipDeadline, detail)
}

func probeDaemonProcess(ctx context.Context, socket string) (DaemonProcessInfo, bool, error) {
	connection, err := net.DialTimeout("unix", socket, 200*time.Millisecond)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ECONNREFUSED) {
			return DaemonProcessInfo{}, false, nil
		}
		return DaemonProcessInfo{}, false, err
	}
	_ = connection.Close()
	client, err := api.Dial(ctx, socket, api.Identity{Actor: "service-setup", RunID: "service-probe", Client: "setup/0.1"})
	if err != nil {
		return DaemonProcessInfo{}, true, fmt.Errorf("listener at %s is not a compatible Holler daemon: %w", socket, err)
	}
	defer client.Close()
	info, err := client.DaemonInfo(ctx)
	if err != nil {
		return DaemonProcessInfo{}, true, err
	}
	return DaemonProcessInfo{PID: info.PID, BuildID: info.Build.ID()}, true, nil
}

func resolveDaemonExecutable(config DaemonServiceConfig, lookPath func(string) (string, error)) (string, error) {
	if strings.TrimSpace(config.DaemonBinary) != "" {
		path, err := stableExecutablePath(config.DaemonBinary, lookPath)
		if err != nil {
			return "", fmt.Errorf("resolve hollerd executable: %w", err)
		}
		return path, nil
	}
	if strings.TrimSpace(config.HollerBinary) != "" {
		candidate := filepath.Join(filepath.Dir(config.HollerBinary), "hollerd")
		if path, err := stableExecutablePath(candidate, lookPath); err == nil {
			return path, nil
		}
	}
	path, err := stableExecutablePath("hollerd", lookPath)
	if err != nil {
		return "", fmt.Errorf("resolve hollerd executable next to holler or on PATH: %w", err)
	}
	return path, nil
}

func currentUID() (string, error) {
	current, err := user.Current()
	if err != nil {
		return "", err
	}
	if _, err := strconv.ParseUint(current.Uid, 10, 32); err != nil {
		return "", fmt.Errorf("invalid current user ID %q", current.Uid)
	}
	return current.Uid, nil
}

func renderLaunchAgent(plan DaemonServicePlan, logDirectory string) string {
	escape := html.EscapeString
	return `<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>` + escape(plan.Label) + `</string>
  <key>ProgramArguments</key>
  <array>
    <string>` + escape(plan.DaemonBinary) + `</string>
    <string>--db</string>
    <string>` + escape(plan.Database) + `</string>
    <string>--socket</string>
    <string>` + escape(plan.Socket) + `</string>
  </array>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <true/>
  <key>ProcessType</key>
  <string>Background</string>
  <key>ThrottleInterval</key>
	<integer>5</integer>
  <key>StandardOutPath</key>
  <string>` + escape(filepath.Join(logDirectory, "hollerd.stdout.log")) + `</string>
  <key>StandardErrorPath</key>
  <string>` + escape(filepath.Join(logDirectory, "hollerd.stderr.log")) + `</string>
</dict>
</plist>
`
}

func renderSystemdUserService(plan DaemonServicePlan) string {
	quote := func(value string) string { return strconv.Quote(value) }
	return `[Unit]
Description=Holler local daemon

[Service]
Type=simple
ExecStart=` + quote(plan.DaemonBinary) + ` --db ` + quote(plan.Database) + ` --socket ` + quote(plan.Socket) + `
Restart=always
RestartSec=2

[Install]
WantedBy=default.target
`
}
