package lab

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"

	"github.com/72olabs/holler/internal/api"
)

type ProcessEvent struct {
	At      time.Time `json:"at"`
	Process string    `json:"process"`
	PID     int       `json:"pid,omitempty"`
	Event   string    `json:"event"`
	Detail  string    `json:"detail,omitempty"`
}

type supervisedProcess struct {
	name string
	cmd  *exec.Cmd
	done chan error
}

type Supervisor struct {
	mu        sync.Mutex
	processes []*supervisedProcess
	events    []ProcessEvent
}

func (s *Supervisor) Start(name, binary string, args, environment []string, stdoutPath, stderrPath string) error {
	stdout, err := os.OpenFile(stdoutPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	stderr, err := os.OpenFile(stderrPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		_ = stdout.Close()
		return err
	}
	cmd := exec.Command(binary, args...)
	cmd.Env = environment
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	configureProcessGroup(cmd)
	if err := cmd.Start(); err != nil {
		_ = stdout.Close()
		_ = stderr.Close()
		return err
	}
	process := &supervisedProcess{name: name, cmd: cmd, done: make(chan error, 1)}
	s.mu.Lock()
	s.processes = append(s.processes, process)
	s.events = append(s.events, ProcessEvent{At: time.Now().UTC(), Process: name, PID: cmd.Process.Pid, Event: "started"})
	s.mu.Unlock()
	go func() {
		err := cmd.Wait()
		_ = stdout.Close()
		_ = stderr.Close()
		s.mu.Lock()
		detail := ""
		if err != nil {
			detail = err.Error()
		}
		s.events = append(s.events, ProcessEvent{At: time.Now().UTC(), Process: name, PID: cmd.Process.Pid, Event: "exited", Detail: detail})
		s.mu.Unlock()
		process.done <- err
		close(process.done)
	}()
	return nil
}

func (s *Supervisor) WaitForDaemon(ctx context.Context, socket string) error {
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	var lastErr error
	for {
		client, err := api.Dial(ctx, socket, api.Identity{Actor: "lab-health", RunID: "lab-health", Client: "lab-supervisor/1"})
		if err == nil {
			_ = client.Close()
			return nil
		}
		lastErr = err
		select {
		case <-ctx.Done():
			return fmt.Errorf("daemon did not become ready: %w", errors.Join(ctx.Err(), lastErr))
		case <-ticker.C:
		}
	}
}

func (s *Supervisor) StopAll(ctx context.Context) []error {
	s.mu.Lock()
	processes := append([]*supervisedProcess(nil), s.processes...)
	s.mu.Unlock()
	errorsFound := make([]error, 0)
	for index := len(processes) - 1; index >= 0; index-- {
		process := processes[index]
		if err := stopProcess(ctx, process); err != nil {
			errorsFound = append(errorsFound, err)
		}
	}
	return errorsFound
}

func (s *Supervisor) Stop(ctx context.Context, name string) error {
	s.mu.Lock()
	var selected *supervisedProcess
	for index := len(s.processes) - 1; index >= 0; index-- {
		if s.processes[index].name == name {
			selected = s.processes[index]
			break
		}
	}
	s.mu.Unlock()
	if selected == nil {
		return fmt.Errorf("supervised process %q was not found", name)
	}
	return stopProcess(ctx, selected)
}

func stopProcess(ctx context.Context, process *supervisedProcess) error {
	select {
	case <-process.done:
		return nil
	default:
	}
	if err := terminateProcessGroup(process.cmd); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return fmt.Errorf("stop %s: %w", process.name, err)
	}
	select {
	case <-process.done:
		return nil
	case <-ctx.Done():
		_ = killProcessGroup(process.cmd)
		select {
		case <-process.done:
			return nil
		case <-time.After(time.Second):
			return fmt.Errorf("process %s pid %d did not exit", process.name, process.cmd.Process.Pid)
		}
	}
}

func (s *Supervisor) OrphanPIDs() []int {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]int, 0)
	for _, process := range s.processes {
		if processGroupAlive(process.cmd.Process.Pid) {
			result = append(result, process.cmd.Process.Pid)
		}
	}
	return result
}

func (s *Supervisor) Events() []ProcessEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]ProcessEvent(nil), s.events...)
}

func daemonLogPaths(evidenceDir string) (string, string) {
	return filepath.Join(evidenceDir, "hollerd.stdout.log"), filepath.Join(evidenceDir, "hollerd.stderr.log")
}
