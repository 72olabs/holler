//go:build !darwin && !linux

package lab

import (
	"os"
	"os/exec"
)

func configureProcessGroup(command *exec.Cmd)       {}
func terminateProcessGroup(command *exec.Cmd) error { return command.Process.Signal(os.Interrupt) }
func killProcessGroup(command *exec.Cmd) error      { return command.Process.Kill() }
func processGroupAlive(pid int) bool                { return false }
