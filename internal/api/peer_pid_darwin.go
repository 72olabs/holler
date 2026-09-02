//go:build darwin

package api

import (
	"fmt"
	"net"
	"syscall"

	"golang.org/x/sys/unix"
)

func peerProcessID(connection net.Conn) (int, error) {
	syscallConnection, ok := connection.(syscall.Conn)
	if !ok {
		return 0, fmt.Errorf("connection does not expose a socket descriptor")
	}
	raw, err := syscallConnection.SyscallConn()
	if err != nil {
		return 0, fmt.Errorf("access socket descriptor: %w", err)
	}
	var pid int
	var socketErr error
	if err := raw.Control(func(fd uintptr) {
		pid, socketErr = unix.GetsockoptInt(int(fd), unix.SOL_LOCAL, unix.LOCAL_PEERPID)
	}); err != nil {
		return 0, fmt.Errorf("inspect peer process: %w", err)
	}
	if socketErr != nil {
		return 0, fmt.Errorf("inspect peer process: %w", socketErr)
	}
	if pid <= 0 {
		return 0, fmt.Errorf("peer process id is unavailable")
	}
	return pid, nil
}
