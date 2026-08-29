//go:build darwin || linux

package fdliveness

import (
	"context"
	"syscall"

	"golang.org/x/sys/unix"
)

// Duplicate duplicates a descriptor while syscall.RawConn guarantees that it
// is valid. The caller owns the returned descriptor and should pass it to
// Watch, which closes it on exit.
func Duplicate(raw syscall.RawConn) (int, bool) {
	descriptor := -1
	var duplicateErr error
	if err := raw.Control(func(fd uintptr) {
		descriptor, duplicateErr = unix.Dup(int(fd))
	}); err != nil || duplicateErr != nil || descriptor < 0 {
		return 0, false
	}
	return descriptor, true
}

// Watch closes the returned channel after the peer closes or invalidates fd.
// It owns fd and observes kernel readiness only; it never reads or writes it.
func Watch(ctx context.Context, fd int) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer unix.Close(fd)
		descriptors := []unix.PollFd{{Fd: int32(fd), Events: peerCloseEvents}}
		for ctx.Err() == nil {
			count, err := unix.Poll(descriptors, 100)
			if err == unix.EINTR {
				continue
			}
			if err != nil || count > 0 && descriptors[0].Revents&peerCloseEvents != 0 {
				return
			}
		}
	}()
	return done
}
