//go:build !darwin && !linux

package fdliveness

import (
	"context"
	"syscall"
)

// Duplicate reports that descriptor liveness is unsupported on this platform.
func Duplicate(_ syscall.RawConn) (int, bool) { return 0, false }

// Watch cannot observe descriptor liveness on unsupported platforms. Holler
// currently serves its universal API over Unix sockets on macOS and Linux.
func Watch(ctx context.Context, _ int) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		<-ctx.Done()
	}()
	return done
}
