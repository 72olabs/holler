//go:build darwin || linux

package fdliveness

import (
	"context"
	"os"
	"syscall"
	"testing"
	"time"
)

func TestWatchSocketPeerClose(t *testing.T) {
	descriptors, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	peer := os.NewFile(uintptr(descriptors[1]), "socket-peer")
	defer peer.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := Watch(ctx, descriptors[0])
	assertStillWatching(t, done)
	if err := peer.Close(); err != nil {
		t.Fatal(err)
	}
	assertWatchClosed(t, done)
}

func TestWatchPipePeerClose(t *testing.T) {
	descriptors := make([]int, 2)
	if err := syscall.Pipe(descriptors); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := Watch(ctx, descriptors[0])
	// Watch owns the read descriptor from here onward.
	assertStillWatching(t, done)
	if err := syscall.Close(descriptors[1]); err != nil {
		t.Fatal(err)
	}
	assertWatchClosed(t, done)
}

func TestWatchPipeOutputDetectsReaderClose(t *testing.T) {
	descriptors := make([]int, 2)
	if err := syscall.Pipe(descriptors); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := Watch(ctx, descriptors[1])
	// Production watches Claude's stdout/stderr write descriptors. Watch owns
	// the write descriptor from here onward.
	assertStillWatching(t, done)
	if err := syscall.Close(descriptors[0]); err != nil {
		t.Fatal(err)
	}
	assertWatchClosed(t, done)
}

func assertStillWatching(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
		t.Fatal("watcher closed while peer was alive")
	case <-time.After(150 * time.Millisecond):
	}
}

func assertWatchClosed(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("watcher did not detect peer closure")
	}
}
