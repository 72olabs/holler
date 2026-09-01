package sqlite

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/72olabs/holler/internal/bus"
	"golang.org/x/sys/unix"
)

func TestAcquireMigrationLockReportsDatabaseOwner(t *testing.T) {
	path := filepath.Join(t.TempDir(), "owned.sqlite3")
	owner, err := os.OpenFile(path+".migrate.lock", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close()
	if err := unix.Flock(int(owner.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		t.Fatal(err)
	}
	defer unix.Flock(int(owner.Fd()), unix.LOCK_UN) //nolint:errcheck

	lock, err := acquireMigrationLock(context.Background(), path, time.Now().Add(40*time.Millisecond))
	if lock != nil {
		lock.Close()
		t.Fatal("acquired a migration lock already held by another process")
	}
	if !errors.Is(err, bus.ErrDatabaseOwned) {
		t.Fatalf("error = %v, want ErrDatabaseOwned", err)
	}
	if !strings.Contains(err.Error(), bus.ErrDatabaseOwned.Error()) {
		t.Fatalf("error %q does not contain actionable ownership diagnostic", err)
	}
}
