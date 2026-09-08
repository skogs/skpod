package agentpod

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/gofrs/flock"
)

// Never delete the sidecar: replacing it can split owners across lock files.
// The OS releases a process's lock when it exits.
func lockMailbox(path string, exclusive bool) (string, *flock.Flock, *Error) {
	path, err := filepath.Abs(path)
	if err != nil {
		return "", nil, databaseError("resolve mailbox path", err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return "", nil, databaseError("create mailbox directory", err)
	}
	if resolved, err := filepath.EvalSymlinks(path); err == nil {
		path = resolved
	} else if errors.Is(err, os.ErrNotExist) {
		dir, err := filepath.EvalSymlinks(filepath.Dir(path))
		if err != nil {
			return "", nil, databaseError("resolve mailbox directory", err)
		}
		path = filepath.Join(dir, filepath.Base(path))
	} else {
		return "", nil, databaseError("resolve mailbox path", err)
	}
	lock := flock.New(path+".lock", flock.SetPermissions(0o600))
	var locked bool
	if exclusive {
		locked, err = lock.TryLock()
	} else {
		locked, err = lock.TryRLock()
	}
	if err != nil || !locked {
		_ = lock.Close()
		if err != nil {
			return "", nil, databaseError("lock mailbox", err)
		}
		return "", nil, newError("MAILBOX_ACTIVE", "mailbox is in use or being reset; stop mailbox commands before resetting, or retry after reset finishes", 4, 409)
	}
	return path, lock, nil
}

// ResetMailbox backs up a compatible, inactive mailbox and atomically empties
// it. The exclusive lifecycle lock covers inspection, backup, and reset.
// Incompatible schemas are refused: use a fresh SKPOD_DB instead.
func ResetMailbox(ctx context.Context, path string) (string, *Error) {
	path, lock, podErr := lockMailbox(path, true)
	if podErr != nil {
		return "", podErr
	}
	defer lock.Close()
	m, podErr := openMailbox(path)
	if podErr != nil {
		return "", podErr
	}
	defer m.Close()
	workers, podErr := m.ListContext(ctx)
	if podErr != nil {
		return "", podErr
	}
	if len(workers) != 0 {
		return "", newError("MAILBOX_ACTIVE", "stop active workers before resetting the mailbox", 4, 409)
	}
	backup := path + "." + time.Now().UTC().Format("20060102T150405.000000000Z") + ".bak"
	if _, err := m.db.ExecContext(ctx, `VACUUM INTO ?`, backup); err != nil {
		return "", databaseError("back up mailbox", err)
	}
	if err := os.Chmod(backup, 0o600); err != nil {
		return "", databaseError("protect mailbox backup", err)
	}
	podErr = m.write(ctx, "reset mailbox", func(tx *sql.Tx) *Error {
		if _, err := tx.ExecContext(ctx, `DELETE FROM tasks; DELETE FROM workers; DELETE FROM meta;`); err != nil {
			return databaseError("clear mailbox", err)
		}
		return nil
	})
	if podErr != nil {
		podErr.Message = fmt.Sprintf("%s; backup retained at %s", podErr.Message, backup)
		return backup, podErr
	}
	return backup, nil
}
