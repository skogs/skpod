//go:build !windows

package agentpod

import (
	"errors"
	"os"
	"syscall"
)

// processAlive checks whether pid is running on POSIX systems via signal 0.
// Note: Unreaped zombies can answer signal 0; if a process is in zombie state,
// it will appear alive until reaped, after which it correctly reports dead.
func processAlive(pid int) bool {
	process, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	err = process.Signal(syscall.Signal(0))
	return err == nil || errors.Is(err, syscall.EPERM)
}
