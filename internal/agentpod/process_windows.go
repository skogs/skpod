//go:build windows

package agentpod

import (
	"errors"
	"syscall"
)

const (
	// processQueryLimitedInformation is supported on Windows Vista and newer.
	processQueryLimitedInformation = 0x1000
	// stillActive (259) is returned while the process is still running.
	stillActive = 259
)

func processAlive(pid int) bool {
	handle, err := syscall.OpenProcess(processQueryLimitedInformation, false, uint32(pid))
	if err != nil {
		// Access denied proves the process exists but is owned by another context.
		// Other failures, notably ERROR_INVALID_PARAMETER for a dead PID, indicate dead.
		return errors.Is(err, syscall.ERROR_ACCESS_DENIED)
	}
	defer syscall.CloseHandle(handle)
	var code uint32
	return syscall.GetExitCodeProcess(handle, &code) == nil && code == stillActive
}

