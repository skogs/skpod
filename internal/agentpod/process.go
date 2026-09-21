package agentpod

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// processIsAlive is swapped only in single-threaded tests.
var processIsAlive = processAlive

var currentHost = hostName

func hostName() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		return "local"
	}
	return host
}

func newSession(token string) string {
	return fmt.Sprintf("%s:%d:%s", currentHost(), os.Getpid(), token)
}

func sessionPID(session string) int {
	parts := strings.SplitN(session, ":", 3)
	switch len(parts) {
	case 3:
		// Format: host:pid:token
		host, pidStr := parts[0], parts[1]
		if host != currentHost() {
			// Different host, container, or namespace; PID liveness is not observable locally.
			return 0
		}
		pid, err := strconv.Atoi(pidStr)
		if err != nil || pid <= 0 {
			return 0
		}
		return pid
	case 2:
		// Format: pid:token
		pid, err := strconv.Atoi(parts[0])
		if err != nil || pid <= 0 {
			return 0
		}
		return pid
	default:
		// Legacy plain token
		return 0
	}
}

func listenerDead(w *workerRow) (int, bool) {
	if w == nil || w.state != WorkerListening {
		return 0, false
	}
	pid := sessionPID(w.session)
	return pid, pid > 0 && !processIsAlive(pid)
}

func listenerActive(w *workerRow, now time.Time) bool {
	if w == nil || w.state != WorkerListening {
		return false
	}
	if _, dead := listenerDead(w); dead {
		return false
	}
	return now.Sub(w.lastSeen) < ListenerLiveness
}

