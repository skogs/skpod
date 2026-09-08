package agentpod

import (
	"testing"
	"time"
)

const testProject = "C:/workspace/project"

type listenOutcome struct {
	result ListenResult
	err    *Error
}

func receiveListen(t *testing.T, result <-chan listenOutcome) listenOutcome {
	t.Helper()
	select {
	case outcome := <-result:
		return outcome
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for Listen()")
		return listenOutcome{}
	}
}

func assertErrorCode(t *testing.T, podErr *Error, code string, exitCode int) {
	t.Helper()
	if podErr == nil || podErr.Code != code || podErr.ExitCode != exitCode {
		t.Fatalf("error = %#v, want %s exit %d", podErr, code, exitCode)
	}
}
