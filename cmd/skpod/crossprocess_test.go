package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/skogs/skpod/internal/agentpod"
)

const cliModeEnv = "SKPOD_TEST_CLI"

// TestMain lets the test binary act as the skpod CLI when SKPOD_TEST_CLI=1, so
// tests can drive real, separate processes against one mailbox file.
func TestMain(m *testing.M) {
	if os.Getenv(cliModeEnv) == "1" {
		os.Exit(run(context.Background(), os.Args[1:], processInput(os.Stdin), os.Stdout, os.Stderr))
	}
	os.Exit(m.Run())
}

type cliProcess struct {
	cmd    *exec.Cmd
	stdout bytes.Buffer
	stderr bytes.Buffer
	err    error         // set before done is closed
	done   chan struct{} // closed once the process has exited and been reaped
}

func startCLI(t *testing.T, dbPath, stdin string, args ...string) *cliProcess {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	process := &cliProcess{cmd: exec.Command(executable, args...), done: make(chan struct{})}
	process.cmd.Env = append(os.Environ(), cliModeEnv+"=1", "SKPOD_DB="+dbPath)
	process.cmd.Stdin = strings.NewReader(stdin)
	process.cmd.Stdout = &process.stdout
	process.cmd.Stderr = &process.stderr
	if err := process.cmd.Start(); err != nil {
		t.Fatalf("start %v: %v", args, err)
	}
	go func() {
		process.err = process.cmd.Wait()
		close(process.done)
	}()
	t.Cleanup(func() {
		select {
		case <-process.done:
		default:
			_ = process.cmd.Process.Kill()
			<-process.done
		}
	})
	return process
}

// wait blocks for the process to exit and returns its exit code and stdout.
func (p *cliProcess) wait(t *testing.T, within time.Duration) (int, string) {
	t.Helper()
	select {
	case <-p.done:
		var exitErr *exec.ExitError
		switch {
		case p.err == nil:
			return 0, p.stdout.String()
		case errors.As(p.err, &exitErr):
			return exitErr.ExitCode(), p.stdout.String()
		default:
			t.Fatalf("%v: %v", p.cmd.Args[1:], p.err)
			return -1, ""
		}
	case <-time.After(within):
		t.Fatalf("%v did not exit within %s", p.cmd.Args[1:], within)
		return -1, ""
	}
}

func decodeEnvelope[T any](t *testing.T, output string) T {
	t.Helper()
	var envelope struct {
		OK    bool            `json:"ok"`
		Data  T               `json:"data"`
		Error *agentpod.Error `json:"error"`
	}
	decodeCLIOutput(t, output, &envelope)
	if !envelope.OK {
		t.Fatalf("command failed: %#v", envelope.Error)
	}
	return envelope.Data
}

func TestCLIProcessesInitializeFreshMailboxTogether(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fresh.db")
	var processes []*cliProcess
	for range 8 {
		processes = append(processes, startCLI(t, path, "", "list"))
	}
	for _, process := range processes {
		if code, output := process.wait(t, 10*time.Second); code != 0 {
			t.Fatalf("concurrent startup = %d: %s", code, output)
		}
	}
}

func TestCLIResetCannotInterruptAnotherProcess(t *testing.T) {
	path := filepath.Join(t.TempDir(), "busy.db")
	listener := startCLI(t, path, "", "listen", "reviewer", "--wait", "5s")
	deadline := time.Now().Add(3 * time.Second)
	for {
		status := startCLI(t, path, "", "status", "reviewer")
		_, output := status.wait(t, 5*time.Second)
		worker := decodeEnvelope[agentpod.WorkerStatus](t, output)
		if worker.State == agentpod.WorkerListening {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("listener did not enroll")
		}
		time.Sleep(10 * time.Millisecond)
	}
	reset := startCLI(t, path, "", "reset", "--yes")
	if code, output := reset.wait(t, 5*time.Second); code != 4 || !strings.Contains(output, "MAILBOX_ACTIVE") {
		t.Fatalf("reset with live process = %d: %s", code, output)
	}
	select {
	case <-listener.done:
		t.Fatal("reset stopped the listener")
	default:
	}
}

func TestCLIProcessesCoordinateSynchronousAskAcrossProcesses(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "cross.db")
	mb, podErr := agentpod.OpenMailbox(dbPath)
	if podErr != nil {
		t.Fatal(podErr)
	}
	t.Cleanup(func() { _ = mb.Close() })

	listener := startCLI(t, dbPath, "", "listen", "reviewer", "--wait", "10s")
	waitForCLIWorkerState(t, mb, "reviewer", agentpod.WorkerListening)

	duplicate := startCLI(t, dbPath, "", "listen", "reviewer", "--wait", "1s")
	if code, out := duplicate.wait(t, 5*time.Second); code != 4 || !strings.Contains(out, `"code":"ALREADY_LISTENING"`) {
		t.Fatalf("duplicate listener exit = %d, output = %s, want ALREADY_LISTENING exit 4", code, out)
	}
	if status, _ := mb.Status("reviewer"); status.State != agentpod.WorkerListening {
		t.Fatalf("first listener after duplicate exit = %#v, want listening", status)
	}

	asker := startCLI(t, dbPath, "inspect the change", "ask", "reviewer", "--from", "driver", "--stdin", "--timeout", "10s")
	code, out := listener.wait(t, 5*time.Second)
	if code != 0 {
		t.Fatalf("listener exit = %d, output = %s", code, out)
	}
	assigned := decodeEnvelope[agentpod.ListenResult](t, out)
	if assigned.State != "assigned" || assigned.Message == nil || assigned.Message.Payload != "inspect the change" {
		t.Fatalf("listener output = %#v, want assigned task", assigned)
	}
	waitForCLIWorkerState(t, mb, "reviewer", agentpod.WorkerWorking)

	replier := startCLI(t, dbPath, "looks fine", "reply", assigned.Message.ID, "--stdin")
	if code, out := replier.wait(t, 5*time.Second); code != 0 || !strings.Contains(out, `"state":"accepted"`) {
		t.Fatalf("reply exit = %d, output = %s", code, out)
	}
	code, out = asker.wait(t, 5*time.Second)
	if code != 0 {
		t.Fatalf("ask exit = %d, output = %s", code, out)
	}
	answer := decodeEnvelope[agentpod.AskResult](t, out)
	if answer.ID != assigned.Message.ID || answer.Payload != "looks fine" {
		t.Fatalf("ask output = %#v, want correlated reply", answer)
	}
	waitForCLIWorkerState(t, mb, "reviewer", agentpod.WorkerPolling)
}

func TestCLIProcessesQueueAndDrainAsyncTasksAcrossProcesses(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "async.db")
	mb, podErr := agentpod.OpenMailbox(dbPath)
	if podErr != nil {
		t.Fatal(podErr)
	}
	t.Cleanup(func() { _ = mb.Close() })

	runCLI := func(stdin string, args ...string) (int, string) {
		t.Helper()
		return startCLI(t, dbPath, stdin, args...).wait(t, 5*time.Second)
	}

	if code, out := runCLI("", "pull", "reviewer"); code != 0 || !strings.Contains(out, `"state":"idle"`) {
		t.Fatalf("initial pull exit = %d, output = %s", code, out)
	}
	var ids []string
	for _, payload := range []string{"first", "second"} {
		code, out := runCLI(payload, "send", "reviewer", "--from", "driver", "--stdin", "--work-timeout", "5s")
		if code != 0 {
			t.Fatalf("send exit = %d, output = %s", code, out)
		}
		ids = append(ids, decodeEnvelope[agentpod.SendResult](t, out).ID)
	}
	code, out := runCLI("", "status", "reviewer")
	if code != 0 {
		t.Fatalf("status exit = %d, output = %s", code, out)
	}
	if status := decodeEnvelope[agentpod.WorkerStatus](t, out); status.State != agentpod.WorkerPolling || status.QueueDepth != 2 {
		t.Fatalf("status = %#v, want polling with two queued tasks", status)
	}

	for index, id := range ids {
		code, out := runCLI("", "pull", "reviewer")
		if code != 0 {
			t.Fatalf("pull %d exit = %d, output = %s", index, code, out)
		}
		claimed := decodeEnvelope[agentpod.ListenResult](t, out)
		if claimed.State != "assigned" || claimed.Message == nil || claimed.Message.ID != id {
			t.Fatalf("pull %d = %#v, want assigned %s in send order", index, claimed, id)
		}
		if code, out := runCLI("done "+id, "reply", id, "--stdin"); code != 0 || !strings.Contains(out, `"state":"accepted"`) {
			t.Fatalf("reply %d exit = %d, output = %s", index, code, out)
		}
	}
	for _, id := range ids {
		code, out := runCLI("", "wait", id, "--timeout", "2s")
		if code != 0 {
			t.Fatalf("wait exit = %d, output = %s", code, out)
		}
		if result := decodeEnvelope[agentpod.WaitResult](t, out); result.Payload != "done "+id || result.ReplyState != "accepted" {
			t.Fatalf("wait result = %#v, want accepted reply for %s", result, id)
		}
	}
	if code, out := runCLI("", "list", "--all"); code != 0 || !strings.Contains(out, `"state":"between_listens"`) {
		t.Fatalf("list exit = %d, output = %s, want polling worker", code, out)
	}
}
