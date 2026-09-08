package main

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/skogs/skpod/internal/agentpod"
)

type cliOutcome struct {
	exitCode int
	stdout   string
	stderr   string
}

func setupTestMailbox(t *testing.T) *agentpod.Mailbox {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "test.db")
	t.Setenv("SKPOD_DB", dbPath)
	mb, err := agentpod.OpenMailbox(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = mb.Close()
	})
	return mb
}

func TestRunCompletesCLIWorkerRoundTripAndReenrolls(t *testing.T) {
	mb := setupTestMailbox(t)

	listenCtx, cancelListen := context.WithCancel(context.Background())
	defer cancelListen()
	listening := beginCLI(listenCtx, []string{"listen", "reviewer"}, "")
	waitForCLIWorkerState(t, mb, "reviewer", agentpod.WorkerListening)

	asking := beginCLI(context.Background(), []string{
		"ask", "reviewer", "--from", "driver", "--stdin", "--timeout", "2s",
	}, "inspect the harmless change")
	listenResult := receiveCLI(t, listening)
	if listenResult.exitCode != 0 || listenResult.stderr != "" {
		t.Fatalf("listen exit = %d, stdout = %s, stderr = %s", listenResult.exitCode, listenResult.stdout, listenResult.stderr)
	}
	var listenEnvelope struct {
		OK   bool                  `json:"ok"`
		Data agentpod.ListenResult `json:"data"`
	}
	decodeCLIOutput(t, listenResult.stdout, &listenEnvelope)
	if !listenEnvelope.OK || listenEnvelope.Data.Message == nil || listenEnvelope.Data.Message.Payload != "inspect the harmless change" {
		t.Fatalf("listen output = %#v, want assigned task", listenEnvelope)
	}
	waitForCLIWorkerState(t, mb, "reviewer", agentpod.WorkerWorking)

	var statusOut bytes.Buffer
	statusExit := run(context.Background(), []string{"status", "reviewer"}, strings.NewReader(""), &statusOut, &bytes.Buffer{})
	if statusExit != 0 || !strings.Contains(statusOut.String(), `"state":"working"`) {
		t.Fatalf("status exit = %d, output = %s", statusExit, statusOut.String())
	}

	var replyOut bytes.Buffer
	replyExit := run(
		context.Background(),
		[]string{"reply", listenEnvelope.Data.Message.ID, "--stdin", "--echo"},
		strings.NewReader("review complete"),
		&replyOut,
		&bytes.Buffer{},
	)
	if replyExit != 0 || !strings.Contains(replyOut.String(), `"state":"accepted"`) || !strings.Contains(replyOut.String(), `"payload":"review complete"`) {
		t.Fatalf("reply exit = %d, output = %s", replyExit, replyOut.String())
	}

	askResult := receiveCLI(t, asking)
	if askResult.exitCode != 0 || askResult.stderr != "" {
		t.Fatalf("ask exit = %d, stdout = %s, stderr = %s", askResult.exitCode, askResult.stdout, askResult.stderr)
	}
	var askEnvelope struct {
		OK   bool               `json:"ok"`
		Data agentpod.AskResult `json:"data"`
	}
	decodeCLIOutput(t, askResult.stdout, &askEnvelope)
	if !askEnvelope.OK || askEnvelope.Data.Payload != "review complete" || askEnvelope.Data.ID != listenEnvelope.Data.Message.ID {
		t.Fatalf("ask output = %#v, want correlated response", askEnvelope)
	}
	waitForCLIWorkerState(t, mb, "reviewer", agentpod.WorkerPolling)
	var offlineOut bytes.Buffer
	if exitCode := run(context.Background(), []string{"status", "reviewer"}, strings.NewReader(""), &offlineOut, &bytes.Buffer{}); exitCode != 0 {
		t.Fatalf("offline status exit = %d, output = %s", exitCode, offlineOut.String())
	}
	if !strings.Contains(offlineOut.String(), `"state":"between_listens"`) || strings.Contains(offlineOut.String(), "expires_at") {
		t.Fatalf("offline status = %s, want no task metadata", offlineOut.String())
	}

	secondCtx, cancelSecond := context.WithCancel(context.Background())
	defer cancelSecond()
	secondListen := beginCLI(secondCtx, []string{"listen", "reviewer", "--wait", "0"}, "")
	waitForCLIWorkerState(t, mb, "reviewer", agentpod.WorkerListening)
	cancelSecond()
	secondResult := receiveCLI(t, secondListen)
	if secondResult.exitCode != 3 || !strings.Contains(secondResult.stdout, `"code":"CANCELED"`) {
		t.Fatalf("second listen result = %#v, want clean cancellation", secondResult)
	}
	waitForCLIWorkerState(t, mb, "reviewer", agentpod.WorkerOffline)
}

func TestRunFiniteListenStillExpires(t *testing.T) {
	mb := setupTestMailbox(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	listening := beginCLI(ctx, []string{"listen", "reviewer", "--wait", "1s"}, "")
	waitForCLIWorkerState(t, mb, "reviewer", agentpod.WorkerListening)
	result := receiveCLI(t, listening)
	if result.exitCode != 0 || !strings.Contains(result.stdout, `"state":"idle"`) {
		t.Fatalf("finite listener = %#v, want idle", result)
	}
	waitForCLIWorkerState(t, mb, "reviewer", agentpod.WorkerOffline)
}

func TestRunReportsOfflineWorkerWithoutQueueing(t *testing.T) {
	_ = setupTestMailbox(t)

	var stdout bytes.Buffer
	var stderr bytes.Buffer

	exitCode := run(
		context.Background(),
		[]string{"ask", "reviewer", "--from", "driver", "--stdin", "--timeout", "1s"},
		strings.NewReader("task"),
		&stdout,
		&stderr,
	)
	if exitCode != 4 || !strings.Contains(stdout.String(), `"code":"AGENT_OFFLINE"`) {
		t.Fatalf("exit = %d, stdout = %s, want AGENT_OFFLINE exit 4", exitCode, stdout.String())
	}
	if stderr.Len() != 0 {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestRunListShowsHubEnrolledWorkers(t *testing.T) {
	mb := setupTestMailbox(t)

	listenCtx, cancelListen := context.WithCancel(context.Background())
	defer cancelListen()
	listening := beginCLI(listenCtx, []string{"listen", "reviewer", "--wait", "2s"}, "")
	waitForCLIWorkerState(t, mb, "reviewer", agentpod.WorkerListening)
	project, err := currentProjectRoot()
	if err != nil {
		t.Fatal(err)
	}

	otherCtx, cancelOther := context.WithCancel(context.Background())
	defer cancelOther()
	otherProject := filepath.Join(t.TempDir(), "other-project")
	go func() {
		_, _ = mb.Listen(otherCtx, agentpod.ListenRequest{
			Agent:   "other-review",
			Project: otherProject,
			Wait:    2 * time.Second,
		})
	}()
	waitForCLIWorkerState(t, mb, "other-review", agentpod.WorkerListening)

	var stdout bytes.Buffer
	exitCode := run(context.Background(), []string{"list"}, strings.NewReader(""), &stdout, &bytes.Buffer{})
	if exitCode != 0 {
		t.Fatalf("list exit = %d, output = %s", exitCode, stdout.String())
	}
	var envelope struct {
		OK   bool                    `json:"ok"`
		Data []agentpod.WorkerStatus `json:"data"`
	}
	decodeCLIOutput(t, stdout.String(), &envelope)
	if !envelope.OK || len(envelope.Data) != 1 || envelope.Data[0].Agent != "reviewer" || envelope.Data[0].Project != project || envelope.Data[0].State != agentpod.WorkerListening {
		t.Fatalf("list output = %#v, want only the current-project reviewer", envelope)
	}

	stdout.Reset()
	exitCode = run(context.Background(), []string{"list", "--all"}, strings.NewReader(""), &stdout, &bytes.Buffer{})
	decodeCLIOutput(t, stdout.String(), &envelope)
	if exitCode != 0 || !envelope.OK || len(envelope.Data) != 2 || envelope.Data[0].Agent != "other-review" || envelope.Data[1].Agent != "reviewer" {
		t.Fatalf("list --all exit = %d, output = %#v, want both projects in name order", exitCode, envelope)
	}

	cancelListen()
	if outcome := receiveCLI(t, listening); outcome.exitCode != 3 {
		t.Fatalf("canceled listener = %#v, want exit 3", outcome)
	}
}

func TestRunCompletesCLIAsyncSendAndWait(t *testing.T) {
	mb := setupTestMailbox(t)

	listening := beginCLI(context.Background(), []string{"listen", "reviewer", "--wait", "2s"}, "")
	waitForCLIWorkerState(t, mb, "reviewer", agentpod.WorkerListening)
	var sendOut bytes.Buffer
	sendExit := run(
		context.Background(),
		[]string{"send", "reviewer", "--from", "driver", "--stdin", "--work-timeout", "2s"},
		strings.NewReader("async CLI task"),
		&sendOut,
		&bytes.Buffer{},
	)
	if sendExit != 0 {
		t.Fatalf("send exit = %d, output = %s", sendExit, sendOut.String())
	}
	var sentEnvelope struct {
		OK   bool                `json:"ok"`
		Data agentpod.SendResult `json:"data"`
	}
	decodeCLIOutput(t, sendOut.String(), &sentEnvelope)
	if !sentEnvelope.OK || sentEnvelope.Data.State != "dispatched" || sentEnvelope.Data.ID == "" {
		t.Fatalf("send output = %#v", sentEnvelope)
	}
	assigned := receiveCLI(t, listening)
	if assigned.exitCode != 0 || !strings.Contains(assigned.stdout, sentEnvelope.Data.ID) || !strings.Contains(assigned.stdout, "async CLI task") {
		t.Fatalf("listen result = %#v, want dispatched task", assigned)
	}

	var replyOut bytes.Buffer
	if exitCode := run(
		context.Background(),
		[]string{"reply", sentEnvelope.Data.ID, "--stdin"},
		strings.NewReader("async CLI result"),
		&replyOut,
		&bytes.Buffer{},
	); exitCode != 0 {
		t.Fatalf("reply exit = %d, output = %s", exitCode, replyOut.String())
	}

	var waitOut bytes.Buffer
	waitExit := run(
		context.Background(),
		[]string{"wait", sentEnvelope.Data.ID, "--timeout", "2s"},
		strings.NewReader(""),
		&waitOut,
		&bytes.Buffer{},
	)
	if waitExit != 0 {
		t.Fatalf("wait exit = %d, output = %s", waitExit, waitOut.String())
	}
	var waitEnvelope struct {
		OK   bool                `json:"ok"`
		Data agentpod.WaitResult `json:"data"`
	}
	decodeCLIOutput(t, waitOut.String(), &waitEnvelope)
	if !waitEnvelope.OK || waitEnvelope.Data.State != "completed" || waitEnvelope.Data.Payload != "async CLI result" {
		t.Fatalf("wait output = %#v, want completed result", waitEnvelope)
	}
}

func TestRunUsesPipedInputDefaultSenderAndTaskCommands(t *testing.T) {
	_ = setupTestMailbox(t)
	var out bytes.Buffer
	if code := run(context.Background(), []string{"pull", "reviewer"}, strings.NewReader(""), &out, &bytes.Buffer{}); code != 0 {
		t.Fatalf("pull = %d, %s", code, out.String())
	}
	out.Reset()
	if code := run(context.Background(), []string{"send", "reviewer", "--work-timeout", "2s"}, strings.NewReader("piped task"), &out, &bytes.Buffer{}); code != 0 {
		t.Fatalf("send = %d, %s", code, out.String())
	}
	var sent struct {
		Data agentpod.SendResult `json:"data"`
	}
	decodeCLIOutput(t, out.String(), &sent)
	if sent.Data.From != "driver" {
		t.Fatalf("default sender = %q", sent.Data.From)
	}
	out.Reset()
	if code := run(context.Background(), []string{"task", sent.Data.ID, "--format", "table"}, strings.NewReader(""), &out, &bytes.Buffer{}); code != 0 || !strings.Contains(out.String(), "piped") && !strings.Contains(out.String(), "pending") {
		t.Fatalf("task table = %d, %s", code, out.String())
	}
	out.Reset()
	if code := run(context.Background(), []string{"cancel", sent.Data.ID}, strings.NewReader(""), &out, &bytes.Buffer{}); code != 0 || !strings.Contains(out.String(), `"state":"canceled"`) {
		t.Fatalf("cancel = %d, %s", code, out.String())
	}
	out.Reset()
	if code := run(context.Background(), []string{"doctor", "--format", "table"}, strings.NewReader(""), &out, &bytes.Buffer{}); code != 0 || !strings.Contains(out.String(), "healthy") {
		t.Fatalf("doctor = %d, %s", code, out.String())
	}
}

func TestRunResetRequiresConfirmationAndCreatesBackup(t *testing.T) {
	path := filepath.Join(t.TempDir(), "reset.db")
	t.Setenv("SKPOD_DB", path)
	mb, podErr := agentpod.OpenMailbox(path)
	if podErr != nil {
		t.Fatal(podErr)
	}
	_ = mb.Close()
	var out bytes.Buffer
	if code := run(context.Background(), []string{"reset"}, strings.NewReader(""), &out, &bytes.Buffer{}); code != 2 {
		t.Fatalf("reset without confirmation = %d, %s", code, out.String())
	}
	out.Reset()
	if code := run(context.Background(), []string{"reset", "--yes"}, strings.NewReader(""), &out, &bytes.Buffer{}); code != 0 || !strings.Contains(out.String(), `"state":"reset"`) {
		t.Fatalf("reset = %d, %s", code, out.String())
	}
	backups, _ := filepath.Glob(path + ".*.bak")
	if len(backups) != 1 {
		t.Fatalf("backups = %v", backups)
	}
}

func TestRunRejectsInvalidInputBeforeReadingPayload(t *testing.T) {
	_ = setupTestMailbox(t)

	cases := []struct {
		name string
		args []string
	}{
		{name: "missing source", args: []string{"ask", "reviewer", "--from", "driver"}},
		{name: "two sources", args: []string{"ask", "reviewer", "--from", "driver", "--stdin", "--file", "task.txt"}},
		{name: "short timeout", args: []string{"ask", "reviewer", "--from", "driver", "--stdin", "--timeout", "999ms"}},
		{name: "negative listen wait", args: []string{"listen", "reviewer", "--wait", "-1s"}},
		{name: "short listen wait", args: []string{"listen", "reviewer", "--wait", "999ms"}},
		{name: "long listen wait", args: []string{"listen", "reviewer", "--wait", "31m"}},
		{name: "zero task work timeout", args: []string{"send", "reviewer", "--from", "driver", "--stdin", "--work-timeout", "0"}},
		{name: "zero result wait", args: []string{"wait", "0123456789abcdef0123456789abcdef", "--timeout", "0"}},
		{name: "positional payload", args: []string{"ask", "reviewer", "--from", "driver", "--stdin", "payload"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var stdout bytes.Buffer
			exitCode := run(context.Background(), tc.args, strings.NewReader(""), &stdout, &bytes.Buffer{})
			if exitCode != 2 {
				t.Fatalf("exit = %d, output = %s, want exit 2", exitCode, stdout.String())
			}
			var envelope errorEnvelope
			decodeCLIOutput(t, stdout.String(), &envelope)
			if envelope.OK || envelope.Error == nil || (envelope.Error.Code != "INVALID_INPUT" && envelope.Error.Code != "USAGE") {
				t.Fatalf("error = %#v, want INVALID_INPUT or USAGE", envelope.Error)
			}
		})
	}
}

func TestReadPayloadRejectsInteractiveStdinWithoutReading(t *testing.T) {
	reader := &terminalAwareReader{Reader: &failingReader{t: t}, terminal: true}
	payload, podErr := readPayload(reader, true, "")
	if payload != "" || podErr == nil || podErr.Code != "INTERACTIVE_STDIN" || podErr.ExitCode != 2 {
		t.Fatalf("readPayload() = %q, error = %#v, want INTERACTIVE_STDIN exit 2", payload, podErr)
	}
}

func TestRunHelpDocumentsWorkerLoopAndRecovery(t *testing.T) {
	var stdout bytes.Buffer
	exitCode := run(context.Background(), []string{"--help"}, strings.NewReader(""), &stdout, &bytes.Buffer{})
	if exitCode != 0 {
		t.Fatalf("exit = %d, want 0", exitCode)
	}
	help := stdout.String()
	for _, required := range []string{"skpod version", "skpod agent NAME", "skpod list [--all]", "skpod send AGENT", "skpod wait TASK_ID", "skpod task TASK_ID", "skpod cancel TASK_ID", "skpod listen AGENT [--wait 0]", "current project", `name contains "review"`, "absolute skpod executable path"} {
		if !strings.Contains(help, required) {
			t.Errorf("help does not contain %q:\n%s", required, help)
		}
	}
}

func TestRunPrintsVersion(t *testing.T) {
	previous := version
	version = "v1.2.3"
	t.Cleanup(func() { version = previous })

	for _, argument := range []string{"version", "--version", "-v"} {
		var stdout bytes.Buffer
		exitCode := run(context.Background(), []string{argument}, strings.NewReader(""), &stdout, &bytes.Buffer{})
		if exitCode != 0 || stdout.String() != "skpod v1.2.3\n" {
			t.Errorf("skpod %s: exit = %d, output = %q", argument, exitCode, stdout.String())
		}
	}
}

func TestResolveVersion(t *testing.T) {
	tests := []struct {
		name          string
		injected      string
		moduleVersion string
		want          string
	}{
		{name: "release linker value wins", injected: "v1.2.3", moduleVersion: "v1.2.2", want: "v1.2.3"},
		{name: "go install module version", injected: "dev", moduleVersion: "v1.2.3", want: "v1.2.3"},
		{name: "local development build", injected: "dev", moduleVersion: "(devel)", want: "dev"},
		{name: "missing build information", want: "dev"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := resolveVersion(test.injected, test.moduleVersion); got != test.want {
				t.Fatalf("resolveVersion(%q, %q) = %q, want %q", test.injected, test.moduleVersion, got, test.want)
			}
		})
	}
}

func TestRunSuggestsClosestCommandWithoutDumpingHelp(t *testing.T) {
	var stdout bytes.Buffer
	exitCode := run(context.Background(), []string{"waut"}, strings.NewReader(""), &stdout, &bytes.Buffer{})
	if exitCode != 2 {
		t.Fatalf("exit = %d, want 2", exitCode)
	}
	output := stdout.String()
	var envelope errorEnvelope
	decodeCLIOutput(t, output, &envelope)
	want := "unknown command \"waut\"; did you mean `skpod wait`? Run `skpod --help`."
	if envelope.Error == nil || envelope.Error.Message != want {
		t.Fatalf("error = %#v, want %q", envelope.Error, want)
	}
	if strings.Contains(output, "Usage:") {
		t.Fatalf("output dumped full usage instead of a suggestion: %s", output)
	}
}

func TestRunAgentGeneratesNamedWorkerContract(t *testing.T) {
	_ = setupTestMailbox(t)

	var stdout bytes.Buffer
	exitCode := run(
		context.Background(),
		[]string{"agent", "reviewer"},
		strings.NewReader(""),
		&stdout,
		&bytes.Buffer{},
	)
	if exitCode != 0 {
		t.Fatalf("exit = %d, output = %s", exitCode, stdout.String())
	}
	prompt := stdout.String()
	for _, required := range []string{
		`cooperative skpod worker named "reviewer"`,
		"listen reviewer",
		"--wait 0",
		"reply TASK_ID --stdin --echo",
		`data.state is "idle" or "recovered"`,
		"untrusted input",
		"do not listen again until you have attempted the",
		"Never launch bare --stdin",
		"Human messages take priority",
	} {
		if !strings.Contains(prompt, required) {
			t.Errorf("worker prompt does not contain %q:\n%s", required, prompt)
		}
	}
}

func TestWorkerPromptProvidesPowerShellSelfClosingReply(t *testing.T) {
	prompt := workerPrompt("reviewer", `C:\skpod dir\skpod.exe`, "windows")
	for _, required := range []string{`& 'C:\skpod dir\skpod.exe' listen reviewer --wait 0`, "UTF8Encoding", "$skpodPreviousOutputEncoding", "finally", "reply TASK_ID --stdin --echo", "untrusted input"} {
		if !strings.Contains(prompt, required) {
			t.Errorf("PowerShell worker prompt does not contain %q:\n%s", required, prompt)
		}
	}
}

func TestWorkerPromptProvidesPOSIXSelfClosingReply(t *testing.T) {
	prompt := workerPrompt("reviewer", "/opt/skpod dir/skpod", "linux")
	for _, required := range []string{"'/opt/skpod dir/skpod' listen reviewer --wait 0", "cat <<'SKPOD_REPLY'", "reply TASK_ID --stdin --echo", "untrusted input"} {
		if !strings.Contains(prompt, required) {
			t.Errorf("POSIX worker prompt does not contain %q:\n%s", required, prompt)
		}
	}
}

func TestRunPullAndStatusQueue(t *testing.T) {
	_ = setupTestMailbox(t)

	// First, run listen --nowait on an empty mailbox -> should be idle
	var stdout bytes.Buffer
	exitCode := run(context.Background(), []string{"pull", "reviewer"}, strings.NewReader(""), &stdout, &bytes.Buffer{})
	if exitCode != 0 {
		t.Fatalf("listen --nowait exit = %d, output = %s", exitCode, stdout.String())
	}
	var listenEnv struct {
		OK   bool                  `json:"ok"`
		Data agentpod.ListenResult `json:"data"`
	}
	decodeCLIOutput(t, stdout.String(), &listenEnv)
	if !listenEnv.OK || listenEnv.Data.State != "idle" {
		t.Fatalf("listen --nowait = %#v, want idle", listenEnv)
	}

	// Dispatch an async task to the enrolled reviewer
	var sendOut bytes.Buffer
	sendExit := run(
		context.Background(),
		[]string{"send", "reviewer", "--from", "driver", "--stdin", "--work-timeout", "5s"},
		strings.NewReader("task for nowait"),
		&sendOut,
		&bytes.Buffer{},
	)
	if sendExit != 0 {
		t.Fatalf("send exit = %d, output = %s", sendExit, sendOut.String())
	}
	var sendEnv struct {
		OK   bool                `json:"ok"`
		Data agentpod.SendResult `json:"data"`
	}
	decodeCLIOutput(t, sendOut.String(), &sendEnv)
	if !sendEnv.OK || sendEnv.Data.ID == "" {
		t.Fatalf("send output = %#v", sendEnv)
	}

	// Check status -> should show queue_depth = 1 and the task in queue
	var statusOut bytes.Buffer
	statusExit := run(context.Background(), []string{"status", "reviewer"}, strings.NewReader(""), &statusOut, &bytes.Buffer{})
	if statusExit != 0 {
		t.Fatalf("status exit = %d, output = %s", statusExit, statusOut.String())
	}
	var statusEnv struct {
		OK   bool                  `json:"ok"`
		Data agentpod.WorkerStatus `json:"data"`
	}
	decodeCLIOutput(t, statusOut.String(), &statusEnv)
	if !statusEnv.OK || statusEnv.Data.QueueDepth != 1 || len(statusEnv.Data.Queue) != 1 || statusEnv.Data.Queue[0].ID != sendEnv.Data.ID {
		t.Fatalf("status output = %#v, want 1 queued task %s", statusEnv, sendEnv.Data.ID)
	}

	// Run listen --nowait again -> should immediately claim the queued task
	stdout.Reset()
	exitCode = run(context.Background(), []string{"pull", "reviewer"}, strings.NewReader(""), &stdout, &bytes.Buffer{})
	if exitCode != 0 {
		t.Fatalf("listen --nowait exit = %d, output = %s", exitCode, stdout.String())
	}
	decodeCLIOutput(t, stdout.String(), &listenEnv)
	if !listenEnv.OK || listenEnv.Data.State != "assigned" || listenEnv.Data.Message == nil || listenEnv.Data.Message.ID != sendEnv.Data.ID {
		t.Fatalf("listen --nowait claimed = %#v, want assigned %s", listenEnv, sendEnv.Data.ID)
	}
}

func beginCLI(ctx context.Context, args []string, stdin string) <-chan cliOutcome {
	result := make(chan cliOutcome, 1)
	go func() {
		var stdout bytes.Buffer
		var stderr bytes.Buffer
		exitCode := run(ctx, args, strings.NewReader(stdin), &stdout, &stderr)
		result <- cliOutcome{exitCode: exitCode, stdout: stdout.String(), stderr: stderr.String()}
	}()
	return result
}

func receiveCLI(t *testing.T, result <-chan cliOutcome) cliOutcome {
	t.Helper()
	select {
	case outcome := <-result:
		return outcome
	case <-time.After(4 * time.Second):
		t.Fatal("timed out waiting for CLI command")
		return cliOutcome{}
	}
}

func decodeCLIOutput(t *testing.T, output string, destination any) {
	t.Helper()
	if err := json.Unmarshal([]byte(output), destination); err != nil {
		t.Fatalf("decode CLI output %q: %v", output, err)
	}
}

func waitForCLIWorkerState(t *testing.T, mb *agentpod.Mailbox, agent, want string) agentpod.WorkerStatus {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		status, podErr := mb.Status(agent)
		if podErr != nil {
			t.Fatalf("Status(%q) error = %v", agent, podErr)
		}
		if status.State == want {
			return status
		}
		if time.Now().After(deadline) {
			t.Fatalf("Status(%q) = %#v, want %q", agent, status, want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

type failingReader struct {
	t *testing.T
}

func (r *failingReader) Read([]byte) (int, error) {
	r.t.Fatal("unexpected read on interactive terminal stdin")
	return 0, nil
}
