package agentpod

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func beginMailboxListen(mb *Mailbox, ctx context.Context, agent string, wait time.Duration) <-chan listenOutcome {
	out := make(chan listenOutcome, 1)
	go func() {
		result, err := mb.Listen(ctx, ListenRequest{Agent: agent, Project: testProject, Wait: wait})
		out <- listenOutcome{result: result, err: err}
	}()
	return out
}

func waitForMailboxState(t *testing.T, mb *Mailbox, agent, want string) WorkerStatus {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
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

func mustSend(t *testing.T, mb *Mailbox, to, payload string, deadline time.Duration) SendResult {
	t.Helper()
	sent, podErr := mb.Send(context.Background(), SendRequest{From: "driver", To: to, Payload: payload, Deadline: deadline})
	if podErr != nil {
		t.Fatalf("Send(%q) error = %v", payload, podErr)
	}
	return sent
}

func mustExec(t *testing.T, mb *Mailbox, query string, args ...any) {
	t.Helper()
	if _, err := mb.db.Exec(query, args...); err != nil {
		t.Fatalf("exec %q: %v", query, err)
	}
}

func TestMailboxRejectsLateReplyAfterRecoveryAndKeepsNewListener(t *testing.T) {
	mb := newTestMailbox(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	first := beginMailboxListen(mb, ctx, "reviewer", 2*time.Second)
	waitForMailboxState(t, mb, "reviewer", WorkerListening)
	sent := mustSend(t, mb, "reviewer", "expire me", 20*time.Millisecond)
	if assigned := receiveListen(t, first); assigned.err != nil || assigned.result.State != "assigned" {
		t.Fatalf("Listen() = %#v, want assigned", assigned)
	}
	time.Sleep(50 * time.Millisecond)
	waitForMailboxState(t, mb, "reviewer", WorkerStale)

	recovered, podErr := mb.Listen(ctx, ListenRequest{Agent: "reviewer", Project: testProject, Wait: 20 * time.Millisecond})
	if podErr != nil || recovered.State != "recovered" || recovered.RecoveredTaskID != sent.ID {
		t.Fatalf("Listen() on stale worker = %#v, %v, want recovered %s", recovered, podErr, sent.ID)
	}

	second := beginMailboxListen(mb, ctx, "reviewer", 5*time.Second)
	waitForMailboxState(t, mb, "reviewer", WorkerListening)

	_, podErr = mb.Reply(ReplyRequest{ID: sent.ID, Payload: "too late"})
	assertErrorCode(t, podErr, "TASK_RECOVERED", 3)

	if status, _ := mb.Status("reviewer"); status.State != WorkerListening {
		t.Fatalf("new listener after late reply = %#v, want listening", status)
	}
	_, podErr = mb.Wait(ctx, sent.ID, 50*time.Millisecond)
	assertErrorCode(t, podErr, "TASK_RECOVERED", 3)

	cancel()
	if outcome := receiveListen(t, second); outcome.err == nil || outcome.err.Code != "CANCELED" {
		t.Fatalf("second listener = %#v, want CANCELED", outcome)
	}
}

func TestMailboxReplyRequiresClaimedTaskAndIsImmutable(t *testing.T) {
	mb := newTestMailbox(t)
	ctx := context.Background()

	if pulled, podErr := mb.Pull(ctx, "reviewer", testProject); podErr != nil || pulled.State != "idle" {
		t.Fatalf("Pull() = %#v, %v, want idle", pulled, podErr)
	}
	sent := mustSend(t, mb, "reviewer", "queued work", 2*time.Second)

	_, podErr := mb.Reply(ReplyRequest{ID: sent.ID, Payload: "early"})
	assertErrorCode(t, podErr, "TASK_NOT_CLAIMED", 4)

	pulled, podErr := mb.Pull(ctx, "reviewer", testProject)
	if podErr != nil || pulled.State != "assigned" || pulled.Message == nil || pulled.Message.ID != sent.ID {
		t.Fatalf("Pull() = %#v, %v, want assigned %s", pulled, podErr, sent.ID)
	}
	if reply, podErr := mb.Reply(ReplyRequest{ID: sent.ID, Payload: "first"}); podErr != nil || reply.State != "accepted" {
		t.Fatalf("Reply() = %#v, %v, want accepted", reply, podErr)
	}
	_, podErr = mb.Reply(ReplyRequest{ID: sent.ID, Payload: "second"})
	assertErrorCode(t, podErr, "TASK_ALREADY_COMPLETED", 4)

	result, podErr := mb.Wait(ctx, sent.ID, 50*time.Millisecond)
	if podErr != nil || result.Payload != "first" {
		t.Fatalf("Wait() = %#v, %v, want immutable first reply", result, podErr)
	}
	_, podErr = mb.Reply(ReplyRequest{ID: strings.Repeat("f", 32), Payload: "nobody"})
	assertErrorCode(t, podErr, "TASK_NOT_FOUND", 4)
}

func TestMailboxReplyThenPullKeepsOwnership(t *testing.T) {
	mb := newTestMailbox(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	listening := beginMailboxListen(mb, ctx, "reviewer", 2*time.Second)
	waitForMailboxState(t, mb, "reviewer", WorkerListening)
	head := mustSend(t, mb, "reviewer", "head", 2*time.Second)
	receiveListen(t, listening)
	queued := mustSend(t, mb, "reviewer", "queued", 2*time.Second)
	if status := waitForMailboxState(t, mb, "reviewer", WorkerWorking); status.QueueDepth != 1 {
		t.Fatalf("status while working = %#v, want one queued task", status)
	}

	if _, podErr := mb.Reply(ReplyRequest{ID: head.ID, Payload: "head done"}); podErr != nil {
		t.Fatalf("Reply(head) error = %v", podErr)
	}
	if status := waitForMailboxState(t, mb, "reviewer", WorkerPolling); status.QueueDepth != 1 {
		t.Fatalf("status after reply = %#v, want polling with one queued task", status)
	}

	pulled, podErr := mb.Pull(ctx, "reviewer", testProject)
	if podErr != nil || pulled.State != "assigned" || pulled.Message.ID != queued.ID {
		t.Fatalf("Pull() = %#v, %v, want assigned %s", pulled, podErr, queued.ID)
	}
	status := waitForMailboxState(t, mb, "reviewer", WorkerWorking)
	if status.TaskID != queued.ID || status.ClaimedAt.IsZero() || status.QueueDepth != 0 {
		t.Fatalf("status after pull = %#v, want owned claim on %s", status, queued.ID)
	}
	if reply, podErr := mb.Reply(ReplyRequest{ID: queued.ID, Payload: "queued done"}); podErr != nil || reply.State != "accepted" {
		t.Fatalf("Reply(queued) = %#v, %v, want accepted", reply, podErr)
	}
	waitForMailboxState(t, mb, "reviewer", WorkerPolling)
}

func TestMailboxCapsQueueForPollingWorkerAndListsIt(t *testing.T) {
	mb := newTestMailbox(t)
	ctx := context.Background()

	if _, podErr := mb.Pull(ctx, "reviewer", testProject); podErr != nil {
		t.Fatalf("Pull() error = %v", podErr)
	}
	workers, podErr := mb.List()
	if podErr != nil || len(workers) != 1 || workers[0].Agent != "reviewer" || workers[0].State != WorkerPolling || workers[0].Project != testProject {
		t.Fatalf("List() = %#v, %v, want one polling reviewer", workers, podErr)
	}
	for i := 0; i < MaxQueueDepth; i++ {
		mustSend(t, mb, "reviewer", "fill", 2*time.Second)
	}
	_, podErr = mb.Send(ctx, SendRequest{From: "driver", To: "reviewer", Payload: "overflow", Deadline: 2 * time.Second})
	assertErrorCode(t, podErr, "QUEUE_FULL", 4)

	status, _ := mb.Status("reviewer")
	if status.State != WorkerPolling || status.QueueDepth != MaxQueueDepth || len(status.Queue) != MaxQueueDepth {
		t.Fatalf("Status() = %#v, want polling with a full queue", status)
	}
	if status.Queue[0].WorkTimeout != "2s" {
		t.Fatalf("queued timing = %#v, want a 2s work timeout", status.Queue[0])
	}
	_, podErr = mb.Ask(ctx, AskRequest{From: "driver", To: "reviewer", Payload: "sync?", Timeout: time.Second})
	assertErrorCode(t, podErr, "AGENT_OFFLINE", 4)
	if !strings.Contains(podErr.Message, "between listens") {
		t.Fatalf("Ask() to polling worker message = %q, want between-listens guidance", podErr.Message)
	}
}

func TestMailboxAsyncDeadlineStartsAtClaim(t *testing.T) {
	mb := newTestMailbox(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	listening := beginMailboxListen(mb, ctx, "reviewer", 2*time.Second)
	waitForMailboxState(t, mb, "reviewer", WorkerListening)
	head := mustSend(t, mb, "reviewer", "head", 2*time.Second)
	receiveListen(t, listening)
	queued := mustSend(t, mb, "reviewer", "queued", 200*time.Millisecond)

	time.Sleep(300 * time.Millisecond) // longer than the queued task's whole allowance
	if _, podErr := mb.Reply(ReplyRequest{ID: head.ID, Payload: "head done"}); podErr != nil {
		t.Fatalf("Reply(head) error = %v", podErr)
	}
	claimedAt := time.Now()
	pulled, podErr := mb.Pull(ctx, "reviewer", testProject)
	if podErr != nil || pulled.State != "assigned" || pulled.Message.ID != queued.ID {
		t.Fatalf("Pull() = %#v, %v, want assigned %s", pulled, podErr, queued.ID)
	}
	if remaining := pulled.Message.Deadline.Sub(claimedAt); remaining < 150*time.Millisecond || remaining > 250*time.Millisecond {
		t.Fatalf("claimed deadline leaves %s, want the full 200ms allowance from claim", remaining)
	}
	if status, _ := mb.Status("reviewer"); status.State != WorkerWorking {
		t.Fatalf("Status() right after claim = %#v, want working, not stale", status)
	}
	if reply, podErr := mb.Reply(ReplyRequest{ID: queued.ID, Payload: "in time"}); podErr != nil || reply.State != "accepted" {
		t.Fatalf("Reply(queued) = %#v, %v, want accepted", reply, podErr)
	}
}

func TestMailboxPrunesAbandonedRowsAndRecoversOrphans(t *testing.T) {
	mb := newTestMailbox(t)
	now := time.Now().UTC()
	old := now.Add(-25 * time.Hour).UnixMilli()
	crashed := strings.Repeat("a", 32)
	orphan := strings.Repeat("b", 32)
	stalePending := strings.Repeat("c", 32)
	oldOutcome := strings.Repeat("d", 32)

	mustExec(t, mb, `INSERT INTO workers VALUES ('killed', ?, 'listening', 's1', NULL, ?)`, testProject, old)
	mustExec(t, mb, `INSERT INTO workers VALUES ('poller', ?, 'between_listens', 's2', NULL, ?)`, testProject, old)
	insertTask := `INSERT INTO tasks (id, from_agent, to_agent, payload, is_async, status, deadline_ms, deadline, created_at, claimed_at, updated_at, completed_at)
		VALUES (?, 'driver', ?, 'p', 1, ?, 1000, ?, ?, ?, ?, ?)`
	mustExec(t, mb, insertTask, crashed, "crashed", "working", old, old, old, old, nil)
	mustExec(t, mb, `INSERT INTO workers VALUES ('crashed', ?, 'working', 's3', ?, ?)`, testProject, crashed, old)
	mustExec(t, mb, insertTask, orphan, "vanished", "working", now.Add(time.Hour).UnixMilli(), old, old, old, nil)
	mustExec(t, mb, insertTask, stalePending, "nobody", "pending", old, old, nil, old, nil)
	mustExec(t, mb, insertTask, oldOutcome, "gone", "completed", old, old, old, old, old)

	workers, podErr := mb.List()
	if podErr != nil || workers == nil || len(workers) != 0 {
		t.Fatalf("List() = %#v, %v, want no present workers", workers, podErr)
	}
	var workerRows int
	if err := mb.db.QueryRow(`SELECT count(*) FROM workers`).Scan(&workerRows); err != nil || workerRows != 0 {
		t.Fatalf("worker rows after prune = %d, %v, want 0", workerRows, err)
	}
	for _, id := range []string{crashed, orphan} {
		_, podErr := mb.Wait(context.Background(), id, 20*time.Millisecond)
		assertErrorCode(t, podErr, "TASK_RECOVERED", 3)
	}
	_, podErr = mb.Wait(context.Background(), stalePending, 20*time.Millisecond)
	assertErrorCode(t, podErr, "TASK_EXPIRED", 3)
	_, podErr = mb.Wait(context.Background(), oldOutcome, 20*time.Millisecond)
	assertErrorCode(t, podErr, "TASK_NOT_FOUND", 4)
}

func TestMailboxRejectsReplyWhenNoWorkerHoldsClaim(t *testing.T) {
	mb := newTestMailbox(t)
	now := time.Now().UTC()
	id := strings.Repeat("e", 32)
	mustExec(t, mb, `INSERT INTO tasks
		(id, from_agent, to_agent, payload, is_async, status, deadline_ms, deadline, created_at, claimed_at, updated_at)
		VALUES (?, 'driver', 'vanished', 'work', 1, 'working', 1000, ?, ?, ?, ?)`,
		id, now.Add(time.Second).UnixMilli(), now.UnixMilli(), now.UnixMilli(), now.UnixMilli())

	_, podErr := mb.Reply(ReplyRequest{ID: id, Payload: "should not land"})
	assertErrorCode(t, podErr, "TASK_NOT_HELD", 4)
}

func TestMailboxTimeoutFinalizersReturnCommittedResults(t *testing.T) {
	mb := newTestMailbox(t)
	ctx := context.Background()
	session := "test-session"
	if _, podErr := mb.enroll(ctx, "reviewer", testProject, session); podErr != nil {
		t.Fatal(podErr)
	}

	syncTask, podErr := mb.dispatch(ctx, "ask", "driver", "reviewer", "sync", time.Second, false)
	if podErr != nil {
		t.Fatal(podErr)
	}
	if claimed, podErr := mb.claimIfPending(ctx, "reviewer", testProject, session); podErr != nil || claimed == nil || claimed.ID != syncTask.ID {
		t.Fatalf("claim sync = %#v, %v", claimed, podErr)
	}
	if _, podErr := mb.Reply(ReplyRequest{ID: syncTask.ID, Payload: "sync done"}); podErr != nil {
		t.Fatal(podErr)
	}
	if result, podErr := mb.finalizeAsk(syncTask.ID, AskRequest{From: "driver", To: "reviewer"}, nil); podErr != nil || result.Payload != "sync done" {
		t.Fatalf("finalizeAsk = %#v, %v", result, podErr)
	}

	asyncTask := mustSend(t, mb, "reviewer", "async", time.Second)
	if claimed, podErr := mb.Pull(ctx, "reviewer", testProject); podErr != nil || claimed.Message.ID != asyncTask.ID {
		t.Fatalf("claim async = %#v, %v", claimed, podErr)
	}
	if _, podErr := mb.Reply(ReplyRequest{ID: asyncTask.ID, Payload: "async done"}); podErr != nil {
		t.Fatal(podErr)
	}
	if result, podErr := mb.finalizeWait(asyncTask.ID); podErr != nil || result.Payload != "async done" {
		t.Fatalf("finalizeWait = %#v, %v", result, podErr)
	}
}

func TestMailboxSeparateHandlesShareOneMailbox(t *testing.T) {
	// Each handle owns its own SQLite connection, exactly like a separate
	// skpod process, so every check-then-write race is real here.
	path := filepath.Join(t.TempDir(), "shared.db")
	open := func() *Mailbox {
		mb, podErr := OpenMailbox(path)
		if podErr != nil {
			t.Fatalf("OpenMailbox() error = %v", podErr)
		}
		t.Cleanup(func() { _ = mb.Close() })
		return mb
	}
	a, b, observer := open(), open(), open()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	listening := beginMailboxListen(a, ctx, "reviewer", 3*time.Second)
	waitForMailboxState(t, observer, "reviewer", WorkerListening)

	_, podErr := b.Listen(ctx, ListenRequest{Agent: "reviewer", Project: testProject, Wait: 20 * time.Millisecond})
	assertErrorCode(t, podErr, "ALREADY_LISTENING", 4)
	if status, _ := observer.Status("reviewer"); status.State != WorkerListening {
		t.Fatalf("winner after duplicate exit = %#v, want listening", status)
	}

	head := mustSend(t, b, "reviewer", "head", 2*time.Second)
	if assigned := receiveListen(t, listening); assigned.err != nil || assigned.result.Message.ID != head.ID {
		t.Fatalf("Listen() = %#v, want assigned %s", assigned, head.ID)
	}

	var accepted, rejected atomic.Int32
	var unexpected atomic.Value
	var wg sync.WaitGroup
	handles := []*Mailbox{a, b, observer}
	for i := 0; i < 3*MaxQueueDepth; i++ {
		wg.Add(1)
		go func(mb *Mailbox) {
			defer wg.Done()
			_, podErr := mb.Send(ctx, SendRequest{From: "driver", To: "reviewer", Payload: "queued", Deadline: 2 * time.Second})
			switch {
			case podErr == nil:
				accepted.Add(1)
			case podErr.Code == "QUEUE_FULL":
				rejected.Add(1)
			default:
				unexpected.Store(podErr)
			}
		}(handles[i%len(handles)])
	}
	wg.Wait()
	if err := unexpected.Load(); err != nil {
		t.Fatalf("concurrent Send() error = %v", err)
	}
	if accepted.Load() != MaxQueueDepth || rejected.Load() != 2*MaxQueueDepth {
		t.Fatalf("concurrent sends accepted = %d, rejected = %d, want exactly %d accepted", accepted.Load(), rejected.Load(), MaxQueueDepth)
	}
	if status, _ := observer.Status("reviewer"); status.QueueDepth != MaxQueueDepth {
		t.Fatalf("Status() = %#v, want a full queue", status)
	}
}

func TestMailboxAskWithdrawsUnclaimedTasks(t *testing.T) {
	mb := newTestMailbox(t)
	ctx := context.Background()
	// A listener row with a fresh heartbeat but no process behind it.
	mustExec(t, mb, `INSERT INTO workers VALUES ('ghost', ?, 'listening', 'dead', NULL, ?)`, testProject, time.Now().UnixMilli())

	_, podErr := mb.Ask(ctx, AskRequest{From: "driver", To: "ghost", Payload: "anyone?", Timeout: 100 * time.Millisecond})
	assertErrorCode(t, podErr, "TASK_TIMEOUT", 3)
	if !strings.Contains(podErr.Message, "withdrawn") {
		t.Fatalf("Ask() timeout message = %q, want withdrawn task", podErr.Message)
	}
	if status, _ := mb.Status("ghost"); status.QueueDepth != 0 {
		t.Fatalf("Status() after timed-out ask = %#v, want empty queue", status)
	}

	askCtx, cancelAsk := context.WithCancel(ctx)
	go func() {
		time.Sleep(60 * time.Millisecond)
		cancelAsk()
	}()
	_, podErr = mb.Ask(askCtx, AskRequest{From: "driver", To: "ghost", Payload: "anyone?", Timeout: 2 * time.Second})
	assertErrorCode(t, podErr, "CANCELED", 3)
	if strings.Contains(podErr.Message, "delivery occurred") {
		t.Fatalf("cancel before claim reported delivery: %q", podErr.Message)
	}
	if status, _ := mb.Status("ghost"); status.QueueDepth != 0 {
		t.Fatalf("Status() after canceled ask = %#v, want rolled-back task", status)
	}

	// A synchronous ask never queues behind another pending task.
	mustSend(t, mb, "ghost", "queued first", 2*time.Second)
	_, podErr = mb.Ask(ctx, AskRequest{From: "driver", To: "ghost", Payload: "me too", Timeout: time.Second})
	assertErrorCode(t, podErr, "AGENT_BUSY", 4)
}

func TestMailboxListenerReenrollsAfterItsRowIsGarbageCollected(t *testing.T) {
	mb := newTestMailbox(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	listening := beginMailboxListen(mb, ctx, "reviewer", 5*time.Second)
	waitForMailboxState(t, mb, "reviewer", WorkerListening)
	mustExec(t, mb, `DELETE FROM workers WHERE agent = 'reviewer'`)
	if status, _ := mb.Status("reviewer"); status.State != WorkerOffline {
		t.Fatalf("Status() after row loss = %#v, want offline", status)
	}
	waitForMailboxState(t, mb, "reviewer", WorkerListening)

	sent := mustSend(t, mb, "reviewer", "still here?", 2*time.Second)
	if assigned := receiveListen(t, listening); assigned.err != nil || assigned.result.Message.ID != sent.ID {
		t.Fatalf("Listen() = %#v, want assigned %s after re-enrollment", assigned, sent.ID)
	}
}

func TestMailboxStampsFreshSchemaAndRejectsNewer(t *testing.T) {
	path := filepath.Join(t.TempDir(), "schema.db")
	mb, podErr := OpenMailbox(path)
	if podErr != nil {
		t.Fatalf("OpenMailbox() error = %v", podErr)
	}
	var version int
	if err := mb.db.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil || version != SchemaVersion {
		t.Fatalf("user_version = %d, %v, want %d", version, err, SchemaVersion)
	}
	if err := mb.Close(); err != nil {
		t.Fatal(err)
	}

	raw, err := sql.Open("sqlite", "file:"+filepath.ToSlash(path))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Exec(`PRAGMA user_version = 99`); err != nil {
		t.Fatal(err)
	}
	_ = raw.Close()
	_, podErr = OpenMailbox(path)
	assertErrorCode(t, podErr, "SCHEMA_MISMATCH", 1)
}

func TestMailboxInspectsAndCancelsOnlyPendingAsyncTasks(t *testing.T) {
	mb := newTestMailbox(t)
	ctx := context.Background()
	if _, podErr := mb.Pull(ctx, "reviewer", testProject); podErr != nil {
		t.Fatal(podErr)
	}
	sent := mustSend(t, mb, "reviewer", "inspect me", 3*time.Second)
	status, podErr := mb.Task(ctx, sent.ID)
	if podErr != nil || status.State != taskPending || status.Payload != "inspect me" || status.WorkTimeout != "3s" || !status.ExpiresAt.IsZero() {
		t.Fatalf("Task() = %#v, %v", status, podErr)
	}
	if canceled, podErr := mb.Cancel(ctx, sent.ID); podErr != nil || canceled.State != taskCanceled {
		t.Fatalf("Cancel() = %#v, %v", canceled, podErr)
	}
	status, podErr = mb.Task(ctx, sent.ID)
	if podErr != nil || status.State != taskCanceled || status.CompletedAt.IsZero() {
		t.Fatalf("Task() after cancel = %#v, %v", status, podErr)
	}
	_, podErr = mb.Wait(ctx, sent.ID, 20*time.Millisecond)
	assertErrorCode(t, podErr, "TASK_CANCELED", 3)
}

func TestMailboxDoctorReportsHealthyState(t *testing.T) {
	mb := newTestMailbox(t)
	result, podErr := mb.Doctor(context.Background())
	if podErr != nil || result.State != "healthy" || result.Schema != SchemaVersion || result.Tasks == nil || result.Path == "" {
		t.Fatalf("Doctor() = %#v, %v", result, podErr)
	}
}

func TestMailboxRejectsLegacyUnversionedSchemaWithoutStampingIt(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.db")
	raw, err := sql.Open("sqlite", "file:"+filepath.ToSlash(path))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Exec(`CREATE TABLE workers (agent TEXT PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	_ = raw.Close()

	_, podErr := OpenMailbox(path)
	assertErrorCode(t, podErr, "SCHEMA_MISMATCH", 1)
	raw, err = sql.Open("sqlite", "file:"+filepath.ToSlash(path))
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	var version int
	if err := raw.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil || version != 0 {
		t.Fatalf("legacy user_version = %d, %v, want unchanged 0", version, err)
	}
}
