package agentpod

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func newTestMailbox(t *testing.T) *Mailbox {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "test_mailbox.db")
	mailbox, err := OpenMailbox(dbPath)
	if err != nil {
		t.Fatalf("OpenMailbox() error = %v", err)
	}
	t.Cleanup(func() {
		_ = mailbox.Close()
	})
	return mailbox
}

func TestMailboxCompletesOneCorrelatedRoundTrip(t *testing.T) {
	mb := newTestMailbox(t)

	listenCtx, cancelListen := context.WithCancel(context.Background())
	defer cancelListen()

	listenCh := make(chan ListenResult, 1)
	errCh := make(chan *Error, 1)

	go func() {
		res, err := mb.Listen(listenCtx, ListenRequest{
			Agent:   "reviewer",
			Project: testProject,
			Wait:    2 * time.Second,
		})
		if err != nil {
			errCh <- err
			return
		}
		listenCh <- res
	}()

	// Wait for worker to be listening
	for {
		st, _ := mb.Status("reviewer")
		if st.State == WorkerListening {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	sent, podErr := mb.Send(context.Background(), SendRequest{
		From:     "driver",
		To:       "reviewer",
		Payload:  "review matcher.go",
		Deadline: 2 * time.Second,
	})
	if podErr != nil {
		t.Fatalf("Send() error = %v", podErr)
	}
	if sent.State != "dispatched" || sent.ID == "" {
		t.Fatalf("Send() result = %#v, want dispatched", sent)
	}

	select {
	case err := <-errCh:
		t.Fatalf("Listen error = %v", err)
	case assigned := <-listenCh:
		if assigned.State != "assigned" || assigned.Message == nil {
			t.Fatalf("Listen() result = %#v, want assigned", assigned)
		}
		if assigned.Message.ID != sent.ID || assigned.Message.Payload != "review matcher.go" {
			t.Fatalf("assigned message mismatch: %#v vs %#v", assigned.Message, sent)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for Listen to receive task")
	}

	// Status should now be working
	st, _ := mb.Status("reviewer")
	if st.State != WorkerWorking || st.TaskID != sent.ID {
		t.Fatalf("Status() = %#v, want working with task %s", st, sent.ID)
	}

	// Reply
	replyRes, podErr := mb.Reply(ReplyRequest{
		ID:      sent.ID,
		Payload: "looks good",
	})
	if podErr != nil {
		t.Fatalf("Reply() error = %v", podErr)
	}
	if replyRes.State != "accepted" {
		t.Fatalf("Reply() state = %s, want accepted", replyRes.State)
	}

	// Wait should now return the completed result
	waitRes, podErr := mb.Wait(context.Background(), sent.ID, 2*time.Second)
	if podErr != nil {
		t.Fatalf("Wait() error = %v", podErr)
	}
	if waitRes.State != "completed" || waitRes.Payload != "looks good" {
		t.Fatalf("Wait() = %#v, want completed with 'looks good'", waitRes)
	}

	// Worker should now be polling
	st, _ = mb.Status("reviewer")
	if st.State != WorkerPolling {
		t.Fatalf("Status() after reply = %#v, want polling", st)
	}
}

func TestMailboxSynchronousAsk(t *testing.T) {
	mb := newTestMailbox(t)

	listenCtx, cancelListen := context.WithCancel(context.Background())
	defer cancelListen()

	listenCh := make(chan ListenResult, 1)
	go func() {
		res, _ := mb.Listen(listenCtx, ListenRequest{
			Agent:   "reviewer",
			Project: testProject,
			Wait:    2 * time.Second,
		})
		listenCh <- res
	}()

	for {
		st, _ := mb.Status("reviewer")
		if st.State == WorkerListening {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	askCh := make(chan AskResult, 1)
	askErrCh := make(chan *Error, 1)
	go func() {
		res, err := mb.Ask(context.Background(), AskRequest{
			From:    "driver",
			To:      "reviewer",
			Payload: "quick question",
			Timeout: 2 * time.Second,
		})
		if err != nil {
			askErrCh <- err
			return
		}
		askCh <- res
	}()

	assigned := <-listenCh
	if assigned.State != "assigned" || assigned.Message == nil {
		t.Fatalf("Listen() = %#v, want assigned", assigned)
	}

	replyRes, podErr := mb.Reply(ReplyRequest{
		ID:      assigned.Message.ID,
		Payload: "quick answer",
	})
	if podErr != nil {
		t.Fatalf("Reply() error = %v", podErr)
	}
	if replyRes.State != "accepted" {
		t.Fatalf("Reply() state = %s", replyRes.State)
	}

	select {
	case err := <-askErrCh:
		t.Fatalf("Ask() error = %v", err)
	case askRes := <-askCh:
		if askRes.State != "completed" || askRes.Payload != "quick answer" {
			t.Fatalf("Ask() = %#v, want completed with answer", askRes)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for Ask to complete")
	}
}

func TestMailboxRejectsOfflineWorker(t *testing.T) {
	mb := newTestMailbox(t)

	_, podErr := mb.Send(context.Background(), SendRequest{
		From:     "driver",
		To:       "reviewer",
		Payload:  "task",
		Deadline: 2 * time.Second,
	})
	if podErr == nil || podErr.Code != "AGENT_OFFLINE" {
		t.Fatalf("Send() to offline worker error = %v, want AGENT_OFFLINE", podErr)
	}
}

func TestMailboxStaleTaskRecovery(t *testing.T) {
	mb := newTestMailbox(t)

	listenCtx, cancelListen := context.WithCancel(context.Background())
	defer cancelListen()

	listenCh := make(chan ListenResult, 1)
	go func() {
		res, _ := mb.Listen(listenCtx, ListenRequest{
			Agent:   "reviewer",
			Project: testProject,
			Wait:    2 * time.Second,
		})
		listenCh <- res
	}()

	for {
		st, _ := mb.Status("reviewer")
		if st.State == WorkerListening {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	sent, podErr := mb.Send(context.Background(), SendRequest{
		From:     "driver",
		To:       "reviewer",
		Payload:  "do work",
		Deadline: 50 * time.Millisecond,
	})
	if podErr != nil {
		t.Fatalf("Send() error = %v", podErr)
	}

	<-listenCh

	// Wait for the claimed task's work timeout to pass.
	time.Sleep(100 * time.Millisecond)

	st, _ := mb.Status("reviewer")
	if st.State != WorkerStale {
		t.Fatalf("Status() = %#v, want stale", st)
	}

	// Next listen should report recovered
	recRes, podErr := mb.Listen(context.Background(), ListenRequest{
		Agent:   "reviewer",
		Project: testProject,
		Wait:    50 * time.Millisecond,
	})
	if podErr != nil {
		t.Fatalf("Listen() on stale error = %v", podErr)
	}
	if recRes.State != "recovered" || recRes.RecoveredTaskID != sent.ID {
		t.Fatalf("Listen() = %#v, want recovered with %s", recRes, sent.ID)
	}
	if recRes.Message == nil || recRes.Message.Payload != "do work" {
		t.Fatalf("Listen() recovered message = %#v, want preserved payload", recRes.Message)
	}
}

func TestMailboxListActiveWorkers(t *testing.T) {
	mb := newTestMailbox(t)

	listenCtx, cancelListen := context.WithCancel(context.Background())
	defer cancelListen()

	go func() {
		_, _ = mb.Listen(listenCtx, ListenRequest{
			Agent:   "reviewer-1",
			Project: testProject,
			Wait:    2 * time.Second,
		})
	}()

	for {
		st, _ := mb.Status("reviewer-1")
		if st.State == WorkerListening {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	workers, err := mb.List()
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if len(workers) != 1 || workers[0].Agent != "reviewer-1" || workers[0].State != WorkerListening {
		t.Fatalf("List() = %#v, want 1 listening worker", workers)
	}
}

func TestMailboxPullNonBlocking(t *testing.T) {
	mb := newTestMailbox(t)

	// Pull on empty mailbox should return idle immediately
	res, podErr := mb.Pull(context.Background(), "reviewer", testProject)
	if podErr != nil {
		t.Fatalf("Pull() error = %v", podErr)
	}
	if res.State != "idle" {
		t.Fatalf("Pull() = %#v, want idle", res)
	}

	// Send a task to the now-enrolled worker
	sent, podErr := mb.Send(context.Background(), SendRequest{
		From:     "driver",
		To:       "reviewer",
		Payload:  "pull me",
		Deadline: 2 * time.Second,
	})
	if podErr != nil {
		t.Fatalf("Send() error = %v", podErr)
	}

	// Pull should now claim the pending task immediately
	res, podErr = mb.Pull(context.Background(), "reviewer", testProject)
	if podErr != nil {
		t.Fatalf("Pull() error = %v", podErr)
	}
	if res.State != "assigned" || res.Message == nil || res.Message.ID != sent.ID || res.Message.Payload != "pull me" {
		t.Fatalf("Pull() claimed task mismatch: %#v", res)
	}
}

func TestMailboxQueuesTasksForWorkingWorker(t *testing.T) {
	mb := newTestMailbox(t)

	listenCtx, cancelListen := context.WithCancel(context.Background())
	defer cancelListen()

	listenCh := make(chan ListenResult, 1)
	go func() {
		res, _ := mb.Listen(listenCtx, ListenRequest{
			Agent:   "reviewer",
			Project: testProject,
			Wait:    2 * time.Second,
		})
		listenCh <- res
	}()

	for {
		st, _ := mb.Status("reviewer")
		if st.State == WorkerListening {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Send task 1
	sent1, err := mb.Send(context.Background(), SendRequest{
		From:     "driver",
		To:       "reviewer",
		Payload:  "task 1",
		Deadline: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("Send(1) error = %v", err)
	}

	<-listenCh
	// Worker is now working on task 1
	st, _ := mb.Status("reviewer")
	if st.State != WorkerWorking {
		t.Fatalf("Status = %s, want working", st.State)
	}

	// Send task 2 while worker is still working on task 1
	sent2, err := mb.Send(context.Background(), SendRequest{
		From:     "driver",
		To:       "reviewer",
		Payload:  "task 2",
		Deadline: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("Send(2) error = %v, want successfully queued into mailbox", err)
	}

	// Status should show queue_depth = 1
	st, _ = mb.Status("reviewer")
	if st.QueueDepth != 1 || len(st.Queue) != 1 || st.Queue[0].ID != sent2.ID {
		t.Fatalf("Status() queue = %#v, want 1 queued task with ID %s", st, sent2.ID)
	}

	// Reply to task 1
	_, err = mb.Reply(ReplyRequest{ID: sent1.ID, Payload: "task 1 done"})
	if err != nil {
		t.Fatalf("Reply(1) error = %v", err)
	}

	// Next Pull or Listen should immediately receive task 2
	pullRes, err := mb.Pull(context.Background(), "reviewer", testProject)
	if err != nil {
		t.Fatalf("Pull() error = %v", err)
	}
	if pullRes.State != "assigned" || pullRes.Message == nil || pullRes.Message.ID != sent2.ID {
		t.Fatalf("Pull() after reply = %#v, want task 2 (%s)", pullRes, sent2.ID)
	}
}
