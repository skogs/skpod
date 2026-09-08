package agentpod

import (
	"context"
	"database/sql"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestProjectOwnershipSurvivesWorkerLifecycle(t *testing.T) {
	for _, lifecycle := range []string{"polling", "offline", "pruned", "recovered", "abandoned-claim"} {
		t.Run(lifecycle, func(t *testing.T) {
			mb := newTestMailbox(t)
			ctx := context.Background()
			now := time.Now().UTC()
			mb.now = func() time.Time { return now }
			if _, err := mb.Pull(ctx, "reviewer", testProject); err != nil {
				t.Fatal(err)
			}
			sent := mustSend(t, mb, "reviewer", "private project A task", time.Minute)
			switch lifecycle {
			case "offline":
				mustExec(t, mb, `UPDATE workers SET state = 'listening', session = 'old' WHERE agent = 'reviewer'`)
				mb.unregisterListener("reviewer", "old")
			case "pruned":
				now = now.Add(abandonedWorkerRetention + time.Minute)
				mb.pruneIfDue(ctx)
			case "recovered", "abandoned-claim":
				if _, err := mb.Pull(ctx, "reviewer", testProject); err != nil {
					t.Fatal(err)
				}
				sent = mustSend(t, mb, "reviewer", "next project A task", time.Minute)
				if lifecycle == "abandoned-claim" {
					now = now.Add(AsyncResultRetention + 2*time.Minute)
					mb.pruneIfDue(ctx)
				} else {
					now = now.Add(2 * time.Minute)
					if result, err := mb.Pull(ctx, "reviewer", testProject); err != nil || result.State != "recovered" {
						t.Fatalf("recover = %#v, %v", result, err)
					}
				}
			}
			for _, mode := range []string{"pull", "listen"} {
				var err *Error
				if mode == "pull" {
					_, err = mb.Pull(ctx, "reviewer", "other-project")
				} else {
					_, err = mb.Listen(ctx, ListenRequest{Agent: "reviewer", Project: "other-project", Wait: 10 * time.Millisecond})
				}
				if err == nil || err.Code != "PROJECT_MISMATCH" {
					t.Fatalf("%s cross-project error = %v", mode, err)
				}
			}
			task, err := mb.Task(ctx, sent.ID)
			if err != nil || task.State != taskPending {
				t.Fatalf("queued task changed: %#v, %v", task, err)
			}
			assigned, err := mb.Pull(ctx, "reviewer", testProject)
			if err != nil || assigned.Message == nil || assigned.Message.ID != sent.ID {
				t.Fatalf("original project cannot claim: %#v, %v", assigned, err)
			}
		})
	}
}

func TestOfflineDrainedNameCanMoveProjects(t *testing.T) {
	mb := newTestMailbox(t)
	ctx := context.Background()
	if _, err := mb.Pull(ctx, "reviewer", testProject); err != nil {
		t.Fatal(err)
	}
	mustExec(t, mb, `UPDATE workers SET last_seen_at = 0`)
	if _, err := mb.Pull(ctx, "reviewer", "other-project"); err != nil {
		t.Fatal(err)
	}
}

func TestExpiryVisibleWithoutOtherMailboxTraffic(t *testing.T) {
	for _, command := range []string{"wait", "task"} {
		t.Run(command, func(t *testing.T) {
			mb := newTestMailbox(t)
			ctx := context.Background()
			now := time.Now().UTC()
			mb.now = func() time.Time { return now }
			if _, err := mb.Pull(ctx, "reviewer", testProject); err != nil {
				t.Fatal(err)
			}
			sent := mustSend(t, mb, "reviewer", "expires in queue", time.Minute)
			now = now.Add(AsyncQueueRetention + time.Minute)
			if command == "wait" {
				if _, err := mb.Wait(ctx, sent.ID, time.Second); err == nil || err.Code != "TASK_EXPIRED" {
					t.Fatalf("wait = %v", err)
				}
			} else if task, err := mb.Task(ctx, sent.ID); err != nil || task.State != taskExpired {
				t.Fatalf("task = %#v, %v", task, err)
			}
		})
	}
}

func TestRunningWaitObservesQueueExpiry(t *testing.T) {
	mb := newTestMailbox(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if _, err := mb.Pull(ctx, "reviewer", testProject); err != nil {
		t.Fatal(err)
	}
	sent := mustSend(t, mb, "reviewer", "expire while waiting", time.Minute)
	var clock atomic.Int64
	clock.Store(time.Now().UnixMilli())
	started := make(chan struct{})
	var once sync.Once
	mb.now = func() time.Time {
		now := fromMillis(clock.Load())
		once.Do(func() { close(started) })
		return now
	}
	result := make(chan *Error, 1)
	go func() {
		_, err := mb.Wait(ctx, sent.ID, time.Second)
		result <- err
	}()
	<-started
	clock.Add((AsyncQueueRetention + time.Minute).Milliseconds())
	if err := <-result; err == nil || err.Code != "TASK_EXPIRED" {
		t.Fatalf("running wait = %v", err)
	}
}

func TestConcurrentFreshMailboxInitialization(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fresh.db")
	start := make(chan struct{})
	var wg sync.WaitGroup
	for range 12 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			mb, err := OpenMailbox(path)
			if err != nil {
				t.Errorf("concurrent open: %v", err)
				return
			}
			_ = mb.Close()
		}()
	}
	close(start)
	wg.Wait()
}

func TestResetLifecycleLockAndBackup(t *testing.T) {
	mb := newTestMailbox(t)
	ctx := context.Background()
	path := mb.path
	if _, err := ResetMailbox(ctx, path); err == nil || err.Code != "MAILBOX_ACTIVE" {
		t.Fatalf("reset with open handle = %v", err)
	}
	if _, err := mb.Pull(ctx, "reviewer", testProject); err != nil {
		t.Fatal(err)
	}
	sent := mustSend(t, mb, "reviewer", "preserve in backup", time.Minute)
	_ = mb.Close()
	if _, err := ResetMailbox(ctx, path); err == nil || err.Code != "MAILBOX_ACTIVE" {
		t.Fatalf("reset with registered worker = %v", err)
	}
	mb, err := OpenMailbox(path)
	if err != nil {
		t.Fatal(err)
	}
	mustExec(t, mb, `UPDATE workers SET last_seen_at = 0, state = 'offline'`)
	_ = mb.Close()
	_, lock, err := lockMailbox(path, true)
	if err != nil {
		t.Fatal(err)
	}
	if other, err := OpenMailbox(path); err == nil {
		_ = other.Close()
		t.Error("mailbox opened during exclusive reset lock")
	} else if err.Code != "MAILBOX_ACTIVE" {
		t.Fatal(err)
	}
	if _, err := ResetMailbox(ctx, path); err == nil || err.Code != "MAILBOX_ACTIVE" {
		t.Errorf("concurrent reset = %v", err)
	}
	_ = lock.Close()
	backup, err := ResetMailbox(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		path     string
		wantTask bool
	}{{backup, true}, {path, false}} {
		db, err := OpenMailbox(tc.path)
		if err != nil {
			t.Fatal(err)
		}
		_, taskErr := db.Task(ctx, sent.ID)
		_ = db.Close()
		if tc.wantTask && taskErr != nil || !tc.wantTask && (taskErr == nil || taskErr.Code != "TASK_NOT_FOUND") {
			t.Fatalf("task in %s: %v", tc.path, taskErr)
		}
	}
}

func TestResetRefusesUnknownSchemaWithoutClearingIt(t *testing.T) {
	path := filepath.Join(t.TempDir(), "unknown.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE private_data (value TEXT); INSERT INTO private_data VALUES ('keep'); PRAGMA user_version = 999;`); err != nil {
		t.Fatal(err)
	}
	if _, err := ResetMailbox(context.Background(), path); err == nil || err.Code != "SCHEMA_MISMATCH" {
		t.Fatalf("reset unknown schema = %v", err)
	}
	var value string
	if err := db.QueryRow(`SELECT value FROM private_data`).Scan(&value); err != nil || value != "keep" {
		t.Fatalf("old data changed: %q, %v", value, err)
	}
	if backups, _ := filepath.Glob(path + ".*.bak"); len(backups) != 0 {
		t.Fatalf("unexpected backups: %v", backups)
	}
}
