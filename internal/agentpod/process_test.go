package agentpod

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestSessionPIDSupportsTaggedAndLegacySessions(t *testing.T) {
	myHost := currentHost()
	if got := sessionPID(fmt.Sprintf("%s:123:token", myHost)); got != 123 {
		t.Fatalf("sessionPID(host:123:token) = %d, want 123", got)
	}
	if got := sessionPID("otherhost:123:token"); got != 0 {
		t.Fatalf("sessionPID(otherhost:123:token) = %d, want 0 (cross-host unknown)", got)
	}
	if got := sessionPID("123:token"); got != 123 {
		t.Fatalf("sessionPID(123:token) = %d, want 123", got)
	}
	for _, legacy := range []string{"token", "bad:token", "0:token", fmt.Sprintf("%s:bad:token", myHost)} {
		if got := sessionPID(legacy); got != 0 {
			t.Fatalf("sessionPID(%q) = %d, want unknown", legacy, got)
		}
	}
}

func TestDeadListenerIsImmediatelyOfflineAndDiagnosed(t *testing.T) {
	previous := processIsAlive
	processIsAlive = func(pid int) bool { return false }
	t.Cleanup(func() { processIsAlive = previous })

	mb := newTestMailbox(t)
	now := time.Now().UTC()
	mb.now = func() time.Time { return now }
	mustExec(t, mb, `INSERT INTO workers (agent, project, state, session, current_task_id, last_seen_at) VALUES (?, ?, 'listening', ?, NULL, ?)`, "reviewer", testProject, "4242:token", millis(now))

	status, podErr := mb.Status("reviewer")
	if podErr != nil || status.State != WorkerOffline || status.Project != testProject || status.Detail == "" {
		t.Fatalf("Status() = %#v, %v, want immediate offline", status, podErr)
	}
	_, podErr = mb.Send(context.Background(), SendRequest{From: "driver", To: "reviewer", Payload: "task", Project: testProject, Deadline: time.Minute})
	if podErr == nil || podErr.Code != "AGENT_OFFLINE" || podErr.Message != `agent "reviewer" listener process (PID 4242) terminated; run `+"`skpod agent reviewer`"+` in that session to re-listen` {
		t.Fatalf("Send() error = %#v", podErr)
	}
	doctor, podErr := mb.Doctor(context.Background())
	if podErr != nil || doctor.State != "degraded" || len(doctor.DeadListeners) != 1 || doctor.DeadListeners[0].PID != 4242 {
		t.Fatalf("Doctor() = %#v, %v", doctor, podErr)
	}
}

func TestDispatchReportsCrossProjectEnrollment(t *testing.T) {
	mb := newTestMailbox(t)
	now := time.Now().UTC()
	mb.now = func() time.Time { return now }
	mustExec(t, mb, `INSERT INTO workers (agent, project, state, session, current_task_id, last_seen_at) VALUES (?, ?, 'listening', ?, NULL, ?)`, "reviewer", "C:/other", fmt.Sprintf("%d:token", 1), millis(now))
	previous := processIsAlive
	processIsAlive = func(pid int) bool { return true }
	t.Cleanup(func() { processIsAlive = previous })

	_, podErr := mb.Send(context.Background(), SendRequest{From: "driver", To: "reviewer", Payload: "task", Project: testProject, Deadline: time.Minute})
	if podErr == nil || podErr.Code != "PROJECT_MISMATCH" {
		t.Fatalf("Send() error = %#v", podErr)
	}
}

func TestWorkingAndPollingWorkersNeverReportedDead(t *testing.T) {
	previous := processIsAlive
	processIsAlive = func(pid int) bool { return false } // all PIDs dead
	t.Cleanup(func() { processIsAlive = previous })

	mb := newTestMailbox(t)
	now := time.Now().UTC()
	mb.now = func() time.Time { return now }

	// 1. Working worker with a claimed task whose listener process already exited:
	taskID := "0123456789abcdef0123456789abcdef"
	mustExec(t, mb, `INSERT INTO tasks (id, from_agent, to_agent, payload, is_async, status, deadline_ms, deadline, created_at, updated_at) VALUES (?, 'driver', 'worker', 'payload', 0, 'working', 60000, ?, ?, ?)`, taskID, millis(now.Add(time.Minute)), millis(now), millis(now))
	mustExec(t, mb, `INSERT INTO workers (agent, project, state, session, current_task_id, last_seen_at) VALUES ('worker', ?, 'working', '9999:dead-token', ?, ?)`, testProject, taskID, millis(now))

	status, podErr := mb.Status("worker")
	if podErr != nil || status.State != WorkerWorking || status.TaskID != taskID {
		t.Fatalf("working worker status = %#v, want working with task %s (durable claim must outlive listener process)", status, taskID)
	}

	// 2. Polling worker between listens:
	mustExec(t, mb, `INSERT INTO workers (agent, project, state, session, current_task_id, last_seen_at) VALUES ('poller', ?, 'between_listens', '9999:dead-token', NULL, ?)`, testProject, millis(now))
	status, podErr = mb.Status("poller")
	if podErr != nil || status.State != WorkerPolling {
		t.Fatalf("polling worker status = %#v, want between_listens", status)
	}
}

func TestDeadListenerAllowsImmediateReenrollment(t *testing.T) {
	previous := processIsAlive
	processIsAlive = func(pid int) bool { return false }
	t.Cleanup(func() { processIsAlive = previous })

	mb := newTestMailbox(t)
	now := time.Now().UTC()
	mb.now = func() time.Time { return now }
	mustExec(t, mb, `INSERT INTO workers (agent, project, state, session, current_task_id, last_seen_at) VALUES (?, ?, 'listening', ?, NULL, ?)`, "reviewer", testProject, "9999:dead-token", millis(now))

	// A new listener should be able to enroll immediately even though last_seen_at was 0s ago
	recovered, podErr := mb.enroll(context.Background(), "reviewer", testProject, "1111:new-token")
	if podErr != nil || recovered != nil {
		t.Fatalf("enroll() error = %v, recovered = %#v, want successful immediate re-enrollment", podErr, recovered)
	}
}



