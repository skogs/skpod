package agentpod

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	MaxAgentNameBytes = 64
	MaxProjectBytes   = 4 * 1024
	MaxPayloadBytes   = 64 * 1024

	MinProtocolWait = 10 * time.Millisecond
	MaxListenWait   = 30 * time.Minute
	MaxTaskDeadline = 30 * time.Minute
	MaxWait         = 30 * time.Minute

	AsyncResultRetention = time.Hour
	AsyncQueueRetention  = 24 * time.Hour
	MaxAsyncResults      = 1024
)

const (
	WorkerOffline   = "offline"
	WorkerListening = "listening"
	WorkerWorking   = "working"
	WorkerStale     = "stale"
	WorkerPolling   = "between_listens"
)

var agentNamePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]*$`)

type Error struct {
	Code       string `json:"code"`
	Message    string `json:"message"`
	ExitCode   int    `json:"-"`
	HTTPStatus int    `json:"-"`
}

func (e *Error) Error() string { return e.Message }

type Message struct {
	ID        string    `json:"id"`
	From      string    `json:"from"`
	To        string    `json:"to"`
	Payload   string    `json:"payload"`
	Timestamp time.Time `json:"timestamp"`
	Deadline  time.Time `json:"expires_at"`
}

type AskRequest struct {
	From, To, Payload string
	Timeout           time.Duration
}

type SendRequest struct {
	From, To, Payload string
	Deadline          time.Duration
}

type ListenRequest struct {
	Agent, Project string
	Wait           time.Duration
}

type AskResult struct {
	ID      string `json:"id"`
	From    string `json:"from"`
	To      string `json:"to"`
	Payload string `json:"payload"`
	State   string `json:"state"`
}

type SendResult struct {
	ID          string `json:"id"`
	From        string `json:"from"`
	To          string `json:"to"`
	State       string `json:"state"`
	WorkTimeout string `json:"work_timeout"`
	expiresAt   time.Time
}

type WaitResult struct {
	ID         string `json:"id"`
	From       string `json:"from"`
	To         string `json:"to"`
	Payload    string `json:"payload"`
	State      string `json:"state"`
	ReplyState string `json:"reply_state"`
}

type ListenResult struct {
	Agent           string   `json:"agent"`
	State           string   `json:"state"`
	Message         *Message `json:"message,omitempty"`
	RecoveredTaskID string   `json:"recovered_task_id,omitempty"`
}

type ReplyRequest struct{ ID, Payload string }

type ReplyResult struct {
	ID    string `json:"id"`
	State string `json:"state"`
}

type TaskStatus struct {
	ID          string    `json:"id"`
	From        string    `json:"from"`
	To          string    `json:"to"`
	State       string    `json:"state"`
	Payload     string    `json:"payload"`
	Reply       string    `json:"reply,omitempty"`
	ReplyState  string    `json:"reply_state,omitempty"`
	WorkTimeout string    `json:"work_timeout"`
	CreatedAt   time.Time `json:"created_at"`
	ClaimedAt   time.Time `json:"claimed_at,omitzero"`
	ExpiresAt   time.Time `json:"expires_at,omitzero"`
	CompletedAt time.Time `json:"completed_at,omitzero"`
}

type CancelResult struct {
	ID    string `json:"id"`
	State string `json:"state"`
}

type DoctorResult struct {
	State       string         `json:"state"`
	Path        string         `json:"path"`
	Schema      int            `json:"schema"`
	Workers     int            `json:"workers"`
	Tasks       map[string]int `json:"tasks"`
	LastPruneAt time.Time      `json:"last_prune_at,omitzero"`
}

type QueuedTask struct {
	ID          string    `json:"id"`
	From        string    `json:"from"`
	To          string    `json:"to"`
	Timestamp   time.Time `json:"timestamp"`
	WorkTimeout string    `json:"work_timeout"`
}

type WorkerStatus struct {
	Agent      string       `json:"agent"`
	Project    string       `json:"project,omitempty"`
	State      string       `json:"state"`
	TaskID     string       `json:"task_id,omitempty"`
	Deadline   time.Time    `json:"expires_at,omitzero"`
	ClaimedAt  time.Time    `json:"claimed_at,omitzero"`
	QueueDepth int          `json:"queue_depth,omitzero"`
	Queue      []QueuedTask `json:"queue,omitempty"`
}

func workerUnavailable(agent, state string) *Error {
	switch state {
	case WorkerWorking:
		return newError("AGENT_BUSY", fmt.Sprintf("agent %q is working; inspect it with `skpod status %s`", agent, agent), 4, 409)
	case WorkerStale:
		return newError("AGENT_STALE", fmt.Sprintf("agent %q exceeded its work timeout; its next listen recovers the claim", agent), 4, 409)
	case WorkerPolling:
		return newError("AGENT_OFFLINE", fmt.Sprintf("agent %q is between listens and cannot take an ask; use send, or have it listen again", agent), 4, 409)
	default:
		return newError("AGENT_OFFLINE", fmt.Sprintf("agent %q is not listening; run `skpod agent %s` in that session", agent, agent), 4, 409)
	}
}

func taskNotFound(id string) *Error {
	return newError("TASK_NOT_FOUND", fmt.Sprintf("task %q is unknown or its retained outcome expired", id), 4, 404)
}

func taskTimeoutError(message Message) *Error {
	return newError("TASK_TIMEOUT", fmt.Sprintf("task %s exceeded its work timeout; agent %q is stale until it replies or listens again", message.ID, message.To), 3, 504)
}

func contextError(operation string, err error, deliveryUnknown bool) *Error {
	code, message := "CANCELED", operation+" was canceled"
	if errors.Is(err, context.DeadlineExceeded) {
		code, message = "TIMEOUT", operation+" timed out"
	}
	if deliveryUnknown {
		message += "; the worker already holds the task"
	}
	return newError(code, message, 3, 408)
}

func validateAgentName(field, value string) *Error {
	if value == "" || len(value) > MaxAgentNameBytes || !utf8.ValidString(value) || !agentNamePattern.MatchString(value) {
		return newError("INVALID_INPUT", fmt.Sprintf("%s must match [a-z0-9][a-z0-9._-]* and be at most %d bytes", field, MaxAgentNameBytes), 2, 400)
	}
	return nil
}

func ValidateAgentName(value string) *Error { return validateAgentName("agent", value) }

func validateTaskDispatch(from, to, payload, timeoutField string, timeout time.Duration) *Error {
	if err := validateAgentName("from", from); err != nil {
		return err
	}
	if err := validateAgentName("to", to); err != nil {
		return err
	}
	if from == to {
		return newError("INVALID_INPUT", "from and to must name different agents", 2, 400)
	}
	if err := validatePayload("task payload", payload); err != nil {
		return err
	}
	return validateWait(timeoutField, timeout, MaxTaskDeadline)
}

func validateProject(value string) *Error {
	if strings.TrimSpace(value) == "" || !utf8.ValidString(value) || strings.ContainsRune(value, '\x00') || len(value) > MaxProjectBytes {
		return newError("INVALID_INPUT", fmt.Sprintf("project must be valid, non-empty UTF-8 and at most %d bytes", MaxProjectBytes), 2, 400)
	}
	return nil
}

func validateTaskID(value string) *Error {
	if len(value) != 32 {
		return newError("INVALID_INPUT", "task ID must be 32 lowercase hexadecimal characters", 2, 400)
	}
	for _, character := range value {
		if !strings.ContainsRune("0123456789abcdef", character) {
			return newError("INVALID_INPUT", "task ID must be 32 lowercase hexadecimal characters", 2, 400)
		}
	}
	return nil
}

func validatePayload(field, value string) *Error {
	if strings.TrimSpace(value) == "" || !utf8.ValidString(value) {
		return newError("INVALID_INPUT", field+" must be non-empty valid UTF-8", 2, 400)
	}
	if len(value) > MaxPayloadBytes {
		return newError("INVALID_INPUT", fmt.Sprintf("%s exceeds the %d-byte limit", field, MaxPayloadBytes), 2, 400)
	}
	return nil
}

func validateListenWait(value time.Duration) *Error {
	if value == 0 {
		return nil
	}
	if err := validateWait("listen wait", value, MaxListenWait); err != nil {
		err.Message += ", or 0 to wait until assignment or cancellation"
		return err
	}
	return nil
}

func validateWait(field string, value, maximum time.Duration) *Error {
	if value < MinProtocolWait || value > maximum {
		return newError("INVALID_INPUT", fmt.Sprintf("%s must be from %s through %s", field, MinProtocolWait, maximum), 2, 400)
	}
	return nil
}

func randomID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(value[:]), nil
}

func newError(code, message string, exitCode, httpStatus int) *Error {
	return &Error{Code: code, Message: message, ExitCode: exitCode, HTTPStatus: httpStatus}
}
