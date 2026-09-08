package agentpod

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/gofrs/flock"
	"modernc.org/sqlite"
)

const (
	// SchemaVersion is recorded in PRAGMA user_version. Bump it for any schema
	// change: there is no in-place upgrade. Only an empty version-0 database is
	// initialized; every populated unversioned or differently versioned mailbox
	// is refused with SCHEMA_MISMATCH.
	SchemaVersion = 3

	// MaxQueueDepth bounds the pending tasks a single worker can hold.
	MaxQueueDepth = 16

	// Liveness windows. They are related: ListenerLiveness must exceed
	// heartbeatInterval plus busyTimeout so one contended write cannot make a
	// live listener look dead, and abandonedWorkerRetention must be far longer
	// than both presence windows so a suspended process is reported offline
	// long before its row is garbage collected.
	heartbeatInterval        = time.Second
	busyTimeout              = 2 * time.Second
	ListenerLiveness         = 5 * time.Second
	PollingPresence          = 5 * time.Minute
	pollInterval             = 50 * time.Millisecond
	pruneInterval            = 30 * time.Second
	abandonedWorkerRetention = 10 * time.Minute
)

const (
	taskPending   = "pending"
	taskWorking   = "working"
	taskCompleted = "completed"
	taskRecovered = "recovered"
	taskExpired   = "expired"
	taskCanceled  = "canceled"
)

// Mailbox is the serverless, SQLite-backed coordinator. Every state transition
// runs inside one BEGIN IMMEDIATE transaction, so concurrent skpod processes
// cannot interleave a check with the write that depends on it. Reads use a
// snapshot transaction or a single statement.
type Mailbox struct {
	db   *sql.DB
	path string
	now  func() time.Time
	lock *flock.Flock // shared for the lifetime of this handle; reset holds exclusive
}

// OpenDefaultMailbox opens the mailbox at the standard local path or SKPOD_DB.
func OpenDefaultMailbox() (*Mailbox, *Error) {
	path, err := DefaultDBPath()
	if err != nil {
		return nil, newError("DATABASE_ERROR", err.Error(), 1, 500)
	}
	return OpenMailbox(path)
}

// DefaultDBPath returns the configured or standard path for mailbox.db. On
// Windows that is %LOCALAPPDATA%\skpod\mailbox.db: the mailbox is
// machine-local state, and SQLite's WAL and shared-memory files must never
// live in a roaming or synced profile. Elsewhere it stays under the user
// config directory.
func DefaultDBPath() (string, error) {
	if value := strings.TrimSpace(os.Getenv("SKPOD_DB")); value != "" {
		return value, nil
	}
	base, err := localStateDir()
	if err != nil {
		return "", fmt.Errorf("resolve local state directory: %w", err)
	}
	return filepath.Join(base, "skpod", "mailbox.db"), nil
}

func localStateDir() (string, error) {
	if runtime.GOOS == "windows" {
		if value := strings.TrimSpace(os.Getenv("LOCALAPPDATA")); value != "" {
			return value, nil
		}
		return os.UserCacheDir()
	}
	return os.UserConfigDir()
}

// OpenMailbox opens or creates a SQLite database at the specified path and
// initializes only a fresh database. It never upgrades an existing schema.
func OpenMailbox(dbPath string) (*Mailbox, *Error) {
	path, lock, podErr := lockMailbox(dbPath, false)
	if podErr != nil {
		return nil, podErr
	}
	m, podErr := openMailbox(path)
	if podErr != nil {
		_ = lock.Close()
		return nil, podErr
	}
	m.lock = lock
	return m, nil
}

// openMailbox requires the caller to hold the mailbox lifecycle lock.
func openMailbox(dbPath string) (*Mailbox, *Error) {
	if err := os.MkdirAll(filepath.Dir(dbPath), 0o700); err != nil {
		return nil, newError("DATABASE_ERROR", fmt.Sprintf("create mailbox directory: %v", err), 1, 500)
	}
	db, err := sql.Open("sqlite", mailboxDSN(dbPath))
	if err != nil {
		return nil, newError("DATABASE_ERROR", fmt.Sprintf("open sqlite database: %v", err), 1, 500)
	}
	// One connection per process keeps every statement of an operation on the
	// same SQLite connection and lets the busy handler do all cross-process
	// serialization.
	db.SetMaxOpenConns(1)
	// SQLite may return BUSY while enabling WAL on a brand-new database,
	// before its normal statement busy handler can serialize callers.
	until := time.Now().Add(busyTimeout)
	for {
		err := db.Ping()
		if err == nil {
			break
		}
		var sqliteErr *sqlite.Error
		if !errors.As(err, &sqliteErr) || (sqliteErr.Code()&0xff != 5 && sqliteErr.Code()&0xff != 6) || !time.Now().Before(until) {
			_ = db.Close()
			return nil, databaseError("connect to mailbox", err)
		}
		time.Sleep(10 * time.Millisecond)
	}

	m := &Mailbox{db: db, path: dbPath, now: time.Now}
	if podErr := m.initSchema(context.Background()); podErr != nil {
		_ = db.Close()
		return nil, podErr
	}
	if runtime.GOOS != "windows" {
		// The file mode is the same-machine trust boundary for payloads.
		_ = os.Chmod(dbPath, 0o600)
	}
	return m, nil
}

// dsnPathEscaper protects the characters SQLite's URI parser would otherwise
// interpret inside a file path.
var dsnPathEscaper = strings.NewReplacer("%", "%25", "?", "%3F", "#", "%23")

func mailboxDSN(dbPath string) string {
	return fmt.Sprintf(
		"file:%s?_txlock=immediate&_pragma=busy_timeout(%d)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)",
		dsnPathEscaper.Replace(filepath.ToSlash(dbPath)),
		busyTimeout.Milliseconds(),
	)
}

// Close closes the underlying database.
func (m *Mailbox) Close() error {
	err := m.db.Close()
	if m.lock != nil {
		return errors.Join(err, m.lock.Close())
	}
	return err
}

// --- schema -----------------------------------------------------------------

const schema = `
CREATE TABLE IF NOT EXISTS tasks (
	id           TEXT PRIMARY KEY,
	from_agent   TEXT NOT NULL,
	to_agent     TEXT NOT NULL,
	payload      TEXT NOT NULL,
	reply        TEXT,
	is_async     INTEGER NOT NULL DEFAULT 0,
	status       TEXT NOT NULL,          -- pending, working, completed, recovered, expired, canceled
	reply_state  TEXT,                   -- accepted, late
	deadline_ms  INTEGER NOT NULL,       -- allowed working time in milliseconds
	deadline     INTEGER NOT NULL,       -- absolute deadline; async tasks reset it at claim
	created_at   INTEGER NOT NULL,
	claimed_at   INTEGER,
	updated_at   INTEGER NOT NULL,
	completed_at INTEGER
);
CREATE INDEX IF NOT EXISTS idx_tasks_to_status ON tasks(to_agent, status, created_at);
CREATE INDEX IF NOT EXISTS idx_tasks_completed ON tasks(status, completed_at);

CREATE TABLE IF NOT EXISTS workers (
	agent           TEXT PRIMARY KEY,
	project         TEXT NOT NULL,
	state           TEXT NOT NULL,       -- listening, working, between_listens
	session         TEXT NOT NULL,       -- random token owned by the enrolling process
	current_task_id TEXT,
	last_seen_at    INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS meta (
	key   TEXT PRIMARY KEY,
	value INTEGER NOT NULL
);
`

// initSchema creates the schema on a fresh mailbox and refuses every populated
// unversioned or differently versioned mailbox. There is no in-place upgrade.
func (m *Mailbox) initSchema(ctx context.Context) *Error {
	var version int
	if err := m.db.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&version); err != nil {
		return databaseError("read mailbox schema version", err)
	}
	if version == SchemaVersion {
		return nil
	}
	if version != 0 {
		return schemaMismatch(version)
	}
	return m.write(ctx, "create mailbox schema", func(tx *sql.Tx) *Error {
		// Re-read under the write lock: another process may have created it first.
		if err := tx.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&version); err != nil {
			return databaseError("read mailbox schema version", err)
		}
		if version == SchemaVersion {
			return nil
		}
		if version != 0 {
			return schemaMismatch(version)
		}
		var existingTables int
		if err := tx.QueryRowContext(ctx,
			`SELECT count(*) FROM sqlite_schema WHERE type = 'table' AND name NOT LIKE 'sqlite_%'`,
		).Scan(&existingTables); err != nil {
			return databaseError("inspect unversioned mailbox", err)
		}
		if existingTables != 0 {
			return schemaMismatch(version)
		}
		if _, err := tx.ExecContext(ctx, schema); err != nil {
			return databaseError("create mailbox schema", err)
		}
		if _, err := tx.ExecContext(ctx, fmt.Sprintf(`PRAGMA user_version = %d`, SchemaVersion)); err != nil {
			return databaseError("record mailbox schema version", err)
		}
		return nil
	})
}

// --- transactions -----------------------------------------------------------

type querier interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// write runs fn inside a BEGIN IMMEDIATE transaction and commits when fn
// returns nil. A cancelled context surfaces as CANCELED or TIMEOUT rather than
// as a storage failure.
func (m *Mailbox) write(ctx context.Context, operation string, fn func(tx *sql.Tx) *Error) *Error {
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return m.storageError(ctx, operation, err)
	}
	if podErr := fn(tx); podErr != nil {
		_ = tx.Rollback()
		if ctx.Err() != nil && podErr.Code == "DATABASE_ERROR" {
			return contextError(operation, ctx.Err(), false)
		}
		return podErr
	}
	if err := tx.Commit(); err != nil {
		return m.storageError(ctx, operation, err)
	}
	return nil
}

// read runs fn inside a deferred (snapshot) transaction.
func (m *Mailbox) read(ctx context.Context, operation string, fn func(tx *sql.Tx) *Error) *Error {
	tx, err := m.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return m.storageError(ctx, operation, err)
	}
	defer func() { _ = tx.Rollback() }()
	if podErr := fn(tx); podErr != nil {
		if ctx.Err() != nil && podErr.Code == "DATABASE_ERROR" {
			return contextError(operation, ctx.Err(), false)
		}
		return podErr
	}
	return nil
}

func (m *Mailbox) storageError(ctx context.Context, operation string, err error) *Error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return contextError(operation, ctxErr, false)
	}
	return databaseError(operation, err)
}

// --- rows -------------------------------------------------------------------

type taskRow struct {
	id, from, to, payload, status  string
	async                          bool
	deadlineMS                     int64
	deadline, createdAt, claimedAt time.Time
}

type workerRow struct {
	agent, project, state, session, taskID string
	lastSeen                               time.Time
	task                                   *taskRow // current task; nil when none or when its row is missing
}

const workerSelect = `
	SELECT w.agent, w.project, w.state, w.session, w.current_task_id, w.last_seen_at,
	       t.id, t.from_agent, t.payload, t.status, t.is_async, t.deadline_ms, t.deadline, t.created_at, t.claimed_at
	FROM workers w LEFT JOIN tasks t ON t.id = w.current_task_id`

func scanWorker(scanner interface{ Scan(dest ...any) error }) (*workerRow, error) {
	var w workerRow
	var taskID sql.NullString
	var lastSeen int64
	var tID, tFrom, tPayload, tStatus sql.NullString
	var tAsync, tDeadlineMS, tDeadline, tCreated, tClaimed sql.NullInt64
	if err := scanner.Scan(
		&w.agent, &w.project, &w.state, &w.session, &taskID, &lastSeen,
		&tID, &tFrom, &tPayload, &tStatus, &tAsync, &tDeadlineMS, &tDeadline, &tCreated, &tClaimed,
	); err != nil {
		return nil, err
	}
	w.taskID = taskID.String
	w.lastSeen = fromMillis(lastSeen)
	if tID.Valid {
		w.task = &taskRow{
			id:         tID.String,
			from:       tFrom.String,
			to:         w.agent,
			payload:    tPayload.String,
			status:     tStatus.String,
			async:      tAsync.Int64 == 1,
			deadlineMS: tDeadlineMS.Int64,
			deadline:   fromMillis(tDeadline.Int64),
			createdAt:  fromMillis(tCreated.Int64),
			claimedAt:  optionalMillis(tClaimed),
		}
	}
	return &w, nil
}

func loadWorker(ctx context.Context, q querier, agent string) (*workerRow, error) {
	w, err := scanWorker(q.QueryRowContext(ctx, workerSelect+` WHERE w.agent = ?`, agent))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return w, err
}

func loadWorkers(ctx context.Context, q querier) ([]*workerRow, error) {
	rows, err := q.QueryContext(ctx, workerSelect+` ORDER BY w.agent ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var workers []*workerRow
	for rows.Next() {
		w, err := scanWorker(rows)
		if err != nil {
			return nil, err
		}
		workers = append(workers, w)
	}
	return workers, rows.Err()
}

// loadQueues returns pending tasks per worker in claim order. An empty agent
// loads every worker's queue.
func loadQueues(ctx context.Context, q querier, agent string) (map[string][]QueuedTask, error) {
	query := `SELECT id, from_agent, to_agent, created_at, deadline_ms FROM tasks WHERE status = 'pending'`
	args := []any{}
	if agent != "" {
		query += ` AND to_agent = ?`
		args = append(args, agent)
	}
	rows, err := q.QueryContext(ctx, query+` ORDER BY to_agent ASC, created_at ASC, rowid ASC`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	queues := make(map[string][]QueuedTask)
	for rows.Next() {
		var task QueuedTask
		var created, deadlineMS int64
		if err := rows.Scan(&task.ID, &task.From, &task.To, &created, &deadlineMS); err != nil {
			return nil, err
		}
		task.Timestamp = fromMillis(created)
		task.WorkTimeout = (time.Duration(deadlineMS) * time.Millisecond).String()
		queues[task.To] = append(queues[task.To], task)
	}
	return queues, rows.Err()
}

func millis(value time.Time) int64 { return value.UnixMilli() }

func fromMillis(value int64) time.Time { return time.UnixMilli(value).UTC() }

func optionalMillis(value sql.NullInt64) time.Time {
	if !value.Valid {
		return time.Time{}
	}
	return fromMillis(value.Int64)
}

// --- claims -----------------------------------------------------------------

// resolveClaim examines a worker's current claim. It repairs a dangling claim,
// recovers an expired one (returning the recovered result), or reports the
// worker busy. A nil result with a nil error means the worker holds no claim.
func resolveClaim(ctx context.Context, tx *sql.Tx, w *workerRow, now time.Time) (*ListenResult, *Error) {
	if w == nil || w.taskID == "" {
		return nil, nil
	}
	if w.task == nil || w.task.status != taskWorking {
		// The claimed task vanished or was completed without clearing the
		// claim; nothing can be recovered, so drop the dangling reference.
		if _, err := tx.ExecContext(ctx,
			`UPDATE workers SET current_task_id = NULL, state = 'between_listens' WHERE agent = ? AND current_task_id = ?`,
			w.agent, w.taskID,
		); err != nil {
			return nil, databaseError("clear dangling claim", err)
		}
		w.taskID, w.task, w.state = "", nil, WorkerPolling
		return nil, nil
	}
	if !now.After(w.task.deadline) {
		return nil, workerUnavailable(w.agent, WorkerWorking)
	}
	if err := recoverExpiredClaim(ctx, tx, w, now); err != nil {
		return nil, databaseError("recover expired claim", err)
	}
	return &ListenResult{
		Agent:           w.agent,
		State:           "recovered",
		RecoveredTaskID: w.task.id,
		Message: &Message{
			ID:        w.task.id,
			From:      w.task.from,
			To:        w.agent,
			Payload:   w.task.payload,
			Timestamp: now,
			Deadline:  w.task.deadline,
		},
	}, nil
}

// recoverExpiredClaim retires a stale claim: an async task keeps a terminal
// recovered outcome for wait, a synchronous task is deleted because its asker
// has already timed out, and the offline worker retains its queue's project.
func recoverExpiredClaim(ctx context.Context, tx *sql.Tx, w *workerRow, now time.Time) error {
	if w.task.async {
		if _, err := tx.ExecContext(ctx,
			`UPDATE tasks SET status = 'recovered', updated_at = ? WHERE id = ? AND status = 'working'`,
			millis(now), w.task.id,
		); err != nil {
			return err
		}
	} else if _, err := tx.ExecContext(ctx, `DELETE FROM tasks WHERE id = ?`, w.task.id); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `UPDATE workers SET current_task_id = NULL, state = 'offline', session = '', last_seen_at = 0 WHERE agent = ? AND current_task_id = ?`, w.agent, w.task.id)
	return err
}

// claimPending claims the oldest claimable pending task for agent and records
// the claim on the worker row owned by session. Expired synchronous tasks are
// withdrawn on the way because their asker is gone. The deadline of an async
// task starts at claim time; a synchronous task keeps the asker's deadline.
func claimPending(ctx context.Context, tx *sql.Tx, agent, session string, now time.Time) (*Message, *Error) {
	rows, err := tx.QueryContext(ctx,
		`SELECT id, from_agent, payload, is_async, deadline_ms, deadline, created_at
		 FROM tasks WHERE to_agent = ? AND status = 'pending'
		 ORDER BY created_at ASC, rowid ASC`, agent)
	if err != nil {
		return nil, databaseError("load pending tasks", err)
	}
	var pending []taskRow
	for rows.Next() {
		var t taskRow
		var async int
		var deadline, created int64
		if err := rows.Scan(&t.id, &t.from, &t.payload, &async, &t.deadlineMS, &deadline, &created); err != nil {
			_ = rows.Close()
			return nil, databaseError("scan pending task", err)
		}
		t.async = async == 1
		t.deadline = fromMillis(deadline)
		t.createdAt = fromMillis(created)
		pending = append(pending, t)
	}
	if err := rows.Close(); err != nil {
		return nil, databaseError("load pending tasks", err)
	}

	for _, t := range pending {
		if !t.async && now.After(t.deadline) {
			if _, err := tx.ExecContext(ctx, `DELETE FROM tasks WHERE id = ? AND status = 'pending'`, t.id); err != nil {
				return nil, databaseError("withdraw expired task", err)
			}
			continue
		}
		deadline := t.deadline
		if t.async && t.deadlineMS > 0 {
			// A zero allowance can only come from a pre-versioned binary
			// writing into a migrated mailbox; keep its send-time deadline.
			deadline = now.Add(time.Duration(t.deadlineMS) * time.Millisecond)
		}
		claimed, err := tx.ExecContext(ctx,
			`UPDATE tasks SET status = 'working', claimed_at = ?, deadline = ?, updated_at = ? WHERE id = ? AND status = 'pending'`,
			millis(now), millis(deadline), millis(now), t.id,
		)
		if err != nil {
			return nil, databaseError("claim task", err)
		}
		if n, _ := claimed.RowsAffected(); n != 1 {
			continue
		}
		owned, err := tx.ExecContext(ctx,
			`UPDATE workers SET state = 'working', current_task_id = ?, last_seen_at = ? WHERE agent = ? AND session = ?`,
			t.id, millis(now), agent, session,
		)
		if err != nil {
			return nil, databaseError("record claim", err)
		}
		if n, _ := owned.RowsAffected(); n != 1 {
			return nil, newError("STATE_ERROR", fmt.Sprintf("agent %q lost its enrollment while claiming a task; listen again", agent), 1, 500)
		}
		return &Message{
			ID:        t.id,
			From:      t.from,
			To:        agent,
			Payload:   t.payload,
			Timestamp: t.createdAt,
			Deadline:  deadline,
		}, nil
	}
	return nil, nil
}

// A name may move projects only after the old worker is offline and its work
// is drained. Discovery is project-local, but names remain mailbox-global.
func checkWorkerProject(ctx context.Context, tx *sql.Tx, w *workerRow, project string, now time.Time) *Error {
	if w == nil || w.project == project {
		return nil
	}
	var pending bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM tasks WHERE to_agent = ? AND status IN ('pending', 'working'))`, w.agent).Scan(&pending); err != nil {
		return databaseError("check project ownership", err)
	}
	live := (w.state == WorkerListening && now.Sub(w.lastSeen) < ListenerLiveness) ||
		(w.state == WorkerPolling && now.Sub(w.lastSeen) < PollingPresence)
	if pending || live {
		return newError("PROJECT_MISMATCH", fmt.Sprintf("agent %q belongs to project %q; use a different name or drain its work in that project first", w.agent, w.project), 4, 409)
	}
	return nil
}

// enrollTx registers a blocking listener or recovers its expired claim. It
// rejects an active claim, a different live listener, or a project mismatch.
func (m *Mailbox) enrollTx(ctx context.Context, tx *sql.Tx, agent, project, session string, now time.Time) (*ListenResult, *Error) {
	w, err := loadWorker(ctx, tx, agent)
	if err != nil {
		return nil, databaseError("load worker", err)
	}
	if podErr := checkWorkerProject(ctx, tx, w, project, now); podErr != nil {
		return nil, podErr
	}
	recovered, podErr := resolveClaim(ctx, tx, w, now)
	if podErr != nil || recovered != nil {
		return recovered, podErr
	}
	if w != nil && w.state == WorkerListening && w.session != session && now.Sub(w.lastSeen) < ListenerLiveness {
		return nil, alreadyListening(agent)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO workers (agent, project, state, session, current_task_id, last_seen_at)
		VALUES (?, ?, 'listening', ?, NULL, ?)
		ON CONFLICT(agent) DO UPDATE SET
			project = excluded.project,
			state = 'listening',
			session = excluded.session,
			current_task_id = NULL,
			last_seen_at = excluded.last_seen_at`,
		agent, project, session, millis(now),
	); err != nil {
		return nil, databaseError("register listener", err)
	}
	return nil, nil
}

func (m *Mailbox) enroll(ctx context.Context, agent, project, session string) (*ListenResult, *Error) {
	var recovered *ListenResult
	podErr := m.write(ctx, "listen", func(tx *sql.Tx) *Error {
		var podErr *Error
		recovered, podErr = m.enrollTx(ctx, tx, agent, project, session, m.now().UTC())
		return podErr
	})
	return recovered, podErr
}

// --- operations -------------------------------------------------------------

// Listen implements worker enrollment and blocking message retrieval.
func (m *Mailbox) Listen(ctx context.Context, request ListenRequest) (ListenResult, *Error) {
	if err := validateAgentName("agent", request.Agent); err != nil {
		return ListenResult{}, err
	}
	if err := validateProject(request.Project); err != nil {
		return ListenResult{}, err
	}
	if err := validateListenWait(request.Wait); err != nil {
		return ListenResult{}, err
	}
	if err := ctx.Err(); err != nil {
		return ListenResult{}, contextError("listen", err, false)
	}
	session, err := randomID()
	if err != nil {
		return ListenResult{}, newError("ID_ERROR", "generate listener session", 1, 500)
	}

	m.pruneIfDue(ctx)

	recovered, podErr := m.enroll(ctx, request.Agent, request.Project, session)
	if podErr != nil {
		return ListenResult{}, podErr
	}
	if recovered != nil {
		return *recovered, nil
	}
	defer m.unregisterListener(request.Agent, session)

	var idle <-chan time.Time
	if request.Wait > 0 {
		timer := time.NewTimer(request.Wait)
		defer timer.Stop()
		idle = timer.C
	}
	poll := time.NewTicker(pollInterval)
	defer poll.Stop()
	heartbeat := time.NewTicker(heartbeatInterval)
	defer heartbeat.Stop()

	for {
		message, podErr := m.claimIfPending(ctx, request.Agent, request.Project, session)
		if podErr != nil {
			return ListenResult{}, podErr
		}
		if message != nil {
			return ListenResult{Agent: request.Agent, State: "assigned", Message: message}, nil
		}
		select {
		case <-ctx.Done():
			return ListenResult{}, contextError("listen", ctx.Err(), false)
		case <-idle:
			return ListenResult{Agent: request.Agent, State: "idle"}, nil
		case <-heartbeat.C:
			if podErr := m.heartbeat(ctx, request.Agent, request.Project, session); podErr != nil {
				return ListenResult{}, podErr
			}
		case <-poll.C:
		}
	}
}

// claimIfPending checks for pending work with a lock-free read and only takes
// the write lock when there is something to claim.
func (m *Mailbox) claimIfPending(ctx context.Context, agent, project, session string) (*Message, *Error) {
	var pending int
	if err := m.db.QueryRowContext(ctx, `SELECT count(*) FROM tasks WHERE to_agent = ? AND status = 'pending'`, agent).Scan(&pending); err != nil {
		return nil, m.storageError(ctx, "listen", err)
	}
	if pending == 0 {
		return nil, nil
	}
	var message *Message
	podErr := m.write(ctx, "listen", func(tx *sql.Tx) *Error {
		now := m.now().UTC()
		// Re-assert our enrollment first: the row may have been garbage
		// collected while this process was suspended, or taken over by a
		// newer listener, and only the owner may claim.
		if recovered, podErr := m.enrollTx(ctx, tx, agent, project, session, now); podErr != nil {
			return podErr
		} else if recovered != nil {
			return newError("STATE_ERROR", fmt.Sprintf("agent %q unexpectedly held an expired claim; listen again", agent), 1, 500)
		}
		var podErr *Error
		message, podErr = claimPending(ctx, tx, agent, session, now)
		return podErr
	})
	return message, podErr
}

// heartbeat refreshes the listener's presence. When the row is gone or owned
// by another session it re-enrolls, which reports ALREADY_LISTENING if the
// name was taken over while this process was suspended.
func (m *Mailbox) heartbeat(ctx context.Context, agent, project, session string) *Error {
	result, err := m.db.ExecContext(ctx,
		`UPDATE workers SET last_seen_at = ? WHERE agent = ? AND session = ? AND state = 'listening'`,
		millis(m.now().UTC()), agent, session,
	)
	if err != nil {
		return m.storageError(ctx, "listen", err)
	}
	if n, _ := result.RowsAffected(); n == 1 {
		return nil
	}
	recovered, podErr := m.enroll(ctx, agent, project, session)
	if podErr != nil {
		return podErr
	}
	if recovered != nil {
		return newError("STATE_ERROR", fmt.Sprintf("agent %q unexpectedly held an expired claim; listen again", agent), 1, 500)
	}
	return nil
}

// unregisterListener removes only this session's idle listener row. A claim
// switches the row to working and a newer session replaces it, so neither is
// touched.
func (m *Mailbox) unregisterListener(agent, session string) {
	_, _ = m.db.ExecContext(context.Background(),
		`UPDATE workers SET state = 'offline', session = '', last_seen_at = 0 WHERE agent = ? AND session = ? AND state = 'listening'`, agent, session)
}

// Pull immediately claims any pending task for the agent without blocking and
// enrolls the agent as a polling worker that stays addressable for
// PollingPresence.
func (m *Mailbox) Pull(ctx context.Context, agent, project string) (ListenResult, *Error) {
	if err := validateAgentName("agent", agent); err != nil {
		return ListenResult{}, err
	}
	if err := validateProject(project); err != nil {
		return ListenResult{}, err
	}
	if err := ctx.Err(); err != nil {
		return ListenResult{}, contextError("pull", err, false)
	}
	session, err := randomID()
	if err != nil {
		return ListenResult{}, newError("ID_ERROR", "generate listener session", 1, 500)
	}

	m.pruneIfDue(ctx)

	var result ListenResult
	podErr := m.write(ctx, "pull", func(tx *sql.Tx) *Error {
		now := m.now().UTC()
		w, err := loadWorker(ctx, tx, agent)
		if err != nil {
			return databaseError("load worker", err)
		}
		if podErr := checkWorkerProject(ctx, tx, w, project, now); podErr != nil {
			return podErr
		}
		recovered, podErr := resolveClaim(ctx, tx, w, now)
		if podErr != nil {
			return podErr
		}
		if recovered != nil {
			result = *recovered
			return nil
		}
		if w != nil && w.state == WorkerListening && now.Sub(w.lastSeen) < ListenerLiveness {
			return alreadyListening(agent)
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO workers (agent, project, state, session, current_task_id, last_seen_at)
			VALUES (?, ?, 'between_listens', ?, NULL, ?)
			ON CONFLICT(agent) DO UPDATE SET
				project = excluded.project,
				state = 'between_listens',
				session = excluded.session,
				current_task_id = NULL,
				last_seen_at = excluded.last_seen_at`,
			agent, project, session, millis(now),
		); err != nil {
			return databaseError("register polling worker", err)
		}
		message, podErr := claimPending(ctx, tx, agent, session, now)
		if podErr != nil {
			return podErr
		}
		if message != nil {
			result = ListenResult{Agent: agent, State: "assigned", Message: message}
		} else {
			result = ListenResult{Agent: agent, State: "idle"}
		}
		return nil
	})
	if podErr != nil {
		return ListenResult{}, podErr
	}
	return result, nil
}

// Send dispatches an asynchronous task into the worker's queue.
func (m *Mailbox) Send(ctx context.Context, request SendRequest) (SendResult, *Error) {
	if podErr := validateTaskDispatch(request.From, request.To, request.Payload, "send work timeout", request.Deadline); podErr != nil {
		return SendResult{}, podErr
	}
	return m.dispatch(ctx, "send", request.From, request.To, request.Payload, request.Deadline, true)
}

// Ask sends a synchronous task to a live listener and waits for its reply.
func (m *Mailbox) Ask(ctx context.Context, request AskRequest) (AskResult, *Error) {
	if podErr := validateTaskDispatch(request.From, request.To, request.Payload, "ask timeout", request.Timeout); podErr != nil {
		return AskResult{}, podErr
	}
	sent, podErr := m.dispatch(ctx, "ask", request.From, request.To, request.Payload, request.Timeout, false)
	if podErr != nil {
		return AskResult{}, podErr
	}
	message := Message{ID: sent.ID, To: request.To}

	timer := time.NewTimer(max(sent.expiresAt.Sub(m.now().UTC()), 0))
	defer timer.Stop()
	poll := time.NewTicker(pollInterval)
	defer poll.Stop()

	for {
		var status, reply, replyState string
		err := m.db.QueryRowContext(ctx,
			`SELECT status, COALESCE(reply, ''), COALESCE(reply_state, '') FROM tasks WHERE id = ?`, sent.ID,
		).Scan(&status, &reply, &replyState)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			// The worker's next listen recovered the expired claim.
			return AskResult{}, taskTimeoutError(message)
		case err != nil:
			if ctx.Err() != nil {
				return m.finalizeAsk(sent.ID, request, ctx.Err())
			}
			return AskResult{}, m.storageError(ctx, "ask", err)
		case status == taskCompleted:
			if replyState == "late" {
				return AskResult{}, taskTimeoutError(message)
			}
			// Synchronous outcomes have exactly one reader; pruning also covers this.
			_, _ = m.db.ExecContext(context.Background(), `DELETE FROM tasks WHERE id = ? AND status = 'completed'`, sent.ID)
			return AskResult{
				ID:      sent.ID,
				From:    request.To,
				To:      request.From,
				Payload: reply,
				State:   "completed",
			}, nil
		}

		select {
		case <-timer.C:
			return m.finalizeAsk(sent.ID, request, nil)
		case <-ctx.Done():
			return m.finalizeAsk(sent.ID, request, ctx.Err())
		case <-poll.C:
		}
	}
}

// finalizeAsk linearizes a timeout or cancellation with Reply. If the reply
// committed first, the caller receives it. Otherwise pending work is withdrawn,
// while an already-delivered claim remains with the worker.
func (m *Mailbox) finalizeAsk(id string, request AskRequest, cause error) (AskResult, *Error) {
	ctx := context.Background()
	var result AskResult
	var outcome *Error
	podErr := m.write(ctx, "finalize ask", func(tx *sql.Tx) *Error {
		deleted, err := tx.ExecContext(ctx, `DELETE FROM tasks WHERE id = ? AND status = 'pending'`, id)
		if err != nil {
			return databaseError("withdraw pending ask", err)
		}
		if n, _ := deleted.RowsAffected(); n == 1 {
			if cause != nil {
				outcome = contextError("ask", cause, false)
			} else {
				outcome = taskUnclaimedError(id, request.To)
			}
			return nil
		}

		var status, reply, replyState string
		err = tx.QueryRowContext(ctx,
			`SELECT status, COALESCE(reply, ''), COALESCE(reply_state, '') FROM tasks WHERE id = ?`, id,
		).Scan(&status, &reply, &replyState)
		if errors.Is(err, sql.ErrNoRows) {
			outcome = taskTimeoutError(Message{ID: id, To: request.To})
			return nil
		}
		if err != nil {
			return databaseError("load final ask state", err)
		}
		if status == taskCompleted && replyState != "late" {
			if _, err := tx.ExecContext(ctx, `DELETE FROM tasks WHERE id = ? AND status = 'completed'`, id); err != nil {
				return databaseError("delete completed ask", err)
			}
			result = AskResult{ID: id, From: request.To, To: request.From, Payload: reply, State: "completed"}
			return nil
		}
		if cause != nil {
			outcome = contextError("ask", cause, status == taskWorking)
		} else {
			outcome = taskTimeoutError(Message{ID: id, To: request.To})
		}
		return nil
	})
	if podErr != nil {
		return AskResult{}, podErr
	}
	return result, outcome
}

func (m *Mailbox) dispatch(ctx context.Context, operation, from, to, payload string, deadline time.Duration, async bool) (SendResult, *Error) {
	if err := ctx.Err(); err != nil {
		return SendResult{}, contextError(operation, err, false)
	}
	id, err := randomID()
	if err != nil {
		return SendResult{}, newError("ID_ERROR", "generate task correlation ID", 1, 500)
	}

	m.pruneIfDue(ctx)

	var result SendResult
	podErr := m.write(ctx, operation, func(tx *sql.Tx) *Error {
		now := m.now().UTC()
		w, err := loadWorker(ctx, tx, to)
		if err != nil {
			return databaseError("load worker", err)
		}
		if w == nil {
			return workerUnavailable(to, WorkerOffline)
		}
		if w.taskID != "" {
			switch {
			case w.task == nil || w.task.status != taskWorking:
				if _, podErr := resolveClaim(ctx, tx, w, now); podErr != nil {
					return podErr
				}
			case now.After(w.task.deadline):
				return workerUnavailable(to, WorkerStale)
			case !async:
				return workerUnavailable(to, WorkerWorking)
			}
		}
		if w.taskID == "" {
			present := false
			switch w.state {
			case WorkerListening:
				present = now.Sub(w.lastSeen) < ListenerLiveness
			case WorkerPolling:
				present = now.Sub(w.lastSeen) < PollingPresence
			}
			if !present {
				return workerUnavailable(to, WorkerOffline)
			}
			if !async && w.state != WorkerListening {
				return workerUnavailable(to, WorkerPolling)
			}
		}

		var depth int
		if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM tasks WHERE to_agent = ? AND status = 'pending'`, to).Scan(&depth); err != nil {
			return databaseError("count queued tasks", err)
		}
		if !async && depth > 0 {
			return workerUnavailable(to, WorkerWorking)
		}
		if async {
			if depth >= MaxQueueDepth {
				return queueFull(to)
			}
			var retained int
			if err := tx.QueryRowContext(ctx,
				`SELECT count(*) FROM tasks WHERE is_async = 1 AND status IN ('completed', 'recovered', 'expired', 'canceled')`,
			).Scan(&retained); err != nil {
				return databaseError("count retained results", err)
			}
			if retained >= MaxAsyncResults {
				return newError("RESULT_CAPACITY", fmt.Sprintf("skpod retains at most %d async results for %s; wait for older results to expire", MaxAsyncResults, AsyncResultRetention), 4, 503)
			}
		}

		provisionalDeadline := now.Add(deadline)
		asyncFlag := 0
		if async {
			asyncFlag = 1
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO tasks (id, from_agent, to_agent, payload, is_async, status, deadline_ms, deadline, created_at, updated_at)
			VALUES (?, ?, ?, ?, ?, 'pending', ?, ?, ?, ?)`,
			id, from, to, payload, asyncFlag, deadline.Milliseconds(), millis(provisionalDeadline), millis(now), millis(now),
		); err != nil {
			return databaseError("insert task", err)
		}
		result = SendResult{ID: id, From: from, To: to, State: "dispatched", WorkTimeout: deadline.String(), expiresAt: provisionalDeadline}
		return nil
	})
	if podErr != nil {
		return SendResult{}, podErr
	}
	return result, nil
}

// Reply records a worker's response to the task it currently holds. Only a
// claimed task can be answered, and only once.
func (m *Mailbox) Reply(request ReplyRequest) (ReplyResult, *Error) {
	return m.ReplyContext(context.Background(), request)
}

// ReplyContext is Reply with caller-controlled cancellation and timeout.
func (m *Mailbox) ReplyContext(ctx context.Context, request ReplyRequest) (ReplyResult, *Error) {
	if err := validateTaskID(request.ID); err != nil {
		return ReplyResult{}, err
	}
	if err := validatePayload("reply payload", request.Payload); err != nil {
		return ReplyResult{}, err
	}

	if err := ctx.Err(); err != nil {
		return ReplyResult{}, contextError("reply", err, false)
	}
	var result ReplyResult
	podErr := m.write(ctx, "reply", func(tx *sql.Tx) *Error {
		now := m.now().UTC()
		var toAgent, status string
		var deadline int64
		var held int
		err := tx.QueryRowContext(ctx, `
			SELECT t.to_agent, t.status, t.deadline,
			       EXISTS(SELECT 1 FROM workers w WHERE w.agent = t.to_agent AND w.state = 'working' AND w.current_task_id = t.id)
			FROM tasks t WHERE t.id = ?`, request.ID,
		).Scan(&toAgent, &status, &deadline, &held)
		if errors.Is(err, sql.ErrNoRows) {
			return taskNotFound(request.ID)
		}
		if err != nil {
			return databaseError("load task", err)
		}
		switch status {
		case taskPending:
			return newError("TASK_NOT_CLAIMED", fmt.Sprintf("task %s has not been claimed by agent %q; only a claimed task can be answered", request.ID, toAgent), 4, 409)
		case taskCompleted:
			return newError("TASK_ALREADY_COMPLETED", fmt.Sprintf("task %s already has a reply; outcomes are immutable", request.ID), 4, 409)
		case taskRecovered:
			return taskRecoveredError(request.ID, toAgent)
		case taskExpired:
			return taskExpiredError(request.ID, toAgent)
		case taskCanceled:
			return taskCanceledError(request.ID)
		case taskWorking:
			if held != 1 {
				return taskUnheldError(request.ID, toAgent)
			}
		default:
			return newError("STATE_ERROR", fmt.Sprintf("task %s has invalid state %q", request.ID, status), 1, 500)
		}

		state := "accepted"
		if now.After(fromMillis(deadline)) {
			state = "late"
		}
		completed, err := tx.ExecContext(ctx, `
			UPDATE tasks SET status = 'completed', reply = ?, reply_state = ?, completed_at = ?, updated_at = ?
			WHERE id = ? AND status = 'working'`,
			request.Payload, state, millis(now), millis(now), request.ID,
		)
		if err != nil {
			return databaseError("record reply", err)
		}
		if n, _ := completed.RowsAffected(); n != 1 {
			return taskNotFound(request.ID)
		}
		// Release only the claim that references this task; a listener that
		// took the name over in the meantime is left alone.
		if _, err := tx.ExecContext(ctx, `
			UPDATE workers SET current_task_id = NULL, state = 'between_listens', last_seen_at = ?
			WHERE agent = ? AND current_task_id = ?`,
			millis(now), toAgent, request.ID,
		); err != nil {
			return databaseError("release claim", err)
		}
		result = ReplyResult{ID: request.ID, State: state}
		return nil
	})
	if podErr != nil {
		return ReplyResult{}, podErr
	}
	return result, nil
}

// Wait blocks for an async task result. Completed and recovered outcomes are
// repeatable for AsyncResultRetention.
func (m *Mailbox) Wait(ctx context.Context, id string, wait time.Duration) (WaitResult, *Error) {
	if podErr := validateTaskID(id); podErr != nil {
		return WaitResult{}, podErr
	}
	if podErr := validateWait("wait timeout", wait, MaxWait); podErr != nil {
		return WaitResult{}, podErr
	}
	if err := ctx.Err(); err != nil {
		return WaitResult{}, contextError("wait", err, false)
	}

	timer := time.NewTimer(wait)
	defer timer.Stop()
	poll := time.NewTicker(pollInterval)
	defer poll.Stop()

	for {
		m.pruneIfDue(ctx)
		var from, to, reply, status, replyState string
		var async int
		err := m.db.QueryRowContext(ctx,
			`SELECT from_agent, to_agent, COALESCE(reply, ''), status, COALESCE(reply_state, ''), is_async FROM tasks WHERE id = ?`, id,
		).Scan(&from, &to, &reply, &status, &replyState, &async)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			return WaitResult{}, taskNotFound(id)
		case err != nil:
			return WaitResult{}, m.storageError(ctx, "wait", err)
		case async == 0:
			return WaitResult{}, newError("TASK_NOT_ASYNC", fmt.Sprintf("task %q was created by ask and cannot be retrieved with wait", id), 4, 409)
		case status == taskCompleted:
			return WaitResult{ID: id, From: to, To: from, Payload: reply, State: "completed", ReplyState: replyState}, nil
		case status == taskRecovered:
			return WaitResult{}, taskRecoveredError(id, to)
		case status == taskExpired:
			return WaitResult{}, taskExpiredError(id, to)
		case status == taskCanceled:
			return WaitResult{}, taskCanceledError(id)
		}

		select {
		case <-timer.C:
			return m.finalizeWait(id)
		case <-ctx.Done():
			return WaitResult{}, contextError("wait", ctx.Err(), false)
		case <-poll.C:
		}
	}
}

// finalizeWait serializes the timeout boundary with Reply, ensuring a result
// committed before the boundary is returned instead of spuriously timing out.
func (m *Mailbox) finalizeWait(id string) (WaitResult, *Error) {
	ctx := context.Background()
	var result WaitResult
	var outcome *Error
	var agent string
	podErr := m.write(ctx, "finalize wait", func(tx *sql.Tx) *Error {
		var from, to, reply, status, replyState string
		var async int
		err := tx.QueryRowContext(ctx,
			`SELECT from_agent, to_agent, COALESCE(reply, ''), status, COALESCE(reply_state, ''), is_async FROM tasks WHERE id = ?`, id,
		).Scan(&from, &to, &reply, &status, &replyState, &async)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			outcome = taskNotFound(id)
		case err != nil:
			return databaseError("load final wait state", err)
		case async == 0:
			outcome = newError("TASK_NOT_ASYNC", fmt.Sprintf("task %q was created by ask and cannot be retrieved with wait", id), 4, 409)
		case status == taskCompleted:
			result = WaitResult{ID: id, From: to, To: from, Payload: reply, State: "completed", ReplyState: replyState}
		case status == taskRecovered:
			outcome = taskRecoveredError(id, to)
		case status == taskExpired:
			outcome = taskExpiredError(id, to)
		case status == taskCanceled:
			outcome = taskCanceledError(id)
		default:
			agent = to
		}
		return nil
	})
	if podErr != nil {
		return WaitResult{}, podErr
	}
	if outcome != nil || result.ID != "" {
		return result, outcome
	}
	return WaitResult{}, m.asyncWaitTimeoutError(id, agent)
}

// Task returns the durable state of one task without waiting or mutating it.
func (m *Mailbox) Task(ctx context.Context, id string) (TaskStatus, *Error) {
	if podErr := validateTaskID(id); podErr != nil {
		return TaskStatus{}, podErr
	}
	m.pruneIfDue(ctx)
	var result TaskStatus
	var deadlineMS, expires, created int64
	var claimed, completed sql.NullInt64
	err := m.db.QueryRowContext(ctx, `
		SELECT id, from_agent, to_agent, payload, COALESCE(reply, ''), status,
		       COALESCE(reply_state, ''), deadline_ms, deadline, created_at, claimed_at, completed_at
		FROM tasks WHERE id = ?`, id,
	).Scan(&result.ID, &result.From, &result.To, &result.Payload, &result.Reply, &result.State,
		&result.ReplyState, &deadlineMS, &expires, &created, &claimed, &completed)
	if errors.Is(err, sql.ErrNoRows) {
		return TaskStatus{}, taskNotFound(id)
	}
	if err != nil {
		return TaskStatus{}, m.storageError(ctx, "task", err)
	}
	result.WorkTimeout = (time.Duration(deadlineMS) * time.Millisecond).String()
	result.CreatedAt = fromMillis(created)
	result.ClaimedAt = optionalMillis(claimed)
	result.CompletedAt = optionalMillis(completed)
	if !result.ClaimedAt.IsZero() {
		result.ExpiresAt = fromMillis(expires)
		if result.State == taskWorking && m.now().UTC().After(result.ExpiresAt) {
			result.State = WorkerStale
		}
	}
	return result, nil
}

// Cancel retires pending asynchronous work. Claimed work cannot be canceled
// because skpod cannot stop an already-running agent safely.
func (m *Mailbox) Cancel(ctx context.Context, id string) (CancelResult, *Error) {
	if podErr := validateTaskID(id); podErr != nil {
		return CancelResult{}, podErr
	}
	var result CancelResult
	podErr := m.write(ctx, "cancel", func(tx *sql.Tx) *Error {
		var status string
		var async int
		if err := tx.QueryRowContext(ctx, `SELECT status, is_async FROM tasks WHERE id = ?`, id).Scan(&status, &async); errors.Is(err, sql.ErrNoRows) {
			return taskNotFound(id)
		} else if err != nil {
			return databaseError("load task", err)
		}
		if async == 0 {
			return newError("TASK_NOT_ASYNC", "only tasks created by send can be canceled", 4, 409)
		}
		if status != taskPending {
			return newError("TASK_NOT_PENDING", fmt.Sprintf("task %s is %s and can no longer be canceled", id, status), 4, 409)
		}
		now := millis(m.now().UTC())
		if _, err := tx.ExecContext(ctx, `UPDATE tasks SET status = 'canceled', completed_at = ?, updated_at = ? WHERE id = ? AND status = 'pending'`, now, now, id); err != nil {
			return databaseError("cancel task", err)
		}
		result = CancelResult{ID: id, State: taskCanceled}
		return nil
	})
	return result, podErr
}

// Doctor reports mailbox health and high-signal operational counts.
func (m *Mailbox) Doctor(ctx context.Context) (DoctorResult, *Error) {
	result := DoctorResult{State: "healthy", Path: m.path, Schema: SchemaVersion, Tasks: make(map[string]int)}
	if err := m.db.QueryRowContext(ctx, `PRAGMA quick_check`).Scan(&result.State); err != nil {
		return DoctorResult{}, m.storageError(ctx, "doctor", err)
	}
	if result.State != "ok" {
		return result, newError("DATABASE_CORRUPT", "SQLite quick_check reported: "+result.State, 1, 500)
	}
	result.State = "healthy"
	if err := m.db.QueryRowContext(ctx, `SELECT count(*) FROM workers`).Scan(&result.Workers); err != nil {
		return DoctorResult{}, m.storageError(ctx, "doctor", err)
	}
	rows, err := m.db.QueryContext(ctx, `SELECT status, count(*) FROM tasks GROUP BY status ORDER BY status`)
	if err != nil {
		return DoctorResult{}, m.storageError(ctx, "doctor", err)
	}
	for rows.Next() {
		var state string
		var count int
		if err := rows.Scan(&state, &count); err != nil {
			return DoctorResult{}, databaseError("doctor task counts", err)
		}
		result.Tasks[state] = count
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return DoctorResult{}, databaseError("doctor task counts", err)
	}
	if err := rows.Close(); err != nil {
		return DoctorResult{}, databaseError("doctor task counts", err)
	}
	var last sql.NullInt64
	if err := m.db.QueryRowContext(ctx, `SELECT value FROM meta WHERE key = 'last_prune_at'`).Scan(&last); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return DoctorResult{}, m.storageError(ctx, "doctor", err)
	}
	result.LastPruneAt = optionalMillis(last)
	return result, nil
}

func (m *Mailbox) asyncWaitTimeoutError(id, agent string) *Error {
	status, podErr := m.Status(agent)
	if podErr != nil {
		return podErr
	}
	guidance := "call wait again"
	switch status.State {
	case WorkerStale:
		guidance = "call wait again for a late reply, or have the worker resume and re-listen to record TASK_RECOVERED"
	case WorkerOffline:
		guidance = "the worker is not present; have it listen again so it can claim the task, or stop waiting"
	}
	return newError(
		"WAIT_TIMEOUT",
		fmt.Sprintf("wait for task %s timed out; the task was not canceled and agent %q is %s; %s", id, agent, status.State, guidance),
		3,
		408,
	)
}

// Status returns a worker's current coordination status, including its queue.
func (m *Mailbox) Status(agent string) (WorkerStatus, *Error) {
	return m.StatusContext(context.Background(), agent)
}

// StatusContext is Status with caller-controlled cancellation and timeout.
func (m *Mailbox) StatusContext(ctx context.Context, agent string) (WorkerStatus, *Error) {
	if err := validateAgentName("agent", agent); err != nil {
		return WorkerStatus{}, err
	}
	if err := ctx.Err(); err != nil {
		return WorkerStatus{}, contextError("status", err, false)
	}
	var status WorkerStatus
	podErr := m.read(ctx, "status", func(tx *sql.Tx) *Error {
		w, err := loadWorker(ctx, tx, agent)
		if err != nil {
			return databaseError("load worker", err)
		}
		queues, err := loadQueues(ctx, tx, agent)
		if err != nil {
			return databaseError("load queue", err)
		}
		status = m.statusOf(agent, w, queues[agent])
		return nil
	})
	if podErr != nil {
		return WorkerStatus{}, podErr
	}
	return status, nil
}

func (m *Mailbox) statusOf(agent string, w *workerRow, queue []QueuedTask) WorkerStatus {
	now := m.now().UTC()
	status := WorkerStatus{Agent: agent, State: WorkerOffline, QueueDepth: len(queue), Queue: queue}
	if w == nil {
		return status
	}
	if w.taskID != "" && w.task != nil && w.task.status == taskWorking {
		status.Project = w.project
		status.State = WorkerWorking
		if now.After(w.task.deadline) {
			status.State = WorkerStale
		}
		status.TaskID = w.task.id
		status.Deadline = w.task.deadline
		status.ClaimedAt = w.task.claimedAt
		return status
	}
	switch w.state {
	case WorkerListening:
		if now.Sub(w.lastSeen) < ListenerLiveness {
			status.State = WorkerListening
		}
	case WorkerPolling:
		if now.Sub(w.lastSeen) < PollingPresence {
			status.State = WorkerPolling
		}
	}
	if status.State != WorkerOffline {
		status.Project = w.project
	}
	return status
}

// List returns every present worker across projects in name order.
func (m *Mailbox) List() ([]WorkerStatus, *Error) {
	return m.ListContext(context.Background())
}

// ListContext is List with caller-controlled cancellation and timeout.
func (m *Mailbox) ListContext(ctx context.Context) ([]WorkerStatus, *Error) {
	if err := ctx.Err(); err != nil {
		return nil, contextError("list", err, false)
	}
	m.pruneIfDue(ctx)

	statuses := make([]WorkerStatus, 0)
	podErr := m.read(ctx, "list", func(tx *sql.Tx) *Error {
		workers, err := loadWorkers(ctx, tx)
		if err != nil {
			return databaseError("load workers", err)
		}
		queues, err := loadQueues(ctx, tx, "")
		if err != nil {
			return databaseError("load queues", err)
		}
		for _, w := range workers {
			status := m.statusOf(w.agent, w, queues[w.agent])
			if status.State != WorkerOffline {
				statuses = append(statuses, status)
			}
		}
		return nil
	})
	if podErr != nil {
		return nil, podErr
	}
	sort.Slice(statuses, func(i, j int) bool { return statuses[i].Agent < statuses[j].Agent })
	return statuses, nil
}

// --- garbage collection -----------------------------------------------------

// pruneIfDue garbage-collects expired state at most once per pruneInterval
// across all processes. It is best effort: a failure here never fails the
// caller's operation, and the next command retries.
func (m *Mailbox) pruneIfDue(ctx context.Context) {
	now := m.now().UTC()
	var last int64
	err := m.db.QueryRowContext(ctx, `SELECT value FROM meta WHERE key = 'last_prune_at'`).Scan(&last)
	if err == nil && now.Sub(fromMillis(last)) < pruneInterval {
		return
	}
	_ = m.write(ctx, "prune", func(tx *sql.Tx) *Error {
		if err := tx.QueryRowContext(ctx, `SELECT value FROM meta WHERE key = 'last_prune_at'`).Scan(&last); err == nil && now.Sub(fromMillis(last)) < pruneInterval {
			return nil
		}
		if err := prune(ctx, tx, now); err != nil {
			return databaseError("prune", err)
		}
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO meta (key, value) VALUES ('last_prune_at', ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
			millis(now),
		); err != nil {
			return databaseError("record prune", err)
		}
		return nil
	})
}

func prune(ctx context.Context, tx *sql.Tx, now time.Time) error {
	retentionCutoff := millis(now.Add(-AsyncResultRetention))
	queueCutoff := millis(now.Add(-AsyncQueueRetention))
	abandonedCutoff := millis(now.Add(-abandonedWorkerRetention))

	// Retained outcomes past retention.
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM tasks WHERE status IN ('completed', 'recovered', 'expired', 'canceled') AND COALESCE(completed_at, updated_at) < ?`, retentionCutoff,
	); err != nil {
		return err
	}
	// Queue retention is independent of the working deadline. Expired async
	// tasks keep a terminal outcome long enough for wait to explain what happened.
	if _, err := tx.ExecContext(ctx, `
		UPDATE tasks SET status = 'expired', completed_at = ?, updated_at = ?
		WHERE is_async = 1 AND status = 'pending' AND created_at < ?`,
		millis(now), millis(now), queueCutoff,
	); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM tasks WHERE is_async = 0 AND status = 'pending' AND deadline < ?`, millis(now)); err != nil {
		return err
	}

	// Claims that stayed stale for a full retention period, and working tasks
	// no worker references, are recovered exactly as a re-listen would do.
	rows, err := tx.QueryContext(ctx, `
		SELECT t.id, t.is_async, t.to_agent
		FROM tasks t LEFT JOIN workers w ON w.current_task_id = t.id
		WHERE t.status = 'working' AND (t.deadline < ? OR w.agent IS NULL)`, retentionCutoff)
	if err != nil {
		return err
	}
	type abandoned struct {
		id, agent string
		async     bool
	}
	var claims []abandoned
	for rows.Next() {
		var claim abandoned
		var async int
		if err := rows.Scan(&claim.id, &async, &claim.agent); err != nil {
			_ = rows.Close()
			return err
		}
		claim.async = async == 1
		claims = append(claims, claim)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, claim := range claims {
		if claim.async {
			if _, err := tx.ExecContext(ctx, `UPDATE tasks SET status = 'recovered', updated_at = ? WHERE id = ?`, millis(now), claim.id); err != nil {
				return err
			}
		} else if _, err := tx.ExecContext(ctx, `DELETE FROM tasks WHERE id = ?`, claim.id); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE workers SET current_task_id = NULL, state = 'offline', session = '', last_seen_at = 0 WHERE agent = ? AND current_task_id = ?`, claim.agent, claim.id); err != nil {
			return err
		}
	}

	// Worker rows nobody has touched for a long time, whatever state a killed
	// process left them in.
	_, err = tx.ExecContext(ctx, `DELETE FROM workers WHERE current_task_id IS NULL AND last_seen_at < ?
		AND NOT EXISTS (SELECT 1 FROM tasks WHERE to_agent = workers.agent AND status IN ('pending', 'working'))`, abandonedCutoff)
	return err
}

// --- errors -----------------------------------------------------------------

func databaseError(operation string, err error) *Error {
	return newError("DATABASE_ERROR", fmt.Sprintf("%s: %v", operation, err), 1, 500)
}

func schemaMismatch(found int) *Error {
	return newError(
		"SCHEMA_MISMATCH",
		fmt.Sprintf("mailbox schema version %d is incompatible with this skpod build (requires %d); stop workers and point SKPOD_DB at a fresh file", found, SchemaVersion),
		1,
		500,
	)
}

func alreadyListening(agent string) *Error {
	return newError(
		"ALREADY_LISTENING",
		fmt.Sprintf("agent %q already has an active listener; choose a unique agent name or stop the other listener", agent),
		4,
		409,
	)
}

func queueFull(agent string) *Error {
	return newError(
		"QUEUE_FULL",
		fmt.Sprintf("agent %q already has %d queued tasks; wait for it to drain or inspect it with `skpod status %s`", agent, MaxQueueDepth, agent),
		4,
		409,
	)
}

func taskRecoveredError(id, agent string) *Error {
	return newError(
		"TASK_RECOVERED",
		fmt.Sprintf("task %s expired and agent %q cleared its claim by listening again; no reply is available", id, agent),
		3,
		410,
	)
}

func taskExpiredError(id, agent string) *Error {
	return newError(
		"TASK_EXPIRED",
		fmt.Sprintf("task %s was not claimed by agent %q within the %s queue-retention window", id, agent, AsyncQueueRetention),
		3,
		410,
	)
}

func taskCanceledError(id string) *Error {
	return newError("TASK_CANCELED", fmt.Sprintf("task %s was canceled before it was claimed", id), 3, 410)
}

func taskUnheldError(id, agent string) *Error {
	return newError(
		"TASK_NOT_HELD",
		fmt.Sprintf("task %s is no longer held by agent %q; have the worker listen again to recover mailbox state", id, agent),
		4,
		409,
	)
}

func taskUnclaimedError(id, agent string) *Error {
	return newError(
		"TASK_TIMEOUT",
		fmt.Sprintf("task %s was withdrawn because agent %q did not claim it before the deadline; its listener is no longer live", id, agent),
		3,
		504,
	)
}
