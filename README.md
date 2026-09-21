# skpod

**Let one coding-agent session delegate work to another.**

`skpod` connects local AI coding sessions (Pi, Claude Code, Cursor, Codex) running in different terminal tabs on the same machine. It uses a serverless, shared local SQLite mailbox to pass tasks and replies.

- **Zero background daemons** — no servers to run, configure, or keep alive.
- **Zero cloud dependencies** — runs entirely on your local machine.
- **Cross-worktree discovery** — primary checkouts and linked Git worktrees share the same project namespace automatically.
- **Dead-process detection** — instantly catches terminated worker subshells without waiting for timeouts.

---

## 60-Second Quickstart

### Step 1: Install `skpod`

No Go toolchain required. Run the one-liner for your platform:

**macOS & Linux**:
```sh
curl -fsSL https://raw.githubusercontent.com/skogs/skpod/main/install.sh | sh
```
*Or via Homebrew:*
```sh
brew install https://raw.githubusercontent.com/skogs/skpod/main/Formula/skpod.rb
```

**Windows** (PowerShell):
```powershell
irm https://raw.githubusercontent.com/skogs/skpod/main/install.ps1 | iex
```

**Manual Binary Download**:  
Download the archive for your OS from **[GitHub Releases](https://github.com/skogs/skpod/releases/latest)**, extract `skpod`, and place it in your `PATH`.

*(Or build from source if you have Go: `go install github.com/skogs/skpod/cmd/skpod@latest`)*

---

### Step 2: Add `AGENTS.md` to your repository

In the root of your Git repository, create `AGENTS.md` (or add to your existing agent instructions):

```markdown
# AGENTS.md

## Agent collaboration

Use `skpod --help` as the authoritative workflow guide.

- To listen as a named agent, run `skpod agent NAME` and follow its instructions.
- Before delegating, run `skpod list` and use only a suitable same-project worker.
  If none is available, say so.
- Give tasks exact scope and permissions. Reviews must forbid edits, staging,
  and commits; resolve or explicitly defer findings before committing.
- Use `ask` when blocked on the result. Otherwise use `send`, retain `data.id`,
  continue useful work, then `wait` once. Never resend solely because a wait
  timed out.

## Troubleshooting

- If an expected worker is missing, run `skpod list --all` to check whether it
  enrolled from another project.
- Run `skpod doctor` to inspect the mailbox and detect terminated listener
  processes.
- For a timed-out task, inspect both `skpod task TASK_ID` and
  `skpod status AGENT` before deciding whether to resend it.
```

Commit this file so any agent session in your repository automatically knows how to cooperate.

---

### Step 3: Open Two Terminals and Delegate!

Open two terminal tabs in the same repository:

```text
┌────────────────────────────────────────┐   ┌────────────────────────────────────────┐
│ Terminal 1 (Worker)                    │   │ Terminal 2 (Driver)                    │
├────────────────────────────────────────┤   ├────────────────────────────────────────┤
│ You tell your agent:                   │   │ You tell your agent:                   │
│                                        │   │                                        │
│ > Listen as agent reviewer.            │   │ > Ask reviewer to inspect auth.go      │
│                                        │   │   for security issues. Do not edit.    │
│                                        │   │                                        │
│ What happens under the hood:           │   │ What happens under the hood:           │
│ 1. Agent runs `skpod agent reviewer`   │   │ 1. Agent runs `skpod list`             │
│ 2. Agent runs `skpod listen reviewer`  │   │ 2. Agent runs `skpod ask reviewer ...` │
│ 3. Receives task, inspects auth.go     │   │ 3. Waits for reply                     │
│ 4. Replies via `skpod reply`           │   │ 4. Displays reviewer's findings        │
│ 5. Returns to listening loop           │   │                                        │
└────────────────────────────────────────┘   └────────────────────────────────────────┘
```

That's it! The agents discover each other, pass tasks, wait, and reply through `skpod`.

---

## How to Talk to Your Agents

Once `AGENTS.md` is present, you don't need to memorize CLI flags—just speak to your agents naturally:

### Starting a Worker
- *"Listen as agent reviewer."*
- *"Become worker tester and wait for tasks."*
- *"Listen as researcher."*

### Delegating Synchronous Work (Blocks until finished)
- *"Ask reviewer to review my uncommitted changes. Read-only: do not edit or stage files."*
- *"Ask tester to check coverage on pkg/api and suggest the 3 most important missing unit tests."*

### Delegating Asynchronous Work (Queues work and continues)
- *"Send researcher a request to trace the login error. I have other work to do, so collect its answer afterward."*

### Pausing or Stopping
- *"Stop listening."* — Exits the listen loop so you can chat or give that session interactive tasks.
- *"Listen again as agent reviewer."* — Re-joins the listen pool under the same name.

---

## When You Need the CLI Directly

You can also run `skpod` directly from your shell for testing, scripting, or manual triage:

| Command | What It Does |
| :--- | :--- |
| `skpod list` | Show active workers in your current project |
| `skpod list --all` | Show active workers across all local projects |
| `skpod ask AGENT --stdin` | Send a task to a listening worker and wait for its reply |
| `skpod send AGENT --stdin` | Queue a task into a worker's mailbox and return task ID |
| `skpod wait TASK_ID` | Wait for a previously sent asynchronous task |
| `skpod status AGENT` | Inspect a worker's state (`listening`, `working`, `offline`, etc.) |
| `skpod task TASK_ID` | Inspect a task's progress, timestamps, and payload/reply |
| `skpod doctor` | Diagnose local mailbox health, database path, and dead listeners |
| `skpod reset --yes` | Back up and reinitialize a clean local mailbox |

Run `skpod --help` for the complete command reference.

---

## Troubleshooting & Diagnostics

If communication between agents stalls or reports an error, run these three diagnostic commands in order:

```sh
# 1. Is the local environment and mailbox healthy?
skpod doctor --format table

# 2. Are workers listening, and in which projects?
skpod list --all --format table

# 3. What is the worker or task actually doing?
skpod status AGENT --format table
skpod task TASK_ID --format table
```

### Common Issues

- **`AGENT_OFFLINE` (worker not found)**:
  - Run `skpod list --all`. If the worker is running in a different folder or repository, it will show under a different `PROJECT`. Either move your terminal or enroll the worker in the current repo.
  - If the worker's subshell was killed by your harness or closed, `skpod doctor` will identify it as a `DEAD LISTENER`. Tell that agent session: *"Listen again as agent NAME"*.
- **A task timed out**:
  - Run `skpod task TASK_ID`. It will show whether the worker claimed the task, is still working, or encountered an error.
- **Git Worktrees**:
  - `skpod` automatically shares agent discovery between your main checkout and all linked Git worktrees. Both resolve to the same canonical project root.

---

## Development

```sh
go test ./...
go vet ./...
go build ./cmd/skpod
```

CI builds and tests on Windows, Linux, and macOS.

## License

[MIT](LICENSE).
