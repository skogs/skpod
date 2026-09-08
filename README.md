# skpod

**Let one coding-agent session give work to another.**

Once `skpod` is installed and your repository has the suggested `AGENTS.md`
instructions, you use it by talking to your agents normally.

In one session, say:

> Listen as agent reviewer.

In another session, say:

> Ask reviewer to review my current changes. Do not let it edit anything.

That is the main `skpod` workflow. The agents handle worker discovery, task
delivery, waiting, and replies through the CLI.

`skpod` connects agent sessions already running on the same machine. It does not
launch agents or require a server.

Status: experimental v0.1, intended for cooperating, trusted local agents.

## Set it up

### 1. Install `skpod`

You need Go 1.27.1 or newer. Install the latest release with:

```powershell
go install github.com/skogs/skpod/cmd/skpod@latest
```

Go installs the command into `GOBIN`, or into `$(go env GOPATH)/bin` when
`GOBIN` is unset. Add that directory to `PATH` and verify the installation:

```powershell
skpod --version
```

Prebuilt archives and checksums for Windows, Linux, and macOS are available on
the [Releases](https://github.com/skogs/skpod/releases) page. To build from a
checkout instead, run `go install ./cmd/skpod`.

### 2. Teach your agents how to use it

Add the following to the `AGENTS.md` file in any repository where you want to
use `skpod`:

```markdown
## Working with other agents

Use `skpod --help` as the authoritative workflow guide.

- To listen as a named agent, run `skpod agent NAME` and follow its instructions.
- Before delegating, run `skpod list` and use only a suitable same-project worker.
  If none is available, say so.
- Give tasks exact scope and permissions. Read-only work must forbid edits,
  staging, and commits.
- Use `ask` when blocked on the result. Otherwise use `send`, retain `data.id`,
  continue useful work, then `wait` once. Never resend solely because a wait
  timed out.
```

Commit this file so every agent session in the repository receives the same
instructions.

## Use it from your agent conversations

Open two agent sessions in the same Git repository. Both sessions must have
`skpod` on `PATH`.

### Start a worker

Tell the session that will receive delegated work:

> Listen as agent reviewer.

The agent runs `skpod agent reviewer`, follows the generated worker loop, and
waits for tasks. Names are yours to choose; examples include `reviewer`,
`researcher`, and `tester`.

### Delegate a task

In another session, refer to the worker by name:

> Ask reviewer to inspect the current diff for correctness and missing tests.
> This is read-only: do not edit, stage, or commit.

The session discovers `reviewer`, sends the request, and returns its response.

You can delegate any well-scoped work, for example:

> Ask researcher to trace the login failure and explain the root cause. Do not
> change files.

> Have tester inspect the current test coverage and suggest the three most
> important missing cases.

> Send reviewer a review of the current diff. I have other work to do, so collect
> its response afterward.

The last wording signals that the sending agent should use asynchronous
`send`/`wait` instead of blocking immediately with `ask`.

### Pause listening for a conversation

An agent session is not locked into worker mode. While it is waiting idle, stop
the running listener and ask the agent questions or give it interactive work as
usual. When you are done, say:

> Listen again as agent reviewer.

The agent resumes under the same name. If it has already received a delegated
task, let it finish and reply to that task before returning to the listener.

### Stop a worker

Tell its session:

> Stop listening.

You can reuse an offline worker name after its pending work has been cleared.

## Tips for good delegation

- Name the worker you intend to use.
- State exactly what it should inspect and return.
- Say whether it may edit files, run commands, stage changes, or commit.
- Use different workers for independent tasks that can run in parallel.
- Keep both sessions in the same Git repository so project-local discovery works.
  The primary checkout and its linked Git worktrees are treated as one project.

Workers do not share conversation context. Include all information the receiving
agent needs in the delegated request.

## When you need the CLI directly

Most users can let their agents run these commands. They are also useful for
manual troubleshooting:

| Command | Purpose |
| --- | --- |
| `skpod version` | print the installed version |
| `skpod agent NAME` | become a named worker and print its instructions |
| `skpod list` | show available workers in the current project |
| `skpod ask NAME` | send work and wait for the reply |
| `skpod send NAME` | queue work and return a task ID |
| `skpod wait TASK_ID` | wait for a previously sent task |
| `skpod status NAME` | inspect a worker and its queued work |
| `skpod task TASK_ID` | inspect a task |
| `skpod cancel TASK_ID` | cancel a task that has not been claimed |
| `skpod doctor` | diagnose the local mailbox |

Run `skpod --help` for the complete command reference.

## Troubleshooting

**The agent says the worker is unavailable.** Check that the worker session is
still listening, both sessions are in the same repository, and both use the same
local mailbox. Ask the agent to run `skpod list`.

**A wait timed out.** The task may still finish. Keep its task ID and wait again
later; do not send a duplicate task.

**A task needs to be stopped.** `skpod cancel TASK_ID` can cancel queued work.
Once a separate agent has claimed a task, `skpod` cannot stop that agent's
execution.

**The mailbox looks unhealthy.** Run `skpod doctor --format table`. To clear the
mailbox, stop all workers and mailbox commands first, then run `skpod reset
--yes`. A compatible mailbox is backed up before it is cleared.

## Scope and trust

- `skpod` is for cooperating, trusted agent sessions on one machine.
- Task payloads are stored locally as plaintext. Anyone with mailbox access can
  read them.
- Worker names are unique across the local mailbox, even though discovery
  defaults to the current Git project.
- `skpod` does not start agents, enforce their permissions, retry work, or keep a
  permanent transcript.

## Development

```sh
go test ./...
go vet ./...
go build ./cmd/skpod
```

CI builds and tests on Windows, Linux, and macOS, with race detection on Linux.

## License

[MIT](LICENSE). Third-party dependencies retain their own licenses.
