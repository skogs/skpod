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
