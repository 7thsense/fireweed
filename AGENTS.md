<!-- DDX-AGENTS:START -->
<!-- Managed by ddx init / ddx update. Edit outside these markers. -->

# DDx

This project uses [DDx](https://github.com/DocumentDrivenDX/ddx) for
document-driven development. Use the `ddx` skill for beads, work,
review, agents, and status — every skills-compatible harness (Claude
Code, OpenAI Codex, Gemini CLI, etc.) discovers it from
`.claude/skills/ddx/` and `.agents/skills/ddx/`.

## Default Interactive Mode

Broad conversational DDx prompts — queue orientation, planning, review,
guidance folding, spec alignment, and bead breakdown — use
`interactive-steward` / `queue_steward`. Explicit worker commands
(`ddx work`, `ddx try <id>`, "execute bead `<id>`") route to
`bead_execution`. Explicit code/doc edit requests route to
`direct_user_implementation`. Explicit review-only requests route to
`review`.

`DDX_MODE=bead_execution` overrides only the interactive queue-steward default.
It **never** overrides tracker, merge, commit, safety, or verification policy —
those apply in every mode.

### Mutation policy

- **read / plan / fresh-eyes review / fold guidance / align specs** — non-mutating
  by default; no tracker writes, no code edits.
- **Tracker mutation** (e.g. `ddx bead create`, `ddx bead update`) requires an
  explicit durable-output verb: "create a bead", "file this as work",
  "break down into beads".
- **Code edits** require explicit implementation intent ("fix this",
  "implement X") or `bead_execution` mode.

## Files to commit

After modifying any of these paths, stage and commit them:

- `.ddx/beads.jsonl` — work item tracker
- `.ddx/config.yaml` — project configuration
- `.agents/skills/ddx/` — the ddx skill (shipped by ddx init)
- `.claude/skills/ddx/` — same skill, Claude Code location
- `docs/` — project documentation and artifacts

## Conventions

- Use `ddx bead` for work tracking (not custom issue files).
- Documents with `ddx:` frontmatter are tracked in the document graph.
- Run `ddx doctor` to check environment health.
- Run `ddx doc stale` to find documents needing review.

## Merge Policy

Branches containing `ddx try` or `ddx work` commits
carry a per-attempt execution audit trail:

- `chore: update tracker (execute-bead <TIMESTAMP>)` — attempt heartbeats
- `Merge bead <bead-id> attempt <TIMESTAMP>- into <branch>` — successful lands
- `feat|fix|...: ... [ddx-<id>]` — substantive bead work

Bead records store `closing_commit_sha` pointers into this history. Any
SHA rewrite breaks the trail. **Never squash, rebase, or filter** these
branches. Use only:

- `git merge --ff-only` when the target is a strict ancestor, or
- `git merge --no-ff` when divergence exists

Forbidden on execute-bead branches: `gh pr merge --squash`,
`gh pr merge --rebase`, `git rebase -i` with fixup/squash/drop,
`git filter-branch`, `git filter-repo`, and `git commit --amend` on
any commit already in the trail.
<!-- DDX-AGENTS:END -->
