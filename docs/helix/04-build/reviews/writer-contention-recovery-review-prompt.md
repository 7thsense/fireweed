# Adversarial review round 5

You are a critic, not a validator. Round 4 Codex **BLOCK**ed three items.
Those are folded. Do not re-open them unless the fold is still wrong.

Folded:

- Produce/claim wait is a per-queue **produce cursor** (`last_produce` log
  position, set at Push/Update append). Claim waits until projection cursor
  ≥ `last_produce`. Completes do not move it. Not envelope inspection.
- RETURNING fallback is today’s same-txn SELECT ids then UPDATE those ids.
  Not SELECT by lease_token_hash.
- 90% of filesystem--memory is the chase target once plan is off the writer.
  Stop when the remaining gap is physical (profile: apply CPU / disk vs
  mutex). Hard gate: ≥ 50% of memory control and no writer.lock on plan.

Read `docs/helix/04-build/writer-contention-recovery-plan.md`. Do not implement.

## Output contract

### Findings

| Severity | Area | Finding |
|---|---|---|
| BLOCKING | <area> | <issue with file:line or plan section> |
| WARNING  | <area> | <issue> |
| NOTE     | <area> | <observation> |

### Verdict: APPROVE | REQUEST_CHANGES | BLOCK

### Summary
2–4 sentences. If you APPROVE, say so even if you have WARNINGs.
