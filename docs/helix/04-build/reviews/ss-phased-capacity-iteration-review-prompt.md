# Adversarial review: SS phased capacity iteration plan

You are a critic, not a validator. Your job is to find every way this
could fail, every constraint it leaves undefined, every assumption it
bakes in without stating, every interface it leaves ambiguous. A BLOCKING
finding is anything that would cause implementation rework, a migration
hazard, or a spec gap that agents will interpret differently. Do not
balance criticism with praise — a useful adversarial review is entirely
about what is wrong.

Read the plan at `docs/helix/04-build/ss-phased-capacity-iteration-plan.md`
in the current workspace. Also read, if needed:

- `docs/helix/00-discover/seventh-sense-phased-capacity-benchmark.md`
- `docs/helix/00-discover/first-principles-performance-model.md`
- `docs/helix/01-frame/prd.md` (FR-44..FR-47, scale substantiation)
- `docs/helix/02-design/contracts/API-003-workload-integration-profiles.md`
- `docs/helix/02-design/adr/ADR-001-cqrs-log-projection-storage-model.md`
- `docs/helix/02-design/adr/ADR-013-log-single-source-of-truth.md`
- `docs/helix/02-design/adr/ADR-017-async-commit-strategy-and-dispatch.md`
- `crates/fireweed-projection/src/lib.rs` (`insert_pending`, `transition`, `record_index_keys`)

## Review question

Pressure-test this as an execution plan for agents (`ddx try` / `ddx work`):

1. Will I0–I5 actually move G1–G3, or is the plan optimizing the wrong layer?
2. Are G1–G3 honest for the Seventh Sense worker loop, or still a vanity bar?
3. Which slice will cause implementation rework (snapshot compat, claim echo, unique indexes, claim_by_query)?
4. What is missing for a cold agent to implement I0 or I2 without asking?
5. Does the stop/blocked rule prevent infinite micro-optimization?

## Output contract

Produce findings as:

### Findings

| Severity | Area | Finding |
|---|---|---|
| BLOCKING | <area> | <specific issue with file/section evidence> |
| WARNING  | <area> | <specific issue with file/section evidence> |
| NOTE     | <area> | <observation with evidence> |

Cite the plan section or source line. A finding with no evidence is invalid.

### Verdict: APPROVE | REQUEST_CHANGES | BLOCK

### Summary
2–4 sentences. Then list the exact plan edits you would require before beads are filed.
