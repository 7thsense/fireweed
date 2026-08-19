# Adversarial review aggregate — writer-contention recovery (tightened)

- Axis: harness (claude, codex)
- Prompt: `docs/helix/04-build/reviews/writer-contention-recovery-review-prompt.md`
- Policy: any BLOCKING escalates; WARNING agreed by both treated as BLOCKING
- Round: 2 (first dispatch cancelled; this is the completed pair)

## Verdicts

| Arm | Verdict | BLOCKING count |
|---|---|---:|
| claude | REQUEST_CHANGES | 10 |
| codex | BLOCK | 14 |

Disagreement: same direction (“just use SQL”), different stop floor. Codex BLOCKs; Claude REQUEST_CHANGES. Do not collapse to the weaker verdict.

## BLOCKING both arms agree

1. Removing pre-claim apply of **produce** (Push/Update) plus “skip Leased updates” breaks live == rebuild.
2. “Skip duplicate push at apply” acks item ids that may never exist. Not a lock fix — a contract change.
3. `validate_push` is more than “keys exist”: retention, group size, item_id, request-id, typed uniques.
4. “Wait only for those keys” has no bound, target, or cancel rule.
5. Outbox/PUT durability and “no next Class S until outbox empty” are omitted.
6. Adapter “same rules” is not an interface.

## BLOCKING one arm

| Arm | Area |
|---|---|
| claude | `live_tokens` is serving authority for pending; delete is a migration, not a delete |
| claude | One shared reader, not `connect()` per call |
| claude | Group-summary: skip on item-level apply **and** refresh on grouped read |
| claude | Claim scope: item-level vs group/cohort/reclaim |
| codex | Request-id replay before first push applies |
| codex | Mixed-hop apply order (deferred BatchUpdate after the loop) |
| codex | Skip semantics for missing/terminal/fenced/superseded |
| codex | Push side-effects (gates/indexes) if a row is skipped |
| codex | Claim apply: other-token keeps the first; do not overwrite bearers |

## WARNING both (treat as blocking for the rewrite)

- Push does **not** call `catch_up_projection`. Ingest slowness is `validate_push` IMMEDIATE + writer lock + append, not apply-wait.
- Done-when gates are too vague.

## Folded into the revised plan

All agreed BLOCKING items and the push-diagnosis correction. Reservation table stays out. Still three cuts.
