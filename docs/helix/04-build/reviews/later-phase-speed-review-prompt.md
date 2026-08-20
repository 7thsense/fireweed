# Adversarial review: later-phase pipeline plan

You are a critic, not a validator. Find every way this could fail, every
constraint it leaves undefined, every assumption it bakes in, every interface
it leaves ambiguous. A BLOCKING finding is anything that would cause
implementation rework, a migration hazard, live≠rebuild, or a spec gap that
two agents would implement differently. Do not balance criticism with praise.

Read `docs/helix/04-build/later-phase-speed.md` in full. Cite plan sections
and, when you claim a code fact, the file:line. Do not implement.

## Governing constraints (already decided; do not reopen)

- Do not collapse sqlite-log vs object-log.
- SQL projection is the item store, including payloads. No planner map. No
  process item HashMap. No reservation table. No `SKIP LOCKED` on Turso.
- Class L produce (Push/Update): log first, ack, apply later (`AsyncProjection`).
- Class S claim: one SQL write txn (select due + lease + outbox), then log PUT.
- `BatchUpdate` cannot update leased items (apply skip, not plan-time wait).
- Default `open()` stays `ResponseBarrier::Strict`. SS uses `AsyncProjection`.
- Projections need no crash durability (rebuild from log).
- Turso must not depend on the sqlite adapter crate.
- Sharing one object-log write across in-flight appends is not a work item.

## API facts reviewers must check against code

- `plan_batch_update` (`crates/fireweed-engine/src/compose.rs`) requires a
  snapshot to resolve `ClientItemKey` → `item_id` and to emit
  `BatchUpdateOutcome::Updated { item_id, client_item_key, item_version }`.
- `UpdateFieldsCommand` (`crates/fireweed-engine/src/command.rs`) has a
  required `item_id`. It is serde-replayed from the object log.
- SS harness (`crates/fireweed/tests/ss_phased_capacity.rs`) updates by
  `ClientItemKey` and asserts every result is `Updated`.
- `planner_update_snapshot` loops until `snapshot.len() == keys.len()` or
  projected ≥ `last_produce` (`crates/fireweed/src/turso_compose.rs`).
- Class S: `dispatch_class_s_claim` always `catch_up_produce`;
  `append_class_s_claim` enqueues apply then `delete_claim_outbox_row`.
- `finalize_validate` on Turso takes `writer` + `BEGIN IMMEDIATE` + rollback
  (`crates/fireweed-turso/src/projection.rs`).
- `apply_update_fields_batch_sql` SELECTs payload when any row is Keep
  (`crates/fireweed-relational/src/apply.rs`).
- `apply_group_summary_rerank` falls back to `refresh_group_summaries` when
  the representative moved.
- Claim apply still `load_grouped_items` + `apply_group_summary_remove`.
- Turso compose forces `apply_start_delay_ms.max(300)`.

## Review question

Is Cut 1 implementable without lying on `BatchUpdateOutcome`, without a
wire-incompatible command change, and without live≠rebuild when a key is
not yet in SQL? Is the Class S seam specified tightly enough that an agent
will not reintroduce per-claim waiting or wait-for-apply-of-this-claim?
Will Cuts 2–3 actually make deliver ≥ ingest, or is a cost left on the
writer that the plan does not name?

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
