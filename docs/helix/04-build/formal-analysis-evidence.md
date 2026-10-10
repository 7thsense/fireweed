---
ddx:
  id: build-formal-analysis-evidence
  type: implementation-evidence
  depends_on: [build-formal-analysis, contract-formal-queue-semantics, tp-formal-analysis]
  status: complete
---

# Formal analysis execution evidence

## Result

Nine finite models exhaust **21,517 reachable states**. All **20 deliberately
faulty variants** trip their designated oracle. **17 checker/litmus tests** pass,
including exact evidence comparison across Python hash seeds. A one-state search
cap returns FAIL and never truncated PASS. The package is a bounded executable
specification, not an unbounded theorem or whole-program Rust refinement proof.

Seven independent Rust conformance suites pass **365 tests**, with zero ignored:

| Suite / command arguments to `cargo test --locked` | Passed | Attribution |
|---|---:|---|
| `-p fireweed-engine --lib` | 279 | generic real planner/ownership/dispatcher/admission seams |
| `-p fireweed-turso --features local --test async_reclaim_selection --test item_mutation_parity` | 15 | real Turso leases/cohorts, selector/mixed outcomes and retained receipts |
| `-p fireweed --lib formal_analysis_tests` | 2 | shared log protocol filesystem adapter × real Turso |
| `-p fireweed-objectlog --lib async_projection_apply::tests` | 33 | apply coordinator and generic memory-projection seams |
| `-p fireweed-objectlog --lib log_engine_store::tests` | 22 | local log protocol plus native S3 emission-cursor CAS |
| `-p fireweed-objectlog --lib async_product::tests::async_projection` | 9 | local log × memory projection, debt/poison/ambiguity seams |
| `-p fireweed --test request_id_idempotency` | 5 | actual public S3 × Turso cell, receipts/renewal/reopen/partial outcomes |

The final conformance invocation uses `--offline -- --test-threads=1`. It runs
against the source hashes captured before and after the run; changes during a
run fail the runner. [JSON](evidence/formal-analysis/conformance.json) retains
exact commands, hashes, attribution and unmodified logs. Model and counterexample
traces are in [formal.json](evidence/formal-analysis/formal.json).

## Six-opportunity mapping

| Opportunity / formal oracle | Exact model or fixture | Executed production seam / named witness | Limits |
|---|---|---|---|
| Crash recovery / F-ACK, F-PREFIX, F-EVENTUAL | recovery-ownership, recovery-authority-loss; upload/publication separate; stable fair Apply suffix | `log_engine_store.rs::local_log_reopen_preserves_large_batches_across_process_boundary`; `async_product.rs::restart_replays_selected_projection_without_loss_or_duplicate_apply`; public retained receipt reopen tests | Physical durable-store loss is outside F-ACK assumption. Fail-closed missing/corrupt authority is modeled; this run does not inject physical S3 authority corruption. Process-kill qualification remains independent. |
| Ownership / F-FENCE | recovery-ownership acquire/fence/confirm, stale publication, cold CP reset, same-owner reaffirmation | `ownership.rs::ephemeral_restart_reacquire_advances_storage_and_serves`; real adapter `lost_claim_response_deferred_apply_handoff_replays_original_lease` | Generic control-plane kernel tests are not shared production-CP deployment qualification. |
| Item lifecycle / F-LEASE | current/replaced token, expiry, renewal, old receipt expiry | `async_reclaim_selection.rs::ordinary_expiry_respects_time_fences_and_cohorts`; `cohort_lease_validation_renew_and_retry_preserve_boundaries` | External side effects remain at least once; recurrence/gate variants need their independent product evidence. |
| Claims/batching / F-BATCH | batching-items and batching-cohort serial witness, distinct tokens, mixed rejected addressed entry, priority change, deferred polls | new real adapter `stale_projection_competing_claims_keep_disjoint_tokens_and_items`; `pending_claim_tail_is_visible_to_selectors_without_double_version_increment`; packed coordinator tests; engine claim-batch generation/fence tests | Cohort fixture has two whole units, not every grouped selection/filter variant; real grouping obligations remain independent. |
| Idempotency / F-REPLAY, F-KEY | one scope/two requests key convergence; two scopes/one request namespace isolation; retained old claim generations | `native_query_claim_receipts_survive_renewal_reopen_and_empty_results`; public batch/body-conflict reopen tests; `retained_commit_receipts_and_binary_prefix_pages_are_queue_scoped` | Abstract scopes represent tenant/queue separation. Operation-specific fingerprint formats and cohort-key/member-count convergence are independent source/test obligations, not proven by scope fixture. |
| Progress/resources / F-BUDGET, F-DEADLINE | peak record+frame reservations; one running slot; cancellation/CAS ambiguity/close; independent eligible-age integral; declared two-item service fixture | `byte_admission.rs::tenant_blocked_waiter_does_not_head_of_line_block_cold_tenant`, `permit_conservation_and_peak_accounting`; `claim_batch.rs::selection_fence_admission_counts_only_waiters_and_releases_on_close`; `async_composed.rs::stalled_queue_does_not_block_actual_unrelated_queue_progress` | One job per model tenant does not establish tenant contention. Round-robin fixture is not a numeric production FR-12 proof; TP-002 workload/deadline qualification remains required. |

## Review and changes

[Astra plan review](reviews/formal-analysis/astra-ultra-plan-review.md) requested
compatible changes; [implementation review](reviews/formal-analysis/astra-ultra-implementation-review.md)
identified six oracle/evidence corrections, all folded and verified. Astra Ultra
then returned **APPROVE** within the declared bounded scope and independently
verified the final model/conformance evidence. TD-013's
strict response visibility and CP/storage epoch equality and TD-003's single
counter rule were reconciled with ADR-024 and the actual ownership implementation.
Item-token validity is independent of the owner's publication authority.

No model control is reported as a discovered production defect. Two new real
adapter interaction tests exercise lost response/deferred apply/handoff/replay
and competing claims against an unapplied durable tail. The formal gate is wired
into local pr-gate and GitHub CI, which retains its JSON artifact.

## S3 setup and provenance

The first public-cell attempt failed during fixture setup because the existing
loopback listener rejected the default credentials. That result was not counted
as product failure or passing public coverage. An isolated task-owned RustFS
listener with generated ephemeral credentials passed the repository's native CAS
preflight: sequential create 200/412, two-writer create exactly one winner,
conditional update 200 and stale update 412. The
[CAS evidence](evidence/formal-analysis/s3-native-cas.json) records no secrets.
The task-owned server was stopped in `finally`; no existing listener was stopped.

Reproduce the isolated run with:

```sh
python3 scripts/formal/with_rustfs.py --binary /absolute/path/to/rustfs --offline --output /tmp/fireweed-formal-qualified.json
python3 -m unittest discover -s scripts/formal -p 'test_*.py'
python3 scripts/formal/check.py --output /tmp/fireweed-formal-evidence.json
```

A local RustFS/Turso pass does not assert production-cloud or OS process-kill
qualification. Existing PostgreSQL user edits are preserved and hashed, not
staged with this task. `ddx doctor` and `ddx doc stale` were attempted; both exit
127 because DDx is absent from PATH. Local Python/Rust/shell/workflow checks pass;
DDx graph/health checks could not run. No tracker file was edited directly.
