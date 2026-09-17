# Maintenance test migration (v0.31.28)

This audit records SQLite retirement, duplicate removal, and restoration of positive native Turso API conformance assertions for this release. It records test ownership rather than execution results; the release evidence must establish which compiled harnesses passed. The assertion baseline protects the resulting test set, and historical test names remain below.

## Ported or renamed assertions

| Source | Old assertion | Current assertion |
| --- | --- | --- |
| `concrete_fireweed.rs` | `sqlite_uses_the_same_concrete_handle_and_operation_families` | `filesystem_turso_uses_the_same_concrete_handle_and_operation_families` |
| `encapsulation.rs` | `open_sqlite_builds_a_usable_fireweed` | `filesystem_log_builds_a_usable_fireweed` |
| `encapsulation.rs` | `open_sqlite_owned_path_reopens_after_all_handles_drop` | `filesystem_log_owned_path_reopens_after_all_handles_drop` |
| `encapsulation.rs` | `open_sqlite_retained_handle_owns_path` | `filesystem_log_retained_handle_owns_path` |
| `item_mutation.rs` | `objectlog_sqlite_reopen_replays_without_selector_evaluation` | `objectlog_turso_reopen_replays_without_selector_evaluation` |
| `public_durability_matrix.rs` | `objectlog_sqlite_async_close_reopen` | `objectlog_turso_async_reopen_and_log_only_rebuild` |
| `public_durability_matrix.rs` | `objectlog_sqlite_strict_close_reopen` | `objectlog_turso_strict_reopen_and_log_only_rebuild` |
| `public_interface_conformance.rs` | `filesystem_sqlite_async_public_interface` | `filesystem_turso_async_public_interface` |
| `public_interface_conformance.rs` | `filesystem_sqlite_strict_public_interface` | `filesystem_turso_strict_public_interface` |
| `public_interface_external_conformance.rs` | `s3_sqlite_async_public_interface` | `s3_turso_async_public_interface` |
| `public_interface_external_conformance.rs` | `s3_sqlite_strict_public_interface` | `s3_turso_strict_public_interface` |
| `storage_matrix_t0_t2.rs` | `storage_matrix_t0_t2_all_twenty_cells` | `storage_matrix_t0_t2_all_twelve_cells` |
| `storage_matrix_t0_t2.rs` | `storage_matrix_registers_exactly_20_distinct_cells` | `storage_matrix_registers_exactly_12_distinct_cells` |

## Retired assertions

| Source | Removed assertion | Reason / remaining coverage |
| --- | --- | --- |
| `concrete_fireweed.rs` | `open_sqlite_sqlite_projection_rejects_identical_paths` | Retired SQLite constructor; native Turso operation-family coverage and explicit retired-selector rejection remain. |
| `concrete_fireweed.rs` | `sqlite_sqlite_projection_uses_the_same_concrete_handle` | Retired SQLite constructor; native Turso operation-family coverage and explicit retired-selector rejection remain. |
| `facade.rs` | `request_id_push_replays_over_sqlite_relational_facade` | Duplicate idempotency contract; request_id_idempotency covers filesystem/Turso replay, conflict and reopen. |
| `objectlog_sqlite_composition.rs` | `public_objectlog_sqlite_async_supports_authoritative_log_commit` | Native public-interface and P9 transaction suites require full transitions, retained outcomes and authoritative recovery reads. The durability matrix reopens and rebuilds Turso from the log. |
| `objectlog_sqlite_composition.rs` | `public_objectlog_sqlite_async_verify_drains_deferred_checkpoint` | SQLite deferred checkpoint machinery is retired. Native durability tests close/reopen and delete/rebuild projection files; optional online projection verification remains deferred. |
| `objectlog_sqlite_composition.rs` | `public_objectlog_sqlite_bounded_mutation_replays_from_authoritative_log` | Native public-interface and P8 mutation suites require positive bounded mutation behavior; selector receipt replay and changed-body conflict are exercised after external reopen. |
| `objectlog_sqlite_composition.rs` | `public_objectlog_sqlite_delete_and_rebuild` | Native durability-matrix tests physically remove Turso projection files and rebuild from the filesystem log under Strict and AsyncProjection barriers. |
| `objectlog_sqlite_composition.rs` | `public_objectlog_sqlite_filtered_claim_survives_delete_and_rebuild` | Native public-interface/P8 suites require positive indexed claim selection. Durable-log reopen/rebuild suites protect recovery; optional online projection-control deletion remains deferred. |
| `objectlog_sqlite_composition.rs` | `public_objectlog_sqlite_filtered_metrics_survive_delete_and_rebuild` | Native public-interface/P6/P8 suites require positive indexed metrics and aggregates. Durable-log reopen/rebuild suites protect recovery; optional online projection-control deletion remains deferred. |
| `objectlog_sqlite_composition.rs` | `public_objectlog_sqlite_lifecycle_interleaves_without_replay_gaps` | Native P7 lifecycle, mutation generation, lost-response and durability suites cover lifecycle operations and authoritative replay; the retired SQLite projection-control interleaving seam is not advertised. |
| `objectlog_sqlite_composition.rs` | `public_objectlog_sqlite_lifecycle_seals_already_buffered_writes_before_reset` | Retired SQLite projection reset seam. Native object-log driver drain, lost-response and durability tests cover accepted work through close/reopen; optional online reset is deferred. |
| `objectlog_sqlite_composition.rs` | `public_objectlog_sqlite_multi_claim_continuation_rebuilds_exactly_once` | Native P9 transaction and public durability suites cover multiple consumed claims, continuation IDs, exact retained outcomes and durable recovery. |
| `objectlog_sqlite_composition.rs` | `public_objectlog_sqlite_namespaces_isolate_shared_object_root` | Retired SQLite composition constructor. Current object-log namespace and storage-matrix tests exercise supported namespace isolation. |
| `objectlog_sqlite_composition.rs` | `public_objectlog_sqlite_strict_commit_transition_round_trip` | Native public-interface and P9 suites require positive full transition, continuation, side-record and replay behavior. |
| `objectlog_sqlite_composition.rs` | `public_objectlog_sqlite_strict_writes_fail_closed_while_projection_is_deleted` | Optional online projection-control deletion remains deferred for native Turso. Offline file deletion plus log-only reconstruction is required by native durability tests; it does not claim concurrent online-delete semantics. |
| `objectlog_sqlite_composition.rs` | `public_objectlog_sqlite_verification_is_exact_per_queue` | Optional online projection verification remains deferred for native Turso. This retired SQLite-specific test is not represented as native verification coverage. |
| `objectlog_sqlite_composition.rs` | `public_s3_sqlite_delete_and_rebuild` | P6s native S3/Turso durability acceptance physically deletes the projection and reconstructs from the authoritative log; optional online projection-control deletion remains deferred. |
| `public_durability_matrix.rs` | `sqlite_log_close_reopen` | SQLite log is retired. Filesystem, PostgreSQL and S3 log recovery are exercised in the current matrix. |
| `public_durability_matrix.rs` | `sqlite_relational_close_reopen` | SQLite log is retired. Filesystem, PostgreSQL and S3 log recovery are exercised in the current matrix. |
| `public_interface_conformance.rs` | `sqlite_memory_public_interface` | SQLite-log cell is retired; current matrix covers four logs and three projections. |
| `public_interface_conformance.rs` | `sqlite_sqlite_public_interface` | SQLite-log cell is retired; current matrix covers four logs and three projections. |
| `secondary_indexes.rs` | `secondary_indexes_sqlite_log_replay_upsert_insert_and_update_typed_unique_conflict` | Retired SQLite-log duplicate. Native public-interface/P6 suites and Turso query tests require typed unique/multi lookup, range ordering and index conflict behavior; memory/PostgreSQL reference coverage remains. |
| `sqlite_create_or_read.rs` | `open_sqlite_atomic_create_rich_reopen_and_capability` | Retired SQLite constructor. Current concrete-handle and queue-template suites check queue creation and policy identity. |
| `sqlite_create_or_read.rs` | `open_sqlite_relational_atomic_create_rich_reopen_and_discovery` | Retired SQLite constructor. Current concrete-handle and queue-template suites check queue creation and policy identity. |
| `storage_matrix_t0_t2.rs` | `sqlite_log_t3_t4_evidence_and_helm_values_present` | SQLite-log cell/Helm fixture is retired. Current 12-cell matrix and Helm gate enumerate supported configurations. |
| `storage_matrix_t0_t2.rs` | `sqlite_log_three_cells_t0_t2` | SQLite-log cell/Helm fixture is retired. Current 12-cell matrix and Helm gate enumerate supported configurations. |
| `objectlog_sqlite_composition.rs` | `public_objectlog_sqlite_side_records_by_prefix_pages_ordered` | Native public-interface transition coverage writes ordered side-record keys and requires exact prefix pages, cursor resumption and exclusion of unrelated prefixes. |

## Other diagnostic cleanup

- Removed three fault-injection aliases that invoked the same filesystem backend under SQLite names. Current-thread and multithreaded lost-response tests remain.
- Removed the old P3i migration ledger and non-S3 barrier duplicates; current matrix validation, filesystem barrier and restart tests exercise the supported paths.
- Removed `turso_indexed_schedule_rewrite_profile`: it attributed costs to `fireweed_items_group_due_idx` and `fireweed_items_active_scope_idx`, both already dropped by the current schema. Its machine-specific historical timing threshold is replaced by the unchanged canonical campaign and primitive gates. Native batch-update correctness, version and statement-shape tests remain.
- Removed cohort-shaped input from the ordinary-item benchmark; callback/cohort workflows use the actual whole-cohort API in the product workflow suite.
- Class B tests distinguish a volatile memory log from reuse of a configured file or PostgreSQL projection. Reusing projection rows does not grant durable-log guarantees; Class A cells retain authoritative reopen and rebuild tests.
- Every external reopen fixture, including S3/Turso and PostgreSQL/Turso, requires selector dry-run, first-match mutation, exact retained batch/selector replay, changed-body conflicts, unchanged row identity and payload, persisted fields and gate state. Native selector calls no longer branch to an `Unavailable` assertion.

## Added regression coverage

The local public-interface matrix now includes memory log × native Turso. It
exercises whole-cohort claims without an external database. PostgreSQL-log/Turso
conformance exposed a missing projection write: cohort claims stored lease hashes
but omitted the bearer rows required for committed response rendering. Cohort
apply now records those bearers in the same projection transaction. The native
relational cohort test also checks exact rendered member order, tokens and expiry.

The shared public-interface helper requires discovery, typed and binary indexes,
range scans, grouped/bucket metrics, query and explicit-item claims, gates,
rescheduling, selector dry-runs and replay, bounded mutations, full transitions,
continuations, prefix pages and authoritative recovery reads on native Turso.
The explicit-item test submits 101 duplicate IDs against a 100-item batch limit
and requires one claimed item/outcome, checking that the limit counts distinct
IDs. P6, P7, P8 and P9 matrices inherit these positive assertions. Native query
and mutation planner suites add direct SQL/validation regressions.

PostgreSQL-log/Turso embedded delivery now uses the log's durable emission cursor.
The server residual delivery fixture appends through the public Redis interface,
requires a nonempty emission cursor to catch up to log high-water, drains the
server, and checks that a new PostgreSQL connection reads the same cursor.

Optional native projection-control verify/delete/rebuild and snapshot/compaction
infrastructure remain capability-gated. Offline log-only rebuild tests remain
mandatory; they do not claim those online maintenance APIs. General
`side_record_query` is still a universally deferred API. Ordered prefix reads
are a supported separate contract and require positive assertions.

## Retired evidence producers and current entrypoints

Removed `record-current-tp003-td008-evidence.sh` and
`record-postgres-transaction-evidence.sh`. Both invoked deleted SQLite-era
`*_log_t3_tp003_ac_txn_exact_pairs` functions; Cargo's exact filter could execute
zero tests, after which their required output file was absent. Their remaining
filesystem/S3 and TD-008 commands duplicate current workspace suites. The
`exact_pair_local_gate_requires_fresh_nonempty_evidence` test only searched that
script for command strings and could not establish that its producer existed,
so it was removed too. Historical JSONL evidence remains unchanged.

Actual transaction coverage remains in `p9n_non_s3_transaction_parity`,
`p9s3_s3_transaction_parity`, `e3_governed_transaction_evidence_matrix`, native
Turso `local_relational`/`differential`, PostgreSQL `composed_log_reconnect`
(including cross-chunk rollback), and the transaction-evidence verifier's
positive and negative semantic fixtures. These tests do not substitute for a
new promoted TP-003 deployment attestation.

The storage-matrix and Snorri S3 entrypoints now select the current Turso feature
and 12-cell matrix. The live kind harness and deployment gate no longer select
deleted SQLite chart files. The authority verifier uses the current 4×3 axes,
retired deferred-flush rejection, and updated composition-profile arithmetic;
profile counts describe configuration coverage, not a claim of live delivery
qualification. The Snorri S3 structural fixture names its current Turso tests.

The P5 structural fixture now lists the three Class B cells and exact current
harness names. The P7N fixture lists nine non-S3 cells and their actual P7N
lifecycle tests, including the PostgreSQL feature and module-qualified filters.
Both are verifier-input examples, not measured pass evidence. The semantic
verifier executes one exact P7N test for each requested lifecycle cell instead
of reporting nine cells after running only four local compositions. Historical
PostgreSQL-axis JSONL fixtures retain their original SQLite labels; current Helm
values and release labels select Turso.

## Upstream ignored tests remain retained

No upstream ignored test was deleted, reclassified as a pass, or weakened during
this maintenance cleanup. The separate serial attempts of all 18 ignored Turso
core/binding tests produced 13 passes, three MVCC failures and two bounded
timeouts. Both timeout cases concern shared B-tree/page-cache code and remain
incomplete stress coverage; ordinary WAL support does not make them irrelevant.
The failed MVCC bootstrap/concurrency cases do not exercise Fireweed's supported
journal mode, which rejects MVCC configuration. Exact failures, limits, regular
suite counts and working evidence locations are recorded in the
[maintenance verification baseline](maintenance-release-baseline.md#vendored-verification-results).
These upstream results neither replace final product conformance nor establish
the pending candidate's performance qualification.
