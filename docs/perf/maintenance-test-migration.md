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

- Removed the unused 1,072-line private `blocking_backend` module and its two uncalled construction/path helpers. The stale helper caused Turso-only builds to reference a feature-disabled module. Its three worker-only tests are retired with the implementation. The two concurrent queue-creation tests now use shared public `open_memory` handles; the two owned-control-plane tests retain their real executor and assertions using the memory backend directly. Public current-thread storage tests remain.
- Restored the server's filesystem/memory composition in builds without PostgreSQL: its match arm had an unrelated PostgreSQL feature guard. No-default, PostgreSQL-only and Turso-only server library builds verify the corrected feature boundary.
- The functional-matrix self-test now removes a cell from the current 12-cell set and checks missing cells, duplicate leaf IDs and duplicate cells separately. Its old `[:19]` negative fixture left the smaller matrix intact.
- Removed three fault-injection aliases that invoked the same filesystem backend under SQLite names. Current-thread and multithreaded lost-response tests remain.
- Removed the old P3i migration ledger and non-S3 barrier duplicates; current matrix validation, filesystem barrier and restart tests exercise the supported paths.
- Removed `turso_indexed_schedule_rewrite_profile`: it attributed costs to `fireweed_items_group_due_idx` and `fireweed_items_active_scope_idx`, both already dropped by the current schema. Its machine-specific historical timing threshold is replaced by the unchanged canonical campaign and primitive gates. Native batch-update correctness, version and statement-shape tests remain.
- Removed cohort-shaped input from the ordinary-item benchmark; callback/cohort workflows use the actual whole-cohort API in the product workflow suite.
- Class B tests distinguish a volatile memory log from reuse of a configured file or PostgreSQL projection. Reusing projection rows does not grant durable-log guarantees; Class A cells retain authoritative reopen and rebuild tests.
- Every external reopen fixture, including S3/Turso and PostgreSQL/Turso, requires selector dry-run, first-match mutation, exact retained batch/selector replay, changed-body conflicts, unchanged row identity and payload, persisted fields and gate state. Native selector calls no longer branch to an `Unavailable` assertion.

## Added regression coverage

The follow-up core-test audit removed four stubs and their unused
`fireweed_core::scaffold` module. None exercised a Fireweed codec, selector or
priority implementation:

| Removed core test file | What it actually checked | Retained behavioral coverage |
| --- | --- | --- |
| `property_scaffolding.rs` | Fixed strings were nonempty after trimming. | Eight generated comparisons of `priority_sort` against timestamp, integer, decimal and text ordering in both directions. |
| `priority_decode_fuzz.rs` | Standard-library `u64::parse`. | Core identifier/timestamp/metadata validation and engine payload/priority round trips. |
| `command_decode_fuzz.rs` | Number of non-whitespace bytes. | Native push/claim/finalize codec round trips, rejection of legacy JSON by the native decoder, and every command variant's JSON round trip. |
| `selector_fuzz.rs` | Number of nonempty comma-separated fragments. | Public selector first-match, lease invalidation, terminal purge and precondition ownership tests, plus query DTO validation. |

The real priority properties retain their 250,000-case default, or two million
generated pairs across eight properties. `PROPTEST_CASES` can explicitly select
another budget. `property-fuzz-smoke.sh` now invokes that exact test target with
10,000 cases per property and serial execution, rather than relying on a name
filter that could select no tests. No cargo-fuzz project is registered, so the
script reports fuzz smoke as not applicable; the deleted stub names were not
evidence of fuzzing.

Scheduled-action acceptance previously assigned `gate_close_reopen = true`.
Its replacement performs public gate operations on memory/memory,
filesystem/memory, and filesystem/Turso Strict and AsyncProjection. It checks
that blocking the priority head leaves unrelated work claimable, the held item
stays unavailable until reopening, and the exact item, payload, gate keys and
terminal metrics are preserved. Profile labels now identify the operations
actually exercised.

The 100,000-item claim/drain calibration retains its workload and now waits
through a public retained read, then compares the physical projection recovery
high-water with the durable log position. Metrics that fold an unapplied tail
cannot substitute for that drain assertion. PostgreSQL projection validation
also checks typed and legacy unique indexes before log append, including
replacement self-exclusion and compact fields; its regression verifies rejected
operations leave log high-water unchanged. The PostgreSQL follow-up completed
with 111 library and 62 conformance passes. The corrected 100,000-item drain
calibration passed in the full release follow-up; separate traced 10,000- and
100,000-item reruns also passed with equal physical and durable high-water
positions. The full release follow-up has 2,032 passes and three external native
Turso `peek` failures for blocked gates after reopen. Those assertions remain
required. After the correction, the focused native debug regression passes and
the external release conformance rerun passes all 15 tests with zero failures or
ignored tests. The initial full-run failure counts remain recorded separately.

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
and mutation planner suites add direct SQL/validation regressions. All 15 native
query tests pass in the ordinary debug profile, including the new multi-string
compound-key regression. The subsequent complete native debug run finished with
139 passes across 15 harnesses, zero failures and three explicitly ignored SQL
timing diagnostics. Separate plan regressions require exact item gate
membership and active client-key membership seeks, preventing the tenant-prefix
and queue-prefix scans observed during triage. Variable-length decoding uses
shallow, ordinary single-reference CTE stages after GDB exposed excessive
recursive translator stack use. The fix adds neither `MATERIALIZED` hints nor
a larger test-thread stack.
The [maintenance verification baseline](maintenance-release-baseline.md#candidate-verification-status)
records working evidence locations and the completed synthetic drain comparison.
The 10,000-item diagnostic improved from 42.383 to 9.700 seconds, with selection
median/p95 falling from 325.022/358.512 to 4.200/4.425 ms per 100-item sample.
That traced tmpfs comparison includes stronger drain assertions and other gate
selection changes; it is not canonical on-disk workflow qualification. The
reported T2 diagnostic remains above its latency budget, and fresh capacity
measurements remain pending.

PostgreSQL-log/Turso embedded delivery now uses the log's durable emission cursor.
The server residual delivery fixture appends through the public Redis interface,
requires a nonempty emission cursor to catch up to log high-water, drains the
server, and checks that a new PostgreSQL connection reads the same cursor.

Optional native projection-control verify/delete/rebuild and snapshot/compaction
infrastructure remain capability-gated. Offline log-only rebuild tests remain
mandatory; they do not claim those online maintenance APIs. General
`side_record_query` is still a universally deferred API. Ordered prefix reads
are a supported separate contract and require positive assertions.

## Post-checkpoint test and ownership fixes

The [durable post-b2 verification manifest](evidence/maintenance-verification-post-b2-v0.31.28/manifest.json)
retains the completed attempts below, including original failures, reruns,
cleanup records and source hashes. The current all-feature, all-target workspace
Clippy check passes with warnings denied in 12.09 seconds
([archived log](evidence/maintenance-verification-post-b2-v0.31.28/fireweed-maintenance-clippy-post-b2.log.gz)).

The ordinary independent benchmark run after `b2c43ea3` passed 30 tests with
zero ignored tests; two live E2 cases were selected separately. Both subsequent
Docker/kind attempts failed because non-owner `XLEN` incorrectly returned zero
for an unprovisioned queue. Cleanup succeeded, and the rejection assertions in
both live tests remain unchanged. `OwnershipRuntime::acquire_queue` now checks
the queue catalog before lease acquisition or log fencing; a missing log epoch
is no longer treated as proof that a queue exists. The new black-box
`objectlog_turso_rejects_unprovisioned_queue_before_ownership_acquisition` checks
Strict and AsyncProjection: configured empty queues are readable, unknown reads
and writes return `ERR no such queue`, and configured queues still accept work.
The complete server integration rerun passes 34 tests in 20.88 seconds with
zero failures or ignored tests. The subsequent raw Docker and kind reruns each
pass one test, in 34.37 and 106.78 seconds respectively, with zero failures or
ignored tests and successful cleanup. Both retain all 56/56 wrong-owner checks
at eight owners. These RESP push/claim/finalize runs reach roughly 16,600
aggregate ingest items/s; they do not exercise the complete
enrichment/retention workflow. The
approximately 2,778 items/s per-queue ingest target remains unmet and is
reported separately from portable correctness/progress gates. These single
runs use the fast release profile and dirty checkpoint-based source; they are
not canonical or governed qualification. The PostgreSQL ownership follow-up
passes both shared-membership/monotonic-epoch and one-hop `MOVED` tests, with
zero failures or ignored tests.

`concurrent_full_delivery_batches_remain_disjoint` retains eight concurrent
1,000-item claims, exact batch sizes and global ID-disjointness. It now retries
only the transient backpressure handled by the existing helper within the
original 45-second overall limit. All 13 local contract tests pass. The full
debug workload attempt has 33 passes, one failure and zero ignored tests:
`full_batches_recycle_original_rows_without_orphaned_leases` exceeded its
120-second watchdog. Its three-cycle workload and accounting assertions remain;
the debug-only watchdog is now 600 seconds while release remains 120 seconds.
The exact traced debug rerun passes all three cycles in 417.47 seconds with
zero pending rows or leases and the expected final dispositions, resolving
that case without changing the earlier full-run totals. Remote CI setup also
now installs its missing `ripgrep` prerequisite;
a corrected remote run is not claimed here.

The applied inventory/policy correction retains vendored findings in explicit
external observations, outside product closure and without declaring them
passes. Product findings cannot be moved outside closure by relabeling their
paths. Only the reviewed `turso_core`/`antithesis_sdk` assertion-macro dependency
and `turso_sdk_kit`/`parking_lot` `send_guard` feature dependency may remain as
machete exceptions, with manifest/source invariants checked by the policy.
Three named SQL timing diagnostics require successful retained historical logs
and are explicitly neither current execution nor capacity evidence. The exact
reclamation counter assignment is classified as backpressure accounting, not a
test skip. All 13 candidate-4 prestage checks pass, including route/binding
regeneration, workflow-policy fixtures and storage-authority/release-identity
checks. The candidate-4 compiled inventory then passed in 2,072.307 seconds,
listing both workspaces and 2,041 harness routes, with five exactly executed
documentation tests. All eight poststage checks pass, including the corrected
storage-policy classifications and negative fixtures: zero product debt, with
148 external observations retained without qualification. The earlier
closure failure and upstream limitations remain recorded. Route listing alone
does not establish execution of every runtime test. The new inventory/poststage
records await final archival and are not included in the existing 67-artifact
post-b2 manifest. The two upstream stress reruns and canonical C1/P1/C2/P2
qualification remain pending. Completed attempt locations and pending
canonical qualification are recorded in the
[maintenance verification baseline](maintenance-release-baseline.md#candidate-verification-status).

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
`p9s3_s3_transaction_parity`, `local_objectlog_transaction_recovery`, native
Turso `local_relational`/`differential`, PostgreSQL `composed_log_reconnect`
(including cross-chunk rollback), and the transaction-evidence verifier's
positive and negative semantic fixtures. These tests do not substitute for a
new promoted TP-003 deployment attestation.

The obsolete E3 live producer and `e3_governed_transaction_evidence_matrix`
emitter are retired. The producer inferred object PUT and seal-cause counters
from caller batches, while its remaining memory-only recovery route did not
assert complete-state digest equality. Its synthetic counters cannot establish
current Turso or remote-S3 performance. Twelve offline schema-v1 contract tests
remain in `historical_e3_evidence_contract_tests.rs`; they validate historical
evidence shapes and rejection rules, not a live deployment. Two actual
filesystem-log recovery tests remain in `local_objectlog_transaction_recovery.rs`:
`append_before_apply_fault_retains_log_and_recovers_exact_item` and
`local_push_and_claim_survive_reopen`.

The three old entrypoints, `run-e3-minio-durable.sh`, `tp002-e3-minio.sh`, and
`tp002-e3-s3.sh`, now fail with an explicit retirement message before starting
builds, provider access or evidence generation. Their fabricated producer
fixtures were replaced by `retired-e3-entrypoints-test.sh`, which passed its
fail-closed checks. Historical E3 artifacts and validators remain retained.
Current public S3 correctness tests and canonical on-disk workflow measurements
do not qualify the historical E3 performance contract.

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
