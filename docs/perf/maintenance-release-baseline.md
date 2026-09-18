# Maintenance release: baseline and verification

## Baseline identity

The reference implementation is source
`49f1b6b3b5f9c8ad9cdb4a8da8e5306fab135707`, with workload binary SHA-256
`0bc944811975d74f8199a4fbbda6c8057d33879ab3c1118d4db842749d5348c7`.
The repeated qualification and compressed raw evidence are documented in
[disk-baseline-and-napkin-math.md](disk-baseline-and-napkin-math.md).
These measurements precede the maintenance dependency refresh. They must not be
reported as measurements of the new release until its candidate is remeasured.

| Measurement | First repetition | Second repetition |
| --- | ---: | ---: |
| Complete recipients/second | 15,816.54 | 13,989.98 |
| Slowest cycle equivalent recipients/second | 13,174.50 | 12,880.61 |
| CPU milliseconds/recipient | 0.81836 | 0.85866 |
| Peak RSS (GiB) | 20.377 | 19.166 |
| Largest sampled store WAL (MiB) | 457.32 | 460.91 |

Both repetitions processed eight million recipients, with one million resident,
64 stores and 128 campaigns. They used the public interface: ingest original
rows, enrich metadata, choose persisted top times, deliver in bounded chunks,
retry, report progress and final outcomes, and purge by retention. Each passed
all 2,278 checks at 12,500 complete recipients/second. Inserts and updates also
passed separate one-million-row primitive tests. Those primitive rates must not
be substituted for full workflow throughput.

## Hardware and capacity arithmetic

Forseti has a Ryzen 7 4800H (eight physical cores, sixteen logical threads),
about 62 GiB usable RAM, and a 512 GB Kingston OM8PCP3512F-AB NVMe behind
LUKS and Btrfs with zstd:3. The investigation found that discard propagation and
scheduled TRIM had been disabled. After enabling discard propagation and
reclaiming 325.6 GiB, short direct-write throughput rose from approximately
67 MiB/s to 900 MiB/s. Buffered throughput rose from 44 to 738 MiB/s.

The longer 180-second, 32.125-GiB direct-write test averaged 182.63 MiB/s.
Its first 19 GiB averaged 884.8 MiB/s; the next 12 GiB averaged 81.36 MiB/s.
This demonstrates duration-dependent behavior. Neither the initial burst nor the
final interval establishes an indefinite device ceiling. Python consumed about
three CPU seconds during that 180-second test, excluding it as the explanation
for the observed sustained slowdown.

For the workflow target, use measured work per recipient:

- CPU demand: `12,500 × (0.81836–0.85866) / 1,000` = **10.23–10.73 CPU
  seconds per second**. Logical thread occupancy is not the same as sixteen
  independent physical cores. CPU and scheduling costs must be measured rather
  than predicting throughput from logical core count alone.
- Sampled host writes: `29.530–29.637 GiB / 8,000,000` = approximately
  **3,964–3,978 bytes per recipient**, or **47.25–47.42 MiB/s** at the target.
  These host counters are not process-attributed and omit startup/tail intervals.
- Logical projection WAL and immutable log traffic are separate accounting
  layers. Do not add them to physical host writes or divide sequential `dd`
  bandwidth by logical payload size to claim a workflow ceiling.

A full-length disk/RAM placement comparison roughly halved host writes without
improving throughput. The retained idle-worker fix instead reduced scheduler
activity and improved measured CPU cost. This supports focusing optimization on
code; it does not prove device latency can never become a bottleneck.

The 10,000 complete-recipient target and its 25% stretch target are tested
workload floors. They are not physical upper bounds derived from `dd`. The worst
qualified cycle exceeded the stretch floor by only 3.04%, so the dependency
refresh requires repeated measurement, not an assumption of unchanged capacity.

## Maintenance verification scope

Verification includes workspace tests with every supported feature, meaningful
ignored integration tests, vendored engine/log tests, the independent benchmark
workspace, and repository script/release/site checks. Removed SQLite log cells
are obsolete. Useful SQLite projection contracts move to native Turso; an
existing equivalent test may replace a duplicate. Historical measurement
artifacts retain their original backend and source labels.

Live fixtures are isolated, user-owned services. PostgreSQL 18.6 is extracted
from the Arch package and runs on loopback. MinIO uses the repository-pinned
`RELEASE.2024-12-18T13-15-44Z`; its local endpoint passes conditional-create,
two-writer race, and stale conditional-update preflight checks. This avoids
requiring privileged Docker access. Test fixtures are not disk performance
baselines. Runtime tests and performance measurements run serially; qualification never overlaps builds or test services.

## Cleanup and API parity

The active storage matrix has four log choices (memory, PostgreSQL, filesystem,
S3) and three projections (memory, Turso, PostgreSQL): 12 strict configurations,
six object-log async configurations, and six invalid non-object-log async
configurations. SQLite selection remains an explicit migration error. Retired
implementations, disabled code, unused dependencies and duplicate tests are
removed. Historical evidence retains its original backend/source labels.

The user requested implementation of missing native Turso API parity. The
candidate now includes native discovery, exact and typed index lookups, range
and aggregate queries, query claims, gates, scalar rescheduling, selector
mutations, full transitions, durable recovery reads and prefix pagination.
The full public conformance suite requires successful behavior for those APIs;
it no longer accepts Turso-specific `Unavailable` responses. The optional
projection-maintenance handle and universally deferred `side_record_query`
capability remain separate from this surface.

Declared query predicates and ordering execute against native SQL indexes.
Leading component constraints narrow the existing compound-key index. Queries
on later components can scan the addressed declared index; this implementation
does not claim constant-time arbitrary filtering. Sparse aggregate/metrics
queries include records awaiting an enrichment field. Rich selector mutations
use one complete, consistent queue image to validate cross-row constraints.
Ordinary unindexed addressed enrichment retains bounded primary-key reads and
generation batching. A bounded mutation validates all accepted updates against
scratch state before one log append.

Fixed-width indexed ordering uses covering seeks with numeric item-ID ties;
its cursor regression checks the actual query plan for unwanted temporary
sorting. Retained-row pagination uses numeric ID order and a partial index,
whose additional write cost will be included in the candidate remeasurement.
An `EXPLAIN QUERY PLAN` check also exposed an unintended read cost: adding that
retained-ID index made a metrics client-key membership join choose a queue-prefix
scan instead of the active client-key lookup. The query now pins
`fireweed_items_active_key`, and the plan regression requires a full
`tenant_id + queue_id + client_item_key` seek. Separately, a 10,000-row gated-claim
diagnostic took 42.38 seconds, with selection around 320 ms per 100-item batch
versus roughly 5–8 ms for apply. Its gate-membership subquery used only a tenant
prefix of the compound key. The corrected query plan seeks by
`tenant_id + queue_id + item_id` and then checks the exact blocked gate key.
These are two independently confirmed query-plan defects. They identify
selection work to optimize; they do not establish an SSD throughput limit.
The claim path now skips the correlated blocked-gate filter when no gate is
blocked, while preserving gate membership in returned rows. When a gate is
blocked, filtering precedes claim limits and uses the full-key seek. The traced
synthetic drain rerun measured the combined selection changes:

| Diagnostic | Before, 10,000 rows | After, 10,000 rows | After, 100,000 rows |
| --- | ---: | ---: | ---: |
| Complete diagnostic wall time | 42.383 s | 9.700 s | 65.931 s |
| 100-item selection samples | 100 | 100 | 1,000 |
| Selection median / p95 | 325.022 / 358.512 ms | 4.200 / 4.425 ms | 4.363 / 4.628 ms |
| Combined claim/finalize apply median / p95 | 7.742 / 8.753 ms | 7.422 / 8.826 ms | 6.966 / 10.386 ms |

The 10,000-row diagnostic completed 4.37 times faster; its median selection
segment improved 77.39 times while apply costs remained similar. Selection
samples cover the `claim_realize` trace segment, excluding earlier queue/gate
probes and later gate-metadata hydration; they are not whole public-claim
latencies. Apply samples are the combined claim/finalize projection batches,
with 100, 100 and 1,000 samples respectively. The table uses nearest-rank p95.

These are serial, traced, tmpfs diagnostics using a release test executable,
not the canonical on-disk workflow build. The reruns also add a stronger physical
projection-drain assertion and additional SQL tracing, so the comparison is
not a controlled measurement of one SQL edit in isolation. Both reruns pass the
physical high-water equality check. The harness configures a 50-ms maximum log
flush latency, so these serial whole-cycle results include its batching policy.
Their mean complete claim/finalize/drain cycles remain 43.926 ms and 45.220 ms per
100 items, above the diagnostic's 25-ms budget; its T2 diagnostic remains false. This diagnostic improvement does
not qualify the historical S3m/S5 contract or the current workflow capacity bar.
Fresh canonical capacity qualification remains pending.

Debug verification also exposed a stack overflow while Turso compiled nested
SQL expressions for variable-length compound-index fields. GDB and disassembly
showed a 119,064-byte explicit stack reservation in the unoptimized
`translate_expr` function, excluding saved registers and the return address;
recursive expression translation exhausted the test thread's stack. The SQL
decoder now separates nibble decoding, length arithmetic and field extraction
into ordinary, single-reference CTE stages. This bounds expression depth
without adding `MATERIALIZED` hints or increasing the test-thread stack.
All 15 native query tests pass in the ordinary debug profile, including a
regression with multiple string fields.
Scalar and batch updates distinguish valid item ID zero from an unresolved
client-key target. Transition validation reserves claims, client keys and
instance fences only for accepted entries, and replay preserves the original
request expiry.

Additional fixes cover sparse log-tail recovery across pages and epochs,
PostgreSQL claim persistence, compact-entity mutation/removal, retained-row
forwarding, native row versions, serialized batch updates, lease renewal
coverage, and atomic cohort bearer persistence. PostgreSQL-log change emission
uses its durable cursor with a Turso projection. The log remains the sole
durability authority; enrichment and disposition stay on original work rows.

A projection containing an old active cohort claim without its bearer requires
a log-only rebuild before workers resume. Ordinary catch-up skips already
applied commands and cannot repair that prior state. Preserve the authoritative
log and request history. Legacy byte-field indexes were previously accepted
without maintained native keys; affected existing projections likewise require
rebuilding unless an explicit migration repairs those keys.

The [test migration audit](maintenance-test-migration.md) records removed and
ported assertions and the current behavioral coverage replacing them. Current
CI and deployment scripts use the same 12-cell matrix. Obsolete evidence
producers that invoked nonexistent tests are removed; a command string alone
is not evidence that a runtime test ran. Local Kafka fixtures can use a
checksum-verified Apache distribution, with process/port/topic ownership and
cleanup tested.

## Candidate verification status

The maintenance changes were first published as review checkpoint `fc0c5bd7`,
followed by identity hotfix `0f9c9601` and verification checkpoint `b2c43ea3`,
before release qualification. These checkpoints include the query, gate,
feature-boundary and test-audit corrections below. The current all-feature,
all-target workspace Clippy check passes with warnings denied in 12.09 seconds
([archived log](evidence/maintenance-verification-post-b2-v0.31.28/fireweed-maintenance-clippy-post-b2.log.gz)).
The default public CI gate also passes all six stages, including 152 facade
library tests, mutation/recovery tests and source-package verification. The
retired-constructor regressions in `encapsulation`,
`facade` and `active_scope_routing` pass all 18 tests in the current all-feature
run. The independent benchmark tests and corrected live E2 reruns now pass as
recorded below. Candidate-4 inventory and poststage checks also pass. The two
upstream stress reruns and fresh capacity qualification still require completion.
This checkpoint is not the final measured source or a completed release.

The earlier compiled inventory passed, but its subsequent storage-remediation
closure check failed on vendored dependency declarations, upstream test annotations,
explicit SQL diagnostics and a reclamation counter assignment. The original
failure remains in
`/tmp/fireweed-maintenance-checkpoint-poststage-followup-results.json` and its
named log. The classification correction is now applied and verified by the
candidate-4 compiled inventory and poststage policy checks, including their
negative fixtures: zero product debt, with 148 external observations retained,
explicitly outside product qualification. Only the two reviewed
macro/feature exceptions remain allowed: `antithesis_sdk` in `turso_core` and
`parking_lot` with `send_guard` in `turso_sdk_kit`; the policy checks their actual
manifest/source requirements. The three named SQL diagnostics require retained
successful historical logs and explicitly claim neither current execution nor
capacity qualification. The reclamation counter records backpressure, not a
skipped test. Product debt remains subject to closure, with normalized vendor
scope checks preventing relabeling of first-party findings.
The source-I/O inventory's stale aggregate identity lookup is corrected; its
write, check and self-test pass. These classification changes do not turn
unexecuted or failing upstream tests into passes.

After `b2c43ea3`, the ordinary independent benchmark run passed all 30 tests with
zero ignored tests. Its two live E2 cases were deliberately deferred to separate,
serial Docker and kind invocations. Both live attempts then failed the same
non-owner assertion: `XLEN` returned zero for a queue absent from that node's
catalog. Fixture cleanup succeeded after both attempts. This was a server
ownership defect, not a stale test expectation: the ownership control plane can
resolve arbitrary keys, while a missing log partition has epoch zero. Ownership
acquisition now verifies the queue definition before creating a lease or
advancing its durable fence. The existing live rejection assertions are
unchanged. The complete server integration rerun passes 34 tests, zero failed
or ignored, in 20.88 seconds, including the new black-box Strict/AsyncProjection
unknown-queue read/write regression. The subsequent raw Docker rerun passes its
one test in 34.37 seconds, and the kind rerun passes its one test in 106.78 seconds;
both have zero failures or ignored tests and successful fixture cleanup. These
are test-harness durations, not the shorter measured ingest/drain intervals.
At eight owners, each run rejects all 56 of 56 cross-node queue probes.

| Eight-owner observation | Raw Docker | kind |
| --- | ---: | ---: |
| Aggregate ingest, items/s | 16,577 | 16,563 |
| Slowest ingest queue, items/s | 2,072 | 2,075 |
| Aggregate claim/finalize, items/s | 81,938 | 72,850 |

Each sweep uses 2/4/8 independent owner containers on this one machine, one
queue per owner, 12,000 items per queue and eight connections per queue. It
measures RESP push and claim/finalize with tmpfs storage; it does not exercise
the complete enrichment/retention workflow. The per-queue ingest target of
approximately 2,778 items/s was not met: the slowest queues across the sweeps
achieved 1,754 items/s and 1,777 items/s. That capacity target is reported
separately from the portable correctness/progress gate. These single runs use
the fast release profile and a dirty `b2c43ea3`-based checkout, so they do not
qualify canonical workflow throughput or a governed deployment.

The PostgreSQL ownership follow-up also passes both tests, zero failed or
ignored, in 0.15 seconds: shared owner membership/monotonic epochs and one-hop
`MOVED` endpoint discovery.

Remote CI also exposed a missing `rg` prerequisite; workflow setup now installs
`ripgrep` explicitly. Its corrected remote run remains unverified here. The
concurrent full-delivery contract now uses its existing transient-backpressure
retry helper within the original 45-second overall deadline, while retaining
full-batch and disjoint-ID assertions. All 13 local contract tests pass. The full
debug workload attempt completed with 33 passes, one failure and zero ignored
tests: the three-cycle original-row recycling test exceeded its 120-second
watchdog. A separate 600-second debug watchdog is now configured; the release
watchdog remains 120 seconds and the workload/accounting assertions are
unchanged. The exact traced debug rerun passes in 417.47 seconds, completing
all three cycles with 12,096 deliveries, 404 planned failures, zero pending rows
and zero leases per cycle. The first two complete cycle times are 139.61 and
139.74 seconds, including purge: the trace shows steady work beyond the old
watchdog. It resolves that failed case without rewriting the full-run totals.
These test deadlines are completion safeguards, not throughput targets; traced
debug execution is not release capacity evidence.

The [durable post-b2 verification manifest](evidence/maintenance-verification-post-b2-v0.31.28/manifest.json)
records 67 artifacts with source paths and hashes: the ordinary benchmark run,
initial live failures and successful reruns, cleanup ledgers, workload failure
and complete debug retry trace, server/PostgreSQL results, remote CI failures,
Clippy and candidate-4 prestage checks. Original failures remain intact. The
runtime ledger records a dirty checkout based on `b2c43ea3`; these results are
not clean-source release evidence. The 67-artifact archive ends at prestage;
it does not contain the later candidate-4 compiled inventory or poststage logs.
Those completed records await archival in the final evidence commit. Fresh
canonical C1/P1/C2/P2 qualification remains pending.

All 13 candidate-4 prestage checks pass, including root/benchmark formatting,
route and binding regeneration, workflow-policy fixtures, storage authority
and release identity. Their [completed prestage ledger](evidence/maintenance-verification-post-b2-v0.31.28/fireweed-maintenance-final-static-candidate-4-prestage-results.json.gz)
is retained in the same archive.
The candidate-4 compiled inventory then passed in 2,072.307 seconds: both
workspaces compile and list successfully, with 2,041 harness routes and five
exactly executed documentation tests. This is route/listing evidence, not a
claim that all 2,041 runtime tests passed in that inventory command. All eight
poststage checks also pass, including workflow-inline classification, the
evidence-I/O baseline write/check/self-test, storage policy and its negative
fixtures, and the final whitespace check. The original closure failure remains
recorded; the corrected classification does not qualify upstream failures.

The completed records are
`/tmp/fireweed-maintenance-final-static-candidate-4-inventory-results.json`,
its named inventory snapshot/log, and
`/tmp/fireweed-maintenance-final-static-candidate-4-poststage-results.json`
with its named logs. These later records are outside the 67-artifact post-b2
archive and await the final evidence commit. The two upstream stress reruns
and fresh canonical C1/P1/C2/P2 measurements remain pending.

The initial full workspace attempt produced 183 completed harness summaries:
2,019 passes and ten failures, plus a separately interrupted large calibration.
Those are diagnostic attempt totals, not final release passes. The next full
release-profile workspace run completed 180 harness summaries with 2,032 passes,
three failures and zero ignored tests, in 1,504.48 seconds. All three failures
were external native Turso `peek` assertions for a gate remaining blocked after
reopen: PostgreSQL/Turso and S3/Turso Strict and AsyncProjection. After the fix,
the focused native debug gate/peek regression passes, and the external release
conformance rerun passes all 15 tests with zero failures or ignored tests. That
rerun resolves the three recorded failures; it does not rewrite the earlier full
run's totals. The strengthened 100,000-item drain calibration
passed within the 22-test `ss_mixed_overlap` harness, whose total was 82.86 seconds;
that time is for the whole harness, not the individual calibration.

The maintenance cleanup was tested before the additional parity request.
The current parity implementation has the completed all-feature runtime attempt
and corrected external rerun described above. Turso-only library builds and
memory/Turso test targets also compile. A focused memory-only workflow run
passes after restricting object-log tests to their required feature. The
additional retired-flush regression verifies zero and positive values reject at
both public and normalized configuration boundaries; its focused run passes.
Source changes after the full run include the focused gate correction,
private-wrapper removal, feature guards, configuration rejection and the
post-checkpoint ownership/fixture corrections above; their affected checks are
recorded separately. Repeated capacity qualification is pending.
The final evidence commit must replace this status with measured candidate
identity, exact completed test results, limitations and fresh performance data.
Earlier diagnostic failures are not release passes.

The follow-up assertion audit removed four core test stubs that exercised string
counting or standard-library parsing without testing Fireweed behavior. The
eight real priority-order properties remain at 250,000 cases each by default;
the explicit PR smoke runs 10,000 cases per property serially. Scheduled gate
evidence now comes from actual public close/claim/reopen operations on memory,
filesystem/memory, and filesystem/Turso Strict and AsyncProjection profiles.
The 100,000-item drain calibration waits on a retained read and checks physical
projection high-water against the durable log, instead of treating folded
metrics as proof of projection drain. PostgreSQL index validation now rejects
unique conflicts before append for pushes, replacements and field updates,
with a consistent snapshot for indexed queues. The PostgreSQL follow-up now
passes 173 tests: 111 library tests and 62 conformance tests. The native debug
query follow-up passes all 15 tests. The subsequent complete native debug run
finished 15 harnesses with 139 passes, zero failures and three explicitly ignored
SQL timing diagnostics; those diagnostics are separate from workflow capacity
gates. The full release follow-up and separate 10,000/100,000-item diagnostics
also complete the strengthened drain assertions. The native and external peek
follow-ups verify the correction to the three failed cases. Final source
verification and capacity qualification remain separate requirements.

Working evidence is in
`/tmp/fireweed-maintenance-postgres-followup.log`,
`/tmp/fireweed-maintenance-debug-query-staged.log`,
`/tmp/fireweed-maintenance-native-debug-followup.log`,
`/tmp/fireweed-maintenance-native-debug-followup-results.json`,
`/tmp/fireweed-maintenance-clippy-followup-final.log`,
`/tmp/fireweed-maintenance-gated-claim-query-plans.log`,
`/tmp/fireweed-maintenance-debug-gate-plan.log`,
`/tmp/fireweed-maintenance-debug-membership-plan.log`,
`/tmp/fireweed-maintenance-debug-aggregate-balanced-gdb.log`, and
`/tmp/fireweed-maintenance-query-translator-frame.log`.
The completed gate/peek reruns are
`/tmp/fireweed-maintenance-native-peek-final.log` and
`/tmp/fireweed-maintenance-external-peek-final.log`.
The full release attempt is recorded in
`/tmp/fireweed-maintenance-workspace-followup-results.json` and its named log.
The claim comparison comes from
`/tmp/fireweed-maintenance-claim-baseline-10000.json`,
`/tmp/fireweed-maintenance-claim-followup-results.json`, their named raw logs,
and `/tmp/fireweed-maintenance-claim-timing-analysis.json`, which records the
sample definitions and raw-log hashes.
The completed records are archived in the
[checkpoint evidence manifest](evidence/maintenance-verification-checkpoint-v0.31.28/manifest.json),
with compressed and decompressed SHA-256 hashes. It preserves failed attempts
alongside their completed corrections; archival does not turn them into passes
or assign the archive checkout's source identity to earlier executions. Final
capacity qualification remains separate.

The subsequent [prepush evidence manifest](evidence/maintenance-verification-prepush-v0.31.28/manifest.json)
preserves 55 completed artifacts covering final Clippy, feature/configuration
checks, the public CI gate and compiled inventory, including failed intermediate
attempts and their corrections. That earlier candidate-3 inventory records both
workspaces compiling/listing, 2,040 harness routes and five observed documentation
tests; the later candidate-4 count above includes the new ownership regression.
Listing a harness
does not establish that its runtime test passed; the runtime outcomes above
remain the applicable evidence.

The E3 audit also retired the live producer and governed conformance emitter
whose inferred PUT/seal counters and incomplete recovery assertions could not
support their qualification claims. Twelve offline historical schema-v1
validator tests and two real filesystem-log recovery tests remain. All three
legacy E3 launch wrappers now fail closed, and the replacement retirement-script
test passed. Neither current public S3 correctness coverage nor the canonical
disk workflow certifies the historical E3 performance bar; that qualification
remains unavailable. The test migration audit records the retained test names.

Completed independent checks include source packaging/channel checks, site
browser verification (88 screenshots, zero layout issues or broken links), and
the vendored suites detailed below. These checks do not replace final candidate
workspace verification or the canonical serial C1/P1/C2/P2 capacity runs. The
maintenance candidate still needs its own clean source and binary identities,
four passing reports, and fresh host-counter evidence before qualification is
claimed; the reference measurements above remain historical.

Docker access is available through `newgrp docker` after adding `erik` to the
Docker group. The pinned container lifecycle tests passed in the full workspace
follow-up. Both real multi-node benchmark tests now pass after the ownership
correction, with the capacity limitations described above.
PostgreSQL, MinIO and Kafka local services already support the
other integration suites. Source-preview packaging is explicitly unsigned and
does not claim the separate governed deployment qualification.

## Vendored verification results

The completed regular suites have the following results. Counts use the outer
test-harness totals; child-process output is not counted as additional tests.

| Suite | Passed | Upstream ignored in the regular run | Failed |
| --- | ---: | ---: | ---: |
| Turso core, default features | 2,125 | 16 | 0 |
| Turso Rust bindings, including local sync | 75 | 2 | 0 |
| Object-log, including local S3 | 62 | 0 | 0 |

The 75 binding passes comprise 32 library tests, 42 integration tests and one
separate deadlock regression. The optional sync tests use the checksum-verified
Turso 0.7.2 CLI at the exact vendored upstream revision; each test starts and
owns its local sync-server process. The earlier localhost:8081 connection
failures were missing-fixture results and are superseded by this complete run.

All 18 upstream-ignored tests were then attempted individually and serially:
**13 passed, three failed, and two reached their external time limits**. The
runner used `SEED=1729`, a 16-GiB address-space limit per test, and process-group
cleanup on timeout. It did not remove or weaken the tests. Seven B-tree tests,
four MVCC tests, and both ignored binding tests passed. A single successful
attempt does not establish that an upstream flaky test is fixed.

| Upstream test | Observed result | Interpretation |
| --- | --- | --- |
| `mvcc::database::tests::test_bootstrap_repairs_torn_short_log_before_metadata_init` | Failed with `Corrupt`: an MVCC logical-log file existed while the database header still selected WAL. | The upstream ignored annotation says its fixture cannot atomically create the intended MVCC-header/missing-metadata/torn-log bootstrap state. This attempt failed at opening that inconsistent fixture. |
| `mvcc::database::tests::test_concurrent_writes` | Failed after 22.22 seconds when `StepResult::Busy` reached an `unreachable!()` branch. | The optional MVCC stress test assumes concurrent writes never return Busy; that assumption did not hold. |
| `mvcc::tests::test_overlapping_concurrent_inserts_read_your_writes` | Failed after 1.82 seconds with `transaction should exist in txs map`, followed by failed thread joins. | A substantive upstream MVCC concurrency failure remains reproducible. |
| `storage::btree::tests::fuzz_long_btree_insert_fuzz_run_equal_size` | Stopped at 60 seconds while still logging inserts; no assertion failure was recorded. | Incomplete B-tree stress coverage, not a pass or demonstrated correctness failure. The test contains 140,000 insert operations across seven sizes. |
| `storage::page_cache::tests::test_clear_memory_stability` | Stopped at 120 seconds before its final memory-growth assertion. | Incomplete memory-stability coverage. The full test performs 100 million page constructions/inserts using recycled 4-KiB buffers; its concluding assertion was not reached. |

Fireweed's supported native projection explicitly accepts ordinary WAL and
rejects MVCC in `TursoRelational::open_with_io` through its configuration validation.
The journal-mode rejection regression passes in the native suite. The three
failed tests therefore exercise an unsupported journal mode or its bootstrap
fixture. Their source was byte-compared with the upstream `turso_core` 0.7.2
registry source and is unchanged; a missing Cargo feature does not explain these
failures. That distinction does not dismiss the two incomplete tests: B-tree
and page-cache code also serve ordinary WAL. Longer optimized reruns, with debug
assertions and overflow checks retained, are planned but remain pending. The
regular core suite and seven additional B-tree passes
are positive evidence, but the complete upstream suite is **not all green**.
The optional sync and MVCC deadlock reproductions passed their bounded attempts;
they do not expand Fireweed's supported journal or replication modes.

The working-run records are
`/tmp/fireweed-maintenance-upstream-ignored-results.json` and the corresponding
`fireweed-maintenance-upstream-ignored-01.log` through `-18.log`. Regular-suite
records are `fireweed-maintenance-vendor-core-default-final.log`,
`fireweed-maintenance-vendor-turso-sync-final.log`, and
`fireweed-maintenance-vendor-log-retry-final.log` in `/tmp`. These completed
records are preserved in the checkpoint evidence manifest linked above.
