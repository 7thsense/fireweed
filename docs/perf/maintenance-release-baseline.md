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
whose additional write cost is included in the candidate remeasurement.
Gate filtering precedes claim limits, including optimized FIFO selection.
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

The maintenance changes are published as a review checkpoint before release
qualification. The all-feature, all-target workspace Clippy check passes with
warnings denied. The retired-constructor regressions in `encapsulation`,
`facade` and `active_scope_routing` pass all 18 tests in the current all-feature
run. Full runtime verification is still in progress, including corrections to
obsolete unsupported-operation assertions and reopen-fixture timestamps.
This checkpoint is not the final measured source or a completed release.

The maintenance cleanup was tested before the additional parity request.
The parity implementation compiles across all workspace targets and features;
its final runtime verification and repeated capacity qualification are pending.
The final evidence commit must replace this status with measured candidate
identity, exact completed test results, limitations and fresh performance data.
Earlier diagnostic failures are not release passes.

Completed independent checks include source packaging/channel checks, site
browser verification (88 screenshots, zero layout issues or broken links), and
the vendored suites detailed below. These checks do not replace final candidate
workspace verification or the canonical serial C1/P1/C2/P2 capacity runs. The
maintenance candidate still needs its own clean source and binary identities,
four passing reports, and fresh host-counter evidence before qualification is
claimed; the reference measurements above remain historical.

Docker access is available through `newgrp docker` after adding `erik` to the
Docker group. The two real multi-node benchmark tests and pinned container
lifecycle tests still require a current-source run; access alone is not a test pass. PostgreSQL, MinIO and Kafka local services already support the
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
| `storage::page_cache::tests::test_clear_memory_stability` | Stopped at 120 seconds before its final memory-growth assertion. | Incomplete memory-stability coverage. The full test allocates 100 million 4-KiB page buffers over its lifetime; its concluding assertion was not reached. |

Fireweed's supported native projection explicitly accepts ordinary WAL and
rejects MVCC in `TursoRelational::open_with_io` through its configuration validation.
The three failed tests therefore exercise an unsupported journal mode or its
bootstrap fixture. That distinction does not dismiss the two incomplete tests:
B-tree and page-cache code also serve ordinary WAL, so their stress coverage
remains unfinished. The regular core suite and seven additional B-tree passes
are positive evidence, but the complete upstream suite is **not all green**.
The optional sync and MVCC deadlock reproductions passed their bounded attempts;
they do not expand Fireweed's supported journal or replication modes.

The working-run records are
`/tmp/fireweed-maintenance-upstream-ignored-results.json` and the corresponding
`fireweed-maintenance-upstream-ignored-01.log` through `-18.log`. Regular-suite
records are `fireweed-maintenance-vendor-core-default-final.log`,
`fireweed-maintenance-vendor-turso-sync-final.log`, and
`fireweed-maintenance-vendor-log-retry-final.log` in `/tmp`. These records must be
preserved with the final release evidence; temporary log paths alone are not
published evidence.
