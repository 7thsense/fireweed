# Maintenance release: baseline and verification

## Source-preview identity and current capacity

The v0.31.28 source-preview artifact source (S3) is
`522ea1f1f9fadf8efbdcd493d2c6f9c7d351981a`. Runtime measurements belong to its
parent (S2), `654e4a175aaaf420d88fbdec5794687b72442ea0`, and normal release
workload binary `8071feb98f8ce9c951be891b77d87deaff25f32a06a2f6eac7fdc99287b6c573`.
S3 changes only three `.gitleaksignore` lines: one exact historical fingerprint
for a deterministic temporary-database encryption test key and two rationale
comments. Every other tracked path is byte-identical. The runtime measurements
were not rerun or relabelled for S3. This source preview makes no governed
readiness, signing or Snorri production-migration claim.

All eight required local public-release gates passed on clean S3, including
functional checks, dependency/license policy, the full-history secret scan,
source-package verification and release-channel checks. Its exact source archive,
SBOM, unsigned provenance and checksum files also passed separate packaging
verification. [S2 CI](https://github.com/7thsense/fireweed/actions/runs/35303227120),
[S2 Turso](https://github.com/7thsense/fireweed/actions/runs/35303227154) and
[S3 CI](https://github.com/7thsense/fireweed/actions/runs/35304454728) passed.
S3 did not trigger the path-filtered Turso workflow. The initial missing-tool
failure and vendor test-key finding remain preserved alongside their corrections.

The [final verification manifest](evidence/maintenance-verification-final-v0.31.28/manifest.json)
contains 348 completed artifacts, including raw failed and successful reports,
executed diagnostic scripts, device counters, observer/process cleanup, exact
source identities and remote run evidence. Compressed and decompressed hashes
are recorded; archiving does not transfer source identities or make failures pass.
Earlier checkpoint/prepush/post-b2 archives retain the preceding test attempts.

**Current sustained campaign qualification failed.** The serial
C1/P1/C2/P2 attempt passed both primitive suites but failed both full campaigns
with authoritative-log post-position produce timeouts after six complete cycles.
Neither failed campaign has a complete result; dividing by eight million would
invent a throughput measurement. The historical `49f1b6b` qualification below
remains historical. The 10k complete-recipient floor and 12.5k stretch goal
remain unqualified on the maintenance source. The release channel and capacity
qualification are separate.

### Four-cycle checkpoint diagnostic

A separate four-cycle diagnostic used the same clean S2 and binary, unchanged
one-million-resident-row/64-store/128-campaign shape, and three explicit tracing
flags. All four million recipient lifecycles completed: each cycle independently
verified and purged one million rows, with 967,741 deliveries, 32,259 planned
terminal failures, and zero pending or leased rows. Its diagnostic rate was
**8,638.52 recipients/sec**, CPU cost **0.911280 ms/recipient**, and peak RSS
**17.104 GiB**. These are traced four-cycle observations, not eight-cycle
qualification or a substitute for the failed full campaigns.

| Cycle | Campaign wall median / maximum | Delivery median |
| --- | ---: | ---: |
| 1 | 62.498 / 64.971 s | 23.219 s |
| 2 | 93.474 / 95.727 s | 29.737 s |
| 3 | 102.340 / 104.069 s | 32.582 s |
| 4 | 188.449 / 193.298 s | 106.047 s |

The trace recorded **78 checkpoint attempts: 77 successes and one error**. The
error's cause was not recorded; the same store subsequently succeeded, and all
64 stores reported successful checkpointing. Successful attempts had median
**3.644321 s** and maximum **10.010102 s**. Their wave coincided with the fourth
cycle: the bracketing device window measured 46.665 MiB/s of host writes,
98.50% busy, 116.565 ms mean completed-write latency, and 2.997 process CPU
seconds per wall second. These whole-device counters cannot attribute traffic
to individual projection or log files or establish an SSD bandwidth ceiling.

The 42,756 completed local publications reached **19.068902 s**; file-data sync
reached **13.775142 s** and directory sync **11.838483 s**. The 22,093 successful
log produce operations reached **26.021177 s**. Operations overlap: their
durations must not be summed into lost wall time. Publication/produce records
lack absolute timestamps and per-store join identifiers, so they cannot be
matched exactly to checkpoint events. This successful-only trace does not identify
the operation that timed out in either earlier campaign.

The workload exited zero; the diagnostic wrapper completed and cleaned up.
The recorder correctly retained **133 failed qualification checks**, including
the trace override, overall/three cycle rate misses, and 64 materialization plus
64 stability checks. With only four cycles, the last-three main-file snapshots
still include the pre-checkpoint 4-KiB files. None of those failures is removed
or converted into a pass. The [trace analysis](evidence/maintenance-verification-final-v0.31.28/fireweed-maintenance-s2-traced-four-cycles-checkpoint-wave-1-analysis.json.gz)
and its raw records, original failed gates and executed analysis scripts are
included in the final verification manifest.

## Completed placement diagnostic: two cycles per arm (2026-09-18 UTC)

Clean source `654e4a175aaaf420d88fbdec5794687b72442ea0`, normal release binary `8071feb98f8ce9c951be891b77d87deaff25f32a06a2f6eac7fdc99287b6c573` completed **disk / RAM / disk**, serially. Each arm completed two million recipients with unchanged workflow assertions and authoritative logs on disk. This is a placement diagnostic, not eight-cycle qualification.

| Metric | Disk 1 | RAM projection | Disk 2 |
| --- | ---: | ---: | ---: |
| Complete recipients/sec | 12,383.20 | 15,693.88 | 11,729.11 |
| Slowest cycle equivalent recipients/sec | 10,611.96 | 15,974.95 | 10,870.18 |
| CPU-ms/recipient | 0.85441 | 0.84816 | 0.93912 |
| Peak process RSS, GiB | 16.21 | 16.52 | 16.24 |
| Sampled peak projection allocation, GiB | 14.87 | 13.53 | 15.16 |
| Largest sampled per-store WAL, MiB | 254.62 | 226.06 | 254.48 |
| Sampled host writes, GiB | 5.8555 | 2.6293 | 5.4987 |
| Sampled host write rate, MiB/sec | 37.50 | 21.52 | 33.50 |
| Mean host write-request time, ms | 24.43 | 41.84 | 42.60 |
| Host device busy time, % | 80.16 | 73.04 | 86.41 |
| Worst progress-read p95, seconds | 0.577 | 0.408 | 0.330 |
| Host zram swap-in / swap-out, MiB | 1.766 / 14.219 | 0.227 / 0.117 | 0.570 / 0.000 |
| Diagnostic checks passed | 517 | 517 | 517 |

Against the arithmetic mean of the disk controls, RAM changed throughput by **+30.17%**, CPU cost per recipient by **-5.42%**, and sampled host write bytes by **-53.69%**. Host bytes are not exclusive Fireweed attribution, and sampling omits startup/tail.

Both-cycle exact outcomes, progress and due bounds passed. **All 64 main DB files per arm remained 4 KiB**, with maximum sampled WAL below 448 MiB/store: this comparison does not exercise the materialized-main checkpoint regime. The unmodified qualification outputs remain archived: two cycles fail the sustained campaign-count requirement, and RAM additionally fails on-disk projection placement. Both disk controls also fail the 12.5k overall-throughput gate; none of the three arms qualifies. Neither the eight-cycle checkpoint/RSS-stability gates nor the cycle-seven timeout is resolved by completing two cycles. The faster RAM arm supports a current short-run projection placement cost, without identifying the cause of the later canonical log timeout.

Conditional arithmetic at the measured two-cycle costs (CPU-seconds/sec / host MiB/sec):

| Complete recipients/sec | Disk 1 | RAM projection | Disk 2 |
| --- | ---: | ---: | ---: |
| 10,000 | 8.54 / 29.98 | 8.48 / 13.46 | 9.39 / 28.15 |
| 12,500 | 10.68 / 37.48 | 10.60 / 16.83 | 11.74 / 35.19 |

These are proportional resource budgets for the completed short shape, not sustained capacity estimates beyond its checkpoint boundary.

Actual zram-only/no-backing identity remained unchanged. Workload VmSwap stayed zero, and host swap-out remained within 16 MiB per arm. Host swap-in is recorded as background CPU/memory context; values above 16 MiB are annotated, without a zero-swapping or no-other-host-activity claim. The earlier three diagnostic aborts and the failed canonical campaigns remain separate evidence.

Exact reports, device/allocation samples, identities, cleanup records, unmodified gates and executed scripts are in the final verification manifest. The [placement summary](evidence/maintenance-verification-final-v0.31.28/fireweed-maintenance-placement-v4-completed-summary.json.gz) records formulas and source hashes. The earlier mount preflight failure and three guard-triggered aborts remain separately archived; no incomplete arm is assigned a complete-run rate.

## Previously rejected concurrency changes

The present investigation must account for experiments already completed:

| Candidate | Observed result and retained decision |
| --- | --- |
| Shared log flush runtimes | The repeated 8M campaign fell to 12,043.94/sec, with an 8,907.78/sec worst cycle, failing the base floor. Earlier thread/RSS savings did not establish sustained capacity. Pooling was removed; independent startup/draining correctness fixes remain. |
| Shared directory-sync barriers | Two candidate screens avoided only 0.252% and 0.498% of publication barriers. Mean throughput fell 1.24%; total sync calls increased 0.13%. Removed without sustained qualification. |
| Global cap of 16 active projection applies | Two one-cycle comparisons lost 6.72% and 5.00% throughput while CPU cost rose 8.20% and 11.67%. The policy was removed despite passing cancellation/fairness/correctness tests. These screens did not exercise a sustained checkpoint wave; they still reject repeating the same cap without a distinct measured mechanism. |

The raw historical manifests are
`fireweed-shared-log-runtime-sustained-manifest.json`,
`fireweed-directory-sync-screen-manifest.json`, and
`fireweed-apply-admission-screen-manifest.json` under
`docs/helix/04-build/evidence/workflow-capacity/`. The
[campaign qualification history](campaign-qualification-plan.md) explains each
candidate, comparison, and rollback. Reducing each log runtime to one worker and
staggering checkpoint thresholds were also rejected; neither is a new fix.

Current native apply acquires one writer per store before `spawn_blocking` and
retains it through commit/rollback. Ordinary Tokio callers use their current
runtime; the two-worker runtime is only a fallback and does not impose a
process-wide two-apply limit. Relational work uses a separate inner blocking hop.
Thus a global admission change would need to address the actual owned apply
path, preserve cancellation and ownership, and improve on the rejected policy.
The four-cycle trace supplies evidence for further attribution. The completed
short placement comparison confirms a projection-placement cost before
checkpointing, but does not explain the later produce timeout. Next code work
must correlate log publication/produce phases with absolute timestamps and
store identities, then isolate checkpoint/writeback interaction without changing
log durability. Compare any proposed change serially against the same source,
retain failed outcomes, and repeat both full eight-cycle campaigns and primitive
suites before declaring the performance goal complete. A blind retry or a longer
produce timeout would not establish that the underlying stall is fixed.

## Historical qualified baseline identity

The reference implementation is source
`49f1b6b3b5f9c8ad9cdb4a8da8e5306fab135707`, with workload binary SHA-256
`0bc944811975d74f8199a4fbbda6c8057d33879ab3c1118d4db842749d5348c7`.
The repeated qualification and compressed raw evidence are documented in
[disk-baseline-and-napkin-math.md](disk-baseline-and-napkin-math.md).
These measurements precede the maintenance dependency refresh. They must not be
reported as measurements of the maintenance source. Its completed remeasurement
and failed full campaigns are recorded above.

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

A historical full-length disk/RAM placement comparison roughly halved host writes without
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
whose additional write cost is included in the current maintenance measurements.
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
The subsequent canonical maintenance attempts failed as recorded above.

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
recorded below. Candidate-4 inventory and poststage checks also pass. Both
upstream shared-code stress reruns now pass with their assertions retained; fresh
capacity qualification failed on S2 as recorded above. These earlier checkpoint
results retain their execution identities; the source-preview package uses S3.

At clean checkpoint `bb443691fdaad86b62a5461ce28dd6fbcdde2a71`, remote
[CI passed](https://github.com/7thsense/fireweed/actions/runs/35296298646), including
the public release gate and storage-remediation policy. The separate
[Turso workflow](https://github.com/7thsense/fireweed/actions/runs/35296298606)
passed native Clippy/tests and public workflow correctness/log recovery, then
failed its 12-cell route test because `FIREWEED_PG_TEST_URL` was unset. The test
correctly rejected a missing PostgreSQL fixture; later facade/server stages were
skipped. This is not a passing full Turso matrix. The CI fixture correction passes all six focused local checks (fixture self-test,
policy, policy tests, formatting, workflow shape and Clippy). The correction was
published as S2; its complete CI and Turso workflows subsequently passed at the
links above, including the provisioned PostgreSQL/S3 matrix and downstream stages.

The canonical attempt on `bb443691` was intentionally stopped with SIGTERM after
that CI fixture omission was found. Its final observer ledger records runner
exit -15, observer exit 143, one observed campaign phase, successful monitor and
summary helpers, and completed process cleanup. A separate cleanup record
confirms removal of only its benchmark-owned temporary data directory while
preserving measurement/observer files. No full campaign report or four-phase
result exists, so this interruption establishes neither workflow throughput nor
a throughput failure. The partial observations remain diagnostic evidence. All
four serial C1/P1/C2/P2 phases subsequently ran on clean S2 using the separate
`ci-fixtures` attempt paths: both primitive suites passed and both campaigns
failed during cycle seven. Their final records remain separate from this
intentionally interrupted attempt.

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
`ripgrep` explicitly. The `bb443691` remote outcomes are recorded above. The
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
Those completed records are now in the final verification archive. Current
canonical outcomes are recorded above; full campaign qualification failed.

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
archive and are now preserved in the final verification manifest. The two
upstream stress reruns and canonical C1/P1/C2/P2 attempt are complete; their
distinct results are recorded below and above respectively.

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
recorded separately. Repeated capacity qualification failed on S2; the completed
reports, exact source identities and fresh device counters are preserved in the
final verification archive. Earlier diagnostic failures are not release passes.

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
maintenance candidate now has its own clean source and binary identities and
fresh host-counter evidence, but only two of the four canonical reports pass.
Both full campaigns must pass before capacity qualification can be claimed;
the reference measurements above remain historical.

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

The initial attempts of all 18 upstream-ignored tests ran individually and serially:
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
failures. B-tree and page-cache code also serve ordinary WAL, so their initial
timeouts required follow-up. Both full tests subsequently passed in serial
optimized runs on unchanged source `bb443691fdaad86b62a5461ce28dd6fbcdde2a71`.
The build retained debug assertions and overflow checks at optimization level 3,
with 16 codegen units and LTO disabled. Test assertions, iteration counts and
`SEED=1729` were unchanged; each run had a 900-second watchdog and 16-GiB
address-space limit. The build took 161.70 seconds. B-tree completed in 3.626
seconds of runner wall time; page-cache completed in 32.466 seconds (32.43 seconds
in libtest), with final memory growth of 5,259,264 bytes below its unchanged
10,000,000-byte limit. These completed attempts resolve the two incomplete stress
cases without converting the original timeouts into passes. The three unsupported
MVCC failures remain, so the complete upstream suite is **not all green**.
The optional sync and MVCC deadlock reproductions passed their bounded attempts;
they do not expand Fireweed's supported journal or replication modes.

The working-run records are
`/tmp/fireweed-maintenance-upstream-ignored-results.json` and the corresponding
`fireweed-maintenance-upstream-ignored-01.log` through `-18.log`. Regular-suite
records are `fireweed-maintenance-vendor-core-default-final.log`,
`fireweed-maintenance-vendor-turso-sync-final.log`, and
`fireweed-maintenance-vendor-log-retry-final.log` in `/tmp`. These completed
records are preserved in the checkpoint evidence manifest linked above. The
completed optimized follow-up is recorded separately in
`/tmp/fireweed-maintenance-upstream-optimized-results.json`,
`fireweed-maintenance-upstream-optimized-build.log`, and
`fireweed-maintenance-upstream-optimized-01.log` / `-02.log` in `/tmp`; these four
records are preserved in the final verification archive. The test binary SHA-256 is
`1431aea46a714b5efdd61256c24ab26e64c28c8c16f6428928939d8f18587dcf`. These are
correctness/stress results, not canonical workflow capacity qualification.
