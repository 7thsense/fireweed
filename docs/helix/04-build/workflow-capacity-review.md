# Fireweed original-row workflow capacity

Date: 2026-09-10. Exact measurement snapshots are recorded in the linked artifacts.

**Primitive targets pass repeatedly; sustained workflow stability remains pending.**
Two clean-build million-row runs measured 14.3–14.8k inserts/sec, 23.3–23.5k
updates by key/sec, and 13.3–13.9k scheduling updates by ID/sec. The supported
64,000-frame checkpoint policy passed one complete 1.6-million workflow run at
8,108/sec. Its repeat averaged 7,196/sec but failed one cycle at 4,044/sec.
Correctness, DB/RSS stability, and the observed WAL budget passed both workflow
runs. The goal stays active until sustained throughput passes repeatedly.

## What is being measured

The primary workload uses the public `Fireweed` API with deterministic asynchronous
handlers. Each original row is inserted, claimed for two enrichments, updated in
place after each enrichment, claimed by scheduled priority, and updated with a
retry or terminal outcome and tracking metadata. Retention purges those same
rows. Loading and delivery overlap. There are no auxiliary workflow records in
this workload and no process-local mutex deciding which row a worker owns.

Workers discover rows through normal queue claims and dispatch by stage metadata.
An enrichment uses `mutate_items` to check the claimed version and lease token,
replace the requested data, and return the row to Pending in one logged command.
Different results and values can share a bounded batch. Equal-priority ordering
and future eligibility have separate contract tests. Ordering is per queue;
physical shards have independent stores and do not imply a global merge order.

This models the load-to-scheduled-to-continuous-delivery behavior reviewed in
Seventh Sense's actions-queue and jobs-scheduled-actions implementations. Those
workers also use bounded batches. The separate Snorri integration profile still
exercises its existing instance/transition commit protocol. Its additional writes
are not prerequisites for qualifying these basic operations, and its adapter was
not migrated by this change.

## Why it was slow, and what changed

1. Several supposedly addressed reads could scan a queue. Full-key VALUES joins
   now drive bounded membership and lease-target reads. The new mutation snapshot
   also forces the lease-bearer primary-key seek: the optimizer otherwise selected
   a token index using only the tenant/queue prefix. EXPLAIN regressions check the
   full lookup keys.
2. Releasing a row and then updating it required multiple commands and a local
   owner workaround. Addressed lease/version-guarded mutations now work directly
   on composed Turso. Disjoint requests can share an append; overlaps are processed
   in FIFO order after preceding replacements become visible. Selection fences
   protect the SQL planning snapshot and lease checks.
3. The previous generation budget effectively prevented 1,000-row requests from
   sharing a generation. The internal budget is now 8,192 items / 32 MiB, while
   public mutation batches remain bounded at 1,000 addressed entries.
4. Generation response delivery depended on a bounded result cache. Delayed
   callers could lose access to a completed result after later generations ran.
   A sustained test stranded 3,000 leased rows and was rejected as evidence. Each
   caller now owns its response cell from admission; an owned driver publishes
   results even if its initiating caller cancels. Completed payload batches need
   no global cache, and callers clone only their own result.
5. Invalidating leases and purging rows could leave persisted lease-bearer rows.
   Those rows are now removed together with in-memory token state. Recycling and
   retained-key tests cover the resulting lifecycle.
6. Ungrouped rows were maintaining an unnecessary grouped-priority index. The
   grouped index now contains only rows with a group. Unchanged payload writes
   are also skipped.
7. `synchronous=OFF` did not eliminate Turso checkpoint synchronization. On this
   release, OFF also prevents completed backfills from advancing durable shared
   checkpoint state, allowing repeated work. The composed projection now opts
   into a filesystem I/O adapter that forwards writes, errors and locks but omits
   stable-storage synchronization. NORMAL checkpoint accounting can then advance
   completed backfills. All authoritative-log synchronization remains enabled.
8. New projection files use 4 KiB pages. Existing files keep their page size.
   Log-backed projections reuse checkpointed WAL files through NORMAL checkpoint
   accounting; repeated forced truncation was removed. Standalone OFF mode retains
   its compatibility workaround. Long runs confirm bounded projection and WAL size.

The I/O boundary is deliberate: a machine/power failure may require deleting and
rebuilding the projection from the log. Projection files never supply independent
durability. Ordinary standalone TursoConfig::local does not opt into this adapter.

## Evidence and qualification

Unless stated otherwise, runs below use eight physical shards on the same disk, 1 KiB deterministic
payloads and bounded public-API batches. These are measurements, not final-version
qualification or a guarantee for independent one-row transactions.

| Measurement | Result | Qualification |
|---|---:|---|
| One million resident rows: insert | 63,157 rows/sec | Component target exceeded; earlier snapshot, repeat required |
| Same rows: enrich by key | 38,331 rows/sec | Component target exceeded; earlier snapshot, repeat required |
| Same rows: schedule by ID | 38,460 rows/sec | Component target exceeded; earlier snapshot, repeat required |
| Native WAL reuse, 16 recycling cycles / 1.6m workflows | 3,108 workflows/sec | FAIL: sustained rate |
| Larger batches, eight workers/shard, 16 cycles / 1.6m workflows | 2,991 workflows/sec | FAIL: sustained rate |

Raw runner artifacts retain source revision, dirty state, binary/source hashes,
command, filesystem, resource usage, correctness output and qualification checks:
[primitives](evidence/workflow-capacity/fireweed-primitives-rebuildable-io-1m-8.json.gz),
[WAL reuse soak](evidence/workflow-capacity/fireweed-qualified-workflow-wal-reuse-100k-8-c16-a.json.gz),
[larger batch soak](evidence/workflow-capacity/fireweed-qualified-workflow-batch8-100k-8-c16-a.json.gz).
Both completed soaks pass correctness and the finite RSS/projection-size checks.
Neither passes the per-cycle throughput checks. Earlier short bursts above 5k/sec
and RAM-projection controls are not sustained qualification evidence.

The claim-tail planner (`332f559e`) removes the pre-mutation SQL coverage barrier
only for a complete bounded tail of disjoint authoritative Claim commands. It
retains post-mutation coverage and falls back to coverage for mixed tails. A paused
apply test checks append progress, expiry/token guards and already-applied claims.
This introduces no auxiliary workflow records.

A traced 300k-workflow run still achieved only 2,848 workflows/sec: mean durable
append was 312 ms (median 284 ms, p95 729 ms), longer than the 80 ms projection join
window. Only 41 of 1,714 apply batches contained both claims and mutations. Raising
the bounded background join window to 500 ms (`c90c69a5`) increased combined batches
to 327 of 1,332 and achieved 3,457 workflows/sec in the same three-cycle diagnostic.
Explicit coverage reads bypass the delay; a focused test enforces this behavior.
These are short traced comparisons, not million-workflow qualification passes:
[80 ms trace](evidence/workflow-capacity/fireweed-claim-tail-apply-trace-100k-8-c3.json.gz),
[500 ms trace](evidence/workflow-capacity/fireweed-claim-join500-apply-trace-100k-8-c3.json.gz).

The current candidate also combines eligible Claim/MutateItems projection writes.
Within a contiguous authoritative claim-then-mutation run, a row with exactly one
lease-invalidating replacement can move directly from Pending to the recorded
after-image. It charges one claim attempt and both version increments, retains
Pending/superseded/version guards, persists both command outcomes and advances the
cursor atomically. Unpaired claims use the ordinary path. A differential test
compares combined apply with individual replay, including partial claims, duplicate
replay and rollback of an invalid claim. A traced 300k-workflow run reached 3,816 workflows/sec, but its last cycle fell
to 2,388/sec at the slowest shard. Four workers/shard were worse: 3,084/sec overall.
Neither is sustained qualification:
[eight workers](evidence/workflow-capacity/fireweed-claim-mutation-fusion-trace-100k-8-c3.json.gz),
[four workers](evidence/workflow-capacity/fireweed-claim-mutation-fusion-trace-100k-8-w4-c3.json.gz).

The million-row primitive qualification **passed** on `671555b5`: 31,390 inserts/sec,
12,169 enrichments by key/sec, and 22,203 scheduled updates by ID/sec. All rows were
subsequently delivered and purged correctly; complete run time was 370.6 seconds.
This is a committed-version pass of the original 10k component targets, not a pass
of the three-stage workflow target:
[qualified primitives](evidence/workflow-capacity/fireweed-qualified-primitives-671555b5-1m-8-a.json.gz).

Further EXPLAIN inspection found that the supposed consecutive-row fast path
still scanned the tenant/queue index prefix for endpoint IN lookup, rowid-range
validation, and rowid-range UPDATE. FIFO bookkeeping had the same IN scan. The
current candidate replaces named lookups with full-key joins, explicitly selects
the integer primary key for rowid ranges, and avoids eagerly evaluating a fallback
range query after a successful endpoint lookup. Query-plan tests use the production
SQL for the named reads. These fixes passed their query-plan and correctness checks, but subsequent sustained
workflow cycles still missed the target; that qualification candidate was stopped.
The current candidate batches payload upserts and gate replacements for disjoint
row replacements. It retains individual version guards and the sequential path
for repeated row IDs or purges. A larger regression checks exact payload/gate
clears, repeated-ID ordering and a bound on SQL statement count. This change
reached 4,058 workflows/sec over three cycles; the final cycle still fell short.
Doubling the claim follow-up join window to one second reduced throughput to
3,564/sec, so it was restored to 500 ms. Covered reads now avoid waking background
apply unnecessarily, with a deterministic notification regression test.

A complete million-recipient original-row workflow on `7398887e` passed outcome,
retry and purge checks in 355.9 seconds: **2,811 workflows/sec**, still below target.
It used approximately 2.9 CPU cores on average. Its 5,311 durable appends averaged
359 ms and 1.82 commands per append. These observations point to append batching
and storage contention as investigation targets, not proof of a hardware ceiling.
Sixteen shards with four workers each (the same 64 total workers) achieved only
3,011/sec over three 100k-recipient cycles.

Raw comparisons:
[batched payload/gates](evidence/workflow-capacity/fireweed-batched-mutation-aux-trace-100k-8-c3.json.gz),
[one-second join](evidence/workflow-capacity/fireweed-join1000-batched-aux-trace-100k-8-c3.json.gz),
[million-recipient workflow](evidence/workflow-capacity/fireweed-covered-read-workflow-1m-8-c1.json.gz),
[sixteen shards](evidence/workflow-capacity/fireweed-covered-read-workflow-100k-16-w4-c3.json.gz).

The workload now offers explicit `--load-workers N` bounded concurrent public
`push_batch` calls per shard (default one). Ordinal priorities and all original-row
outcome/recycling checks remain unchanged. This tests producer concurrency without
changing Fireweed's interface or substituting a private load path.

The same test exposed anonymous-push response matching by optional client/request
keys. The candidate matches responses by admitted request identity and driver
order; a public concurrent-push regression checks distinct row IDs and payloads.

The correctness suite includes public lease/version conflicts, cancellation,
exact retries/outcomes, priority/FIFO/eligibility, key reuse, full-sized concurrent
batches, repeated retention and abrupt child-process exit followed by rebuilding
from only the copied log. Library checks cover query plans, response lifetime,
storage-error/lock propagation and existing database page-size compatibility.

Use the Python runner's `--qualify` flag. It rejects RAM-backed storage and external
I/O overrides. Component qualification requires one million resident rows and at
least 10k records/sec in insertion and both addressed-update phases. Workflow
qualification requires at least one million complete workflows, three or more
recycling cycles, faults and purge, 5k workflows/sec overall, and every shard's
fair share in every cycle. Last-three-cycle RSS variation must be at most 10%;
per-shard projection-size variation must be at most 5%. These are finite measured
stability checks, not a claim that an arbitrarily long soak has been performed.

## Napkin math and limits

10k individually addressed row changes/sec is reasonable when they share bounded
transactions/appends. It is not equivalent to 10k independent durable log flushes
per second. At batch size 1,000, 10k records/sec requires only ten batches/sec.
Each entry can still have a different address, version guard and replacement.

At 1 KiB per body, 10k inserts/sec carries about 10 MiB/sec of body data. A complete
three-stage workflow needs an insert, three claims and three mutations, plus
occasional retry work and retention. Therefore 5k complete workflows/sec asks for
roughly 35k state changes/sec before those extras; insert rate alone is not the
right comparison. Index maintenance, serialized log commands and page-based WAL
writes add to the body bytes.

The measured host has a Ryzen 7 4800H (8 physical cores / 16 threads), 62 GiB RAM,
and one encrypted Btrfs NVMe filesystem with zstd compression. Eight physical
projection/log pairs share that same SSD; sharding adds writer concurrency, not
eight times the disk bandwidth. Payloads are deterministic, about 1 KiB and
compressible. The results do not establish a rate for arbitrary large or
incompressible payloads, remote object-store latency or every optional index.

During the million-row delivery phase, an isolated short device sample showed
about 55 MiB/sec writes, ~99% device busy time, and no physical reads. That supports
write pressure as a scaling limit, not a universal hardware ceiling. Process
write-accounting bytes are not physical NAND bytes.

The addressed mutation implementation currently supports ungrouped rows on queues
without secondary/typed indexes, entity schemas or cohort policies. Other mutation
shapes remain explicitly unavailable. The next Snorri migration step is a separate
adapter/integration qualification against the accepted interface and workload;
these basic-operation results should not be advertised as that richer protocol's
throughput. Queue retention removes projection rows; authoritative log history is
expected to grow and is reported separately.

Live S3 qualification has not been rerun: Docker socket access is denied on this host,
and no S3 test endpoint is configured. Local filesystem-log tests do not substitute
for that remote-store qualification.

## Producer concurrency and compact-row controls

On `5535a71d`, four loaders with eight shards/eight delivery workers reached
3,964 workflows/sec, with 2.47 commands per append versus approximately 1.8 in the
million-row single-loader run. Two shards with 32 delivery workers each reached
only 1,672/sec. Both completed exact outcomes and purge. Increasing to 32 delivery
workers on each of eight shards reached 3,035/sec and also failed the sustained target.

The 64-byte body control averaged 5,037 workflows/sec over three cycles, but its
slowest final-cycle shard achieved only 2,838/sec. This is **not** a qualification
pass. Reported filesystem write traffic fell from 9.65 GiB to 3.41 GiB compared with
the equivalent 1 KiB-body run, without eliminating the late slowdown. No default
payload size or throughput gate was weakened.

Raw artifacts:
[four loaders](evidence/workflow-capacity/fireweed-concurrent-load4-100k-8-c3.json.gz),
[two shards](evidence/workflow-capacity/fireweed-concurrent-load4-100k-2-w32-c3.json.gz),
[compact bodies](evidence/workflow-capacity/fireweed-compact64-load4-100k-8-c3.json.gz),
[32 workers per shard](evidence/workflow-capacity/fireweed-load4-100k-8-w32-c3.json.gz).

The runner now reports processing and purge wall time separately within each
cycle, while qualification continues to include both in total elapsed time.
This will distinguish slow delivery from synchronous retention costs in further
measurements. All 24 public workload tests passed after concurrent loading was
added, including original-row recovery from the authoritative log alone.

The four-loader trace also exposes the late slowdown directly: mean log-produce
latency was approximately 17 ms before the first shard completed cycle zero,
362 ms in the next interval, and 540 ms in the following interval. These are
intervals bounded by the first shard finishing a cycle, not perfectly synchronized
per-cycle measurements. Projection apply cost per item increased much less.
Opt-in log tracing now times the existing blob adapter's segment and manifest
PUTs separately, without replacing its I/O or durability barriers.

The adapter-timed run on `f9326762` reached 4,208 workflows/sec. Late segment and
manifest PUTs both took roughly 200–260 ms. The phase breakdown showed retention
cost rising from under one second per shard in cycle zero to 8–10 seconds in
cycle one. Processing itself also slowed, so retention is only part of the gap.
[Adapter and phase trace](evidence/workflow-capacity/fireweed-blob-trace-load4-100k-8-c3.json.gz).

`--purge-batch N` now allows retention to use an independent bounded public-API
batch (1–8,192; defaults to the handler batch). This models periodic bulk cleanup
without changing 1,000-row loading/handler calls or removing purge from throughput
measurement. The full-batch original-row recycling regression exercises 8,000-row
purges and checks exact terminal outcomes and zero remaining rows.

The 8,000-row retention run on `0e4647ea` reached **5,207 workflows/sec** overall,
with later purge phases around 2–3 seconds per shard. It remains a failed sustained
candidate: the slowest final-cycle shard took 28.5 seconds, equivalent to only
3,513 workflows/sec at aggregate fair share.
[Bulk retention trace](evidence/workflow-capacity/fireweed-purge8k-load4-100k-8-c3.json.gz).

An experimental candidate scaled the aggregation deadline by the front
queued generation's item count: 40 microseconds per item, capped at 40 ms, with
full generations starting immediately. This gives large peers more time to join
an append without imposing the cap on small requests. The previous fixed-delay
entry point remains available to its existing callers. FIFO admission and all
item, response-byte and generation-count limits remained unchanged.

The size-scaled window produced 3.03 commands/append versus 2.96 for the matching
bulk-retention control, and reached
only 4,991 workflows/sec in the three-cycle comparison. Its untraced full
qualification completed 1.6 million workflows correctly in 443.3 seconds:
**3,611 workflows/sec, FAIL**. Last-three-cycle RSS variation was 1.22%; per-shard
projection variation was below 0.3%. The previous 10 ms aggregation window was
restored because a reliable throughput benefit was not demonstrated.
[Scaled-window trace](evidence/workflow-capacity/fireweed-scaled40-purge8k-load4-100k-8-c3.json.gz),
[completed qualification](evidence/workflow-capacity/fireweed-qualified-scaled40-purge8k-load4-100k-8-c16-a.json.gz).

A further candidate removes unchanged bodies from addressed-mutation log records.
The existing `Replace` record continues to represent payload replacement or clear;
an appended `ReplaceKeepingPayload` variant preserves the preceding version's
body while recording the remaining resolved values. Version and lease guards
remain unchanged. This avoids logging a 1 KiB body again when delivery only
changes outcome metadata, and avoids the associated payload-table lookup/write.
New readers retain the original record tags and can replay existing logs. Older
readers do not support the new variant; rollback across newly written records
requires a reader that understands it. Qualification of this candidate is pending.

Validation for the unchanged-body candidate: all 24 public workload tests passed,
including log-only recovery; Fireweed library tests passed (148, one ignored),
and the complete Turso suite passed (79, one ignored). The new differential test
covers legacy inline payloads, binary log round-trips, explicit clears, equal-body
replacements and repeated-ID sequential preservation. Engine and local object-log
unit tests passed; the two live S3 checks were explicitly excluded because no
endpoint is configured. The PostgreSQL projection was compile-checked across all
targets; no live PostgreSQL performance claim is made.


The unchanged-body candidate on `2d6996ff` reduced serialized log bytes from
1,362.4 MiB to 1,053.4 MiB across 300,000 workflows (about 23%). Its short run
reached 5,152 workflows/sec overall, but the slowest final-cycle shard took
29.2 seconds. This is a log-volume improvement, not a sustained throughput pass.
Keeping the same 8,000-row in-flight worker capacity with 500-row batches and
16 workers per shard regressed to 3,712 workflows/sec. More commands per append
in that control still meant fewer rows per append.
[Unchanged-body trace](evidence/workflow-capacity/fireweed-keep-payload-purge8k-load4-100k-8-c3.json.gz),
[500-row control](evidence/workflow-capacity/fireweed-keep-payload-b500-w16-load8-purge8k-100k-8-c3.json.gz).

A separate syscall-timing diagnostic forwarded every real `fsync` and `fdatasync`
while measuring file and directory calls. Across 806 appends, segment file sync
averaged 268 ms and segment directory sync 104 ms; manifest file and directory
sync averaged 105 ms and 107 ms. The last 100 segment file calls averaged 710 ms.
These are overlapping per-call durations across shards, not additive wall time.
Instrumentation and machine variability prevent attributing the diagnostic's
3,090 workflows/sec directly to the production candidate. It is explicitly
ineligible for qualification because it uses `LD_PRELOAD`. The result does show
that namespace durability barriers contribute substantial latency alongside file
barriers. Removing either barrier from the existing file-per-object protocol
would weaken durability and is not an acceptable optimization.
[Syscall diagnostic](evidence/workflow-capacity/fireweed-sync-call-diagnostic-100k-8-c3.json.gz),
[interposer source](evidence/workflow-capacity/fireweed-fsync-timing.c),
[build provenance](evidence/workflow-capacity/fireweed-sync-call-diagnostic-provenance.json).


The runner now records directory and projection DB/WAL `lsattr` output, where
available, alongside mount provenance. Mount options alone do not show inherited
per-file Btrfs attributes. A planned disk-backed control marks only a new empty
projection directory `chattr +C`, allowing its new DB/WAL files to inherit NOCOW;
the log root retains its existing attributes and durable publish protocol.
NOCOW also disables file data checksums and compression, so this is a storage
tradeoff for a rebuildable projection, not an equivalent filesystem setting.
[Btrfs documentation](https://btrfs.readthedocs.io/en/stable/Administration.html).
The disk-backed qualification thresholds remain unchanged.


The sustained disk-log/RAM-projection control on the `2d6996ff` binary completed
1.6 million original-row workflows in 195.9 seconds: **8,176 workflows/sec**.
All 16 cycles exceeded the 5k slowest-shard-equivalent threshold; the lowest was
5,411/sec. Exact outcomes, retries and purge checks passed. Last-three-cycle RSS
variation was 0.53%; measured main database sizes varied by at most 0.24% per
shard. Final RAM filesystem projection DB/WAL allocation was 272.2 MiB across
all eight shards. This final allocation is not a peak or per-cycle tmpfs memory
measurement, and RSS does not include all filesystem memory.

The unchanged disk-backed gate rejects this run solely for its RAM projection
filesystem. It is useful isolation evidence: the original-row API and disk log
can sustain the target once concurrent projection writes leave the shared SSD.
It does not establish the same result for disk projections or qualify the final
deployment. No RAM storage default was introduced.
[Full sustained control](evidence/workflow-capacity/fireweed-disk-log-ram-projection-2d6996ff-100k-8-c16.json.gz),
[separate gate evaluation](evidence/workflow-capacity/fireweed-disk-log-ram-projection-2d6996ff-100k-8-c16-gate.json).


The NOCOW disk-projection control was stopped after seven complete aggregate cycles
because it repeatedly missed the target and showed no advantage. Recent shard
cycles took approximately 38–46 seconds; both DB and WAL files were verified to
have inherited `C`. The child received SIGTERM, and the runner preserved its
nonzero exit, full cycle prefix, storage attributes and failed qualification.
There is no completed-run throughput claim for this interrupted candidate, and
no NOCOW production default was introduced.
[Interrupted NOCOW control](evidence/workflow-capacity/fireweed-nocow-projection-2d6996ff-100k-8-c16.json.gz),
[stop reason](evidence/workflow-capacity/fireweed-nocow-projection-2d6996ff-100k-8-c16-stop.json).


An isolated `turso_core 0.7.2` dependency diagnostic increased the native automatic
checkpoint threshold from 1,000 to 8,000 frames. The short disk-log/disk-projection
run completed 300,000 workflows at **9,415/sec**, with slowest-shard cycle times
11.25, 10.71 and 11.08 seconds. Process filesystem write accounting fell from
8.60 GiB in the matching original-body-preservation control to 5.22 GiB. The
larger threshold lets a checkpoint collapse more repeated page versions before
writing the main database. Native file writes, WAL restart, and durable log
synchronization were retained.

The dependency patch is not yet a supported product change. Its exact one-line
diff, build command and dependency tree hash are embedded as diagnostic
provenance. The runner now accepts `--diagnostic-provenance PATH` and the gate
rejects any artifact containing that field, preventing experimental dependency
builds from being mistaken for production qualification. The runner compiles as
Python and all three gate unit tests pass, including the new rejection case.
[8,000-frame short diagnostic](evidence/workflow-capacity/fireweed-checkpoint-8000-100k-8-c3.json.gz).


The 8,000-frame sustained diagnostic completed 1.6 million workflows correctly
in 300.9 seconds: **5,322 workflows/sec overall**. Process write accounting was
30.48 GiB; last-three-cycle RSS variation was 0.68%. The overall average improved
substantially over earlier disk-backed soaks, but 11 of 16 cycles missed the
slowest-shard-equivalent threshold. The slowest cycle was equivalent to 2,868/sec.
It therefore remains a failed sustained candidate, independently of its diagnostic
dependency status. The next isolation run raises the native threshold to 32,000
frames to measure further checkpoint coalescing and its WAL-space cost.
[8,000-frame sustained diagnostic](evidence/workflow-capacity/fireweed-checkpoint-8000-100k-8-c16.json.gz).


The 32,000-frame diagnostic completed 300,000 workflows at **9,535/sec** with
4.07 GiB of process writes. Its sustained run completed **1.6 million workflows
at 7,644/sec**, with 22.90 GiB of process writes. Every cycle passed the throughput
threshold; RSS variation over the final three cycles was 0.47%, and all measured
main database sizes stabilized. Its sole gate rejection is the experimental
source override. This is evidence for the checkpoint fix, not final production
qualification.
[32,000-frame short diagnostic](evidence/workflow-capacity/fireweed-checkpoint-32000-100k-8-c3.json.gz),
[32,000-frame sustained diagnostic](evidence/workflow-capacity/fireweed-checkpoint-32000-100k-8-c16.json.gz).

The supported candidate backports effective ordinary-WAL checkpoint configuration
into the pinned Turso core. The upstream default remains 1,000 frames; Fireweed's
log-backed projection explicitly selects 32,000 and verifies readback, while
standalone projections keep 1,000. Read-only connections select zero. The
checkpoint protocol, native file I/O, and authoritative log are unchanged.

The published core and four small binding/support crates are vendored with their
upstream license, checksums and minimal diffs. Only the core contains behavior
changes; the binding manifests use local paths so the fix remains effective when
Fireweed is consumed from a separate workspace. A root-only Cargo patch would
not provide that guarantee. No Snorri edit or consumer-side override is required.

Three focused tests verify connection-local and cached setting readback, actual
checkpoint suppression/backfill, explicit checkpoints, reopen and pinned-reader
snapshot correctness. The workload report advances to v5, adding per-cycle WAL
sizes; the v5 qualification gate requires stable final-three-cycle WAL size as
well as the existing DB-size and RSS bounds. Historical v4 artifacts retain their
original evaluation contract. Supported-build qualification remains pending.


Validation exposed an existing debug-build timing sensitivity: the 24-reader
probe's fixed 90 ms assertion failed at 107 ms with the candidate and 110 ms in
an unchanged `e3bc93c6` checkout on this machine. Snapshot and setting checks did
not report a correctness failure. The assertion has not been relaxed; the full
Turso suite is being run in release mode to evaluate its tight timing contract.


Supported-candidate validation: the complete Turso release suite passed **82 tests
with one existing ignored test**, including the unchanged 90 ms reader assertion
and the three new checkpoint tests. All **24 public workload tests** passed,
including four log-only recovery cases and full-batch recycling. The independent
public-crate fixture successfully opened a filesystem-log/Turso instance with no
consumer override; its stale retired-SQLite constructor example was replaced by
the supported storage API, and its lockfile was refreshed. The Python gate tests
also pass, covering diagnostic rejection and v5 WAL growth.
[Baseline debug timing failure](evidence/workflow-capacity/fireweed-checkpoint-reader-baseline-test.log.gz),
[candidate debug timing failure](evidence/workflow-capacity/fireweed-checkpoint-reader-isolated-test.log.gz),
[full Turso release suite](evidence/workflow-capacity/fireweed-checkpoint-configurable-turso-release-tests.log.gz),
[public tests](evidence/workflow-capacity/fireweed-checkpoint-configurable-public-tests.log.gz),
[independent consumer startup](evidence/workflow-capacity/fireweed-checkpoint-public-boundary-smoke.log.gz).


The first committed-build v5 qualification on `4625e9f5` completed 1.6 million
workflows correctly in 206.5 seconds: **7,756 workflows/sec overall, FAIL**.
Cycle nine's slowest shard took 21.90 seconds, equivalent to 4,565 workflows/sec,
so the unchanged per-cycle throughput requirement was not met.

The added flat-WAL-size criterion also failed for all eight shards. That criterion
was too restrictive for the native protocol: `WalFile::prepare_wal_start` truncates
orphaned frames to the header after restart to preserve authority classification.
The measured file therefore grows and shrinks normally; final-three-cycle ranges
such as 91.9 → 75.6 → 54.0 MiB show reclamation rather than a leak. The highest
end-of-cycle WAL sample was 179.2 MiB. End-of-cycle samples do not establish the
within-cycle peak, so the next gate must observe that peak against a fixed storage
budget while retaining the DB/RSS checks. The archived v5 result remains failed,
and its throughput miss is independent of this measurement correction.
[First supported-build qualification](evidence/workflow-capacity/fireweed-qualified-checkpoint-4625e9f5-100k-8-c16-a.json.gz).


The next candidate selects 64,000 checkpoint frames. The v6 qualification contract
replaces only v5's flat-WAL-size rule: the runner samples WAL lengths every 100 ms,
retains the maximum for each shard across restarts/removal, and combines those
observations with cycle-end measurements. Every shard must remain within a fixed
**512 MiB WAL budget**, declared before the candidate run. This allows roughly
two 64,000-frame windows at the current 4 KiB page size. It is an acceptance
budget, not an engine-enforced hard cap; sampling may miss very short transients.
Missing shard observations, sampling errors, or less than 80% of the nominal
sampling frequency fail qualification. DB-size/RSS
stability, every-cycle throughput, correctness and durability requirements remain
unchanged. Historical v4/v5 evaluations are preserved. Four Python tests cover
normal restart shrinkage, missing evidence and excessive sampled peaks.

The 64,000-frame candidate passed all 24 public workload tests and the complete
Turso release suite (82 passed, one existing ignored test). Native checkpoint
control and recovery code are unchanged from the backport; this candidate changes
only the selected log-backed checkpoint limit and the measurement contract.
[Public validation](evidence/workflow-capacity/fireweed-checkpoint64k-public-tests.log.gz),
[Turso validation](evidence/workflow-capacity/fireweed-checkpoint64k-turso-tests.log.gz).

### First supported 64,000-frame qualification

The clean `097c85f4` build passed the v6 workflow qualification: 1.6 million
original-row workflows in 197.568 seconds, **8,108.17 workflows/sec**. Every
cycle throughput check, exact outcome check, and final DB/RSS stability check
passed. The external monitor collected 1,968 samples with a largest observed
per-shard WAL of 262.01 MiB against the predeclared 512 MiB budget. Full evidence:
[evidence](evidence/workflow-capacity/fireweed-qualified-checkpoint64k-100k-8-c16-a.json.gz).

The subsequent million-row primitive run failed at the post-load metrics read
with retryable projection-coverage backpressure, before reporting any phase.
[Failed evidence](evidence/workflow-capacity/fireweed-qualified-primitives-checkpoint64k-1m-8-a.json.gz)
is retained unchanged. Primitive metrics reads now use the same deadline-bounded
backpressure retry as writes and workflow metrics; all waiting remains included
in phase wall time. The remaining direct retention metrics read also uses this
policy. This changes no production API or acceptance threshold. Current-build
primitive qualification and repeat workflow qualification are still required.

### Repeat evidence after bounded metrics retries

Clean `8ac819e9` retains the production implementation from `097c85f4`, changing
only workload metrics retry handling. Both million-row primitive runs passed:

| Phase | Run A rows/sec | Run B rows/sec |
|---|---:|---:|
| Insert, including projection coverage | 14,788 | 14,347 |
| Enrichment by key | 23,292 | 23,507 |
| Scheduling by ID | 13,939 | 13,337 |
| Claim and complete | 10,255 | 8,834 |
| Purge | 16,390 | 20,468 |

[Primitive A](evidence/workflow-capacity/fireweed-qualified-primitives-8ac819e9-1m-8-a.json.gz),
[primitive B](evidence/workflow-capacity/fireweed-qualified-primitives-8ac819e9-1m-8-b.json.gz).

The [workflow repeat](evidence/workflow-capacity/fireweed-qualified-workflow-8ac819e9-100k-8-c16-b.json.gz)
averaged 7,195.57/sec but failed cycle six: all shards slowed to 20.2–24.7
seconds, with the slowest equivalent rate 4,043.63/sec. Other checks passed,
including a sampled WAL maximum of 511.94 MiB. WAL sizes across shards track
closely through most cycles, suggesting synchronized checkpoint pressure as a
remaining source of shared-device stalls. This is a hypothesis for a staggered
checkpoint experiment, not yet a demonstrated fix. Failed evidence is retained
without changing the gate.
