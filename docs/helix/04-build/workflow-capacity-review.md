# Fireweed original-row workflow capacity

Date: 2026-09-10. Exact measurement snapshots are recorded in the linked artifacts.

> 2026-09-11: The richer million-resident campaign now measures 10,850 recipients/sec
> over three cycles and has not qualified against its 10k/12.5k targets. The results
> below describe the earlier saturation fixture. See the [current campaign plan](../../perf/campaign-qualification-plan.md).

## Historical qualification: within five percent of 10,000 workflows/sec

The stricter follow-up goal **passes on clean source `a73b067f`**. Two fresh
three-million-lifecycle runs achieved **12,125 and 11,810 complete workflows/sec**;
the worst cycle's slowest-shard equivalent rates were **9,839 and 10,036/sec**,
above the 9,500/sec floor. Both million-row primitive repetitions passed the
unchanged 10,000/sec insertion and update gates. All correctness, fairness,
retention, RSS/projection stability, and sampled WAL checks passed.

The final preset uses 32 shards and eight workers per shard, executable-owned
mimalloc, portable thin LTO, and corrected native checkpoint retries after
partial backfill. Maximum sampled shard WAL was below 260 MiB in both runs.
See the [follow-up investigation and raw evidence](../../perf/workflow-9500-iteration.md)
for exact provenance, tests, rejected candidates, and reproduction commands.
The same original row is enriched, delivered, and purged; log durability remains
enabled. Snorri migration is a separate integration task.

## Historical qualification before the stricter follow-up goal

**The earlier targets passed repeated qualification on clean build `b0f89563`.** The final
configuration uses 16 independent filesystem-log/Turso shards, 4 KiB projection
pages, effective checkpoint control, and repaired page-cache accounting. All four
qualification reports use the same binary and pass every acceptance check.

| Public-API measurement | Run A | Run B | Target |
|---|---:|---:|---:|
| Insert, one million resident rows | 72,832 rows/sec | 94,091 rows/sec | 10,000 |
| Enrichment update by key | 74,363 rows/sec | 69,567 rows/sec | 10,000 |
| Scheduling update by ID | 38,956 rows/sec | 40,350 rows/sec | 10,000 |
| Complete original-row workflow, including faults and purge | 7,873/sec | 8,029/sec | 5,000 |

Each workflow run recycled 500,000 recipients six times: **three million
lifecycles per run, six million across the repeat**. Every shard met its fair
share in every cycle. Exact outcomes, zero remaining queue rows after purge,
last-three-cycle DB/RSS stability, and the sampled 512 MiB/shard WAL budget all
passed. Workflow peak RSS was about 6.1 GiB in run A; its final-three-cycle RSS
range was 4.53%. Sampled per-shard WAL peaks were 264.29 and 503.48 MiB. These
are observed finite-run bounds, not an engine-enforced WAL cap.

[Workflow A](evidence/workflow-capacity/fireweed-qualified-workflow-cache-accounting-page4k-b0f89563-500k-16-c6-a.json.gz),
[workflow B](evidence/workflow-capacity/fireweed-qualified-workflow-cache-accounting-page4k-b0f89563-500k-16-c6-b.json.gz),
[primitive A](evidence/workflow-capacity/fireweed-qualified-primitives-cache-accounting-page4k-b0f89563-1m-16-a.json.gz),
[primitive B](evidence/workflow-capacity/fireweed-qualified-primitives-cache-accounting-page4k-b0f89563-1m-16-b.json.gz).

The napkin target was reasonable. The blocking costs were avoidable projection
work, ineffective checkpoint configuration, stale native cache accounting, and
a per-shard working set/configuration that did not sustain the target. The log
remains the sole durability authority. The workload updates each original row
through both enrichments and final delivery outcome; it requires no auxiliary
workflow entities. The existing separate Snorri integration protocol is not the
capacity baseline and its adapter has not been migrated.

The final native fix reconciles the evictable-page estimate after WAL commit
clears dirty flags, instead of repeatedly scanning the entire cache during later
allocations. The fallback count also stops after finding enough pages. All
native eviction/spill safety checks remain. Four KiB pages retain better packing
for this payload than the trial 2 KiB pages. Actual checkpoint readback is verified;
the limit is 64,000 frames at 4 KiB, adjusted by actual page size for existing
files. Sixteen shards keep the larger population's per-shard working set and
checkpoint write coalescing effective. Simply doubling workers within eight
shards was slower. The failed trials remain below and in the raw evidence.

These rates use bounded batches of 1,000 independently addressed rows, 1 KiB
compressible payloads, and deterministic asynchronous handlers. They do not mean
10,000 separate durable single-record RPCs/sec. A lifecycle includes an insert,
three claims, three mutations, purge, and occasional retry work: approximately
8.105 row operations per lifecycle, or about **64,000 logical row operations/sec**
at the qualified workflow rate. Sixteen shards share one SSD; they add concurrency
and smaller per-shard working sets, not sixteen physical devices. Remote S3,
external delivery services, optional indexes, and Snorri migration remain separate
qualification scopes, as detailed below.

Reproduce both workloads twice, without competing tests/builds/capacity runs:

```sh
bash scripts/perf/qualify-workflow-capacity.sh target/workflow-qualification
```

The output directory must not already exist. The script builds once, runs the
same four measured commands serially, preserves each complete JSON report, and
returns failure if any qualification fails. The final implementation passed all
106 combined Fireweed/Turso release tests, plus 43 native cache and 3 native pager
tests; the two suites each retain one pre-existing ignored test. Debug-only timing
sensitivity documented below is not represented as a passing debug suite.

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
and one encrypted Btrfs NVMe filesystem with zstd compression. The final sixteen
physical projection/log pairs share that same SSD; sharding adds writer
concurrency, not sixteen times the disk bandwidth. Payloads are deterministic, about 1 KiB and
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

### Staggered checkpoint candidate

The next production candidate hashes each configured database path into a
48,000–64,000-frame automatic checkpoint threshold. Identical shard workloads
therefore have different checkpoint boundaries on a shared device. The policy
is repeatable for each path, verified by actual connection readback, and retains
the prior maximum threshold. Standalone projections retain the native 1,000
frames; committed read-only connections retain zero. No native checkpoint
locking, restart, backfill, or authoritative-log synchronization changes.

The release Turso suite passed 83 tests with one existing ignored test, including
a bounded/repeatable/database-specific policy test. All 24 public contract,
recovery, and workflow tests passed. Capacity qualification remains pending.

The [staggered checkpoint run](evidence/workflow-capacity/fireweed-qualified-workflow-spread-2c3b4ca2-100k-8-c16-a.json.gz)
finished at 7,800.54 workflows/sec but again failed cycle six (4,085.71/sec).
All other gates passed. Staggering did not resolve the repeated shared stall;
the experiment is reverted to the supported fixed 64,000-frame policy. The
next investigation timestamps real log synchronization calls across the slow
cycle; no durability bypass is permitted.

### Larger resident population exposes sustained write pressure

The fixed-policy [500,000-row, six-cycle run](evidence/workflow-capacity/fireweed-qualified-workflow-a74a6218-500k-8-c6-a.json.gz)
completed three million original-row workflows at **4,955.44/sec** over 605.73
process seconds. The final four cycles failed throughput (slowest-shard
equivalents 4,786, 3,844, 3,515, and 3,536/sec). Correctness, DB/RSS stability,
and WAL-budget checks passed. Process filesystem output was **65.214 GiB**,
about 23.3 KiB per workflow; this is process accounting, not physical NVMe
traffic. A late live sample showed zero physical read bytes and substantial
system I/O pressure. This workload is retained as a required larger-population
regression case for subsequent optimization.

A preceding [eight-cycle synchronization diagnostic](evidence/workflow-capacity/fireweed-fsync-timeline-a74a6218-100k-8-c8.json.gz)
completed at 9,059/sec and did not reproduce the failing cycle. Real log-sync
mean latency rose from roughly 3 ms in early ten-second buckets to 79.1 ms in
the 50–60-second bucket (maximum 161.5 ms); another bucket reached a 239.6 ms
maximum. The wrapper preserves real synchronization and is excluded from
qualification. [Source](evidence/workflow-capacity/fireweed-fsync-timeline.c)
and [provenance](evidence/workflow-capacity/fireweed-fsync-timeline-provenance.json)
are preserved. Direct wrapper writes can interleave with stderr cycle lines;
structured result cycles remain intact.

The next candidate changes only new projection pages from 4 KiB to 2 KiB,
keeping the 64,000-frame checkpoint threshold. Existing projection files retain
their page size. The experiment tests whether smaller WAL frames reduce
random row-update write amplification; it has not yet qualified.

The 2 KiB candidate passed the combined release suites: 82 Turso tests and
24 public contract/recovery/workflow tests, with one existing ignored test.
The page-size compatibility test now checks new 2 KiB files and retention
of existing 4 KiB files. [Validation log](evidence/workflow-capacity/fireweed-page2k-tests.log.gz).

The [2 KiB / 64,000-frame run](evidence/workflow-capacity/fireweed-qualified-workflow-page2k-558b34f7-500k-8-c6-a.json.gz)
completed three million workflows at **5,664.93/sec**, 14.3% above the 4 KiB
baseline, but failed cycle four at 4,261.53/sec. All other checks passed.
Process filesystem output increased to **83.852 GiB**, so these measurements
do not establish reduced write traffic as the reason for improvement.

The next candidate preserves the prior checkpoint byte window across page
sizes: `64,000 * 4096 / actual_page_size` frames, giving 128,000 frames for
new 2 KiB databases and retaining 64,000 for existing 4 KiB databases. Startup
uses actual page-size readback and verifies the resulting checkpoint setting.
This separates smaller-page behavior from the inadvertently halved checkpoint
byte budget in the preceding trial. The fixed 512 MiB WAL acceptance budget
and all throughput/stability gates remain unchanged.

The page-size-adjusted checkpoint candidate passed all 106 combined release
tests, with one existing ignored test. [Validation log](evidence/workflow-capacity/fireweed-page2k-bytebudget-tests.log.gz).

### Page-cache accounting regression found under the larger WAL window

The [128,000-frame candidate run](evidence/workflow-capacity/fireweed-qualified-workflow-page2k-bytebudget-184f4d60-500k-8-c6-a.json.gz)
was stopped after its first cycle reached 148.22 seconds (about 3,373/sec).
CPU was high and I/O pressure low. Host ptrace policy denied live attachment;
a separate debugger-launched run captured this [stack snapshot](evidence/workflow-capacity/fireweed-page2k-bytebudget-gdb.txt.gz)
before being interrupted and killed. Neither partial run qualifies.

Three writer stacks were inside `PageCache::count_evictable_pages`, called
from page allocation. WAL commit clears page dirty flags without reconciling
`evictable_count`; that stale estimate causes later allocations to repeatedly
scan the full cache. The next patch refreshes the estimate once per WAL commit
and short-circuits the fallback count after finding enough evictable pages.
Native spillability, eviction checks, and durable-write ordering remain intact.
Capacity improvement is not yet measured.

The cache-accounting patch passed [43 native cache tests](evidence/workflow-capacity/fireweed-core-cache-tests.log.gz)
and [3 native pager tests](evidence/workflow-capacity/fireweed-core-pager-tests.log.gz),
with one existing ignored native cache test. All [106 combined release tests](evidence/workflow-capacity/fireweed-cache-accounting-public-tests.log.gz)
passed, with one existing ignored Turso test. The native tests use features
`fs,uuid`; the upstream test module references UUID even when defaults are off.
The larger-population qualification must still be rerun before accepting the fix.

### Cache fix qualification and page-size follow-up

Clean `793e15d8` (2 KiB pages, adjusted checkpoint window, cache reconciliation)
passed its first [three-million-workflow qualification](evidence/workflow-capacity/fireweed-qualified-workflow-cache-accounting-793e15d8-500k-8-c6-a.json.gz):
**6,015.22/sec**, every cycle above target, exact outcomes and storage/RSS
stability, and sampled per-shard WAL maximum 269.83 MiB. Process output was
72.887 GiB. The [smaller 16-cycle qualification](evidence/workflow-capacity/fireweed-qualified-workflow-cache-accounting-793e15d8-100k-8-c16-a.json.gz)
also passed at **7,400.38/sec** over 1.6 million workflows, including every
cycle and stability check.

The [million-row primitive qualification](evidence/workflow-capacity/fireweed-qualified-primitives-cache-accounting-793e15d8-1m-8-a.json.gz)
passed with **71,298 inserts/sec**, **34,046 enrichment updates/sec**, and
**36,333 scheduling updates/sec**, including projection coverage. Claim/complete
was 16,975/sec and purge 32,601/sec; all correctness checks passed.

The [larger repeat](evidence/workflow-capacity/fireweed-qualified-workflow-cache-accounting-793e15d8-500k-8-c6-b.json.gz)
failed cycle two (zero-based): its slowest shard took 111.89 seconds, equivalent
to 4,468.86/sec. It was stopped after that definitive failure, retaining all
completed-cycle stderr and termination status. This is not a qualifying repeat.

The next candidate retains cache reconciliation but restores new database pages
to 4 KiB. The smaller-page trials increased main DB size and process write
accounting; the original 4 KiB setting must now be compared with the actual
cache-accounting fix in place. Existing files retain their page size, and the
checkpoint byte-window calculation remains based on actual page-size readback.

The restored-4-KiB/cache-fix candidate passed all 106 combined release tests,
with one existing ignored test. [Validation](evidence/workflow-capacity/fireweed-cache-accounting-page4k-tests.log.gz).

### Final 4 KiB and sharding comparison

The [eight-shard/eight-worker comparison](evidence/workflow-capacity/fireweed-qualified-workflow-cache-accounting-page4k-b0f89563-500k-8-c6-a.json.gz)
was stopped after cycle two narrowly missed at about 4,972/sec. The
[eight-shard/sixteen-worker trial](evidence/workflow-capacity/fireweed-qualified-workflow-cache-accounting-page4k-b0f89563-500k-8-w16-c6-a.json.gz)
was stopped after its second cycle reached only about 4,300/sec. These partial
failed reports retain completed cycle output and termination status. Increasing
worker count within the same projections did not fix the larger-population case.

Sixteen physical shards with eight workers each passed both complete six-cycle
runs on that same binary, at 7,873.17 and 8,029.20 lifecycles/sec. Run A's process
filesystem output was 46.663 GiB, versus 65.214 GiB for the earlier complete
eight-shard/4-KiB baseline without cache reconciliation. This comparison combines
the cache repair and increased physical sharding; it does not isolate either
change's contribution. Both final million-row primitive runs passed as recorded
in the opening table. No throughput or stability acceptance threshold was relaxed.
