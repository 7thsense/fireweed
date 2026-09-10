# Fireweed original-row workflow capacity

Date: 2026-09-10. Measurement snapshots: `02730571` through `2d6996ff`.

**Qualification is still failing.** Million-row insert/update measurements exceed
10k rows/sec, but completed 1.6-million-workflow soaks sustain about 3k workflows/sec,
below the 5k target. Memory and projection sizes remain bounded in those runs.
The optimization work and performance goal remain active; Snorri migration is not qualified.

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
