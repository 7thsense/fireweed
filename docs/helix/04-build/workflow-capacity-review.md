# Fireweed workflow capacity review

Date: 2026-09-09. Initial implementation reviewed: `1c8a2c4f`.
This is an implementation and measurement report, not a new throughput promise.

**Verdict:** 10k batched inserts/point updates per second is a reasonable target,
and measured runs reach it. It is not yet a stable end-to-end capacity guarantee.
The implementation had real correctness and scaling defects; this change fixes
14 documented failures/costs and adds public-API acceptance and capacity tests.
Four-shard bulk delivery reaches 5.2k complete lifecycles/s, while the richer
Snorri profile reaches 1.53k with larger batches. The mutable-row profile is much
slower. Keep Snorri migration behind the qualification steps below.

## Workload and boundary

Fireweed owns durable intent, projected queue/state, eligibility, leases,
transitions, and retention of queue items. Snorri owns workflow interpretation,
handler dispatch, and external effects. No sibling Snorri or Cayce code is changed.

The public-API harness is `crates/fireweed-workload`; its README records source
mapping and exact commands. It models both Seventh Sense implementations:
`actions-queue` has a SKIP LOCKED scheduler-job owner plus bounded FIFO queued
inputs and continuous scheduled delivery; `jobs-scheduled-actions` loads a
scheduled backlog then repeatedly marks due ordered batches Executing and
records delivery outcomes. The reference is `telepathdata/7thsense` master
`30c9e4bd817c53c8918215f9b94dae01b8f89fe8`. It was retrieved read-only because
there was no sibling Seventh Sense checkout. Cayce reference:
`20c31168d079ae5e4c5ddb407922373bc0dcd283`.

Snorri's `state_store_claim`/`state_store_commit` establish the atomic input
finalize + opaque state + next lifecycle inputs + instance fence pattern. The
harness calls that existing Fireweed API directly. It does not claim that the
current sibling adapter is drop-in compatible: that adapter still refers to a
retired SQLite opening API.

## Why prior numbers were misleading

Recorded phased filesystem-log/Turso runs already showed a queue-size collapse:

| Resident records | Push records/s | Enrich records/s | Schedule records/s | Claim/complete records/s |
| ---: | ---: | ---: | ---: | ---: |
| 10,000 | 13,020 | 16,702 | 20,446 | 21,618 |
| 100,000 | 11,175 | 6,128 | 5,819 | 7,040 |
| 1,000,000 | 2,265 | 389 | 401 | 510 |

These are existing repository measurements, not results of the new harness.
Source: `ss-objectlog-turso-memory-goal.md`, evidence `ss-phased1788924812`.
The million-record run took roughly two hours and added about 1.13 GiB RSS.
Uniform replacement values and priority values permitted special cases;
streaming tests that handed downstream stages keys via channels omitted actual
work discovery. Neither establishes continuous retention stability.

The new component profile explicitly reports individual operation throughput.
The autonomous workflow profiles report **complete lifecycles per second**.
They include claims, handler state changes, retries, finalization, receipt writes,
and final projection coverage. Those two units must not be compared as though
one insert equalled one complete workflow.

## Confirmed defects

1. **Addressed updates could scan the queue.** Turso 0.7.2 chose
   `fireweed_items_pending_group_idx` with just the tenant/queue prefix for
   UPDATE predicates that also supplied exact item IDs or client keys. An
   index hint with an IN list still did not guarantee point seeks. A controlled
   100-row update took about 8 ms at 10k rows, 90 ms at 100k, and 970 ms at 1m;
   an explicitly bounded active-key range took about 1 ms. Arbitrarily scattered
   addresses require actual point seeks, not a min/max range spanning the queue.
   The ungrouped homogeneous replacement path now performs indexed equality
   updates within the existing batch transaction.
2. **Generation validation copied historical identity.** Push and batch update
   cloned the accumulated identity map, then cloned it again into an overlay
   and scanned it to produce a delta. Work grew with all retained queue history.
   Validation now copies only relevant non-key facts; applied client-key
   membership is read by indexed points, and recovery no longer loads all keys
   into a permanent process cache. Retained request IDs are also read with their
   expiry instead of accumulating permanent fingerprints. Eight public retention
   cycles exercise request-ID and item-key reuse. Unapplied generations still
   carry their necessary identity overlay.
3. **The payload-sidecar migration was incomplete.** Filtered claims and several
   reads fetched the old inline column; mutation paths could write a different
   payload from the one readers returned. Explicit clearing skipped sidecar
   writes. Purge/reaping omitted sidecar deletion. Public tests reproduced
   immediate payload loss. Reads preserve legacy inline rows while preferring
   sidecar presence (including an explicit NULL); mutations update the sidecar,
   and removals reclaim bodies in the same transaction.
4. **A shared selection lock could be acquired twice.** The generation held a
   shared lock and the append committer reacquired it. A queued exclusive
   filtered claim could wait for the first acquisition while preventing the
   second. This caused intermittent indefinite stalls in the overlapping
   workload. Append now carries the fact that the generation's shared permit
   remains live, avoiding a second acquisition.
5. **Validation could see stale projection state.** Claim acknowledgement can
   precede projection application. Commit validation and lease reclaim could
   reject or overlook those claims. Item-ID batch updates did not load their
   addressed rows at all. Those paths now obtain the required coverage and
   bounded snapshots before validation.
6. **Pipelined batch-update responses bypassed real validation and replay.**
   The generation path could report success based on unresolved client keys,
   and repeated request IDs returned `NotFound` instead of the original outcome.
   It now validates actual addressed snapshots, logs the response, and reads
   retained outcomes for replay.
7. **Existing interfaces had unwired operations.** Turso had fence storage and
   fence writes but no implementation of the fence read used by Snorri commits.
   Opaque side-record reads were also unavailable through the facade. These
   existing operations are now wired. `mutate_items` remains an explicit
   unsupported composed-Turso surface; the preparation profile documents its
   job-owned release/update sequence instead of pretending that it is atomic.


8. **Priority pagination was unstable under projection progress.** The ordered
   selector used OFFSET while an asynchronous writer removed earlier Pending
   rows between pages. This skipped lower-priority work and produced observable
   priority inversions after several thousand deliveries. Selection now uses
   the last examined priority/creation sequence as a keyset cursor, with an
   explicit outer ordering after payload joins. FIFO shortcuts are restricted
   to queues whose actual priorities and eligibility allow insertion-order reads.
9. **Fixed waits stacked on dependent work.** A generation linger, a second log
   packing linger, and an optional 80 ms claim/completion fusion wait could all
   delay a synchronous workflow step. Already aggregated local-filesystem generations now seal
   their log append, generation linger is 1 ms, and projection coverage waiters
   interrupt optional fusion waiting. The authoritative log acknowledgement
   remains required; this does not remove a durability barrier.
10. **Legal delivery batches could retry forever.** The public queue accepted
    1,000 rows but generation admission capped work at 800 and returned temporary
    backpressure for an intrinsically oversized request. The internal limit now
    admits 1,000 rows and rejects requests exceeding its item/byte bounds with
    a permanent size error. A public 1,000-row push/claim/complete test covers this.

11. **Cancellation could release an active transaction's owner.** The adapter's
    blocking SQL worker owns a cloned connection and can outlive cancellation of
    its async caller. Previously that cancellation could drop the transaction
    and writer guard while SQL was still running. An apply now acquires the
    writer before starting an owned task that retains the transaction through
    completion. Cancellation while queued starts nothing; cancellation after
    admission drops only the response waiter. The cancellation regression now
    settles the writer and requires the admitted batch and replay outcome to
    exist before the next apply.

12. **A generation released its turn before publishing claimed IDs.** The
    helper extracting append commands consumed the structure owning the
    generation guard. A second ordinary claim could therefore take its snapshot
    before the first generation appended and published its exclusion delta.
    Two Snorri dispatcher workers reproducibly returned the same 30 IDs; the
    authoritative apply correctly poisoned the projection on the duplicate
    claim. Command extraction now borrows the owner, retaining the turn through
    append and publication. The shared-dispatch workflow and a direct eight-worker
    public claim contract cover disjoint selection and successful finalization.

13. **Successor insertion repeated the identity scan through another API.**
    Snorri commits use `index_validate_push`, rather than the ordinary bulk-push
    generation path. Its combined `(item_id = ? OR client_item_key = ?)` check
    searched only the tenant/queue prefix for each successor. The query plan
    confirms the scan. Separate indexed existence checks now seek the full item
    ID and full active client key; identity conflict semantics remain the same.
    The initial 10k shared-dispatch diagnostic was deliberately interrupted after
    more than four minutes to fix this; it is not a completed capacity result.

14. **Immediate retry planned against pre-claim versions and attempts.**
    Ordinary claims acknowledge from the log before the SQL row becomes Leased.
    The retry/release path trusted a remembered bearer but read the old SQL
    version and attempt count. A concurrent apply could produce a validation
    conflict, or retry exhaustion could be planned with stale data and poison
    later apply. A public claim→retry regression reproduced projection poisoning.
    Retry/release now wait for coverage before planning; the regression requires
    exactly five attempts and all 240 rows terminally failed.

### Checkpoint experiment (not retained)

**Manual checkpoints discard the hot cache.** Fireweed attempted explicit
    `wal_checkpoint(TRUNCATE)` whenever the WAL reached 4 MiB. In the pinned
    Turso 0.7.2 source, explicit checkpoints request page-cache clearing, whereas
    automatic checkpoints do not (`storage/pager.rs`, checkpoint finalization).
    WAL truncation also synchronizes the WAL file (`storage/wal.rs`,
    `truncate_wal`). The accepted `wal_autocheckpoint=0` pragma does not set the
    engine's hardcoded 1,000-frame automatic threshold. The manual truncation
    threshold was tested at 64 MiB. That run was slower, including insertion and
    claim/completion, and used more memory. The change was reverted: the measured
    implementation retains the original 4 MiB policy. This is opportunistic
    maintenance, not a hard cap while readers pin WAL history. Avoiding cache
    eviction alone is not sufficient; retained WAL history also has a cost.

The writer already uses `synchronous=OFF`, but that does not make explicit
checkpoint maintenance free. The log remains authoritative. Queue-wide scans,
copied history, missing API operations, lock cycles, and cancellation ownership
needed separate fixes; simply setting another pragma was insufficient.

## Is 10k records/second reasonable?

It is a reasonable target for **batched point operations**, not an unconditional
promise for 10k separately durable sequential calls, or for 10k complete
multi-stage workflows on one writer.

* 10k operations/s allows 100 microseconds per operation on one serial resource.
  If a caller waits for each durable call and it takes 1 ms, the ceiling is
  1,000 calls/s. A batch of 100 amortizes that fixed latency: its budget for
  10k records/s is 10 ms, including log work, projection work, and API overhead.
* At 1 KiB per body, 10k inserts/s carries about 10 MiB/s of body bytes alone.
  That is not an inherently unreasonable byte rate. Actual traffic includes
  encoded log records, hot rows, indexes, side records, and WAL/page writes;
  measure that amplification rather than equating payload bytes to disk bytes.
* A mutable three-stage lifecycle requires an insert, preparation claims,
  enrichment updates, releases under the current API, a delivery claim, and a
  finalization. The Snorri profile also inserts successor inputs and writes
  state/fences/receipts. Even 10k per-operation throughput implies substantially
  fewer than 10k complete lifecycles/s on the same serial writer.
* A broad scan changes the arithmetic entirely. Scanning one million rows for
  a 100-row update means 10,000 examined rows per changed row. At 10k changed
  rows/s, that asks for roughly 100 million row examinations/s before any real
  update work. That is an implementation defect, not an event-sourcing cost.
* Physical shards add independent writers. If each sustains R records/s, S
  shards have an upper bound near S×R only while CPU, storage bandwidth, log
  serialization, and shared coordination have spare capacity. Logical queue
  labels sharing one Turso database do not add writers.

## Measurement status

The initial test host is a Ryzen 7 4800H (8 cores / 16 logical CPUs), 62 GiB RAM,
Linux. Capacity runs use the release binary and are kept separate from compiler
and test activity. `scripts/perf/workflow-capacity.py` records command, source
and binary digests, wall time, exact child CPU usage, peak RSS, backing filesystem,
storage sizes, and JSON results. Initial `v1`–`v5` diagnostic harness runs used
`/tmp`, which is tmpfs on this host. They measure the software paths and do **not**
establish disk-backed durable throughput. The capacity runner now places its
default data under `target/workflow-capacity/` on the encrypted NVMe-backed
Btrfs filesystem. Tables below must identify which backing store was measured.

### Component capacity: records/second, not full workflows

The NVMe is a Kingston OM8PCP3512F-AB 512 GB class drive. The normal runs use
an encrypted Btrfs mount with `compress=zstd:3`. Batch size is 1,000, body size
1 KiB. All completed phases include projection coverage and state assertions.
Raw artifacts are in [the evidence directory](evidence/workflow-capacity/).

| Run / backing configuration | Resident records / shards | Insert | Enrich by key | Schedule by ID | Claim + complete | Purge | Total wall |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Earlier NVMe implementation snapshot | 100k / 1 | 18,922 | 13,014 | 12,162 | 10,668 | 16,052 | 37.0 s |
| Earlier NVMe implementation snapshot | 1m / 8 | 11,880 | 13,767 | 13,613 | 3,215 | 9,766 | 600.7 s |
| Final fixes, NVMe run A | 100k / 1 | 15,585 | 7,554 | 6,731 | 2,830 | 9,145 | 80.9 s |
| Final fixes, NVMe run B | 100k / 1 | 15,481 | 5,340 | 9,722 | 2,654 | 4,170 | 97.3 s |
| Final fixes, isolated Btrfs NOCOW control | 100k / 1 | 7,536 | 12,099 | 12,016 | 11,409 | 6,143 | 55.1 s |

“Earlier” rows precede the final generation-ownership, executor compatibility,
successor-validation, and immediate-retry fixes; they are diagnostic snapshots,
not a claim that the final binary was rerun at one million rows. Binary digests
and source-diff digests distinguish them. The earlier single-shard million-row
run reached insert/enrich/schedule/claim rates of 10,883 / 6,300 / 7,065 / 2,800,
but timed out at 900 seconds during purge. Its failure is retained in the evidence.

These results do **not** establish stable 10k operation throughput on this host.
The final normal-filesystem runs used about 50 seconds of CPU while wall time
varied much more; earlier 100k wall time was 37 seconds with 43 CPU seconds.
The eight-shard million-row run attributed about 67 GiB of output blocks to the
process, versus roughly 2.3 GiB of retained log and 1.9 GiB of projection files.
These OS write-accounting bytes are not identical to physical NAND writes.
Short device samples during its drain showed roughly 75–93% NVMe busy time,
39–45 MiB/s of writes and 47–117 flushes/s. That supports storage pressure as a
contributor, not a proof that all remaining time is I/O or a hard device ceiling.

The NOCOW experiment set `chattr +C` on an **empty isolated benchmark directory**;
its log, projection, and WAL inherited the flag. Setup and actual attributes are
recorded in `nocow-method.json`. This changes the filesystem treatment of both
log and projection, so it is a control experiment, not a recommended production
switch. It improved update/drain throughput but slowed insertion and did not
make every phase meet 10k/s. Do not infer a universal fix from one such run.

The batch-size-one control is explicitly serial. At 1,000 rows it achieved only
24–29 records/s in the completed insert/update phases and hit its 180-second
limit during drain. A complete 100-row control took 14.2 seconds, with rates
16 insert / 63 enrich / 63 schedule / 31 claim+complete / 68 purge per second.
Those measurements include durable API overhead, not just a SQL UPDATE.
They do not measure many concurrent one-row callers sharing a generation.

### Autonomous workflows: complete recipient lifecycles/second

Every run below has 10,000 recipients, 1 KiB bodies, deterministic failure stubs,
527 retries, 323 permanent failures and 9,677 successful terminal recipients.
There are two workers per pool per physical shard. Mutable uses three stage
pools; Snorri uses one shared dispatcher with explicit lease reclamation.

| Profile | Batch | Physical shards | Complete lifecycles/s | Wall time |
| --- | ---: | ---: | ---: | ---: |
| Bulk scheduled delivery | 1,000 | 1 | 1,063 | 9.4 s |
| Bulk scheduled delivery | 1,000 | 4 | 5,191 | 2.0 s |
| Mutable preparation + delivery | 100 | 1 | 136 | 73.8 s |
| Mutable preparation + delivery | 100 | 4 | 421 | 23.8 s |
| Snorri transitions | 100 | 1 | 194 | 51.7 s |
| Snorri transitions, three repeats | 100 | 4 | 457–481 | 20.8–22.0 s |
| Snorri transitions | 500 | 1 | 400 | 25.0 s |
| Snorri transitions | 500 | 4 | 1,530 | 6.6 s |
| Snorri transitions | 1,000 | 1 | 415 | 24.2 s |

The single-shard bulk/mutable rows use schema-v2 workloads before the final
successor/retry fixes; final four-shard and Snorri rows use the final fixes.
All complete workload checks passed in those reported runs. The intermediate
four-shard `Conflict` is retained separately; the immediate-retry regression
reproduced projection poisoning and passed after the coverage fix. Three fresh
four-shard runs then passed. Do not silently count the failed run as throughput.

Schema-v1 diagnostics used individual retry calls and stage-filtered Snorri
consumers. Schema v2 batches equal retry outcomes and follows Snorri's ordinary
shared-claim fallback. Thus the workflow revisions are not backend-only speedup
comparisons. Larger Snorri batches help, but 500→1,000 adds little on one shard.
At 1,530 recipients/s the three-stage profile executes approximately 4,591
committed stage transitions/s plus retries. That remains distinct from 10k
complete three-stage recipients/s and from the user's existing PostgreSQL rate.
No matched PostgreSQL run on this hardware was performed.

### Retention and correctness

The same physical store completed three 100k-record load→enrich→deliver→purge
cycles, reusing expired request IDs and client keys. Cycle wall times were
57.1 / 66.4 / 68.9 seconds. End-of-cycle RSS was 359 / 396 / 399 MiB; projection
file size was 177.6 / 178.7 / 178.7 MiB. These are earlier implementation-snapshot
measurements. The final binary passes the retention contracts and profile tests.
The projection reused space; three cycles do not prove an indefinite memory
plateau or a sustained multi-million-row soak. Snorri's opaque instance/receipt
record retention is not covered by this queue-item purge profile.

Final verification:

* 518 library tests passed across Fireweed, engine, object log, relational code,
  and Turso, with two live-S3 tests explicitly excluded and one ignored test.
* The final adapter/workload run passed 85 tests (including 31 Turso library
  tests already represented above), with one performance qualification ignored.
  A final facade rerun passed all 146 tests, with one ignored test.
* Public regressions cover payload replacement/Keep/clear, priority and FIFO
  ties, due eligibility, reclaim/stale leases, simultaneous instance fences,
  disjoint ordinary claims, immediate retry exhaustion, legal 1,000-row batches,
  retention reuse and log-only recovery after child-process exit without Drop.
* Formatting, diff whitespace and GitHub Actions policy checks passed.
* Strict Turso clippy is blocked by four pre-existing engine lints; the initial
  HEAD contains those patterns. Broader integration selection also encounters
  stale tests using retired SQLite constructors. The entire workspace/CI lane
  is **not** claimed green. See `verification.json` for commands and limitations.

## Remaining work and adoption plan

1. **Batch the transition implementation, preserving its semantics.**
   `prepare_commit_transition` still loops over entries, validates their claims,
   reads fences, validates successor pushes, and emits separate side-record,
   fence, push and finalize commands. A 100-entry API call is not one efficient
   relational row batch. Add bounded multi-entry snapshot reads and batch apply
   where dependencies permit. Preserve per-entry rejection outcomes, repeated
   instance-fence ordering, side-record ordering, IDs and log-only replay.
   Verify equivalence with mixed accepted/rejected entries and same-instance
   transitions before comparing the unchanged public workload. This can begin
   behind the existing interface; it is not a reason to merge Snorri into Fireweed.
2. **Make stage-filtered discovery proportional to returned work.**
   The mutable profile searches stage metadata and uses a serialized
   release→update sequence. Generic JSON residual filtering can inspect unrelated
   pending stages, including empty polls. Qualify an indexed transition/stage
   selector or a queue layout appropriate to the host, with actual backlog
   discovery at 10k/100k/1m. Do not replace discovery with a harness key channel.
   Keep scheduler-job ownership explicit until lease-guarded mutation is supported.
3. **Measure and reduce storage amplification.**
   Instrument log append, projection apply, checkpoint and device latency
   separately. Repeat the same workload on the intended production filesystem,
   and compare independent storage devices with multiple writers on one device.
   Do not loosen authoritative log durability to improve a derived projection
   number. The rejected 64 MiB checkpoint experiment and mixed NOCOW result show
   why a pragma/filesystem recommendation needs measurements of every phase.
4. **Set a rollout gate using both throughput units.**
   Retain 10k batched insert/update records/s as an engineering target. Define a
   separate end-to-end recipient/transition target against a matched PostgreSQL
   baseline, including tail latency and fairness. Run millions of resident rows
   and a sustained recycling soak with bounded active state, fault injection,
   recovery and memory/storage accounting. Warm-cache bursts and three successful
   short runs are insufficient evidence of stable production capacity.
5. **Migrate Snorri only after those gates pass.**
   Qualify its preferred typed transition-index path, then update its retired
   constructor/capability handling separately. Keep workflow interpretation and
   effects in Snorri/Cayce; keep durable queue/state mechanics in Fireweed. Repair
   the stale CI/lint baseline and exercise live-S3 deployment paths before making
   a deployment-readiness claim.

The performance goal is not proven impossible. The raw byte rate is modest and
batched runs meet it under some conditions. What is unproven is a stable
end-to-end guarantee for this implementation and storage configuration. The
remaining work is batch execution, selection design and storage qualification,
not an inherent impossibility of event sourcing.

## Scope of the current qualification

The payload fixture contains a deterministic header and repeated padding, so its
1 KiB logical size is not a claim about incompressible I/O. Btrfs compression is
reported with the mount. Peak RSS includes the harness's ID/receipt oracle as
well as Fireweed. Final storage sizes include the authoritative log: purging queue
rows deliberately does not erase log history. Projection files may retain freed
pages for reuse rather than shrink; separate tests verify that payload-sidecar
row counts return to zero after each cycle.

Snorri's current adapter prefers its typed transition index through
`claim_by_query_at` and contains a normal-claim fallback. This harness qualifies
the public claim/commit state-machine boundary, not that adapter's typed-query
path or its migration. The mutable profile's released-Pending update window is
protected by one scheduler-job owner per preparation stage, as documented in
the harness. Distributed scheduler ownership belongs in the workflow host.

Capacity runs use a local filesystem log and a local Turso projection. They do
not qualify remote S3 latency, multi-host failover, network RPC cost, or external
handler throughput. The low-latency forced-seal change is local-filesystem only.
