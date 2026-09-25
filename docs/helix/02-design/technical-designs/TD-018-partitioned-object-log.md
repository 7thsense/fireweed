---
ddx:
  id: td-partitioned-object-log
  depends_on:
    - api-fireweed-kernel-contract
    - adr-kernel-records-contract
    - adr-cqrs-log-projection-storage-model
    - adr-queue-as-shard-unit-and-projection-families
    - adr-s3-turso-public-cell
    - td-sharding-and-shard-ownership
    - td-object-log-turso-projection
    - tp-scale-substantiation
    - spike-record-key-storage
    - spike-claim-path-at-scale
    - spike-declared-field-storage-layout
  links:
    - {kind: informed_by, to: api-fireweed-kernel-contract}
    - {kind: informed_by, to: adr-kernel-records-contract}
    - {kind: informed_by, to: adr-cqrs-log-projection-storage-model}
    - {kind: informed_by, to: adr-queue-as-shard-unit-and-projection-families}
    - {kind: informed_by, to: adr-s3-turso-public-cell}
    - {kind: informed_by, to: td-sharding-and-shard-ownership}
    - {kind: informed_by, to: td-object-log-turso-projection}
    - {kind: informed_by, to: tp-scale-substantiation}
    - {kind: informed_by, to: spike-record-key-storage}
    - {kind: informed_by, to: spike-claim-path-at-scale}
    - {kind: informed_by, to: spike-declared-field-storage-layout}
  status: proposed
---

# Technical Design: TD-018 Partitioned Object Log

**Status**: Proposed; measurement-gated. Nothing here is committed until the spikes in "Measurement plan" report and the owner accepts the result.
**Contract**: API-006 (unaffected) | **ADR**: ADR-001, ADR-008, ADR-024, ADR-025 | **TD**: TD-003, TD-010, TD-017 | **Evidence**: SPIKE-002, SPIKE-004, SPIKE-006
**Scope**: the S3×Turso object-log layout, its commit path, its placement unit, and how its storage grows.

## Scope

This design replaces the per-queue object-log metadata of the S3×Turso cell with **partitions**. The control plane assigns each queue to one partition. A partition holds one partition log, one head document, one Turso projection file and one lease with its epoch. Many queues share the partition's data objects and its single head, so one commit costs one data-object PUT and one head compare-and-swap (CAS), however many queues it carries.

In scope:
- what runs today, verified from code with file and line;
- the partition cell, queue-to-partition assignment, the head document, group commit, offsets, fencing and handoff;
- PUT economics, recomputed against the code that runs;
- the measurement plan that gates every step;
- three proposals that are **not** commitments: snapshot plus truncation, log splitting by partition range, and archive and full-delete garbage collection (GC).

Out of scope:
- the external contract. API-006 is unaffected (see "External contract").
- the Turso table layout (declared fields, the claims table, indexes), which TD-017 (`docs/helix/02-design/technical-designs/TD-017-kernel-projection-storage-layout.md`) designs;
- a remote transport;
- implementation beads. None are filed before the owner reviews the spike results.

## Decided placement model

These points are decided (ADR-025 §5) and bind this design:

- **No partitions inside a queue.** A queue is never split.
- **Mapped, not hashed.** Each queue is assigned one partition by the control plane. The assignment fixes the queue's node and its object-log residency.
- **Tenants are independent of partitions.** One partition holds queues of many tenants, and one tenant's queues may span many partitions.
- **Shared log and head.** Many queues share a partition's log (ADR-025's "log stream"; this design calls it the partition log) and a single head/index document, which replaces today's per-queue index documents.
- **No rebalancing.** A queue never moves between partitions.
- **Measure first.** Compaction, snapshots and truncation, log splitting by partition range, and full-delete GC are proposals that must be measured before anyone commits to them: "be very careful and measure first; get proper performance in place first". The first step is measuring today's per-queue commit cost on MinIO at 1, 10, 100 and 1,000 queues.

These are this design's own choices, open to review:

- **A partition** has one control-plane lease and epoch. It also has one Turso projection file, which TD-017 §1 fixes.
- **Group commit** covers all of the partition's queues.
- **Per-queue dense offsets are preserved**: each queue keeps its own stream inside the partition log.

## Terms

| Term | Meaning in this design |
|---|---|
| Partition | The Fireweed placement and storage cell, identified by a `PartitionId` that the control plane assigns. |
| Partition log | The partition's one log: one `LogEngine`, its data objects and its head. ADR-025 calls it the partition's "log stream". |
| Stream | object-log's `PartitionKey`. Today there is one per queue, named `tenant\0queue` (`crates/fireweed-objectlog/src/log_engine_store.rs:79-85`). It **stays per queue** inside a partition log. This design says "stream" only for this per-queue `PartitionKey`, to avoid the name clash. |
| Head | The one CAS-updated document per partition. It replaces per-queue index documents, the catalog, epoch documents and high-water hints. |
| Namespace | The S3 prefix of one deployment. |
| L | The engine linger (commit latency bound). The default is 20 ms. |

object-log line references below are to tag `v0.3.3` of the object-log repository, which is the version Fireweed pins (`crates/fireweed-objectlog/Cargo.toml:20`).

## Current state (verified from code)

| # | Finding | Evidence | Consequence |
|---|---|---|---|
| 1 | **FWSG was deleted in v0.24.0.** The segmented authority-head substrate (manifest candidate, versioned head, recovery index, retirement marker) no longer exists. | `crates/fireweed-objectlog/src/storage_generation.rs:1-7`; `docs/releases/v0.24.0.md:3-5` | Any cost or design argument based on FWSG describes a protocol that does not run. |
| 2 | **TP-002 still prices the deleted protocol.** The granularity table charges `5 + index height` PUTs per seal (8.35 on average) and assumes "independent queues cannot share a segment". Both statements describe FWSG. | `docs/perf/tp002-objectlog-granularity-economics.md:11`, generated by `crates/fireweed-release/src/lib.rs:2744-2800`; epic bead `fireweed-e9f22903` records the same | The published figures ($1,097 per queue per month at 10 cmd/s) overstate today's cost and model the wrong mechanism. |
| 3 | **What runs** is object-log v0.3.3: one `LogEngine` with a `ManifestSequencer` per `ObjectLogEngineStore`, and that engine already packs every queue's batches into shared data objects. | `crates/fireweed-objectlog/Cargo.toml:20`; `log_engine_store.rs:653-679`; object-log `src/engine.rs:236-240` | Data objects are already shared. The per-queue unit is the index document, not the segment. |
| 4 | **Per-queue index-document CAS, run serially.** Every commit reads the catalog (one GET), then loops over the distinct queues in the commit one after another. Each queue's index document is updated by CAS, which on S3 is a GET of the whole object followed by a PUT with `If-Match`. The commit is not atomic across queues: each queue publishes on its own and may be rejected alone. | object-log `src/manifest_sequencer.rs:389-440` (catalog at `:408`, which reads at `:173`; the loop at `:411`); `publish_partition` at `:211-304`; `src/s3.rs:558-600` (the GET at `:564`); the `Sequencer` contract that says commit is atomic across the slice, `src/sequencer.rs:97-100` | Commit cost grows linearly with the number of active queues, and all of it is serial inside the single committer. |
| 5 | **The index document grows without bound.** It keeps every entry since `log_start` and is re-serialized and re-uploaded on every commit. Fireweed makes one `produce` per public call and never calls `truncate_before`. | object-log `src/manifest_sequencer.rs:275` (`doc.entries.push`), `:539` (`truncate_before`, unused); `log_engine_store.rs:1274-1406` (`produce_immediate`) | Estimated at 170–200 B of JSON per entry: about 6–7 MB per queue after one hour at 10 produces/s and about 150–170 MB after one day. All of it is downloaded and uploaded again on every commit for that queue. **Not measured.** |
| 6 | **More per-queue metadata:** an epoch document written after the sequencer's epoch fence, and a high-water hint written on the 1st append and every 64th. | `log_engine_store.rs:714-720`, `:973-985` (`store_epoch`), `:1408-1445` (cadence at `:1433`) | These add λ/64 PUT/s, plus one PUT per fence. |
| 7 | **Open cost grows with every queue in the namespace**, not with the queues a node owns. `ManifestSequencer::open` GETs the catalog and then each index document serially, and `load_meta` LISTs and GETs every definition serially. | object-log `src/manifest_sequencer.rs:77-100`; `log_engine_store.rs:906-935` | Start-up and handoff time is O(all queues). |
| 8 | **Server bug: a new namespace on every process start.** The S3×Turso server open builds the namespace from the pid and the current time (`fw-s3-turso-{pid}-{nanos}`), so a restarted server never sees its earlier log. The facade uses the stable `StorageConfig.namespace` instead. | `crates/fireweed-server/src/lib.rs:2872-2879` (inside `open_objectlog_s3_turso_backend`, `:2859-2891`) | A server restart loses every acknowledged write as far as that server can tell. This is a correctness blocker for any restartable deployment, independent of this design. |
| 9 | **Stale-index handoff bug** (read from code, not tested). `ManifestSequencer::lookup` serves only its in-memory index and never re-reads a document. On a CAS conflict, `fence_epoch` returns an error without refreshing its cached copy, so every retry fails the same way until the store is reopened. `fence_epoch` also returns `Ok` without writing anything when the queue is unknown in memory or has no cached document. | object-log `src/manifest_sequencer.rs:503-518` (`lookup`), `:444-500` (`fence_epoch`; the no-op returns at `:456-463`, the conflict error at `:495-499`) | A new owner whose store opened before the previous owner's last commits reads a stale tail, and cannot fence until it reopens. If the queue was created after it opened, the "fence" succeeds without bumping the durable epoch. The old owner can then keep committing, and the new owner's commits are rejected. Safety holds through the commit-time CAS and the projection gap check; liveness does not. |
| 10 | **Placement is per queue.** Leases, epochs and highest-random-weight (HRW) placement are keyed by queue. The public S3×Turso backend uses a process-local control plane; multi-node ownership exists only through an injection path. | `crates/fireweed-engine/src/control_plane.rs:62-79` (`QueueLease`), `:558-589` (`rendezvous_weight`, `resolve_target`); `crates/fireweed/src/turso_compose.rs:3118`; `crates/fireweed-server/src/lib.rs:3313` | Partitions move the lease, the epoch and the placement key from the queue to the partition. |
| 11 | **No snapshot or compaction exists.** `snapshot_now` records an empty payload as a position marker, `compact_log_behind_snapshot` returns `Unavailable`, retention is "not yet rewired to LogEngine", and a projection rebuild is a genesis replay. Hydration for ownership replays from the local Turso high-water. | `crates/fireweed/src/lib.rs:4635-4640`; `crates/fireweed/src/facade.rs:1394-1418`; `crates/fireweed-objectlog/src/maintenance.rs:7`; `crates/fireweed/src/turso_compose.rs:4984-5002` (`rebuild_projection`), `:3296-3350` (`recover_async`), `:1865-1910` (hydration) | The log grows forever, and a node with no local Turso file replays from genesis. ADR-001 (`:127-129`, `:235-236`, `:290`) always intended "snapshot to object storage, then expire segments". |

**Inferred, not measured: the committer stalls above a handful of active queues on S3.** A commit groups at most 8 finished data objects (object-log `src/engine.rs:974-986`), so it covers about 8 × 20 ms = 160 ms of arrivals. It costs one catalog GET plus one GET and one conditional PUT for each distinct queue. With a CAS round trip c ≈ 40 ms, the committer falls behind once more than about 160 ms / c ≈ 4 queues are active in every window. Latency then grows until the 30 s pre- and post-position timeouts (`log_engine_store.rs:127-129`) poison queues. Finding 5 makes this worse over time, because every CAS moves a larger document. Spike A must confirm or refute this.

Flush triggers today, for reference:
- `flush_config_from_segment` sets the linger to `max_latency_ms` (default 20 ms, `crates/fireweed-server/src/env_config.rs:231`), `max_inflight_flushes = 8`, and turns the budget controller on (`log_engine_store.rs:1123-1138`).
- `target_bytes` does not trigger a seal unless it exceeds the 1 GiB engine ceiling.
- An idle engine flushes early after 10 ms (object-log `src/budget.rs:63`).

## Proposed model

### The partition cell

```
namespace/
  p{id}/fwlog/{writer}/{epoch}/{n}     data objects: every queue of the partition, packed (unchanged engine)
  p{id}/fwmeta/head                    the one CAS document: epoch, commit counter, recent tail, run refs
  p{id}/fwmeta/runs/{commit_seq}       immutable index runs and checkpoints
  dir/{tenant}/{queue}                 create-only queue directory entry: the partition id only
  dir/{tenant}/{queue}/closed          create-only close marker: archived or deleted
  dir/{tenant}/{queue}/archive         create-only archive reference (P3 only)

owning node:
  one LogEngine + HeadSequencer per owned partition
  one Turso file per owned partition (one writer, Mutex<Connection>)
  control-plane lease on PartitionId; the epoch lives in the head
```

A partition is one cell:
- **one partition log:** one `LogEngine` with one flush worker and one committer. Data objects are private to the partition.
- **one head document**, updated by CAS. It replaces the per-queue index documents, the catalog, the per-queue epoch documents and the high-water hints.
- **one Turso projection file** on the owning node;
- **one control-plane lease and epoch.**

Per-queue streams stay inside the partition log, so the engine and projection interfaces see the same per-queue positions as today.

### Queue-to-partition assignment

- **When.** On a queue's first `define_queue`, the control plane picks a `PartitionId`. It writes the queue's directory entry, which holds only that id, with a create-only PUT (`If-None-Match: *`) **before** the definition command is appended to that partition's log. This is the primitive Fireweed already uses for its queue-definition authority (`crates/fireweed-objectlog/src/s3_create_only.rs:5`, `:63`). A concurrent creator that loses the PUT reads the entry and routes to the same partition.
- **The definition lives in the log, not the directory.** The partition's single committer orders definition commands, so the first one applied is the queue's definition, and D2 and D3 compare every later `define_queue` against it (API-006 D1–D3).
- **Immutable.** The entry never changes and is never rewritten. Closing a queue writes a separate create-only marker, `dir/{tenant}/{queue}/closed`, after the close command is durable in the partition log; P3's archive reference is another create-only object, `dir/{tenant}/{queue}/archive`. The log's close command is authoritative, and the marker lets a router answer `QueueClosed` without opening the partition (API-006 D6, DQ3).
- **Crash between the two steps.** If a directory entry exists but no definition landed, the next `define_queue` routes to the partition and appends its own definition; that is the normal D1 path, and nothing in the directory has to match it. Every other op returns `QueueNotFound` (D5) until the definition lands. A crash between the close command and its marker leaves the log authoritative; the next op routed to the partition gets `QueueClosed`, and a later close retry writes the marker (`already_closed`, DQ5).
- **Routing.** A node resolves queue → partition from the directory and caches the answer. The partition never changes, and the close is enforced inside the partition, so a stale cache still routes correctly and gets `QueueClosed`. A cache miss costs one GET.
- **Policy.** Choosing a partition is a control-plane and admin concern. A reasonable default is the least-loaded open partition, with an admin override (for example, a set of partitions reserved for a tenant class). The policy is not part of the contract, and it is not a hash.
- **Mapping versus hashing.** Mapping costs a directory lookup on a routing miss. In return, the control plane can pack queues by load, co-locate or separate them on purpose, and add partitions without rehashing any queue.
- **No rebalancing.** A queue's partition is permanent. So a hot partition is relieved only by moving the whole partition to another node, or by proposal P2. A single queue's ceiling is one partition's committer and one Turso writer, which is ADR-008's one-owner ceiling.
- **Partition to node.** Each partition has a control-plane lease. Whether the target node is assigned explicitly or chosen by HRW over partition ids is an open question. A partition moves only on node failure, drain, or cluster expansion.

**Queues stay cheap** (ADR-025 §2: one queue per job type). An idle queue costs one directory entry, a few bytes in its partition's index runs, and its Turso rows. It has no per-queue document on the commit path, and no CAS.

### The head document (`HeadSequencer`)

`HeadSequencer` is a new object-log `Sequencer` implementation. Whether it lives upstream in object-log or in Fireweed is an open question. The head holds:
- the epoch and a partition-wide commit counter (`commit_seq`);
- a bounded recent tail: the index entries since the last run checkpoint, at most 64 KiB;
- references to immutable index runs, merged in the background into a checkpoint that carries each stream's `{next_offset, log_start}`;
- references to retained snapshots (P1 only).

Rules:
- **Commit** is one conditional PUT of the head. The writer caches the ETag of the head it last wrote, so it does not re-GET the document. That needs a versioned CAS on `BlobStore`; today's S3 CAS GETs the object to compare bytes (object-log `src/s3.rs:564`).
- **Conflict.** Re-read the head. If the planned entries are already there, treat the commit as done, as `publish_partition` does today (`manifest_sequencer.rs:286-296`). Otherwise reject: the epoch was lost, and the batch goes down the existing post-position poison path (`PostPositionAmbiguous`, `log_engine_store.rs:1380-1406`).
- **Open** is one GET for the head plus parallel GETs for the runs it references. There is no LIST, which bead `fireweed-f500277e` already requires.
- **Head size.** Per-stream state for 10,000 queues is up to about 2.7 MB with full `tenant\0queue` names, so it lives in the runs, never in the head. The head can name streams by a partition-local queue number assigned at definition, which is the same per-queue integer surrogate SPIKE-002 recommends for Turso rows (`docs/helix/02-design/spikes/SPIKE-002-record-key-storage.md`).

### Group commit across a partition's queues

- **Data level: unchanged.** The engine's linger already packs every stream's batches into one data object (object-log `src/engine.rs:236-240`).
- **Index level: one head CAS per commit** covering every queue in the commit, instead of today's serial per-queue loop (`manifest_sequencer.rs:411`). A commit groups up to 8 ready data objects (`src/engine.rs:974-986`), so head CASes H ≤ data PUTs F.
- **Atomic across the slice.** One CAS makes the commit all-or-nothing for every queue in it, which is what the `Sequencer` contract states (`src/sequencer.rs:97-100`). This removes the partial-commit rule that bead `fireweed-f500277e` had to define. It is not exposed: API-006 A4 still says nothing is atomic across queues.

### Ordering and offsets

- **Streams stay per queue.** Each queue keeps its own dense offsets. `CommandPosition` does not change, and neither does the projection's replay gap check (`crates/fireweed-relational/src/apply.rs:581-607`).
- **Per-queue order** is arrival order, as the engine guarantees (object-log `src/sequencer.rs:99`).
- **Partition-wide order** comes from `commit_seq`. It is internal: snapshots, truncation and GC use it, and nothing else does.
- **Claim ids** are derived from the claiming queue's own stream: the offset of the append that carries the claim and the claim's ordinal within it (TD-017 §3). They are unique per queue, not per partition, and a rebuild reproduces them; `commit_seq` is not used.
- **Rejected: per-partition sequence numbers.** They would change every position consumer and the gap check, and they buy nothing that `commit_seq` does not.

### Ownership, fencing and handoff

- **Lease key:** `PartitionId`. **Epoch:** in the head. A stale owner's commit fails its CAS and goes down the existing post-position poison path.
- **Handoff** follows TD-003's sequence, per partition: drain, release or expire the old lease, acquire epoch + 1, **open the cell fresh** (read the current head), fence with a head CAS, hydrate the Turso file, then serve.
- **Opening fresh at handoff fixes Finding 9.** The new owner reads the head after it wins the lease, and a CAS conflict always re-reads the head. Nothing is served from an index loaded before the fence.
- **Data objects stay private to one partition.** GC, migration and deletion stay local to it. Object names already carry the writer id and epoch (object-log `src/engine.rs:270-271`).
- **Rejected for v1:** per-node shared data objects. They would save about (k − 1) · F PUT/s on a node with k busy partitions, but they need GC and liveness checks across partitions. P2 reconsiders this trade.

### Open and rehydration

- **A node opens only the partitions it owns:** one head GET plus run GETs each, and no namespace-wide LIST (compare Finding 7).
- **Local Turso file present,** with cursors at or beyond the head's floors: replay only the tail.
- **Local file missing:** without P1, replay the partition from genesis, which is unbounded. With P1, download the latest snapshot and replay the tail.
- **Nothing is copied between nodes.** The log already lives in S3.
- **Prerequisite:** Finding 8 is fixed first. The server must open the configured, stable namespace.

### Sizing a partition

Two forces pull in opposite directions, and only measurement settles them:

- **The PUT floor favors few, busy partitions.** A busy partition costs about 2/L PUT/s whatever its queue count: about 100 PUT/s ($1,314/month) at 20 ms, and 20 PUT/s ($263/month) at 100 ms.
- **Turso favors more, smaller files.**
  - Each file has exactly one writer. Independent writer connections fail with `SQLITE_BUSY` at N = 32, so the `Mutex<Connection>` writer is mandatory, and a node gets writer parallelism only from more partition files (`docs/helix/02-design/spikes/SPIKE-004-claim-path-at-scale.md`, Finding 8).
  - Many queues in one file hold their claim throughput only with a lean claim row. At 100 queues × 1M records in one file, with field bytes on the claim row, claim throughput fell 45.8% and p99 rose 52× from page-cache pressure (SPIKE-004, Finding 8). With TD-017's lean claim row, the same shape claims within 4.3% of a 10M single-queue control, with p99 at 1.33× the control (SPIKE-004, Finding 9). Larger files are not measured.
  - Snapshot size and rehydration time grow with the partition's records (P1). The lean 100M-record file in SPIKE-004 Finding 9 was 47.91 GB.

The number of partitions per node, and how many queues and records a partition holds, are therefore measured placement parameters, not constants of this design.

## PUT economics (recomputed)

**Inputs** are the TP-002 ones, plus the cited GET price:
- 730 h/month;
- S3 Standard at $0.005 per 1,000 PUTs, $0.0004 per 1,000 GETs, and $0.023 per GB-month (ADR-001:306);
- 1 KiB commands, one command per `produce`, L = 20 ms, and every queue at 10 cmd/s;
- F, the data-object flushes per second, is λ while λL < 1 and 1/L = 50 after that.

At these prices, 1 PUT/s sustained costs $13.14 per month.

**Today:**
- PUT/s ≈ F + D + λ/64, where D is the number of per-queue CASes and λ/64 is the high-water hints. D ≈ λ is an upper bound. In practice the serial CAS loop makes large queue counts latency-bound before they are PUT-bound (see the inference above).
- GET/s ≈ C + D: one catalog GET per commit (C ≤ F), plus one GET inside each CAS.

**Proposed:**
- PUT/s ≈ F + H + R, where H ≤ F is the head CASes and R = λ × 64 B / 64 KiB is the index-run checkpoints.
- The commit path does no GETs, because the ETag is cached.

| Queues in one partition | λ (cmd/s) | TP-002 published (deleted FWSG) | Today, PUT/s upper bound | Today, GET/s | Proposed, PUT/s | Proposed, per queue |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 10 | $1,097 | 20.2 ($265) | 20 ($21) | 20.0 ($263) | $263 |
| 10 | 100 | $10,974 | 152 ($1,992) | 150 ($158) | 100 ($1,315) | $132 |
| 100 | 1,000 | $109,738 | 1,066 ($14,002) | 1,050 ($1,104) | 101 ($1,327) | $13.27 |
| 1,000 | 10,000 | $1,097,382 | 10,206 ($134,110) | 10,050 ($10,565) | 110 ($1,442) | $1.44 |
| 10,000 | 100,000 | $10,973,824 | 101,613 ($1.34M) | 100,050 ($105,173) | 198 ($2,597)\* | $0.26 |

Dollar figures are per month. The TP-002 column is its published per-queue figure ($1,097.38) times the queue count, which is how TP-002 scales fleets.

\* About 100 MB/s into one partition, which is probably beyond what one Turso writer can apply.

What the table says:
- **A lone low-rate queue does not get cheaper.** It still pays 2 PUTs per command (a data object and an index CAS). Only a longer linger helps it, at the cost of write latency, since DUR1 acknowledges only after the commit.
- **The gain comes from packing.** A busy partition costs about 2/L PUT/s regardless of how many queues it holds.
- **Spreading load multiplies the floor.** k busy partitions cost about 2 · k · min(λ/k, 1/L) + λ/1,024 PUT/s. For 1,000 queues at 10 cmd/s:
  - k = 1: about 110 PUT/s ($1.4k/month);
  - k = 4: about 410 ($5.4k);
  - k = 16: about 1,610 ($21k);
  - k = 64: about 6,410 ($84k).

  Because placement is mapped, k is a control-plane choice. It trades against the Turso limits in "Sizing a partition".
- **Storage is the other cost.** 1,000 queues × 10 cmd/s × 1 KiB is about 26.9 TB a month of ingress. With no truncation, storage alone reaches about $7.4k/month by month 12 and keeps rising; with 24 h retention it is about $20/month. Only P1 bounds it.
- **Snapshot uploads are negligible in PUTs:** a few multipart uploads a day per partition.

All of these figures are modelled, not measured. Spikes A and B replace them, and the TP-002 generator must be rewritten from this model (see "Effect on other documents").

## External contract

API-006 does not change.

- **Placement.** `define_queue` has no placement input. Placement is carried by the admin handle and the control plane (API-006 §7), and §8 excludes partition placement, snapshots, compaction, log splitting and GC from the contract. The partition count and policy are implementation-specific constructor or deployment inputs (API-006 §7, "Construction inputs").
- **DUR1** keeps its meaning. `Ok` means the data object and the head CAS have both landed.
- **A4** still holds. One head CAS covering several queues is an implementation detail, and nothing atomic across queues is exposed.
- **Request-id retention R** (at least 24 h, API-006 §6.8) is stored in the Turso projection: today in `fireweed_request_idempotency` (`crates/fireweed-relational/src/schema.rs:162`), which TD-017 §9 replaces with its `requests` table. Under P1, snapshots carry it; log retention does not.
- **DQ5** leaves the timing of storage reclamation after a full delete to the implementation. P3 is one way to provide it.
- **The conformance suite** (API-006 CS1) runs in-process and cannot observe partitions. The same suite passes whether one partition or many back the instance.

## Proposals (not commitments)

Each proposal below needs its spike and an explicit owner decision. None is part of the v1 partition cell.

### P1. Partition snapshot and log truncation

- **Snapshot.** The owner checkpoints the partition's Turso file, uploads it as an immutable multipart object, and records `SnapshotRef { key, commit_seq, epoch }` in the head.
- **Floors.** Each queue's floor comes from the Turso cursor table (`relational_cursor.next_seq`, `crates/fireweed-relational/src/schema.rs:103`). A cold queue's floor is its `next_offset`, so a cold queue never pins old objects.
- **Retention.** Keep K = 2 snapshots. Take a new one when the tail bytes reach α × the snapshot bytes (α ≈ 1 to start). Storage then stays at about K × snapshot + tail instead of growing with time.
- **Truncation.** First advance the floors in a head CAS. Then, after a reader grace period, delete the index runs and data objects whose entries all sit below the floors. This is local to the partition, because data objects are private to it.
- **No segment rewrite.** The snapshot is the compaction. Superseded commands disappear with truncation.
- **What it changes:**
  - Genesis rebuild ends. `projection_control` rebuild becomes "latest snapshot plus retained tail".
  - Bead `fireweed-f500277e` requires that "a lost Turso file must still rebuild from the log". Changing that needs the owner's approval.
  - ADR-024's recovery rule (loss of the Turso file is recovered by replaying the object log) and TP-003 AC-TXN-1 would read "rebuilt from the latest snapshot plus the retained tail".
- **Unknowns for Spike C:**
  - Can vendored Turso 0.7.2 produce a consistent copy while the writer applies? The options are a checkpoint and copy under the writer mutex (which stalls the writer for the copy), `VACUUM INTO` if it is supported, or a backup API.
  - Upload and download time at partition sizes up to the 48 GB shape, against TD-003's recovery budget.

### P2. Log splitting by partition range

For example, a log that carries partitions 1–255 splits into 1–127 and 128–255 when the cluster grows.

- **Reading.** A partition log carries a contiguous range of partition ids and is split in two, so each half can move to its own node. Queues keep their partition id, and only the mapping from range to partition log changes. So this is not queue rebalancing.
- **Relation to the v1 cell.** In v1 each partition has its own partition log, so expansion is a whole-partition handoff with no data copy (the log is already in S3), as long as there are more partitions than nodes. A split is needed only if partition logs carry ranges. Ranges lower the PUT floor on small clusters, with one committer per range instead of one per partition. That is the per-node shared-data-object option v1 rejects.
- **Sketch.**
  1. Fence the parent log (epoch + 1).
  2. Write two child heads, each with the parent's per-queue floors and offsets for its range, and references to the parent's retained objects.
  3. The parent becomes read-only.
  4. Children read the shared parent objects until truncation passes them. Parent objects are deleted only when both children's floors pass them; that shared reference counting is the cost of the design.
- **Turso files.** Keep one Turso file per partition id even if partition logs carry ranges. Then a split never has to split a projection file.
- **Gate.** Spikes A and B show whether one partition log per partition is affordable at the expected cluster sizes and linger. If it is, P2 is unnecessary.

### P3. Archive and full-delete GC

API-006 DQ1–DQ5 define the observable behavior: archive keeps records in durable storage but unreachable, full delete runs only under `AllowFull`, and reclamation timing is implementation-defined.

- **Close.** A close command is appended to the queue's stream in the partition log, and then the create-only close marker is written. Ops ordered after the close return `QueueClosed`. The queue's Turso rows can be dropped lazily after the close is durable, because every op on a closed queue fails before it reads them. The row-deletion design and its cost belong to TD-017 §8.
- **Without P1:** nothing is truncated, so the log keeps an archived queue's commands and "archive keeps the log" holds trivially. A full delete is physically the same as an archive until GC exists.
- **With P1, the proposals conflict.** Data objects are shared by all of a partition's queues, so truncation would delete an archived queue's commands. There are two ways out:
  - pin the archived queue's floor. That pins every shared object holding its entries and blocks the partition's truncation. **Rejected.**
  - **export before release.** Write an immutable per-queue archive object under `{ns}/archive/` (the queue's retained stream entries, or its final state read from the Turso file), record its reference in the create-only `dir/{tenant}/{queue}/archive` object, and then set the queue's floor to its `next_offset`. **Proposed.**
- **Full delete:** set the floor to `next_offset`, leave the queue out of the next snapshot, and let normal truncation remove objects once every other stream's floor has passed them. A full delete of an archived queue also discards its archive object (DQ5).
- **Not proposed: time-bounded erasure.** An erasure deadline would need shared data objects rewritten, or per-tenant encryption keys. Neither is proposed unless the owner requires it.
- **Gate.** P1, plus a measurement of export cost per archived queue.

## Measurement plan

The spikes change no product code. Spike A comes first (ADR-025 §5).

| Spike | Question | Method | Outputs | Decision it gates |
|---|---|---|---|---|
| **A. Today's per-queue commit cost on MinIO** | Is the committer latency-bound at a few active queues? How fast does the index document grow? | New bench under `crates/fireweed-bench/` against the existing MinIO harness (`crates/fireweed-objectlog/src/test_minio.rs`), on a stable namespace. 1, 10, 100 and 1,000 queues × 10 cmd/s, 1 KiB commands, 20 ms linger. Inject 0, 20 and 40 ms of round-trip delay, because loopback MinIO is far faster than S3. Run long enough, or pre-seed index documents at 1 h and 24 h sizes, to expose Finding 5. | PUTs and GETs per command; commit p50 and p99 and committer backlog; index-document bytes and bytes moved per commit over time; open time against queue count. | Confirms or refutes Findings 4–5 and the stall inference. If they are refuted, this design is re-derived before B continues. |
| **B. `HeadSequencer` prototype** | Does one head CAS per commit hold at 1,000 and 10,000 streams? | Branch in the object-log repository: `src/head_sequencer.rs`, and a versioned CAS in `src/blob.rs` and `src/s3.rs`. | object-log's sequencer conformance tests pass; two writers cannot both commit; the epoch fence works; 2 PUTs per commit at 1,000 and 10,000 streams; head bytes stay bounded; head bytes uploaded per second; commit p50 and p99 at Spike A's delays; open does GETs only, with no LIST. | Go or no-go for the partition cell. |
| **C. Turso partition snapshot** | Can a partition's Turso file be snapshotted while it serves, and how long does rehydration take? | New bench on the vendored Turso 0.7.2. | Whether a consistent copy works while the writer applies, and how long it stalls the writer; upload and download times at 1M and 10M records, and at the SPIKE-004 100M shape if feasible; tail replay rate, which gives the cadence formula and α. | Go or no-go for P1, and so for P3's export design. |

Proposed pass bars for review:
- Spike B: at most 2 PUTs and 0 GETs per steady-state commit; a head of at most 64 KiB plus references; commit p99 within 2× one conditional-PUT round trip at the same injected delay.
- Spike C: a writer stall per snapshot that stays inside the claim p99 budget, and rehydration of the largest planned partition inside TD-003's recovery budget.

The owner may replace these bars.

Partition sizing also takes SPIKE-004 Finding 9 as an input: with the lean claim row, 100 queues × 1M records in one Turso file claim within 4.3% of a single-queue 10M control. Files beyond that size are unmeasured.

## Indicative sequence after the spikes

This sequence is not authorized. No beads are filed until the owner accepts the spike results.

| Slice | Goal | Main files | Proof | Depends on |
|---|---|---|---|---|
| S0 | Stable server namespace (Finding 8). This is a bug fix and does not depend on this design. | `crates/fireweed-server/src/lib.rs:2859-2891` | A restarted server reads its earlier log. | – |
| S1 | Upstream `HeadSequencer` with versioned CAS; bump the Fireweed pin. | object-log repository; `crates/fireweed-objectlog/Cargo.toml` | Spike B's proofs as tests. | B |
| S2 | Partition cell, queue directory and router in the log store. | `crates/fireweed-objectlog/src/{log_engine_store.rs, compose_log.rs}`, new `partition.rs` | Open reads only owned partitions (count the GETs); 2 PUTs per commit at 1,000 queues; reopen works without epoch and high-water documents. | S1 |
| S3 | One Turso file per partition, with a router and lazy open. | The Turso composition, after the kernel contract moves it | Per-queue ops route to their partition; live reads match a rebuild. | S2 |
| S4 | Ownership by partition. Update ADR-008 and TD-003 first. | `crates/fireweed-engine/src/{control_plane.rs, ownership.rs}`, `crates/fireweed-postgres/src/control_plane.rs` | A handoff opens fresh, fences, hydrates, then serves; a stale owner's commit is rejected. | S2 |
| S5 | P1, only if accepted. | New snapshot module; `maintenance.rs`; the rebuild path | Storage stays bounded under constant load; kill-and-move restores state exactly. | S3, S4, C |
| S6 | Replace the FWSG cost model and regenerate TP-002. | `crates/fireweed-release/src/lib.rs:2700-2900`; docs | The economics table above reproduces from measured inputs. | A, B |

The critical path is B → S1 → S2 → S3. S3 and S4 touch different files, but both need the server crate, so `crates/fireweed-server/src/lib.rs` belongs to S0, S2 and S4 only. These slices will conflict with the kernel contract's execution slices, which move the Turso composition, so they are sequenced after them.

## Effect on other documents

Accepting the partition cell changes:
- **ADR-008 and TD-003:** the ownership lease, the epoch and the fencing key belong to the partition, not the queue, and TD-003's assignment function and authority-record key follow. The one-owner-per-queue ceiling stays.
- **TD-010:** the object-log serving protocol.
- **TP-002:** `docs/perf/tp002-objectlog-granularity-economics.md` is regenerated from a rewritten cost model.
- **P1 only:** ADR-024's recovery rule, ADR-001's snapshot wording, and TP-003 AC-TXN-1.

## Risks

| Risk | Prob | Impact | Mitigation |
|---|---|---|---|
| The head, or its CAS latency, grows with 10,000 queues in a partition. | M | H | Per-stream state lives in immutable runs; the head holds only a bounded tail; Spike B measures head bytes and commit latency. |
| A hot partition cannot shed queues, because there is no rebalancing. | M | H | Load-aware placement at creation; whole-partition moves; P2 if measured necessary; ADR-008's per-queue ceiling stays explicit. |
| A poor placement decision is permanent. | M | M | Admin override at creation; placement inputs come from measured load. The owner accepts this trade by ruling out rebalancing. |
| A partition's Turso file outgrows the measured shape, and claims slow from page-cache pressure (SPIKE-004, Finding 8). | M | H | The lean claim row, measured flat at 100 queues × 1M records per file (SPIKE-004, Finding 9); bounded queues and records per partition; more partitions per node. |
| Snapshot size and rehydration time exceed the recovery budget. | M | H | Spike C; partition size limits; P1 stays a proposal until it is measured. |
| Turso cannot produce a consistent copy while it serves. | M | H | Spike C evaluates checkpoint and copy, `VACUUM INTO`, and a backup API before P1 is accepted. |
| One engine per partition costs threads (up to 8 flush-runtime threads each, object-log `src/engine.rs:770-774`). | M | M | Tune `OBJECT_LOG_FLUSH_RUNTIME_THREADS`; keep partitions per node moderate. |
| This is a new storage generation with no in-place converter. | H | L | Fireweed is pre-production with no replay compatibility (API-006 §8); fail closed on the old layout, as `storage_generation.rs` does for FWSG. |
| A routing miss costs a directory GET. | L | L | Cache entries (the entry is immutable, and the close is enforced inside the partition); resolve at open for owned partitions. |
| Genesis rebuild is lost under P1. | – | H | Owner approval required (bead `fireweed-f500277e`); P1 stays a proposal until then. |
| Merge conflicts with the kernel contract's execution slices. | H | M | Sequence S2 to S4 after those slices land. |

## Open questions for the owner

1. **Partition ids.** How wide is the id space (the example uses 1–255), and is it fixed per namespace?
2. **Placement policy.** What is the default at `define_queue` (least-loaded, admin-directed, per tenant class)? Is partition → node explicit or HRW?
3. **P2 intent.** Should partition logs carry partition ranges from the start (so a split is needed), or should v1 keep one partition log per partition, with expansion done by whole-partition handoff?
4. **P1.** Accept losing genesis rebuild? Confirm K = 2 and α ≈ 1 as starting values.
5. **Linger.** 20 ms with its cost floor, or longer?
6. **Erasure.** Is time-bounded tenant or queue erasure required?
7. **Ownership of the code.** Should `HeadSequencer` go upstream in object-log (Fjord could reuse it) or stay in Fireweed?
8. **Directory.** Should the queue directory be create-only S3 objects (recommended: no new dependency, and the primitive already exists) or rows in the control-plane database?

## Review gate

This design authorizes Spikes A, B and C, which change no product code. It authorizes no source, schema, configuration or deployment change, and no implementation beads, until the owner has reviewed the spike results and accepted the partition cell. S0 is a bug fix that stands on its own and may be filed separately. P1, P2 and P3 each need their own acceptance after measurement.
