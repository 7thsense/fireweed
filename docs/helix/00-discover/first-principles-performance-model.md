---
ddx:
  id: discover-first-principles-performance-model
  type: analysis
  depends_on:
    - product-vision
    - prd
    - discover-foqs-scaling-distributed-priority-queue
  links:
    - {kind: informs, to: prd}
    - {kind: informs, to: tp-scale-substantiation}
    - {kind: informs, to: tp-fireweed-performance-matrix}
  review:
    self_hash: cda6f175ad5931d1307460863d730e5ca9ea8e4c9c247a5266386d4bcf8ccfdb
    deps:
      discover-foqs-scaling-distributed-priority-queue: 11d77dcbfdcdde0ee514d585bddf82f180527287199e365e87328d1d0b7e95a2
      prd: cd3004bd0dc9ac531d1cd2596e875e51c2de4601e330007fee60da1ea7b3d5ce
      product-vision: 745a023af9f66c4b71312a0271dbea18b3947970eb47e051d4312bb6222befeb
    reviewed_at: "2026-08-15T00:12:10Z"
---

# First-Principles Performance Model

- **Status**: discovery analysis; not a release SLA and not a public preview claim
- **Date**: 2026-08-14
- **Scope**: requirements-level capacity for a durable, batch-centric priority
  queue, derived from disk I/O, CPU, memory bandwidth, and network bandwidth
- **Out of scope**: storage layout, implementation hot-path choices, and
  host-specific TP-002 / TP-005 evidence

This note answers one question: given the *work* the product must do, what
throughput of what size is a reasonable **high-performance** single-node
benchmark once batching and concurrency are used to saturate hardware
primitives?

It does not replace the PRD rule that absolute rates are topology-bound
capacity observations. It does not authorize a portable pass/fail TPS gate.
It does not change public-preview claims. Fold any of the numbers below into
PRD, TP-002, or TP-005 only after an explicit product decision.

## Authority boundary

- Product requirements that define the *work*: [product-vision](./product-vision.md),
  [PRD](../01-frame/prd.md).
- Peer-system calibration, not a compatibility target:
  [FOQS](./foqs-scaling-distributed-priority-queue.md).
- Public preview remains "not a performance benchmark"
  ([public-preview-boundary](./public-preview-boundary.md)).
- Measured Fireweed numbers live in TP-002 / TP-005 evidence and are
  calibration only. They are not inputs to this model.

## 1. The work, not the design

The required primitive is a **mutable, leased, rank-ordered work item**,
not an immutable log record. Clients write and update items idempotently,
workers claim eligible batches under a single active lease, and claimed items
are settled. An acknowledged write is durable in the S3 object log (API-006
DUR1).
A node must keep a 10M-item hot queue live and exercise at least 1,000
concurrent queues. Horizontal scale is more queues on more owner nodes, not
a faster single logical stream.

That work is closer to FOQS than to Kafka. Kafka-class append rates are the
wrong ceiling.

### Units

Mixing units is how systems get fake "millions of TPS" claims.

| Unit | Meaning | Conversion |
|---|---|---|
| **Item-mutation** | One item changes state once (write, update, claim, or settle) | Primary capacity unit |
| **Request / batch** | One batch operation | `request_tps = item_tps / batch_size` |
| **Durable commit** | One fsync, WAL group-commit, or object PUT | `commit_tps = item_tps / items_per_commit` |
| **Completed item** | One item through ingest → optional update → claim → settle | `completions/s = item_tps / mutations_per_lifecycle` |

Seventh Sense's validation mix is about **3.5 item-mutations per completed
item** (1 write, 0.5 reschedules, 1 claim, 1 settle). Quote completions when
talking about application work; quote item-mutations when talking about the
engine.

### Record size

Capacity is set by the **durable encoded record**, not the field bytes alone.
Identity (key), stage, rank, lease/claim id, group, gates, version and framing
are ~256–512 B even when fields are tiny.

| Class | Fields | Encoded record | Role |
|---|---:|---:|---|
| **S** | 256 B | **~512 B** | Typical Seventh Sense / work ticket |
| **M** | 1 KiB | **~1.5 KiB** | Headline hot record |
| **L** | 16 KiB | **~17 KiB** | Large field values (FOQS capped its payload at 10 KiB) |

## 2. Reference host and primitive ceilings

"High performance" here means one **2026 commodity server**, not a laptop and
not a fleet.

**H-server:** 32 cores, 256 GB RAM, 2× Gen4 enterprise NVMe with power-loss
protection (PLP), 25 GbE, ~250 GB/s DRAM.

| Primitive | Honest sustained number | What it limits |
|---|---|---|
| fsync / FLUSH | ~100 µs → **10,000 commits/s** per log stream | Unbatched durable TPS |
| Sequential write | **3.5 GB/s** per drive (sustained, not spec-sheet burst) | Large records, high batch density |
| Random 4K write | **200–500K IOPS** | Page-oriented stores that do not pack records |
| CPU, rich path | **5–20 µs/item** → 50–200K item-ops/s/core | Validate, encode, stage / rank / lease indexes |
| CPU, cheap path | **1–3 µs/item** → 300K–1M/core | Append + hash, small item |
| DRAM bandwidth | **200–400 GB/s** | Almost never first unless copies explode |
| 25 GbE | **3.125 GB/s** | Request + response bytes |
| Object-store PUT | **20–80 ms**; hundreds to low thousands PUTs/s cheaply | Object log without group-commit |
| Remote DB sync commit | **1–5 ms** | Postgres-style durability per commit |

Consumer NVMe without PLP is a different machine: fsync is typically
**0.5–2 ms** → **500–2,000 commits/s**. Do not publish a high-performance
claim from that host.

Default write amplification until measured:

- Sequential log only: **1.5×**
- Log plus serving projection both on disk: **3×**
- 4 KiB pages, one item per page: **max(4096 / record, 1)**

Working set: 10M resident **M** records plus indexes is ~20–30 GB and fits
comfortably. 10M × 16 KiB is ~160 GB and still fits on H-server. One thousand
*hot* 10M queues does not; the requirement is 1,000 cold queues plus one hot
queue.

## 3. Closed-form model

```text
item_tps = min(
  commit_hz          * items_per_commit,
  disk_bw            / (record_bytes * write_amp),
  iops               * items_per_io,
  effective_cores    * core_item_hz,
  mem_bw             / mem_bytes_per_item,
  net_bw             / wire_bytes_per_item,
  in_flight_requests / rtt * items_per_request
)
```

Group-commit density:

```text
items_per_commit ≈ min(
  offered_item_tps × commit_latency_bound,
  max_batch,
  segment_bytes / record_bytes
)
```

Client concurrency (Little's law):

```text
in_flight_requests = request_tps * service_time
item_tps           = in_flight_requests / service_time * batch_size
```

A 1 ms same-AZ RTT with tens of in-flight batches of 100 already offers
100K+ item-mutations/s. The server binds on a primitive long before the
client runs out of concurrency.

A well-engineered system delivers **30–50% of the binding primitive**. The
haircut is serialization, kernel, checksums, copies, index maintenance, and
required telemetry — not optional slack.

### Worked binds on H-server (local NVMe log, write amp 1.5)

| Record | Unbatched (B=1) | Batched (B=100) | Binding resource at B=100 |
|---|---:|---:|---|
| **S (512 B)** | 10K (fsync) | **~1.2–1.6M** | CPU (16 effective cores) or 25 GbE |
| **M (1.5 KiB)** | 10K (fsync) | **~0.9–1.0M** | NIC or fsync × batch |
| **L (17 KiB)** | 10K (fsync) | **~140K** | Sequential disk |

DRAM bandwidth does not bind at these sizes.

Unbatched durable TPS is `commit_hz`. On PLP NVMe that is ~10K. That is a
fine latency benchmark and a terrible throughput benchmark. Batching and
concurrency are the same first-order lever: they convert commit latency into
item throughput until a byte-rate primitive (disk, NIC, or CPU) takes over.

The PRD commit-latency dial is this equation. It is a cost/latency tradeoff,
not a correctness knob.

| Commit bound | Incoming 10K items/s, 1 KiB | Items/commit | Durable item-TPS if commit_hz allows |
|---:|---|---:|---|
| 0 (per request) | B=1 | 1 | **10K** |
| 1 ms | | 10 | **100K** |
| 10 ms | | 100 | **1M** (then CPU/NIC bind) |
| 50 ms (object log) | | 500 | PUT rate × 500 |

### Serial section on one hot queue

Concurrency has two layers:

1. **Client in-flight batches** hide RTT.
2. **Queue parallelism** hides a serialized per-queue critical section.

If one queue's exclusive mutation is 10 µs, that queue caps at **100K
item-mutations/s** no matter how fast the disk is. The 1,000-queue density
requirement is how a node uses 32 cores. A single 10M hot queue only hits
the headline bar if that exclusive section stays in the low microseconds.

High performance on one hot queue is therefore a **serial-section budget**,
not a disk spec-sheet number. Claim selection must stay O(log n) at 10M
resident items; a scan is a 100 ms–class failure.

## 4. What "high performance" means for this product class

| System | Typical published number | Why it is the wrong ceiling |
|---|---|---|
| Kafka / Redpanda broker | 100K–1M+ msgs/s | Immutable sequential append; no per-item lease or priority mutation |
| Redis Streams | 100K–1M ops/s | Often `everysec` or no fsync |
| SQS FIFO | 3,000 msgs/s/queue (300 without batching) | AWS published FIFO bar |
| Postgres / PGMQ-style | 1–10K durable row-TPS unbatched | Chatty SQL |
| FOQS (Meta, 2021) | ~1e12 items/day fleet-wide; hot topics up to **10M+/min ≈ 167K/s** | Hyperscale fleet on sharded MySQL |

FOQS's **hot topic** rate is the right peer: a single busy priority queue
near 10⁵ items/s. Fleet-wide millions/s is a horizontal-scale claim (queues
across owner nodes), not a single-node benchmark.

### High-performance region on H-server

Every acknowledged write waits for the object-log commit (API-006 DUR1). A
commit is one data-object PUT plus one head compare-and-swap for the whole
partition, cut at the engine linger L (TD-018). A busy partition therefore
costs about 2/L PUT/s however many items it carries, and write latency is
about L plus two conditional-PUT round trips.

| Durability | Size | Batch | High-performance bar | Completions/s at 3.5 mut/item |
|---|---|---:|---:|---:|
| S3 object log, 20 ms linger | **M (1 KiB)** | **100** | **50–200K item-mutations/s per node** | **15–60K** |
| S3 object log, 20 ms linger | S (512 B) | 100–1000 | 100–300K per node | 30–85K |
| S3 object log, 20 ms linger | L (16 KiB) | 100 | 20–50K per node | 6–15K |
| S3 object log, 100 ms linger | M | 100 | same byte binds; 20 PUT/s per busy partition instead of 100 | same |

### Headline benchmark

The one number that deserves to be called high-performance for this product:

> **H-server, S3 object log at the 20 ms default linger (TD-018), 1 KiB
> items, batch 100, mixed lifecycle 1 : 0.5 : 1 : 1**
>
> **50,000 completed items/s ≈ 175,000 durable item-mutations/s per node**
>
> batch p99 ≤ 250 ms (the linger plus one data PUT and one head CAS),
> telemetry on, 10M items already resident.

Why that number:

- It only happens if the linger packs thousands of mutations into each
  commit: at 20 ms, ~3,500 item-mutations (~5 MiB) share one data PUT and
  one head CAS, so a busy partition stays near 2/L = 100 PUT/s (TD-018).
- ~15× a typical chatty Postgres queue; ~15× SQS FIFO's per-queue bar.
- Same order as a FOQS hot topic.
- Well under the H-server byte binds for size M (§3). On the object log,
  commit latency and the PUT budget set the bar, not bytes.
- A 10M hot queue drains in ~200 s; 10M ingest at 200K/s is ~50 s.
- It covers the 10–50K completions/s that is adequate for Seventh
  Sense-like peaks.

Request rate at that point is modest: **1,750 batch-TPS**. At a ~150 ms
commit, Little's law needs ~260 batches in flight. The scarce resources are
bytes, commits, and the per-queue serial section — not RPCs.

### What is not a reasonable high-performance claim

- 1M durable commits/s (each commit is a data PUT plus a head CAS).
- 1M/s of 16 KiB records (25 GbE forbids it).
- S3-backed 100K/s at a 5 ms durability bound (PUT RTT forbids it).
- Single-connection chatty SQL at 100K/s (round trips forbid it).

## 5. The small matrix that proves the model

Do not publish a single TPS cell. A high-performance *claim* on a declared
host needs these six runs. Anything else lets an unbatched or non-durable
number impersonate the product.

| Run | What it proves | Pass-as-high-performance on H-server |
|---|---|---|
| B1 / S / durable | commit path is healthy | p99 within the linger plus two conditional-PUT round trips |
| **B100 / M / durable mixed lifecycle** | **Headline** | **≥ 50K completions/s**, p99 ≤ 250 ms |
| B1000 / S / write-only | group commit packs the linger | **≥ 200K** ingest/s |
| B100 / M / 10M resident | indexes stay O(log n) | ≥ 80% of headline |
| B100 / L / durable | bandwidth honesty | **≥ 20K** mut/s |
| 1,000 cold + 1 hot | density / noisy neighbor | hot ≥ 80% of single-queue headline |

Every run reports **items/s and PUTs/s** at the stated linger. A second
linger (for example 100 ms) shows the cost/latency dial; its lower PUT rate
and higher latency are the dial, not a failed high-performance claim.

## 6. Implications for requirements (not yet folded)

If maintainers later want a declared-host capacity envelope, this model
suggests four statements. None of these are in force until PRD / TP-002 /
TP-005 are explicitly updated.

1. **Primary envelope:** 1 KiB / batch 100 / 50K completions/s / H-server /
   S3 object log at a 20 ms linger.
2. **Not-dead floor:** unbatched durable ≥ 1K mut/s, plus the existing
   portable TP-002 correctness / 10M-resident / 1,000-queue envelope.
3. **Horizontal envelope:** more queues on more owners. Do not promise 100K
   completions/s on one logical stream without stating the serial-section
   budget.
4. **Object-log envelope:** publish the linger curve: latency, throughput
   and PUTs/s at each linger.

Seventh Sense "millions resident, sub-second client latency" is easy at
these numbers. Sub-second is a latency SLO (p99 of a B100 call ≤ 250 ms).
Millions resident is a working-set problem (tens of GB), not a TPS problem.
The scarce resources for that workload are **claim/index CPU on one hot
queue** and **durable commit batching**.

## Sources

- Product work definition: `docs/helix/00-discover/product-vision.md`,
  `docs/helix/01-frame/prd.md`.
- FOQS scale and item shape: Meta Engineering, 2021-02-22,
  <https://engineering.fb.com/2021/02/22/production-engineering/foqs-scaling-a-distributed-priority-queue/>
  (~1e12 items/day; topics from tens/min to 10M+/min; payload up to 10 KiB).
- Enterprise NVMe + PLP fsync: tens of microseconds to low hundreds of
  microseconds on capacitor-backed drives; consumer NVMe FLUSH typically
  0.5–2 ms. See e.g. Small Datum, 2026-01,
  <http://smalldatum.blogspot.com/2026/01/ssds-power-loss-protection-and-fsync.html>.
- Hardware ceilings are conservative sustained values for a 2026 dual-Gen4
  NVMe 32-core server, not manufacturer peak IOPS slides.
