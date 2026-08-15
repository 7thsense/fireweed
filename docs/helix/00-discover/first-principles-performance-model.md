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
- **Out of scope**: current Fireweed storage-cell design, implementation
  hot-path choices, and host-specific TP-002 / TP-005 evidence

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

The required primitive is a **mutable, leased, priority-ordered work item**,
not an immutable log record. Clients push and update items idempotently,
workers claim eligible batches under a single active lease, and claimed items
are finalized. Durability after process death follows the selected log class.
A node must keep a 10M-item hot queue live and exercise at least 1,000
concurrent queues. Horizontal scale is more queues on more owner nodes, not
a faster single logical stream.

That work is closer to FOQS than to Kafka. Kafka-class append rates are the
wrong ceiling.

### Units

Mixing units is how systems get fake "millions of TPS" claims.

| Unit | Meaning | Conversion |
|---|---|---|
| **Item-mutation** | One item changes state once (push, update, claim, or finalize) | Primary capacity unit |
| **Request / batch** | One batch operation | `request_tps = item_tps / batch_size` |
| **Durable commit** | One fsync, WAL group-commit, or object PUT | `commit_tps = item_tps / items_per_commit` |
| **Completed item** | One item through ingest → optional update → claim → finalize | `completions/s = item_tps / mutations_per_lifecycle` |

Seventh Sense's validation mix is about **3.5 item-mutations per completed
item** (1 push, 0.5 reschedules, 1 claim, 1 finalize). Quote completions when
talking about application work; quote item-mutations when talking about the
engine.

### Record size

Capacity is set by the **durable encoded record**, not the JSON payload.
Identity, lease, priority, `not_before`, `group_key`, versions, and framing
are ~256–512 B even when the payload is tiny.

| Class | Payload | Encoded record | Role |
|---|---:|---:|---|
| **S** | 256 B | **~512 B** | Typical Seventh Sense / work ticket |
| **M** | 1 KiB | **~1.5 KiB** | Headline hot record |
| **L** | 16 KiB | **~17 KiB** | Large job blob (FOQS capped payload at 10 KiB) |

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
| CPU, rich path | **5–20 µs/item** → 50–200K item-ops/s/core | Validate, encode, priority / eligibility / lease indexes |
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
the headline bar if that exclusive section stays in the low microseconds, or
if bounded-relaxed ordering lets more than one core work the same queue.

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

| Durability | Size | Batch | High-performance bar | Completions/s at 3.5 mut/item |
|---|---|---:|---:|---:|
| Local NVMe, group-commit | **M (1 KiB)** | **100** | **300–500K item-mutations/s** | **85–140K** |
| Local NVMe, group-commit | S (512 B) | 100–1000 | 400–800K | 115–230K |
| Local NVMe, group-commit | L (16 KiB) | 100 | 50–80K | 15–23K |
| Local NVMe, **unbatched** | any | 1 | 5–10K | 1.5–3K |
| Object log (S3), 50–500 ms commit bound | M | fill MB segments | 50–200K / node (PUT-rate × items/PUT) | 15–60K |
| Object log, 5–10 ms bound | M | small segments | 100–1,000 / queue | tens–hundreds |
| Remote Postgres, batched | M | 100, pipelined | 30–150K | 10–40K |
| Memory / Class B serving | S/M | 100+ | **1M+** | 300K+ |

### Headline benchmark

The one number that deserves to be called high-performance for this product:

> **H-server, Class A local log, 1 KiB items, batch 100, mixed lifecycle
> 1 : 0.5 : 1 : 1**
>
> **100,000 completed items/s ≈ 350,000 durable item-mutations/s**
>
> batch p99 ≤ 10 ms, telemetry on, 10M items already resident.

Why that number:

- ~35× the unbatched fsync floor, so it only happens if group-commit and
  concurrency work.
- ~30–50× a typical chatty Postgres queue; ~30× SQS FIFO's per-queue bar.
- Same order as a FOQS hot topic.
- ~30–40% of the H-server bind for size M — ambitious, not magical.
- A 10M hot queue drains in ~100 s; 10M ingest at 500K/s is ~20 s.
- Far more than a SaaS scheduler typically needs (10–50K completions/s is
  already adequate for Seventh Sense-like peaks). 100K is headroom.

Request rate at that point is modest: **3,500 batch-TPS**. The scarce
resources are bytes, commits, and the per-queue serial section — not RPCs.

### What is not a reasonable high-performance claim

- 1M durable fsync-per-item TPS (commit_hz forbids it).
- 1M/s of 16 KiB records (sequential disk and 25 GbE forbid it).
- S3-backed 100K/s at a 5 ms durability bound (PUT RTT forbids it).
- Single-connection chatty SQL at 100K/s (round trips forbid it).
- Comparing Class B memory 1M+/s to Class A durable as if they were the
  same product.

## 5. The small matrix that proves the model

Do not publish a single TPS cell. A high-performance *claim* on a declared
host needs these six runs. Anything else lets a memory-only or unbatched
number impersonate the product.

| Run | What it proves | Pass-as-high-performance on H-server |
|---|---|---|
| B1 / S / durable | fsync path is healthy | 5–10K mut/s, p99 **< 1 ms** |
| **B100 / M / durable mixed lifecycle** | **Headline** | **≥ 100K completions/s**, p99 ≤ 10 ms |
| B1000 / S / push-only | group-commit + sequential write | **≥ 500K** ingest/s |
| B100 / M / 10M resident | indexes stay O(log n) | ≥ 80% of headline |
| B100 / L / durable | bandwidth honesty | **≥ 50K** mut/s |
| 1,000 cold + 1 hot | density / noisy neighbor | hot ≥ 80% of single-queue headline |

Object-log cells report **items/s and PUTs/s** at two commit bounds (for
example 50 ms and 500 ms). Those numbers will be lower; that is the
cost/latency dial, not a failed high-performance claim.

Class B / memory 1M+/s is a serving-path ceiling, useful as an upper bound,
not a durability claim.

## 6. Implications for requirements (not yet folded)

If maintainers later want a declared-host capacity envelope, this model
suggests four statements. None of these are in force until PRD / TP-002 /
TP-005 are explicitly updated.

1. **Primary envelope:** 1 KiB / batch 100 / 100K completions/s / H-server /
   Class A local log.
2. **Not-dead floor:** unbatched durable ≥ 1K mut/s, plus the existing
   portable TP-002 correctness / 10M-resident / 1,000-queue envelope.
3. **Horizontal envelope:** more queues on more owners. Do not promise 100K
   completions/s on one logical stream without stating the serial-section
   budget.
4. **Object-log envelope:** publish the commit-bound curve. Do not put S3 on
   the same chart as NVMe group-commit.

Seventh Sense "millions resident, sub-second client latency" is easy at
these numbers. Sub-second is a latency SLO (p99 of a B100 call in 5–20 ms).
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
