---
ddx:
  id: tp-verification-acceptance-criteria
  depends_on:
    - prd
    - api-fireweed-kernel-contract
    - adr-kernel-records-contract
    - adr-rust-workspace-and-toolchain-policy
    - adr-full-async-storage-boundaries
    - adr-async-commit-strategy-and-dispatch
    - adr-turso-derived-projection
    - adr-s3-turso-public-cell
    - td-sharding-and-shard-ownership
    - td-object-log-turso-projection
    - td-kernel-projection-storage-layout
    - td-partitioned-object-log
    - tp-governing-test-traceability
    - tp-scale-substantiation
    - spike-record-key-storage
    - spike-record-field-storage
    - spike-claim-path-at-scale
    - spike-turso-value-size-boundary
    - spike-declared-field-storage-layout
  status: accepted
  review:
    self_hash: c211fd97e18d7693ab736fb6a81ba3a73c8a3772bf22e0d317b8d6f2ea7d7fcc
    deps:
      adr-async-commit-strategy-and-dispatch: 6daa55d01fce58248b5b607c3015ed0600d23ff123912e2bc1fd63a484a8ab49
      adr-auth-tenancy-and-storage-isolation: 1d4296498a187d4d4c3bb4a4e37647f7193036c3a20ab2a8b66154a45937ece2
      adr-cqrs-log-projection-storage-model: 63ed2521bc7d0e785529aafbd179b3ef22d51cbf3897d51c511540be52ee9ba3
      adr-full-async-storage-boundaries: 0543121229a415143387307275263908017b43697ddac970d54d6d30a2c7ccaa
      adr-granularity-mapping-and-claim-domain: c44740df25ed32569e86f4c3459ed07839d33325367cdafc49fb1317deab606d
      adr-queue-as-shard-unit-and-projection-families: 64a7c7b0e2e5f4caa2c7d775b84c87a9a1e4484ae3df9dccbe3d145d22681a7e
      adr-rust-workspace-and-toolchain-policy: df18055fca75337c9dc5e7e099d14afd44428e4883a0784af0a0c1e2192e98f6
      adr-turso-derived-projection: b93a1a9c4ba242940b86878551dddd35f9aa4e399357417c620e66f5ab2a7b67
      api-native-client-interface: b99403ef55afffd134ac3ef1a71065c497558c94de379c2b257b119000a0f488
      api-operator-repair-contract: f4fdf0e4e8e29431d8ff2f89a8bc843497dca743ad5e32439996a4ad7c611197
      prd: cd3004bd0dc9ac531d1cd2596e875e51c2de4601e330007fee60da1ea7b3d5ce
      td-object-log-turso-projection: d05ba185fd6e0be01855a97f39abbce04a486adf4a6f2c69f14e236befecc25f
      td-postgres-native-reference-mode: 4088399aca0beb5840c4ace629039ad655bf4ff928bc22f86072b94857bdbd3e
      td-resp-wire-adapter: 351f429307d19a5b4ba3c3e39e9d9168e999a5b067b07d15f67636e5fd5d8040
      td-s3-object-log-sqlite-projection-mode: 7770bb133f4ace189bfc715e3be6472f894f7c62d52adfc051540fea97c6a4b2
      td-sharding-and-shard-ownership: f7309199e3810447398caa11f2f0241ce05c035e9b9d0241aada4ad3759582e1
      td-storage-architecture-backend-contracts: 2d88d342aac82f23616fdff6d94f4ac88701ab6e70c80a0315003c5e66432c74
      tp-governing-test-traceability: 61cbc8becaacf00f3925f5d282cf33dec77e01e07c0bd077d93a3a2f05cc6d67
      tp-scale-substantiation: 23f20e8dab88330e4ddd165a0d2230151b7ef0f99ca16c016671558ed5719686
    reviewed_at: "2026-08-04T04:50:53Z"
---

# Test Plan: TP-003 Verification and Acceptance Criteria

## Scope

TP-001 says **what** to test and which suite covers it. TP-002 sets the scale
bars. This plan makes correctness **quantifiable**. It turns each requirement
into a build-gating **acceptance criterion** (`AC-*`) with concrete inputs and
a pass bar, names the **safety invariants** (`INV-*`) that stress and property
tests hold to zero violations, and defines the **CI quality gates** a build
must turn green. A feature or bead is verified only when every acceptance
criterion it claims is green and recorded in the verification ledger (§6).

The contract under test is API-006, the kernel records contract. Fireweed has
one implementation of it: the S3 object log × Turso projection (ADR-024),
laid out by TD-017. The object log is the durable copy, and the Turso
projection is rebuilt from it through projection control on the admin handle.

Four rules govern every criterion:

- **One black-box suite qualifies the contract.** `fireweed-contract-suite`
  runs in-process against `Arc<dyn Store>` through
  `run_all(constructor, reopen, Δ, clock, fault_hook)` (API-006 CS1). It is the
  functional qualification of every `Store` implementation. Implementation
  suites, product workflows and consumer tests add evidence; they never
  replace, skip or weaken it.
- **No backend knobs.** No criterion branches on backend, feature set,
  capability or backend identity (CS4). Every implementation passes or fails
  the same scenarios.
- **Operator ban.** Only the operator suite reaches the `Operator` trait or the
  admin handle. A CI guard fails the build when Snorri, the workload or loadgen
  crates, or the contract suite reference either one (OP4, CS5).
- **Portable pass bars.** A pass bar is an exact outcome, a structural check
  (query plan, statement trace, schema shape) or a same-run ratio. Absolute
  rates and latencies are capacity evidence for the host and topology that
  produced them (TP-005, TP-002 E3), never a pass bar here.

## 1. Quantification Conventions

| Symbol | Meaning | Standard values |
|--------|---------|-----------------|
| `C` | concurrent workers or callers | {1, 8, 64, 256} |
| `T_smoke` / `T_soak` | stress durations | 60 s (PR) / 600 s (nightly) |
| `S` | resident record count | {10K, 1M, 10M} |
| `K_q` | concurrently active queues | {10, 100, 1000} |
| `N_kill` | induced worker or process kills in a run | ≥ 1000 |
| latency | reported percentiles | p50, p95, p99, p999 |
| `props` | property-test cases per property | ≥ 10,000 (PR), ≥ 1,000,000 (nightly) for the encoders and ordering |
| `fuzz` | fuzz time per target | ≥ 10 s (PR), ≥ 30 min per target (nightly) |
| seed | randomized tests | seeded, recorded RNG; a failing seed becomes a fixed regression case |
| `Δ` | the instance's documented lease-reclaim bound (API-006 V4) | at most 5 s, measured on the injected `Clock` |

Every benchmark and stress result is reported **with telemetry enabled** and
records the command, exit status, environment and seed (§6).

## 2. Safety Invariants (zero-violation bars)

These hold on S3×Turso under the stress matrix (`C=256`, `S∈{1M,10M}`,
`T_soak`, `N_kill≥1000`, skewed rank and group distributions). The violation
count MUST be **0**.

| ID | Invariant | Quantified check |
|----|-----------|------------------|
| INV-1 | One live claim per record (C6) | Records observed attached to two live claims at once = **0** over the run. |
| INV-2 | No lost work (DUR1) | After `N_kill` crashes, every acknowledged record is in the stage, rank and field state of its last acknowledged write, or deleted by an acknowledged write; lost records = **0**. |
| INV-5 | Request-id replay (RQ1–RQ4, C8) | A resend of a `write`, `claim` or `force_clear` with the same `request_id` and fingerprint within R that is re-evaluated, or whose response breaks RQ2 (C8 for a claim) = **0**. |
| INV-6 | Claim order (C4, ORD2, C5a) | Claim results out of `(rank, insertion sequence)` order within the stage = **0**; on a quiet queue, claims that do not return the first candidates = **0**. |
| INV-7 | Group atomicity (C4, C6a, C7) | `WholeGroups` results that include a group with a Leased member in any stage, or that split a group outside the `partial_group` case = **0**. |
| INV-8 | Queue isolation (ISO1) | Ops that observe or change another queue, and tokens accepted on another queue = **0**. |
| INV-9 | A queue is never split (ADR-025 §5) | Queues whose records, claims or log entries span more than one partition or owner = **0**. |
| INV-10 | Durable acknowledgement (DUR1) | Acknowledged effects missing after crash and reopen = **0** (kill after acknowledgement). |
| INV-11 | Force-clear fences its record (OP2) | A force-cleared record that still accepts the claim's token, or a force-clear that invalidates the token for the claim's other records = **0**. |
| INV-12 | Success visibility (W12, V2, V3) | An acknowledged write that a later `read` or `scan` does not observe, or a claim that returns a record which was not a candidate under every acknowledged write = **0**. |
| INV-13 | Rejection has no effect (E1, W2) | A request rejected with an E1 error, or a rejected unit, with any durable effect after reopen = **0**. |
| INV-14 | An unknown outcome resolves once (E2, RQ2, RQ4) | Resolving an `Unavailable` request, by resend with its `request_id` or by `read`, that yields more than one applied effect = **0**. |

## 3. Acceptance Criteria by Area

Each `AC-*` has Setup (quantified), Assertion and Pass bar. A bead cites the
`AC-*` it turns green. Suites are named in TP-001.

### 3.1 Contract properties (`fireweed-contract`)

| AC | Setup | Assertion | Pass bar |
|----|-------|-----------|----------|
| AC-CORE-3 record keys and request ids | `props ≥ 10,000` over generated keys (1..=256 B, non-UTF-8 included), `Expect` values and request bodies | `Expect::Absent` on a present key reports `Exists`; `Expect::Any` upserts; a resend with the same `request_id` and fingerprint returns the recorded outcome with `replayed: true`; a different fingerprint returns `RequestIdConflict` (W3, W5, RQ2) | 0 violations |
| AC-CORE-5 ESF index-key encoder | `props` over every field type and the built-ins `Stage`, `Rank` and `Group`, both directions | The byte order of `esf_index_key` equals API-006 §4.0 value order: `Str` and `Bytes` bytewise, `Float` numeric with `-0.0` normalized, stages by declaration position; decoding round-trips; a key over 1024 B is rejected at the same boundary as LIM1 | 0 mismatches over 100% of generated pairs |
| AC-CORE-6 contract crate | The `fireweed-contract` crate, its public items and golden vectors for `fingerprint` and `canonical_len` | Every public item cites an API-006 clause id (the clause manifest); the crate has no I/O dependency; the golden vectors reproduce on every build | 0 uncited items; 0 golden-vector changes without a versioned encoder change |

### 3.2 Claims and leases

| AC | Setup | Assertion | Pass bar |
|----|-------|-----------|----------|
| AC-CLAIM-1 one live claim | `C=256`, `S=1M`, `T_soak` | INV-1 | 0 double leases |
| AC-CLAIM-2 lease expiry | `N_kill≥1000` holder kills mid-lease | Every record still attached to an expired claim becomes Free within Δ (V4), keeps its stage, rank, insertion sequence and `attempts`, and returns to its place in claim order; token use at or after `until` gets `StaleLease` | 0 lost records; 0 records still Leased Δ after `until`; 0 token uses accepted after `until` |
| AC-CLAIM-3 claimability | Records with `rank > horizon`, records carrying a Blocked gate, and a Blocked `"*"` gate | `Next` and `WholeGroups` never return them (C1); a later horizon returns a future-ranked record once it reaches its rank; `Keys` ignores the horizon but reports `Blocked` (C2) | 0 violations |
| AC-CLAIM-4 claim order | `C=64`, skewed ranks with long runs of equal rank | INV-6 | 0 inversions of `(rank, insertion sequence)` |
| AC-CLAIM-6 claim renewal | `Extend` at 1 ms and 1 h on live, expired and emptied claims, and in units that release some of the claim's records first | `Extend` sets `until` = now + d for every record still attached, keeps the token and changes no version; a stale token, an expired claim, or a claim whose last record an earlier write of the request detached gets `StaleLease` at `At::Extend(i)` (W2a, W14) | 0 stale extends accepted; 0 double leases; reported `until` equals now + d |

### 3.3 Groups and gates

| AC | Setup | Assertion | Pass bar |
|----|-------|-----------|----------|
| AC-GRP-1 whole groups | `WholeGroups { max_groups: 300 }` over ≥ 1000 groups of 1 to 10 records (the Marketo lead shape), grouped records in two stages, `C=64` | Each claim returns at most 300 groups and `max_records` records, every candidate of each group it takes, in first-candidate order; it stops before a group that would not fit, and only an oversized first group returns as a prefix with `partial_group`; no returned group has a Leased member in any stage (C6a); ungrouped records never return | INV-7 = 0; 0 split groups outside `partial_group` |
| AC-GATE-1 constant-cost gate flip | `S=10M`; flip a gate that `G` groups' records carry | The gate op writes no record row and changes no version (W9); the next claim observes it (V2); work and latency do not grow with `G` | 0 record-row writes; constant statement count; largest-`G` p99 ≤ 1.25× the interleaved smallest-`G` control |
| AC-GATE-2 gated records | Records carrying Blocked gates beside Open ones | A record with a Blocked gate is never claimed by `Next` or `WholeGroups`; `Keys` reports it `Blocked`; `stats.groups` still counts it, because S2 ignores gates | 0 violations |

### 3.4 Queue isolation and lease tokens

| AC | Setup | Assertion | Pass bar |
|----|-------|-----------|----------|
| AC-SEC-1 queue isolation | Negative matrix over two tenants × two queues with equal names: every `Store` op, `Extend`, and `Held` and `Release` with tokens from each queue | ISO1: no op observes or changes another queue; a token used on another queue gets `StaleLease` | INV-8 = 0 |
| AC-SEC-2 token handling | Inspect the Turso file, the object log, logs, and every `read`, `scan`, `stats` and `aggregate` response | Tokens are stored only as hashes, in the claim's log entry and the claims table (TD-017 §3); `read`, `scan`, `stats` and `aggregate` never return one (L1); no log line contains one | 0 plaintext tokens |

### 3.5 Partition ownership

| AC | Setup | Assertion | Pass bar |
|----|-------|-----------|----------|
| AC-OWN-2 fence and handoff | Move a partition to another node under `C=64` load on its queues; `N_kill≥100` mid-drain | One live claim per record across the handoff (INV-1); a deposed owner's commit with a stale epoch is rejected; the new owner recovers every acknowledged effect of every queue in the partition from the object log; live tokens stay valid across the handoff (TD-003, TD-017 §1) | 0 stale-epoch commits; 0 double leases; 0 lost acknowledged effects |

### 3.6 Operator under load

| AC | Setup | Assertion | Pass bar |
|----|-------|-----------|----------|
| AC-OP-1 force-clear fences one record | `force_clear` on Leased records, with and without `then`, while `C=64` holders keep settling and extending | OP2: the cleared record is detached with a new version, and its holder's `Held` or `Release` on it gets `StaleLease`; the claim's other records stay attached and the token stays valid for them; `then` applies as a lease-free write | INV-11 = 0 |

### 3.7 Portable degradation gates

Run on S3×Turso under `C=64`, `S=1M`, telemetry on, with interleaved same-run
controls. Absolute throughput and percentiles are reported with the host and
topology as capacity evidence. Full scale, density and horizontal coverage is
TP-002 E0–E3.

| AC | Operation | Pass bar |
|----|-----------|----------|
| AC-LAT-1 | batch write, claim and settle | exact operation counts and monotonic progress; loaded throughput ≥ 0.80× and p99 ≤ 1.25× the interleaved identical-work control; declared resource bounds hold |
| AC-LAT-2 | gate op | 0 record-row writes and constant logical work; largest affected-group p99 ≤ 1.25× the interleaved smallest-group control |
| AC-LAT-4 | telemetry overhead | core-op p99 with telemetry on ≤ 1.15× telemetry off |

### 3.8 Turso projection qualification

These criteria qualify the local Turso 0.7 ordinary-WAL projection. Remote,
sync, embedded-replica and MVCC modes are outside it.

| AC | Setup | Assertion | Pass bar |
|----|-------|-----------|----------|
| AC-TURSO-1 schema and initialization | Open a fresh file; exercise the known `execute_batch` PRAGMA trap, then apply each setting on its own | The TD-017 §12 settings read back (4 KiB page, WAL, synchronous NORMAL, the checkpoint cap, busy timeout 5 s), and the TD-017 schema is created exactly | the trap's partial effect is proven; every setting reads back; 0 schema mismatches |
| AC-TURSO-2 apply and reopen | Apply generated histories of every `Store` op, with rollback injection, then close and reopen | The state after reopen equals the state before it | 0 mismatches in records, claims, gates, counts, indexes, metadata or request records |
| AC-TURSO-3 rebuild from the object log | Overlap, gap, a crash after log commit and before apply, and loss of the Turso file | Overlapping replay is idempotent; a gap fails closed; the projection cursor never leads its rows; a rebuild through projection control reproduces the state exactly (TD-017 §1) | 0 mismatches after rebuild; every gap fails closed |
| AC-TURSO-4 async cancellation and concurrency | AC-TXN-8 and AC-TXN-9, plus 16 concurrent callers on disjoint keys and on one key | Exactly one winner on the shared key; every disjoint write is present | 0 reactor stalls, lost waiters, duplicate outcomes or unrecoverable acknowledged state |
| AC-TURSO-6 focused CI lane | A path-filtered lane over the Turso adapter | The lane runs the adapter and plan tests on every change to the Turso crate | lane green |

### 3.9 Durability and error semantics under duress

The contract suite reaches crash and unknown-outcome cases only through
`reopen` and its post-commit fault hook. These criteria verify that S3×Turso
keeps API-006's durability, visibility and error semantics (DUR1, V2, V3, E1,
E2, RQ1–RQ4) under real crashes, faults and cancellation. An ignored test, a
missing fixture, `n/a`, or a successful process with no assertions is a
release failure; there are no silent skips. Evidence binds the implementation
revision, test revision, command, exit status, assertion count and artifact
digest.

| AC | Setup | Assertion | Pass bar |
|----|-------|-----------|----------|
| AC-TXN-1 success is durable and visible | Run each mutating op (`define_queue`, `write`, `claim`, `delete_queue`, `force_clear`); kill and restart; rebuild the Turso projection from the object log through projection control | DUR1, INV-10, INV-12. The object log is the durable copy; the Turso file never is | 0 lost acknowledgements |
| AC-TXN-2 rejection has no effect | Envelope-invalid requests, rejected units (W3, W4, W13), `Busy` and capacity paths; restart and replay | INV-13. Applied sibling units keep their effects (W2) | 0 durable effects from rejected requests or units |
| AC-TXN-3 unknown outcome | Drop responses, time out callers, kill processes, and resend each `request_id` at these cut points: before append, after append and before commit, after commit and before apply, after apply and before response, after response | INV-5, INV-14. A resend returns the recorded outcome, or evaluates afresh when nothing committed; an id-less write resolves by `read` (RQ4) | 0 duplicate effects |
| AC-TXN-4 object-log crash points | For each linger setting of TP-002 E3, inject failures before the data-object write, after it and before the conditional commit, during the conditional commit, after commit and before projection apply, during apply, after apply and before response, and during a partition handoff | INV-1, INV-2, INV-10, INV-12, INV-14 | 0 lost acknowledged effects; 0 double leases; committed commands replay exactly once; orphan data objects ignored; stale-epoch commits rejected; a provider without conditional writes fails construction before I/O |
| AC-TXN-5A internal apply barrier | Inject apply lag, out-of-order scheduler wakeups, a durable apply failure, crashes on both sides of the response, and apply debt beyond its configured bound | Success is durable on the object log; apply is ordered; a failed apply fails closed; `read` and `scan` observe every acknowledged write (V3); a claim never returns a non-candidate (V2), and omits candidates only within C5a | 0 read-after-success gaps; 0 out-of-order applies; 0 C5a violations |
| AC-TXN-6 model conformance under duress | Generated operation histories with the AC-TXN-4 failure schedule, compared with an independent model of API-006 | Final records, claims, gates, metadata, request records and stage counts match the model | 0 divergences |
| AC-TXN-7 linger is not a correctness knob | Repeat AC-TXN-1 to AC-TXN-6 across the TP-002 E3 linger sweep | Invariants do not change with the latency or cost setting | 0 invariant deltas |
| AC-TXN-8 cancellation | Drop a caller's future before append, after staging and before commit, during commit, after the durable append and before apply, and while waiting for the writer; resend with the same and a conflicting `request_id` | ADR-015 cancellation: pre-commit cuts leave no durable effect; a cancelled commit resolves to exactly one outcome; a conflicting resend gets `RequestIdConflict` | 0 stranded waiters; 0 poisoned locks; 0 duplicate outcomes |
| AC-TXN-9 non-blocking runtime | Slow I/O injected into the S3 and Turso adapters on a single-thread Tokio runtime with a heartbeat | ADR-015 adapter boundary | heartbeat within its scheduling tolerance; 0 worker stalls |
| AC-TXN-10 lock and bridge structure | Source search over the storage paths, plus the dependency guard | ADR-015 structure | no `std::sync::MutexGuard` held across an await; no nested runtime or `block_on` bridge; blocking work offloads whole transactions |
| AC-TXN-11 commit dispatch | Cancel a caller after its commit is submitted; stall one queue at each mutation phase while another queue runs | ADR-017 dispatch | a submitted commit resolves exactly once; a stalled queue does not stop another queue; 0 stranded permits |
| AC-TXN-12 byte admission | Small, target-sized and oversize requests under a stalled store, fencing, caller drops, close and drain; hot and cold tenants and queues | ADR-017 byte admission | global and per-tenant permits return to zero after drain; charged bytes never exceed caps; an oversize request is `TooLarge` (E3); exhaustion is `Busy`; a cold tenant makes progress |

### 3.10 Product workflows

These gates prove that the primitives compose into the product's workflows.
Each runs through `Fireweed` over `Arc<dyn Store>` on S3×Turso with telemetry
on. No workflow replaces the contract suite; they sit above it. Commands take
`FIREWEED_E2E_SCALE=smoke|release` and `FIREWEED_E2E_SEED=<seed>`; `smoke` is
the PR shape and `release` the shape below. The ledger records the scale,
seed, command, topology, and the test binary and filter that ran each suite.

`stats` and `aggregate` may be torn under concurrent writes (V3), so workflow
assertions on them are made on a quiet queue; `read` and `scan` are fresh.

Failure-heavy criteria depend on `fault_injection_harness_tests`: it
terminates worker and service processes at deterministic cut points, injects
duplicate resends, forces partial appends and commits, partition handoffs and
epoch advances, and records the seed and failure schedule. It must exist
before AC-CLAIM-2, AC-OWN-2, AC-E2E-2, AC-E2E-3, AC-E2E-5 and AC-E2E-7 are
claimed. AC-E2E-6's release bar also needs TP-002 E2's
`queue_density_single_node_tests` harness.

| AC | Release shape | Topology |
|----|---------------|----------|
| AC-E2E-1 | `S=1M`, `C=64`, `T_soak`, `N_kill≥1000` | one node, and partitions spread over ≥ 2 nodes |
| AC-E2E-2 | ≥ 1000 groups of 1, 3 or 10 records, `max_groups=300`, `C=64`, `T_soak`, `N_kill≥100` during claims and settles | partitions spread over ≥ 4 nodes |
| AC-E2E-3 | ≥ 10K callback batches of 2, 10 or 1000 records, `C=64`, `T_soak`, `N_kill≥100` during claims and settles | partitions spread over ≥ 4 nodes |
| AC-E2E-4 | `S=1M` recurring records, `C=64`, `T_soak`, `N_kill≥1000` | partitions spread over ≥ 4 nodes |
| AC-E2E-5 | `S=1M`, `C=64`, `T_soak`, `N_kill≥1000`, duplicate resends and partition handoffs enabled | partition failover with epoch fencing |
| AC-E2E-6 | hot queue `S=10M`, one small queue with candidates, `K_q=1000`, `C=64`, `T_soak` | partitions spread over nodes (TP-002 E2) |
| AC-E2E-7 | ≥ 1M records across pause, inspection, force-clear, redrive, purge and archive cases, `C=64`, `T_soak`, `N_kill≥100` | one node |
| AC-E2E-8 | `S=1M`, `C=64`, `T_soak`, skewed non-timestamp ranks | one node |
| AC-E2E-9 | `S=1M`, one hot group, `max_records ∈ {1, 25, 100}`, paced claims, `C=8`, `T_soak` | one node |

| AC | Suite | Workflow | PRD coverage | Pass bar |
|----|-------|----------|--------------|----------|
| AC-E2E-1 scheduled action delivery | `product_workflow_scheduled_action_delivery_e2e` | One queue per job type with consumer-named stages. Insert records early, later write each record's scheduled send time in µs as its rank by key, block and reopen account, connector and campaign gates with gate ops, claim with `Next`, and settle each claim with `Release(t)` and a stage move. | FR-3, FR-17, FR-44, FR-45, FR-46 | INV-1, INV-2 = 0; claim order matches rank; no record claimed while it carries a Blocked gate; the same queue name in a second tenant stays isolated; stage counts on the quiet queue match the harness ledger |
| AC-E2E-2 Marketo group batching | `product_workflow_marketo_group_batching_e2e` | Model a downstream call that accepts up to 300 distinct leads: each lead is a group of tasks with skewed ranks; read `stats.groups` as the batching threshold, then claim with `WholeGroups { max_groups: 300 }` under contention and settle. | FR-31a, FR-32, FR-47, FR-47b | AC-GRP-1's pass bar; concurrent claimers never share a group |
| AC-E2E-3 callback batch execution | `product_workflow_callback_batch_e2e` | Model `actions_scheduled` callback batches: insert each batch as one group in one atomic write unit, claim with `WholeGroups`, and check the group's record count against the batch's expected count in the caller. | FR-47c | 0 batches observed partially; 0 count-check failures; INV-7 = 0 |
| AC-E2E-4 jobs and connectors recurring | `product_workflow_jobs_connectors_recurring_e2e` | One record per job or connector key in its type's queue: claim, work, and settle each cycle with `Release(t)` and the next run time as rank; idle periods with future ranks; teardown by `Delete`. | FR-49, FR-51, FR-52, FR-53, FR-54, FR-55 | one record per key; `version` increases on every cycle; `attempts` rises by one per claim and is never reset; no record claimed before its rank; `stats` and `aggregate` tell idle records from ready ones on the quiet queue; a settle after the record's delete gets `StaleLease` |
| AC-E2E-5 worker crash recovery | `product_workflow_worker_crash_recovery_e2e` | Real processes: worker exits after claim, service exits after an acknowledged write, duplicate resends, partition handoff and epoch advance, restart. | FR-22, FR-24, FR-25, FR-26, FR-27, FR-28 | INV-1, INV-2, INV-5, INV-10 = 0; expired claims free their records within Δ at their rank and insertion sequence |
| AC-E2E-6 noisy neighbor | `product_workflow_noisy_neighbor_scale_e2e` | One hot queue with a 10M-record backlog, one small queue with candidates, and `K_q=1000` active queues; the workload declares a ready-to-claim bound (TP-002 E0). | FR-1, FR-43 | the small queue meets AC-LAT-1's exact-work and same-run bars; 0 records claimed later than the declared bound after becoming claimable; shared worker, connection and task bounds hold |
| AC-E2E-7 operator workflow | `product_workflow_operator_repair_redrive_e2e` | Pause with the `"*"` gate; inspect with `read` and `scan`; force-clear stuck records through the admin handle; redrive failed records by moving them back to a claimed stage by key; purge with `Delete`; resume; archive with `delete_queue`. | FR-17, FR-20, FR-57, FR-58, FR-60 | INV-11 = 0; no claim while paused; no token in any response or log; `force_clear` is reached only through the admin handle (OP1); the archived queue returns `QueueClosed` |
| AC-E2E-8 generic rank ordering | `product_workflow_generic_rank_ordering_e2e` | Non-timestamp work: priority buckets as negative and positive rank offsets, skewed ranks, fields no Seventh Sense shape needs, claimed and settled under contention. | FR-2, FR-4 | INV-6 = 0; no Seventh Sense field is required |
| AC-E2E-9 downstream pacing is not a queue feature | `product_workflow_downstream_pacing_non_goal_e2e` | Many candidates in one group; claims with caller-chosen `max_records` and pauses between calls. | FR-45 | each claim on the quiet queue returns `min(max_records, candidates)` (C5a); no candidate is withheld for a downstream-rate reason; no rate or quota state exists |

`AC-SEN` is the aggregate product gate. The `product_validation_tests` release
job runs AC-E2E-1 to AC-E2E-6, AC-E2E-8 and AC-E2E-9 at their release bars and
fails when any lacks ledger evidence. The `operator_validation_tests` job owns
AC-E2E-7. The `seventh_sense_validation_tests` job is the Seventh Sense-shaped
subset, AC-E2E-1 to AC-E2E-4; it does not replace the product gate.

### 3.11 Kernel contract conformance (API-006)

API-006 is the contract a consumer depends on. These criteria make its
conformance suite, its operator suite and its operator ban build-gating for
every `Store` implementation.

| AC | Setup | Assertion | Pass bar |
|----|-------|-----------|----------|
| AC-KC-1 contract suite | `run_all` on every implementation: its constructor under both `DeletePolicy` values, the suite's manual `Clock`, the documented Δ, `reopen`, and the post-commit fault hook | At least one scenario per MUST clause of API-006 §4, §5 and §6, each citing its clause id (CS2), including every CS6 scenario | 100% pass; 0 skipped, ignored or zero-assertion scenarios; every MUST clause of §4, §5 and §6 has a scenario except the three cost rules of §3.12: C9, the S2 cost bound, and F4's rule that a projection reads no unreturned field value |
| AC-KC-2 torn-read adversary | Every AC-KC-1 scenario again, through the adversary that serves `stats` and `aggregate` from a mix of pre- and post-write state under concurrent writes (CS3) | Scenarios hold with torn `stats` and `aggregate`; `read` and `scan` get no lag wrapper and stay fresh (V3) | 100% pass in both runs |
| AC-KC-3 no backend knobs | Inspect the suite source and its entry point | No per-backend branch, `cfg`, knob, capability probe or backend identity; `run_all` takes only the CS1 inputs (CS4) | 0 occurrences |
| AC-KC-4 operator ban | CI dependency and source guard over Snorri, the workload and loadgen crates, and `fireweed-contract-suite` | No reference to the `Operator` trait, `force_clear` or the admin handle (OP4, CS5) | 0 references; any reference fails the build |
| AC-KC-5 operator suite | A separate suite that takes the admin handle | OP1: no `Store` value reaches an `Operator`. OP2: each entry is its own atomic unit; a Leased record is detached with a new version while the claim's other records stay attached and its token stays valid for them. OP3: validation, durability, visibility and replay match `write` | 100% pass |

### 3.12 Claim-path and storage performance gates

C9, the S2 cost bound and F4's projection read rule are the cost rules the
black-box suite leaves to performance gates (API-006 CS2). These gates verify
them on S3×Turso, together with the storage rules the spikes found necessary.
TD-017 §13 maps each check of the layout to one of these gates, and this
section owns their pass bars. AC-PERF-1 to AC-PERF-6 and AC-PERF-8 are gates;
AC-PERF-7 is a recorded measurement.

Pass bars are structural (query plans, statement traces, schema shape) or
same-run ratios. The spike figures explain why each gate exists; they are not
pass bars, and the spikes ran on a shared host, so their relative results are
the signal. Spike sources:
`docs/helix/02-design/spikes/SPIKE-002-record-key-storage.md`,
`docs/helix/02-design/spikes/SPIKE-003-record-field-storage.md`,
`docs/helix/02-design/spikes/SPIKE-004-claim-path-at-scale.md`,
`docs/helix/02-design/spikes/SPIKE-005-turso-value-size-boundary.md` and
`docs/helix/02-design/spikes/SPIKE-006-declared-field-storage-layout.md`.

| AC | Gate | Pass bar | Why (spike evidence) |
|----|------|----------|----------------------|
| AC-PERF-1 stage-bound claims, bounded group reads, projected field reads | A plan test (`EXPLAIN QUERY PLAN`) over every claim-path and `stats.groups` statement on the production schema, plus statement traces of claims and reads with and without projections | Claim walk: an indexed range seek on the named stage-prefixed claim index over unclaimed records, with no table scan and no sorter; 0 stage-blind claim-path statements; a `Next` claim visits no record of another stage, no Leased record and no record with `rank > horizon` (C9). `stats.groups`: `ready_groups` seeks the group-head index and visits at most `limit` groups; `oldest_ready_since` walks the ready index in `free_since` order with no sorter, stops at its first entry with `rank <= horizon`, and reads no record row for an entry it skips (S2). Projection: a claim or read with `fields: Some([])` issues no statement on the field tables, and a projected claim or read reads no spilled value it does not return (F4) | A stage-blind claim query cost 5,000–6,500× the stage-bound one at 1M records (SPIKE-004 Finding 5). A spilled value costs about 50–70 µs more to read than an inline one (SPIKE-005), so reading unreturned large values would tax every claim. |
| AC-PERF-2 claim cost at scale | `Next` claim and bulk settle on one queue at 1M and 10M records, interleaved in one run | 10M claim and settle medians within the workload's declared same-run envelope of the 1M control | Claims ran at 626.2/s at 1M and 649.0/s at 10M with the bulk finish (SPIKE-004 Findings 1 and 6). |
| AC-PERF-3 claims table | Leases are per claim: one claims row per claim and a nullable indexed claim id on each record | `Extend` cost does not grow with the claim's record count; expiry discovery cost grows with expired claims, not with claimed records; settling a claim's records with one shared patch is one bulk update by claim id, not one statement per record | Extend about 80× and expiry discovery about 75× faster than per-record lease columns; the bulk finish held finish flat at 10M and 100M records (SPIKE-004 Findings 2, 3, 6 and 8). |
| AC-PERF-4 statement discipline | A test over every hot statement, re-run on every Turso version bump, plus review | The index is named on every hot `SELECT`, `UPDATE` and `DELETE`. Each key is looked up by its own statement, never through one `IN (…)` list against a composite index, and no `UPDATE … FROM (VALUES …)` runs against a composite-key table. Each multi-statement op runs in one explicit transaction and reuses its prepared statements. The passive WAL checkpoint cap is on, and each Turso file has one serialized writer connection. The key index is maintained during ingest, not built after a bulk load | The planner skipped the secondary index without a hint, and `IN (…)` did not seek, about 28× slower (SPIKE-002 Findings 0 and 1). `UPDATE … FROM (VALUES …)` ran about 100× slower than per-row statements (SPIKE-005). Independent writers failed with `database is locked` at 32 concurrent claimers, and a post-load unique index over 100M records took 84.65 minutes (SPIKE-004 Findings 8 and 9). Without one transaction and reused statements the claim path was 300–500× slower (SPIKE-006 Method). Without the checkpoint cap, a 10,000-record probe did not finish in 270 s (SPIKE-004 Finding 0). |
| AC-PERF-5 lean claim row and value placement | Schema test | The claim-path row carries no field bytes; fields live in a separate binary-map row; a value over 4,096 B spills per field; the page size is 4 KiB; the queue is stored as a per-queue integer surrogate and the key as a `BLOB` | Layout C, with no field bytes on the claim row, was fastest on the claim path (SPIKE-006 Finding 2). An inline large value cost 3.5–9× the update throughput of the same value off-row (SPIKE-003). The spill knee sits between 3 and 4 KiB at a 4 KiB page, and larger pages did not help (SPIKE-005). The surrogate saves 11–21% per row and `BLOB` keys 11–18% (SPIKE-002). |
| AC-PERF-6 many queues per file | TD-017 §13's G-100M shape: one Turso file with 100 queues × 1M records (950,000 in a stage nothing claims, 50,000 in the claimed stage); 100-record `Next` claims, bulk settle, extend and expiry sweep round-robin over 20 queues; 3 trials × 100 repetitions; against a 10M-record single-queue control in the same run | Claim median at least 90% of the control's and worst-trial claim p99 at most 2× the control's, with settle, extend and sweep within the same bounds. The envelope is INFERRED from SPIKE-004's 2–24% trial spreads; a workload may declare a tighter one | With about 300 B of fields on the claim row, claims fell 45.8% and p99 rose 52× (SPIKE-004 Finding 8). With the lean row of AC-PERF-5 and the default 128 MiB cache, the spike met this envelope (SPIKE-004 Finding 9). Claims ran at 620.9/s, 95.7% of the control's 649.0/s, with p99 at 2,395 µs, 1.33× the control's 1,806.9 µs. Finish, extend and sweep stayed flat. The gate runs the same shape on the product implementation. |
| AC-PERF-7 per-queue commit cost (a measurement, not a gate) | The current object log's per-queue commit cost on the local S3 test endpoint at 1, 10, 100 and 1,000 queues × 10 cmd/s, 1 KiB commands, 20 ms linger, with 0, 20 and 40 ms of injected round-trip delay (TD-018 Spike A) | Recorded with its topology (PUTs and GETs per command, commit p50 and p99, committer backlog, index-document bytes over time, open time against queue count) before any compaction, snapshot, log-split or full-delete GC design is committed | Those designs are proposals, and none is committed before it is measured (ADR-025 §5). |
| AC-PERF-8 queues per file | `define_queue` and file open at 100, 1,000 and 10,000 queues in one Turso file, in one run | `define_queue` creates no table or index, so the file's schema object count does not depend on its queue count; `define_queue` latency and file open time at 10,000 queues within the workload's declared same-run envelope of the 100-queue control | Per-queue tables took 13,111.3 s to create and 26.64 s to open at 10,000 queues, which breaks "queues are cheap" (SPIKE-006 Finding 7; ADR-025 §2). |

## 4. Conformance Gate

The contract suite (AC-KC-1 to AC-KC-3) is the conformance gate for the
`Store` contract. The operator suite (AC-KC-5) is the conformance gate for the
`Operator` trait. A `Store` implementation is release-eligible only when both
pass at 100% with zero skips. The implementation criteria of §3.2 to §3.9 and
§3.12 are evidence that S3×Turso meets the contract at scale and under faults.
They cannot replace, skip or weaken the contract suite.

## 5. CI Quality Gates

Gates run at two cadences. **Per-PR** gates MUST pass to merge. **Release**
gates MUST pass to call a build verified; they include every per-PR gate plus
the soak, scale and exhaustive gates. A build is verified only when the whole
release column is green.

| Gate | Threshold | Cadence |
|------|-----------|---------|
| `cargo fmt --all --check` | clean | per-PR |
| `cargo clippy --workspace --all-targets -- -D warnings` | 0 warnings | per-PR |
| `cargo test --workspace` | 100% pass | per-PR |
| `cargo deny check` and `cargo machete` | clean | per-PR |
| `#![forbid(unsafe_code)]` | enforced; any exception recorded in an ADR | per-PR |
| Coverage of `fireweed-contract` | ≥ 90% line, ≥ 85% branch | per-PR |
| Property tests (AC-CORE-3, AC-CORE-5) | ≥ `props` (PR tier); 0 falsifications | per-PR |
| Fuzz targets (request validation, ESF decode, object-log command decode) | ≥ `fuzz` (PR tier); 0 new crashes | per-PR |
| Object-log format | golden bytes of the current format; integrity checks; single-bit mutations, truncation and malicious lengths rejected; an object log of any other generation or command format fails closed at open (API-006 §8: no replay compatibility) | per-PR |
| Kernel contract suite AC-KC-1 to AC-KC-3 | 100% pass in the direct and torn-read runs; 0 skipped, ignored or zero-assertion scenarios; no backend knob | per-PR |
| Operator ban AC-KC-4 and operator suite AC-KC-5 | 0 references from Snorri, the workload and loadgen crates, or the contract suite; operator suite 100% pass | per-PR |
| Claim-path structural gates AC-PERF-1, AC-PERF-4, AC-PERF-5 | plan, statement-trace and schema assertions pass | per-PR |
| Every other `AC-*` in §3 | 100% of claimed criteria green at their bars | per-PR for unit and integration criteria and product smoke; release for soak, scale and release-shape criteria |
| Product smoke (`FIREWEED_E2E_SCALE=smoke`) | AC-E2E-1 to AC-E2E-9 pass at smoke shape | per-PR |
| Claim-path and storage scale gates AC-PERF-2, AC-PERF-3, AC-PERF-6, AC-PERF-8 | same-run bars pass on the product implementation | release |
| Per-queue commit cost AC-PERF-7 | recorded with its topology | once, before any compaction, snapshot, log-split or full-delete GC design is committed |
| Portable degradation gates AC-LAT-1, AC-LAT-2, AC-LAT-4 | exact work, same-run ratios and declared resource bounds pass; absolute percentiles are reported only | release |
| Durability under duress AC-TXN-1 to AC-TXN-12 | green with INV-10, INV-12, INV-13 and INV-14 = 0 | release |
| Safety invariants INV-1, INV-2, INV-5 to INV-10, INV-12 to INV-14 | 0 violations under the §2 stress matrix and the §3.9 duress matrix | release |
| Operator invariant INV-11 | 0 violations under the §2 stress matrix with `force_clear` enabled | operator release |
| Loom, for each custom concurrent structure | exhaustive to the bounded preemption depth; 0 failing interleavings | release |
| Property and fuzz, nightly tier | ≥ nightly `props` and `fuzz`; 0 falsifications or crashes | release |
| Flaky rate | < 0.1% over 100 CI repeats of the suite | release |
| TP-002 E0 to E3 | pass at TP-002 bars | release |
| `AC-SEN` product aggregate | AC-E2E-1 to AC-E2E-6, AC-E2E-8 and AC-E2E-9 green with ledger evidence | release |
| Operator product aggregate | AC-E2E-7 green with ledger evidence; INV-8 and INV-11 = 0 | operator release |

The release gate runs from a clean checkout: `scripts/ci/release-gate.sh`
validates TP-002 E0 to E3 from closed DDx source beads (passed with
`--tp002-e0e1-source`, `--tp002-e2-source` and `--tp002-e3-source`), then
regenerates and strictly validates the `product_validation_tests` ledger. It
never relies on pre-existing ledger artifacts.

## 6. Verification Ledger

Each bead that claims an `AC-*` records in its evidence:

- the `AC-*` and `INV-*` ids satisfied;
- the exact commands run and their exit status;
- the environment: toolchain, implementation revision, topology, host class
  and seed;
- the measured values against the pass bar;
- the named suites (TP-001) that produced them.

A feature is verified when every `AC-*` it lists is green with recorded values.
A bead MUST NOT close on formatting or unit tests alone when its criteria touch
storage, concurrency, claims, leases, the operator or scale (ADR-003 testing
policy).

## 7. Exit Criteria

Fireweed v1 is verified when:

1. AC-KC-1 to AC-KC-5 pass on every `Store` implementation.
2. AC-PERF-1 to AC-PERF-6 and AC-PERF-8 pass on S3×Turso, and AC-PERF-7 is
   recorded.
3. INV-1, INV-2, INV-5 to INV-10 and INV-12 to INV-14 hold with 0 violations
   under the §2 stress matrix and the §3.9 duress matrix.
4. Every other `AC-*` in §3 passes at its bar and is recorded in the ledger.
5. The §5 CI quality gates are green.
6. TP-002 E0 to E3 pass on S3×Turso.
7. `AC-SEN`: `product_validation_tests` runs AC-E2E-1 to AC-E2E-6, AC-E2E-8
   and AC-E2E-9 at their release bars: scheduled delivery, Marketo group
   batching, callback batches, recurring jobs and connectors, crash recovery,
   noisy-neighbor isolation, generic rank ordering, and pacing left to the
   caller.
8. Operator support is verified when AC-KC-5 passes and
   `operator_validation_tests` runs AC-E2E-7 at its release bar with INV-11
   holding under the §2 stress matrix. It is required before claiming
   operator support, and it does not block items 1 to 7.

Any gap is recorded as an explicit, dated deferred item with an owner, never
dropped silently.
