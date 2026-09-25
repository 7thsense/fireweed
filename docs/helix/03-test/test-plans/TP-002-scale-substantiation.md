---
ddx:
  id: tp-scale-substantiation
  depends_on:
    - prd
    - api-fireweed-kernel-contract
    - adr-kernel-records-contract
    - adr-cqrs-log-projection-storage-model
    - adr-queue-as-shard-unit-and-projection-families
    - adr-s3-turso-public-cell
    - td-sharding-and-shard-ownership
    - td-object-log-turso-projection
  status: accepted
  review:
    self_hash: e0ca180cb81c98e7c451341f1ea912bf152ac2c75d422a3b315516fc9f8ee7d3
    deps:
      adr-cqrs-log-projection-storage-model: 849c0bd7e15200ab056c2e5fcedb4b04a116aba520993fb4bab63b1195146107
      adr-queue-as-shard-unit-and-projection-families: 50fb11c85cbf40fa182469b036ef5210b304f330171a17ab371ae485524cb924
      prd: 2d97b05f9c0c0db576149bdfef21c729d66e07dbb674c95f6b7135ddcffa3b91
      td-sharding-and-shard-ownership: b98590bc7a51f8e904052d64aaa6ab4d8a9c9729d155d17ee0823ffcf6b64a0d
      td-storage-architecture-backend-contracts: b1d17cc3481f52097ea0b2233a4a0e7bfa1512381c0b1fed7b3830fd3f02cc4e
    reviewed_at: "2026-07-20T20:00:20Z"
---

# Test Plan: TP-002 Scale Substantiation

## Scope

This plan defines the evidence that substantiates every horizontal-scale,
write-rate and hot-queue claim in the Fireweed frame and design. It is the
home of the scale evidence records (E0–E3), their pass bars, the coverage rows
for cross-queue scale-out, the named scale suites, and the docs-lint checklist
for scale claims.

The PRD asserts horizontal scale beyond a single database. It names the one
production implementation and its queue-to-partition placement, and no storage
layout or query. Its scale claims are publishable only by reference to
the evidence records defined here. Mechanisms live in the
design: ADR-001 and ADR-024 for the object log and its Turso projection,
ADR-008, TD-003 and ADR-025 §5 for placement and partition ownership, and
TD-010 for the object log's serving protocol. This plan binds them to
measurable benchmarks.

Every run measures the one implementation of API-006: the S3 object log ×
Turso projection (ADR-024). A queue is never split: the control plane assigns
it to one partition, which fixes its node (ADR-025 §5). Horizontal scale is
therefore **cross-queue**: partitions, and the queues in them, are distributed
across owner nodes.

The functional contract is outside this plan. The contract suite, the
operator suite and every pass bar other than the scale bars are in TP-001 and
TP-003. Scale evidence qualifies an implementation only after the contract
suite passes on it with zero skips (TP-003 AC-KC-1 to AC-KC-3).

## The Scale-Claim Rule

> **Scale-Claim Rule**: Every scale claim MUST reference an **evidence record**
> that names (a) the deployment shape (single-deployment Tier-1 or cross-queue
> Tier-2), (b) the workload envelope, and (c) the design or test artifact and
> benchmark that substantiates it. The PRD and product vision may name the
> one production implementation (ADR-024) and queue-to-partition placement
> (ADR-025 §5); they MUST NOT name storage layout, queries or SQL, and they
> reference the evidence record by id.

| Envelope | Deployment shape | Evidence record |
|----------|------------------|-----------------|
| **Tier-1 (single deployment)** | one S3 object log and one owner node; the queue under test owned by that node | **E1**, against the portable contract **E0** |
| **Tier-2 (cross-queue horizontal)** | queues mapped to partitions, and partitions distributed across independent owner nodes, each partition owned by one node at a time | **E2** and **E3** |

Absolute rates and latencies are capacity observations for the host, topology
and commit that produced them. They are never portable pass bars, and a miss
of a script's rate target is not a product failure. A scale claim cites a
passing evidence record; without one, no claim is made.

## Scale Evidence Records

Every PRD, ADR and TD scale claim references one of these records. This plan
owns the ids E0–E3; no other document mints evidence ids.

`scripts/release/build-governed-evidence-bundle.sh` stages the explicitly
named E0, E1, E2 cross-owner, E2 density, E2 failover and E3 producer outputs
for the checked-out revision. It writes `composite-contract.json` in an
explicit run-owned `tp002-release` directory and dispatches
`scripts/ci/verify-governed-release-composite.sh`; neither command scans a
ledger or evidence directory for substitutes. `scripts/ci/release-gate.sh`
generates fresh smoke-tier E2 and E3 rows, requires that exact composite after
promotion to `target/tp002-release`, and verifies every named semantic
authority against the checked-out `HEAD`. The tag workflow verifies the
exact-revision archive, its SHA-256 sidecar and its attestation against the
resolved tag and commit before packaging or publication.

### Release-evidence freshness and source binding

The release freshness policy is **exact-tag rerun**. Earlier E0–E3 rows are
useful build records, but they cannot make a later tag green. For every
release tag, the governed evidence commands run from the exact 40-character
commit the tag names and produce a new reviewed attestation. The tag gate
passes both the tag and the resolved commit to
`fireweed-verify-evidence-attestation`; a mismatch fails closed.

The policy favors a simple, auditable rule over reviewed-range reuse: even a
docs-only release commit invalidates the prior attestation, so automation
never has to infer whether a change could affect a benchmark. Expensive
evidence may run before a release candidate, but the final tag's evidence is
rerun at the exact release commit.

### Attestation contract (`schema_version: 1`)

Each tag has one JSON attestation with these required fields:

- `policy: "exact-tag-rerun"` and `scope: "tp002-release-v1"`;
- `source.tag` and the full lowercase `source.commit` resolved from that tag;
- the exact non-empty `producing_command`, plus UTC `produced_at` and
  `reviewed_at` timestamps;
- one or more `evidence` entries, each a repository-relative file or directory
  `path` with its lowercase SHA-256;
- `inputs` entries with `path`, SHA-256, and one of the mandatory kinds
  `product_code`, `harness`, `config` or `dependency_lock`. Every kind is
  present, and the inputs cover the product crates and workspace manifests,
  benchmark and deployment scripts, chart and runtime configuration, and
  dependency lockfiles.

E1–E3 qualification runs also bind a provisioned-runner capability
attestation: runner identity, topology and resource limits; the S3 provider,
version and region, native atomic conditional create and update, consistency
contract, TLS mode and bucket-ownership acknowledgement; and, where the
topology has one, the control plane's store and version. Provider brand and
host name are observations, not required values. A missing, unreachable or
unattested live prerequisite fails closed before governed work begins; no
required row becomes `skip`, `not_configured` or a successful zero-test
result.

The normative wire schema is
[`release-evidence-attestation.schema.json`](../../../perf/evidence/release-evidence-attestation.schema.json).
The Rust deserializer independently rejects unknown fields, and the semantic
verifier enforces the cross-field and filesystem rules that JSON Schema
cannot.

Directory SHA-256 values use `fireweed_release::attestation::digest_path`:
recursively sorted regular files, with relative names, lengths and contents in
the digest. Absolute paths, `..`, symlinks, missing inputs, duplicate
bindings, malformed hashes, unknown schema fields and digest drift are
rejected. A code, harness, configuration, dependency or evidence change
without a freshly reviewed attestation therefore cannot reuse a green result.

The enforcement command is:

```bash
cargo run -p fireweed-release --bin fireweed-verify-evidence-attestation -- \
  --manifest <attestation.json> --repo-root . \
  --tag "${RELEASE_TAG}" --commit "${RELEASE_COMMIT}"
```

### Invalidation and emergency releases

Any new tag or release commit, and any change to product code, harness,
configuration, dependency lock, evidence, producing command or reviewed
attestation, requires a rerun and review. Review updates `reviewed_at`; it
does not waive a digest or source mismatch.

An emergency exception is never an alternate green state. A manifest may carry
an `exception` record (`approval_id`, `approved_by`, `reason` and
`expires_at`) for auditability, and the verifier returns failure whenever one
is present. Shipping while evidence is red needs an out-of-band
release-manager approval, a release note that names the missing evidence and
the user-visible risk, and a follow-up exact-tag evidence run. The tag and the
release stay visibly unverified until that run passes.

### E0: Portable per-queue progress and capacity contract

E0 is the host-independent scale **requirement**:

> Under ordinary concurrent load, every queue MUST keep exact outcomes (every
> acknowledged write, claim and settle takes effect once and is never lost),
> keep claim order (API-006 C4), make progress (every queue with candidates
> completes non-empty claims, and a quiet queue returns its first candidates,
> C5a), and consume only bounded shared workers, connections, memory and
> pending tasks as queue count and total load grow.

Seventh Sense needs high-volume queues that stay correct and keep making
progress while the system is busy. A representative record and field band and
an insert, update, claim and settle mix drive E1–E3. Each run reports absolute
throughput and latency with its exact host, topology and resource limits as
capacity evidence. Those values do not decide release eligibility and do not
need an idle, dedicated or specially selected host.

A queue has no progress-bound setting. The E0 and E1 workloads declare a
positive **ready-to-claim bound** as a workload parameter. The harness
measures, for every claimed record, the interval from when the workload made
it claimable (its scheduled time, or when it became Free in its stage,
whichever is later) to its claim, and reports zero intervals over the bound.
Latency buckets, rates and percentiles are capacity observations; they never
substitute for or redefine the declared bound.

**What "preserved for every queue at any scale" means.** Adding queues or load
must not lose or duplicate work, strand a queue that has candidates, break the
declared ready-to-claim bound, or create per-queue background resources. One
designated hot queue and at least 1000 cold queues stay active. Interleaved
same-run controls give the comparison baseline, and a declared degradation
envelope is justified by the workload and a resource-saturation signal.
Aggregate capacity is reported, never extrapolated to other hosts or required
to equal 1000 times a per-queue number.

### E1: Tier-1 single-deployment envelope (pass/fail)

Deployment: one attested S3 object log and a local Turso projection on one
owner node.

| Parameter | Value |
|-----------|-------|
| Batch sizes | write, claim and settle at 1, 100 and 1000 records per request (the API-006 maximum) |
| Records and fields | a representative Seventh Sense record and field band |
| Operation mix | a representative Seventh Sense insert, update, claim and settle ratio |
| Groups and skew | group-heavy and skewed-rank profiles |
| Telemetry | enabled |
| Resident set | 10M records, including settled records in stages nothing claims |
| Pass: progress and correctness | exact acknowledged, claimed and settled counts; no lost or duplicate effect; monotonic progress samples; claim order within each stage; zero ready-to-claim violations of the declared bound |
| Pass: resources | shared workers, connections, pending tasks and memory stay within workload-declared bounds |
| Capacity report | write, claim and settle throughput with p50, p95 and p99 latency, tied to this topology and never used as a portable pass bar |

### E2: Tier-2 cross-queue scale-out (pass/fail)

Mechanism: the control plane maps each queue to one partition and places each
partition on one owner node at a time (TD-003). A queue is never split across
partitions or owners, so every claim is local to one owner (ADR-025 §5). E2
runs on an attested S3 provider with native conditional writes; missing S3
infrastructure is a failed qualification prerequisite, not a skipped row.

| Parameter | Value |
|-----------|-------|
| Owner counts | at least three owner-node counts (for example 2, 4 and 8), each with a fixed number of active queues per owner |
| Pass: cross-queue scale-out | at every owner count, each phase completes exact work, aggregate logical progress is monotonic, ownership is single-valued, and shared resources stay bounded. Interleaved controls report scaling efficiency and saturation as capacity observations; no fixed host-speed multiple is a release bar |
| Pass: queue density | one node runs **at least 1000 cold queues plus one hot queue**: every cold queue keeps a candidate and completes a non-empty claim and settle during loaded hot work; hot and cold counts reconcile exactly; zero ready-to-claim violations; background work runs on bounded shared pools, never one loop, task or connection per queue. Same-run controls quantify noisy-neighbor degradation |
| Pass: per-queue behavior preserved | adding queues or load does not lose or duplicate work, strand a queue with candidates, break the ready-to-claim bound, or exceed declared shared-resource bounds (E0) |
| Pass: partition failover and fencing | after a partition's owner is killed and its lease expires, the target node acquires a strictly greater epoch and fences it before serving, the deposed owner's commit is rejected, and every queue of the partition recovers every acknowledged effect from the object log (TD-003) |
| Pass: one live claim | no record is attached to two live claims across a partition handoff or drain, and a live claim's token keeps working after the handoff (API-006 C6, TD-003) |

### E3: Object-log latency, cost and recovery (pass/fail)

E3 request-cost rows come from a test-scoped production `BlobMetricsRecorder`
snapshot delta around the measured interval, never from a separate counting
store. PUT, GET, DELETE and LIST totals count primitive attempts. Tests assert
that each LIST page counts as an attempt and not a retry, that protocol
retries equal loop iterations minus one, and that hostile key and error inputs
cannot create new series. Disabled-recorder transparency and deterministic
accounting are ordinary CI gates. Recorder overhead is judged against an
interleaved, same-run disabled-recorder control with identical seeded work.

E3 runs on a live attested S3 provider against the portable E0 contract. It is
the only home of governed wall-clock release thresholds; TP-003 correctness
uses fixed work and exact outcomes, and TP-005 timings are host-bound
observations unless an E3 row adopts one of its metrics on this controlled
topology.

| Parameter | Value |
|-----------|-------|
| Linger sweep | at least four commit-latency settings, including low-latency, balanced and cost-optimized values (for example 1, 5, 20 and 100 ms) |
| Pass: commit semantics | every acknowledged request is durable (API-006 DUR1) and observed by later reads (V3) at every setting; p50, p95 and p99 are reported as capacity evidence |
| Pass: progress and resources | exact logical operation counts, monotonic progress, and bounded memory and work queues at every setting |
| Pass: cost | dollars per billion commands and object requests per billion commands at each setting, from the cost model of the object log that runs, with the per-queue commit cost of TP-003 AC-PERF-7 as its first measured input; any comparative threshold names its reference topology and attested provider in this record |
| Pass: recovery | rebuild an exact 10M-record Turso projection from the object log through projection control on the admin handle, with count, checksum and order equality, monotonic replay progress, and bounded memory and work queues; wall time is capacity evidence. The rebuild runs from a suite outside the workload and loadgen crates, which never reference the admin handle (API-006 OP4) |
| Pass: conditional-write fencing | a stale-epoch writer's conditional commit is rejected; a provider without atomic conditional writes is rejected before any I/O |
| Pass: durability semantics | TP-003 AC-TXN-1 to AC-TXN-4 hold across the sweep; no latency or cost setting weakens them (AC-TXN-7) |
| Pass: byte admission | global and per-tenant byte admission against request-count-only admission, for small, target-sized and oversize requests under a stalled store and hot and cold tenants: charges never exceed caps; a cold tenant progresses; median throughput regresses at most 5% and p99 at most 10%, including serialization before an oversize rejection |

### Recurring records under scale

A recurring record is one record per key, settled each cycle with
`Release(t)` and the next run time as its rank. These rows show that recurring
records need no special handling in either envelope.

| Benchmark | Required evidence |
|-----------|-------------------|
| Recurring records under scale | (a) **high-frequency re-rank**: records settled each cycle with a rank of now sustain their throughput, with `version` increasing on every cycle and no projection divergence; (b) **idle inventory**: N idle records with future ranks do not slow claims of ready records, because a claim never visits a record ranked beyond its horizon (API-006 C9), and `stats` and `aggregate` tell idle records from ready ones; (c) **delete under load**: `Delete` of Free records, and of held records with `Release(t)`, on one owner and idempotent by `request_id`, leaves no record behind. |

## Requirement Coverage Matrix

These rows extend TP-001 with the scale mechanism. PRD P0 items are cited by
name.

| Requirement | Governing artifact | Required test evidence |
|-------------|--------------------|------------------------|
| PRD P0 horizontal distribution | PRD / ADR-008 / TD-003 / ADR-025 §5 | E2 cross-queue scale-out: exact work and monotonic logical progress as owner count rises; E0 holds for every queue under K-queue concurrency; one live claim across a partition handoff. |
| PRD P0 performance at scale | PRD / ADR-024 / TD-010 | E1 and E2 keep exact outcomes, claim order, the declared ready-to-claim bound and bounded resources while queues spread across owners. Throughput and latency are capacity evidence for the declared topology. |
| PRD P0 queue density | PRD / TD-003 / TD-010 | E2 density on an attested live S3 row: at least 1,000 cold queues plus one hot queue; every cold queue keeps a candidate and completes a non-empty claim and settle during loaded hot work; hot counts reconcile across baseline, load and baseline; shared workers, tasks and connections stay within declared bounds. Quiet-host and fixed-speed gates are forbidden. |
| Placement and partition ownership | ADR-008 / TD-003 | A queue's partition is recorded at its first `define_queue` and never changes; one live lease per partition; epoch fencing of a stale owner; drain and handoff without loss or duplication; recovery of every queue in the partition from the object log. |
| Object log | ADR-024 / TD-010 | E3 latency, cost and recovery with the linger sweep; conditional-write fencing; providers without it fail before I/O. |
| Recurring records under scale | API-006 / ADR-024 | High-frequency re-rank, idle inventory and delete under load on S3×Turso. |
| Contract conformance first | API-006 / TP-003 | The contract suite passes with zero skips before any scale evidence can qualify the implementation. |

## Named Test Suites

Implementation beads create or extend these suites:

- `queue_ownership_fencing_tests`
- `queue_reassignment_drain_tests`
- `object_log_commit_recovery_tests`
- `object_log_latency_cost_matrix_tests`
- `external_transaction_contract_matrix_tests`
- `performance_single_deployment_baseline_tests`
- `performance_cross_queue_scale_out_tests`
- `queue_density_single_node_tests`
- `recurrence_scale_envelope_matrix_tests`

## Scale Evidence Requirements

Scale benchmarking includes:

- single-deployment exact outcomes, monotonic progress, claim order and
  bounded resources under the E0 contract, with throughput and latency
  reported as capacity for the declared topology (E1);
- cross-queue scale-out at three or more owner counts, with aggregate write
  and claim rates reported per owner count (E2);
- the E0 contract kept for every queue as active-queue count and total load
  grow (E2);
- queue density: at least 1000 cold queues plus one hot queue on one node,
  exact completion for every queue, same-run degradation within its declared
  envelope, and background work on bounded shared pools (E2,
  `queue_density_single_node_tests`);
- partition failover and fencing, with recovery from the object log and no
  double lease across the handoff (E2, TD-003);
- object-log commit latency across the linger sweep, dollars and object
  requests per billion commands, and 10M-record projection rebuild time (E3);
- conditional-write fencing and pre-I/O rejection of an S3 provider that
  cannot supply it (E3);
- the durability semantics of TP-003 AC-TXN-1 to AC-TXN-4 across the linger
  sweep, so a lower-latency or lower-cost setting cannot weaken them (E3);
- recurring records under scale in both envelopes.

## Deferred Evidence

Seventh Sense's production ready-to-claim bound, which sets the E0 and E1
workload bound, is required before claiming product validation but not before
the first implementation bead.

## Scale-Claim Review Checklist (docs lint)

A document fails review if any of these hold:

- It asserts "horizontal scale", a write rate or a "10M hot queue" without
  referencing an evidence record (E0–E3) that names the deployment shape,
  workload envelope and substantiating artifact.
- A PRD or product-vision scale sentence names storage layout, a query or
  SQL, or states a scale claim without an evidence-record id.
- A scale claim in any document lacks an evidence-record id.

Reviewers MUST reject documents that match any rule above.

## Exit Criteria

Before a scale claim is published, the evidence records it cites pass against
the E0 contract: E1 for the single-deployment envelope, E2 for the horizontal
envelope (including every-queue progress under K-queue concurrency), and E3
for object-log latency, cost and recovery. A scale claim cites at least one
evidence record and, where it asserts a benchmark outcome, the named suite
that produced it. A horizontal-scale claim never rests on E1 alone.
