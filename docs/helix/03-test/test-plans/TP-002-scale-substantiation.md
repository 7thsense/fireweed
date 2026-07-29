---
ddx:
  id: tp-scale-substantiation
  depends_on:
    - prd
    - adr-cqrs-log-projection-storage-model
    - adr-queue-as-shard-unit-and-projection-families
    - td-storage-architecture-backend-contracts
    - td-sharding-and-shard-ownership
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

This plan defines the scale evidence required to substantiate every horizontal-
scale, write-rate, and hot-queue claim made across the fireweed frame and design.
It is the canonical home for the scale evidence-record scheme (E0–E3), the
benchmark pass bars, the requirement-coverage rows for the cross-queue scale-out
mechanism, the named scale test suites, and the docs-lint scale-claim checklist.

This plan exists because the PRD asserts horizontal scale beyond a single
database, but the PRD must name no storage backend, scale mechanism, or query
(prd "Scale Substantiation"). Those claims are made publishable only by
reference to the evidence records defined here. Backend names and mechanism IDs
live in ADR-001, ADR-008 (`adr-queue-as-shard-unit-and-projection-families`),
TD-001, TD-002, TD-003 (`td-sharding-and-shard-ownership`), and
TD-004 (`td-s3-object-log-sqlite-projection-mode`); this plan binds them to
measurable benchmarks. Per ADR-008 the queue is the unit of sharding, so
horizontal scale is **cross-queue** — distributing queues across owner nodes —
not intra-queue sharding.

This is a pre-implementation test plan. Exact Rust function and harness names may
change when the workspace is created, but implementation beads must preserve the
evidence-record intent and cite the relevant evidence IDs.

The general lifecycle, conformance, idempotency, and per-backend coverage live in
the governing test traceability plan (`tp-governing-test-traceability`). This
plan covers only the scale-substantiation surface and references that plan's
shared backend conformance suite (TD-001) rather than restating it.

## The Scale-Claim Rule

> **Scale-Claim Rule**: Every scale claim MUST reference an **evidence record**
> that names (a) the deployment shape (single-deployment Tier-1 vs cross-queue
> Tier-2), (b) the workload envelope, and (c) the design/test artifact plus
> benchmark that substantiates it. The PRD and product-vision MUST NOT name a
> storage backend, profile, scale mechanism, or SQL; they reference the evidence
> record by ID. Backend names live only in ADR-001 / ADR-008 / TD-001 / TD-002 /
> TD-003 / TD-004 / TP-002.

The two v1 scale envelopes both deliver and both substantiate:

| Envelope | Deployment shape | Delivered by | Evidence record |
|----------|------------------|--------------|-----------------|
| **Tier-1 (single-deployment)** | one storage deployment, one queue owned by one node | `postgres_native` (TD-002) | **E1** vs the portable progress/capacity contract **E0** |
| **Tier-2 (cross-queue horizontal)** | N queues distributed across N independent owner nodes (per-queue ownership leases), each queue's progress bound local to its owner | per-queue ownership (TD-003) + cross-queue distribution (ADR-008) + object-log local-projection profiles (TD-004) | **E2** (cross-queue scale-out) and **E3** (object-log latency/cost + recovery) |

## Scale Evidence Records

Every PRD/ADR/TD scale claim references one of these records.

**Evidence-ID convention**: this plan owns the canonical IDs **E0–E3**. All
documents MUST reference these canonical E-IDs; no document mints its own
evidence IDs.

Release-gate mapping as of 2026-06-16 (**pre-ADR-008 build record**):

| Evidence ID | Source bead(s) |
|-------------|----------------|
| E0, E1 | `pqueue-7e2b3132` |
| E2 | `pqueue-9afd88cc`, `pqueue-76d92a33` |
| E3 | `pqueue-b1abd895`, `pqueue-472a09d4` |

> **Build-record note (ADR-008 reframe).** The E2 source beads above measured the
> *prior* intra-queue-shard build (single-queue-over-N-shards scale-out). Under
> ADR-008 (queue is the unit of sharding) the **E2 requirement is reframed to
> cross-queue scale-out** (below); the E0/E1/E3 records keep their meaning. The
> prior E2 measurement stands as a historical attestation of the retired
> multi-shard mechanism. E0, E1, and E3 (the portable progress contract, the
> single-deployment envelope, and the object-log latency/cost/recovery profile)
> are unaffected by the reframe.
>
> **Re-measurement DONE (B3.3, 2026-07-08).** The horizontal-scale claim now cites
> a fresh live multi-node run of the **reframed cross-queue (ADR-008 per-queue
> owner)** mechanism, superseding the retired multi-shard attestation. Evidence:
> [`docs/perf/evidence/tp002-e2-cross-queue-remeasured.jsonl`](../../../perf/evidence/tp002-e2-cross-queue-remeasured.jsonl)
> — one release-tier E2 ledger row from a live kind (Kubernetes-in-docker) cluster
> of independent `object_log_sqlite_projection` owner pods (2/4/8 owners, one queue
> per owner on disjoint bootstrap queues, CPU-limited server pods driven by a lean
> in-cluster RESP load Job over Service ClusterIP; harness
> `crates/fireweed-bench/tests/performance_cross_queue_scale_out_tests.rs` ::
> `live_multi_node_object_log_sqlite_projection_e2` +
> `scripts/perf/tp002-e2-kind.sh`). The historical run recorded:
> (1) ingest aggregate non-decreasing **8,206 → 15,726 → 30,088 items/s** across
> 2 → 4 → 8 owners; (2) 8-owner / 2-owner ingest multiple **3.67× ≥ 3.5×** (≈73%
> cross-node efficiency); (3) worst single-queue floor held — ingest **3,761/s**
> and claim+finalize **34,234/s**; (4)
> one-owner-per-queue proven live — **56 of 56** cross-node "no such queue"
> confirmations at 8 owners. Host: 32 cores, node image `kindest/node:v1.36.1`,
> kind v0.32.0. (Build-provenance note: in this sandbox the source `Dockerfile.e2`
> cannot authenticate the private git dependencies inside the Docker builder, so
> the harness image was assembled from host-built release binaries via the
> prebuilt-image path — `SKIP_BUILD=1` + `FIREWEED_E2_IMAGE` — which is a packaging
> detail only; the binaries, backend, cluster topology, and load are identical to
> the source-build path.) These absolute rates and ratios remain topology-bound
> capacity evidence; current release qualification applies the portable E0/E2
> correctness, progress, resource, and same-run comparison bars below.

`scripts/release/build-governed-evidence-bundle.sh` stages explicitly named E0,
E1, E2 cross-owner, E2 density, E2 failover/routing, and E3 producer outputs for
the checked-out revision. It writes `target/tp002-release/composite-contract.json`
and dispatches `scripts/ci/verify-governed-release-composite.sh`; neither command
scans a ledger or evidence directory for substitutes. `scripts/ci/release-gate.sh`
separately generates fresh smoke-tier E2/E3 rows, then requires that exact
composite and verifies every named semantic authority against checked-out
`HEAD`. The tag workflow acquires the deterministic exact-revision archive and
SHA-256 sidecar, reruns the composite verifier with `GITHUB_SHA`, and verifies
the archive's attestation against the resolved tag and commit before packaging
or publication.

## Release-evidence freshness and source binding

TP-002 selects **exact-tag rerun** as the release freshness policy. Historical
E0–E3 rows remain useful build records, but they cannot make a later tag green.
For every release tag, the governed evidence commands MUST run from the exact
40-character commit named by that tag and produce a new reviewed attestation.
The tag gate supplies both the tag and resolved commit to
`fireweed-verify-evidence-attestation`; a mismatch fails closed.

The policy deliberately favors a simple, auditable rule over reviewed-range
reuse: even a docs-only release commit invalidates the prior attestation. This
avoids asking automation to infer whether a source-range change can affect a
benchmark. Expensive evidence may be scheduled before a release candidate, but
the final tag's evidence must still be rerun at the exact release commit.

### Attestation contract (`schema_version: 1`)

Each tag has one JSON attestation with these required fields:

- `policy: "exact-tag-rerun"` and `scope: "tp002-release-v1"`;
- `source.tag` and the full lowercase `source.commit` resolved from that tag;
- the exact non-empty `producing_command`, plus UTC `produced_at` and
  `reviewed_at` timestamps;
- one or more `evidence` entries containing a repository-relative file or
  directory `path` and its lowercase SHA-256;
- `inputs` entries with `path`, SHA-256, and one of the mandatory kinds
  `product_code`, `harness`, `config`, or `dependency_lock`. Every kind must be
  present. Producers must bind the complete inputs to the command, including
  the product crates/workspace manifests, benchmark and deployment scripts,
  chart/runtime configuration, and dependency lockfiles.

The normative wire schema is
[`release-evidence-attestation.schema.json`](../../../perf/evidence/release-evidence-attestation.schema.json).
The Rust deserializer independently rejects unknown fields, and the semantic
verifier enforces the cross-field and filesystem rules that JSON Schema cannot.

Directory SHA-256 values use the canonical implementation in
`fireweed_release::attestation::digest_path`: recursively sorted regular files,
with relative names, lengths, and contents in the digest. Absolute paths,
`..`, symlinks, missing inputs, duplicate bindings, malformed hashes, unknown
schema fields, and digest drift are rejected. Therefore a code, harness,
configuration, dependency, or evidence-file change without a freshly reviewed
attestation cannot silently reuse a green result.

The enforcement command is:

```bash
cargo run -p fireweed-release --bin fireweed-verify-evidence-attestation -- \
  --manifest <attestation.json> --repo-root . \
  --tag "${RELEASE_TAG}" --commit "${RELEASE_COMMIT}"
```

### Invalidation and emergency releases

Any new tag or release commit, or any product-code, harness, config,
dependency-lock, evidence, producing-command, or reviewed-attestation change,
requires a rerun and review. Review updates `reviewed_at`; it does not waive a
digest or source mismatch.

An emergency/manual exception is never an alternate green state. A manifest
may carry an `exception` record (`approval_id`, `approved_by`, `reason`, and
`expires_at`) for auditability, but the automated verifier intentionally
returns failure whenever it is present. Shipping while evidence is red requires
an out-of-band release-manager approval, a release note that names the missing
evidence and user-visible risk, and a follow-up exact-tag evidence run. The tag
and release UI must remain visibly unverified until that follow-up passes; an
exception cannot suppress or relabel the failed gate.

### E0 — Portable per-queue progress and capacity contract

E0 is the host-independent scale **requirement**:

> Under ordinary concurrent load, every queue MUST preserve exact accepted,
> claimed, and finalized outcomes; monotonically advance; meet its configured
> queue-global progress bound; and consume only bounded shared workers,
> connections, memory, and pending tasks as queue count and total load increase.

Origin: Seventh Sense requires a high-volume queue that remains correct and makes
progress while the system is busy. A representative item/payload band and
ingest/claim/finalize mix drive E1/E2/E3. Each run reports absolute throughput
and latency with its exact host, topology, and resource limits as capacity
evidence. Those values do not decide release eligibility and do not require an
idle, quiet, dedicated, or specially selected host.

The E0/E1 release workload MUST explicitly declare a positive
`progress_bound_ms` as part of its queue configuration; there is no universal
canonical duration chosen by this test plan. The harness MUST read back the
persisted queue definition, prove it matches the declaration, and report zero
accepted-to-claim intervals or discovery ages beyond that bound. Fixed latency
buckets, rates, and percentiles are topology-bound capacity observations only;
they MUST NOT substitute for or silently redefine the declared queue contract.

**What "preserved for every queue at any scale" means.** Adding queues or load
must not lose or duplicate work, strand an eligible queue, violate its progress
bound, or create per-queue background resources. One designated hot queue and at
least 1000 cold queues remain active. Interleaved same-run controls provide the
comparison baseline; a declared degradation envelope must be justified by the
workload and resource saturation signal. Aggregate capacity is reported, never
extrapolated to other hosts or required to equal 1000 times a per-queue number.

### E1 — Tier-1 single-deployment envelope (pass/fail)

Backend: `postgres_native` (TD-002). Deployment: one Postgres, one queue owned by one node.

| Parameter | Value |
|-----------|-------|
| Batch sizes | push/update/claim/finalize at 1, 100, and max-configured batch size |
| Item / payload | representative Seventh Sense item and payload band |
| Operation mix | representative Seventh Sense ingest / claim / finalize ratio |
| Group cardinality / skew | group-heavy and skewed-priority profiles |
| Telemetry | enabled |
| Postgres sizing | stated instance class, CPU, memory, IOPS, pool |
| Resident set | 10M items including terminal retained rows under retention policy |
| Pass: progress and correctness | exact accepted/claimed/finalized counts, no lost or duplicate transitions, monotonic cursor/progress samples, an explicitly declared positive queue-global progress bound equal to the persisted queue definition, and zero accepted-to-claim or discovery-age violations of that declaration |
| Pass: resources | shared workers, connections, pending tasks, and memory remain within workload-declared bounds |
| Capacity report | ingest and claim/finalize throughput plus p50/p95/p99 latency, tied to this topology and not used as a portable pass bar |

### E2 — Tier-2 cross-queue scale-out (pass/fail)

Mechanism: per-queue ownership (TD-003) + cross-queue distribution (ADR-008) —
many queues spread across many owner nodes; each queue is a single-owner,
single-hop claim (no intra-queue sharding, no scatter-gather).
**Backend: object-log local projection (TD-004) is REQUIRED** for the headline
horizontal evidence. This release matrix is resolved to
`object_log_sqlite_projection` only: the released E2 authority is the durable
SQLite projection profile, while `object_log_inmemory_projection` remains a
non-authoritative comparator for hot-path analysis and hybrid benchmarking.
Revisit the comparator exclusion only if a release-tier, strict-validated
`object_log_inmemory_projection` E2 row is produced under the same bars and
reviewed into the governed release manifest. **`postgres_native` MAY additionally
be run as a comparator** but does not on its own satisfy E2 (per ADR-001 "Scale
Claim Scoping", `postgres_native` alone is not evidence for the horizontal
envelope).

| Parameter | Value |
|-----------|-------|
| Owner counts | benchmark at ≥ 3 owner-node counts (e.g. 2, 4, 8 owners), distributing a fixed-per-owner number of active queues across them |
| Pass: cross-queue scale-out | at 2, 4, and 8 owners, every phase completes exact work, aggregate logical progress is monotonic, ownership is single-valued, and shared resources remain bounded. Interleaved controls report scaling efficiency and saturation; no fixed host-speed multiple is a release bar. |
| Pass: queue density (≥1000 active queues, single-node target) | a single node exercises **at least 1000 cold queues plus one hot queue**; every cold queue becomes claim-visible and progress-eligible, all hot and cold counts reconcile exactly, queue-global progress violations and empty post-reseed claims are zero, and background work is multiplexed onto bounded shared pools rather than one loop/connection per queue. Same-run controls quantify noisy-neighbor degradation. |
| Pass: per-queue behavior preserved at scale (E0 invariant) | adding queues or total load must not lose or duplicate work, strand a queue, violate its progress bound, or exceed declared shared-resource bounds. |
| Pass: per-queue local progress | each queue's oldest-eligible item is claimed before `progress_bound_ms` from its own owner's local computation (queue-global, D1 / FR-12); there is no cross-shard aggregation. |
| Pass: owner failover / fencing | killing a queue's owner: after lease expiry a new owner acquires a strictly greater epoch, the deposed owner's append is fenced, and the queue recovers from snapshot + log tail with no lost/double work (TD-003); a queue left unowned past `progress_bound_ms` surfaces as a progress-bound violation in metrics (FR-41) and `DiscoverActiveScopes` (TD-003). |
| SP-06 handoff profile | The E2 failover evidence schema v2 may carry a dedicated-recorder `handoff_object_store_profile`; schema v1 remains readable for historical evidence. The explicit matrix runs 200 post-fence/pre-serve samples for 256- and 1,000-item queues at scripted 25 ms and 100 ms request latency, with clean and one-unapplied-tail arms. Recorder totals reconcile with named requests. Clean: 20,300 immutable / 20,100 avoidable / 20,099 repeated GETs. Tail: 40,600 immutable / 40,400 avoidable / 39,999 repeated GETs, including 200 unique required segment GETs and 200 replayed commands. First local read requests = 0. Queue item count is not active-queue density. |
| Pass: single lease | no item double-leased across an owner reassignment/drain (TD-003). |
| Pass: routing redirect | a client addressing a queue on the wrong node is redirected (`-MOVED`-style) to the current owner and converges in a single hop; a stale/misrouted write is fenced, never corrupting state (TD-006 §1A). |

### E3 — Object-log latency/cost + recovery (pass/fail)

E3 request-cost rows MUST come from a test-scoped production `BlobMetricsRecorder` snapshot delta around the
measured interval, not a separate counting `BlobStore`. PUT/create, GET, DELETE, and physical LIST page totals
come from primitive attempts; logical head/acquire/fence/branch spans are excluded from billable totals.
Tests assert LIST pages are attempts rather than retries, protocol retries equal loop iterations minus one,
and hostile key/error inputs cannot create new series.

Disabled-recorder transparency and deterministic accounting are normal CI gates. Recorder overhead is
judged against an interleaved, same-run disabled-recorder control with identical seeded work; no pass/fail
decision may depend on an otherwise idle or specially selected host. Absolute throughput and latency are
capacity evidence only when the deployment topology and resource limits are declared with the result.

SP-06 is a negative cache spike, not a release performance claim. The deterministic single-page BlobStore
model assigns each
physical request a fixed 25/100 ms cost without sleeping, so it identifies request shape and computes modeled
p95 reproducibly. Its ignored harness output is separate from live E2 evidence; the live schema-v2 row carries
a null profile. No cache may land from this evidence:
avoidable reads exceed 70% and absolute modeled gain exceeds 50 ms, but relative p95 gain is only 8.97% to
11.69%, below 20%. The observed authority-head history amplification is a new
design input for constant-time head access and async bounded-parallel tail recovery.

Backend: `object_log_inmemory_projection` and `object_log_sqlite_projection`
(TD-004). Evaluated against the portable E0 contract.

| Parameter | Value |
|-----------|-------|
| Commit-latency-bound sweep | run at ≥ 4 configured bounds, including low-latency, balanced, and cost-optimized values (for example 1 ms, 5 ms, 20 ms, 100 ms or implementation-equivalent documented values) |
| Pass: commit-bound semantics | every acknowledged request is durable and visible within the declared commit-bound semantics; p50/p95/p99 are reported as topology-bound capacity evidence |
| Pass: progress and resources | exact logical operation counts, monotonic progress, and bounded memory/work queues hold for every bound/profile; throughput is reported but is not an absolute release threshold |
| Pass: cost | $/billion-commands and object/log requests per billion commands reported for each latency bound; the cost-optimized point beats `postgres_native` at high sustained volume (ADR-001 cost table) |
| Pass: recovery | rebuild an exact 10M-item projection from snapshot + log tail with checksum/count/order equality, monotonic replay progress, and bounded memory/work queues; wall time is capacity evidence only |
| Pass: manifest fencing | a stale-epoch writer's manifest CAS commit is rejected; on a no-CAS object store the Postgres-held authoritative pointer atomically commits the head and assignment epoch, performs zero object-store manifest-head writes, fences stale writers, and remains directly readable through a fresh Postgres client after restart (TD-004) |
| Pass: transaction contract | success-visible, rejection-no-effect, and unknown-outcome replay invariants hold under the same bound sweep; no latency setting may weaken TP-003 transaction invariants |
| Pass: byte admission | Compare request-count-only evidence with global+tenant byte admission for small, target-sized, and oversize payloads under stalled-store and hot/cold-tenant contention. Global/tenant charges never exceed caps; a cold tenant progresses; median throughput regression is <=5% and p99 regression <=10%, including serialization paid before oversize rejection. |

### Recurrence under scale (both backend profiles)

Run the recurrence scale row under BOTH the Postgres-native profile (E1 shape)
and the object-log + SQLite profile (E2/E3 shape). This row substantiates that
recurring/never-terminal items participate in the scale envelopes without special
handling (recurring items participate in the per-queue local oldest-eligible
computation like any item).

| Benchmark | Required Evidence (both profiles) |
|-----------|-----------------------------------|
| Recurrence under scale (D4) | (a) **High-frequency immediate rearm** (`not_before` = now tight loop) sustains target throughput without version-monotonicity or projection corruption; (b) **idle recurring inventory** of N idle re-armed items does not inflate active-scope discovery, busy-poll, or `oldest_eligible_age_ms`, and `recurring_pending` is reported within its documented lag; (c) **purge under load** (targeted + `force` while leased), queue-local (one owner) and idempotent by `request_id`, completes within bound and leaves consistent tombstones. |

## Requirement Coverage Matrix

These rows extend the governing test traceability plan with the scale mechanism.
P0 items are referenced by name (not number) to stay robust to PRD renumbering.

| Requirement | Governing Artifact | Required Test Evidence |
|-------------|--------------------|------------------------|
| PRD P0 horizontal-distribution item | PRD / TD-001 / TD-003 / ADR-008 | E2 cross-queue scale-out: exact work and logical progress remain monotonic as owner count rises; the portable E0 contract holds for every queue under K-queue concurrency; single lease across owner reassignment. |
| PRD P0 performance-at-scale item | PRD / TD-001 / TD-002 / TD-004 | E1 and E2 preserve exact outcomes, queue-global progress, and bounded resources while distributing queues across owners. Throughput and latency remain declared-topology capacity evidence. |
| PRD P0 queue-density item | PRD / TD-001 / TD-002 / TD-003 / TD-004 | E2 queue density: the release command `scripts/perf/tp002-e2-density-kind.sh` proves exactly 1,000 cold queues plus one hot queue on one live objectlog/SQLite node using canonical 300,000-item hot windows, 8 hot connections, 8 cold workers, 4 server workers, and seed 42. Every cold queue retains an eligible item and completes a non-empty claim/finalize operation during loaded hot work; additional exact hot sustain windows keep load active until all 1,000 queues progress. Hot baseline/load/baseline counts reconcile, shared workers/tasks/connections stay within declared bounds, and quiet-host or fixed-speed gates are forbidden. Absolute rates, latency, and retention remain declared-topology capacity evidence. |
| TD-003 queue ownership | TD-003 | Deterministic queue-to-owner assignment, epoch fencing of a stale owner, graceful drain without loss/duplication, recovery, and stalled-queue visibility. |
| TD-004 object-log backend | TD-004 / ADR-001 | E3 latency/cost/recovery; commit-latency-bound sweep; manifest-CAS or authoritative Postgres-pointer current-epoch fencing; passes the shared TD-001 backend conformance suite. |
| Per-queue local progress (D1) | TD-001 / TD-003 | Each queue's oldest-eligible age is computed locally on its owner (gate-aware); the oldest item is claimed before the bound; no cross-shard aggregation. |
| TD-006 client routing | TD-006 / TD-003 | A wrong-node command is `-MOVED`-redirected to the queue's owner and converges in one hop; a stale/misrouted write is fenced, never corrupting state. |
| Recurrence under scale (D4) | TD-001 / TD-002 / TD-004 | Recurrence scale row passes under both backend profiles: high-frequency rearm, idle inventory bound, queue-local purge under load. |
| Shared backend conformance | TD-001 | `postgres_native`, `object_log_inmemory_projection`, and `object_log_sqlite_projection` pass the same TD-001 shared backend conformance suite (core + transaction contract + log / relational-reconnect-durability classes, including group/cohort, `same_group_key`, ownership/fence, and recovery rows) before any is selectable by backend profile. |

## Named Test Suites

Implementation beads should create or extend these suites:

- `queue_ownership_fencing_tests`
- `queue_reassignment_drain_tests`
- `per_queue_progress_tests`
- `routing_redirect_tests`
- `object_log_commit_recovery_tests`
- `object_log_latency_cost_matrix_tests`
- `external_transaction_contract_matrix_tests`
- `performance_cross_queue_scale_out_tests` (replaces the retired `performance_multi_shard_scale_out_tests`)
- `performance_single_deployment_baseline_tests`
- `queue_density_single_node_tests`
- `recurrence_scale_both_profiles_tests`

## Scale Evidence Requirements

Scale benchmarking must include:

- single-deployment exact lifecycle outcomes, monotonic progress, and bounded
  resources under the portable E0 contract, with throughput/latency reported as
  declared-topology capacity (E1);
- cross-queue scale-out at ≥ 3 owner-node counts, reported as aggregate accepted
  write/claim rate per owner count, scaling monotonically with owner count (E2);
- the portable E0 correctness, progress, and resource contract preserved for
  every queue as active-queue count and total load grow (E2);
- queue density: at least 1000 cold queues plus one hot queue on one node, exact
  cold/hot lifecycle completion, every queue progress-eligible, same-run
  degradation within its declared envelope, and all background work multiplexed
  onto bounded shared pools (E2, `queue_density_single_node_tests`);
- per-queue local progress: each queue's oldest-eligible item claimed before the
  bound from its owner's local computation (E2);
- owner failover/fencing and stalled/unowned-queue visibility as a progress-bound
  violation, with epoch-fenced recovery and no double-lease across reassignment
  (E2 / TD-003);
- client routing redirect convergence in a single hop and fence-safety of a
  misrouted write (E2 / TD-006);
- object-log group-commit ack latency across the commit-latency-bound sweep,
  $/command and object/log requests per billion commands at high volume, and
  10M-item projection rebuild time for each committed object-log projection
  variant (E3);
- manifest-CAS fencing or, on no-CAS object stores, fencing through the
  Postgres-held authoritative manifest pointer with zero object-store
  manifest-head writes and direct fresh-client restart reads (E3);
- external transaction-contract invariants under the E3 latency-bound sweep, so
  lower latency or lower cost configurations cannot publish weaker semantics;
- recurrence under scale on both backend profiles.

## Workload profile — Seventh Sense (RESP black box)

Product-shaped black-box profile for **jobs / actions / scheduled_actions** over
the RESP worker surface only:

- Profile: `docs/perf/workload-seventh-sense-actions-scheduler.md`
- Harness: `examples/python-resp` suite `ss` (`SS_N=5000` smoke;
  `./examples/python-resp/scripts/start_ss_service.sh`)
- Hard bars: insert / mutate / point-query / drain exactness on three bootstrap
  queues (`ss:jobs`, `ss:actions`, `ss:scheduled`)
- Soft latency: sub-second p95 defaults on smoke (capacity + regression); not a
  substitute for E1–E3 Class A release stamps

## Manual or Deferred Evidence

The following are not required before the first implementation bead but must be
covered before claiming product validation:

- Seventh Sense production scheduling SLA for concrete `progress_bound_ms`
  validation (partially proxied by the RESP black-box drain timeout and first-
  claim latency in the profile above; full bound remains library/metrics).
- P1 operator redrive, purge, repair, and archive APIs, and any P1
  operator/compatibility-adapter discovery surface. (The native
  `DiscoverActiveScopes` operation is P0/native-service per PRD and API-001 and is
  NOT deferred; only operator/adapter-facing discovery surfaces remain P1.)
- Kafka/Redpanda and DynamoDB backend conformance (later design targets).

Object-log and SQLite projection scale profiles are NO LONGER deferred: they are
committed v1 evidence via E2/E3.

## Scale-Claim Review Checklist (docs lint)

A document fails review if any of the following hold:

- It asserts "horizontal scale", a write rate, or "10M hot queue" without
  referencing an evidence record (E0–E3) that names deployment shape + workload
  envelope + substantiating artifact.
- A PRD or product-vision scale sentence names a storage backend, profile, scale
  mechanism, or SQL.
- A scale claim in any document lacks an E-record ID.

Reviewers MUST reject documents matching any rule above.

## Exit Criteria

Before scale claims are published, the referencing evidence records must pass
against the portable E0 correctness, progress, and resource contract: E1 for the
single-deployment envelope, E2 for the horizontal envelope (including every-queue
progress under K-queue concurrency),
and E3 for the object-log latency/cost/recovery profile. A scale claim in any
document must cite at least one evidence record (E0–E3) and, where it asserts a
benchmark outcome, the named scale test suite that produces it. A horizontal-scale
claim MUST NOT be substantiated by `postgres_native` alone.

## Resolved Decisions

- Cross-owner throughput and efficiency are published capacity observations, not
  universal bars. Release qualification uses the portable E0/E2 contract and
  never waits for an operator to select a machine-speed threshold.
- Object-log remains required for E2. A `postgres_native` comparator may be
  recorded when useful but is not required to qualify the portable envelope.

The queue-density target is **at least 1000 cold queues plus one hot queue on one
node**. Every queue meets its progress contract, all lifecycle counts reconcile,
and shared resources remain bounded; aggregate capacity is reported for that
node without extrapolation.
