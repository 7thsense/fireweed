---
ddx:
  id: tp-verification-acceptance-criteria
  depends_on:
    - prd
    - api-native-client-interface
    - api-operator-repair-contract
    - adr-cqrs-log-projection-storage-model
    - adr-auth-tenancy-and-storage-isolation
    - adr-rust-workspace-and-toolchain-policy
    - adr-granularity-mapping-and-claim-domain
    - adr-queue-as-shard-unit-and-projection-families
    - adr-full-async-storage-boundaries
    - adr-async-commit-strategy-and-dispatch
    - adr-turso-derived-projection
    - td-storage-architecture-backend-contracts
    - td-postgres-native-reference-mode
    - td-sharding-and-shard-ownership
    - td-resp-wire-adapter
    - td-s3-object-log-sqlite-projection-mode
    - td-object-log-turso-projection
    - tp-governing-test-traceability
    - tp-scale-substantiation
  review:
    self_hash: 450177278bfc6a0d50fa4c5395dea18fc6dc7738087d88bef7b062ce5fce81ab
    deps:
      adr-async-commit-strategy-and-dispatch: 61bf761b8f8b84581b174eb8f1c64a8893ede0dce9353707fb284f751fb82b5e
      adr-auth-tenancy-and-storage-isolation: 822b3589f2ae4a413ffb4bce8cd46991d733951968f368fd58445d0de5dae950
      adr-cqrs-log-projection-storage-model: 849c0bd7e15200ab056c2e5fcedb4b04a116aba520993fb4bab63b1195146107
      adr-full-async-storage-boundaries: 26d2c37c96eb0801dbb99e4a02213ecfa747aa533572acde3917801a13cebfcd
      adr-granularity-mapping-and-claim-domain: 29444ade97bb5bce95a3f9d3c8878f5dc1ec2ea0bfe562f914ae17ff84984a18
      adr-queue-as-shard-unit-and-projection-families: 50fb11c85cbf40fa182469b036ef5210b304f330171a17ab371ae485524cb924
      adr-rust-workspace-and-toolchain-policy: 7d743ad4ee99e4fb53736f83eb854924be3af511a439d1e510eb1135351461eb
      adr-turso-derived-projection: 76ec5fe8523c4fe831441229aa5f09f0bf966ac3849174764a7ba2c2d805f22a
      api-native-client-interface: ae6c682dbf6e269b6792351f1677477f2324fb24cb4cc4f85392f6369fd43b0b
      api-operator-repair-contract: 92d0dae8debf7fc9ac68fae06fdbe6d9a330f2914a58329c046331da9d5b4c6e
      prd: 2d97b05f9c0c0db576149bdfef21c729d66e07dbb674c95f6b7135ddcffa3b91
      td-object-log-turso-projection: 1e3623771c800e9d2c6874c19e94103d00c165f1afdd27ece4760fb43f6f7f69
      td-postgres-native-reference-mode: 1b657638258f7d3fa15e46b7536d33d766ade1a0948a32598dc5c9ae65b7828b
      td-resp-wire-adapter: d33d11d4e7e087384828e3ca3289d4f0b7bb6aefd88a4245ddb7f441f0706bc6
      td-s3-object-log-sqlite-projection-mode: 56d80c3e6ad5ab54460e300fdf4ddfe535dc75a47b0a2a0e32d0de46c38c7e49
      td-sharding-and-shard-ownership: b98590bc7a51f8e904052d64aaa6ab4d8a9c9729d155d17ee0823ffcf6b64a0d
      td-storage-architecture-backend-contracts: b1d17cc3481f52097ea0b2233a4a0e7bfa1512381c0b1fed7b3830fd3f02cc4e
      tp-governing-test-traceability: a987698e797f33f52168aba5ba54f41bcc18bd3fcabe278af085afdea7b82768
      tp-scale-substantiation: e0ca180cb81c98e7c451341f1ea912bf152ac2c75d422a3b315516fc9f8ee7d3
    reviewed_at: "2026-07-20T20:00:41Z"
---

# Test Plan: TP-003 Verification and Acceptance Criteria

## Current object-log durable-format gate

Fireweed has one object-log frame and manifest shape. The verification suite MUST prove current-format
goldens, CRC32C/SHA-256 integrity, exact manifest/key identity, recovery-index replay, restart, branch,
retention, and deletion-watermark behavior. Retired frame versions, missing or unknown manifest fields, and
retired namespaces appear only as negative fixtures and MUST fail closed. No environment variable or public
API may select a durable format.

## Scope

TP-001 (governing traceability) says **what** to test and which suite covers it.
TP-002 (scale substantiation) sets the scale/density bars. This plan, TP-003,
makes correctness **quantifiable**: it turns each governing requirement into a
build-gating **acceptance criterion** (`AC-*`) with concrete inputs and a numeric
pass bar, names the **safety invariants** (`INV-*`) that stress and property tests
must hold to zero violations, and defines the **CI quality gates** a build must
turn green. A feature/bead is "verified" only when every acceptance criterion it
claims is green and recorded in the verification ledger.

This plan exists so implementation beads can be driven toward verified correctness
rather than "tests pass": every number here is a target a build is measured
against. It satisfies the `verification` concern's running-system evidence
requirement.

## 1. Quantification Conventions (defined once, reused everywhere)

| Symbol | Meaning | Standard values |
|--------|---------|-----------------|
| `C` | concurrent workers / clients | {1, 8, 64, 256} |
| `T_smoke` / `T_soak` | stress durations | 60 s (PR) / 600 s (nightly) |
| `S` | resident item-set size | {10K, 1M, 10M} |
| `K_q` | concurrent active queues | {10, 100, 1000} |
| `N_kill` | induced worker/process kills in a run | ≥ 1000 |
| latency | reported percentiles | p50, p95, p99, p999 |
| `props` | property-test cases per property | ≥ 10,000 (PR), ≥ 1,000,000 (nightly) for priority encoding/ordering |
| `fuzz` | fuzz time per target | ≥ 10 s (PR), ≥ 30 min/target (nightly) |
| seed | randomized tests | seeded, recorded RNG; a failing seed becomes a fixed regression case |
| `L_metrics` | documented lag budget for approximate metrics/counts | ≤ 5 s (PR smoke), ≤ 1 s (release), or a stricter backend profile value |
| `L_apply` | object-log cross-operation apply-lag budget | the configured TD-004 commit/apply budget for the backend profile |

Every benchmark/stress result MUST be reported **with telemetry enabled** (the
o11y-otel concern), and MUST record the command, exit status, environment, and the
seed (verification ledger, §6).

## 2. Safety Invariants (zero-violation bars)

These hold under the stress matrix (`C=256`, `S∈{1M,10M}`, `T_soak`, `N_kill≥1000`,
skewed priority + group distributions) for every committed backend profile.
Violation count MUST be **0**.

| ID | Invariant | Quantified check |
|----|-----------|------------------|
| INV-1 | Single active lease (FR-25) | Count of items observed with ≥2 simultaneously-active leases = **0** over the full run. |
| INV-2 | No lost work (FR-27/FR-28) | After `N_kill` crashes, every accepted item is in a terminal state or eligible/redeliverable; lost items = **0**. |
| INV-3 | No conflicting terminal | An item that reached a terminal state is never observed in a different terminal state later; occurrences = **0**. |
| INV-4 | Progress bound (FR-9/FR-12) | Eligible items claimed after `eligible_age_ms > progress_bound_ms` = **0** (queue-global; across skew/group/relaxed profiles). |
| INV-5 | Idempotency (API-001/API-002) | Replay of any mutating `request_id` (or async `operation_id`) yields byte-identical committed state and an equivalent response; divergences = **0**. |
| INV-6 | Ordering | Strict-queue claim order inversions vs the spec ordering tuple = **0**; bounded-relaxed stays within the queue's declared rank-error bound (measured). |
| INV-7 | Group/cohort atomicity | Partial `whole_group`/`whole_cohort` leases = **0**; cohort members leaked to another claim unit = **0**. |
| INV-8 | Tenant isolation (ADR-002) | Cross-tenant reads/writes across the negative matrix = **0**. |
| INV-9 | Group co-residency by construction (ADR-008) | The queue is the unit of sharding, so a `group_key` whose members are split across more than one owner = **0** (co-residency holds by construction; no placement flag). |
| INV-10 | Durable ack (ADR-001/TD-001) | Acknowledged commands missing after crash + replay = **0** (kill-after-ack). |
| INV-11 | Lease fence on operator action (API-002) | Operator mutation of a leased item that leaves the old lease usable = **0**. |
| INV-12 | Success visibility (API-001 external transaction contract) | A successful mutating response whose accepted effects are not visible to the next read/claim/idempotency replay on the authoritative owner = **0**. |
| INV-13 | Rejection no-effect (API-001 external transaction contract) | A structured envelope rejection with any durable item effect, or a per-item rejection with a durable effect for that item = **0** after restart/replay. |
| INV-14 | Unknown outcome resolves once (API-001 external transaction contract) | Retrying an interrupted/timed-out mutating `request_id` produces more than one committed state-machine transition, or fails to resolve a committed original result within retention = **0**. |

SP-03 verification reuses the SP-02 phase-addressed store and independent model. It covers exact-reread
resolution of create-only effect-then-error, CAS loss, crash after authority-head fence publication, reopen,
non-serving `PendingFence` admission shutdown, an already-admitted old-epoch prefix before the storage fence,
old-epoch retry rejection after that fence, floor-before-delete, no watermark past a present/failed-delete segment, stale-cache
non-authority, and retained-address collision. The 128-seed/48-operation generated suite and typed corpus
expect confirmed success—not unknown outcome—when the exact authoritative reread proves the create landed.
`pending_fence_gap_has_one_safe_old_prefix_then_fences_stale_retry` is the non-skipping CP/storage-gap test;
the live Postgres/S3 equivalent is
`pending_fence_gap_linearizes_old_commit_before_storage_fence_then_rejects_stale_retry`. The maintenance
budget test `deletion_watermark_proof_request_budget_is_linear_and_bounded` bounds current maintenance
GET, PUT, LIST, and DELETE growth linearly in reclaimed entries.
`authority_mode_deletion_proof_cost_ignores_total_head_history` uses underlying-store counters to prove the
incremental completed-prefix proof adds no LIST and costs only per reclaimed entry at 8 versus 128 retained
head versions. This does not claim total authority-mode maintenance is O(reclaimed): the default
`read_manifest_head` recovery step still scans retained head versions and remains a release-scale benchmark/
optimization condition. `successful_create_performs_zero_rereads` protects the successful create-only hot
path. Retired watermark-cache objects are negative fixtures and must make open fail closed.

## 3. Acceptance Criteria by Area

Each `AC-*` has Setup (quantified) → Assertion → Pass bar. A bead cites the `AC-*`
it turns green. Suites are named in TP-001.

### 3.1 Core semantics (`fireweed-core`)

| AC | Setup | Assertion | Pass bar |
|----|-------|-----------|----------|
| AC-CORE-1 priority encoding | `props ≥ 1,000,000` over each priority model (timestamp/int64/decimal/string), both directions | `priority_sort` byte order == declared total order; round-trip stable | 0 mismatches; 100% of generated pairs |
| AC-CORE-2 lifecycle transitions | exhaustive state×event matrix | only legal transitions accepted; illegal rejected with typed error | 100% of matrix; 0 illegal accepted |
| AC-CORE-3 idempotency keys | `props ≥ 10,000` | duplicate `client_item_key` converges; replayed `request_id` no-ops; conflicting body → `request-id-conflict` | 0 violations |
| AC-CORE-4 retry exhaustion | `retry_policy.max_attempts ∈ {1,3,10}` | attempt `max_attempts+1` makes item terminal `failed` exactly once | 0 off-by-one |

### 3.2 Claim / lease / eligibility

| AC | Setup | Assertion | Pass bar |
|----|-------|-----------|----------|
| AC-CLAIM-1 single active lease | `C=256`, `S=1M`, `T_soak` | INV-1 | 0 double leases |
| AC-CLAIM-2 lease expiry redelivery | `N_kill≥1000` mid-lease | item re-eligible after expiry; progress clock preserved (FR-11) | 0 lost; 0 progress-clock resets |
| AC-CLAIM-3 eligibility gates | future `not_before`, retry backoff, metadata blockers, dynamic gates | ineligible items never claimed; eligible age not accrued while ineligible | 0 violations (INV-4 inputs) |
| AC-CLAIM-4 strict ordering | `C=64`, skewed priority | INV-6 strict | 0 inversions |
| AC-CLAIM-5 bounded-relaxed bound | `C=64`, relaxed queue | rank error ≤ declared bound; oldest eligible still claimed ≤ `progress_bound_ms` | within bound; INV-4 holds |
| AC-CLAIM-6 lease renewal | active leases at durations {short, max}, expired leases, fenced leases after operator repair/purge | renew extends only the active matching lease; stale/expired/fenced tokens fail without extending visibility; renewal preserves single-active-lease safety | 0 stale renewals accepted; 0 duplicate active leases; renewed lease expiry equals requested duration capped by queue policy |

### 3.3 Group-batching, cohort, gates (gap features)

| AC | Setup | Assertion | Pass bar |
|----|-------|-----------|----------|
| AC-GRP-1 whole-group | `max_groups=300`, multi-task groups (Marketo lead shape) | up to 300 whole eligible groups; never a partial group; `batch-too-large` when next group won't fit | INV-7; 0 partial groups |
| AC-GRP-2 same_group_key is filter | one group, `max_items` < group size | returns a partial group (item filter), never whole-group semantics | matches spec; 0 conflations |
| AC-COH-1 whole-cohort atomic | cohorts of size {2,10,1000}; `C=64` | whole complete cohort leased under one shared lease, or none; members never individually claimable | INV-7; 0 leaks |
| AC-COH-2 cohort completion bound | `completion_bound_ms ≤ progress_bound_ms` | incomplete cohort never blocks past bound; `expire_cohort` fires | 0 bound violations |
| AC-GATE-1 O(1) gate flip | `S=10M`, flip a gate key affecting `G` groups | flip touches 0 item rows; claim/discovery reflect gate at read; work and same-run latency do not grow with `G` | 0 item-row writes; constant query/command count; largest-`G` p99 <= 1.25x interleaved smallest-`G` control |
| AC-GATE-2 gate eligibility | gated items present | gate-blocked items never claimed; oldest-eligible advances past blocked (not excluded) | INV-4 holds with gates |

### 3.4 Recurring (g5)

| AC | Setup | Assertion | Pass bar |
|----|-------|-----------|----------|
| AC-REC-1 rearm | high-frequency rearm loop, `T_soak` | `rearm` releases lease, sets `eligible_since=max(commit,not_before)`, resets per-cycle retry, bumps version, never terminal | 0 version-monotonicity breaks; 0 spurious terminals |
| AC-REC-2 idle inventory | `S=1M` idle re-armed items | idle items excluded from oldest-eligible age and retry backlog; `recurring_pending` within documented lag | 0 inflation; lag ≤ documented bound |
| AC-REC-3 purge teardown | `PurgeItems` per-key, incl. `force` while leased (queue-local on the owner) | row removed; tombstone + replay-safe; duplicate purge → `not_found`; late finalize → `not_found` | 0 resurrected items; idempotent |

### 3.5 Discovery + queue ownership / routing (g4, TD-003 / TD-006)

| AC | Setup | Assertion | Pass bar |
|----|-------|-----------|----------|
| AC-DISC-1 ranking | multi-queue (tenant-wide) and single-queue group granularity, gated | tenant-wide top-N across queues, and owner-local group top-N for one queue, by oldest-eligible age via the single summary; gate-current advance | matches ground truth; 0 mis-ranks at terminal read |
| AC-OWN-1 per-queue local progress | one queue under skewed/group load; the oldest-eligible item is not in the hot group | oldest-eligible item claimed ≤ `progress_bound_ms` from the owner's local computation (no cross-shard aggregation) | INV-4; 0 violations |
| AC-OWN-2 fence + reassignment | reassign a queue's owner under load; `N_kill` mid-drain | single writer (INV-1); a deposed owner's stale-epoch appends rejected; no double-lease across drain; new owner recovers from snapshot + log tail | 0 stale-epoch commits; 0 double leases; 0 lost items |
| AC-ROUTE-1 client routing redirect | client addresses a queue on a non-owner node; epoch advances mid-flight | wrong-node command is `-MOVED`-redirected to the recorded owner and converges in one hop; a misrouted write is epoch-fenced (never corrupts state); a stale read is bounded-stale, never authoritative (TD-006 §1A) | 0 misrouted durable writes accepted; redirect converges |

### 3.6 Operator contract (API-002)

| AC | Setup | Assertion | Pass bar |
|----|-------|-----------|----------|
| AC-OP-1 repair fences lease | `RepairItems force_*`/`clear_lease` on leased items, `C=64` | INV-11; worker sees `stale_lease`; `item_version` bumped; `force_release` preserves progress clock | 0 usable stale leases |
| AC-OP-2 redrive | `RedriveItems` over `S=1M` terminal-failed (large match, bounded batches on the owner), incl. a future `redrive.not_before` | items return eligible with `retry_count_mode`; `eligible_since = max(commit_time, redrive.not_before)` (future-dated items accrue no eligible age until `not_before`); guards (`max_affected`/`expected_match_count`) enforced; async converges | 0 guard bypass; 100% convergence; 0 premature eligible-age accrual |
| AC-OP-3 bulk purge | `PurgeQueueItems` selector over `S=1M`; `dry_run` first | `dry_run` side-effect-free + exact matched count; purge writes tombstones; queue-local bounded-batch partial-commit re-drives & converges; idempotent (`not_found`) | 0 side effects in dry_run; count exact; 0 resurrected items |
| AC-OP-4 async ops idempotent | replay create `request_id` ×100; `CancelOperation` mid-run | one `operation_id`; no second op; terminal-state counts exact; cancel never rolls back committed batches | 0 duplicate ops; counts exact at terminal |
| AC-OP-5 operator authz | (a) data-plane principal (lacking `operator:*`) attempts each operator op on its own authorized queue; (b) operator principal attempts an op in another tenant | (a) denied `operator-forbidden`; (b) denied `queue-forbidden`/`queue-not-found` with no existence leak (INV-8); audit record emitted w/o payload | 0 unauthorized successes; 0 existence leaks |
| AC-OP-6 pause/resume | `PauseQueue` under `C=64` active claimers, then `ResumeQueue`; crash + recover mid-pause | while paused, `BatchClaim` returns empty + `queue_paused=true`, pushes/finalizes still succeed, no eligible age accrues (single Eligibility Precedence cond. 0); pause survives restart; resume restores claims | 0 claims while paused; pause durable across `N_kill`; 0 second eligibility definition |
| AC-OP-7 archive + retention | `ArchiveItems` over terminal `S=1M`, then `PurgeQueueItems`; `RunRetention` | archive is idempotent (`archived`), marks retained/exports before purge; archived-then-purged order safe; `RunRetention` reclaims only within policy and returns exact counts | 0 over-retention deletes; archive idempotent; counts exact |
| AC-OP-8 operator inspection + token redaction | `GetItem`/`ListItems` over leased+terminal items; inspect raw storage/logs | full item state returned; `page_token` stable; lease **token never returned** by inspection and never logged (INV/AC-SEC-2) | 0 plaintext tokens surfaced; pagination stable |
| AC-OP-9 operator cohort wholeness | each of `RepairItems`/`RedriveItems`/`PurgeQueueItems`/`ArchiveItems` targets a strict subset of a live cohort, with and without `cohort_whole=true` | without `cohort_whole`: subset members rejected `conflict` (`cohort-partial-target`), 0 mutated; with `cohort_whole=true`: whole cohort mutated atomically (INV-7); 0 cohort split — verified for all four operations | 0 partial-cohort mutations; 0 leaks |

### 3.7 Security / tenancy

| AC | Setup | Assertion | Pass bar |
|----|-------|-----------|----------|
| AC-SEC-1 tenant isolation | negative matrix: create/read/push/update/claim/renew/finalize/discover/operator across tenants | INV-8; storage queries carry tenant scope | 0 cross-tenant access |
| AC-SEC-2 lease token handling | inspect storage + logs | tokens stored only as hashes; never returned by inspection; not logged | 0 plaintext tokens |

### 3.8 Portable capacity and degradation gates (single deployment)

Run under `C=64`, `S=1M`, telemetry on, `postgres_native`, with interleaved
same-run controls. Absolute throughput and percentile values are reported with
the host/topology as capacity evidence. They are not release thresholds. (Full
scale/density/horizontal coverage is TP-002 E0–E3.)

| AC | Operation | Pass bar |
|----|-----------|----------|
| AC-LAT-1 | batch push / update / claim / finalize | exact operation counts and monotonic progress; loaded throughput >= 0.80x and p99 <= 1.25x the interleaved identical-work control; declared resource bounds hold |
| AC-LAT-2 | gate flip (`SetGates`) | 0 item-row writes and constant logical work; largest affected-group p99 <= 1.25x the interleaved smallest-group control |
| AC-LAT-3 | `DiscoverActiveScopes` top-N | no full scan (query plan asserted); exact authorized top-N; logical reads and same-run latency scale with requested top-N, not `S=10M` |
| AC-LAT-4 | telemetry overhead | core-op p99 with telemetry on ≤ 1.15× telemetry off |

### 3.9 Observability correctness

| AC | Setup | Assertion | Pass bar |
|----|-------|-----------|----------|
| AC-OBS-1 metrics ground truth | `S∈{10K,1M}`, mixed pending/future/leased/retry/complete/failed/recurring states, both backend profiles, telemetry enabled | `GetQueueMetrics` lifecycle counts, active leases, retry backlog, recurring counters, throughput/latency buckets, and progress-bound risk are compared to a ground-truth scan/checkpoint for the test fixture; `oldest_eligible_age_ms` is exact-on-read as of the reported frontier | exact fields have 0 mismatches; approximate count fields converge within `L_metrics`; throughput/latency values match the harness-recorded operation log within documented aggregation tolerance |

### 3.10 External transaction contract under duress

These criteria are the release gate for API-001's backend-independent mutation
contract. The route register is the Cartesian product of five canonical logs
(`memory`, `sqlite`, `postgres`, `filesystem`, `s3`) and three canonical
projections (`memory`, `sqlite`, `postgres`): exactly **15** cells. Every cell
MUST execute the common criteria and its durability-class assertions. Class A
has 12 durable-log rows; Class B has three memory-log rows and proves
projection-only persistence without claiming log replay. An unregistered row,
ignored test, missing required feature/fixture, `n/a`, or process-successful test
with no assertions is a release failure. There are zero silent skips.

Evidence binds the exact cell, durability class, barrier disposition, test
revision, command, exit status, assertion count, and artifact digest. Historical
`tp003-ac-txn-matrix*.jsonl` and legacy profile rows may be retained as
provenance, but they do not qualify this 15-cell register. No current complete
claim is made until all required rows pass on one release-candidate revision.

| AC | Setup | Assertion | Pass bar |
|----|-------|-----------|----------|
| AC-TXN-1 success durable + visible | In every cell, run each mutating operation (`CreateQueue`, `BatchPush`, `BatchUpdate`, `SetGates`, `BatchClaim`, `BatchRenewLeases`, `BatchFinalize`, `PurgeItems`) and verify immediate visibility. Then kill/restart: Class A recovers from its log; Class B reopens from its selected projection without log replay | INV-12 everywhere; INV-10 for Class A; API-005 Class B persistence boundary | 0 read-after-success gaps; 0 missing Class A acknowledgements; memory-log × durable-projection rows preserve latest projection state; memory × memory restarts empty and exposes no replay/history capability rather than claiming durability |
| AC-TXN-2 rejection no-effect | Generate envelope-invalid batches, per-item invalid/conflict/stale cases, capacity/unavailable paths, and commit-timeout paths; restart and replay from durable state | INV-13 | 0 durable effects for rejected envelopes or rejected items; accepted siblings in partial batches retain normal success semantics |
| AC-TXN-3 unknown outcome replay | Drop responses, time out clients, kill service processes, and duplicate retry each mutating `request_id` across before-append, after-append-before-commit, after-commit-before-apply, after-apply-before-response, and after-response cut points | INV-5 and INV-14 | same `request_id` resolves to exactly one committed result or a fresh execution when no original commit exists; 0 duplicate state transitions |
| AC-TXN-4 object-log crash-point matrix | For the six `filesystem`/`s3` log cells and each commit-latency-bound setting from TP-002 E3, inject failures before segment write, after segment write before manifest, after manifest before projection apply, during projection apply, after projection apply before response, during snapshot write, during owner reassignment, and during native conditional manifest commit | INV-1, INV-2, INV-10, INV-12, INV-14 | 0 lost accepted items; 0 duplicate active leases; committed commands replay exactly once; orphan segments ignored or reconciled per TD-004; stale-epoch commits rejected; a provider lacking native conditional publication fails configuration before I/O |
| AC-TXN-5 `Strict` response barrier | Run all 15 cells with `ResponseBarrier::Strict`; inject selected-projection apply failure and cuts after log append, after durable commit, after projection apply, and before response; cover push, claim, renew, finalize, retry/release, update, purge, and operator mutations with same and conflicting `request_id` bodies | Provider-neutral strict apply/poison contract, INV-5, INV-10, INV-12, INV-14 | No success precedes selected-projection visibility; apply failure returns no success and the cell fails closed or recovers from its class authority; same-body retry yields one result, conflicting body returns `request-id-conflict`; final state and error semantics do not vary by projection implementation |
| AC-TXN-5A `AsyncProjection(AsyncProjectionSpec)` response barrier | Give each of the 15 cells an explicit async-barrier disposition. For every valid tuple, inject lag, out-of-order scheduler wakeups, durable apply failure/poison, crash before/after the response boundary, debt beyond each of the five configured bounds, and operator mutation while poisoned; for a tuple that cannot preserve its durability-class contract, assert typed rejection during pre-I/O durability validation rather than skip it | Provider-neutral async success barrier, ordered apply, poison/fail-closed, bounded debt/backpressure, unknown-outcome contract, INV-5, INV-10, INV-11, INV-12, INV-14 | Every matrix row reports either the complete runtime assertions or its specified construction rejection; valid rows return success only after the class authority plus response state can resolve replay, apply committed batches in order exactly once, advance projection high-water only after complete apply, preserve read-after-success through the serving state, and apply typed backpressure without acknowledging extra commands; same-body replay converges and conflicting bodies fail |
| AC-TXN-6 implementation-combination parity | Run the same generated operation history and applicable failure schedule across all 15 cells, then compare final visible queue state, idempotency records, terminal outcomes, active leases, and metrics exact fields | backend-independent API semantics | no semantic divergence except the documented Class A/Class B persistence boundary and latency/cost/recovery metadata; fireweed callers need no backend-specific repair path |
| AC-TXN-7 latency-bound is not a correctness knob | Repeat AC-TXN-1..6 across the TP-002 E3 commit-latency-bound sweep | invariants unchanged by latency/cost setting | 0 invariant deltas across lower-latency vs cost-optimized settings |
| AC-TXN-8 async cancellation cuts | For every backend class cancel before append, after staging/before commit, during commit, after durable append/before eventual apply, and while waiting for serialization; replay the same and conflicting `request_id` | ADR-015 cancellation and unknown-outcome contract | pre-commit cuts leave no durable effect; commit cancellation converges to exactly one outcome; eventual append repairs exactly once; conflicting replay fails; no stranded waiter or poisoned lock |
| AC-TXN-9 runtime non-blocking boundary | Inject slow blocking-driver and native-async I/O for every public SQLite, Postgres, filesystem, and S3 adapter on a single-thread Tokio runtime with a heartbeat and bounded timeout; internal Turso compatibility may run as additive evidence only | ADR-015 adapter boundary | heartbeat continues within its documented scheduling tolerance; no runtime-worker stall |
| AC-TXN-11 async commit strategy and dispatch | Attempt atomic-profile construction with separate append/apply, cancel a caller after owned-task submission, stall one queue at each mutation phase, and drive another queue concurrently | ADR-017 strategy, submission, and queue-gate contract | invalid atomic composition is unrepresentable or rejected at construction; submitted commit resolves exactly once; stalled queue does not stop unrelated queue progress; no duplicate claim planning or stranded permit |
| AC-TXN-12 object-log byte admission | Generate acquire/release/cancel traces; run small/target/oversize commands through stalled-store, epoch-fence, watermark self-fence, same-epoch CAS-loss, seal-success, post-seal apply-failure, caller-drop, close, and drain paths; contend hot and cold tenants/queues | ADR-017 byte admission, TD-004 buffered-byte admission, INV-10, INV-12, INV-13 | global and tenant permit conservation returns to zero after drain; charged bytes never exceed caps; oversize is permanent invalid-request; exhaustion/timeout is typed retryable backpressure; retained records never outlive their permit; unrelated tenant progress and queue FIFO remain intact |
| AC-TXN-10 forbidden lock/bridge structure | Search production storage paths and run the dependency guard | ADR-015 structural boundary | no `std::sync::MutexGuard` crosses an await; no nested runtime/block-on bridge; blocking adapters offload whole transactions rather than statements |

### 3.11 Product end-to-end workflow validation

These are the product-facing "does fireweed work?" gates. Lower-level ACs prove
individual primitives; these workflows prove those primitives compose through the
native service API against committed backend profiles. Each workflow MUST be
automated. No product E2E may replace the shared conformance suite; it sits above
conformance and exercises the real service, auth context, backend wiring,
metrics, and failure behavior.

Unless a row states otherwise, run each product E2E against both committed
backend profiles (`postgres_native` and `object_log_sqlite_projection`) with
telemetry enabled. Product E2E commands are parameterized with environment
variables, not hidden test defaults:

- `FIREWEED_BACKEND_PROFILE=postgres_native|object_log_sqlite_projection`
- `FIREWEED_E2E_SCALE=smoke|release`
- `FIREWEED_E2E_SEED=<recorded-seed>`

`FIREWEED_E2E_SCALE=smoke` is the PR-tier shape: small item sets, short runtime,
and deterministic failures while preserving the same workflow assertions.
`FIREWEED_E2E_SCALE=release` MUST use the row's release shape below. `N/A` means
that parameter is not part of that workflow's release evidence. The verification
ledger records the profile, scale, seed, command, topology, and measured values.

Failure-heavy ACs depend on a shared `fault_injection_harness_tests` capability
that can terminate worker/service processes at deterministic cut points, inject
duplicate request replay, force partial batch/segment append/commit outcomes and
owner-reassignment/epoch-advance events, and record the seed and failure
schedule. Build sequencing must create that harness before AC-CLAIM-2, AC-OWN-2,
AC-E2E-2, AC-E2E-3, AC-E2E-5, and AC-E2E-7 are claimed. AC-E2E-6 release evidence
separately depends on the cross-queue distribution (TD-003/ADR-008) and the
`queue_density_single_node_tests` harness from TP-002 E2; its smoke run may start
earlier, but its release bar cannot be claimed until that scale/density
infrastructure exists.

Approximate count/metric assertions use `L_metrics` from §1 and the exact/lagged
metric contract in API-001 (`GetQueueMetrics`, `DiscoverActiveScopes`) plus the
TD-001/TD-002/TD-004 projection rules. `oldest_eligible_age_ms` and discovery
oldest age are never approximate; count lag is bounded by `L_metrics`. For
`object_log_sqlite_projection`, unrelated-reader visibility additionally uses
`L_apply` from TD-004's cross-operation apply-lag contract.

Release shapes and topology (per ADR-008 the unit is the queue; horizontal
topology is cross-queue across owner nodes, never intra-queue shards):

| AC | Release shape | Required topology |
|----|---------------|-------------------|
| AC-E2E-1 | `S=1M`, `C=64`, `T_soak`, `N_kill>=1000` | Both committed profiles; single owner per queue, plus cross-queue distribution across ≥2 owners where the profile supports it. |
| AC-E2E-2 | ≥1000 groups, task counts per group in {1,3,10}, `max_groups=300`, `C=64`, `T_soak`, `N_kill>=100` during whole-group claim/finalize | Both committed profiles; group-batching enabled (co-residency by construction); object-log release run distributes the queues across ≥4 owner nodes; `postgres_native` may run single-owner unless the optional cross-queue comparator is configured. |
| AC-E2E-3 | cohort sizes {2,10,1000}, ≥10K cohorts, `C=64`, `T_soak`, `N_kill>=100` during whole-cohort claim/finalize | Both committed profiles; `cohort_policy.enabled` (co-residency by construction); object-log release run distributes the queues across ≥4 owner nodes for replay parity. |
| AC-E2E-4 | `S=1M` recurring singleton keys, `C=64`, `T_soak`, `N_kill>=1000` | Both committed profiles; object-log release run distributes the queues across ≥4 owner nodes; `postgres_native` single-owner run is sufficient for the Tier-1 profile. |
| AC-E2E-5 | `S=1M`, `C=64`, `T_soak`, `N_kill>=1000`, duplicate replay and owner-reassignment/epoch-advance injection enabled | Owner failover + epoch fence + snapshot/log-tail recovery (no fan-out; claims are single-owner-local). Both committed profiles run the crash/replay + failover subset; the object-log run additionally exercises manifest-CAS fencing under reassignment. |
| AC-E2E-6 | hot queue `S=10M`, one small eligible queue, `K_q=1000`, `C=64`, `T_soak`, `N_kill=N/A` | Cross-queue active-scope routing + ≥1000-queue density; object-log release run distributes the queues across owner nodes. `postgres_native` runs the single-deployment noisy-neighbor subset; cross-queue distribution is the headline density evidence (TP-002 E2). |
| AC-E2E-7 | ≥1M selected leased/terminal items across repair/redrive/purge/archive cases, `C=64`, async operation replay x100, `T_soak`, `N_kill>=100` | Both committed profiles; large selector mutations run queue-local in bounded batches; object-log release run distributes the queues across ≥4 owner nodes for partial async operation convergence. |
| AC-E2E-8 | `S=1M`, `C=64`, `T_soak`, skewed priority distributions, strict `int64` descending queue plus bounded-relaxed non-timestamp queue | Both committed profiles; single-owner is sufficient for generic non-timestamp product proof, with optional cross-queue comparator. |
| AC-E2E-9 | `S=1M`, one hot `group_key`, `max_items∈{1,25,100}`, paced claim cadence, `C=8`, `T_soak`, `N_kill=N/A` | Both committed profiles; single-owner is sufficient because the contract is admission behavior, not scale-out. |

| AC | Automated suite / command | Product workflow | PRD coverage | Pass bar |
|----|---------------------------|------------------|--------------|----------|
| AC-E2E-1 scheduled action delivery | `FIREWEED_BACKEND_PROFILE=<profile> FIREWEED_E2E_SCALE=smoke cargo test -p fireweed-service --test product_workflows scheduled_action_delivery_e2e -- --ignored` | Model `scheduled_actions`: create a timestamp-ascending queue, push items early before optimized send time, later `BatchUpdate` `priority`/`not_before`, close and reopen account/connector/campaign gates with `SetGates`, claim by `group_key`, renew/finalize, and assert queue metrics. Release scale: `S=1M`, `C=64`, `T_soak`, `N_kill>=1000`. | FR-1..FR-3, FR-7, FR-10..FR-12, FR-18..FR-28, FR-33..FR-34, FR-40..FR-46, FR-47a | INV-1..INV-4 = 0; accepted items are either correctly terminal, leased, or eligible/redeliverable after the run; schedule order matches timestamp priority; no gated item is claimed while its gate is blocked; same `queue_id` in a second tenant remains isolated and cross-tenant data-plane access is denied; metrics match terminal state within `L_metrics`. |
| AC-E2E-2 Marketo group-cardinality batching | `FIREWEED_BACKEND_PROFILE=<profile> FIREWEED_E2E_SCALE=smoke cargo test -p fireweed-service --test product_workflows marketo_group_batching_e2e -- --ignored` | Model a downstream API call that accepts up to 300 distinct lead groups: create a group-batching queue (`max_eligible_group_size` set), set `group_batching.max_groups=300`, load >=1000 lead groups with skewed priorities and multiple tasks per lead, discover active groups, then claim/finalize whole eligible groups under contention. (Co-residency holds by construction — every group is owner-local.) | FR-29..FR-32, FR-31a, FR-35, FR-47, FR-47b, FR-48 | INV-7 = 0 partial groups; each claim contains <=300 groups and <=`max_items`; group representatives are ordered by claim order; `batch-too-large` fires when the next whole group cannot fit; concurrent claimers do not duplicate groups. |
| AC-E2E-3 callback cohort execution | `FIREWEED_BACKEND_PROFILE=<profile> FIREWEED_E2E_SCALE=smoke cargo test -p fireweed-service --test product_workflows callback_cohort_e2e -- --ignored` | Model `actions_scheduled` callback batches: create a cohort-enabled queue (`cohort_policy.enabled`; co-residency by construction), push incomplete and complete callback cohorts, verify incomplete cohorts are hidden from claim/discovery, then claim/finalize complete cohorts atomically; run an expiry case for incomplete cohorts. | FR-32a..FR-32c, FR-47a, FR-47c, FR-48 | INV-7 = 0 cohort leaks; incomplete cohorts are never claimed or discovered; complete cohorts lease atomically under one shared lease; `completion_bound_ms <= progress_bound_ms` is enforced; expired incomplete cohorts become terminal `failed` with the required reason. |
| AC-E2E-4 jobs/connectors recurring singleton | `FIREWEED_BACKEND_PROFILE=<profile> FIREWEED_E2E_SCALE=smoke cargo test -p fireweed-service --test product_workflows jobs_connectors_recurring_e2e -- --ignored` | Model `jobs_queue` and `connectors_queue` poll-cursor rows: one logical item per job/connector key, repeated claim -> work -> `rearm` cycles with new `not_before`, idle periods, per-cycle retry exhaustion, `recurrence.until`, and `PurgeItems` teardown. | FR-36..FR-39, FR-49..FR-55 | No duplicate singleton rows; `item_version` increases monotonically across re-arms; `rearm` never consumes retry budget; idle recurring items do not inflate exact `oldest_eligible_age_ms`, and approximate recurring/retry counters converge within `L_metrics`; purge is idempotent and late finalize returns `not_found`. |
| AC-E2E-5 worker crash recovery | `FIREWEED_BACKEND_PROFILE=<profile> FIREWEED_E2E_SCALE=smoke cargo test -p fireweed-service --test product_workflows worker_crash_recovery_e2e -- --ignored` | Drive the native API with real service/backend processes while injecting worker exits after claim, service process exits after acknowledged append, duplicate request replay, owner reassignment / epoch advance under load, and restart/recovery. | FR-23..FR-28, FR-33..FR-39, API-001 idempotency, TD-001 durability, TD-003 fencing | INV-1, INV-2, INV-3, INV-5, INV-10 = 0 violations; acknowledged commands survive restart; replayed `request_id`s converge; expired leases redeliver without resetting eligible age; no accepted item is lost. |
| AC-E2E-6 noisy-neighbor and active-scope routing | `FIREWEED_BACKEND_PROFILE=<profile> FIREWEED_E2E_SCALE=smoke cargo test -p fireweed-service --test product_workflows noisy_neighbor_scale_e2e -- --ignored` | Run one hot queue with a 10M resident backlog, one small eligible queue, and `K_q=1000` active queues; declare a positive `progress_bound_ms` in every queue definition, read the persisted definitions back, and use `DiscoverActiveScopes` to route group claims while the hot queue is under load. | FR-1, FR-12, FR-40..FR-43, FR-48, TP-002 E1/E2 | Small-queue claims meet AC-LAT-1's exact-work and interleaved same-run degradation bars; persisted bounds equal the declarations; INV-4 holds with zero accepted-to-claim or discovery-age violations of those bounds; discovery ranks authorized active scopes by true oldest eligible age; unauthorized queues are excluded; shared worker, loop, connection, and task bounds hold. |
| AC-E2E-7 operator repair/redrive workflow | `FIREWEED_BACKEND_PROFILE=<profile> FIREWEED_E2E_SCALE=smoke cargo test -p fireweed-service --test product_workflows operator_repair_redrive_e2e -- --ignored` | Model an operator repairing production damage: pause queue, inspect leased and terminal items without exposing lease tokens, force-release or reschedule leased items, redrive failed items, dry-run and execute bulk purge/archive, resume queue, and observe async operation convergence. | API-002, FR-38..FR-43, FR-52, ADR-002 | INV-8 and INV-11 = 0; data-plane principals cannot perform operator actions; stale leases become unusable after repair; dry-run has no side effects; async operation replay returns the same `operation_id`; audit records omit payload and lease tokens. |
| AC-E2E-8 generic priority and bounded-relaxed service workflow | `FIREWEED_BACKEND_PROFILE=<profile> FIREWEED_E2E_SCALE=smoke cargo test -p fireweed-service --test product_workflows generic_priority_bounded_relaxed_e2e -- --ignored` | Prove fireweed is not timestamp-only or Seventh-Sense-only: create an `int64` descending strict queue and a non-timestamp bounded-relaxed queue, push generic work with skewed priorities and opaque payloads, claim/finalize through the native service API, and verify progress under contention without any Seventh Sense metadata shape. | FR-1, FR-2, FR-4, FR-5..FR-9, FR-12..FR-16, FR-18..FR-21, Non-Goals | Strict non-timestamp claim order has 0 inversions; bounded-relaxed rank error stays within the declared bound and INV-4 holds; opaque payload/metadata round-trip; no Seventh Sense field is required by core validation. |
| AC-E2E-9 downstream pacing non-goal | `FIREWEED_BACKEND_PROFILE=<profile> FIREWEED_E2E_SCALE=smoke cargo test -p fireweed-service --test product_workflows downstream_pacing_non_goal_e2e -- --ignored` | Prove fireweed does not enforce downstream API rate/quota admission: load many eligible items for one compatibility group, claim with caller-selected `max_items` values and deliberate pauses between calls, and compare results to eligibility/`max_items` only. | FR-45, Non-Goals, PRD acceptance sketch "No downstream rate enforcement" | Each `BatchClaim` returns up to `max_items` subject only to normal eligibility, active leases, filters, and batch limits; a short or empty batch is valid per API-001; fireweed never withholds otherwise-eligible work for a downstream-rate reason and never emits a downstream-rate lifecycle/admission state. |

`AC-SEN` is the aggregate product release gate for these suites: the
`product_validation_tests` release job runs the P0/core workflows AC-E2E-1
through AC-E2E-6 plus AC-E2E-8 and AC-E2E-9 at their release bars and fails if
any required product workflow lacks ledger evidence. The
`operator_validation_tests` release job owns the P1/operator workflow AC-E2E-7.
The `seventh_sense_validation_tests` job is the Seventh-Sense-shaped subset
(AC-E2E-1 through AC-E2E-4); it does not replace the generic product workflow
gate.

Canonical suite IDs for bead citation are the TP-001 names
(`product_workflow_*`, `seventh_sense_validation_tests`, and
`product_validation_tests`). The Rust test binary is `product_workflows`; the
test filter in each command is the executable entry point for that canonical
suite.

## 4. Backend Conformance Gate

Every `LogStore`/`ProjectionStore`/`SnapshotStore`/`ControlPlaneStore`
implementation MUST pass **100%** of the TD-001 shared conformance scenarios (durable
append, commit-timeout retry, request-id conflict, duplicate push, mutable
schedule, leased-update conflict, single active lease, stale-lease finalize, claim
replay, success-visible, rejection-no-effect, unknown-outcome replay, snapshot
recovery, progress-bound risk, tenant isolation, group co-residency by
construction, cohort, gates, queue ownership/fence/routing) in each of the exact
15 `StorageConfig` cells. A cell at <100% conformance is not release-eligible.
The core/transaction class runs everywhere; Class A adds durable-log replay and
recovery; Class B adds projection-persistence assertions and negative history
capability checks. Projection-specific suites may add substrate obligations but
cannot replace, skip, or weaken the common route.

The public route register contains only the five log and three projection names.
The following table is the single disposition registry for the legacy Turso and
Hybrid-bound requirements; immutable historical evidence may keep old strings,
but no retired selector or enabled Turso server route counts toward current
qualification.

| Legacy requirement / selector | Disposition | Current binding |
|---|---|---|
| AC-TURSO-1 | Retained, internal | Initialization compatibility probe only; never a public matrix row. |
| AC-TURSO-2 | Retained, internal | SQLite-versus-Turso differential corpus for implementation learning only. |
| AC-TURSO-3 | Retained, internal | Replay/rebuild compatibility probe only. |
| AC-TURSO-4 | Retained, internal | Native-async cancellation/concurrency compatibility probe only. |
| AC-TURSO-5 enabled server selection | Retired | No positive server route exists or may count as evidence. |
| AC-TURSO-5 disabled selection | Retained, negative | The retired `turso` selector fails with the exact configuration error before I/O. |
| AC-TURSO-6 | Retained, focused | One path-filtered internal compatibility lane; it adds no public matrix dimension. |
| AC-TXN-5 legacy Hybrid strict selector | Replaced | AC-TXN-5 runs `ResponseBarrier::Strict` independently across the 15-cell register. |
| AC-TXN-5A legacy Hybrid async selector | Replaced | AC-TXN-5A runs or rejects `AsyncProjection(AsyncProjectionSpec)` explicitly for every cell; there are no silent skips. |
| `hybrid`, `hybrid-strict`, `hybrid-async`, `objectlog/*`, `object_log_*` public selectors | Retired | Negative selector-error coverage only; pair strings remain permissible solely in immutable provenance and non-product test IDs. |

### 4.1 Internal Turso compatibility gates

These criteria preserve useful implementation probes without creating a public
projection value, server-positive route, support claim, or release-matrix row.

| AC | Setup | Assertion | Pass bar |
|----|-------|-----------|----------|
| AC-TURSO-1 Schema and initialization (internal) | Open a fresh file and exercise the known `execute_batch` PRAGMA trap followed by supported individual configuration | TD-010 initialization compatibility | trap proves partial WAL side effect; supported path reads back WAL, synchronous `1`, timeout `5000`; exact shared schema succeeds |
| AC-TURSO-2 Full differential corpus (internal) | Apply every supported queue command/history to SQLite and Turso, including rollback injection, then close/reopen | Internal projection compatibility | 0 mismatches in `ProjectionImage`, cursor, counters, replay outcomes, reads, eligibility, leases, indexes, summaries, or errors |
| AC-TURSO-3 Replay and rebuild (internal) | Exercise overlap, gap, manifest-sealed-before-apply crash, snapshot tail, reset, and local-file loss | ADR-013 log authority compatibility | overlap is idempotent; gap fails closed; cursor never leads rows; rebuild from object log is exact |
| AC-TURSO-4 Async cancellation and concurrency (internal) | Run AC-TXN-8/9 plus 16 disjoint writers and same-active-key conflict | ADR-015 native-async compatibility | exactly one conflict winner; all disjoint writes present; zero reactor stalls, waiter loss, duplicate outcomes, or unrecoverable accepted state |
| AC-TURSO-5 enabled server selection (retired) | Attempt the retired selector in a feature-enabled build | Public namespace closure | exact configuration error before I/O; no constructor or server route opens Turso |
| AC-TURSO-5 disabled selection (retained negative) | Attempt the retired selector in a feature-disabled build | Public namespace closure | the same exact configuration error before I/O; feature state does not change the public namespace |
| AC-TURSO-6 Focused CI compatibility | Inspect workflow expansion and run the internal Turso lane | ADR-016 CI constraint | one focused/path-filtered lane; no public projection-by-kind matrix dimension |

### 4.2 Historical Hybrid evidence (non-qualifying)

AC-HYB-1 through AC-HYB-10 below are preserved only to interpret immutable
evidence and extract reusable assertions. They are not current product gates,
and their retired selectors MUST NOT appear in the 15-cell result count. Their
current replacements are AC-TXN-5 (`Strict`), AC-TXN-5A
(`AsyncProjection(AsyncProjectionSpec)`), the common projection-conformance
class, and TP-005's provider-neutral performance rows.

AC-HYB-5 and AC-HYB-6 use portable under-load comparisons and exact recovery
invariants. Wall-clock and absolute rate observations may report capacity for a
declared topology, but quiet-host or fixed host-speed thresholds cannot decide
support across hosts.

| AC | Setup | Assertion | Pass bar |
|----|-------|-----------|----------|
| AC-HYB-1 ProjectionImage completeness | Export SQLite `ProjectionImage` from a queue containing pending, leased, terminal, delayed, paused, gated, indexed, cohort, recurring, side-record, instance-fence, metrics, counter, and request-id replay state; hydrate memory from it | Memory-visible state equals SQLite-visible state before returning recovery high-water | 0 field mismatches; recovery refuses to return high-water on partial or failed hydration |
| AC-HYB-2 Strict SQLite-first poison | Run `objectlog/hybrid-strict`; inject deterministic memory-apply failure after SQLite batch commit | TD-004 strict poison contract | Current op returns storage failure; all subsequent reads, validation, and writes fail closed; restart hydrates memory from SQLite and resumes with no acknowledged-state loss |
| AC-HYB-3 Request-id replay matrix | For both `objectlog/hybrid-strict` and `objectlog/hybrid-async`, crash/retry around the mode-specific success barrier for push, claim, renew, finalize, retry/release, update, purge, and operator-style mutations with same and conflicting `request_id` bodies; keep retries inside and outside `request_id_retention_ms` | Durable committed-but-unreturned, unknown-outcome replay, and outcome retention | Same-body retry returns the original result without second append while the outcome retention window is active; different-body retry returns `request-id-conflict`; 0 duplicate state transitions; async mode resolves committed-before-memory-render outcomes from the object log or an equivalent durable replay record |
| AC-HYB-4 Authority, lineage, and retention frontier | Remove the local SQLite file and recover from retained object log; separately attempt retention using only local SQLite `sqlite_high_water`, including WAL checkpoint/fsync states that are ahead of, equal to, and behind the logical applied marker; inject manifest/segment checksum mismatch, segment sequence gap/overlap, divergent `request_id` fingerprint, and memory-vs-SQLite `ProjectionImage` mismatch; compute the async retention frontier from committed snapshots, active manifest tail, request-id retention, item-key retention, and SQLite apply lag | Object log remains authority and lineage gates retention | Disk-loss recovery reconstructs exact metrics, indexes, leases, and request-id replay state with 0 invariant violations; manifest -> segment -> sequence -> memory image -> SQLite image lineage validates before recovery or expiry; local SQLite logical high-water alone never authorizes segment expiry; WAL/fsync/checkpoint state is ignored for object-log trimming; no segment at or above the minimum retention frontier is deleted |
| AC-HYB-5 Hot-read performance | Compare `objectlog/hybrid-strict` and `objectlog/hybrid-async` with interleaved same-run `objectlog/inmemory` and `objectlog/sqlite` controls under identical seeded work and segment settings, telemetry enabled | TD-004 performance model | Exact operation counts and monotonic progress; hot reads served from memory; shared resources/debt within declared bounds; relative degradation stays within the workload-declared same-run envelope. Strict reports SQLite apply amortization, async reports max/p99 SQLite lag; both report throughput, p50/p95/p99, segment batch density, object PUT count, recovery elapsed time, replayed tail length, and max rehydrate time as topology-bound capacity. |
| AC-HYB-6 Recovery gates | Smoke: 100k resident items, local SQLite present. Release: 10M resident items, local SQLite present. Disk-loss: SQLite removed, retained object log present | Bounded owner-local restart and exact disk-loss reconstruction | Snapshot + contiguous tail reconstruct exact counts/checksums/indexes/leases with 0 invariant violations; replay progress is monotonic; replayed commands, memory, and pending work stay within declared snapshot/tail bounds. Wall time is reported as topology-bound capacity only. |
| AC-HYB-7 Async success barrier and ordered batching | Run `objectlog/hybrid-async`; inject SQLite apply delay/failure after manifest commit while memory apply/render succeeds, then read/claim/replay before SQLite catches up; seal at least three batches, delay batch N, and verify batch N+1 cannot advance `sqlite_high_water` first; record manifest tail, segment sequence ranges, memory image high-water, SQLite image high-water, replay-record counts, async apply debt, `sqlite_apply_lag_ms`, oldest unapplied `batch_sequence`, high-water advancement decisions, configured debt/backpressure thresholds, and computed retention frontier | TD-004 async success barrier, lineage validation, ordered batching contract, bounded async apply debt, INV-10, INV-12, INV-14 | Success is returned only after manifest commit plus synchronous memory apply/render; next owner-local read/claim/replay observes the committed effect from memory; SQLite lag and replay debt remain within budget or fail closed for new high-water/recovery/retention advancement; sealed batches apply in order exactly once; `sqlite_high_water` is a logical high-water distinct from WAL/fsync state; lineage evidence is complete; 0 read-after-success gaps; no high-water advancement is recorded without complete debt/backpressure evidence |
| AC-HYB-8 Async poison, repair, debt, and backpressure gates | Run `objectlog/hybrid-async`; inject durable async SQLite apply failure, repeated retry failure, local SQLite corruption/missing-file recovery, replay-tail debt above budget, and an operator repair/redrive/purge attempt against leased and terminal items while async apply is poisoned or backpressured | TD-004 async poison/fail-closed, API-002 operator repair, bounded async apply debt, backpressure gates, INV-5, INV-10, INV-11, INV-12, INV-14 | Poisoned async apply records the failed batch sequence, SQLite error class, memory high-water, `sqlite_high_water`, async apply debt, replay debt, and operator-visible repair state; all later reads that require SQLite authority, validation, writes, recovery high-water advancement, and retention decisions fail closed until repair/recovery clears lineage; owner-local memory reads that remain allowed by TD-004 continue to satisfy INV-12; operator repair is rejected or queued with a typed poison/backpressure result until it can preserve lease fences, idempotency, and high-water lineage; after repair, same-body `request_id`/`operation_id` replay returns the original result and conflicting bodies fail; when SQLite lag or replay debt exceeds budget, mutating admission applies typed backpressure without acknowledging new commands and resumes only after measured debt falls below the configured threshold |
| AC-HYB-9 Hybrid-async crash matrix release gate | Run `objectlog/hybrid-async` against the full crash matrix for push, claim, renew, finalize, retry/release, update, purge, and operator-style mutations; inject crash/restart cut points before manifest commit, after manifest commit before memory apply/render, during memory apply/render, after memory apply/render before response, during async SQLite apply, during partial SQLite batch transactions, after SQLite lag recovery, during replay, and during high-water recovery; force ordered batching with delayed sealed batches and include request-id same-body and conflicting-body retries | TD-004 async success barrier, ordered batching contract, lineage validation, bounded async apply debt, retention frontier gates, INV-5, INV-10, INV-11, INV-12, INV-14 | The hybrid-async crash matrix records manifest tail, segment sequence ranges, batch_sequence ordering, memory high-water, `sqlite_high_water`, request-id replay outcome, async SQLite lag, replay contract outcome, lineage validation result, retention_frontier inputs, and typed poison/backpressure state for every cut point; success is acknowledged only after manifest commit plus memory apply/render; `sqlite_high_water` advances only after complete logical batch apply; restart/replay/high-water recovery produce 0 lost accepted commands, 0 duplicate state transitions, 0 read-after-success gaps, and no retention or high-water advancement without complete lineage and debt evidence |
| AC-HYB-10 Hybrid-async perf matrix release gate | Compare `objectlog/hybrid-async` with interleaved same-run `objectlog/inmemory`, `objectlog/sqlite`, and committed release-lane controls under identical seeded object-log settings and hot-path mixes for push, claim, renew, finalize, retry/release, update, purge, replay, recovery, and hot read/claim paths; run both no-lag and injected-lag async SQLite profiles with ordered batching enabled | TD-004 performance model, async success barrier, ordered batching contract, lineage validation, retention frontier authority, release-lane hot path evidence | The perf matrix records exact counts/progress, declared resource bounds, p50/p95/p99 ack latency, push throughput, claim/finalize hot-path latency, replay latency, recovery elapsed time, segment batch density, object PUT count, memory high-water, `sqlite_high_water`, lineage validation, retention frontier, async SQLite lag, apply/replay debt, thresholds, typed backpressure, replay counters, high-water decisions, and same-run comparison deltas. Async mode satisfies AC-HYB-5's portable relative/resource bars or fails; absolute timings remain capacity evidence, and WAL/fsync/checkpoint state is never logical high-water or retention authority. |

### 4.3 Deterministic object-log model corpus (SP-02)

The bounded deterministic suite complements, but does not replace, AC-TXN-4's process-kill harness:

```text
scripts/ci/repeat-suite.sh --count 100 --max-flaky-rate 0 --suite-list scripts/ci/deterministic-simulation-suites.toml
```

One seed controls operation choice, logical time, retry, crash, and scripted store outcomes. The independent
`fireweed-sim-support` crate has no engine/object-log/runtime dependency. The production adapter drives the real
synchronous `SegmentedObjectLog`, advances the real durable retention floor, performs real expiry, and
compares recovery visibility plus executable storage projections of INV-1, INV-2, INV-10, INV-12, and INV-14
after every executed durable cut. Failure output includes the seed, failing index, and compact trace; delta
debugging preserves violated-invariant identity and retains
the failure in at most 32 operations.

SP-08 adds two enumerated composed-path cases and one honestly scoped log-level retry case to the same
integration target. They drive the real `InMemoryControlPlane`, engine `acquire_and_fence`, and
`SegmentedObjectLog` with logical time through `PendingFence`, failure-before-effect, effect-then-error,
exact fence retry, lease expiry/reassignment, stale returned-session rejection, unrelated-queue progress
before retry, log-level ambiguous manifest retry, and exact fresh reopen. The focused full target passes
11/11 locally:

```text
cargo test -p fireweed-objectlog --test deterministic_storage_simulation
```

The generated suite exercises current-format crash and store-fault behavior. Cross-host repeats, clean
target-dir growth, and process-kill replay remain release evidence. The suite has a precise repeat-suite entry
but is not wired into broad GitHub Actions; no host-capacity benchmark is part of this suite.

## 5. CI Quality Gates (the green set)

### SP-05 maintenance evidence

The focused SP-05 gate requires table tests for every typed frontier axis, filter exclusion, and fail-closed
orphan proof; bounded orphan-GC tests for dry-run/live parity, partial replay, pin-last ordering, stale-owner
fencing, `page_size = 1` segment/sentinel convergence, and request-cap enforcement; and a scheduler
regression proving terminal projection rows are reaped by the single reclaim driver only after emission
advances its cursor. Partial-effect reports cover retryable delete failure and epoch loss after deletion.
Restart tests prove completion from persisted object-size inventory. Providers without an exact one-attempt
primitive-call guarantee fail closed.

Bounded segment-expiry acceptance additionally requires a large manifest prefix to converge across multiple
passes without exceeding per-pass object, byte, request, elapsed-time, or page-size limits. A restart between
passes must discard only the soft cursor, rescan durable reclaimed markers, delete every remaining eligible
segment exactly once, and publish the deletion watermark only after the complete unblocked traversal. A call-site
gate verifies that the composed production scheduler invokes the bounded expiry seam and merges its summary;
the unbounded compatibility helper is not a scheduler dependency.
Regression cases increase `through_seq` between passes, expire a live pin between passes, and scan a branch
registry larger than the per-pass request cap. They assert that skipped entries are reconsidered, a pinned
pass is never reported complete, registry paging converges, and the report charges actual watermark calls
while admission reserves the maximum ambiguous-publication cost.

For `AsyncProjection(AsyncProjectionSpec)` on a durable object log,
segment/manifest deletion has a negative acceptance result until a single
owner-fenced API can prove the complete TD-004 authority snapshot. Its required
assertion is conservative retention plus a missing-frontier/storage-growth
signal; recovery-success and bounded-growth claims remain unverified. The SP-04
same-run overhead comparison remains separate and is not part of this
functional gate.

Gates run at two cadences. **Per-PR (fast)** gates MUST pass to merge.
**Release (full)** gates MUST pass to call a build "verified" for v1; they include
every per-PR gate plus the soak/scale/exhaustive gates. A build is **verified**
only when the entire Release column is green — the per-PR set alone is necessary
but not sufficient.

| Gate | Threshold | Cadence |
|------|-----------|---------|
| `cargo fmt --all --check` | clean | per-PR |
| `cargo clippy --workspace --all-targets -- -D warnings` | 0 warnings | per-PR |
| `cargo test --workspace` | 100% pass | per-PR |
| `cargo deny check` / `cargo machete` | clean | per-PR |
| `#![forbid(unsafe_code)]` | enforced (any exception ADR-recorded) | per-PR |
| Coverage — `fireweed-core` | ≥ 90% line, ≥ 85% branch | per-PR |
| Coverage — `fireweed-service` | ≥ 80% line | per-PR |
| Property tests | ≥ `props` (PR tier); 0 falsifications | per-PR |
| Fuzz targets (command decode, selector, priority decode) | ≥ `fuzz` (PR tier); 0 new crashes | per-PR |
| Current segment format (golden, bounds, corruption, recovery) | Literal current bytes; CRC32C/SHA-256 standards; all single-bit mutations; truncated/oversized/malicious lengths; manifest/key/header mismatch; deterministic data/control replay; retired shapes rejected | per-PR |
| Product E2E smoke (`FIREWEED_E2E_SCALE=smoke`) | P0/core AC-E2E-1..6 and AC-E2E-8..9 pass at smoke shape for each implemented suite and required backend profile; include AC-E2E-7 once the P1/operator surface is implemented | per-PR once the suite exists |
| **Every `AC-*` in §3 executes and passes at its stated bar** | 100% of claimed `AC-*` green | per-PR for unit/integration ACs and product smoke; release for soak, scale, and release-shape product E2E ACs |
| Portable capacity/degradation gates `AC-LAT-1..4` | exact work, same-run ratios, structural complexity, and declared resource bounds pass; absolute p50/p95/p99 are reported only | release |
| Operator suites (`operator_repair/redrive/purge/async/auth` + `AC-OP-1..9`) | 100% pass | operator-enabled release |
| Backend conformance (§4) — exact 15-cell register | 100% of scenarios; zero missing/ignored/fixture-skipped rows | release |
| External transaction contract (§3.10) — exact 15-cell register | AC-TXN-1..6 green with Class A/Class B bars and explicit barrier dispositions; INV-12..INV-14 = 0 | release |
| Coverage — `fireweed-storage` conformance scenarios | 100% executed | release |
| Loom (each custom concurrent structure) | exhaustive to the bounded preemption depth; 0 failing interleavings | release |
| Property + fuzz (nightly tier) | ≥ `props`/`fuzz` nightly values; 0 falsifications/crashes | release |
| Flaky rate | < 0.1% over 100 CI repeats of the suite | release |
| P0/core safety invariants INV-1..INV-10 and INV-12..INV-14 | 0 violations under the §2 stress matrix and §3.10 duress matrix | release |
| Operator safety invariant INV-11 | 0 violations under the §2 stress matrix with operator repair/purge actions enabled | operator-enabled release |
| TP-002 E0 (portable progress/capacity contract), E1, E2 (cross-queue scale-out + ≥1000-queue density), E3 (object-log latency/cost/recovery) | pass at TP-002 bars | release |
| `AC-SEN` P0/core product workflow aggregate | AC-E2E-1..6 and AC-E2E-8..9 green with ledger evidence; INV-1..INV-10 = 0 where applicable | release |
| Operator product workflow aggregate | AC-E2E-7 green with ledger evidence; INV-8 and INV-11 = 0 | operator-enabled release |

The release gate is intentionally self-contained from a clean checkout:
`scripts/ci/release-gate.sh` validates TP-002 E0-E3 from closed DDx source beads
when passed `--tp002-e0e1-source`, `--tp002-e2-source`, and
`--tp002-e3-source`, then regenerates and strictly validates the
`product_validation_tests` aggregate ledger. It must not require pre-existing
`target/fireweed-ledger/*.jsonl` artifacts to pass.

## 6. Verification Ledger

Each bead that claims an `AC-*` MUST record, in the bead's evidence:

- the `AC-*` / `INV-*` IDs satisfied;
- the exact command(s) run and their exit status;
- the environment (toolchain, exact log × projection cell, response barrier,
  durability class, instance class, seed);
- the measured numbers vs the pass bar;
- the named test suite(s) (TP-001) that produced them.

A feature is "verified" when every `AC-*` it lists is green with recorded numbers.
A bead MUST NOT be closed on formatting or unit tests alone when its acceptance
criteria touch storage, concurrency, claim, lease, operator, or scale behavior
(ADR-003 testing policy).

## 7. Exit Criteria (v1 verified)

fireweed P0/core v1 is "verified" when:

1. INV-1..INV-10 and INV-12..INV-14 hold with 0 violations where applicable
   across the §2 stress matrix and §3.10 duress matrix on all 15 cells; Class B
   rows prove their projection-only boundary instead of claiming durable-log
   history.
2. Every `AC-*` in §3 passes at its stated bar, recorded in the ledger.
3. The §4 backend conformance gate is 100% for the exact 15-cell registry, with
   zero silent skips.
4. The §5 CI quality gates are green.
5. TP-002 E0 (portable progress/capacity contract), E1, E2 (cross-queue scale-out + ≥1000-queue
   density), and E3 (object-log latency/cost/recovery) pass.
6. AC-SEN — the product validation suite (`product_validation_tests`) runs the
   P0/core product workflows AC-E2E-1 through AC-E2E-6 plus AC-E2E-8 and
   AC-E2E-9 at their release bars, proving the scheduled
   delivery/action, Marketo group batching, callback cohort, recurring
   jobs/connectors, crash recovery, noisy-neighbor routing, generic
   non-timestamp bounded-relaxed, and downstream pacing non-goal workflows
   end-to-end with the applicable invariants holding.
7. The P1/operator-enabled product surface is "verified" only when
   `operator_validation_tests` runs AC-E2E-7 and the API-002 operator suites at
   their release bars, including INV-11 under the §2 stress matrix with
   operator repair/purge actions enabled. This is required before claiming
   operator support, but it does not block the P0/core v1 gate above.

Any gap MUST be recorded as an explicit, dated deferred item with an owner, not
silently dropped.
