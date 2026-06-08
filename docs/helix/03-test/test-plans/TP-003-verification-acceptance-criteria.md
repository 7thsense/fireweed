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
    - td-storage-architecture-backend-contracts
    - td-postgres-native-reference-mode
    - td-sharding-and-shard-ownership
    - td-s3-object-log-sqlite-projection-mode
    - tp-governing-test-traceability
    - tp-scale-substantiation
---

# Test Plan: TP-003 Verification and Acceptance Criteria

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
| INV-9 | Group co-residency (ADR-004) | On a `group_co_residency=true` queue, `group_key`s resolving to >1 shard = **0**. |
| INV-10 | Durable ack (ADR-001/TD-001) | Acknowledged commands missing after crash + replay = **0** (kill-after-ack). |
| INV-11 | Lease fence on operator action (API-002) | Operator mutation of a leased item that leaves the old lease usable = **0**. |

## 3. Acceptance Criteria by Area

Each `AC-*` has Setup (quantified) → Assertion → Pass bar. A bead cites the `AC-*`
it turns green. Suites are named in TP-001.

### 3.1 Core semantics (`pqueue-core`)

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

### 3.3 Group-batching, cohort, gates (gap features)

| AC | Setup | Assertion | Pass bar |
|----|-------|-----------|----------|
| AC-GRP-1 whole-group | `max_groups=300`, multi-task groups (Marketo lead shape) | up to 300 whole eligible groups; never a partial group; `batch-too-large` when next group won't fit | INV-7; 0 partial groups |
| AC-GRP-2 same_group_key is filter | one group, `max_items` < group size | returns a partial group (item filter), never whole-group semantics | matches spec; 0 conflations |
| AC-COH-1 whole-cohort atomic | cohorts of size {2,10,1000}; `C=64` | whole complete cohort leased under one shared lease, or none; members never individually claimable | INV-7; 0 leaks |
| AC-COH-2 cohort completion bound | `completion_bound_ms ≤ progress_bound_ms` | incomplete cohort never blocks past bound; `expire_cohort` fires | 0 bound violations |
| AC-GATE-1 O(1) gate flip | `S=10M`, flip a gate key affecting `G` groups | flip touches 0 item rows; claim/discovery reflect gate at read; latency independent of `G` | 0 item-row writes; flip p99 < 50 ms |
| AC-GATE-2 gate eligibility | gated items present | gate-blocked items never claimed; oldest-eligible advances past blocked (not excluded) | INV-4 holds with gates |

### 3.4 Recurring (g5)

| AC | Setup | Assertion | Pass bar |
|----|-------|-----------|----------|
| AC-REC-1 rearm | high-frequency rearm loop, `T_soak` | `rearm` releases lease, sets `eligible_since=max(commit,not_before)`, resets per-cycle retry, bumps version, never terminal | 0 version-monotonicity breaks; 0 spurious terminals |
| AC-REC-2 idle inventory | `S=1M` idle re-armed items | idle items excluded from oldest-eligible age and retry backlog; `recurring_pending` within documented lag | 0 inflation; lag ≤ documented bound |
| AC-REC-3 purge teardown | `PurgeItems` per-key, incl. `force` while leased, multi-shard split | row removed; tombstone + replay-safe; duplicate purge → `not_found`; late finalize → `not_found` | 0 resurrected items; idempotent |

### 3.5 Discovery + multi-shard (g4, TD-003)

| AC | Setup | Assertion | Pass bar |
|----|-------|-----------|----------|
| AC-DISC-1 ranking | multi-queue, multi-shard, gated | top-N by oldest-eligible age across shards via single summary; gate-current advance | matches ground truth; 0 mis-ranks at terminal read |
| AC-DISC-2 non-co-resident group aggregation | `group_co_residency=false`, one `group_key` spanning ≥2 shards with oldest eligible item on one shard and newer eligible items on another | discovery reports the group once, with `oldest_eligible_age_ms` equal to the cross-shard oldest eligible age, not a per-shard top-N union | single row; age matches ground truth; 0 duplicate group descriptors |
| AC-SHARD-1 fan-out order | `shard_count∈{2,4,8}`, strict | deterministic k-way merge within global `max_items`; INV-6 | 0 inversions |
| AC-SHARD-2 cross-shard progress | hot shard + cold shard holding queue-global oldest | cold oldest claimed ≤ `progress_bound_ms` | INV-4; 0 violations |
| AC-SHARD-3 fence + rebalance | reassign under load; `N_kill` mid-drain | single writer (INV-1); stale-epoch appends rejected; no double-lease across drain | 0 stale-epoch commits; 0 double leases |

### 3.6 Operator contract (API-002)

| AC | Setup | Assertion | Pass bar |
|----|-------|-----------|----------|
| AC-OP-1 repair fences lease | `RepairItems force_*`/`clear_lease` on leased items, `C=64` | INV-11; worker sees `stale_lease`; `item_version` bumped; `force_release` preserves progress clock | 0 usable stale leases |
| AC-OP-2 redrive | `RedriveItems` over `S=1M` terminal-failed, multi-shard, incl. a future `redrive.not_before` | items return eligible with `retry_count_mode`; `eligible_since = max(commit_time, redrive.not_before)` (future-dated items accrue no eligible age until `not_before`); guards (`max_affected`/`expected_match_count`) enforced; async converges | 0 guard bypass; 100% convergence; 0 premature eligible-age accrual |
| AC-OP-3 bulk purge | `PurgeQueueItems` selector over `S=1M`; `dry_run` first | `dry_run` side-effect-free + exact matched count; purge writes tombstones; multi-shard partial-commit re-drives & converges; idempotent (`not_found`) | 0 side effects in dry_run; count exact; 0 resurrected items |
| AC-OP-4 async ops idempotent | replay create `request_id` ×100; `CancelOperation` mid-run | one `operation_id`; no second op; terminal-state counts exact; cancel never rolls back committed shards | 0 duplicate ops; counts exact at terminal |
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

### 3.8 Latency / throughput micro-bars (single deployment)

Per-operation bars under `C=64`, `S=1M`, telemetry on, `postgres_native`. (Full
scale/density/horizontal magnitude is TP-002 E0–E3; these are the per-op gates.)

| AC | Operation | Pass bar |
|----|-----------|----------|
| AC-LAT-1 | batch push / update / claim / finalize | p95 < 250 ms, p99 < 1000 ms (sub-second p99, FR success metric) |
| AC-LAT-2 | gate flip (`SetGates`) | p99 < 50 ms, independent of affected group count |
| AC-LAT-3 | `DiscoverActiveScopes` top-N | p99 < 250 ms over `S=10M`, no full scan (query plan asserted) |
| AC-LAT-4 | telemetry overhead | core-op p99 with telemetry on ≤ 1.15× telemetry off |

### 3.9 Observability correctness

| AC | Setup | Assertion | Pass bar |
|----|-------|-----------|----------|
| AC-OBS-1 metrics ground truth | `S∈{10K,1M}`, mixed pending/future/leased/retry/complete/failed/recurring states, both backend profiles, telemetry enabled | `GetQueueMetrics` lifecycle counts, active leases, retry backlog, recurring counters, throughput/latency buckets, and progress-bound risk are compared to a ground-truth scan/checkpoint for the test fixture; `oldest_eligible_age_ms` is exact-on-read as of the reported frontier | exact fields have 0 mismatches; approximate count fields converge within `L_metrics`; throughput/latency values match the harness-recorded operation log within documented aggregation tolerance |

### 3.10 Product end-to-end workflow validation

These are the product-facing "does pqueue work?" gates. Lower-level ACs prove
individual primitives; these workflows prove those primitives compose through the
native service API against committed backend profiles. Each workflow MUST be
automated. No product E2E may replace the shared conformance suite; it sits above
conformance and exercises the real service, auth context, backend wiring,
metrics, and failure behavior.

Unless a row states otherwise, run each product E2E against both committed
backend profiles (`postgres_native` and `object_log_sqlite_projection`) with
telemetry enabled. Product E2E commands are parameterized with environment
variables, not hidden test defaults:

- `PQUEUE_BACKEND_PROFILE=postgres_native|object_log_sqlite_projection`
- `PQUEUE_E2E_SCALE=smoke|release`
- `PQUEUE_E2E_SEED=<recorded-seed>`

`PQUEUE_E2E_SCALE=smoke` is the PR-tier shape: small item sets, short runtime,
and deterministic failures while preserving the same workflow assertions.
`PQUEUE_E2E_SCALE=release` MUST use the row's release shape below. `N/A` means
that parameter is not part of that workflow's release evidence. The verification
ledger records the profile, scale, seed, command, topology, and measured values.

Failure-heavy ACs depend on a shared `fault_injection_harness_tests` capability
that can terminate worker/service processes at deterministic cut points, inject
duplicate request replay, force partial shard append/commit outcomes, and record
the seed and failure schedule. Build sequencing must create that harness before
AC-CLAIM-2, AC-SHARD-3, AC-E2E-2, AC-E2E-3, AC-E2E-5, and AC-E2E-7 are claimed.

Approximate count/metric assertions use `L_metrics` from §1 and the exact/lagged
metric contract in API-001 (`GetQueueMetrics`, `DiscoverActiveScopes`) plus the
TD-001/TD-002/TD-004 projection rules. `oldest_eligible_age_ms` and discovery
oldest age are never approximate; count lag is bounded by `L_metrics`. For
`object_log_sqlite_projection`, unrelated-reader visibility additionally uses
`L_apply` from TD-004's cross-operation apply-lag contract.

Release shapes and shard topology:

| AC | Release shape | Required topology |
|----|---------------|-------------------|
| AC-E2E-1 | `S=1M`, `C=64`, `T_soak`, `N_kill>=1000` | Both committed profiles, single-shard and multi-shard where the profile supports it. |
| AC-E2E-2 | ≥1000 groups, task counts per group in {1,3,10}, `max_groups=300`, `C=64`, `T_soak`, `N_kill>=100` during whole-group claim/finalize | Both committed profiles; `group_co_residency=true`; object-log release run uses ≥4 shards; `postgres_native` may run single-shard unless the optional multi-shard comparator is configured. |
| AC-E2E-3 | cohort sizes {2,10,1000}, ≥10K cohorts, `C=64`, `T_soak`, `N_kill>=100` during whole-cohort claim/finalize | Both committed profiles; `group_co_residency=true`; object-log release run uses ≥4 shards for cohort placement and replay parity. |
| AC-E2E-4 | `S=1M` recurring singleton keys, `C=64`, `T_soak`, `N_kill>=1000` | Both committed profiles; object-log release run uses ≥4 shards; `postgres_native` single-shard run is sufficient for the Tier-1 profile. |
| AC-E2E-5 | `S=1M`, `C=64`, `T_soak`, `N_kill>=1000`, duplicate replay and partial-commit injection enabled | Object-log multi-shard is required for partial-shard append/fan-out failure assertions. `postgres_native` runs the single-shard crash/replay subset; multi-shard Postgres is an optional comparator and does not satisfy TP-002 E2 by itself. |
| AC-E2E-6 | hot queue `S=10M`, one small eligible queue, `K_q=1000`, `C=64`, `T_soak`, `N_kill=N/A` | Object-log multi-shard is required for cross-shard active-scope routing and density evidence. `postgres_native` runs the single-deployment noisy-neighbor subset; optional multi-shard Postgres is comparator-only. |
| AC-E2E-7 | ≥1M selected leased/terminal items across repair/redrive/purge/archive cases, `C=64`, async operation replay x100, `T_soak`, `N_kill>=100` | Both committed profiles; object-log release run uses ≥4 shards for partial async operation convergence. |
| AC-E2E-8 | `S=1M`, `C=64`, `T_soak`, skewed priority distributions, strict `int64` descending queue plus bounded-relaxed non-timestamp queue | Both committed profiles; single-shard is sufficient for generic non-timestamp product proof, with optional multi-shard comparator. |
| AC-E2E-9 | `S=1M`, one hot `group_key`, `max_items∈{1,25,100}`, paced claim cadence, `C=8`, `T_soak`, `N_kill=N/A` | Both committed profiles; single-shard is sufficient because the contract is admission behavior, not scale-out. |

| AC | Automated suite / command | Product workflow | PRD coverage | Pass bar |
|----|---------------------------|------------------|--------------|----------|
| AC-E2E-1 scheduled action delivery | `PQUEUE_BACKEND_PROFILE=<profile> PQUEUE_E2E_SCALE=smoke cargo test -p pqueue-service --test product_workflows scheduled_action_delivery_e2e -- --ignored` | Model `scheduled_actions`: create a timestamp-ascending queue, push items early before optimized send time, later `BatchUpdate` `priority`/`not_before`, close and reopen account/connector/campaign gates with `SetGates`, claim by `group_key`, renew/finalize, and assert queue metrics. Release scale: `S=1M`, `C=64`, `T_soak`, `N_kill>=1000`. | FR-1..FR-3, FR-7, FR-10..FR-12, FR-18..FR-28, FR-33..FR-34, FR-40..FR-46, FR-47a | INV-1..INV-4 = 0; accepted items are either correctly terminal, leased, or eligible/redeliverable after the run; schedule order matches timestamp priority; no gated item is claimed while its gate is blocked; same `queue_id` in a second tenant remains isolated and cross-tenant data-plane access is denied; metrics match terminal state within `L_metrics`. |
| AC-E2E-2 Marketo group-cardinality batching | `PQUEUE_BACKEND_PROFILE=<profile> PQUEUE_E2E_SCALE=smoke cargo test -p pqueue-service --test product_workflows marketo_group_batching_e2e -- --ignored` | Model a downstream API call that accepts up to 300 distinct lead groups: create `group_co_residency=true`, set `group_batching.max_groups=300`, load >=1000 lead groups with skewed priorities and multiple tasks per lead, discover active groups, then claim/finalize whole eligible groups under contention. | FR-29..FR-32, FR-31a, FR-35, FR-47, FR-47b, FR-48 | INV-7 = 0 partial groups; each claim contains <=300 groups and <=`max_items`; group representatives are ordered by claim order; `batch-too-large` fires when the next whole group cannot fit; concurrent claimers do not duplicate groups. |
| AC-E2E-3 callback cohort execution | `PQUEUE_BACKEND_PROFILE=<profile> PQUEUE_E2E_SCALE=smoke cargo test -p pqueue-service --test product_workflows callback_cohort_e2e -- --ignored` | Model `actions_scheduled` callback batches: create a cohort-enabled queue with `group_co_residency=true`, push incomplete and complete callback cohorts, verify incomplete cohorts are hidden from claim/discovery, then claim/finalize complete cohorts atomically; run an expiry case for incomplete cohorts. | FR-32a..FR-32c, FR-47a, FR-47c, FR-48 | INV-7 = 0 cohort leaks; incomplete cohorts are never claimed or discovered; complete cohorts lease atomically under one shared lease; `completion_bound_ms <= progress_bound_ms` is enforced; expired incomplete cohorts become terminal `failed` with the required reason. |
| AC-E2E-4 jobs/connectors recurring singleton | `PQUEUE_BACKEND_PROFILE=<profile> PQUEUE_E2E_SCALE=smoke cargo test -p pqueue-service --test product_workflows jobs_connectors_recurring_e2e -- --ignored` | Model `jobs_queue` and `connectors_queue` poll-cursor rows: one logical item per job/connector key, repeated claim -> work -> `rearm` cycles with new `not_before`, idle periods, per-cycle retry exhaustion, `recurrence.until`, and `PurgeItems` teardown. | FR-36..FR-39, FR-49..FR-55 | No duplicate singleton rows; `item_version` increases monotonically across re-arms; `rearm` never consumes retry budget; idle recurring items do not inflate exact `oldest_eligible_age_ms`, and approximate recurring/retry counters converge within `L_metrics`; purge is idempotent and late finalize returns `not_found`. |
| AC-E2E-5 worker crash recovery | `PQUEUE_BACKEND_PROFILE=<profile> PQUEUE_E2E_SCALE=smoke cargo test -p pqueue-service --test product_workflows worker_crash_recovery_e2e -- --ignored` | Drive the native API with real service/backend processes while injecting worker exits after claim, service process exits after acknowledged append, duplicate request replay, partial shard append failure, and restart/recovery. | FR-23..FR-28, FR-33..FR-39, API-001 idempotency, TD-001 durability, TD-003 fencing | INV-1, INV-2, INV-3, INV-5, INV-10 = 0 violations; acknowledged commands survive restart; replayed `request_id`s converge; expired leases redeliver without resetting eligible age; no accepted item is lost. |
| AC-E2E-6 noisy-neighbor and active-scope routing | `PQUEUE_BACKEND_PROFILE=<profile> PQUEUE_E2E_SCALE=smoke cargo test -p pqueue-service --test product_workflows noisy_neighbor_scale_e2e -- --ignored` | Run one hot queue with a 10M resident backlog, one small eligible queue, and `K_q=1000` active queues; workers use `DiscoverActiveScopes` to route group claims while the hot queue is under load. | FR-1, FR-12, FR-40..FR-43, FR-48, TP-002 E1/E2 | Small queue batch-claim latency meets AC-LAT-1 (p95 < 250 ms, p99 < 1000 ms); progress satisfies INV-4 against its configured `progress_bound_ms`; discovery ranks authorized active scopes by true oldest eligible age; unauthorized queues are excluded; no per-queue/per-shard unbounded worker, loop, or connection growth is observed. |
| AC-E2E-7 operator repair/redrive workflow | `PQUEUE_BACKEND_PROFILE=<profile> PQUEUE_E2E_SCALE=smoke cargo test -p pqueue-service --test product_workflows operator_repair_redrive_e2e -- --ignored` | Model an operator repairing production damage: pause queue, inspect leased and terminal items without exposing lease tokens, force-release or reschedule leased items, redrive failed items, dry-run and execute bulk purge/archive, resume queue, and observe async operation convergence. | API-002, FR-38..FR-43, FR-52, ADR-002 | INV-8 and INV-11 = 0; data-plane principals cannot perform operator actions; stale leases become unusable after repair; dry-run has no side effects; async operation replay returns the same `operation_id`; audit records omit payload and lease tokens. |
| AC-E2E-8 generic priority and bounded-relaxed service workflow | `PQUEUE_BACKEND_PROFILE=<profile> PQUEUE_E2E_SCALE=smoke cargo test -p pqueue-service --test product_workflows generic_priority_bounded_relaxed_e2e -- --ignored` | Prove pqueue is not timestamp-only or Seventh-Sense-only: create an `int64` descending strict queue and a non-timestamp bounded-relaxed queue, push generic work with skewed priorities and opaque payloads, claim/finalize through the native service API, and verify progress under contention without any Seventh Sense metadata shape. | FR-1, FR-2, FR-4, FR-5..FR-9, FR-12..FR-16, FR-18..FR-21, Non-Goals | Strict non-timestamp claim order has 0 inversions; bounded-relaxed rank error stays within the declared bound and INV-4 holds; opaque payload/metadata round-trip; no Seventh Sense field is required by core validation. |
| AC-E2E-9 downstream pacing non-goal | `PQUEUE_BACKEND_PROFILE=<profile> PQUEUE_E2E_SCALE=smoke cargo test -p pqueue-service --test product_workflows downstream_pacing_non_goal_e2e -- --ignored` | Prove pqueue does not enforce downstream API rate/quota admission: load many eligible items for one compatibility group, claim with caller-selected `max_items` values and deliberate pauses between calls, and compare results to eligibility/`max_items` only. | FR-45, Non-Goals, PRD acceptance sketch "No downstream rate enforcement" | Each `BatchClaim` returns up to `max_items` subject only to normal eligibility, active leases, filters, and batch limits; a short or empty batch is valid per API-001; pqueue never withholds otherwise-eligible work for a downstream-rate reason and never emits a downstream-rate lifecycle/admission state. |

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
replay, snapshot recovery, progress-bound risk, tenant isolation, group
co-residency, cohort, gates, multi-shard) before that backend is selectable by
backend profile. A backend at <100% conformance is not v1-eligible. Both committed
profiles (`postgres_native`, `object_log_sqlite_projection`) run the identical
suite.

## 5. CI Quality Gates (the green set)

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
| Coverage — `pqueue-core` | ≥ 90% line, ≥ 85% branch | per-PR |
| Coverage — `pqueue-service` | ≥ 80% line | per-PR |
| Property tests | ≥ `props` (PR tier); 0 falsifications | per-PR |
| Fuzz targets (command decode, selector, priority decode) | ≥ `fuzz` (PR tier); 0 new crashes | per-PR |
| Product E2E smoke (`PQUEUE_E2E_SCALE=smoke`) | P0/core AC-E2E-1..6 and AC-E2E-8..9 pass at smoke shape for each implemented suite and required backend profile; include AC-E2E-7 once the P1/operator surface is implemented | per-PR once the suite exists |
| **Every `AC-*` in §3 executes and passes at its stated bar** | 100% of claimed `AC-*` green | per-PR for unit/integration ACs and product smoke; release for soak, scale, and release-shape product E2E ACs |
| Latency micro-bars `AC-LAT-1..4` | meet stated p95/p99 | release |
| Operator suites (`operator_repair/redrive/purge/async/auth` + `AC-OP-1..9`) | 100% pass | operator-enabled release |
| Backend conformance (§4) — both committed profiles | 100% of scenarios | release |
| Coverage — `pqueue-storage` conformance scenarios | 100% executed | release |
| Loom (each custom concurrent structure) | exhaustive to the bounded preemption depth; 0 failing interleavings | release |
| Property + fuzz (nightly tier) | ≥ `props`/`fuzz` nightly values; 0 falsifications/crashes | release |
| Flaky rate | < 0.1% over 100 CI repeats of the suite | release |
| Safety invariants INV-1..INV-11 | 0 violations under the §2 stress matrix | release |
| TP-002 E0 (per-queue floor ≥10M items/hr), E1, E2 (multi-shard + ≥1000-queue density), E3 (object-log cost/ack/recovery) | pass at TP-002 bars | release |
| `AC-SEN` P0/core product workflow aggregate | AC-E2E-1..6 and AC-E2E-8..9 green with ledger evidence; INV-1..INV-11 = 0 where applicable | release |
| Operator product workflow aggregate | AC-E2E-7 green with ledger evidence; INV-8 and INV-11 = 0 | operator-enabled release |

## 6. Verification Ledger

Each bead that claims an `AC-*` MUST record, in the bead's evidence:

- the `AC-*` / `INV-*` IDs satisfied;
- the exact command(s) run and their exit status;
- the environment (toolchain, backend profile, instance class, seed);
- the measured numbers vs the pass bar;
- the named test suite(s) (TP-001) that produced them.

A feature is "verified" when every `AC-*` it lists is green with recorded numbers.
A bead MUST NOT be closed on formatting or unit tests alone when its acceptance
criteria touch storage, concurrency, claim, lease, operator, or scale behavior
(ADR-003 testing policy).

## 7. Exit Criteria (v1 verified)

pqueue P0/core v1 is "verified" when:

1. INV-1..INV-11 hold with 0 violations across the §2 stress matrix on both
   committed backend profiles.
2. Every `AC-*` in §3 passes at its stated bar, recorded in the ledger.
3. The §4 backend conformance gate is 100% for both committed profiles.
4. The §5 CI quality gates are green.
5. TP-002 E0 (per-queue floor ≥10M items/hr), E1, E2 (multi-shard + ≥1000-queue
   density), and E3 (object-log cost/ack/recovery) pass.
6. AC-SEN — the product validation suite (`product_validation_tests`) runs the
   P0/core product workflows AC-E2E-1 through AC-E2E-6 plus AC-E2E-8 and
   AC-E2E-9 at their release bars, proving the scheduled
   delivery/action, Marketo group batching, callback cohort, recurring
   jobs/connectors, crash recovery, noisy-neighbor routing, generic
   non-timestamp bounded-relaxed, and downstream pacing non-goal workflows
   end-to-end with the applicable invariants holding.
7. The P1/operator-enabled product surface is "verified" only when
   `operator_validation_tests` runs AC-E2E-7 and the API-002 operator suites at
   their release bars. This is required before claiming operator support, but it
   does not block the P0/core v1 gate above.

Any gap MUST be recorded as an explicit, dated deferred item with an owner, not
silently dropped.
