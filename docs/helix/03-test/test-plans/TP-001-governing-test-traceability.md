---
ddx:
  id: tp-governing-test-traceability
  depends_on:
    - prd
    - api-fireweed-kernel-contract
    - adr-kernel-records-contract
    - api-workload-integration-profiles
    - adr-s3-turso-public-cell
    - adr-rust-workspace-and-toolchain-policy
    - td-kernel-projection-storage-layout
    - adr-queue-as-shard-unit-and-projection-families
    - td-sharding-and-shard-ownership
    - tp-scale-substantiation
  status: accepted
  review:
    self_hash: 1f28a43e7928388a17e76507cc38e3b5ffc411860ecba33bba16fb8f320aa209
    deps:
      adr-auth-tenancy-and-storage-isolation: 1d4296498a187d4d4c3bb4a4e37647f7193036c3a20ab2a8b66154a45937ece2
      adr-cqrs-log-projection-storage-model: 63ed2521bc7d0e785529aafbd179b3ef22d51cbf3897d51c511540be52ee9ba3
      adr-granularity-mapping-and-claim-domain: c44740df25ed32569e86f4c3459ed07839d33325367cdafc49fb1317deab606d
      adr-queue-as-shard-unit-and-projection-families: 64a7c7b0e2e5f4caa2c7d775b84c87a9a1e4484ae3df9dccbe3d145d22681a7e
      adr-rust-workspace-and-toolchain-policy: df18055fca75337c9dc5e7e099d14afd44428e4883a0784af0a0c1e2192e98f6
      api-native-client-interface: b99403ef55afffd134ac3ef1a71065c497558c94de379c2b257b119000a0f488
      api-operator-repair-contract: f4fdf0e4e8e29431d8ff2f89a8bc843497dca743ad5e32439996a4ad7c611197
      api-workload-integration-profiles: 3c3dd594f1723e987015d4790634b1088016f5f41a049e661eba4b752cfb4c39
      prd: cd3004bd0dc9ac531d1cd2596e875e51c2de4601e330007fee60da1ea7b3d5ce
      td-postgres-native-reference-mode: 4088399aca0beb5840c4ace629039ad655bf4ff928bc22f86072b94857bdbd3e
      td-resp-wire-adapter: 351f429307d19a5b4ba3c3e39e9d9168e999a5b067b07d15f67636e5fd5d8040
      td-s3-object-log-sqlite-projection-mode: 7770bb133f4ace189bfc715e3be6472f894f7c62d52adfc051540fea97c6a4b2
      td-sharding-and-shard-ownership: f7309199e3810447398caa11f2f0241ce05c035e9b9d0241aada4ad3759582e1
      td-storage-architecture-backend-contracts: 2d88d342aac82f23616fdff6d94f4ac88701ab6e70c80a0315003c5e66432c74
      tp-scale-substantiation: 23f20e8dab88330e4ddd165a0d2230151b7ef0f99ca16c016671558ed5719686
    reviewed_at: "2026-08-07T11:25:30Z"
---

# Test Plan: TP-001 Governing Test Traceability

## Scope

This plan maps the PRD's functional requirements and the design's decisions to
the test suites that verify them. It is the governing implementation and
release test plan: every implementation bead cites at least one requirement or
API-006 clause, and at least one suite named here. Suite names are stable;
Rust function names may change.

The contract a consumer depends on is API-006, the kernel records contract
(ADR-025). Three kinds of evidence verify it, and none substitutes for
another:

- **The black-box contract suite.** `fireweed-contract-suite` runs in-process
  against `Arc<dyn Store>` through `run_all(constructor, reopen, Δ, clock,
  fault_hook)`. It has at least one scenario per MUST clause of API-006 §4, §5
  and §6, each citing its clause id, and runs every scenario twice: directly,
  and through a torn-read adversary for `stats` and `aggregate` (CS1–CS3,
  CS6). It is the functional qualification of every `Store` implementation.
  It has no per-backend clause, knob, capability probe or backend identity
  (CS4).
- **The operator suite and the operator ban.** A separate suite takes the admin
  handle and tests OP1–OP3. CI bans the `Operator` trait and the admin handle
  in Snorri, in the workload and loadgen crates, and in the contract suite
  (OP4, CS5).
- **Performance gates.** The three cost rules the black-box suite cannot
  observe (C9, the S2 cost bound, and F4's rule that a projection reads no
  unreturned field value), and the storage rules the spikes found necessary,
  are verified on S3×Turso by the TP-003 §3.12 gates. Their evidence is
  SPIKE-002 to SPIKE-006.

Companion plans:

- **TP-002** owns scale evidence: the E0–E3 records.
- **TP-003** owns every pass bar: the acceptance criteria (`AC-*`), the safety
  invariants (`INV-*`) and the CI quality gates.
- **TP-004** owns the public surface and consumer acceptance (Snorri).
- **TP-005** owns the host-bound capacity matrix.

## The implementation under test

Fireweed has one implementation of API-006: the S3 object log × Turso
projection (ADR-024). TD-017 lays out the Turso projection, and TD-018 designs
the partitioned object log. The object log is the durable copy. The Turso
projection is derived, and it is rebuilt from the log through projection
control on the implementation's admin handle (`TursoAdmin`). A test that
verifies, deletes or rebuilds the projection calls projection control;
unlinking the Turso files and reopening is not a substitute. Crash loss and
rebuild on open belong in the recovery suites.

## Test Layers

| Layer | Location | Purpose |
|-------|----------|---------|
| Contract crate | `fireweed-contract` unit and property tests | Validation, `fingerprint`, `canonical_len` (LIM1), the ESF index-key encoder and the order comparators; the clause manifest, under which every public item cites an API-006 clause (TP-003 §3.1). |
| Kernel contract | `fireweed-contract-suite`, run by each `Store` implementation's tests | `run_all` over the constructor (both `DeletePolicy` values), `reopen`, Δ, a manual `Clock` and the post-commit fault hook; one scenario per MUST clause, run directly and through the torn-read adversary; no backend knobs (TP-003 AC-KC-1 to AC-KC-3). |
| Operator | a separate suite that takes the admin handle | OP1–OP3 (`force_clear`), plus the CI guard for OP4 (TP-003 AC-KC-4, AC-KC-5, AC-OP-1). |
| S3×Turso implementation | the implementation crates' tests | Turso qualification, object-log durability and rebuild, crash and fault duress, partition ownership and fencing, lease-token storage, and the plan tests of TD-017 §11 (TP-003 §3.2 to §3.9). |
| Performance gates | claim-path and storage gates on S3×Turso | C9, the S2 cost bound, F4's read rule, the claims table, statement discipline, the lean claim row, and queues per file (TP-003 AC-PERF-1 to AC-PERF-8). |
| Product workflows | `product_workflow_*` suites over `Fireweed` | The product's workflows composed from the eight ops, with fault injection (TP-003 §3.10). |
| Consumer acceptance | TP-004 | The public surface of `fireweed`, and Snorri running on the kernel contract as API-003 maps it. |
| Scale and capacity | TP-002, TP-005 | Scale evidence records E0–E3; the host-bound capacity matrix. |
| Toolchain | CI | ADR-003: `cargo fmt`, `cargo clippy -D warnings`, `cargo test`, dependency checks, unsafe denial. |

## Requirement Coverage Matrix

Clause ids are API-006 clauses. Pass bars are in TP-003.

| Requirement | Governing clauses | Required test evidence |
|-------------|-------------------|------------------------|
| API-006 conformance | CS1–CS7 | `fireweed-contract-suite` passes on every `Store` implementation with at least one scenario per MUST clause of §4, §5 and §6, each citing its clause id, including every CS6 scenario; every scenario also passes through the torn-read adversary; zero skips; no per-backend clause, knob or capability probe (AC-KC-1 to AC-KC-3). |
| API-006 cost rules | C9, S2, F4 | Stage-bound claim plans, bounded `stats.groups` reads and projected field reads pass AC-PERF-1; per-claim leases, statement discipline, the lean claim row, same-run scale ratios and queues per file pass AC-PERF-2 to AC-PERF-6 and AC-PERF-8; the per-queue commit cost is recorded (AC-PERF-7). |
| FR-1 queue isolation | ISO1, A4 | The contract suite and AC-SEC-1: equal queue names in two tenants stay isolated, and a token used on another queue gets `StaleLease`. |
| FR-2, FR-3, FR-4, FR-7, FR-30 rank and claim order | C1, C4, ORD1–ORD3 | Claims return candidates in `(rank, insertion sequence)` order within their stage; equal ranks go in insertion order across stage moves; a future rank is not claimable until the horizon reaches it (CS6; AC-CLAIM-3, AC-CLAIM-4). |
| FR-6 queue definition | D1–D6, §5.1 | Create-or-verify, normalization, `DefinitionConflict` with the stored definition, the default stages, and validation of stages, fields and indexes. |
| FR-56 queue metadata | W13, R4, §5.2 | A new queue's empty metadata at version 0; a metadata CAS in one unit with record writes, and its rejection rolling the unit back; the 64 KiB bound; fresh `read{queue: true}`. |
| FR-57 `delete_queue` | DQ1–DQ5 | Both policies on a non-empty queue with a live claim: `NotPermitted` for a full delete under `ArchiveOnly` on an open and on an archived queue; `QueueClosed` for every other op afterwards, including after `reopen`; `already_closed` on repeat. |
| FR-10, FR-41 ready groups and ready age | S2 | `ready_groups` and `oldest_ready_since`, which reports when the longest-waiting ready record became Free, never its rank; a horizon earlier than now is `Invalid`. |
| FR-11, FR-26 lease expiry | V4, L2, L4 | Expiry frees every record still attached to the claim within Δ, at its rank and insertion sequence (AC-CLAIM-2). |
| FR-14, FR-15 claimability | C1, C5, C5a | Candidates are Free records of the stage with `rank <= horizon` and no Blocked gate while the queue is not paused; a quiet queue returns the first candidates. |
| FR-16 fields and indexes | F1–F3, W8, §5.1 | Undeclared fields and wrong types rejected; 64 KiB `Str` and `Bytes` values round-trip through `write`, `claim` and `read`, and after `reopen`; the 4 MiB record bound; a `scan` over a `Bytes`-keyed index returns hits in bytewise order, and an encoded index key over 1024 B is `Invalid`. |
| FR-59 field projection | F4 | `claim` and `read` with `None`, one field, and an empty list; an undeclared or repeated name is `Invalid`; records of about 4 MiB claimed with an empty list come back up to `max_records` (CS6), and an empty projection reads no field storage (AC-PERF-1). |
| FR-17, FR-45 gates and pause | W9, C1 | Gate ops change no record and no version; a record with a Blocked gate is never claimed; the `"*"` gate pauses the queue (AC-GATE-1, AC-GATE-2). |
| FR-18 to FR-22 keys, writes and request ids | W1–W5, W7, W8, RQ1–RQ4 | Envelope validation before any effect; W2a precedence; `Expect` checks; sparse patches; per-unit outcomes; replay and conflict by `request_id`; an id-less unknown outcome resolved by `read` (AC-CORE-3; AC-TXN-2, AC-TXN-3). |
| FR-23 stages | §3, W5 | The default stages and the insert default; a move between any two stages, back to the first included. |
| FR-24, FR-25, FR-28 claims and leases | C3, C6, L1–L6 | One token per claim; `Extend` renews every attached record; `Release` detaches one record while the others stay attached; a lease-free write to a held record gets `Leased` (AC-CLAIM-1, AC-CLAIM-6). |
| FR-27 durability | DUR1 | `reopen` scenarios in the contract suite; rebuild from the object log and the crash-point matrix on S3×Turso (AC-TXN-1, AC-TXN-4). |
| FR-29, FR-31, FR-31a, FR-32, FR-35 batch and group claims | C2, C4, C6a, C7, W10, §6.8 | `Keys` skip reasons; `WholeGroups` exclusivity across stages, whole groups only, and `partial_group`; the response byte limit never empties a claim (AC-GRP-1). |
| FR-33, FR-34 settling | W2, W4, W5, W7 | Settles as stage moves, new ranks and deletes under `Release(t)`; rejected units report the failing write and its reason. |
| FR-36, FR-49, FR-51 to FR-55 attempts and recurring records | §3, W5, W7, S1, Q4 | `attempts` unchanged across a release and a reschedule; a recurring record is one record per key, re-ranked on settle and ended by `Delete` (AC-E2E-4). |
| FR-40, FR-41 counts and aggregates | S1, S2, Q1–Q5 | Stage counts and `paused`; `aggregate` over declared index ranges; every scenario also runs through the torn-read adversary (AC-KC-2). |
| FR-60 inspection | R1, R3, R4, Q1–Q3, V3 | `read` of up to 1000 keys in any stage and lease state, with the definition and metadata on request; `scan` paging that neither duplicates nor skips unchanged records; both fresh, and neither returns a token (AC-SEC-2). |
| FR-42 telemetry | outside the contract | Telemetry overhead stays within AC-LAT-4. |
| FR-43 noisy-neighbor isolation | outside the contract | TP-002 E2 queue density and TP-003 AC-E2E-6. |
| FR-44 to FR-47c, FR-55 Seventh Sense validation | API-003 | TP-003 AC-E2E-1 to AC-E2E-4 and AC-E2E-9. |
| FR-58 operator | OP1–OP4 | The operator suite passes OP1–OP3 through the admin handle, and the CI guard finds no `Operator` or admin-handle reference in Snorri, the workload and loadgen crates, or the contract suite (AC-KC-4, AC-KC-5, AC-OP-1). |
| Public surface (ADR-025 §1) | CS7, §7 | TP-004's surface gates: `Fireweed` delegates 1:1 to `Arc<dyn Store>`, and the public API is the eight ops, the contract types, the S3×Turso constructor and its admin handle. |
| Consumer migration (API-003) | ADR-025 §4 | TP-004's Snorri semantic ids. |
| Object-log durability and rebuild | ADR-024, TD-010 | AC-TXN-1, AC-TXN-4, AC-TURSO-3, and the object-log format gate (TP-003 §5). |
| Queue placement and partition ownership | ADR-008, TD-003, ADR-025 §5 | Epoch fencing of a deposed partition owner, a partition handoff without loss or double lease, and no queue split across partitions or owners (AC-OWN-2, INV-9; TP-002 E2). |
| Durability under duress | DUR1, E1, E2, RQ1–RQ4, V2, V3 | TP-003 §3.9, AC-TXN-1 to AC-TXN-12. |
| ADR-003 Rust policy | ADR-003 | `cargo fmt`, `cargo clippy -D warnings`, `cargo test`, dependency checks and unsafe denial run in CI. |

## Named Test Suites

Implementation beads create or extend these suites.

| Suite | Verifies |
|-------|----------|
| `kernel_contract_suite` (`fireweed-contract-suite`, API-006 §9; direct and torn-read runs) | AC-KC-1 to AC-KC-3 |
| `kernel_operator_suite` (through the admin handle) | AC-KC-5 |
| `operator_ban_guard` (CI guard) | AC-KC-4 |
| `contract_crate_tests` | AC-CORE-3, AC-CORE-5, AC-CORE-6 |
| `claim_lease_stress_tests` | AC-CLAIM-1 to AC-CLAIM-6 |
| `group_gate_stress_tests` | AC-GRP-1, AC-GATE-1, AC-GATE-2 |
| `queue_isolation_tests` | AC-SEC-1, AC-SEC-2 |
| `queue_ownership_fencing_tests`, `queue_reassignment_drain_tests` | AC-OWN-2; TP-002 E2 |
| `operator_stress_tests` | AC-OP-1 |
| `degradation_gate_tests` | AC-LAT-1, AC-LAT-2, AC-LAT-4 |
| `turso_projection_qualification_tests` | AC-TURSO-1 to AC-TURSO-4, AC-TURSO-6 |
| `object_log_format_tests` | the TP-003 §5 object-log format gate |
| `object_log_commit_recovery_tests` | AC-TXN-1; TP-002 E3 |
| `external_transaction_contract_matrix_tests` | AC-TXN-1 to AC-TXN-12 |
| `fault_injection_harness_tests` | the kill, resend and partition-handoff harness that AC-CLAIM-2, AC-OWN-2, AC-TXN and AC-E2E use |
| `invariant_stress_matrix_tests` | the TP-003 §2 invariants |
| `claim_path_performance_gates` | AC-PERF-1 to AC-PERF-8 |
| `product_workflow_scheduled_action_delivery_e2e` | AC-E2E-1 |
| `product_workflow_marketo_group_batching_e2e` | AC-E2E-2 |
| `product_workflow_callback_batch_e2e` | AC-E2E-3 |
| `product_workflow_jobs_connectors_recurring_e2e` | AC-E2E-4 |
| `product_workflow_worker_crash_recovery_e2e` | AC-E2E-5 |
| `product_workflow_noisy_neighbor_scale_e2e` | AC-E2E-6 |
| `product_workflow_operator_repair_redrive_e2e` | AC-E2E-7 |
| `product_workflow_generic_rank_ordering_e2e` | AC-E2E-8 |
| `product_workflow_downstream_pacing_non_goal_e2e` | AC-E2E-9 |
| `seventh_sense_validation_tests` | the Seventh Sense-shaped subset, AC-E2E-1 to AC-E2E-4 |
| `product_validation_tests` | `AC-SEN`: AC-E2E-1 to AC-E2E-6, AC-E2E-8, AC-E2E-9 |
| `operator_validation_tests` | AC-E2E-7 and the operator suites |
| `public_surface_tests`, `snorri_acceptance` | TP-004 |

The scale suites (`performance_single_deployment_baseline_tests`,
`performance_cross_queue_scale_out_tests`, `queue_density_single_node_tests`,
`object_log_latency_cost_matrix_tests` and
`recurrence_scale_envelope_matrix_tests`) are owned by TP-002, and the capacity
matrix runner by TP-005.

## Performance Evidence

Performance testing covers:

- the TP-003 §3.12 gates: stage-bound claim plans with no stage-blind
  statement, bounded `stats.groups` reads, projected field reads, per-claim
  lease cost, statement discipline, the lean claim row, the 1M-to-10M and
  100-queue same-run ratios, and queues per file;
- the per-queue commit cost on the local S3 test endpoint at 1, 10, 100 and
  1,000 queues, recorded as a measurement (AC-PERF-7);
- batch write, claim, extend and settle throughput, with p95 and p99 latency
  and telemetry enabled;
- a 10M-record hot queue with records in several stages, future ranks and live
  claims, and skewed rank and group distributions;
- `WholeGroups` claims over many groups;
- one hot queue beside one small queue with candidates (AC-E2E-6, TP-002 E2).

## Deferred Evidence

These are not required before the first implementation bead, but must be
covered before claiming product validation:

- Seventh Sense's production ready-to-claim bound, which sets the E0 and E1
  workload bound (TP-002).
- Seventh Sense's own cutover. It is written in Scala, so it needs a transport
  over `Store` (ADR-025 §4). Until that transport exists, the product workflow
  suites model its shapes in-process.

## Exit Criteria

Every implementation bead cites at least one requirement or API-006 clause and
at least one suite named here. A `Store` implementation is accepted only when
the contract suite and the operator suite pass with zero skips, the operator
ban holds, and the TP-003 §3.12 gates pass on it. Every other gap is recorded
as an explicit deferred item with an owner.
