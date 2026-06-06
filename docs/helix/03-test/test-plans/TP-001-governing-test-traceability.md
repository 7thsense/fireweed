---
ddx:
  id: tp-governing-test-traceability
  depends_on:
    - prd
    - api-native-client-interface
    - adr-auth-tenancy-and-storage-isolation
    - adr-rust-workspace-and-toolchain-policy
    - td-storage-architecture-backend-contracts
    - td-postgres-native-reference-mode
---

# Test Plan: TP-001 Governing Test Traceability

## Scope

This plan defines the minimum test coverage required before the first pqueue
implementation can be considered aligned with the current HELIX frame and
design. It maps PRD functional areas and design decisions to named test suites.

This is a pre-implementation test plan. Exact Rust function names may change
when the workspace is created, but implementation beads must preserve the suite
intent and cite the relevant requirement IDs.

## Test Layers

| Layer | Location | Purpose |
|-------|----------|---------|
| Core unit | `crates/pqueue-core/src/**` | Pure validation, priority encoding, lifecycle, retry, idempotency, and version rules. |
| Storage conformance | `crates/pqueue-storage/tests/**` | Backend-independent durability, replay, lease, idempotency, and progress scenarios. |
| Postgres integration | `crates/pqueue-postgres/tests/**` | TD-002 schema, transaction, locking, retention, and fencing behavior against real Postgres. |
| Service/API integration | `crates/pqueue-service/tests/**` | HTTP route, auth, tenant, error shape, and API-001 response behavior. |
| Performance | `benches/**` or `crates/*/benches/**` | Batch throughput, claim latency, 10M-item projection/query fixtures, telemetry overhead. |
| Security | service and backend integration tests | Denied paths, tenant isolation, lease-token handling, and payload/log safety. |

## Requirement Coverage Matrix

| Requirement | Governing Artifact | Required Test Evidence |
|-------------|--------------------|------------------------|
| FR-1 | PRD queue namespace isolation | Create two queues in one tenant and same queue IDs in different tenants; routing, metrics, and storage keys remain isolated. |
| FR-2, FR-3, FR-4 | PRD priority model | Create timestamp and non-timestamp queues; valid priorities encode to deterministic `priority_sort`; invalid priorities fail. |
| FR-5, FR-6 | PRD ordering mode immutability | Create strict and bounded-relaxed queues; attempts to mutate priority model or ordering mode fail. |
| FR-7, FR-14 | PRD strict ordering | Strict claim returns eligible items by priority plus tie-breaker under concurrent workers. |
| FR-8, FR-9, FR-12, FR-13 | PRD bounded-relaxed progress | Relaxed claim may reorder within bounds but cannot starve oldest eligible items beyond `progress_bound_ms`. |
| FR-10, FR-11, FR-15, FR-17 | PRD eligibility | Future `not_before`, retry backoff, active lease, and metadata blockers prevent claim; lease expiry preserves progress clock. |
| FR-16 | PRD payload/metadata | Opaque payload and caller metadata round-trip through push and claim without queue-owned interpretation. |
| FR-18, FR-19, FR-21, FR-22 | PRD/API idempotent push | Duplicate `client_item_key` converges; repeated `request_id` replays; conflicting request body fails; retention expiry bounds dedupe. |
| FR-20 | PRD/API batch update | Pending items update priority, `not_before`, payload, and metadata; leased and terminal items return per-item conflicts. |
| FR-23, FR-24, FR-25, FR-26, FR-27, FR-28 | PRD lease lifecycle | Claim creates one active lease; concurrent claims never duplicate; crash/retry after lease expiry redelivers; state survives restart. |
| FR-29, FR-30, FR-31, FR-32, FR-35 | PRD batch/group claim | Batch limit enforced; same-group and metadata-compatible claims return deterministic compatible batches without group starvation. |
| FR-33, FR-34, FR-36, FR-37 | PRD finalize/retry | Complete, fail, retry, release, stale lease, terminal conflict, and retry exhaustion produce correct per-item results. |
| FR-38, FR-39 | PRD terminal durability/retention | Terminal complete/failed facts persist with final command position and are deleted only after configured retention windows. |
| FR-40, FR-41, FR-42 | PRD observability | Metrics expose lifecycle counts, active leases, retry backlog, oldest eligible age, progress-bound risk, throughput, and latency. |
| FR-43 | PRD noisy-neighbor isolation | Hot queue or tenant load does not prevent another queue from meeting claim latency and progress bounds under configured limits. |
| FR-44, FR-45, FR-46, FR-47 | PRD Seventh Sense validation | Timestamp scheduled work can be pushed early, updated later, gated through metadata, and claimed in compatible batches. |
| API-001 idempotency | API-001 | Request replay, request conflict, claim replay while leases are active, and request-expired after leases end. |
| API-001 auth | API-001 / ADR-002 | Principal authorized for tenant A cannot access tenant B routes or storage-backed data. |
| TD-001 durability | TD-001 | Kill process after acknowledged append; replay or committed Postgres rows preserve the command and projection state. |
| TD-001 backend conformance | TD-001 | Every backend passes shared conformance before it is selectable by backend profile. |
| TD-002 Postgres fencing | TD-002 | Stale `assignment_epoch` appends are rejected; current epoch appends succeed. |
| TD-002 Postgres locking | TD-002 | `FOR UPDATE SKIP LOCKED` claim tests prove single active lease under concurrent workers. |
| ADR-003 Rust policy | ADR-003 | `cargo fmt`, `cargo clippy -D warnings`, `cargo test`, dependency checks, and unsafe denial run in CI. |

## Named Test Suites

Implementation beads should create or extend these suites:

- `core_priority_model_tests`
- `core_lifecycle_transition_tests`
- `core_idempotency_tests`
- `storage_conformance_durability_tests`
- `storage_conformance_claim_tests`
- `storage_conformance_progress_tests`
- `postgres_schema_migration_tests`
- `postgres_transaction_flow_tests`
- `postgres_concurrency_claim_tests`
- `postgres_retention_tests`
- `service_auth_tenant_tests`
- `service_api_error_semantics_tests`
- `performance_hot_queue_10m_tests`
- `performance_batch_operation_tests`
- `seventh_sense_validation_tests`

## Performance Evidence

Performance testing must include:

- batch push, update, claim, renew, and finalize throughput;
- p95 and p99 latency with telemetry enabled;
- 10M-item hot queue with mixed pending, future, leased, retry, complete, and
  failed states;
- skewed priority distribution;
- group-heavy compatibility claims;
- one hot queue/tenant alongside one small eligible queue;
- Postgres claim query plans showing index use instead of full scans.

## Manual or Deferred Evidence

The following are not required before the first implementation bead, but must be
covered before claiming product validation:

- Seventh Sense production scheduling SLA for concrete `progress_bound_ms`
  validation.
- P1 operator redrive, purge, repair, archive, and active-queue discovery APIs.
- Object-log and SQLite projection performance profiles.
- Kafka/Redpanda and DynamoDB backend conformance.

## Exit Criteria

Before implementation beads are filed, each bead must cite at least one
governing artifact and at least one named test suite from this plan. Before a
backend implementation is accepted, the corresponding conformance, integration,
security, and performance evidence must exist or the gap must be explicitly
recorded as a deferred non-v1 profile.
