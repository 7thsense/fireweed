---
ddx:
  id: concerns
  depends_on:
    - product-vision
    - prd
  review:
    self_hash: 7e3b81e376f75f71691f55ac1ca4d9599eddcfe6eefe70f614c366c132e07992
    deps:
      prd: 6cbaa8249fac452e44d8cbde9f63982fc2fc5f9f04f1eeeba68b0b1a9c86291f
      product-vision: 0e59c80e42299b3426b95bc91d71ad509059dcadf3a47aa74b688acb6c626e5e
    reviewed_at: "2026-07-06T00:56:00Z"
---

# Project Concerns

Project Concerns declare active cross-cutting context for downstream pqueue
work. They are not principles, requirements, ADRs, test plans, or
implementation tasks.

## Active Concerns

| Concern | Source | Areas | Why Active | Key Practices |
|---------|--------|-------|------------|---------------|
| `durable-priority-queue-semantics` | project-local | `area:core`, `area:api`, `area:data`, `area:infra`, `area:testing` | pqueue's core value depends on preserving priority, eligibility, lease, idempotency, progress-bound, and batch/group semantics across every downstream artifact. | Keep priority separate from eligibility; enforce single active lease; state at-least-once delivery; preserve idempotency horizons; make progress bounds observable and non-starving; test batch and group-aware claims under skew. |
| `enterprise-integration-patterns` | library | `area:core`, `area:api`, `area:infra`, `area:testing` | pqueue is itself an asynchronous work channel and may expose compatibility adapters shaped by known messaging patterns. | Treat delivery as at-least-once; design idempotent receivers; define dead-letter/invalid-message paths; carry correlation identifiers; keep broker/client mechanics behind explicit gateways. |
| `concurrency-model` | library | `area:core`, `area:data`, `area:infra`, `area:testing` | Claims, leases, batch operations, and relaxed priority ordering are concurrency-sensitive and must remain bounded under worker contention. | Name the concurrency model; eliminate or guard shared mutable state; bound in-flight work; avoid unbounded buffers; test duplicate execution and contention paths. |
| `resilience` | library | `area:core`, `area:api`, `area:infra`, `area:testing` | Queue users and pqueue internals will depend on bounded calls, retry safety, lease recovery, overload handling, and steady-state resource limits. | Bound outbound calls with timeouts; retry only idempotent operations with backoff and jitter; use bulkheads or quotas for noisy workloads; define load-shedding/backpressure; cap accumulating resources. |
| `o11y-otel` | library | `area:core`, `area:api`, `area:infra` | Progress bounds, leases, retries, queue depth, and latency targets are only governable if observable. | Emit traces, metrics, and structured logs; propagate correlation IDs; expose RED metrics and queue-specific gauges; verify observability overhead under load. |
| `api-style` | library | `area:api` | pqueue will need a native API and likely compatibility adapters, so contract shape, versioning, errors, and input validation must stay consistent. | Publish typed/versioned contracts; validate inputs at the boundary; use standard error shapes; keep wire contracts separate from the internal queue model; record non-default API style choices in ADRs. |
| `security-owasp` | library | `area:api`, `area:data`, `area:infra` | pqueue accepts untrusted payloads, metadata, API requests, and dependency inputs; a future service form must not leak secrets, internal errors, or cross-queue data. | Validate all external inputs; parameterize queries; audit dependencies; keep secrets out of source; enforce TLS for network surfaces; avoid implementation-detail leakage. |
| `auth` | library | `area:api`, `area:data`, `area:infra` | pqueue must be designable as a service where queue access is scoped by authenticated principals, not only by a single outer gate. | Model principals/accounts; derive scope from the authenticated principal; keep auth provider swappable; ensure protected operations are exercised end to end when a service surface exists. |
| `authorization-model` | library | `area:api` | Queue operations need a deny-by-default permission model for create, push, update, claim, finalize, inspect, repair, and purge. | Choose RBAC/ABAC/ReBAC deliberately; centralize policy decisions; authorize every state-changing and data-returning handler; test denied paths. |
| `multi-tenancy` | library | `area:api`, `area:data`, `area:infra`, `area:testing` | Queue namespaces should be able to carry tenant/account boundaries into storage and capacity control so one tenant cannot read, mutate, or starve another. | Choose and record an isolation model; enforce tenant scope at the data-access/storage boundary; test negative cross-tenant reads/writes; use quotas/rate limits for noisy-neighbor protection. |
| `deployment-topology` | library | `area:api`, `area:data`, `area:infra`, `area:testing` | pqueue is expected to run like Niflheim where possible: stateless service containers behind a load balancer, tenant/queue shards assigned to workers, and durable state externalized or persisted through simple storage primitives. | Keep compute nodes stateless or rebuildable; make shard ownership and reassignment explicit; avoid mandatory external coordinators or embedded consensus unless an ADR proves they are necessary; test failover, replay, and noisy-neighbor isolation. |
| `rust-cargo` | library | `area:core`, `area:api`, `area:data`, `area:infra`, `area:testing` | pqueue is a high-performance, bounded-memory, reliability-sensitive system; Rust gives memory safety, predictable resource control, and strong concurrency tooling. | Use a pinned stable Rust toolchain; organize as a Cargo workspace; deny unsafe by default; use explicit error handling; run fmt, clippy, cargo-deny, cargo-machete, tests, benchmarks, and loom for critical concurrency. |
| `testing` | library | `area:core`, `area:api`, `area:data`, `area:infra`, `area:testing` | Queue correctness depends on adversarial scenarios, not only happy-path unit tests. | Use multi-layer tests; prefer stubs over mocks; add property, fuzz, chaos, and performance ratchets; trace tests to acceptance criteria. |
| `verification` | library | `area:core`, `area:api`, `area:data`, `area:infra`, `area:testing` | A queue cannot be called correct until its real claim, lease, retry, recovery, and batch paths are observed under realistic load. | Record commands, exit status, environment, exercised flows, guard branches, and adversarial re-review before claiming completion. |

## Deferred Concerns

These concerns are intentionally not active yet:

| Concern | Why Deferred |
|---------|--------------|
| `admin-console` | The PRD does not include a v1 operator UI; a dashboard is P2. |
| `auth-local-sessions` | Auth is active as a product/design concern, but the provider filler is undecided. Select local sessions only if the service/control-plane design chooses it over an external IdP or machine-token model. |
| `relational-data-modeling` | Storage requires design and spiking, but the storage engine is intentionally undecided. Select this only if a relational store becomes the chosen design. |

## Project Overrides

| Concern | Practice | Override | Authority |
|---------|----------|----------|-----------|
| `auth` | Real signup/login/session surface | pqueue may first ship as a library or machine-facing service, so human signup/login is not assumed. Service mode uses provider-neutral principal resolution and operation-scoped authorization. | ADR-002 |
| `multi-tenancy` | Tenant identity derived from authenticated principal | Queue namespace and tenant/account identity may be distinct. The v1 model maps principal -> tenant -> queue -> shard, with tenant keys carried into storage. | ADR-002 |
| `deployment-topology` | Modular service or app deployment chosen during implementation | The preferred topology is a Rust service of stateless containers behind a load balancer, with tenant/queue shards allocated through a Postgres control plane and fenced by assignment epoch. pqueue does not require ZooKeeper, etcd, or an internally maintained consensus protocol for the first backend. | ADR-001, TD-001, TD-002 |
| `rust-cargo` | Latest stable Rust, Cargo workspace, and strict lint/tooling policy | Rust is selected with a pinned stable toolchain, Cargo workspace boundaries, Tokio service/backend runtime, strict linting, dependency checks, and unsafe denied by default. | ADR-003 |
| `verification` | Whole-stack evidence for buildable products | For docs-only and pre-implementation artifacts, evidence is document review plus `git diff --check`/placeholder checks. Implementation work must use running-system evidence or a recorded exception. | HELIX verification exception |

## Area Labels

This project uses the following area labels for concern scoping:

- `area:core` - queue engine semantics, priority ordering, eligibility,
  lifecycle, leases, batch/group claims, retry, and progress bounds
- `area:api` - native pqueue APIs, client SDK surfaces, and compatibility
  adapters such as SQS-shaped operations
- `area:data` - durable item state, idempotency state, retention, archival,
  indexes, and storage consistency
- `area:infra` - deployment, scaling, observability pipeline, capacity controls,
  benchmark environments, and operational repair surfaces
- `area:testing` - unit, integration, property, fuzz, chaos, load, and
  verification evidence

## Concern Conflicts

| Conflict | Resolution |
|----------|------------|
| `durable-priority-queue-semantics` vs. `concurrency-model` throughput pressure | Bounded-relaxed ordering may trade exact order for throughput, but mandatory progress bounds and single active lease are not optional. |
| `enterprise-integration-patterns` at-least-once delivery vs. consumer side effects | pqueue guarantees at-least-once with a single active lease; downstream consumers must remain idempotent. |
| `resilience` retry guidance vs. queue retry semantics | Retries must be capped and observable. Queue retry policy governs item redelivery; synchronous client retries must still use idempotency keys and backoff. |
| `o11y-otel` instrumentation vs. performance targets | Instrumentation is required, and load tests must measure overhead with telemetry enabled. |
| `api-style` compatibility adapters vs. pqueue native model | Compatibility APIs may mirror SQS or other systems, but they cannot force the core model to drop priority updates, progress bounds, or group-aware claims. |
| `security-owasp` input validation vs. opaque payloads | Payloads may be opaque to pqueue, but queue-owned fields, metadata predicates, priority values, and API envelopes must be validated at the boundary. |
| `auth`/`multi-tenancy` vs. general open-source library use | Core semantics should not require a hosted auth system, but all service and storage designs must preserve principal-scoped queue access and tenant/account isolation boundaries. |
| `multi-tenancy` vs. storage flexibility | Tenant-aware storage isolation is a design goal, but the concern does not pick a storage engine; storage design must compare isolation, noisy-neighbor control, and operational cost. |
| `deployment-topology` stateless containers vs. local SQLite/WAL candidate | Container-local state may only be hot shard state or a recoverable cache unless the storage spike proves the durability boundary. Ack semantics, replay, compaction, and object-store persistence must be explicit. |
| `deployment-topology` shard assignment vs. no mandatory coordinator | Shard placement must avoid a required ZooKeeper/etcd-style dependency and avoid embedding a cluster consensus algorithm in pqueue. If single-writer shard ownership needs coordination, prefer storage-backed leases, deterministic assignment, or other simple mechanisms that can be validated under failure. |
| `deployment-topology` vs. `multi-tenancy` | Shard placement and load balancing must preserve tenant/queue isolation and noisy-neighbor controls; generic load balancing cannot bypass shard ownership. |
| `rust-cargo` vs. performance shortcuts | Bounded memory and reliability take precedence over unsafe shortcuts. Any unsafe code or lock-free structure requires explicit design rationale, safety invariants, and concurrency tests. |

## Required Design Coverage

The following concern questions must be answered before implementation work is
broken into beads. Current coverage:

| Question | Status | Governing Artifact |
|----------|--------|--------------------|
| How queue namespace, tenant/account identity, and physical storage partition map to each other. | Covered for v1 service and Postgres-native backend. | ADR-002, ADR-004, TD-002 |
| Whether isolation uses shared predicates, database-enforced isolation, tenant partitions, dedicated storage, or a hybrid. | Covered as a layered isolation model; shared predicates are the v1 default with stronger placement allowed by deployment class. | ADR-002 |
| How noisy-neighbor controls work at the storage layer for hot tenants, 10M-item queues, and group-heavy claims. | Covered as storage/test requirements; exact deployment-level quotas and rate-limit policy remain P1. Enforcing callers' downstream-API rate limits is a non-goal; callers pace via API-001 claim knobs. | ADR-002, TD-002, TP-002 |
| Whether a Niflheim-like topology can satisfy pqueue without a mandatory coordinator or embedded consensus. | Covered through Postgres control-plane assignments and backend epoch fencing. | ADR-001, TD-001, TD-002 |
| How shard ownership, fencing, lease epochs, reassignment, and graceful drain work behind a load balancer. | Covered by sharding & shard ownership: deterministic assignment, storage-backed leases, epoch fencing, rebalance, and graceful drain. | TD-003 |
| Whether deterministic assignment, storage-backed leases, or another simple ownership mechanism is enough. | Covered: deterministic assignment + storage-backed shard leases + monotonic epoch fencing, no consensus. | TD-003 |
| Whether SQLite with WAL persisted to object storage can meet durability, replay, compaction, and write-amplification requirements. | Covered by the committed object-log + SQLite projection backend (group commit, manifest CAS, snapshot, bounded replay). | TD-004 |
| How stateless containers rebuild local hot state after restart, failure, or owner reassignment. | Covered for both backends: Postgres-native via Postgres projection; object-log via SQLite snapshot + bounded log replay; queue-owner reassignment recovery via TD-003. | TD-002, TD-003, TD-004 |
| How the storage model supports batch push, batch update, batch claim, group-aware claim, lease expiry, progress-bound enforcement, per-group active-scope discovery (owner-local ranking), and retention. | Covered for the native contract and both v1 backends. | API-001, ADR-004, TD-001, TD-002, TD-004 |
| At what domain claim result order is guaranteed (effective claim domain; a single `group_key` gives exact per-group order because the group is co-resident on the queue's owner by construction; `same_group_key` is a restriction filter only and may return a partial group), that physical placement is never a client-visible ordering/progress scope, and that the queue is the unit of sharding (ADR-008; no `group_co_residency` flag). | Covered. | ADR-008, ADR-004, API-001, TD-001, TD-002 |
| How all-or-nothing complete-cohort claims are gated on completeness + per-member eligibility, leased atomically under one shared lease, owner-local by construction (every `group_key`'s members on the queue's owner), expired against a cohort-liveness bound enforced `<= progress_bound_ms` (without a second progress scope), idempotent under duplicate push, and prevented from leaking members to other claim units, across both v1 backends. | Covered for the native contract and both backends (Postgres-native TD-002 and object-log + SQLite TD-004) via opt-in `cohort_policy`. | API-001, ADR-004, TD-001, TD-002, TD-004 |
| How recurring / never-terminal item lifecycle works: `rearm` outcome (caller-supplied eligibility materialized as `max(commit_time, not_before)` fed into the shared Eligibility Precedence timing stage, attempt-exempt, per-cycle retry reset), explicit termination (terminal finalize / `recurrence.until` drain / targeted in-band `PurgeItems` with tombstone + replay semantics, distinct from the P1 operator purge contract), recurring metrics served from the metrics projection (idle vs active, excluded from retry backlog/failure and from oldest eligible age while idle), queue-global (not per-group) progress parity, owner-local co-residency by construction (D2 / ADR-008), cohort / recurrence mutual exclusivity (ADR-004), gate composition, active-discovery parity via `pqueue_group_summary`, and queue-local purge replay semantics. | Covered for the native contract and both backends. | API-001, ADR-004, TD-001, TD-002, TD-004 |
| Whether the headline horizontal-scale claim is substantiated by a buildable, benchmarked mechanism rather than by `postgres_native` alone. | Covered by committed v1 per-queue ownership + cross-queue distribution (ADR-008) + object-log backend, each with a benchmark evidence record. Tracked until E2/E3 prove the per-queue floor E0 (>=10M items/hr per queue, preserved for every queue at any scale). | product-vision, PRD, ADR-001, ADR-008, TD-001, TD-003, TD-004, TP-002 |
| Whether a single node supports the required queue density (>=1000 concurrently active queues) without per-queue resource blow-up or cross-queue degradation. | Covered by the bounded shared per-node resource rule: per-queue background work (lease-expiry sweeps, progress-bound aggregation, summary recompute, recurring rearm, idempotency/retention GC) multiplexed onto bounded shared pools, batched per-node lease renewal, and LRU-bounded per-queue projection handles. Tracked until `queue_density_single_node_tests` (TP-002 E2) passes. | PRD, ADR-002, ADR-003, TD-001, TD-002, TD-003, TD-004, TP-002 |
| How migration from Seventh Sense's existing queue-like tables preserves tenant/account boundaries and operational visibility. | Deferred until pqueue core and Postgres-native backend exist; requires a later migration design. | PRD, TD-001 |
