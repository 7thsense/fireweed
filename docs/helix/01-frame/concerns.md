---
ddx:
  id: concerns
  depends_on:
    - product-vision
    - prd
  review:
    self_hash: 52b6bbb92cff001a75227115afb20f4d0a73781ec98f49ab446a6866c17284dc
    deps:
      prd: 2d97b05f9c0c0db576149bdfef21c729d66e07dbb674c95f6b7135ddcffa3b91
      product-vision: d70aaff09b5d5f59211e5ef3ae9156ee30776e95bce7a70398978e83e39d39e8
    reviewed_at: "2026-07-20T00:01:20Z"
---

# Project Concerns

Project Concerns declare active cross-cutting context for downstream fireweed
work. They are not principles, requirements, ADRs, test plans, or
implementation tasks.

## Active Concerns

| Concern | Source | Areas | Why Active | Key Practices |
|---------|--------|-------|------------|---------------|
| `durable-priority-queue-semantics` | project-local | `area:core`, `area:api`, `area:data`, `area:infra`, `area:testing` | fireweed's core value depends on preserving rank order, claimability, leases, idempotency, progress, and batch/group semantics across every downstream artifact. | Keep rank separate from claimability; keep at most one live claim per record; state at-least-once delivery; preserve request-id replay; keep claims in strict rank order within a stage with the C5a progress rule (API-006); test batch and whole-group claims under skew. |
| `enterprise-integration-patterns` | library | `area:core`, `area:api`, `area:infra`, `area:testing` | fireweed is itself an asynchronous work channel, and a later transport may expose compatibility adapters shaped by known messaging patterns. | Treat delivery as at-least-once; design idempotent receivers; give failed work a consumer-named failure stage; carry correlation identifiers; keep broker/client mechanics behind explicit gateways. |
| `concurrency-model` | library | `area:core`, `area:data`, `area:infra`, `area:testing` | Claims, leases, batch operations, and storage suspension are concurrency-sensitive and must remain bounded under worker contention. | Name the concurrency model; prevent reactor stalls; preserve transaction affinity; bound in-flight work; avoid unbounded buffers; test cancellation, duplicate execution, and contention paths. |
| `resilience` | library | `area:core`, `area:api`, `area:infra`, `area:testing` | Queue users and fireweed internals depend on bounded calls, retry safety, determinate outcomes after interruption, lease recovery, overload handling, and steady-state resource limits. | Bound calls with timeouts; make interrupted commits resolvable; retry only idempotent operations with backoff and jitter; use bulkheads; define load-shedding/backpressure; cap accumulating resources. |
| `o11y-otel` | library | `area:core`, `area:api`, `area:infra` | Claim progress, leases, attempts, stage counts, and latency targets are only governable if observable. | Emit traces, metrics, and structured logs; propagate correlation IDs; expose RED metrics and queue-specific gauges; verify observability overhead under load. |
| `api-style` | library | `area:api` | The contract is the Rust `Store` trait (API-006); a network transport and compatibility adapters come later, so contract shape, versioning, errors, and input validation must stay consistent. | Publish typed/versioned contracts; validate inputs at the boundary; use standard error shapes; keep wire contracts separate from the internal queue model; record non-default API style choices in ADRs. |
| `security-owasp` | library | `area:api`, `area:data`, `area:infra` | fireweed accepts untrusted field values, queue metadata, requests, and dependency inputs; a future service form must not leak secrets, internal errors, or cross-queue data. | Validate all external inputs; parameterize queries; audit dependencies; keep secrets out of source; enforce TLS for network surfaces; avoid implementation-detail leakage. |
| `auth` | library | `area:api`, `area:data`, `area:infra` | fireweed must be designable as a service where queue access is scoped by authenticated principals, not only by a single outer gate. | Model principals/accounts; derive scope from the authenticated principal; keep the auth provider swappable; ensure protected operations are exercised end to end when a service surface exists. |
| `authorization-model` | library | `area:api` | Queue operations need a deny-by-default permission model for define, write, claim, read, stats, scan, aggregate, delete-queue, and force-clear. | Choose RBAC/ABAC/ReBAC deliberately; centralize policy decisions; authorize every state-changing and data-returning handler; test denied paths. |
| `multi-tenancy` | library | `area:api`, `area:data`, `area:infra`, `area:testing` | Queue namespaces carry tenant/account boundaries into storage and capacity control so one tenant cannot read, mutate, or starve another. | Choose and record an isolation model; enforce tenant scope at the data-access/storage boundary; test negative cross-tenant reads/writes; use quotas/rate limits for noisy-neighbor protection. |
| `deployment-topology` | library | `area:api`, `area:data`, `area:infra`, `area:testing` | fireweed is expected to run like Niflheim where possible: stateless or rebuildable service containers, queues mapped to partitions that nodes own, and durable state in simple storage primitives. | Keep compute nodes stateless or rebuildable; make partition ownership and handoff explicit; avoid mandatory external coordinators or embedded consensus unless an ADR proves they are necessary; test failover, replay, and noisy-neighbor isolation. |
| `rust-cargo` | library | `area:core`, `area:api`, `area:data`, `area:infra`, `area:testing` | fireweed is a high-performance, bounded-memory, reliability-sensitive system; Rust gives memory safety, predictable resource control, and strong concurrency tooling. | Use a pinned stable Rust toolchain; organize as a Cargo workspace; deny unsafe by default; use explicit error handling; run fmt, clippy, cargo-deny, cargo-machete, tests, benchmarks, and loom for critical concurrency. |
| `testing` | library | `area:core`, `area:api`, `area:data`, `area:infra`, `area:testing` | Queue correctness depends on adversarial scenarios, not only happy-path unit tests. | Use multi-layer tests; prefer stubs over mocks; add property, fuzz, chaos, and performance ratchets; trace tests to acceptance criteria. |
| `verification` | library | `area:core`, `area:api`, `area:data`, `area:infra`, `area:testing` | A queue cannot be called correct until its real claim, lease, retry, recovery, and batch paths are observed under realistic load. | Record commands, exit status, environment, exercised flows, guard branches, and adversarial re-review before claiming completion. |

## Deferred Concerns

These concerns are intentionally not active yet:

| Concern | Why Deferred |
|---------|--------------|
| `admin-console` | The PRD does not include a v1 operator UI; a dashboard is P2. |
| `auth-local-sessions` | Auth is active as a product/design concern, but the provider filler is undecided. Select local sessions only if the service/control-plane design chooses it over an external IdP or machine-token model. |
| `relational-data-modeling` | The Turso projection's schema (TD-017) is an implementation detail behind the contract, not a product data model. Select this concern if a relational implementation of `Store` is taken on. |

## Project Overrides

| Concern | Practice | Override | Authority |
|---------|----------|----------|-----------|
| `auth` | Real signup/login/session surface | fireweed ships first as an in-process library, so human signup/login is not assumed. A service form uses provider-neutral principal resolution and operation-scoped authorization. | ADR-002 |
| `multi-tenancy` | Tenant identity derived from authenticated principal | Queue namespace and tenant/account identity may be distinct. A queue is addressed by `(tenant, queue)`, and the tenant is independent of the partition the queue is mapped to. | ADR-002, ADR-025 |
| `deployment-topology` | Modular service or app deployment chosen during implementation | The topology is Rust nodes with rebuildable local state. The control plane maps each queue to one partition; each partition is owned by one node under a lease and fenced by epoch. fireweed does not require ZooKeeper, etcd, or an internally maintained consensus protocol. | ADR-025, TD-003, TD-018 |
| `rust-cargo` | Latest stable Rust, Cargo workspace, and strict lint/tooling policy | Rust is selected with a pinned stable toolchain, Cargo workspace boundaries, the Tokio runtime, strict linting, dependency checks, and unsafe denied by default. | ADR-003 |
| `verification` | Whole-stack evidence for buildable products | For docs-only and pre-implementation artifacts, evidence is document review plus `git diff --check`/placeholder checks. Implementation work must use running-system evidence or a recorded exception. | HELIX verification exception |

## Area Labels

This project uses the following area labels for concern scoping:

- `area:core` - queue engine semantics: rank order, claimability, stages,
  leases, batch and whole-group claims, and progress
- `area:api` - the kernel contract (API-006), the Rust facade, and a later
  network transport and compatibility adapters
- `area:data` - records, claims, request-id state, archives, indexes, and
  storage consistency
- `area:infra` - deployment, placement, observability pipeline, capacity
  controls, benchmark environments, and operational repair surfaces
- `area:testing` - unit, integration, property, fuzz, chaos, load, and
  verification evidence

## Concern Conflicts

| Conflict | Resolution |
|----------|------------|
| `durable-priority-queue-semantics` vs. `concurrency-model` throughput pressure | Every claim is in strict rank order within its stage, with C5a progress (API-006 C4, C5a). Throughput comes from stage-bound index walks (C9) and from more queues, never from trading order. At most one live claim per record is not optional. |
| `enterprise-integration-patterns` at-least-once delivery vs. consumer side effects | fireweed guarantees at-least-once delivery with at most one live claim per record; downstream consumers must remain idempotent. |
| `resilience` retry guidance vs. queue retry semantics | Retries must be capped and observable. The engine counts `attempts`; a consumer retries a record by re-ranking it and moves it to its own failure stage past its limit. Synchronous client retries use request ids, or `read`, and backoff. |
| `o11y-otel` instrumentation vs. performance targets | Instrumentation is required, and load tests must measure overhead with telemetry enabled. |
| `api-style` compatibility adapters vs. fireweed native model | A transport or compatibility adapter may mirror SQS or other systems, but it cannot force the core model to drop rank updates, claim order and progress (API-006 C4, C5a), or whole-group claims. |
| `security-owasp` input validation vs. uninterpreted data | The engine never interprets field values or queue metadata, but keys, stage names, field types, index keys, ranks, and every byte limit are validated at the boundary (API-006 W1). |
| `auth`/`multi-tenancy` vs. general open-source library use | Core semantics should not require a hosted auth system, but all service and storage designs must preserve principal-scoped queue access and tenant/account isolation boundaries. |
| `multi-tenancy` vs. shared partitions | Queues of many tenants share a partition and its Turso file. Isolation is by the queue key on every row and statement (API-006 ISO1, TD-017); stronger separation is a control-plane placement choice. |
| `deployment-topology` stateless containers vs. local Turso files | A node's Turso file is derived state, rebuilt from the S3 object log through projection control. The object log is the only durable authority (ADR-024). |
| `deployment-topology` partition ownership vs. no mandatory coordinator | Partition placement must avoid a required ZooKeeper/etcd-style dependency and avoid embedding a cluster consensus algorithm in fireweed. Ownership uses storage-backed partition leases and epoch fencing that can be validated under failure. |
| `deployment-topology` vs. `multi-tenancy` | Partition placement and load balancing must preserve tenant/queue isolation and noisy-neighbor controls; generic load balancing cannot bypass partition ownership. |
| `rust-cargo` vs. performance shortcuts | Bounded memory and reliability take precedence over unsafe shortcuts. Any unsafe code or lock-free structure requires explicit design rationale, safety invariants, and concurrency tests. |

## Required Design Coverage

The following concern questions must be answered before implementation work is
broken into beads. Current coverage:

| Question | Status | Governing Artifact |
|----------|--------|--------------------|
| How queue namespace, tenant/account identity, and physical placement map to each other. | Covered: a queue is `(tenant, queue)`. The control plane maps each queue to one partition, by assignment and never by hash; tenants are independent of partitions; queues are never rebalanced. | ADR-002, ADR-025, TD-018 |
| Which isolation model the storage uses. | Covered: queues of many tenants share a partition and its Turso file, isolated by the queue key on every row and statement (API-006 ISO1); a stronger separation is a placement choice. | ADR-002, TD-017, TD-018 |
| How noisy-neighbor controls work at the storage layer for hot tenants, 10M-record queues, and group-heavy claims. | Covered as storage and test requirements: one queue's backlog cannot stop another queue's progress (PRD FR-43). A partition's queues share one committer and one Turso writer, so a hot partition is relieved by moving the whole partition. Deployment-level quotas are P1. Enforcing callers' downstream-API limits is a non-goal; callers pace with claim size, cadence, a future rank, and `WholeGroups`. | TD-018, TP-002 |
| How partition ownership, fencing, handoff, and graceful drain work without a mandatory coordinator. | Covered: one control-plane lease and epoch per partition, with the epoch fenced by compare-and-swap on the partition's head document. Handoff is drain, lease, epoch + 1, fresh open, fence, hydrate, then serve. A partition moves only on node failure, drain, or expansion. | ADR-025, TD-003, TD-018 |
| Whether the object log meets durability, replay, and write-amplification requirements at scale. | Measurement-gated: TD-018 measures today's per-queue commit cost on the local S3 test endpoint at 1 to 1,000 queues first. Snapshots, truncation, log splitting, and full-delete GC are proposals until measured. | ADR-001, ADR-024, TD-018, TP-002 |
| How the async `Store` and the single Turso writer share a runtime without blocking a reactor or losing transaction affinity. | Covered: `Store` is async and object-safe (API-006 §4.0). Each Turso file has one `Mutex<Connection>` writer, and every multi-statement op runs in one explicit transaction. | API-006, TD-017 |
| Whether Turso can serve as a derived projection without becoming an authority. | Covered: Turso is the derived projection of the S3 object log and is rebuilt from it through projection control on the admin handle. A node with its Turso file replays only the log tail. | ADR-024, TD-010, TD-017 |
| How the storage model supports write units, batch and whole-group claims, per-claim leases, lease reclaim, and the ready-groups read. | Covered: every claim is a stage-bound index walk of O(log n + k + b); leases live in a claims table with one token per claim; reclaim frees expired claims within Δ; `stats.groups` reads a group-head index and a ready index. | API-006, ADR-025, TD-017 |
| At what domain claim order is guaranteed, and that placement is never client-visible. | Covered: claim order is `(rank, insertion sequence)` within one declared stage of one queue; whole groups come from `WholeGroups`, exclusive across stages; placement is never client-visible; the queue is the unit of placement. | API-006, ADR-008, ADR-025 |
| Whether the headline horizontal-scale claim is substantiated by a buildable, benchmarked mechanism. | Covered by per-queue placement across partitions and nodes on the object log. TP-002 E2/E3 must prove exact outcomes, claim order and progress (API-006 C4, C5a), fencing, recovery, and bounded shared resources under load. Absolute rates remain capacity observations for the declared topology. | product-vision, PRD, ADR-008, ADR-025, TD-018, TP-002 |
| Whether a single node supports the required queue density (at least 1000 concurrently active cold queues plus one hot queue) without per-queue resource blow-up or cross-queue degradation. | Covered by the bounded shared per-node resource rule: per-queue background work (lease reclaim, count maintenance, request-id GC) is multiplexed onto bounded shared pools, and many queues share one partition log, head document, and Turso file. TP-002 E2 requires the exact 1,001-queue shape, non-empty cold claims during sustained hot load, exact count reconciliation, and same-run controls. | PRD, TD-017, TD-018, TP-002 |
| How Seventh Sense's existing queue-like tables move onto fireweed while preserving tenant/account boundaries and operational visibility. | Covered by API-003, which maps each Seventh Sense queue onto the contract. The cutover needs the network transport (PRD P1-1), since Seventh Sense is written in Scala. | API-003, ADR-025 |
