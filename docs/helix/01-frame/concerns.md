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

## Catalogue and applicability

Use [HELIX v0.15.4](https://github.com/DocumentDrivenDX/helix/releases/tag/v0.15.4).
The `.helix.yml` value `helix_version: 2` versions the marker schema, not the
plugin. Source-code applicability is handwritten Rust and Python. Library
construction remains caller-owned; service and execution-runner practices apply
only to their process boundaries.

## Active Concerns

| Concern | Source | Areas | Why Active | Key Practices |
|---------|--------|-------|------------|---------------|
| `modularity-and-encapsulation` | library; baseline-policy, HELIX v0.15.4 | all | Shared queue semantics and two durable adapters require protected ownership and substitutable storage. | Architecture owns the module/type map, integration owners, construction root and allowed/forbidden edges. Run the real dependency checker with allowed and forbidden controls; record exact legacy exceptions and remove them during cutover. Small files alone do not establish boundaries. |
| `configuration-design` | project-local; project owner | `area:core`, `area:api`, `area:infra`, `area:testing` | Limits, clocks, backend settings and credentials affect durability and comparability. | Construction Contract owns typed options, defaults, precedence, ranges, secret redaction and validation before I/O. Business modules do not read environment variables. Explicit configuration is authoritative; hosts adapt environment/CLI inputs once. |
| `twelve-factor` | library; HELIX v0.15.4 | `area:infra`, `area:api` | Deployed service and CLI processes need reproducible configuration, disposable workers and platform log capture. | Keep backing-service credentials outside source; hosts own process configuration and log transport. The library accepts caller-owned clocks and telemetry. Drain owned operations; process death does not turn ambiguous publication into rejection. |
| `formal-methods` | library; explicit project-owner adoption, HELIX v0.15.4 | all | Lease/fence, durable replay, atomic workflow state/new work and cancellation have material interacting-state risks. Owner: queue protocol maintainer. | Migration rigor is precise specification. Critical affected slices require executable bounded analysis before readiness: states, transitions, safety IDs, bounds/assumptions, positive witnesses, targeted negative controls and model/code/test correspondence. Unchanged properties retain only demonstrated evidence; incompatible old results are unqualified. Routine document/layout and straightforward sequential code do not require a solver. |
| `durable-priority-queue-semantics` | project-local | `area:core`, `area:api`, `area:data`, `area:infra`, `area:testing` | fireweed's core value depends on preserving priority, eligibility, lease, idempotency, progress-bound, and batch/group semantics across every downstream artifact. | Keep priority separate from eligibility; enforce single active lease; state at-least-once delivery; preserve idempotency horizons; make progress bounds observable and non-starving; test batch and group-aware claims under skew. |
| `enterprise-integration-patterns` | library | `area:core`, `area:api`, `area:infra`, `area:testing` | fireweed is itself an asynchronous work channel and may expose compatibility adapters shaped by known messaging patterns. | Treat delivery as at-least-once; design idempotent receivers; define dead-letter/invalid-message paths; carry correlation identifiers; keep broker/client mechanics behind explicit gateways. |
| `concurrency-model` | library | `area:core`, `area:data`, `area:infra`, `area:testing` | Claims, leases, batch operations, relaxed priority ordering, and storage suspension are concurrency-sensitive and must remain bounded under worker contention. | Name the concurrency model; prevent reactor stalls; preserve transaction affinity; bound in-flight work; avoid unbounded buffers; test cancellation, duplicate execution, and contention paths. |
| `resilience` | library | `area:core`, `area:api`, `area:infra`, `area:testing` | Queue users and fireweed internals depend on bounded calls, retry safety, determinate cancellation outcomes, lease recovery, overload handling, and steady-state resource limits. | Bound calls with timeouts; make interrupted commits resolvable; retry only idempotent operations with backoff and jitter; use bulkheads; define load-shedding/backpressure; cap accumulating resources. |
| `o11y-otel` | library; HELIX v0.15.4 | all | Library operation signals, deployed-service diagnosis and agent/CI run diagnostics must explain queue failure without leaking payloads. GenAI and frontend instrumentation are not required for this library. | Diagnostics Contract owns pinned OpenTelemetry mapping, event names/units, valid trace context, separate run IDs, one emission/ingestion route, pre-sink redaction, bounded records/queues/capture/retrieval, disclosed loss and quiet default consoles. Callers own exporters; runners own evidence files and rotation. Real receiver tests precede claims of OTel integration. Correlation IDs are not metric labels. |
| `api-style` | library | `area:api` | fireweed will need a native API and likely compatibility adapters, so contract shape, versioning, errors, and input validation must stay consistent. | Publish typed/versioned contracts; validate inputs at the boundary; use standard error shapes; keep wire contracts separate from the internal queue model; record non-default API style choices in ADRs. |
| `security-owasp` | library | `area:api`, `area:data`, `area:infra` | fireweed accepts untrusted payloads, metadata, API requests, and dependency inputs; a future service form must not leak secrets, internal errors, or cross-queue data. | Validate all external inputs; parameterize queries; audit dependencies; keep secrets out of source; enforce TLS for network surfaces; avoid implementation-detail leakage. |
| `auth` | library | `area:api`, `area:data`, `area:infra` | fireweed must be designable as a service where queue access is scoped by authenticated principals, not only by a single outer gate. | Model principals/accounts; derive scope from the authenticated principal; keep auth provider swappable; ensure protected operations are exercised end to end when a service surface exists. |
| `authorization-model` | library | `area:api` | Queue operations need a deny-by-default permission model for create, push, update, claim, finalize, inspect, repair, and purge. | Choose RBAC/ABAC/ReBAC deliberately; centralize policy decisions; authorize every state-changing and data-returning handler; test denied paths. |
| `multi-tenancy` | library | `area:api`, `area:data`, `area:infra`, `area:testing` | Queue namespaces should be able to carry tenant/account boundaries into storage and capacity control so one tenant cannot read, mutate, or starve another. | Choose and record an isolation model; enforce tenant scope at the data-access/storage boundary; test negative cross-tenant reads/writes; use quotas/rate limits for noisy-neighbor protection. |
| `deployment-topology` | library | `area:api`, `area:data`, `area:infra`, `area:testing` | fireweed is expected to run like Niflheim where possible: stateless service containers behind a load balancer, tenant/queue shards assigned to workers, and durable state externalized or persisted through simple storage primitives. | Keep compute nodes stateless or rebuildable; make shard ownership and reassignment explicit; avoid mandatory external coordinators or embedded consensus unless an ADR proves they are necessary; test failover, replay, and noisy-neighbor isolation. |
| `rust-cargo` | library | `area:core`, `area:api`, `area:data`, `area:infra`, `area:testing` | fireweed is a high-performance, bounded-memory, reliability-sensitive system; Rust gives memory safety, predictable resource control, and strong concurrency tooling. | Use a pinned stable Rust toolchain; organize as a Cargo workspace; deny unsafe by default; use explicit error handling; run fmt, clippy, cargo-deny, cargo-machete, tests, benchmarks, and loom for critical concurrency. |
| `testing` | library | `area:core`, `area:api`, `area:data`, `area:infra`, `area:testing` | Queue correctness depends on adversarial scenarios, not only happy-path unit tests. | Use multi-layer tests; prefer stubs over mocks; add property, fuzz, chaos, and performance ratchets; trace tests to acceptance criteria. |
| `verification` | library | `area:core`, `area:api`, `area:data`, `area:infra`, `area:testing` | A queue cannot be called correct until its real claim, lease, retry, recovery, and batch paths are observed under realistic load. | Record commands, exit status, environment, exercised flows, guard branches, and adversarial re-review before claiming completion. |

## Deferred Concerns

These concerns are intentionally not active yet:

| Concern | Why Deferred |
|---------|--------------|
| `admin-console` | The PRD does not include a v1 operator UI; a dashboard is P2. |
| `auth-local-sessions` | Auth is active as a product/design concern, but the provider filler is undecided. Select local sessions only if the service/control-plane design chooses it over an external IdP or machine-token model. |
| `relational-data-modeling` | Covered by the accepted PostgreSQL and Turso backend designs; no separate concern selection changes their identical queue semantics. |

## Project Overrides

| Concern | Practice | Override | Authority |
|---------|----------|----------|-----------|
| `auth` | Real signup/login/session surface | fireweed may first ship as a library or machine-facing service, so human signup/login is not assumed. Service mode uses provider-neutral principal resolution and operation-scoped authorization. | ADR-002 |
| `multi-tenancy` | Tenant identity derived from authenticated principal | Queue namespace and tenant/account identity may be distinct. The v1 model maps principal -> tenant -> queue -> shard, with tenant keys carried into storage. | ADR-002 |
| `deployment-topology` | Library-first composition with explicit host ownership | PostgreSQL and S3/Turso share API-007. Hosts own placement; storage publication authority is backend-local. No mandatory PostgreSQL control plane or consumer owner-session protocol. | Architecture, ADR-026 |
| `rust-cargo` | Latest stable Rust, Cargo workspace, and strict lint/tooling policy | Rust is selected with a pinned stable toolchain, Cargo workspace boundaries, Tokio service/backend runtime, strict linting, dependency checks, and unsafe denied by default. | ADR-003 |
| `verification` | Whole-stack evidence for buildable products | For docs-only and pre-implementation artifacts, evidence is document review plus `git diff --check`/placeholder checks. Implementation work must use running-system evidence or a recorded exception. | HELIX verification exception |

## Area Labels

This project uses the following area labels for concern scoping:

- `area:core` - queue engine semantics, priority ordering, eligibility,
  lifecycle, leases, batch/group claims, retry, and progress bounds
- `area:api` - native fireweed APIs, client SDK surfaces, and compatibility
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
| `enterprise-integration-patterns` at-least-once delivery vs. consumer side effects | fireweed guarantees at-least-once with a single active lease; downstream consumers must remain idempotent. |
| `resilience` retry guidance vs. queue retry semantics | Retries must be capped and observable. Queue retry policy governs item redelivery; synchronous client retries must still use idempotency keys and backoff. |
| `o11y-otel` instrumentation vs. performance targets | Instrumentation is required, and load tests must measure overhead with telemetry enabled. |
| `api-style` compatibility adapters vs. fireweed native model | Compatibility APIs may mirror SQS or other systems, but they cannot force the core model to drop priority updates, progress bounds, or group-aware claims. |
| `security-owasp` input validation vs. opaque payloads | Payloads may be opaque to fireweed, but queue-owned fields, metadata predicates, priority values, and API envelopes must be validated at the boundary. |
| `auth`/`multi-tenancy` vs. general open-source library use | Core semantics should not require a hosted auth system, but all service and storage designs must preserve principal-scoped queue access and tenant/account isolation boundaries. |
| `multi-tenancy` vs. storage flexibility | Tenant-aware storage isolation is a design goal, but the concern does not pick a storage engine; storage design must compare isolation, noisy-neighbor control, and operational cost. |
| `deployment-topology` stateless containers vs. local SQLite/WAL candidate | Container-local state may only be hot shard state or a recoverable cache unless the storage spike proves the durability boundary. Ack semantics, replay, compaction, and object-store persistence must be explicit. |
| `deployment-topology` shard assignment vs. no mandatory coordinator | Shard placement must avoid a required ZooKeeper/etcd-style dependency and avoid embedding a cluster consensus algorithm in fireweed. If single-writer shard ownership needs coordination, prefer storage-backed leases, deterministic assignment, or other simple mechanisms that can be validated under failure. |
| `deployment-topology` vs. `multi-tenancy` | Shard placement and load balancing must preserve tenant/queue isolation and noisy-neighbor controls; generic load balancing cannot bypass shard ownership. |
| `rust-cargo` vs. performance shortcuts | Bounded memory and reliability take precedence over unsafe shortcuts. Any unsafe code or lock-free structure requires explicit design rationale, safety invariants, and concurrency tests. |

## Required Design Coverage

Concern adoption defines requirements, not completed qualification. The Architecture and current Contracts own these decisions:

| Question | Decision and remaining gate | Governing artifact |
| --- | --- | --- |
| One simple consumer boundary? | API-007 atomic claim-group commit includes state/audit/new work. PostgreSQL public path exists; full S3/Turso construction and one Snorri adapter cutover remain pending. | API-007, Snorri contract, current refactor plan |
| Native eligibility and priority? | Backend filtering precedes selection, with full typed names/keys and collision separation. Wide selectors, whole-claim replay, current renewal and numeric progress require both public suites. | API-007, TP-004 |
| Module ownership? | Pure shared policy, bounded spool, independent native adapters and facade-only composition; explicit legacy exceptions and real manifest/compiler rejection controls. | Architecture, TD-001 |
| Configuration ownership? | Caller-resolved typed configuration, checked defaults/ranges before I/O, explicit resource caps, independent operational clock and redacted secret handling. Several enforcement/capacity surfaces remain implementation gates. | Construction contract |
| Telemetry and logs? | Caller-owned export, one route, structured correlation, bounded records/capture/retrieval, redaction/loss disclosure and real receiver tests. OTel pins are explicit; an SDK bridge is not presumed implemented. | Diagnostics contract |
| Critical protocol assurance? | Backend-neutral abstract history with PostgreSQL transaction and S3 log refinements; positive witnesses, rejecting controls, bounds and code correspondence. Old stale-read/reused-identity models are superseded for API-007. | CONTRACT-006/007, TP-006 |
| Reliability and scale? | Durable ACK, fresh successful observations, original-genesis recovery and exact counts are mandatory. Full manifest memory/rewrite bounds, live provider faults, settled 10M scale and 1,000 cold plus hot density are outstanding qualification gates. | PRD, TP-002/004/005 |
| Application migration? | Preserve arbitrary JSON/pins, account/connector/job/campaign filters, pause/suppression/preview, per-item outcomes, visibility and state-version rules in one adapter. Actual consumer acceptance precedes cutover. | Snorri contract, TP-004 |
