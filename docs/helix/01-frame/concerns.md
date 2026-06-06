---
ddx:
  id: concerns
  depends_on:
    - product-vision
    - prd
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
| `testing` | library | `area:core`, `area:api`, `area:data`, `area:infra`, `area:testing` | Queue correctness depends on adversarial scenarios, not only happy-path unit tests. | Use multi-layer tests; prefer stubs over mocks; add property, fuzz, chaos, and performance ratchets; trace tests to acceptance criteria. |
| `verification` | library | `area:core`, `area:api`, `area:data`, `area:infra`, `area:testing` | A queue cannot be called correct until its real claim, lease, retry, recovery, and batch paths are observed under realistic load. | Record commands, exit status, environment, exercised flows, guard branches, and adversarial re-review before claiming completion. |

## Deferred Concerns

These concerns are intentionally not active yet:

| Concern | Why Deferred |
|---------|--------------|
| `admin-console` | The PRD does not include a v1 operator UI; a dashboard is P2. |
| `auth`, `auth-local-sessions`, `authorization-model` | Per-user authentication and authorization are not v1 product scope. Revisit when a network service or hosted control plane is designed. |
| `multi-tenancy` | pqueue requires queue namespace isolation, but not yet a SaaS tenant/principal model. Use the project-local queue semantics concern for now. |
| `relational-data-modeling` | Storage is intentionally undecided until technical design. Select this only if a relational store becomes a design decision. |
| Language/runtime concerns | Implementation stack is intentionally undecided until technical design. |
| `deployment-topology` | Deployment shape is intentionally undecided until technical design. |

## Project Overrides

| Concern | Practice | Override | Authority |
|---------|----------|----------|-----------|
| `security-owasp` | Authentication and authorization checked on protected endpoints | No auth model is selected for v1 core/library work. If a hosted service or control plane is introduced, auth and authorization concerns must be selected before design completes. | PRD Non-Goals; Needs ADR for hosted/service deployment |
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
