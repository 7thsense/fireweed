---
ddx:
  id: concerns
  depends_on:
    - product-vision
    - prd
---

# Project Concerns

Project Concerns declare active cross-cutting context for Fireweed work.
They are not requirements, ADRs or test plans.

## Active Concerns

| Concern | Why active | Key practices |
|---------|------------|---------------|
| `durable-queue-semantics` | Fireweed's value is that claims, leases, retries and request replay are exact. | Token-fenced leases; recorded rejections; request ids refused past the horizon; versions never repeat (API-008). |
| `concurrency-model` | Many processes claim and change one space at once. | `READ COMMITTED` with row locks; `FOR UPDATE SKIP LOCKED` claims; guards lock what they count; deadlocks retried (API-008 §Transactions). |
| `performance-by-index` | Work must scale with the rows touched, not the size of a queue. | The index rule; no row caps but no table scans; sequential scans off in program transactions; tests count scans and index reads. |
| `no-hidden-state` | In-process stores and caches were the failure that ADR-027 removes (D41). | The executor keeps nothing between calls; each call reads what it needs from the database. |
| `security-owasp` | Programs come from consumer services and carry untrusted values. | Programs are typed data, never SQL text; values are always bound parameters; names are validated and quoted. |
| `multi-tenancy` | Each space belongs to one tenant. | Every call names a tenant and a space; a space has its own PostgreSQL schema. |
| `rust-cargo` | Reliability-sensitive library code. | Pinned Rust 1.99.0, `cargo fmt`, `clippy -D warnings`, `cargo deny`; no `unsafe`. |
| `testing` | Correctness depends on concurrent and time-driven cases. | The executor's tests run against PostgreSQL 18 with a fixed clock, several connections and index-use checks (API-008 §Conformance). |

## Concern Conflicts

| Conflict | Resolution |
|----------|------------|
| Flexible bulk changes vs. bounded work | Statement limits and stepped updates bound each statement; the statement timeout bounds the rest. |
| At-least-once delivery vs. consumer side effects | Fireweed guarantees one active lease per item; consumers keep effects idempotent with request ids. |
| Ad-hoc queries vs. the index rule | Selections need an index path; declared reads cover the UI's queries. |
