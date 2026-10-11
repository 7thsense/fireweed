---
ddx:
  id: tp-fireweed-facade-and-snorri-acceptance
  depends_on: [architecture, api-queue-claim-commit-contract, build-simple-queue-consumer-integration-binding, contract-construction-config, contract-diagnostics, contract-durable-queue-records, contract-formal-queue-semantics, contract-model-trace-replay]
  status: accepted
---

# TP-004: Queue facade and Snorri acceptance

## Contract and ownership

API-007, ADR-026, Architecture and the Snorri integration contract are the authorities. Run one mandatory public-path suite on PostgreSQL and S3/object-log with stock Turso 0.8.2. No backend-dependent skips, Unsupported substitutes, weaker freshness, private storage-port bypasses or consumer-owned transactions qualify. Legacy facade tests remain regression tests until their callers are migrated; they do not qualify API-007.

## Common acceptance

- Exercise every QueueStore operation through the concrete facade, including definition creation/open, insert, filtered claim, atomic commit, state reads/queries, admin preview/application and statistics.
- Native filtered priority claims cover declared/additional typed fields, missing values, full identifiers and deliberate digest collisions, wide IN sets and 5,001-field conjunctions. No whole-queue claim followed by client filtering. Compare exact ordering, scheduled eligibility, lease expiry and numeric progress against the common oracle.
- Commit settles claimed work, CAS-protected first-class workflow/audit state and new work atomically. One failed precondition leaves the entire group unchanged. Mixed per-item business outcomes are returned within a successful atomic commit.
- Exact request replay preserves identities and insertion order. Changed payloads conflict; expired requests cannot be reused. Changed grouping cannot replay an earlier group.
- Replay of a claim requires every member to remain owned. One expired, deleted or settled member expires the whole claim. Renewal returns current versions and expiry while retaining stable tokens; creation versions are not ownership predicates.
- Test Preserve/Replace/Edit independently from Complete/Selected field knowledge. Omitted bodies/fields are not deletions. The complete JsonEqualityEncoding namespace/schema pin is checked before both reads and writes in NumericVariants and LiteralRepresentations modes, including feature/model drift rejection. Arbitrary JSON and RFC6901 paths round-trip exactly.
- Successful reads and claims cover previously acknowledged mutations. Persisted original genesis, full queue address and epoch/head/sequence checks prevent stale or foreign recovery.
- Statistics return exact scoped counts, 50 finite histogram bins, a coherent as_of timestamp and correct floor division before the epoch. State-query pagination and admin preview are stable and bounded; preview creates no receipt or mutation.
- Exercise exact limits, huge identifiers/values, overflow, admission cancellation, whole-worker drain, poisoned connections and malformed durable bytes. Known TooLarge precedes publication; ambiguity after possible publication never masquerades as rejection.

## Consumer acceptance

Use one Snorri adapter and concrete queue facade for both constructors. Compile every supported Snorri feature combination against the same Fireweed revision and run actual dispatch, transition, await consumption, timeout, retry, pause/suppress and next-step scenarios. Workflow state_version advances on transitions and await consumption; bookkeeping alone does not advance it. Filter account/connector/job/campaign work natively, preserve compatible batches and downstream business limits, and keep business pacing/quota outside the queue kernel. No auxiliary side-record or session interface is permitted.

## Backend and fault acceptance

PostgreSQL uses a run-owned database with version, fsync/WAL settings and isolation recorded. Run lost-reply, process-death, server-restart and reopen checks on the public path. S3 qualification uses an attested live provider with durable conditional publication, consistent authority reads and full-address isolation; local log tests are supporting coverage only. Run stale-owner fencing, concurrent publication, missing/corrupt original genesis, manifest uncertainty, poisoned projection, delete/rebuild, interrupted replay and provider/process failures. Stock Turso transactions roll back or poison before worker resources are released.

A missing provider fails qualification before the suite; it is never a passing skip. Use synthetic data and run-owned namespaces. Record non-secret provider/server/toolchain versions, source revisions, workload identities and resource limits in target-owned run artifacts.

## Architecture, configuration and diagnostics

Run python3 scripts/ci/check_queue_boundaries.py --controls, including forbidden dependency, conditional/renamed dependency, cycle and compiler privacy controls. Invalid configuration fails before I/O; verify exact defaults/ranges/precedence, injected operational clock, redacted Debug/errors and independent infrastructure deadlines. Submit traced and untraced diagnostic events to a real declared receiver; assert correlation, redaction, bounded cardinality, routing, size/loss/retention and flush behaviour from the diagnostics contract. Receiver/bridge versions are explicit run pins.

## Formal correspondence and release gates

CONTRACT-006/007 and TP-006 govern revised bounded models, assumptions, positive witnesses, negative controls and implementation correspondence. Old models that permit stale empty reads or expired identity reuse are not evidence for this contract. Formal analysis does not replace real fault/conformance tests or numeric performance qualification.

All common tests and provider faults must pass before TP-005 timing qualification. No checkpoint, debug microbenchmark or legacy suite is a complete backend qualification.
