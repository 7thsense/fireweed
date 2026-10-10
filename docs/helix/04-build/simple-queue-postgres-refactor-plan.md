---
ddx:
  id: build-simple-queue-postgres-refactor
  depends_on:
    - api-queue-claim-commit-contract
    - adr-postgres-baseline-queue-contract
  status: accepted
---

# Simple queue refactor and PostgreSQL baseline plan

## Authorization and review protocol

Owner requests a complete modularity refactor, an identical PostgreSQL baseline,
matched benchmarks/tests, learning from and discarding the API-006 branch, and
Astra Ultra plan review before implementation plus review of every iteration
before committing and pushing main. No squash/rebase or audit-history rewrites.
Review is gpt-6-astra at ultra. Resolve findings and rerun review before landing.
Never commit user edits. Fetch/check current main before each fast-forward push;
merge divergence without rewriting history. No deployment or live email sending.

## Target and learning boundary

API-007 is the target: insert, filtered ordered claim, atomic commit group and
stats, with typed state read/query/receipt access. Atomic commit carries claimed
updates, CAS state/audit and emitted work; no exposed transaction session. One
Snorri adapter, mandatory equal behavior, PostgreSQL SQL baseline and S3/Turso.
Existing behavior remains available until cutover passes; no hybrid public APIs
remain after completion. Do not remove current reliability regression coverage.

Preserve API-006 lessons as a dated evidence inventory: projected fields avoid
loading unneeded large values; indexed point lookup avoids key IN-list cliffs;
lean claim rows separate selection from large-value hydration; explicit canonical
limits; separate admin authority; mandatory black-box conformance and lost-response
fault hooks. Measurements are engine/source-specific, not inherited benchmarks.
Do not adopt stages/WholeGroups, weaken Snorri state-version rules, force 64 KiB
state, or replace numeric progress requirements with quiescent-only liveness.
Preserve originals as an immutable Git archive tag and evidence docs, then delete
only the API-006 rescue WIP branch after verifying archive reachability and extracting
useful evidence. Other rescue/tooling branches and main remain untouched.

## Modules and dependency law

- fireweed-core: shared public DTOs, schema/filter/order/limit validation, canonical
  fingerprints and receipt/error vocabulary; no database or object-store dependency.
- fireweed-engine: backend-neutral orchestration and atomic-group semantic planning;
  storage port covers entire groups rather than exposing low-level transaction handles.
  Backend-specific atomic execution is behind one internal async boundary.
- fireweed-postgres: native async SQL transactions, row-level selection/locking,
  SQL state/work/receipt persistence and restart; no S3/objectlog/Turso runtime requirement.
- fireweed-turso plus objectlog composition: same commands and outcomes, durable log
  authority, committed visibility, apply/rebuild; ownership clocks remain independent.
- fireweed facade: thin construction/delegation, one concrete consumer handle; explicit
  PostgreSQL and S3/Turso constructors, no capability-based ordinary-operation fallback.
- fireweed-conformance: one public-handle suite, shared workload generator and assertions;
  adapter-specific fixtures/fault hooks only. Query semantics and CAS checks shared,
  SQL generation/storage scheduling necessarily adapter-specific.
- Snorri adapter: maps transition entries to commit groups and reads to typed state;
  business validation/state-version/digests remain in Snorri. Host owns maintenance.

## Ordered iterations and exit gates

1. Documentation/evidence: review API-007/ADR-026 and this whole plan; extract lessons,
   pin archived branch, mark API-006 superseded. Validate graph/link semantics. No code
   implementation before Astra Ultra approves the plan. Land docs after review.
2. Contract binding: concrete Rust DTOs/validation/limits and receipt/error semantics;
   compile consumer examples and tests for fingerprints, independent group outcomes,
   rebatching including durable rejected outcomes, full claim fingerprint/replay
   (empty, partial settle, renewal, restart, cleanup), ABA-safe versions, ordered overlapping-record
   groups and size bounds. Preserve Snorri vectorized_resplit_replay_check and
   same_instance_siblings_use_independent_candidate_projections unchanged. Measure current Snorri instance
   serialization before fixing limits. Support its 1 MiB output/512 effects/512 awaits
   without hidden weakening. Bind query/filter work-kind/schedule/keys/run/version fields.
3. Modular ports/facade: remove monolithic LibBackend requirement from new surface;
   isolate shared planning from stores, build public conformance factory and dependency
   guards. Retain compatibility internally until migration. Native async SQL boundary.
4. PostgreSQL baseline: implement indexed filtered priority claims, durable leases/CAS,
   atomic state+new-work commit groups, per-group durable receipts, typed reads/stats/pages.
   Real DB lane including multi-process contention, lost responses, process-kill reopen,
   clock separation, limits and cancellation. No mocks or missing-fixture pass.
5. S3/Turso parity: implement identical interface, log-first per-group receipt ownership,
   known-rejection/unknown distinction and fresh successful observations. Paused-applier,
   ownership handoff, corruption, poison, shutdown and exact rebuild tests; preserve stock
   Turso schema/transaction exclusion and formal/model replay gates. Do not introduce
   snapshots/truncation or unrelated partition redesign to complete this iteration.
   The necessary versioned collision-free address encoding and checked old-address
   migration are required by the actual consumer integration binding.
6. Snorri migration: single adapter against both constructors. Remove claim-all/filter,
   side-record consumer API, capability downgrade and client projection polling. Preserve
   atomic per-transition independent replay and state-version behavior including await
   consumption. Convert bootstrap/migration/recovery to claimless CAS commit groups,
   typed state queries and durable receipt resolution. Move maintenance into host-owned
   component without weakening lifecycle ownership. Run Snorri full shared conformance,
   native durable timers/effects/awaits, recovery/audit and process restart on both stores.
7. Benchmarks: same generator/seed/source/host/topology/concurrency, item/body/state sizes,
   eligible/filter distributions and identical settlement correctness. Record insert,
   filtered claim, commit with state+fanout, stats/query, full lifecycle, 10M resident hot
   queue and 1000 cold+hot density. Record p50/p95/p99, useful settled rate, drain/apply
   debt, RSS, storage and S3 requests/cost. Separate warmup/setup/recovery; PostgreSQL is
   reference, no cheaper durability on either side. Failed correctness disqualifies rates.
   Report improvement or failure honestly; no portable 10k/12.5k bar invented.
8. Persisted-data cutover/cleanup: gate old main fixture reopen, versioned crash-safe
   migration/backfill, old and new command genesis rebuild, stable namespace/keys,
   lease authority and receipt preservation before enabling writes or removing readers; remove superseded public surfaces and dead fallback paths,
   regenerate docs/examples/public API/dependency guards, full workspace fmt/clippy/test,
   Snorri/Cayce compile/conformance, formal replay, docs/site gates when affected. Record
   exact commits and limitations. Baseline and S3/Turso both mandatory in CI correctness;
   controlled performance lane separate from hosted CI. No obsolete accepted docs left
   that contradict constructors, freshness, leases, replay or state storage.

## Mandatory common tests

API-007 section 8 is normative. Test insert identity, exact filtered ordered selection,
concurrent disjoint leases, expiry/renewal, atomic state+work update, rejection no-effects,
independent group replay across rebatching, freshness with delayed apply, process loss,
state nonclaimability, await race/state-version, audit corruption, cancellation/shutdown,
stats/query parity, envelope boundaries and durable retention. Include active overload
progress obligations independently of quiescent tests: shared skew/near-bound
scenario verifies progress-guard precedence within filter, eligibility/age anchors
and stats age, preserving the existing queue-global numeric bound. Benchmark correctness uses the
same reference oracle; never compare databases by backend-private fast paths.

## Actual consumer integration gate

The [consumer integration binding](simple-queue-consumer-integration-binding.md)
closes actual-source gaps before cutover: indexed additional attributes, one
server-resolved Addressed/SelectFirst administrative intent within commit groups,
compact exact outcomes, read-only preview without identity retention, one-snapshot
bucket stats and collision-free object-log address migration. Its reviewed sequence
extends this plan; the current native adapter is not yet full Snorri conformance.
No fixed-field restriction, client-side selector scan, 4096-match truncation or
per-item group translation may substitute for the original whole-operation result.

## Open bindings to settle during reviewed iteration 2

Concrete state/request/body bounds from actual serialization, receipt retention cleanup
representation and capacity, fair priority/progress implementation, query declaration
shape, and clock trait adapters. These are bounded binding decisions, not permission to
change user-visible guarantees or delay baseline implementation indefinitely.
