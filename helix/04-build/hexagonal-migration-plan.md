# fireweed Re-Architecture — Comprehensive Implementation Plan (v4, master)

Status: DRAFT, post-review-round-2. **Pre-launch clean cutover.** Single authoritative plan;
consolidates ADR-007 (architecture), TD-006 (RESP surface — refolded), TD-007 (durability +
reclaim + upsert — to author in Phase 0).

## 0. Goal & non-negotiables

- **One engine.** CQRS: priority-ordered projection (speed) over a log store (cost durability).
- **Two interfaces, asymmetric by design.** RESP (fireweed-flavored Redis; limited but contract-faithful)
  + Rust library (full power). The library is strictly more capable; recorded, not accidental (§3).
- **Hexagonal, modular encapsulation.** Domain defines ports; adapters depend inward; enforced by a
  dependency-direction test.
- **Clean cutover at ALL touchpoints.** No stubs, no legacy fallbacks, no compatibility shims. Done =
  `fireweed-service`/`-client`/`-kafka` deleted, docs superseded, beads re-scoped.
- **Launch scope = single-shard.** Multi-shard *coordination* (owner assignment + loop + cross-shard
  guards) is **post-launch**, recorded as an intentional subset (§2.5). The ports admit it (shard_id
  in keys, `ControlPlaneStore` for assignments); the launch build implements one shard completely —
  *not* a multi-shard stub.
- **Verified completeness.** Every phase: implement → review → test. Phase 7 reviews the finished
  system against this plan, item by item.

## 1. Target architecture

| Crate | Role | Outward deps | I/O |
|---|---|---|---|
| `fireweed-core` | Domain — types & rules | — | none |
| `fireweed-engine` | Domain — execution, ports, migrated service logic, **ReclaimDriver** (§2.4) | core | none |
| `fireweed-memory` | Driven adapter — InMemory log+projection (reference) | engine, core | none |
| `fireweed-sqlite` | Driven adapter — sqlite log+projection+control-plane | engine, core | rusqlite |
| `fireweed-postgres` | Driven adapter — postgres, atomic claim via `ClaimPort` | engine, core | tokio-postgres |
| `fireweed-objectlog` | Driven adapter — object-log (eventual-apply class) | engine, core | S3 |
| `fireweed-resp` | Driving adapter — RESP server (§3) | engine | tokio net |
| `fireweed` | Driving adapter — Rust library facade | engine + adapters | none |
| `fireweed-server` | Composition root — bin; DI; runs the ReclaimDriver task; ops probe (§ M2) | all | net |
| ~~service / client / kafka~~ | **DELETE** | — | — |

Dependency-direction test asserts no engine/core → adapter edge. The CLI is a **library consumer**,
not a third interface.

## 2. Engine model

**2.1 Ports (`fireweed-engine`):** `LogWriter`/`LogRead`, `ProjectionWriter`/`ProjectionRead`
(`select_eligible` priority order, `peek`, `pending`, `metrics`), `Backend` (atomic `write(|log,proj|)`
UoW), **`ClaimPort`** (backend may claim atomically), **`UpsertPort`** (`replace_if_pending`, §3 Inv 2 —
runs in the *same unit of work / item lock* as claim), `ControlPlaneStore` (queue defs + epoch source),
`SnapshotStore`, **`ReclaimDriver`** (§2.4), `Clock`, `IdGen`. None silently dropped. **Every port
method has a conformance test that fails if the impl returns a default/no-op** (§6).

**2.2 Two-class durability (TD-007):**
- **Atomic** (memory lock / sqlite / postgres one txn): append+apply commit together; post-commit
  projection globally consistent.
- **Eventual-apply** (objectlog): ack after log commit, apply within a bounded window; guarantee is
  self-read-after-write only. Priority order is "over applied state, eventual". It exposes the same
  inherent mutation surface as the atomic class; deterministic re-validation and the response barrier
  close upsert/update/reschedule races without making storage choice visible to callers.

**2.3 Single *logical* claim path.** Engine is the single logical claim authority; a backend MAY
implement claim atomically behind `ClaimPort` (postgres keeps `FOR UPDATE SKIP LOCKED`). Upsert and
claim **mutually exclude** on the same item (same lock / row) on the atomic class.

**2.4 ReclaimDriver (new component — closes the orphaning gap).** Redis evaluates lease idle-time
lazily inside `XCLAIM`/`XAUTOCLAIM`, so a quiet stream needs no timer. fireweed models reclaim,
cohort-`completion_bound_ms`, `not_before`/recurrence promotion, and `progress_bound_ms` enforcement
as **state transitions that something must fire** — otherwise an item on a queue with no claim traffic
orphans forever. The `ReclaimDriver` is engine-owned policy driven by the composition root:
`fireweed-server` (and an async library embedding) spawns a periodic task; a **synchronous** library
embedding with no async runtime drives it deterministically via an explicit `engine.tick(now)` entry
point. The reclaim *logic* is domain; the *clock* is the composition root's. DoD: an item is
reclaimed/expired with **zero** intervening client commands on its queue (§6).

**2.5 Sharding.** Launch = single shard; `command_position`, idempotency, and `client_item_key`
uniqueness are naturally shard-local. Multi-shard owner-assignment + coordination loop + cross-shard
guards are **post-launch** (recorded subset; ports already carry `shard_id`).

## 3. RESP surface — "fireweed-flavored Redis" (semantic-contract fidelity)

**Bar:** a stock Redis client gets correct, non-surprising results — the *semantic contract* of each
command holds even where fireweed's flavor differs. Two implementation invariants:

- **Invariant 1 — per-item delivery tracking, no single `last-delivered-id` cursor.** `XREADGROUP >`
  returns highest-priority *undelivered* items, tracked per item; never orphans a low-priority small-id
  item; transactional advancement (claimed → PEL). **On the atomic class this is strict; on
  eventual-apply it is "priority over applied state, eventual."**
- **Invariant 2 — upsert = atomic `XDEL old` + `XADD new`, pending-only, every storage class.** Re-`XADD`
  colliding with a **pending** item (via `UpsertPort`, same UoW as claim) returns a new monotonic id;
  old id reads deleted; `XLEN` nets unchanged. Collision with **claimed/terminal** → reject. Absent
  `client_item_key` ⇒ always append. Eventual-apply profiles use TD-004's deterministic re-validation
  and response barrier. A later `XACK`/`XCLAIM` of a **superseded** old id returns
  **`-ERR fireweed superseded`** (never a silent `nil` — preserves at-least-once "no silent drop").

**Stock surface (faithful per contract):**
- `XADD` — upsert-on-key (Inv 2).
- `XREADGROUP >` — priority-ordered *delivery* (Inv 1); cursorless.
- `XACK` — complete; **operator-fenced lease → `-ERR fireweed stale_lease`** (NOT `0` — a `0` would read
  as success and defeat the fence).
- `XCLAIM`/`XAUTOCLAIM` — **reclaim is entry-id-ordered (cursor-faithful)**; priority governs delivery,
  not reclaim. **Same-consumer `XCLAIM`/`XCLAIM JUSTID` = renew and charges no attempt; cross-consumer
  = reclaim and charges one** (lets a stock worker renew safely without a Fireweed-specific command). The
  `ReclaimDriver` (§2.4) handles timed reclaim independently, so quiet queues don't depend on a client
  running `XAUTOCLAIM`.
- `XPENDING`/`XLEN`/`XINFO`/`XDEL` — faithful.

**Documented flavor differences (visible, explainable):**
1. `XINFO GROUPS last-delivered-id` is not a meaningful high-water mark (priority ≠ id order).
2. `XAUTOCLAIM` reclaims in **entry-id order, not priority order** (it is cursor-based). Client-driven
   reclaim is FIFO-over-PEL; priority reclaim is the `ReclaimDriver`'s job. Across a completed PEL
   sweep every entry is covered.
3. Low-priority starvation is possible, **bounded by `progress_bound_ms`** (enforced by ReclaimDriver).
4. At-least-once delivery (crash → reclaim); consumer-side idempotency is the app's job, as on Redis.
5. On **eventual-apply** backends, priority order and no-double-claim are "over applied state, eventual";
   the operation surface, including upsert, is unchanged.
6. `XREADGROUP` replies carry fireweed reserved fields (`item_version`, `lease_expires_at`, …) as extra
   entry fields — benign; stock clients ignore unknown fields.
7. Same-consumer `XCLAIM` is a no-charge **renew** (Redis would bump the delivery count); strictly more
   forgiving — a client relying on self-`XCLAIM` to advance retry count for poison detection sees it
   not advance. Use the library for explicit attempt control.

Canonical error replies (asserted verbatim by e2e/conformance): `-ERR fireweed stale_lease`,
`-ERR fireweed superseded`, `-ERR fireweed unavailable`, `-ERR fireweed terminal`, `-ERR fireweed invalid`.

**RESP capability = {RESP-stock, library}.** No required Fireweed-specific commands. Filtered claims, gates, cohorts,
rich finalize dispositions, mutable-priority, create/config, scopes, operator/inspect are
**library-only — explicitly marked** in the capability matrix (§6). "No *silent* library-only cells";
marked ones are intentional. An optional custom command for atomic rich finalize is a post-launch decision.

**e2e (off-the-shelf Redis client — pinned `redis-py` + one of `go-redis`/`redis-rs`):**
- **Inv 1 — drain-and-reconcile:** produce N mixed-priority, drain via `XREADGROUP >` to empty,
  assert delivered-set == produced-set, each once, no hang (proves no orphaning *through the command
  surface*).
- **Inv 2 — effects + collision:** re-`XADD` pending key → new id, old id `XRANGE`→nil, `XLEN`
  unchanged; re-`XADD` claimed key → rejected; `XACK` superseded id → `-ERR fireweed superseded`.
  (Atomicity itself is proven by **engine-level** tests — the stock client cannot observe it.)
- **Cursor:** `XAUTOCLAIM 0-0`→…→`0-0` pagination loop terminates and covers the whole PEL.
- **Race:** upsert concurrent with claim on the same key (engine-level + a best-effort e2e).
- **Crash recovery:** kill a consumer mid-PEL, reclaim via `XAUTOCLAIM`, no lost/double work.
- **Fence:** operator stale → staled worker's `XACK` returns `-ERR fireweed stale_lease`.
- **Intra-group exclusion:** two consumers, concurrent `XREADGROUP >`, never the same item.

## 4. Retired-surface teardown — ALL touchpoints (no stubs/fallbacks)

**4a. Domain logic → `fireweed-engine`, durable (closed inventory):** AuthContext + authorize_*;
request-id idempotency + operator replay→409; **operator-operation store + get/cancel/list**; lease
fencing (+un-fence+compaction); pause/resume (+un-pause); **`command_position`**; **QueueCatalog**
(capabilities, metrics, active-scopes+roll-up); claim/finalize/rearm/purge validation; lease-token
hashing. Transport (ApiProblem, axum routes/router, runtime HTTP/health) deletes with REST.

**4b. Crates:** keep core; split storage→engine+memory then dissolve; refactor sqlite/postgres/
objectlog; delete service/client/kafka.

**4c. Docs:** SUPERSEDE ADR-005. REWRITE API-001 (neutral + RESP binding), TP-001. ADD ADR-007;
KEEP TD-006 aligned with §3 — it records the launch `{RESP-stock, library}` matrix, excludes required
Fireweed-specific commands, specifies `-ERR fireweed stale_lease`, and documents the `XAUTOCLAIM` cursor caveat.
AUTHOR **TD-007** (two-class durability, ReclaimDriver, UpsertPort,
durable-state schema/retention/compaction/replay). KEEP+update ADR-001/2/3/4/6, TD-001/2/3/4/5,
TP-002/3.

**4d. Tests:** delete ~3 kafka + ~20 HTTP-route; **re-home** service invariant tests to the engine;
migrate ~56 to conformance. (Test churn is ~20k LOC — see § scope.)

**4e. Beads:** re-scope (claimed-item shape → transport-neutral; Lakebase deploy → `fireweed-server`
image + health probe); none halted.

## 5. Implementation phases (each: implement → review → test; nothing stubbed)

- **Phase 0 — gating docs (own review gate).** ADR-007; validate TD-006 against the launch
  `{RESP-stock, library}` matrix; author TD-007 (durability classes, **ReclaimDriver**,
  **UpsertPort**, cross-class upsert race closure, superseded reply, cross-shard deferred). Converge all
  three before any code. Resolve whether an optional custom finalize command is needed.
- **Phase 1 — ports + reference engine + early RESP smoke.** Define ports; extract `fireweed-memory`;
  implement priority claim/lease/ack + Invariants 1 & 2 + ReclaimDriver over memory; conformance green.
  Stand up a throwaway in-memory RESP front and run the §3 **drain-and-reconcile** stock-client e2e
  against memory — validates the semantic model *before* backend work.
- **Phase 2 — migrate domain logic (move-and-delete, test-first).** Drop `fireweed-service` from
  `default-members`. For each 4a unit: write the engine test, move the logic durable, **delete the
  service code path in the same step** (the service crate shrinks to nothing by phase end — no
  delegation, no shim).
- **Phase 3 — driven adapters.** sqlite, postgres (`ClaimPort`), objectlog (eventual-apply with the same
  inherent operations). Conformance green on each — incl. concurrent-claim races, intra-group exclusion, and each
  durability class's *declared* guarantee (strong on atomic; weaker on eventual-apply).
- **Phase 4 — full RESP adapter + e2e.** `fireweed-resp`; the complete §3 e2e suite (all backends,
  cursor loop, crash recovery, fence, race) is the headline acceptance gate.
- **Phase 5 — library + composition root.** `fireweed` facade + `fireweed-server` (DI, ReclaimDriver task,
  ops probe).
- **Phase 6 — delete legacy.** Remove service/client/kafka + tests; dissolve storage; supersede/rewrite
  docs; re-scope beads.
- **Phase 7 — final gap review against this plan.** Every §1–§6 item → {implemented+tested | descoped
  with reason}; written reconciliation report.

## 6. Definition of Done (gates that actually catch half-done work)

- `rg` finds **zero** refs to service/client/kafka, `NativeRoute`, `axum`, `/v1`, problem+json.
- **No-stub = behavioral, not grep.** The single conformance suite runs **every adapter × every port
  method × every RESP command**, and **every port method has ≥1 test that fails if it returns a
  default/no-op** (a no-op `route_stub`-style impl must fail a real assertion). Grep for
  `todo!/unimplemented!/// TODO/// legacy` is a *secondary* check, not the proof.
- **Capability matrix {RESP-stock, library}** signed: every API-001/API-002 op marked
  RESP-stock-pass / library-only-intentional / n-a. No *unmarked* library-only cells.
- Every migrated invariant (auth, idempotency, operator-op model, fencing, pause, recurrence,
  cohort/group, purge, `command_position` monotonicity) has an engine-level test.
- **ReclaimDriver test:** item reclaimed/expired with zero intervening client commands on its queue.
- e2e RESP suite green with pinned off-the-shelf client(s): drain-reconcile, cursor loop, crash
  recovery, fence=`-ERR fireweed stale_lease`, upsert effects+collision+superseded, intra-group exclusion.
- One conformance suite green on memory+sqlite+postgres+objectlog; eventual-apply asserts its distinct
  visibility/durability guarantee while retaining the same inherent operation surface. TD-004 proves
  deterministic apply-time re-validation and the response barrier for mutable-write races.
- Two driving adapters + one composition root; **dependency-direction test passes**.
- Durable-state debt closed (idempotency/fences/pause/`command_position` reconstructable from the log).
- Docs consistent; ADR-007/TD-006(refolded)/TD-007 recorded; capability asymmetry recorded.
- **Single-shard launch scope recorded**; multi-shard coordination listed as post-launch subset.
- **Phase 7 reconciliation report** shows no dropped item.

## Scope (honest)
Source ≈17.3k LOC + tests ≈19.7k LOC ≈ **37k LOC touched**. Net-new: `fireweed-resp` protocol server,
`ReclaimDriver`, `UpsertPort`, durable-state re-architecture. The likely stall points are the
durable-state design (Phase 0/TD-007) and re-homing the seventh-sense/invariant-stress test suites
onto the engine API (non-mechanical). Health/readiness probe transport on `fireweed-server` is a small
non-`axum` endpoint (decide in Phase 5), distinct from the two client interfaces.
