# Engine-Enforced Coordination + Encapsulated Library Surface — design convergence (pre-ADR-009)

Status: **CONVERGED** (review round 2 — two independent grounded reviewers returned CONVERGED, no
blocking issues; cosmetic nits folded in). Ready to mint ADR-009.

Governing prior art: **ADR-007** (hexagonal, one engine / two interfaces), **ADR-008** (queue as shard
unit, pluggable control plane), **ADR-002** (auth, tenancy, deny-by-default), **TD-003** (sharding &
ownership), **TD-006** (RESP wire adapter / `route`), **TD-007** (durability, reclaim, durable state).
This doc does **not** re-decide the ownership model TD-003/ADR-008 specify; it decides (a) **where that
model is enforced**, (b) that the **published library is the only external surface** to it, and (c)
closes one documented correctness gap (fence threading). It **refines, and does not supersede, ADR-007.**

> **v2 changelog (review round 1):** moved the coordination locus from "the library" to "the engine,
> below the ports" (R1.B1); replaced live-owner "contend by acquiring" with target-affinity + drain
> (R1.B2/R3.B1); downgraded the multi-instance safety guarantee to *conditional on BQ-23 + L4, postgres
> only, object-log excluded* (R3.B2/B4); restated L4 as a contract reversal sourced from the cached
> session epoch and checked at commit-time (R3.B3); replaced the infeasible `dyn`-erasure / true-sealing
> encapsulation with static per-backend constructors + `publish=false` topology + private `new` + split
> guard test (R2.B1-B6); scoped the authz claim to ADR-002 embedded mode (R1.B3); added split-brain
> fail-closed (R3.B5) and the density MUST (R1).

---

## 0. Problem & thesis

`pqueue` ships two driving interfaces over one engine: the in-process Rust library (`pqueue` crate,
`Pqueue<B>`) and the RESP wire front (`pqueue-resp`). Per ADR-007 they are **co-equal driving adapters
over the engine ports — siblings, not a stack.** Verified in code:

- RESP's `route` decision (`pqueue-resp/src/routing.rs`) consults ownership via the **engine**
  `OwnerResolution` and assembles `authorize → resolve → serve/redirect`.
- `Pqueue<B>` (`crates/pqueue/src/lib.rs`) holds `{ backend, clock, ids }`, has **no owner identity**,
  and **never** resolves or fences — it delegates straight to `backend.push/claim/finalize`. Its bound
  (`LibBackend`) requires `ControlPlaneStore`; the capability is present and unused.
- Below both, the real data-plane ports **self-stamp the current epoch** and so **never self-fence**
  (`ownership.rs:14-21`, `port.rs:33-34`). A superseded owner's claim is not rejected today.

**Thesis (two distinct claims — do not conflate):**

- **T1 — Coordination is an ENGINE responsibility, enforced below the ports.** Ownership resolution and
  epoch fencing are *coordination*, not *security*; they exist the moment >1 instance shares a queue,
  independent of transport. They live in `pqueue-engine` (per ADR-007: "the engine owns … fencing …
  validation") and are **invoked identically by every driving adapter**. The fence is enforced *below*
  the ports, so neither adapter can skip it — this is what kills the reach-around, without coupling the
  wire protocol to the library's API.
- **T2 — The published library is the only external SURFACE to that engine.** A downstream crate reaches
  the engine *only* through `Pqueue`. The raw ports and concrete backends are not a usable external
  construction-or-call surface (L6/§4a). "The library is the contract" means this — the curated external
  face — **not** that RESP sits on top of the library. RESP consumes the *engine session*, the same one
  the library consumes; it does not depend on the `pqueue` crate.

Authn/authz are **out of the library** in the ADR-002 *embedded-trusted* sense only (L2). The library is
not a substitute for ADR-002 deny-by-default in a multi-tenant host.

---

## 1. Locked decisions (revised)

- **L1 — Engine is the coordination locus; library is the external surface.** Resolve + fence live in
  `pqueue-engine`, below the ports, invoked identically by the library and RESP (T1). Separately, the
  published `pqueue` crate is the sole external surface (T2). These are two claims, both locked; neither
  makes RESP depend on the `pqueue` crate.
- **L2 — Coordination is universal; auth is transport-edge AND host-owned.** Every queue-addressed op
  resolves ownership + operates under the engine fence. The library performs **no authn**. The library
  performs **no authz itself**, but per **ADR-002 §deny-by-default**, a *multi-tenant* embedding host
  MUST authorize (tenant, queue, op) before calling the library; the library neither relaxes nor
  substitutes for that. Trusted single-tenant embedders (ADR-002 embedded mode) may pass a fixed
  principal. RESP adds network authn (`HELLO`/ACL) + wire codec only.
- **L3 — Topology: per-queue ownership, shared-store competition, with target-affinity.** Multiple
  instances share one durable backend and compete for per-queue leases via the pluggable
  `QueueControlPlane`; the storage `assignment_epoch` is the hard fence. **Only the rendezvous
  `target_owner` acquires an unowned/expired queue** (§3); non-targets return `OwnedElsewhere`. This is
  the TD-003/ADR-008 model with the advisory placement made authoritative *at the library policy layer*.
- **L4 — Fence below the ports, sourced from the cached session, checked at commit.** The owner's cached
  `OwnedSession.fence_epoch` (captured at acquire, **never re-read from `current_epoch`**) is threaded
  through `PushPort`/`ClaimPort`/`FinalizePort` into the append and checked **inside the atomic unit of
  work at commit time** (closes the resolve→commit TOCTOU). A superseded owner is `EpochFenced` for
  **both** interfaces. This **reverses** the `port.rs:33-34` "in-process owners pass current epoch, never
  self-fence" contract (see OD-5) and is the load-bearing correctness fix (bead `pqueue-c33c367e`/BQ-23).
- **L5 — "Owned elsewhere" is an engine-resolution value rendered by each adapter.** Both adapters render
  the engine's `OwnerResolution`: RESP → `-MOVED`; the library → `OwnedElsewhere{owner, epoch}`. Neither
  reaches around the resolution.
- **L6 — Encapsulation: the published facade is the only reachable surface.** A client depending on
  published `pqueue` reaches the engine only through `Pqueue`. The raw ports and backends are not a usable
  external construction-or-call surface. Enforced structurally (§4a). Acceptance bar: no safe,
  non-`unsafe`, non-internal-feature path from published `pqueue` to a value on which a downstream crate
  can call `PushPort::push`/`ClaimPort::claim`/`FinalizePort::finalize` directly.

---

## 2. The model: per-queue ownership + shared-store competition

Restating TD-003/ADR-008 as it applies, with the round-1 corrections:

- **Unit of ownership = the queue** (`QueueKey = (tenant, queue)`). No intra-queue sharding.
- **Shared durable store.** N instances share one backend + one **durable** control plane. Each instance
  is an `OwnerId` registered with a heartbeat.
- **Deterministic assignment (now authoritative at policy layer).** `resolve_queue_owner` computes a
  `target_owner` by rendezvous hashing over the live owner set (`control_plane.rs:399-406`). The engine
  keeps acquire *cooperative* (any live owner may acquire an unowned/expired queue —
  `control_plane.rs:35-37`); the **library policy layer restricts acquisition to the `target_owner`** to
  prevent the ping-pong livelock (R3.B1).
- **Acquire rejects a live different owner.** `lease_decide_acquire` (`control_plane.rs:119-121`) Rejects
  if a *different* owner holds a **live** lease. Online handoff is therefore **`begin_drain`** (TD-003
  §Graceful Drain), **not** a contended acquire. The epoch fence only catches a *straggler after*
  expiry/release/drain — it does not arbitrate two live acquirers.
- **The fence requires a single durable epoch.** The multi-instance guarantee holds only when the lease
  epoch and the storage append-fence epoch are **one durable value**, advanced atomically at acquire.
  Today they are **two separate counters on every backend, including postgres** (`ownership.rs:23-30`;
  `postgres/src/control_plane.rs:16-19`; `port.rs:480-484`). Collapsing them is **BQ-23**.

### 2a. Backend safety table (multi-instance shared-store competition)

| Backend | Durability | Shared durable control plane? | Single epoch (lease == append fence)? | Safe for multi-instance? |
|---|---|---|---|---|
| memory | Atomic | No — `InMemoryControlPlane` is per-process `Mutex<HashMap>`, resets on restart | No (two in-proc counters) | **No** — single process only |
| sqlite | Atomic | No shared lease authority across instances | No | **No** — no shared durable control plane |
| postgres | Atomic | Yes (`pqueue_queue_owner`, BQ-22) | **Not today** (separate from `queues.assignment_epoch`; BQ-23 unbuilt) | **No today; Yes once BQ-23 + L4 land** — the only conditionally-safe backend |
| object-log | EventualApply | n/a | No — per-entry epoch non-recoverable; `advance_epoch_object` non-atomic fs write | **No — excluded**; single-owner only until manifest-CAS epoch fence lands |

**The safe-today set is empty.** Multi-instance competition is a **postgres-only, BQ-23-gated**
capability. OD-2 makes the runtime refuse multi-owner construction on any backend that does not present a
single atomic acquire→fence epoch.

---

## 3. The library coordination contract (`Pqueue` per op)

Per queue-addressed op (`push`/`upsert`/`claim`/`finalize`/`renew`/`reassign`/`purge`):

1. **Hold an owner identity + control-plane handle.** Construction supplies an `OwnerId`. (Single-owner
   embedders get a degenerate sole-owner policy — OD-3 — so they pay no ceremony.)
2. **Operate under a fenced session.** Acquire-and-fence the queues this instance is the `target_owner`
   for, caching `OwnedSession{lease_epoch, fence_epoch}`. A bounded, shared, **per-node** renew/heartbeat
   driver (never one task per queue — ADR-002 density MUST) keeps leases live. After a renew/acquire
   **timeout**, the library **re-`resolve`s — it does NOT blindly retry `acquire`** (non-idempotent;
   `control_plane.rs:296-303`), else it fences its own writes.
3. **Resolve ladder for a queue this instance does not already hold:**
   - this instance is the live `active_owner` → serve under cached `fence_epoch`;
   - this instance **is the `target_owner`** and the queue is unowned/expired → `acquire_and_fence`, then
     serve;
   - a **different live owner** holds it (or this instance is not the target) → return
     `OwnedElsewhere{owner, epoch}` (the value form of `-MOVED`). Default policy: **do not acquire**;
     forward or surface to the caller (OD-1). Never "contend by acquiring" against a live lease.
   - lease is `Draining` and this instance is the draining owner → serve **in-flight** ops, refuse a
     **new** claim with a retryable unavailable (drain split, mirrors `route`); a non-owner of a draining
     queue gets `OwnedElsewhere` to the recorded `target_owner`.
4. **Stamp the cached `fence_epoch` on the write** (L4) — checked at commit inside the atomic UoW, so a
   superseded instance's own in-flight write is rejected `EpochFenced` even mid-operation. **Lease
   liveness must fail closed on the DATA path** via this epoch, not only on the control-path renew loop
   (R3.B5): a host stall that lets the lease expire and a peer reclaim it ⇒ the stalled instance's next
   append is `EpochFenced`, regardless of whether its renew task has run.

Because §3's capabilities (resolve, fence, `OwnedElsewhere`, drain split) are expressed in the engine and
surfaced through the library API, RESP needs nothing the library can't express — the reach-around has no
place to hide.

---

## 4. The load-bearing correctness fix (L4 detail)

Today (`ownership.rs:14-21`, `port.rs:33-34`, sqlite `append_durable` `lib.rs:135-146`): the
claim/push/finalize fast paths read the queue's **current** epoch and pass it as `expected_epoch` —
always-current, never fencing. Only the raw `LogWriter::append` seam checks a caller-supplied epoch
(BQ-20), and the library never routes through it.

The fix, precisely:

- Source `expected_epoch` from the owner's **cached** `OwnedSession.fence_epoch`, **never** from
  `ControlPlaneStore::current_epoch` (sourcing from `current_epoch` makes L4 a no-op — R3.B3).
- Thread it through `PushPort`/`ClaimPort`/`FinalizePort` into the backend's atomic unit of work, and
  **check it at commit time** within that UoW (closes the resolve-at-N / commit-at-N+1 TOCTOU).
- Requires the single durable epoch (BQ-23) to be meaningful; until then the two-counter hazard
  (`ownership.rs:23-30`) leaves a crash window. **L4 and BQ-23 ship together or the guarantee is void.**
- This **reverses** `port.rs:33-34` and the `append_durable` BQ-20 notes — those become contract text to
  rewrite (OD-5), not invariants to preserve.

---

## 4a. Encapsulation mechanism (L6 detail — corrected for Rust feasibility)

The leak is at **construction**: `pub fn new(Arc<B>, clock)` (`lib.rs:107`) hands `B` *in*, leaving the
client holding an `Arc<B>: ClaimPort` to call `.claim()` directly. The corrected, feasible mechanism set:

1. **Static per-backend constructors; backend type is private.** `Pqueue::open_sqlite(path) ->
   EngineResult<Pqueue<impl LibBackend>>`, `open_postgres(cfg) -> …`, etc. — each builds the backend
   internally and returns an **opaque** `Pqueue<impl LibBackend>`; the client never names or holds `B`.
   **No `dyn` erasure** — every `LibBackend` port method returns RPITIT (`impl Future`), which makes
   `dyn LibBackend` not object-safe (the dispositive bar); `Backend::write<R,F>` being a generic method
   additionally bars any *future* erased-`Backend` shim. A single runtime `open(cfg)` switch is therefore
   **out of scope** unless that erased-shim port redesign happens (record in OD-6). Backend selection is
   compile-time, **feature-gated**: each adapter is an `optional = true` dependency of `pqueue` activated
   by a feature (`pqueue/features = ["sqlite","postgres",…]`, each pulling `dep:pqueue-sqlite` etc.) so a
   sqlite-only consumer never compiles postgres/objectlog (R2.B6). This makes `pqueue` a thin composition root that fans into
   the (feature-selected) adapter crates — an accepted, explicit dependency inversion.
2. **`Pqueue::new` becomes crate-private** (`pub(crate)` / `#[doc(hidden)]`). The public construction path
   is the `open_*` constructors only. This is required independent of everything else (R2.B5).
3. **Publish topology is the real wall.** Mark `pqueue-engine`, `pqueue-projection`, `pqueue-conformance`,
   and the four adapter crates `publish = false` (or `*-internal` with a "not a stable API" banner);
   **`pqueue` is the only `publish = true` crate.** Only an adapter can construct a port-callable `B`, so
   if adapters are unpublishable, an external client cannot obtain one without a deliberate path-dependency
   on an internal crate (R2.B4). First-party crates (`pqueue-resp`, `pqueue-server`) are *not* downstream
   consumers — they legitimately depend on the engine in-workspace (R1 non-blocking).
4. **Trait hygiene, not sealing.** `#[doc(hidden)]` the port traits + a `#[doc(hidden)] pub` marker
   supertrait adapters impl. True sealing is **infeasible** (the blessed impls live in adapter crates the
   engine may not name under `dependency_direction.rs` — R2.B3); closure comes from #3 (unreachable
   adapters), not sealing. Sealing blocks `impl`, not call, and call is the leak — so hygiene only.
5. **Split guard tests.** (a) extend the manifest-scan `dependency_direction.rs` to assert the publish
   topology (only `pqueue` publishable; engine/adapters not). (b) a **`cargo-public-api`** snapshot of
   `pqueue` asserting no exported `*Port` trait, no `pub fn -> impl ClaimPort`, no public `Arc<B>`
   accessor; or a **`trybuild`** compile-fail proving a `pqueue`-only downstream cannot name a port or
   reach `.backend`. The two halves are different properties (manifest text vs type surface) and cannot be
   one test (R2.#4).

Conformance is **unaffected** (R2.N1): `pqueue-conformance` is generic over the backend and invoked from
each adapter's own tests against the raw backend — it never constructs `Pqueue`, and the ports staying
*in-workspace*-callable is fine because the adapters become unpublished.

Pit of success: `use pqueue::Pqueue; let q = Pqueue::open_postgres(cfg)?;` is the only ergonomic path and
is fully coordinated/fenced. Reaching a raw port requires a deliberate dependency on an internal crate.

---

## 5. What exists vs what this work adds

| Capability | State today | This work |
|---|---|---|
| `QueueControlPlane` (resolve/acquire/renew/rendezvous) | exists (in-mem ref + postgres BQ-22) | reuse |
| `acquire_and_fence` / `OwnedSession` | exists (`engine/ownership.rs`) | reuse; call from library |
| Epoch fence at raw append seam (BQ-20) | exists | reuse |
| **Single durable epoch (lease == append fence)** | **missing — two counters even on postgres** | **BQ-23 (prereq for L4)** |
| **Fence threaded through real ports, commit-time, cached-session-sourced** | **missing (self-stamp)** | **add (L4)** |
| RESP `route` decision | exists (decision only; live wiring deferred) | unblocked by L4; wiring tracked separately |
| **Library owner identity + session + resolve ladder + drain split** | **missing** | **add (§3)** |
| **`OwnedElsewhere` rendered from `OwnerResolution`** | **missing** | **add (L5)** |
| **Target-affinity acquire policy (anti-livelock)** | **missing (engine acquire is cooperative)** | **add (§3, policy layer)** |
| **Data-path fail-closed on lease expiry** | **missing** | **add (§3.4, depends on L4+BQ-23)** |
| **Encapsulated published surface** | **missing — `new(Arc<B>)` leaks `B`; all crates publishable** | **add (§4a)** |
| Multi-instance conformance over shared (postgres) store | **missing** | **add (§7)** |

---

## 6. Open decisions (revised)

- **OD-1 — non-owner default.** A queue owned by a different live instance (or one this instance is not
  the rendezvous target for): the library returns `OwnedElsewhere` and **does not acquire**. Caller
  options: forward (needs owner→endpoint resolution) or surface. **Decision: return `OwnedElsewhere` as
  the primitive; default policy = surface (no auto-forward), forwarding is opt-in with an endpoint
  resolver.** Anti-flap: a non-target may acquire an expired queue only after it has been expired ≥ one
  lease-TTL (hold-down), and only if it has *become* the rendezvous `target_owner` for that queue after a
  membership change. The same target-affinity policy MUST also govern the deferred RESP server
  acquire-runtime (`pqueue-c33c367e`), not only the library, so the two adapters cannot thrash a queue
  against each other.
- **OD-2 — atomic-epoch backend requirement.** Runtime-refuse multi-owner construction on any backend that
  does not present a **single atomic acquire→fence epoch** (i.e. all backends until BQ-23; then postgres
  only). **Decision: refuse, don't just document.** Surface via a capability flag on the backend, not a
  hardcoded type check.
- **OD-3 — single-instance ergonomics.** Sole-owner is a degenerate **policy** (constant "I own
  everything", constant always-current session), not a separate code path — so coordination is uniform and
  existing single-owner `Pqueue` constructions/tests stay green. **Decision: yes.**
- **OD-4 — renew/heartbeat ownership + density.** Provide a **bounded, shared, per-node** renew/heartbeat
  driver the host spawns (never one task/connection per queue — ADR-002 density MUST; TD-003 §Queue
  density). Do not hide a runtime inside the handle by default. Data-path fail-closed (§3.4) means a
  stalled renew cannot cause split-brain. **Decision: host-spawned bounded driver + data-path fence.**
- **OD-5 — conformance/contract reversal from L4.** Enumerate `port.rs:33-34` and the `append_durable`
  BQ-20 notes as contract text to **rewrite** under L4. OD-3's sole-owner constant-session is the
  mechanism that keeps existing single-owner tests green. **Decision: rewrite the contracts; preserve
  single-owner behavior via the degenerate session.**
- **OD-6 — encapsulation set + escape hatch.** Enforce §4a #1+#2+#3+#5 (static constructors, private
  `new`, publish topology, split guard test) + #4 hygiene. Escape hatch: a single, feature-gated,
  loudly-documented `internal` path to a raw backend for custom adapters/tests — strong-by-default, not
  absolute, no `unsafe`. Runtime `open(cfg)` switch deferred (needs an object-safe erased port redesign).
  **Decision: as stated.**

---

## 7. What "converged" means (exit criteria for Phase 1)

1. L1–L6 survive a verification review round with no unresolved blocking objection.
2. OD-1…OD-6 each have a recorded decision (above) that a reviewer agrees is consistent with the cited
   code/specs.
3. §2–§4a are confirmed consistent with ADR-002 / ADR-007 / ADR-008 / TD-003 / TD-006 (no contradiction);
   the new requirements (L4 + BQ-23 dependency) are confirmed as adopting documented deferrals.
4. The 2a backend-safety table is confirmed accurate against the backends' durability classes and epoch
   bindings.
5. A reviewer can restate the boundary in one sentence: *coordination + fence live in the engine below
   the ports and both adapters invoke them; the published library is the only external surface; transports
   add only codec + authn.* If they can't, it isn't converged.
