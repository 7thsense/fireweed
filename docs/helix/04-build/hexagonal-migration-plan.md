# Hexagonal Migration Plan v2 — unified engine, two interfaces

Status: DRAFT, post-review-round-1. Pre-release software — **clean cutover**, no compatibility
shims. "Done" includes deletions. v2 folds the adversarial-review findings and the container-object
resolution of the RESP surface.

## 0. Goal & invariants

- **One engine, one path.** CQRS: a **priority-ordered projection** (speed/efficiency) over a
  **log store** (cost-optimized durability). The old "split vs fused" distinction is demoted to a
  backend's durability class (see §2), never visible at the surface.
- **Exactly two interfaces:** a **RESP wire adapter** and a **Rust library API**.
  - **Data plane** (produce/claim/renew/ack/reclaim) is **faithful Redis Streams** — stock
    `redis-cli`/`redis-py` work. pqueue's per-message semantics ride as **reserved entry fields**
    (the "container object", §2.1) plus **server-side policy**; they are not command-grammar changes.
  - **Control plane** (gates, operator repair, queue/group config) is a **thin pqueue-specific
    command vocabulary** over RESP, mirrored by the library. Not Redis-compatible by design — it
    never was a data-plane concern.
  - Documented divergence from Redis: claim delivery is **priority-ordered, not stream-ID order**
    (invisible to a normal consumer-group client; it processes whatever batch it receives).
- **Hexagonal:** the domain core defines ports; adapters depend inward; the domain depends on
  nothing outward. Enforced by crate dependency direction.
- **Clean cutover:** end state deletes `pqueue-service`, `pqueue-client`, `pqueue-kafka`; supersedes
  legacy docs; re-scopes in-flight beads. No legacy path survives.

## 1. Target crate topology

| Crate | Role | Derived from | I/O |
|---|---|---|---|
| `pqueue-core` | **Domain** — types & rules | keep (pure) | none |
| `pqueue-engine` | **Domain** — execution layer, port defs, **shard coordination**, migrated service logic | NEW: absorbs `pqueue-storage` trait defs + `pqueue-service` domain logic | none |
| `pqueue-memory` | **Driven adapter** — InMemory log+projection (reference) | NEW: extracted reference impl | none |
| `pqueue-sqlite` | **Driven adapter** — sqlite log+projection (+ ControlPlane) | refactor onto new ports | rusqlite |
| `pqueue-postgres` | **Driven adapter** — postgres log+projection+ControlPlane, **atomic claim** | refactor onto new ports | tokio-postgres |
| `pqueue-objectlog` | **Driven adapter** — object-log LogStore (eventual-apply class) | refactor onto new ports | object-log/S3 |
| `pqueue-resp` | **Driving adapter** — RESP server (Streams data-plane + pqueue control-plane) | NEW | tokio net |
| `pqueue` | **Driving adapter** — Rust library facade (re-exports a builder; composition stays in root) | NEW thin umbrella | none |
| `pqueue-server` | **Composition root** — bin; DI + operational surface (health/readiness/deploy-probe) | NEW: DI from `runtime.rs`, HTTP stripped | net |
| ~~`pqueue-service`~~ | **DELETE** after §3a migration | — | axum |
| ~~`pqueue-client`~~ | **DELETE** | — | — |
| ~~`pqueue-kafka`~~ | **DELETE** | — | rdkafka |

Dependency rule: no arrow from `pqueue-engine`/`pqueue-core` outward to an adapter; adapters never
call each other; the composition root is the only place that names concrete adapter types.

**Ports (defined in `pqueue-engine`, none silently dropped):** `LogWriter`/`LogRead`,
`ProjectionWriter`/`ProjectionRead`, **`ControlPlaneStore`** (queue defs + shard assignments +
**epoch source of truth** for fencing — kept, was dropped in v1), **`SnapshotStore`** (replay
acceleration — kept; if retired, object-log recovery is full-replay-only, stated in TD-007),
`ClaimPort` (see §2.2), `Clock`, `IdGen`. **Shard coordination** (TD-003: owner assignment +
coordination loop) is homed in `pqueue-engine` and is **net-new build** (the rendezvous/HRW
assignment fn and the runtime loop do not exist yet — not a migration).

## 2. Engine model

### 2.1 The container-object entry contract
A Streams entry is field/value pairs. pqueue reserves a fixed set of field names (the existing
`PushItem` shape) and treats the rest as opaque payload:

| Reserved field | Meaning | Default if absent |
|---|---|---|
| `priority` | ordering key (per queue's priority model) | queue default → FIFO by ID |
| `group_key` | co-residency / per-group ordering | none |
| `cohort_id` | atomic cohort claim/finalize unit | none |
| `not_before` | delayed eligibility | now |
| `max_attempts` | retry bound | queue default |
| `payload` (+ any non-reserved fields) | opaque | — |

`XADD tenant:queue * priority 5 group_key g1 payload <bytes>` is a stock call; the engine reads the
fields and applies server-side ordering/placement. **No client-visible command change.**

### 2.2 Two-class durability contract (replaces v1's single UoW seam)
The single `Backend::write(|log, proj|)` claim was false — object-log is eventual-apply, postgres is
async, and two trait objects can't share one transaction. Model **two declared durability classes**:

- **Atomic** (`pqueue-memory` via a lock, `pqueue-sqlite`/`pqueue-postgres` via one txn): append+apply
  commit together; post-commit projection state is globally consistent.
- **Eventual-apply** (`pqueue-objectlog`): ack after log/manifest commit; projection applies later
  within a bounded window. Guarantee is **self-read-after-write**, not global consistency.

The engine may rely only on the **weakest** guarantee (self-RAW + bounded eventual apply) unless a
backend *declares* the atomic capability. Port methods are **async** (dictated by tokio-postgres);
the seam carries a transaction/unit-of-work handle, not two independent objects. TD-007 owns this
and must exist before the ports freeze (Phase 1).

### 2.3 Single *logical* claim path (not single physical)
"The engine is the single claim path" means single **logical** authority — but a backend MAY
implement claim atomically in one transaction behind `ClaimPort` (postgres keeps its
`FOR UPDATE SKIP LOCKED` CTE; the engine orchestrates, the adapter executes). This preserves
postgres's exactly-once-ordered guarantee instead of unwinding 3.7k LOC of claim SQL.

### 2.4 Driving API (Streams-shaped)
`add`(XADD+fields), `claim`(XREADGROUP `>`, priority order), `reclaim`(XCLAIM/XAUTOCLAIM),
`ack`(XACK), finalize dispositions (complete=XACK; retry/release=PEL/reclaim; rearm=re-XADD;
fail=XACK+DLQ; **at most one custom disposition verb** if conventions are insufficient — pin in
TD-006), `renew`, `pending`(XPENDING), `peek`, `metrics`, group admin. Control-plane verbs
(gates, operator repair, queue config) are pqueue-specific (§0).

## 3. Legacy touchpoint teardown

### 3a. Domain logic to migrate out of `pqueue-service` — CLOSED INVENTORY
Derived from `QueueAdminState` + `AppState`/`QueueCatalog`, cross-checked against the 11 API-001 and
the API-002 operation sets. Every item maps to an engine command/projection or is dropped with
rationale. Several are **in-memory only today** (design debt) → become **log-backed**.

| Logic | Today | Target | Durability |
|---|---|---|---|
| AuthContext, authorize_tenant, authorize_operator_repair | lib.rs:67–110 | engine rule; principal from adapter (RESP `HELLO`/ACL or lib context) | decision in engine |
| Request-id idempotency + operator replay→409 fingerprint | lib.rs:957,1068–1118,1407 | engine | **durable** (TD-007: schema, retention/compaction, replay, **cross-shard** key) |
| **Operator-operation store + get/cancel_operation** (API-002 async-op model) | lib.rs:149,1027–1066 | engine | **durable** (was MISSED in v1) |
| Lease fencing (fence/is_fenced) + **un-fence + compaction** | lib.rs:187–203,605 | engine lease SM | **durable** |
| Queue pause/resume + **un-pause** | lib.rs:170–209,762 | engine eligibility gate | **durable** |
| **`command_position` monotonic counter** (item_version source) | lib.rs:152,205 | engine | **durable** (was MISSED in v1; must not reset on cutover) |
| **QueueCatalog: capabilities, metrics, active-scopes + roll_up** (`GetQueueMetrics`, `DiscoverActiveScopes`) | lib.rs:223–280,1547 | engine projection reads | (was MISSED in v1) |
| Claim-compatibility / finalize / rearm / purge validation | lib.rs:1294–1505 | engine (semantics) + adapter (field shape) | unchanged |
| hash_lease_token / RedactedLeaseToken | lib.rs:32–64 | engine | unchanged |

Deletes-with-REST: `ApiProblem`, axum routes/router, stub `QueueCatalog` HTTP wiring, `runtime.rs`
HTTP/health specifics (the health/readiness/deploy-probe **operational surface** is re-homed to
`pqueue-server` — see M1).

### 3b. Crates
`pqueue-core` KEEP. `pqueue-storage` SPLIT (traits→engine, reference impl→`pqueue-memory`) then
dissolve. `pqueue-sqlite`/`-postgres`/`-objectlog` REFACTOR onto new ports (KEEP). `pqueue-service`/
`-client`/`-kafka` DELETE after §3a.

### 3c. Docs
- **SUPERSEDE:** ADR-005 (Kafka).
- **REWRITE as transport-neutral + binding annex:** API-001 (operations survive; author a neutral
  contract + a RESP binding, not an edit pass); TP-001 (service/HTTP test layer → RESP+lib+conformance).
- **ADD (must exist before deletions/port-freeze):**
  - **TD-006** — RESP surface: command-by-command mapping table (engine op → RESP command, marked
    `[stock-Streams | custom-control | library-only]`), the §2.1 field contract, finalize
    dispositions, and the **tenant+principal+operator-scope presentation** over RESP.
  - **TD-007** — unified engine: the §2.2 two-class durability contract + the durable-state design
    (idempotency/fence/pause: command schema, projection rep, retention/compaction, replay,
    cross-shard semantics).
  - **ADR-007** — hexagonal architecture + two-interface decision (incl. the priority-order
    divergence from Redis and the data-plane/control-plane split).
- **KEEP (update port signatures + API-002 async-op model home):** ADR-001/002/003/004/006,
  TD-001/002/003/004/005, TP-002/003.

### 3d. Beads — re-scope, no blanket halt
Retarget `pqueue-f6fbde17`/`-9c77d5e7`/`-922eaf00` from "API-001 REST claimed-item shape" →
"engine claim-response shape" (transport-neutral). Re-point Lakebase deploy beads
(`-2f57fbe4` helm, `-ea625701` acceptance) from the HTTP service image to the `pqueue-server` image
**and re-specify the health-probe contract** (RESP `PING` or a side health port — M1).

### 3e. Tests
DELETE ~3 Kafka + ~20 HTTP-route tests. **RE-HOME (do not delete — real invariants):** service
product-workflow, operator, invariant-stress, seventh-sense tests → rewritten against the **engine
API**. MIGRATE ~56 core/storage/backend tests to the engine + one backend-conformance suite
(logic unchanged). Conformance must include a **concurrent-claim-race** stress for the `ClaimPort`
backends.

## 4. Phased cutover — test-first, invariant-by-invariant, compiles at every step

1. **Ports + reference engine.** Freeze ports (after TD-007); extract `pqueue-memory`; implement
   claim/ack/lease/eligibility + the §2.1 fields + §2.2 durability classes over memory. Conformance
   green on memory.
2. **Migrate domain logic — one invariant at a time, test-first.** For each §3a item: write the
   engine test, move the logic, make `pqueue-service` **delegate to the engine via an internal call**
   (not a compatibility shim) so the crate keeps compiling. Old HTTP test deleted only after the
   engine test is green for that invariant. (This is a per-invariant gate, not a phase aspiration.)
3. **Refactor driven adapters** onto the ports (memory+sqlite first, then postgres via `ClaimPort`,
   then objectlog as eventual-apply). Conformance green on each, incl. concurrent-claim races.
4. **RESP adapter** (own phase, gated on TD-006): implement the stock-Streams data-plane + the
   custom control-plane + the auth presentation. E2E with `redis-cli`/`redis-py` for the data-plane
   subset and a pqueue client for control-plane.
5. **Library facade + composition root**; full capability matrix (§5) green on both interfaces.
6. **Delete legacy:** remove `pqueue-service`/`-client`/`-kafka` + their tests; dissolve
   `pqueue-storage`; supersede/rewrite docs; re-scope beads.

## 5. Definition of Done (clean-cutover gates)

- `rg` finds zero refs to `pqueue-service`/`-client`/`-kafka`, `NativeRoute`, `axum`, `/v1`,
  problem+json.
- Exactly two driving adapters (`pqueue-resp`, `pqueue`) + one composition root.
- **Capability conformance matrix** signed off: every API-001/API-002 operation × {RESP-stock,
  RESP-custom, library} marked pass / intentional-subset / n-a. A "subset" cell is a *recorded
  decision*, not a silent gap. (This — not the grep — is the gate that actually prevents a half-built
  second interface hiding behind clean names.)
- Every driven adapter implements the same ports; one conformance suite passes on memory+sqlite
  (+postgres+objectlog), including concurrent-claim races and each durability class's declared
  guarantee.
- Every migrated invariant (auth, idempotency, **operator-op model**, fencing, pause, recurrence,
  cohort/group, purge, **command_position monotonicity**) has an engine-level test.
- Durable-state debt closed: idempotency cache, fences, pause, `command_position` reconstructable
  from the log (TD-007), not in-process `Mutex` state. ControlPlaneStore epoch-fencing intact.
- Docs carry no surviving REST/Kafka normative claims; ADR-007/TD-006/TD-007 recorded.

## Open design work that must land BEFORE any deletion (reviewers' top condition)
1. **TD-006** (RESP mapping + field contract + auth presentation) — proves the RESP surface on paper.
2. **TD-007** (two-class durability + durable-state semantics) — proves the engine contract before
   the ports freeze.
Until both exist, Phases 1–6 do not start. Everything else in this plan is mechanical once they do.
