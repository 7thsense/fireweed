---
ddx:
  id: storage-matrix-conformance-classes
  depends_on:
    - orthogonal-storage-matrix-brief
    - storage-matrix-composition-inventory
    - td-storage-architecture-backend-contracts
  status: accepted
---

# Storage matrix conformance by durability class (A / B)

**Bead**: `fireweed-13a7b76c`  
**Scope**: Map the public 5×3 log × projection matrix to product **durability
classes** (Class A / Class B), the conformance **capability claims** each cell
may make, and the **CI job shapes** that produce evidence.  
**Normative sources**:
[orthogonal-storage-matrix-brief](../02-design/orthogonal-storage-matrix-brief.md)
§2.2–2.4, [TD-001](../02-design/technical-designs/TD-001-storage-architecture-backend-contracts.md)
conformance classes, [composition inventory](storage-matrix-composition-inventory.md).

This is Phase-3 **evidence orientation** for the matrix program: which suite
obligations bind which cells, and what CI must exercise. It does **not** replace
adapter-owned integration tests or TP-003 transaction-matrix evidence files.

---

## 1. Product durability classes (normative)

| Class | Logs | After process death | Client contract (summary) |
|-------|------|---------------------|---------------------------|
| **A — Durable log** | `sqlite`, `postgres`, `filesystem`, `s3` | Log is system of record; projection rebuildable | Success ⇒ durable on log and visible in serving projection; recovery via high-water + tail replay; `request_id` resolves ambiguity across crash |
| **B — Memory log** | `memory` | In-process log only while alive; **only the projection remains** | Success ⇒ visible in projection; durable **iff** projection is durable (`sqlite` / `postgres`); **no** log rebuild, branch, read-as-of, or change-record-from-log |

Class B is a weaker **persistence envelope**, not a second architecture. Every
cell remains `LogStore × ProjectionStore` with append → apply → acknowledge.

### 1.1 Full matrix (15 cells)

| Log \ Projection | `memory` | `sqlite` | `postgres` |
|------------------|----------|----------|------------|
| `memory` | **Class B** | **Class B** | **Class B** |
| `sqlite` | **Class A** | **Class A** | **Class A** |
| `postgres` | **Class A** | **Class A** | **Class A** |
| `filesystem` | **Class A** | **Class A** | **Class A** |
| `s3` | **Class A** | **Class A** | **Class A** |

---

## 2. Conformance capability claims (by class)

Shared harness: `crates/fireweed-conformance` (`core_suite!`, `log_replay_suite!`,
`durable_reconnect_suite!` / `relational_reconnect_suite!`,
`eventual_apply_suite!`). Adapters compose the macros that match their cell.

### 2.1 Claim vocabulary

| Claim flag | Meaning | Who may claim it |
|------------|---------|------------------|
| **core** | Substrate-independent ports: ordering, eligibility, claim, finalize, lease/epoch fencing, progress bound, request_id push replay **in-process** | **Every** matrix cell |
| **durable_log_replay** | After process death (or durable reopen of the **same** log substrate), state is recoverable from the command log (high-water + tail / snapshot+tail / segment-manifest). Product Class A log-class obligation (TD-001 **log** class). | **Class A only** |
| **projection_reopen** | After process death, a **durable** projection still holds acknowledged state without requiring log rebuild | Class A with durable projection (supplement); **Class B with `sqlite`/`postgres` projection** (sole cross-restart path) |
| **relational_reconnect** | DB-authoritative reconnect suite (`durable_reconnect_suite!`) | Cells whose projection is relational (`sqlite` / `postgres` projection axis) and whose factory reopens shared durable state |
| **eventual_apply** | Log acks before projection; bounded apply window (`eventual_apply_suite!` / engine `DurabilityClass::EventualApply`) | Class A object-log cells (`filesystem` / `s3`) that use eventual-apply composition |
| **in_process_log_read** | In-process `LogRead` / snapshot exercises while the process lives | Allowed on Class A; **allowed on Class B only as a live-process development check — never labeled durable log-replay** |

### 2.2 Class B hard rule (no false log-replay)

> **Class B (`log=memory`) MUST NOT claim `durable_log_replay`.**

- Do **not** assert “kill process → rebuild projection from the memory log.”
- Do **not** record TP-003 / AC-TXN restart+replay clauses as if a durable log
  substrate existed (existing matrix evidence correctly marks those N/A on pure
  in-memory profiles).
- With a durable projection (`memory` × `sqlite` / `memory` × `postgres`):
  reopen tests are **projection-only** (`projection_reopen` /
  `relational_reconnect`), never log-tail recovery.
- In-process `log_replay_suite!` scenarios on `memory` × `memory` exercise
  `LogRead` while the process is alive. That is **`in_process_log_read`**, not
  product Class A log recovery. Evidence and docs must keep that distinction.

Checked-in code map: `fireweed_conformance::matrix_classes` (unit-tested).

### 2.3 Per-cell capability table

Legend: ✓ claim allowed · — not claimed · (P) optional / partial wiring today

| Cell (log × projection) | Class | core | durable_log_replay | projection_reopen | relational_reconnect | eventual_apply | Typical suite wiring |
|-------------------------|-------|------|--------------------|-------------------|----------------------|----------------|----------------------|
| memory × memory | B | ✓ | — | — | — | — | `core_suite!(@atomic)` (+ optional in-process `LogRead`; **not** durable log-replay) |
| memory × sqlite | B | ✓ | — | ✓ | ✓ | — | `core_suite!(@atomic)` + `durable_reconnect_suite!` (projection reopen) |
| memory × postgres | B | ✓ | — | ✓ | ✓ | — | same as memory×sqlite; `postgres` feature + live DSN |
| sqlite × memory | A | ✓ | ✓ | — | — | — | `conformance_suite!` = core + log-replay; reopen from sqlite log |
| sqlite × sqlite | A | ✓ | ✓ | ✓ | ✓ | — | core + log-replay and/or durable reconnect (orthogonal paths) |
| sqlite × postgres | A | ✓ | ✓ | ✓ | ✓ | — | core + log-replay; postgres feature |
| postgres × memory | A | ✓ | ✓ | — | — | — | core + log-replay (`pg_conformance!`, env-gated) |
| postgres × sqlite | A | ✓ | ✓ | ✓ | ✓ | — | core + reconnect; server-primary composition today |
| postgres × postgres | A | ✓ | ✓ | ✓ | ✓ | — | core + relational reconnect (unified relational) |
| filesystem × memory | A | ✓ | ✓ | — | — | ✓* | object-log eventual or segmented in-memory path |
| filesystem × sqlite | A | ✓ | ✓ | ✓ | ✓ | ✓* | `eventual_apply_suite!` / hybrid composed + reconnect where durable image |
| filesystem × postgres | A | ✓ | ✓ | ✓ | ✓ | ✓* | object-log + postgres projection; `postgres` feature |
| s3 × memory | A | ✓ | ✓ | — | — | ✓* | same protocol as filesystem×memory over S3 blob store |
| s3 × sqlite | A | ✓ | ✓ | ✓ | ✓ | ✓* | production-shaped object-log × sqlite |
| s3 × postgres | A | ✓ | ✓ | ✓ | ✓ | ✓* | object-log × postgres; live when S3+PG fixtures present |

\* Object-log compositions that ack at the log barrier and apply within a window
declare engine `DurabilityClass::EventualApply`. Segmented “apply before response”
variants may be atomic at the engine layer but remain **Class A** (durable log).

### 2.4 Engine `DurabilityClass` vs product Class A/B

| Concept | Values | Axis |
|---------|--------|------|
| Product durability class | Class A / Class B | **Log durability after process death** (matrix brief) |
| Engine `DurabilityClass` | `Atomic` / `EventualApply` | **Commit visibility** (append+apply together vs bounded apply lag) |

These are independent:

| Example | Product class | Engine durability |
|---------|---------------|-------------------|
| `memory` × `memory` | Class B | Atomic |
| `sqlite` × `memory` | Class A | Atomic |
| `filesystem` × `sqlite` (group-commit object-log) | Class A | EventualApply |

Conformance macros select **engine** atomic vs eventual suites; **product** class
selects whether durable log-replay and cross-restart log recovery are allowed.

---

## 3. CI evidence layout

Phase-3 intent (matrix brief §7): **filesystem** for default unit/integration,
**S3-compatible sample** for multi-node/provider paths, **postgres feature jobs**
for `postgres` axis cells.

### 3.1 Job shapes

| Shape | What it proves | Where today | Env / fixtures |
|-------|----------------|-------------|----------------|
| **Filesystem default** | Class A object-log protocol + local durability without cloud deps; sqlite/memory cells | PR `ci.yml` functional / fast gates; adapter `cargo test` with temp dirs / `FIREWEED_OBJECT_LOG_ROOT` | No external services; local disk only |
| **S3-compatible sample** | Same protocol over real S3 API (MinIO / Garage / cloud); multi-node / shared-store shapes; **required** product jobs must not skip `s3×{memory,sqlite,postgres}` | Helm gate s3 cell values; kind/deploy sample paths; see `scripts/ci/s3-matrix-job-requirements.md` | `FIREWEED_S3_TEST_ENDPOINT` (+ bucket/region/keys); native create-only; never claim S3 without a live sample |
| **Postgres feature jobs** | All cells with `postgres` log and/or `postgres` projection | `release.yml` (Postgres 16 service); adapter tests env-gated | `FIREWEED_PG_TEST_URL`; build with `--features postgres` (and `objectlog` where needed) |

### 3.2 Suggested cargo commands (local / CI)

```bash
# Harness unit tests (matrix class map + capability rules — no adapters)
cargo test -p fireweed-conformance --lib

# Full conformance crate (includes integration/matrix tests; some need fixtures)
cargo test -p fireweed-conformance

# Class A — filesystem / local object-log default (no cloud, no postgres)
cargo test -p fireweed-objectlog
cargo test -p fireweed-sqlite --test conformance

# Class B — memory log reference (in-process only; no durable_log_replay claim)
cargo test -p fireweed-memory

# Class A — postgres axis (feature + live DSN)
FIREWEED_PG_TEST_URL=postgres://… \
  cargo test -p fireweed-postgres --features postgres

# Class A — s3 sample (when fixtures present; skip cleanly otherwise)
# Set S3 endpoint/bucket/creds per container-runtime-contract / operator guide.
cargo test -p fireweed-objectlog -- --ignored   # only if suite documents S3-gated tests
```

### 3.3 Operator / release notes

- Public support claims name **cells** with evidence on the release revision
  (see `DEPLOYMENT-READINESS.md` and `public-preview-boundary.md`).
- Class B cells in release notes carry an explicit durability disclaimer.
- Class A claims require durable_log_replay (or documented relational-reconnect
  + log class where both apply) evidence for that cell — not “memory suite green”
  alone.

---

## 4. Code map (checked-in)

| Path | Role |
|------|------|
| `crates/fireweed-conformance/src/matrix_classes.rs` | Enumerates 15 cells, product class, claim flags; unit tests enforce Class B ⇒ `!durable_log_replay` |
| `crates/fireweed-conformance/src/lib.rs` | Suite macros; documents adapter wiring (engine atomic/eventual) |
| Adapter crates `tests/` / `src/tests.rs` | Invoke `core_suite!` / `conformance_suite!` / `eventual_apply_suite!` / reconnect suites per cell |

---

## 5. Acceptance (this bead)

```bash
rg -n 'Class B|Class A|filesystem|durability class' \
  crates/fireweed-conformance docs/helix 2>/dev/null | head -40
cargo test -p fireweed-conformance --lib 2>&1 | tail -30
```

Expected: doc + code mention Class A/B; `matrix_classes` unit tests pass; package
does not regress unrelated lib tests.
