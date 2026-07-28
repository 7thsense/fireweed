---
ddx:
  id: orthogonal-storage-matrix-brief
  depends_on:
    - product-vision
    - adr-cqrs-log-projection-storage-model
    - adr-orthogonal-log-projection-composition
    - adr-log-single-source-of-truth
    - api-fireweed-rust-facade
    - td-storage-architecture-backend-contracts
    - public-preview-boundary
  status: accepted
---

# Orthogonal Storage Matrix — Product Brief

**Status**: Accepted product intent (2026-07-28)  
**Scope**: Public storage model, configuration layering, durability classes, and the
work sequence that aligns code, contracts, and preview messaging.  
**Non-scope**: Control-plane redesign, hybrid/turso as public projection types,
unbounded custom backends.

This brief is the governing intent for subsequent ADR/TD/API amendments and
implementation beads. Where it conflicts with older public wording (profile SKUs,
`postgres/*` deferred, production ban on memory log), **this brief wins** until
those documents are updated to match.

## 1. Problem

1. Public docs present storage as **profiles** (`objectlog/sqlite`, `postgres/*`),
   which hides that log and projection are independent axes and implies non-interchangeability.
2. Collapsing Postgres as `postgres/*` deferred reads as an incomplete backend, not a
   packaging or evidence-scope choice.
3. Runtime wiring is a **sparse allowlist**, not the full orthogonal product of
   designed axes.
4. Plan and ops language sometimes centers `FIREWEED_*` env vars, though the library
   and Helm already have structured configuration.

## 2. Product model (normative)

```text
Backend = LogStore × ProjectionStore × ControlPlane
```

Assembled by one composition path (ADR-012). There is **no** public “profile” product
type. Pair strings may appear only in test IDs and historical evidence filenames.

### 2.1 Public axes

| Axis | Public values | Responsibility |
|------|---------------|----------------|
| **Log** | `memory`, `sqlite`, `postgres`, `filesystem`, `s3` | Command append, epoch/fence authority, replay when durable |
| **Projection** | `memory`, `sqlite`, `postgres` | Serving, claim selection, validation, apply |
| **Control plane** | (unchanged; in-process / postgres, etc.) | Queue definitions, placement, ownership — composed but not redefined here |

**Not public product values:** `hybrid`, `hybrid-async`, `hybrid-strict`, `turso`,
`objectlog/*` profile names, `postgres/*` wildcards. Hybrid/async knobs, if retained
later, are optional implementation details under a durable projection—not matrix rows.

### 2.2 Full matrix (15 cells)

Every cell is a valid selection. Semantics differ only by **durability class**.

| Log \ Projection | `memory` | `sqlite` | `postgres` |
|------------------|----------|----------|------------|
| `memory` | Class B | Class B | Class B |
| `sqlite` | Class A | Class A | Class A |
| `postgres` | Class A | Class A | Class A |
| `filesystem` | Class A | Class A | Class A |
| `s3` | Class A | Class A | Class A |

### 2.3 Object-log peers

| Log | Blob store | Typical use |
|-----|------------|-------------|
| `filesystem` | Directory tree (local disk, NAS e.g. `/tank/…`) | Single-site shared FS, simple tests, real path durability |
| `s3` | S3-compatible API | Multi-node cloud / MinIO / Garage |

Same object-log protocol (segments, manifest, conditional write / authority,
retention). Multi-writer still requires ownership and fencing rules; a NAS path
is not an automatic free multi-writer free-for-all.

### 2.4 Durability classes (CQRS-safe)

| Class | Logs | Authority after restart | Client contract |
|-------|------|-------------------------|-----------------|
| **A — Durable log** | `sqlite`, `postgres`, `filesystem`, `s3` | Log is system of record; projection is rebuildable cache | Success ⇒ durable on log and visible in serving projection; recovery via high-water + tail replay; `request_id` resolves ambiguity across crash |
| **B — Memory log** | `memory` | In-process log for ordering while alive; **after process death only projection remains** | Success ⇒ visible in projection; durable **iff** projection is durable (`sqlite`/`postgres`); no log rebuild, branch, read-as-of, or change-record-from-log |

**CQRS is preserved:** every cell remains `LogStore × ProjectionStore` with
append → apply → acknowledge for that class. Class B is a weaker **persistence
envelope**, not a second architecture and not “no LogStore.”

**ADR-013 amendment required:** replace “production null-log retired / log always
mandatory” with: Class A requires a durable log (ADR-013 rules stand); Class B is
explicit, selectable, and documented. No silent null-log; no claiming Class A
guarantees for Class B.

### 2.5 Postgres

Postgres is a **first-class** log backend and a **first-class** projection backend.
It is not a deferred or incomplete product family. Feature flags or image builds that
omit the adapter are packaging choices; they must fail closed with a clear message,
not be framed as “Postgres unfinished.”

## 3. Configuration layering (normative)

| Layer | Role | Normative for product? |
|-------|------|------------------------|
| Typed **`StorageConfig`** (facade + server composition root) | Single model: log × projection (+ control plane, segments, recovery, authority as fields) | **Yes** |
| **Helm** `storage.*` | Deploy document isomorphic to `StorageConfig` | **Yes** for Kubernetes |
| Config file (optional) | Deserialize → `StorageConfig` | Adapter |
| Environment variables | Container inject → `StorageConfig` | **Adapter only** |

### 3.1 Illustrative typed shape

```text
StorageConfig
  log:
    Memory
    | Sqlite { path }
    | Postgres { url, … }
    | Filesystem { root }
    | S3 { endpoint, bucket, region, credentials, … }
  projection:
    Memory
    | Sqlite { path }
    | Postgres { url, … }
  control_plane: …
  # object-log: segments, authority, recovery where applicable
```

- **Embedders** construct from typed config (API-005 must cover the full matrix, not
  only object-log convenience structs).
- **Service** builds one `StorageConfig` / `Config` at startup; composition uses only that.
- **Public docs and preview** describe axes and structured fields / Helm keys.
- **Env name tables** are a “container injection map” appendix, not the definition of storage.

Compat (one minor): map legacy `objectlog` + store local/s3 into `filesystem` / `s3`.
Public examples use the five log names.

## 4. Public messaging

- Preview and boundary docs use **axes + combination matrix**, not a Profile column.
- Support claims name **cells** with evidence on the release revision.
- Class B cells carry an explicit durability disclaimer.
- Kill voice lines like “postgres/* deferred” as product incompleteness.

## 5. Non-goals

- Unbounded custom backends outside the 5×3 matrix  
- Public hybrid / turso projection backends as matrix rows  
- Env vars as the product vocabulary for storage  
- Class A recovery/branch/read-as-of claims for Class B  
- Treating filesystem object log as test-only or “fake S3”  
- Mass-renaming historical perf JSONL filenames (verifiers may alias first)

## 6. Current gap (summary)

| Area | Today | Target |
|------|--------|--------|
| Public story | Profiles; postgres deferred | 5×3 matrix; axes |
| Config | Partial facade types; env-hydrated server; Helm `objectlog`+`store` | Unified `StorageConfig`; Helm isomorphic |
| Wiring | Sparse allowlist | All 15 cells |
| ADR-013 | Bans production null-log | Class B explicit |
| Hybrid | Public projection values | Not public |

## 7. Execution sequence

Phases are strict. Implementation beads must not precede governance where noted.

### Phase 0 — Law and contracts

1. This brief (accepted).  
2. Amend ADR-013 (Class A / Class B).  
3. Align TD-001 / TD-007 capability and durability tables.  
4. API-005 `StorageConfig` (full matrix construction).  
5. Preview boundary + site + DESIGN voice.  
6. Deployment readiness + runtime contract (structured first; env appendix).

### Phase 1 — Config surface

1. Implement typed `StorageConfig` shared by facade and server.  
2. Server startup parses **into** `StorageConfig` (env/file adapters).  
3. Helm schema/values: five logs, three projections; filesystem root and s3 block.  
4. Compat aliases or migration errors for old spellings.  
5. Operator guide examples (including NAS filesystem paths).

### Phase 2 — Wire compositions

Inventory then wire gaps: Class B (`memory` × projections); `sqlite` × all
projections; `postgres` × all (via config, not profile paths); `filesystem` × all;
`s3` × all (including **`s3` × `postgres`**). Remove public hybrid/turso match arms
without regressing Class A object-log × sqlite baseline.

**Per-cell bar:** open via `StorageConfig`; push → claim → finalize; reopen matches
class; rejection has no effect; Class A crash/`request_id` as applicable; Class B
projection-only reopen tests.

### Phase 3 — Evidence

Conformance by class; TP-003 (or equivalent) for claimed Class A cells; Class B
suite without false log-replay claims; CI: filesystem for unit/integration, real
S3-compatible sample for multi-node/provider, postgres feature jobs for pg cells.

### Phase 4 — Preview honesty

Matrix support claims only for evidenced cells; Postgres first-class; Class B
disclaimer; release notes/checklist aligned.

## 8. Critical path

```text
brief → ADR-013 → API-005 StorageConfig
     → typed config + Helm
     → Class B + s3×postgres (hardest wires)
     → evidence → preview claims
```

## 9. Success criteria for the program

1. Preview and operator docs describe interchangeable log and projection stores.  
2. All 15 cells start and satisfy the per-cell bar under their durability class.  
3. Typed `StorageConfig` is the composition root; Helm matches; env is adapter-only.  
4. ADR-013 and preview messaging agree on Class A vs Class B.  
5. No public profile SKU; no “Postgres incomplete” framing.

## 10. Related artifacts

| Artifact | Role after this brief |
|----------|------------------------|
| ADR-012 | Composition law (keep; drop profile-centric examples over time) |
| ADR-013 | Amend for Class B |
| ADR-001 | Align vocabulary (axes, not deployment profiles) where it still says profiles |
| TD-001, TD-007 | Capability / durability tables for 5×3 |
| API-005 | Full-matrix construction contract |
| `public-preview-boundary.md`, `docs/site/preview.html` | Axes + matrix |
| `DEPLOYMENT-READINESS.md`, container runtime contract | Structured config; matrix wiring status |
| Helm `charts/fireweed-queue` | `storage.log` / `storage.projection` isomorphic to `StorageConfig` |
