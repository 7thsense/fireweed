---
ddx:
  id: orthogonal-storage-matrix-brief
  depends_on:
    - product-vision
    - prd
    - adr-cqrs-log-projection-storage-model
    - adr-orthogonal-log-projection-composition
    - adr-log-single-source-of-truth
    - api-fireweed-rust-facade
    - td-storage-architecture-backend-contracts
    - public-preview-boundary
  status: accepted
  review:
    self_hash: 3e6dda6559c43fb47179240e3aa0b32e280c93ef1dca15177e37c5f7289134c4
    deps:
      adr-cqrs-log-projection-storage-model: 63ed2521bc7d0e785529aafbd179b3ef22d51cbf3897d51c511540be52ee9ba3
      adr-log-single-source-of-truth: c88063a069f43bd90f31e4875ad8b35fca9876de5b52cb777908d314d46abd1b
      adr-orthogonal-log-projection-composition: 5e35283d3ad0cc38c61d57aac7a63ce7c5fc8028bc8ff5f51a2bb4c28a1f13e6
      api-fireweed-rust-facade: 26104ab47a5ecfa0f2fea739303d599d3a414461770f73e48a87a14dd48cba37
      prd: cd3004bd0dc9ac531d1cd2596e875e51c2de4601e330007fee60da1ea7b3d5ce
      product-vision: 745a023af9f66c4b71312a0271dbea18b3947970eb47e051d4312bb6222befeb
      public-preview-boundary: 55311585862169eef8077076f873813037c660be7d4af86cd2dd378da2f48d24
      td-storage-architecture-backend-contracts: 2d88d342aac82f23616fdff6d94f4ac88701ab6e70c80a0315003c5e66432c74
    reviewed_at: "2026-08-04T04:52:28Z"
---

# Orthogonal Storage Matrix — Product Brief

**Status**: Accepted product intent (2026-07-28)  
**Scope**: Public storage model, configuration layering, durability classes, and the
work sequence that aligns code, contracts, and preview messaging.  
**Non-scope**: Control-plane redesign, Hybrid as a public projection type,
unbounded custom backends, remote/sync/MVCC Turso modes.

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

Assembled by one native-async composition path (ADR-012, ADR-015, ADR-017).
An inherently blocking adapter may isolate a complete transaction behind a
bounded actor; that does not create a second facade or a process-global blocking
execution model. There is **no** public “profile” product type. Pair strings may
appear only in test IDs and historical evidence filenames.

### 2.1 Public axes

| Axis | Public values | Responsibility |
|------|---------------|----------------|
| **Log** | `memory`, `sqlite`, `postgres`, `filesystem`, `s3` | Command append, epoch/fence authority, replay when durable |
| **Projection** | `memory`, `sqlite`, `turso`, `postgres` | Serving, claim selection, validation, apply; `turso` is the default |
| **Control plane** | Optional (in-process / postgres, etc.) | Queue definitions, placement, ownership — composed independently and not redefined here |

**Not public product values:** `hybrid`, `hybrid-async`, `hybrid-strict`,
`objectlog/*` profile names, `postgres/*` wildcards. Hybrid/async knobs, if
retained later, are optional implementation details under a durable
projection—not matrix rows. The public `turso` value is limited to the
embedded/local Turso 0.7 adapter in ordinary WAL mode; remote, sync, and MVCC
modes are outside this product boundary.

### 2.2 Full matrix (20 cells)

Every cell is a valid selection. Semantics differ only by **durability class**.

| Log \ Projection | `memory` | `sqlite` | `turso` (default) | `postgres` |
|------------------|----------|----------|-------------------|------------|
| `memory` | Class B | Class B | Class B | Class B |
| `sqlite` | Class A | Class A | Class A | Class A |
| `postgres` | Class A | Class A | Class A | Class A |
| `filesystem` | Class A | Class A | Class A | Class A |
| `s3` | Class A | Class A | Class A | Class A |

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
| **B — Memory log** | `memory` | In-process log for ordering while alive; **after process death only projection remains** | Success ⇒ visible in projection; durable **iff** projection is durable (`sqlite`/`turso`/`postgres`); no log rebuild, branch, read-as-of, or change-record-from-log |

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

Retired public spellings (`objectlog`, `inmemory`, and Hybrid selectors) fail
closed. Public examples and help use only the five log and four projection
names. Turso is the default projection selector. Historical evidence may retain
old strings as immutable provenance; that does not make retired aliases accepted
configuration.

## 4. Public messaging

- Preview and boundary docs use **axes + combination matrix**, not a Profile column.
- Support claims name **cells** with evidence on the release revision.
- Class B cells carry an explicit durability disclaimer.
- Kill voice lines like “postgres/* deferred” as product incompleteness.

## 5. Non-goals

- Unbounded custom backends outside the 5×4 matrix
- Public Hybrid projection backends as matrix rows
- Remote, sync, or MVCC Turso operation
- Env vars as the product vocabulary for storage  
- Class A recovery/branch/read-as-of claims for Class B  
- Treating filesystem object log as test-only or “fake S3”  
- Mass-renaming historical perf JSONL filenames (verifiers may alias first)

## 6. Alignment state (2026-08-03)

| Area | Aligned state | Remaining governed work |
|------|---------------|---------------------------|
| Product law | Vision, PRD, and this brief define axes, 20 cells, Turso as the default projection, and Class A/B | Reconcile lower ADR/TD/API copies without changing this authority |
| Config | Typed `StorageConfig` validates the matrix; server accepts canonical public names | Complete the single facade dispatcher and prove Helm/config bijection in their owning work |
| Wiring | Existing adapters cover part of the 20-cell matrix | Complete Turso facade/server composition and per-cell evidence in their owning work |
| Execution | Product composition is native async; blocking stores use bounded adapter isolation | Remove residual facade bridges only after every adapter is runtime-safe |
| Legacy | Retired selectors are not public product values | Remove remaining prose/source residue while preserving immutable history |

## 7. Governing execution sequence

This sequence records authority and dependency order. Completed implementation
does not lower the remaining evidence bar, and a later phase cannot redefine an
earlier product contract.

| Phase | Required outcome | Alignment state |
|-------|------------------|-----------------|
| **0 — Product law** | Vision, PRD, and this brief define the axes, classes, and closed public set | Aligned; lower contracts reconcile in authority order |
| **1 — Config surface** | Typed config, server/file/env adapters, Helm fields, canonical names, and migration errors are isomorphic | Typed/server canonical surface exists; facade/Helm proof remains owned downstream |
| **2 — Composition** | Every cell opens through the one composition model and implements the complete public method surface | Turso facade/server wiring and per-method closure remain evidence-bearing work |
| **3 — Evidence** | Per-cell conformance, Class A replay, Class B projection-only recovery, and live provider fixtures fail closed | In progress; no support claim may substitute a compile-only or skipped route |
| **4 — Preview honesty** | Preview, operator, release, and deployment claims name only evidenced behavior and its durability boundary | Normative 20-cell boundary is set; release evidence remains the claim gate |

The per-cell bar is unchanged: open through typed configuration; push → claim →
finalize; rejection has no effect; reopen matches the class; Class A proves
crash/request-id recovery from the durable log; Class B proves only its
projection-persistence boundary.

## 8. Critical path

```text
vision + PRD + brief
     → ADR/TD/API reconciliation
     → canonical requirement and route registry
     → per-cell conformance + live-provider evidence
     → preview and release claims
```

## 9. Success criteria for the program

1. Preview and operator docs describe interchangeable log and projection stores.  
2. All 20 cells start and satisfy the per-cell bar under their durability class.
3. Typed `StorageConfig` is the composition root; Helm matches; env is adapter-only.  
4. ADR-013 and preview messaging agree on Class A vs Class B.  
5. No public profile SKU; no “Postgres incomplete” framing.

## 10. Related artifacts

| Artifact | Role after this brief |
|----------|------------------------|
| ADR-012 | Composition law (keep; drop profile-centric examples over time) |
| ADR-013 | Amend for Class B |
| ADR-001 | Align vocabulary (axes, not deployment profiles) where it still says profiles |
| TD-001, TD-007 | Capability / durability tables for 5×4 |
| API-005 | Full-matrix construction contract |
| `public-preview-boundary.md`, `docs/site/preview.html` | Axes + matrix |
| `DEPLOYMENT-READINESS.md`, container runtime contract | Structured config; matrix wiring status |
| Helm `charts/fireweed-queue` | `storage.log` / `storage.projection` isomorphic to `StorageConfig` |
