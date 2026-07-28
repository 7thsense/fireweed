---
ddx:
  id: storage-matrix-completion-brief
  depends_on:
    - orthogonal-storage-matrix-brief
    - storage-matrix-composition-inventory
    - storage-matrix-conformance-classes
    - public-preview-boundary
    - api-fireweed-rust-facade
  status: accepted
---

# Storage Matrix Completion Brief — Zero Gap

**Status**: Accepted program intent (2026-07-28)  
**Supersedes soft “evidence evolving” posture** for the public 5×3 matrix.  
**Governing product model**: [orthogonal-storage-matrix-brief.md](../02-design/orthogonal-storage-matrix-brief.md).

## 1. Definition of done

The public product is **exactly** the 5×3 log × projection matrix. When this program ends:

| Requirement | Meaning |
|-------------|---------|
| **Every cell opens** | `Fireweed::open(StorageConfig)` and server/Helm select the same pair and start |
| **Every cell works** | Push → claim → finalize; reopen matches durability class; rejection has no effect; Class A `request_id` across crash |
| **Every cell is tested** | Unit + integration for all 15; Class A cells have TP-003 (or equivalent) AC-TXN evidence that cannot pass on stale JSONL alone; deploy-facing cells have kind/Helm smoke |
| **Every cell is supported** | Preview boundary lists all 15 as supported (Class B carries **semantic** disclaimer only, not incompleteness) |
| **No second product** | No profile SKUs; no demoted-but-selectable projections; no long-lived “compat is the real name” |
| **No legacy / YAGNI product surface** | Dead names and parallel construction paths are **removed**, not soft-deprecated forever |

### 1.1 Public matrix (complete feature set)

| Log \ Projection | `memory` | `sqlite` | `postgres` |
|------------------|----------|----------|------------|
| `memory` | Class B | Class B | Class B |
| `sqlite` | Class A | Class A | Class A |
| `postgres` | Class A | Class A | Class A |
| `filesystem` | Class A | Class A | Class A |
| `s3` | Class A | Class A | Class A |

There is no 16th backend. Hybrid/turso are not public matrix rows. “Wired · evidence evolving” is **forbidden** at end state.

### 1.2 Durability classes (unchanged)

- **Class A** (`sqlite` \| `postgres` \| `filesystem` \| `s3` log): log is SoT; projection rebuildable; log replay + `request_id` across crash.
- **Class B** (`memory` log): in-process log for ordering; after process death only projection remains; **must not** claim durable log-replay.

## 2. Per-cell test bar

For **each** of the 15 cells:

| Layer | Class A | Class B |
|-------|---------|---------|
| **T0 Construct** | `StorageConfig` validate + open succeeds | same |
| **T1 Lifecycle** | push / claim / finalize / reject | same |
| **T2 Reopen** | process death → recover via log (+ projection high-water/tail) | process death → recover via projection only; no log-rebuild claim |
| **T3 Contract** | TP-003 AC-TXN-1/2/3/6 for that exact pair; verifier fails on skip/stale | projection durability + rejection; log-replay ACs N/A and **fail if claimed** |
| **T4 Deploy** | Helm render + kind smoke for chart-installable cells | process-local cells: T0–T3 sufficient; document no multi-node claim |

**CI rule:** required jobs for product-claimed cells **must not skip** when fixtures are missing. Provision temp FS, S3-compatible, and Postgres for required matrix jobs.

## 3. Target architecture (clean)

### 3.1 Config — one model

```text
StorageConfig { log, projection, control_plane, … }
  log: Memory | Sqlite | Postgres | Filesystem | S3
  projection: Memory | Sqlite | Postgres
```

- **Facade:** `open` / `open_async(StorageConfig)` is the sole full-matrix entry. Thin convenience wrappers only if pure sugar over `StorageConfig`.
- **Server:** parse env/file **into** `StorageConfig`; composition uses only that.
- **Helm:** `storage.log` / `storage.projection` isomorphic; no `objectlog`+`store` product shape.
- **Defaults:** public names only (`filesystem`, `s3`, `memory` — not `objectlog` / `inmemory`).

### 3.2 Delete list (product surface)

| Junk | Action |
|------|--------|
| `FIREWEED_LOG_BACKEND=objectlog` + store selector | Remove; only `filesystem` / `s3` |
| `inmemory` alias | Remove; only `memory` |
| Public hybrid / hybrid-strict / hybrid-async / turso projection select | Remove parse, Helm enum, match arms, env, docs |
| Profile product language | Eradicate from operator/Helm/CI behavioral names |
| Parallel open stacks that bypass `StorageConfig` | Collapse to sugar or delete |
| “Evidence evolving” preview rows for public cells | Forbidden at end state |

**YAGNI:** if it is not one of the 15 cells or a required field for those cells, it does not ship on the public surface.

## 4. Phases (sequential)

| Phase | Work | Exit |
|-------|------|------|
| **0** | This brief + API-005 target + preview target table | Accepted |
| **1** | Fix red recovery/idempotency tests; Class B hard rule executable | Server recovery suites green |
| **2** | `open(StorageConfig)` all 15; server/Helm public axes only; remove product aliases | 15 opens; allowlist = 15 |
| **3** | Full T0–T4 (or T0–T3 for process-local) for all 15 | Required CI matrix green, zero skips |
| **4** | Delete legacy names/types/docs; CI grep gate | Product surface clean |
| **5** | Usability: examples, operator, chart values set, preview 15 supported | Docs match code |
| **6** | Release/tag gate binds full matrix | Cannot tag with a failed cell |

**Critical path:** `0 → 1 → 2 → 3 → 4 → 5 → 6`

Capacity evidence (10M recovery, E3 cost) is **not** a substitute for T0–T4 cell correctness. Scale beads stay separate; they do not leave cells untested.

## 5. Success checklist

- [ ] 15/15 open via `StorageConfig`
- [ ] 15/15 T0–T2 in required CI
- [ ] 12/12 Class A T3 green, non-stale
- [ ] Deploy-facing cells T4 green
- [ ] Zero public hybrid/turso/objectlog/inmemory product select
- [ ] Zero “evidence evolving” public matrix rows
- [ ] Known recovery/idempotency failures fixed
- [ ] Preview + operator + API-005 agree with code

## 6. Related artifacts

| Artifact | Role |
|----------|------|
| [orthogonal-storage-matrix-brief](../02-design/orthogonal-storage-matrix-brief.md) | Product axes and Class A/B |
| [storage-matrix-composition-inventory](./storage-matrix-composition-inventory.md) | Wire-up map (update as gaps close) |
| [storage-matrix-conformance-classes](./storage-matrix-conformance-classes.md) | Capability claims by class |
| [public-preview-boundary](../00-discover/public-preview-boundary.md) | End state: 15 supported |
| [API-005](../02-design/contracts/API-005-fireweed-rust-facade.md) | `StorageConfig` open surface |
