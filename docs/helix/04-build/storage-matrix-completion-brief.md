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
  review:
    self_hash: 16a37c5b1c592108039bb5cfa176503112fc8509e1ab3334861643e7866c390f
    deps:
      api-fireweed-rust-facade: 26104ab47a5ecfa0f2fea739303d599d3a414461770f73e48a87a14dd48cba37
      orthogonal-storage-matrix-brief: 3e6dda6559c43fb47179240e3aa0b32e280c93ef1dca15177e37c5f7289134c4
      public-preview-boundary: 55311585862169eef8077076f873813037c660be7d4af86cd2dd378da2f48d24
      storage-matrix-composition-inventory: 430f635373938f1d080d471a1ac7c4ba6445b429324477e15cd0888cb3da8c4d
      storage-matrix-conformance-classes: d58ba90b499526a8bdb1b8097597701ed6ae58dd3afa6315e69d9e52dbee830c
    reviewed_at: "2026-08-04T04:52:28Z"
---

# Storage Matrix Completion Brief — Zero Gap

**Status**: Accepted program intent (2026-07-28)  
**Supersedes soft “evidence evolving” posture** for the public 5×4 matrix.
**Governing product model**: [orthogonal-storage-matrix-brief.md](../02-design/orthogonal-storage-matrix-brief.md).

## 1. Definition of done

The public product is **exactly** the 5×4 log × projection matrix. Turso is
the default projection. When this program ends:

| Requirement | Meaning |
|-------------|---------|
| **Every cell opens** | `Fireweed::open(StorageConfig)` and server/Helm select the same pair and start |
| **Every cell works** | Push → claim → finalize; reopen matches durability class; rejection has no effect; Class A `request_id` across crash |
| **Every cell is tested** | Unit + integration for all 20; Class A cells have TP-003 (or equivalent) AC-TXN evidence that cannot pass on stale JSONL alone; deploy-facing cells have kind/Helm smoke |
| **Every cell is supported** | Preview boundary lists all 20 as supported (Class B carries **semantic** disclaimer only, not incompleteness) |
| **No second product** | No profile SKUs; no demoted-but-selectable projections; no long-lived “compat is the real name” |
| **No legacy / YAGNI product surface** | Dead names and parallel construction paths are **removed**, not soft-deprecated forever |

### 1.1 Public matrix (complete feature set)

| Log \ Projection | `memory` | `sqlite` | `turso` (default) | `postgres` |
|------------------|----------|----------|-------------------|------------|
| `memory` | Class B | Class B | Class B | Class B |
| `sqlite` | Class A | Class A | Class A | Class A |
| `postgres` | Class A | Class A | Class A | Class A |
| `filesystem` | Class A | Class A | Class A | Class A |
| `s3` | Class A | Class A | Class A | Class A |

There is no 21st backend. Hybrid is not a public matrix row. Public `turso`
means embedded/local Turso 0.7 in ordinary WAL mode; remote, sync, and MVCC
modes are excluded. “Wired · evidence evolving” is **forbidden** at end state.

### 1.2 Durability classes (unchanged)

- **Class A** (`sqlite` \| `postgres` \| `filesystem` \| `s3` log): log is SoT; projection rebuildable; log replay + `request_id` across crash.
- **Class B** (`memory` log): in-process log for ordering; after process death only projection remains; **must not** claim durable log-replay.

## 2. Per-cell test bar

For **each** of the 20 cells:

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
  projection: Memory | Sqlite | Turso | Postgres  # Turso is the default
```

- **Facade:** `open` / `open_async(StorageConfig)` is the sole full-matrix entry. Thin convenience wrappers only if pure sugar over `StorageConfig`.
- **Server:** parse env/file **into** `StorageConfig`; composition uses only that.
- **Helm:** `storage.log` / `storage.projection` isomorphic; no `objectlog`+`store` product shape.
- **Defaults:** projection defaults to `turso`; all defaults use public names only
  (never `objectlog`, `inmemory`, or Hybrid aliases).

### 3.2 Delete list (product surface)

| Junk | Action |
|------|--------|
| `FIREWEED_LOG_BACKEND=objectlog` + store selector | Remove; only `filesystem` / `s3` |
| `inmemory` alias | Remove; only `memory` |
| Public hybrid / hybrid-strict / hybrid-async projection select | Remove parse, Helm enum, match arms, env, docs; keep canonical `turso` |
| Profile product language | Eradicate from operator/Helm/CI behavioral names |
| Parallel open stacks that bypass `StorageConfig` | Collapse to sugar or delete |
| “Evidence evolving” preview rows for public cells | Forbidden at end state |

**YAGNI:** if it is not one of the 20 cells or a required field for those cells, it does not ship on the public surface.

## 4. Phases (sequential)

| Phase | Work | Exit |
|-------|------|------|
| **0** | This brief + API-005 target + preview target table | Accepted |
| **1** | Fix red recovery/idempotency tests; Class B hard rule executable | Server recovery suites green |
| **2** | `open(StorageConfig)` all 20; server/Helm public axes only; remove product aliases | 20 opens; allowlist = 20; Turso default is explicit |
| **3** | Full T0–T4 (or T0–T3 for process-local) for all 20 | Required CI matrix green, zero skips |
| **4** | Delete legacy names/types/docs; CI grep gate | Product surface clean |
| **5** | Usability: examples, operator, chart values set, preview 20 supported | Docs match code |
| **6** | Release/tag gate binds full matrix | Cannot tag with a failed cell |

**Critical path:** `0 → 1 → 2 → 3 → 4 → 5 → 6`

Capacity evidence (10M recovery, E3 cost) is **not** a substitute for T0–T4 cell correctness. Scale beads stay separate; they do not leave cells untested.

## 5. Success checklist

- [ ] 20/20 open via `StorageConfig`
- [ ] 20/20 T0–T2 in required CI
- [ ] 16/16 Class A T3 green, non-stale
- [ ] Deploy-facing cells T4 green
- [ ] Zero public Hybrid/objectlog/inmemory aliases; canonical `turso` remains public and default
- [ ] Zero “evidence evolving” public matrix rows
- [ ] Known recovery/idempotency failures fixed
- [ ] Preview + operator + API-005 agree with code

## 6. Related artifacts

| Artifact | Role |
|----------|------|
| [orthogonal-storage-matrix-brief](../02-design/orthogonal-storage-matrix-brief.md) | Product axes and Class A/B |
| [storage-matrix-composition-inventory](./storage-matrix-composition-inventory.md) | Wire-up map (update as gaps close) |
| [storage-matrix-conformance-classes](./storage-matrix-conformance-classes.md) | Capability claims by class |
| [public-preview-boundary](../00-discover/public-preview-boundary.md) | End state: 20 supported |
| [API-005](../02-design/contracts/API-005-fireweed-rust-facade.md) | `StorageConfig` open surface |
