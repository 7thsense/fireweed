---
ddx:
  id: public-preview-boundary
  depends_on:
    - product-vision
    - production-deployment-readiness
    - orthogonal-storage-matrix-brief
    - storage-matrix-completion-brief
  status: accepted
  review:
    self_hash: 5ba43c1229b88bb13dcced736ff7adfd3346d68ad0af1f3cd771e3b1e2b4f906
    deps:
      orthogonal-storage-matrix-brief: 3e6dda6559c43fb47179240e3aa0b32e280c93ef1dca15177e37c5f7289134c4
      product-vision: 745a023af9f66c4b71312a0271dbea18b3947970eb47e051d4312bb6222befeb
      production-deployment-readiness: 198c3d00238ad2c3e5bfed9384409fef3b48d925327e76d5e36855f5475daa7c
      storage-matrix-completion-brief: 16a37c5b1c592108039bb5cfa176503112fc8509e1ab3334861643e7866c390f
    reviewed_at: "2026-08-07T11:25:30Z"
---

# Public Preview Boundary

## What This Preview Is

Public preview is the externally named slice of Fireweed Queue: a durable work-state engine for
ordered, recoverable execution. It promises the queue lifecycle, not a workflow DAG, not a generic
broker, and not a performance benchmark.

The repository vision in [product-vision.md](./product-vision.md) says the product is a batch-centric
state-machine queue engine. This boundary narrows the public claim to the parts that are already backed
by release-readiness evidence and can be supported without overpromising.

Storage is modeled as independent axes—not as fixed product SKUs. Governing product intent:
[orthogonal-storage-matrix-brief.md](../02-design/orthogonal-storage-matrix-brief.md).
Completion / zero-gap program:
[storage-matrix-completion-brief.md](../04-build/storage-matrix-completion-brief.md).

## Supported

“Preview-supported” means maintainers accept correctness reports against the
documented contract and intend to preserve configuration compatibility within
each supported 0.x minor release line. A breaking change requires a minor
version bump and migration guidance. This is not a 1.0 SemVer stability, SLA,
capacity, provider certification, or production-readiness claim.

### Storage axes

```text
Backend = LogStore × ProjectionStore × ControlPlane
```

| Axis | Public values | Responsibility |
|------|---------------|----------------|
| **Log backend** | `s3` | Command append, epoch/fence authority, Class A replay (ADR-024) |
| **Projection** | `turso` | Serving, claim selection, validation, apply; rebuildable, not the command log |
| **Control plane** | not a storage cell | Queue definitions, placement, ownership — not a second public log or projection |

There is no public “profile” product type. Pair strings may appear only in test IDs and historical
evidence filenames. The public log is the S3 object log (segments, manifest, conditional write /
authority). `memory`, `sqlite`, `postgres`, and `filesystem` are not public log or projection
selectors (ADR-024). They are not a roadmap.

**Turso** is the public projection (embedded/local Turso 0.7, ordinary WAL mode). Remote,
sync, and MVCC Turso modes are outside this boundary. It is not a default among other
projections; it is the only public projection.

**Not public product values:** `hybrid`, `hybrid-async`, `hybrid-strict`, `objectlog`, `inmemory`,
and combined-profile SKUs. Public env/Helm hard-reject those names. Historical Hybrid evidence is
non-governing provenance only
([tp002-objectlog-hybrid-evidence.md](../perf/tp002-objectlog-hybrid-evidence.md)).

### Durability

| Class | Log | Authority after restart | Client contract |
|-------|-----|-------------------------|-----------------|
| **A — Object log** | `s3` | Object log is the system of record; Turso is a rebuildable cache (`projection_control`) | Success ⇒ durable on the object log. Claim polls applied rows (`AsyncProjection`); an empty claim is not a command failure. Recovery replays the object log. `request_id` resolves ambiguity across crash |

There is no public Class B cell.

### Public cell

One cell is preview-supported (ADR-024). Open it via typed `StorageConfig`
(`Fireweed::open` / `open_async`, including `StorageConfig::s3_turso`). Server
and Helm select the same pair. Other selectors reject before storage I/O.

| Log \ Projection | `turso` |
|------------------|---------|
| `s3` | Class A · **supported** · `AsyncProjection` only |

### Preview support posture

The public cell is **preview-supported**. Maintainers accept correctness reports
against the documented contract for that cell and intend configuration compatibility within the
0.x minor line (definition above).

| Log backend | Projection | Durability | Barrier | Preview posture |
|-------------|------------|------------|---------|-----------------|
| `s3` | `turso` | Class A | `AsyncProjection` | **Supported** — object log is durable; Turso rebuilds through `projection_control` |

The only public response barrier is `AsyncProjection`. `Strict` is not a public
barrier. S3 publication authority is `NativeConditionalWrite`; a missing authority
or a missing `AsyncProjectionSpec` rejects before I/O. `StorageConfig::s3_turso`
sets both. Provider brand names (including historical Garage notes in release
history) are not product SKUs.

The public cell preserves one external transaction contract: successful mutations
are durable on the object log, rejected mutations have no durable effect, and
ambiguous retries are resolved by request identity. A following claim may be
empty while apply lags; that is a poll, not a lost mutation.

Conformance obligations by durability class:
[storage-matrix-conformance-classes.md](../04-build/storage-matrix-conformance-classes.md).
Composition inventory:
[storage-matrix-composition-inventory.md](../04-build/storage-matrix-composition-inventory.md).
Deployment readiness evidence (scale, cost, broader topologies) does not redefine which cells are
in the public support set; it informs production claims beyond this preview boundary.

## Experimental

Experimental surfaces are present in the repository but are not part of the public support claim
or the public storage matrix:

- Remote / sync / MVCC Turso modes (local embedded Turso WAL is the public projection).
- Non-matrix implementation knobs under durable projections may change or be removed without
  compatibility aliases.
- Historical Hybrid product names remain internal/test-only construction paths and are hard-rejected
  on the public env/Helm surface.

## Crate Support Classes

Crate status describes its role in this repository and preview, not a promise
that every crate will be published independently or has a stable SemVer API.
The artifact-topology bead owns registry publication decisions. These 16 workspace crates are
classified below so the preview boundary remains explicit and auditable.

| Crate | Preview class | Public commitment |
|---|---|---|
| `fireweed` | Public Rust facade | Supported ergonomic library and composition surface (`StorageConfig` / `open`). |
| `fireweed-core` | Public contract | Supported domain types and queue contract used by the facade. |
| `fireweed-engine` | Runtime substrate | Supported through the public facade and server, not promised as a standalone API. |
| `fireweed-projection` | Runtime substrate | Supported through shipped log × projection compositions, not promised as a standalone API. |
| `fireweed-relational` | Runtime substrate | Shared implementation used by supported relational projections. |
| `fireweed-objectlog` | Runtime adapter | Supported through the public `s3` log above. |
| `fireweed-sqlite` | Retired | Rusqlite log/projection adapter; not a public cell. |
| `fireweed-server` | Public runtime | Supported service binary for the s3 × turso cell above. |
| `fireweed-resp` | Public protocol adapter | Supported RESP surface subject to its documented conformance contract. |
| `fireweed-memory` | Not a public cell | Historical adapter. Not a preview storage cell (ADR-024). |
| `fireweed-postgres` | Not a public storage cell | Not a public log or projection. Not a second storage product (ADR-024). |
| `fireweed-turso` | Public projection adapter | The public local Turso projection (embedded WAL); not a log or control-plane authority. |
| `fireweed-conformance` | Test tooling | Contributor-facing contract tests; not a runtime product artifact. |
| `fireweed-loadgen` | Test tooling | Load and evidence generation; no public runtime API commitment. |
| `fireweed-release` | Release tooling | Maintainer tooling; not a runtime product artifact. |
| `fireweed-sim-support` | Test tooling | Simulation fixtures and support; not a runtime product artifact. |

## Non-goals

Non-goals for this release boundary:

- no claim of multi-region failover or capacity leadership;
- no claim that the product is a workflow engine or dependency graph engine;
- no promise that the preview support slice will stay frozen across future releases;
- no performance proof beyond the existing readiness and probe evidence;
- no public cell other than s3 × turso (ADR-024);
- no Class B memory-log product and no `Strict` barrier;
- no framing of retired selectors as a deferred product family;
- no public Hybrid projection backends or profile SKUs;
- no treating S3 provider brands (including historical Garage) as current product authority.

## Support

Support posture for public preview is best-effort and release-boundary limited:

- supported issues are correctness regressions, schema drift, reopen/rebuild failures, and mismatches
  with the documented preview contract (the s3 × turso cell, ADR-024);
- unsupported issues include workload sizing, operator hardening, SLA requests, and deployment
  topologies outside the boundary above;
- production support claims are deferred until the relevant release-readiness gates are explicitly
  re-affirmed.

## Deferred

Deferred production claims include:

- remote / sync / MVCC Turso modes;
- provider certification and universal capacity claims for every object store;
- release-tier cost and multi-region failover, SLA, and capacity leadership claims.

The repository contains deployment and scale evidence beyond the per-cell correctness bar. That
evidence informs production readiness; it does not add a public cell beside s3 × turso (ADR-024).
