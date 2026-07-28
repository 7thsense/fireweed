---
ddx:
  id: public-preview-boundary
  depends_on:
    - product-vision
    - production-deployment-readiness
    - orthogonal-storage-matrix-brief
    - storage-matrix-completion-brief
  review:
    self_hash: 654a47a2f3754980ce33f1348dbe42bb15c05a6f64dda4e4e2686bd84cc9f9d0
    deps:
      product-vision: d70aaff09b5d5f59211e5ef3ae9156ee30776e95bce7a70398978e83e39d39e8
      production-deployment-readiness: a8c78f2f4659471b79c52db30c18a22fe6d3d74b0f8a4dd2a62b6c195ea5f6be
    reviewed_at: "2026-07-23T01:03:20Z"
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
| **Log backend** | `memory`, `sqlite`, `postgres`, `filesystem`, `s3` | Command append, epoch/fence authority, replay when durable |
| **Projection** | `memory`, `sqlite`, `postgres` | Serving, claim selection, validation, apply |
| **Control plane** | (unchanged; in-process / postgres, etc.) | Queue definitions, placement, ownership — composed but not redefined here |

There is no public “profile” product type. Pair strings may appear only in test IDs and historical
evidence filenames. `filesystem` and `s3` are peer object-log backends (same protocol: segments,
manifest, conditional write / authority); multi-writer still requires ownership and fencing rules.

**Postgres** is a first-class log backend and a first-class projection backend. Feature flags or image
builds that omit the adapter are packaging choices and must fail closed with a clear message.

### Durability classes

| Class | Logs | Authority after restart | Client contract |
|-------|------|-------------------------|-----------------|
| **A — Durable log** | `sqlite`, `postgres`, `filesystem`, `s3` | Log is system of record; projection is rebuildable cache | Success ⇒ durable on log and visible in serving projection; recovery via high-water + tail replay; `request_id` resolves ambiguity across crash |
| **B — Memory log** | `memory` | In-process log for ordering while alive; **after process death only projection remains** | Success ⇒ visible in projection; durable **iff** projection is durable (`sqlite` / `postgres`); no log rebuild, branch, read-as-of, or change-record-from-log |

Class B is a weaker **persistence envelope**, not a second architecture. Every cell remains
`LogStore × ProjectionStore` with append → apply → acknowledge for that class. Class B cells carry
an explicit **semantic durability disclaimer**: durability is limited to the projection. That is the
only Class B caveat—not incompleteness, not “development only,” and not a demoted product row.

### Full matrix (15 cells) — all preview-supported

Every cell is a valid, preview-supported selection. Semantics differ only by durability class.
Open via typed `StorageConfig` (`Fireweed::open` / `open_async`); server and Helm select the same pair.

| Log \ Projection | `memory` | `sqlite` | `postgres` |
|------------------|----------|----------|------------|
| `memory` | Class B · **supported** | Class B · **supported** | Class B · **supported** |
| `sqlite` | Class A · **supported** | Class A · **supported** | Class A · **supported** |
| `postgres` | Class A · **supported** | Class A · **supported** | Class A · **supported** |
| `filesystem` | Class A · **supported** | Class A · **supported** | Class A · **supported** |
| `s3` | Class A · **supported** | Class A · **supported** | Class A · **supported** |

### Preview support posture

All **15** public matrix cells are **preview-supported**. Maintainers accept correctness reports
against the documented contract for each cell and intend configuration compatibility within the
0.x minor line (definition above).

| Log backend | Projection | Durability | Preview posture |
|-------------|------------|------------|-----------------|
| `memory` | `memory` | Class B | **Supported** — process-local; after process death neither log nor projection remains |
| `memory` | `sqlite` / `postgres` | Class B | **Supported** — durability limited to the projection; **no** Class A log rebuild, branch, read-as-of, or change-record-from-log claims |
| `sqlite` | `memory` / `sqlite` / `postgres` | Class A | **Supported** — durable sqlite log; projection as selected |
| `postgres` | `memory` / `sqlite` / `postgres` | Class A | **Supported** — first-class durable postgres log; projection as selected (`postgres` cargo feature / image packaging may omit the adapter and must fail closed) |
| `filesystem` / `s3` | `memory` / `sqlite` / `postgres` | Class A | **Supported** — durable object log (filesystem and s3 are peers); projection as selected |

Optional implementation details under a durable projection (hot-memory / async knobs, feature-gated
adapters such as Turso) may exist in the repository for evaluation. They are **not** public matrix
rows and carry no compatibility promise as product values.

All preview-supported **Class A** combinations must preserve the same external transaction
contract: successful mutations are durable and visible, rejected mutations have no durable effect,
and ambiguous retries are resolved by request identity. Class B combinations preserve visibility
and rejection semantics, with durability limited to the projection when that projection is durable.

Conformance obligations by durability class:
[storage-matrix-conformance-classes.md](../04-build/storage-matrix-conformance-classes.md).
Composition inventory:
[storage-matrix-composition-inventory.md](../04-build/storage-matrix-composition-inventory.md).
Deployment readiness evidence (scale, cost, broader topologies) does not redefine which cells are
in the public support set; it informs production claims beyond this preview boundary.

## Experimental

Experimental surfaces are present in the repository but are not part of the public support claim
or the public storage matrix:

- Feature-gated adapters (for example Turso) remain validation-oriented until separately promoted.
- Non-matrix implementation knobs under durable projections may change or be removed without
  compatibility aliases.
- Experimental surfaces may change or be removed without migration guidance.

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
| `fireweed-objectlog` | Runtime adapter | Supported through `filesystem` / `s3` log backends above. |
| `fireweed-sqlite` | Runtime adapter | Supported as log backend and as projection backend in the matrix. |
| `fireweed-server` | Public runtime | Supported service binary within the storage-axes boundary above. |
| `fireweed-resp` | Public protocol adapter | Supported RESP surface subject to its documented conformance contract. |
| `fireweed-memory` | Runtime adapter | Supported Class B memory log and memory projection paths in the matrix. |
| `fireweed-postgres` | Runtime adapter | Supported first-class log and projection adapter in the matrix. |
| `fireweed-turso` | Experimental adapter | Feature-gated validation surface; no compatibility promise; not a public matrix value. |
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
- no support for unbounded custom backends outside the 5×3 matrix;
- no Class A recovery / branch / read-as-of claims for Class B (memory log) cells;
- no framing of Postgres as an incomplete or deferred product family;
- no public profile SKUs or demoted-but-selectable projections as product values.

## Support

Support posture for public preview is best-effort and release-boundary limited:

- supported issues are correctness regressions, schema drift, reopen/rebuild failures, and mismatches
  with the documented preview contract (all 15 matrix cells);
- unsupported issues include workload sizing, operator hardening, SLA requests, and deployment
  topologies outside the boundary above;
- production support claims are deferred until the relevant release-readiness gates are explicitly
  re-affirmed.

## Deferred

Deferred production claims include:

- general production support for feature-gated experimental adapters (for example Turso);
- provider certification and universal capacity claims for every object store;
- release-tier cost and 10-million-item recovery claims until their open evidence beads close;
- multi-region failover, SLA, and capacity leadership claims.

The repository contains deployment and scale evidence beyond the per-cell correctness bar. That
evidence informs production readiness; it does not shrink or expand the public 15-cell support set.
