---
ddx:
  id: public-preview-boundary
  depends_on:
    - product-vision
    - production-deployment-readiness
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

## Supported

“Preview-supported” means maintainers accept correctness reports against the
documented contract and intend to preserve configuration compatibility within
each supported 0.x minor release line. A breaking change requires a minor
version bump and migration guidance. This is not a 1.0 SemVer stability, SLA,
capacity, provider certification, or production-readiness claim.

The public preview supports these 11 profiles, and each row below carries one unambiguous preview
status:

| Profile | Preview status | Boundary |
|---|---|---|
| `sqlite/inmemory` | Supported | Single-process, locally durable embedded use. |
| `objectlog/inmemory` | Supported | Durable object log with a rebuildable in-memory projection. |
| `objectlog/sqlite` | Supported baseline | Durable object log with the reference persistent local projection. |
| `objectlog/hybrid` | Supported | Hot-memory reads over the durable SQLite projection. |
| `objectlog/hybrid-async` | Supported with limits | Async projection debt and backpressure controls are required; production scale and cost claims remain deferred. |
| `memory/inmemory` | Development only | Conformance, examples, and local evaluation; accepted work does not survive process loss. |
| `objectlog/turso` | Experimental | Feature-gated differential and recovery evaluation; not a supported user profile. |
| `objectlog/hybrid-strict` | Experimental | Direct-config test surface; not chart-selectable or production-supported. |
| `postgres/inmemory` | Deferred | Wired and exercised, but excluded from the preview support contract. |
| `postgres/sqlite` | Deferred | Wired and exercised, but excluded from the preview support contract. |
| `postgres/postgres` | Deferred | Wired and exercised; its composition and operational caveats remain outside this preview. |

All supported durable profiles must preserve the same external transaction
contract: successful mutations are durable and visible, rejected mutations
have no durable effect, and ambiguous retries are resolved by request identity.
The wider evidence in
[DEPLOYMENT-READINESS.md](../04-build/DEPLOYMENT-READINESS.md) does not promote a
wired profile into this support boundary.

## Experimental

Experimental components are present in the repository but are not part of the public support claim:

- `objectlog/hybrid-strict` remains explicitly experimental and not production-supported.
- `objectlog/turso` and `fireweed-turso` remain feature-gated and
  validation-oriented until the focused Turso lane is separately promoted.
- Experimental surfaces may change or be removed without compatibility aliases.

## Crate Support Classes

Crate status describes its role in this repository and preview, not a promise
that every crate will be published independently or has a stable SemVer API.
The artifact-topology bead owns registry publication decisions. These 16 workspace crates are
classified below so the preview boundary remains explicit and auditable.

| Crate | Preview class | Public commitment |
|---|---|---|
| `fireweed` | Public Rust facade | Supported ergonomic library and composition surface. |
| `fireweed-core` | Public contract | Supported domain types and queue contract used by the facade. |
| `fireweed-engine` | Runtime substrate | Supported through the public facade and server, not promised as a standalone API. |
| `fireweed-projection` | Runtime substrate | Supported through shipped profiles, not promised as a standalone API. |
| `fireweed-relational` | Runtime substrate | Shared implementation used by supported relational projections. |
| `fireweed-objectlog` | Runtime adapter | Supported through the object-log profiles above. |
| `fireweed-sqlite` | Runtime adapter | Supported through `sqlite/inmemory` and object-log projection profiles. |
| `fireweed-server` | Public runtime | Supported service binary within the profile boundary above. |
| `fireweed-resp` | Public protocol adapter | Supported RESP surface subject to its documented conformance contract. |
| `fireweed-memory` | Development/reference | Local evaluation and conformance only; no durability claim. |
| `fireweed-postgres` | Deferred adapter | Present and tested, but outside the public-preview support boundary. |
| `fireweed-turso` | Experimental adapter | Feature-gated validation surface; no compatibility promise. |
| `fireweed-conformance` | Test tooling | Contributor-facing contract tests; not a runtime product artifact. |
| `fireweed-loadgen` | Test tooling | Load and evidence generation; no public runtime API commitment. |
| `fireweed-release` | Release tooling | Maintainer tooling; not a runtime product artifact. |
| `fireweed-sim-support` | Test tooling | Simulation fixtures and support; not a runtime product artifact. |

## Non-goals

Non-goals for this release boundary:

- no claim that every wired backend is publicly supported;
- no claim of multi-region failover or capacity leadership;
- no claim that the product is a workflow engine or dependency graph engine;
- no promise that the preview backend mix will stay frozen across future releases;
- no performance proof beyond the existing readiness and probe evidence;
- no support for unbounded custom backend combinations.

## Support

Support posture for public preview is best-effort and release-boundary limited:

- supported issues are correctness regressions, schema drift, reopen/rebuild failures, and mismatches
  with the documented preview contract;
- unsupported issues include workload sizing, operator hardening, SLA requests, and deployment
  topologies outside the boundary above;
- production support claims are deferred until the relevant release-readiness gates are explicitly
  re-affirmed.

## Deferred

Deferred production claims include:

- general production support for `objectlog/turso`;
- production support for `objectlog/hybrid-strict`;
- provider certification and universal capacity claims for every object store;
- release-tier cost and 10-million-item recovery claims until their open evidence beads close;
- any public support claim for the wired `postgres/*` matrix;
- any public promise that the preview backend selection is identical to the full deployment-readiness
  matrix.

The repository already contains deployment evidence for a broader set of runtime combinations. This
document intentionally does not promote that evidence into a public support promise until the matching
release gate says to do so.
