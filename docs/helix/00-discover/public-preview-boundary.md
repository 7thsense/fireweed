---
ddx:
  id: public-preview-boundary
  depends_on:
    - product-vision
    - prd
    - api-fireweed-kernel-contract
    - adr-s3-turso-public-cell
    - adr-rust-workspace-and-toolchain-policy
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

Public preview is the externally named slice of Fireweed Queue: a durable
work-state engine for ordered, recoverable execution. It promises the queue
lifecycle of the
[kernel contract](../02-design/contracts/API-006-fireweed-kernel-contract.md):
records in queues, stages, rank-ordered crash-safe claims, and writes by key.
It is not a workflow DAG, not a generic broker, and not a performance
benchmark.

[product-vision.md](./product-vision.md) describes the product. This boundary
narrows the public claim to what maintainers support without overpromising.

## Supported

"Preview-supported" means maintainers accept correctness reports against the
documented contract and intend to preserve the contract and the constructor's
configuration within each supported 0.x minor release line. A breaking change
requires a minor version bump and migration guidance. This is not a 1.0 SemVer
stability, SLA, capacity, provider certification, or production-readiness
claim.

### Contract

The supported surface is API-006, in-process, in Rust:

- `fireweed-contract` holds the `Store` trait, its types and errors, `Clock`,
  the contract constants and the encoders.
- `fireweed` holds the `Fireweed` handle over `Arc<dyn Store>` and the S3×Turso
  constructor. The constructor returns `Fireweed` together with the admin
  handle, `TursoAdmin` (ADR-009, ADR-024).

`Store` has eight operations: `define_queue`, `write`, `claim`, `read`,
`stats`, `scan`, `aggregate` and `delete_queue`. `TursoAdmin` carries the
`Operator` (force-clear), projection control and queue placement. It is for
on-call repair and deployment control, and no data path uses it (API-006 OP4).

### Implementation

There is one production implementation: an S3 object log with a Turso
projection (ADR-024). There is no storage selector, cell name or visibility
barrier to choose.

| Part | Role |
|------|------|
| S3 object log | The durable record. An acknowledged write is durable in the log. |
| Turso projection | Derived serving state: embedded, local Turso 0.7.2 in ordinary WAL mode. `TursoAdmin` projection control verifies it, deletes it and rebuilds it from the log. |
| Control plane | Maps each queue to one partition and holds the partition leases. It is not a storage choice. |

The filesystem publication adapter and Turso on `:memory:` are test tools
inside the implementation, not supported configurations.

### Durability and visibility

- An `Ok` means every effect it reports survives process loss (API-006 DUR1).
  A rejected write has no durable effect. An interrupted request is resolved
  by resending it with the same request id, or by `read` (RQ2, RQ4).
- A claim observes every acknowledged write. `read` and `scan` are fresh; only
  `stats` and `aggregate` may be torn (V1–V5). A short or empty claim is a
  successful poll, bounded by the progress rule C5a.
- A lost Turso file is rebuilt by replaying the object log.

## Crate Support Classes

Classes follow the crate layers of ADR-003. A crate's class describes its
role, not a promise that it is published on its own.

| Layer | Crates | Preview class |
|---|---|---|
| Contract | `fireweed-contract` | Public contract: the supported types and traits. |
| Public facade | `fireweed` | Public Rust facade: the supported handle and constructor. |
| Conformance | `fireweed-contract-suite` | Test tooling that certifies a `Store` implementation; no runtime commitment. |
| Implementation | `fireweed-engine`, `fireweed-objectlog`, `fireweed-relational`, `fireweed-turso` and the control-plane store | Runtime substrate, supported only through `fireweed` and never published (ADR-009). |
| Binaries and harnesses | `fireweed-server`, and the workload, loadgen and bench crates | No preview commitment. A network transport over `Store` is later work (PRD P1-1). |

## Non-goals

Non-goals for this release boundary:

- no claim of multi-region failover or capacity leadership;
- no claim that the product is a workflow engine or dependency graph engine;
- no promise that the preview support slice will stay frozen across future
  releases;
- no performance proof beyond the recorded evidence (TP-002, TP-005);
- no second implementation and no storage selector (ADR-024, ADR-025);
- no network transport or compatibility adapter in this preview (PRD P1-1,
  P2-1);
- no remote, sync, embedded-replica or MVCC Turso mode;
- no S3 provider brand as a product SKU.

## Support

Support posture for public preview is best-effort and release-boundary limited:

- supported issues are correctness regressions against API-006, reopen and
  rebuild failures, and mismatches with this boundary;
- unsupported issues include workload sizing, operator hardening, SLA
  requests, and deployment topologies outside this boundary;
- production support claims wait until the relevant release-readiness gates
  are explicitly re-affirmed.

## Deferred

Deferred production claims include:

- provider certification and capacity claims for every S3-compatible store;
- release-tier cost, multi-region failover, SLA, and capacity leadership
  claims;
- partition snapshots, log truncation, log splitting and full-delete garbage
  collection, which are proposals pending measurement (TD-018).

Deployment and scale evidence informs production readiness. It does not widen
this boundary.
