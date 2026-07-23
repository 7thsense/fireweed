---
ddx:
  id: public-preview-boundary
---

# Public Preview Boundary

## What This Preview Is

Public preview is the externally named slice of Queueyard: a durable work-state engine for ordered,
recoverable execution. It promises the queue lifecycle, not a workflow DAG, not a generic broker, and
not a performance benchmark.

The repository vision in [product-vision.md](./product-vision.md) says the product is a batch-centric
state-machine queue engine. This boundary narrows the public claim to the parts that are already backed
by release-readiness evidence and can be supported without overpromising.

## Supported

Supported in the public preview:

- `objectlog/sqlite` is the baseline supported backend for preview users.
- `objectlog/turso` is supported only as the gated Rust-native projection path described in ADR-016 and
  TD-010, and only within the focused validation envelope those documents describe.

Operationally, the preview expects:

- a single release-line codebase and schema;
- one authoritative object-log deployment for durable command history;
- a local projection that can reopen and rebuild cleanly;
- the supported backend pairings to preserve the same external transaction contract.

The wider matrix recorded in [DEPLOYMENT-READINESS.md](../04-build/DEPLOYMENT-READINESS.md) is
implementation evidence, not a promise that every wired combination is publicly supported in preview.

## Experimental

Experimental components are present in the repository but are not part of the public support claim:

- `objectlog/hybrid-strict` remains explicitly experimental and not production-supported.
- `pqueue-turso` is feature-gated and validation-oriented until the focused Turso lane is promoted.
- Any backend profile that is wired only for internal verification, local development, or matrix
  coverage stays outside the public preview claim even if it is runnable.

## Stable Crates

Stable crates for the preview boundary are the ones that make up the supported public surface and the
shared substrate it depends on:

- `pqueue-core`
- `pqueue-engine`
- `pqueue-projection`
- `pqueue-relational`
- `pqueue-objectlog`
- `pqueue-sqlite`
- `pqueue-server`
- `pqueue-resp`

These crates are the public preview backbone because they carry the product contract, the shared
relational model, and the supported runtime surface.

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
- any public claim that the wired `postgres/*` matrix is part of this preview release;
- any public promise that the preview backend selection is identical to the full deployment-readiness
  matrix.

The repository already contains deployment evidence for a broader set of runtime combinations. This
document intentionally does not promote that evidence into a public support promise until the matching
release gate says to do so.
