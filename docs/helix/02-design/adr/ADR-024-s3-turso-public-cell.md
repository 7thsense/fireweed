---
ddx:
  id: adr-s3-turso-public-cell
  depends_on:
    - adr-log-single-source-of-truth
    - adr-full-async-storage-boundaries
    - adr-turso-derived-projection
    - adr-kernel-records-contract
    - api-fireweed-kernel-contract
  links:
    - {kind: informed_by, to: adr-log-single-source-of-truth}
    - {kind: informed_by, to: adr-full-async-storage-boundaries}
    - {kind: informed_by, to: adr-turso-derived-projection}
    - {kind: informed_by, to: adr-kernel-records-contract}
    - {kind: informed_by, to: api-fireweed-kernel-contract}
  status: accepted
---

# ADR-024: The production implementation is the S3 object log × Turso projection

| Date | Status | Deciders | Related | Confidence |
|------|--------|----------|---------|------------|
| 2026-09-24 | Accepted | Project owner | API-006, ADR-001, ADR-007, ADR-008, ADR-013, ADR-016, ADR-025, TD-010, TD-016, TD-017, TD-018 | High |

## Context

- API-006 is the external interface, and any implementation of `Store` that passes the conformance suite is interchangeable (ADR-007, ADR-025).
- Every additional production composition multiplies the conformance runs, performance evidence and support that each release needs. Consumers need one durable, cheap, horizontally placed implementation.
- The storage model is an object log as the record and a derived projection for serving (ADR-001, ADR-013), and Turso is the projection engine (ADR-016).

## Decision

1. **There is one production implementation.** It is an object log published to S3, with a Turso projection. TD-017 designs its projection layout, and TD-018 its partitioned object log. ADR-025 commits to no second implementation.
2. **No storage choice reaches the public surface.** The S3×Turso constructor takes its own configuration plus the construction inputs the contract fixes (the injected `Clock`, the documented Δ and the `DeletePolicy`), and returns the `Fireweed` handle together with the admin handle, `TursoAdmin` (API-006 §7). There is no storage selector, cell name or visibility barrier to choose.
3. **The log engine is `fireweed-objectlog`.** It has one protocol and two publication adapters: S3, for the shared store, and the local filesystem, for a single process. Filesystem publication exists to test the protocol without MinIO. It is not a production configuration, a filesystem test is not evidence about S3, and an S3 result is not evidence about the filesystem adapter.
4. **Durability is the object log's.** The Turso projection is derived: it is not the command log, the retention authority or the control plane. `TursoAdmin`'s projection control verifies, deletes and rebuilds a projection by replaying the log, and the same replay recovers a lost Turso file (ADR-013).
5. **Visibility is the contract's.** A claim observes every acknowledged write, `read` and `scan` are fresh, and only `stats` and `aggregate` may be torn (API-006 V1–V5). A short or empty claim is a successful poll, bounded by the progress rule C5a. TD-016 and TD-017 design how the implementation meets these rules while the projection applies behind the log.
6. **Tests use the same code paths.** Queue-semantics tests and small non-durable benches open Turso on `:memory:`, which has the same schema, SQL and apply path as a file. Tests of checkpoint, reopen and rebuild use a temporary Turso file. No other projection engine counts as coverage of this implementation.
7. **Evidence names its source.** A measured rate or latency is attributed to the commit and configuration that produced it and is never quoted as a property of another build.

## Alternatives

| Option | Evaluation |
|--------|------------|
| A public storage matrix of log and projection selectors | Rejected. Every cell multiplies conformance, evidence and support, and no consumer needs more than one durable implementation. |
| A caller-selectable visibility barrier | Rejected. API-006 fixes one visibility contract for every implementation. |
| PostgreSQL storage plugged in at the engine ports | Rejected. The seam is the contract (ADR-007). A PostgreSQL implementation of `Store` remains possible later. |
| A filesystem object log in production | Rejected. It serves one process and has no shared store for handoff. |

## Consequences

| Type | Impact |
|------|--------|
| Positive | Vision, PRD, contracts and designs cite one implementation, and the suite and gates run on one composition. |
| Positive | Turso state stays disposable and rebuildable from the log. |
| Negative | A deployment without S3-compatible object storage cannot run Fireweed. |
| Negative | Commit latency always includes the object-log linger and S3 round trips (ADR-001). |
| Neutral | Filesystem publication and Turso `:memory:` remain test tools inside the implementation. |

## Risks

| Risk | Prob | Impact | Mitigation |
|------|------|--------|------------|
| A test on the filesystem adapter is cited as evidence about S3 | M | H | Evidence records name the adapter; S3 behavior is qualified against MinIO or S3. |
| An operator path treats the Turso file as the command log | L | H | Rebuild goes only through projection control; live-versus-rebuild comparisons in the suite. |
| The implementation misses a performance gate at scale | M | H | TD-017 §13 gates at 10M records and at 100 queues × 1M records per file. |

## Validation

| Success Metric | Review Trigger |
|----------------|----------------|
| The `fireweed` public API is the eight `Store` ops, the S3×Turso constructor and `TursoAdmin` | A storage selector, second constructor family or backend type becomes public. |
| The contract suite passes on S3×Turso through `run_all` (API-006 CS1) | A scenario needs a backend knob or a per-implementation clause. |
| A rebuild through projection control matches the live projection | Any divergence between rebuilt and live state. |

## Concern Impact

- `resilience`: the object log is the durable record; the Turso projection is rebuilt through projection control.
- `technology-radar`: the projection mode is Turso embedded and local, in ordinary WAL; remote replicas, sync and experimental Turso modes are not used.

## References

- `docs/helix/02-design/contracts/API-006-fireweed-kernel-contract.md`
- `docs/helix/02-design/adr/ADR-013-log-single-source-of-truth.md`
- `docs/helix/02-design/adr/ADR-016-turso-derived-projection.md`
- `docs/helix/02-design/adr/ADR-025-kernel-records-contract.md`
- `docs/helix/02-design/technical-designs/TD-017-kernel-projection-storage-layout.md`
- `docs/helix/02-design/technical-designs/TD-018-partitioned-object-log.md`
