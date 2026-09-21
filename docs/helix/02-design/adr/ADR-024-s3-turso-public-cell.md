---
ddx:
  id: adr-s3-turso-public-cell
  depends_on:
    - adr-orthogonal-log-projection-composition
    - adr-log-single-source-of-truth
    - adr-full-async-storage-boundaries
    - adr-turso-derived-projection
  links:
    - {kind: informed_by, to: adr-orthogonal-log-projection-composition}
    - {kind: informed_by, to: adr-log-single-source-of-truth}
    - {kind: informed_by, to: adr-full-async-storage-boundaries}
    - {kind: informed_by, to: adr-turso-derived-projection}
    - {kind: supersedes, to: adr-orthogonal-log-projection-composition}
  status: accepted
---

# ADR-024: The public cell is s3 log × turso projection

| Date | Status | Deciders | Related | Confidence |
|------|--------|----------|---------|------------|
| 2026-09-21 | Accepted | Project owner | ADR-012, ADR-013, ADR-015, ADR-016, ADR-017, AR-2026-09-21 | High |

## Context

`v0.31.30` ships one storage cell. `StorageConfig::validate` accepts only an
S3 object log composed with a Turso projection. Every other log/projection
pair returns `RETIRED_STORAGE_CELL`
(`storage is s3 log × turso projection only; other selectors are retired`)
before storage I/O. `ResponseBarrier` has only `AsyncProjection`. Claim polls
applied rows and may be empty; that empty result is not a failed command.

The 2026-09-17 amendment, repeated on the product vision, the PRD, ADR-012,
and ADR-016, still describes twelve public cells: logs
`memory | postgres | filesystem | s3` crossed with projections
`memory | turso | postgres`, Strict on all twelve, and AsyncProjection only as
an object-log deferral. The alignment review
`docs/helix/06-iterate/alignment-reviews/AR-2026-09-21-repo.md` records that
split (findings F-01 and F-02). It does not shrink the written amendment, and
it does not treat the binary as an unfinished twelve-cell build. The
retirement is newer and intentional. This ADR is the product decision that
review was waiting on: accept the shipped cut (path R1).

## Decision

This ADR supersedes the 2026-09-17 12-cell amendment for public selectors.

The public product is one cell: S3 object-log × Turso projection, named in
the gate as **s3 log × turso projection**. `RETIRED_STORAGE_CELL` is the
public rejection for every other selector. Callers, the server, Helm, and
tests open that cell; they do not assemble a second product beside it.

`ResponseBarrier` has only `AsyncProjection`. There is no public `Strict`
barrier. An empty claim on this barrier is a poll of applied projection
rows, not a command failure and not a signal to retry the mutation.

The other eleven axis pairs are retired. They are not a roadmap, not a
deferred matrix, and not feature-gated public cells. `Strict` is not a
roadmap item either. Restoring either would be a new decision, not the
completion of this one.

Class A durability is the object log. The Turso projection is derived. It is
rebuildable through `Fireweed::projection_control` (verify, delete, rebuild)
and is not the command log, the retention authority, or the control plane.
Loss of the Turso file is recovered by replaying the object log. The Turso
file does not by itself grant log-history semantics.

Historical measurements, DDx ids, and older matrix counts keep the cell that
produced them. They do not qualify a twelve-cell or twenty-cell product and
they do not reinstate a retired selector.

## Alternatives

| Option | Pros | Cons | Evaluation |
|--------|------|------|------------|
| Keep the 2026-09-17 12-cell amendment, including Strict | Matches the last written desired state before this ADR | Contradicts `RETIRED_STORAGE_CELL` and the shipped `v0.31.30` binary | Rejected for the public product |
| Treat the eleven retired pairs and Strict as a roadmap behind the one open cell | Preserves the amendment as future work | Makes a fail-closed retirement look unfinished | Rejected; they are not a roadmap |
| **s3 log × turso projection, AsyncProjection only** | Matches the shipped gate, the release notes, and `projection_control` rebuild from the object log | One cell; claim is a poll | **Selected** |

## Consequences

| Type | Impact |
|------|--------|
| Positive | Vision, PRD, contracts, and the server have one storage law and one barrier to cite. |
| Positive | Class A durability stays on the object log; Turso remains disposable and rebuildable. |
| Negative | Memory, Postgres, and filesystem logs, and memory and Postgres projections, are not public cells. |
| Negative | Callers cannot ask for read-your-writes via Strict. Empty claim means poll again. |
| Neutral | Older ADRs keep their historical sentences only where a later edit marks them historical. This ADR wins on current selectors. |

## Risks

| Risk | Prob | Impact | Mitigation |
|------|------|--------|------------|
| A reader cites the 2026-09-17 amendment as the current product | M | H | This ADR supersedes that amendment for public selectors. Normative sections cite ADR-024. |
| Server `start` still assembles a retired family the facade rejects | H | H | `Server::validate_for_start` must reject every pair except s3 × turso with `RETIRED_STORAGE_CELL` before I/O. |
| A missing object-log authority or async spec is filled in silently | M | H | On this cell, missing `ObjectLogAuthority::NativeConditionalWrite` or `AsyncProjectionSpec` is a validation error. `StorageConfig::s3_turso` sets both. |
| Capacity numbers from another cell are quoted as this pin | M | M | Absolute rates stay attributed to the commit and cell that produced them. `v0.31.30` is a source preview, not a qualification pin. |

## Validation

| Success Metric | Review Trigger |
|----------------|----------------|
| `StorageConfig::validate` and `Server::validate_for_start` accept only s3 × turso and reject every other pair with `RETIRED_STORAGE_CELL` before I/O | A second public cell, or a server path that assembles a family the check already rejected. |
| `ResponseBarrier` has only `AsyncProjection` | A Strict constructor, variant, or required-cell count returns. |
| Turso rebuild goes through `projection_control` and replays the object log | A test or operator path that treats the Turso file as the command log. |
| Missing `NativeConditionalWrite` authority or missing `AsyncProjectionSpec` fails closed; `s3_turso` still sets both | `unwrap_or` substitution on the open path. |

## Supersession

- **Supersedes**: the 2026-09-17 12-cell amendment for public selectors,
  including the copies on the product vision, the PRD storage boundary,
  ADR-012, and ADR-016, and the requirement that Strict cover those twelve
  cells. ADR-012's orthogonal composition model remains the historical
  design of how a log and a projection are assembled. It does not keep
  twelve current cells. ADR-016 remains the decision that Turso is a derived
  projection and not a SQLite differential oracle. It does not keep Turso as
  a default among three public projections.
- **Does not restore**: SQLite log or projection selectors, a Strict barrier,
  or the eleven retired axis pairs.
- **Aligned with**: ADR-013's rule that the log is the single source of
  truth, narrowed so Class A durability is the object log, and with the
  alignment review `AR-2026-09-21-repo`, which asked for this decision
  before any further spec or code edit.

## Concern Impact

- `resilience`: the object log is the durable record. Turso state is
  disposable and is rebuilt through `projection_control`.
- `technology-radar`: the supported embedded projection mode remains Turso
  local ordinary WAL. Remote replicas, sync, and experimental Turso modes
  are not public. The other projection engines are not waiting as a matrix.

## References

- `RETIRED_STORAGE_CELL` in `crates/fireweed/src/lib.rs`
- `docs/helix/06-iterate/alignment-reviews/AR-2026-09-21-repo.md`
- `docs/releases/v0.31.30.md`
- `docs/helix/02-design/adr/ADR-012-orthogonal-log-projection-composition.md`
- `docs/helix/02-design/adr/ADR-013-log-single-source-of-truth.md`
- `docs/helix/02-design/adr/ADR-016-turso-derived-projection.md`
