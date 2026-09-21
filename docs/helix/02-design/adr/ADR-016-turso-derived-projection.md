---
ddx:
  id: adr-turso-derived-projection
  depends_on:
    - adr-full-async-storage-boundaries
  links:
    - {kind: informed_by, to: adr-full-async-storage-boundaries}
    - {kind: informed_by, to: adr-embedded-engine-integration-and-public-surface}
    - {kind: informed_by, to: adr-orthogonal-log-projection-composition}
    - {kind: informed_by, to: adr-log-single-source-of-truth}
    - {kind: informed_by, to: discover-rust-native-embedded-projection-alternatives}
    - {kind: informed_by, to: discover-turso-0-7-compatibility-probe-results}
  status: accepted
  review:
    self_hash: b93a1a9c4ba242940b86878551dddd35f9aa4e399357417c620e66f5ab2a7b67
    deps:
      adr-full-async-storage-boundaries: 0543121229a415143387307275263908017b43697ddac970d54d6d30a2c7ccaa
    reviewed_at: "2026-08-04T04:50:53Z"
---

# ADR-016: Turso is the default public derived projection

| Date | Status | Deciders | Related | Confidence |
|------|--------|----------|---------|------------|
| 2026-09-17 | Accepted; SQLite log/projection retired by the project owner | Project owner | ADR-006, ADR-012, ADR-015, TD-010 | High |

## Context

The Rust-native projection evaluation compared bundled SQLite, libSQL, redb,
Turso Database, Fjall, and sled. Turso 0.7 accepted the production relational
schema, partial indexes, priority/FIFO query, blocked-gate anti-join, typed-index
range, cursor/item atomic transaction, rollback, reopen, concurrent writers,
and active-key conflict. Its initial no-go result was limited to the old
synchronous storage-port boundary. ADR-015 removed that boundary, and the
implemented native-async adapter now aligns with the product vision of
interchangeable projection stores.

The 2026-08-03 internal-only disposition treated implementation packaging as a
product constraint. The project owner has corrected that interpretation: Turso
is a supported projection axis value and the default selection. This ADR keeps
the qualified technical boundary narrow instead of implying support for every
Turso operating mode.

## Decision

ADR-024 supersedes the multi-projection public axis in this Decision. Turso
Database in `fireweed-turso` is the public relational projection, composed only
with the S3 object log. `ResponseBarrier` has only `AsyncProjection`. The other
axis pairs and `Strict` are not a roadmap. Class A durability is the object
log. Turso is rebuildable through `projection_control` and is not the command
log, the retention authority, or the control plane. The rusqlite SQLite log and
projection are retired. Retained compatibility selectors and any supplied
`sqlite_projection_deferred_flush_chunk` value are rejected before storage I/O;
they are not additional cells. There is no public Class B cell.

The supported boundary is embedded/local `turso = 0.7.2`, pinned with default
features disabled and using ordinary WAL. Remote databases, embedded replicas,
sync/remote replication, experimental MVCC, FTS, and allocator features are not
supported public modes. Initialization uses individual pragma operations with
result consumption and readback; it never retries the rusqlite `execute_batch`
sequence that the probe proved can fail after partially applying
`journal_mode=wal`.

Public enablement requires command/read behavioral conformance, native replay
and in-memory/file parity, reopen/rebuild, cancellation, concurrency,
non-blocking heartbeat, batch-shape, and performance evidence. The retained
native replay pairs use the same Turso implementation on both sides; they do
not constitute an independent SQLite differential oracle. Expected-state
assertions and the shared public conformance suite remain necessary. A build that omits Turso support must reject a requested `turso`
projection as feature-unavailable before storage I/O; a qualifying default
distribution includes the feature. SQLite is no longer a supported adapter or
the current differential reference. Historical compatibility-probe results
retain their original SQLite/Turso identities.

## Alternatives

| Option | Pros | Cons | Evaluation |
|--------|------|------|------------|
| redb | Stable Rust-native KV engine; synchronous fit | Reimplements SQL schema, indexes, joins, and every command arm | Rejected for first adapter |
| Fjall | Rust-native LSM; strong write profile | Similar port cost; explicit durability and compaction tuning | Retained as fallback, not selected |
| libSQL | High SQLite compatibility | C engine and async wrapper; does not meet Rust-native objective | Rejected |
| Keep bundled SQLite only | Proven during the original evaluation | Does not meet Rust-native goal | Retired; original probe results remain historical evidence |
| **Turso 0.7 local WAL projection** | Rust-native async SQL; probe preserves current schema/query approach | Pre-1.0 compatibility and cold-build cost | **Selected as the supported default projection** |

## Consequences

| Type | Impact |
|------|--------|
| Positive | Fireweed's default relational projection is genuinely native async and implements the common projection contract. |
| Positive | The public matrix remains orthogonal: selecting Turso does not select or redefine the log. |
| Negative | Turso is pre-1.0 and its compatibility surface must be re-probed on every upgrade. |
| Negative | Cold builds are materially larger; focused Turso qualification remains useful on the one public cell (ADR-024). |
| Neutral | Retained native replay tests compare Turso instances; independent behavioral assertions prevent treating same-engine agreement as an independent differential proof. |

## Risks

| Risk | Prob | Impact | Mitigation |
|------|------|--------|------------|
| Unprobed SQL diverges in one of the full command arms | M | H | Exact expected-state/public conformance assertions plus `ProjectionImage` and read-surface replay comparisons across the command corpus. |
| Cursor advances ahead of materialized state | L | H | One immediate transaction; injected rollback and reopen checks. |
| Upgrade changes file or SQL behavior | M | H | Exact version pin and mandatory compatibility-probe rerun before upgrades. |
| Compile cost grows CI disproportionately | H | M | Cache the pinned dependency and retain a focused adapter job in addition to manifest-driven matrix qualification. |
| Operators infer remote or sync support from the Turso name | M | H | Name the embedded/local 0.7 ordinary-WAL boundary in config, help, and deployment docs; reject unsupported modes before I/O. |

## Validation

| Success Metric | Review Trigger |
|----------------|----------------|
| Supported commands satisfy expected state and native replay/reopen parity | Any image, query, cursor, lease, or index divergence; same-engine equality alone is insufficient. |
| Turso passes the common projection suite on the s3 × turso cell (ADR-024) | Any backend-specific semantic repair or a second public cell. |
| Default public configuration resolves to `turso`; retired SQLite selections and deferred-flush values reject before I/O | Default drift, accepted retired selection, or feature-dependent silent fallback. |
| No reactor blocking under Turso load | Single-thread heartbeat stalls. |
| Turso version remains exactly the probed version | Dependency update or feature expansion. |

## Supersession

- **Supersedes**: ADR-006's statement that Rust-native replacement evaluation is
  out of scope for the derived projection, and ADR-016's own 2026-08-03
  internal-only disposition. The 2026-09-17 retirement also supersedes the
  earlier explicit-SQLite-support and SQLite-differential-reference decisions.
  ADR-006 and the original probe artifacts retain their historical identities;
  they do not reinstate retired selectors or implementations.
- **Aligned with**: ADR-012's orthogonal public storage-product contract and
  ADR-015's native-async storage boundary.

## Concern Impact

- `technology-radar`: Turso 0.7 remains version-sensitive and must be re-probed
  on upgrade even though its qualified local mode is supported and default.
- `resilience`: Turso state is disposable and rebuildable from the log; it never authorizes log retention
  by itself.

## References

- `docs/helix/00-discover/rust-native-embedded-projection-alternatives.md`
- `docs/helix/00-discover/turso-0.7-compatibility-probe-results.md`
- `docs/helix/02-design/technical-designs/TD-010-object-log-turso-projection.md`
