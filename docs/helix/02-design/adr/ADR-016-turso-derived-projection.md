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
| 2026-08-04 | Accepted; supersedes the 2026-08-03 internal-only disposition | Project owner | ADR-006, ADR-012, ADR-015, TD-010 | High |

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

Turso Database in `fireweed-turso` is a supported public relational projection
adapter and the default projection selected when an embedder, service, or
deployment does not specify another projection. The canonical projection axis
is `memory | sqlite | turso | postgres`; all four projections compose with all
five public logs.

Turso is derived and rebuildable. It is never the authoritative command log,
retention authority, or control plane. Durability and replay capability are
determined by the selected log under ADR-012/ADR-013. With the Class B memory
log, only the persisted Turso projection may survive process death, and that
cell does not acquire log-history semantics.

The supported boundary is embedded/local `turso = 0.7.0`, pinned with default
features disabled and using ordinary WAL. Remote databases, embedded replicas,
sync/remote replication, experimental MVCC, FTS, and allocator features are not
supported public modes. Initialization uses individual pragma operations with
result consumption and readback; it never retries the rusqlite `execute_batch`
sequence that the probe proved can fail after partially applying
`journal_mode=wal`.

Public enablement requires the full command/read differential corpus against
the SQLite relational reference, projection conformance, reopen/rebuild,
cancellation, concurrency, non-blocking heartbeat, batch-shape, and performance
evidence. A build that omits Turso support must reject a requested `turso`
projection as feature-unavailable before storage I/O; a qualifying default
distribution includes the feature. SQLite remains supported explicitly and is
the differential reference, not the default.

## Alternatives

| Option | Pros | Cons | Evaluation |
|--------|------|------|------------|
| redb | Stable Rust-native KV engine; synchronous fit | Reimplements SQL schema, indexes, joins, and every command arm | Rejected for first adapter |
| Fjall | Rust-native LSM; strong write profile | Similar port cost; explicit durability and compaction tuning | Retained as fallback, not selected |
| libSQL | High SQLite compatibility | C engine and async wrapper; does not meet Rust-native objective | Rejected |
| Keep bundled SQLite only | Proven and lowest risk | Does not meet Rust-native goal | Retained as explicit reference projection, not the default |
| **Turso 0.7 local WAL projection** | Rust-native async SQL; probe preserves current schema/query approach | Pre-1.0 compatibility and cold-build cost | **Selected as the supported default projection** |

## Consequences

| Type | Impact |
|------|--------|
| Positive | Fireweed's default relational projection is genuinely native async and implements the common projection contract. |
| Positive | The public matrix remains orthogonal: selecting Turso does not select or redefine the log. |
| Negative | Turso is pre-1.0 and its compatibility surface must be re-probed on every upgrade. |
| Negative | Cold builds are materially larger; focused Turso qualification remains useful even though all 20 cells require release evidence. |
| Neutral | Bundled SQLite remains a supported explicit projection and the differential relational reference. |

## Risks

| Risk | Prob | Impact | Mitigation |
|------|------|--------|------------|
| Unprobed SQL diverges in one of the full command arms | M | H | Differential `ProjectionImage` and read-surface suite across the complete command corpus. |
| Cursor advances ahead of materialized state | L | H | One immediate transaction; injected rollback and reopen checks. |
| Upgrade changes file or SQL behavior | M | H | Exact version pin and mandatory compatibility-probe rerun before upgrades. |
| Compile cost grows CI disproportionately | H | M | Cache the pinned dependency and retain a focused adapter job in addition to manifest-driven matrix qualification. |
| Operators infer remote or sync support from the Turso name | M | H | Name the embedded/local 0.7 ordinary-WAL boundary in config, help, and deployment docs; reject unsupported modes before I/O. |

## Validation

| Success Metric | Review Trigger |
|----------------|----------------|
| SQLite and Turso projections are equal after every supported command and reopen | Any image, query, cursor, lease, or index divergence. |
| Turso passes the common projection suite and all five log compositions | Any backend-specific semantic repair or skipped matrix cell. |
| Default public configuration resolves to `turso`; explicit SQLite remains selectable | Default drift, alias-based selection, or feature-dependent silent fallback. |
| No reactor blocking under Turso load | Single-thread heartbeat stalls. |
| Turso version remains exactly the probed version | Dependency update or feature expansion. |

## Supersession

- **Supersedes**: ADR-006's statement that Rust-native replacement evaluation is
  out of scope for the derived projection, and ADR-016's own 2026-08-03
  internal-only disposition. ADR-006's SQLite design remains the relational
  reference and an explicit supported projection.
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
