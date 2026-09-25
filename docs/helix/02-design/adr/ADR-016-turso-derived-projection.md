---
ddx:
  id: adr-turso-derived-projection
  depends_on:
    - adr-full-async-storage-boundaries
    - adr-log-single-source-of-truth
  links:
    - {kind: informed_by, to: adr-full-async-storage-boundaries}
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

# ADR-016: Turso is the derived projection

| Date | Status | Deciders | Related | Confidence |
|------|--------|----------|---------|------------|
| 2026-09-17 | Accepted | Project owner | ADR-013, ADR-015, ADR-024, TD-010, TD-017 | High |

## Context

- The S3×Turso implementation serves every op from a local projection derived from the object log (ADR-001, ADR-013). The projection needs SQL, partial and composite indexes, one-transaction apply with rollback, and reopen, and it must not block the async runtime (ADR-015).
- The Rust-native projection evaluation compared bundled SQLite, libSQL, redb, Turso Database, Fjall and sled. Turso 0.7 accepted the relational schema, partial indexes, the ordered claim query, the blocked-gate anti-join, typed-index ranges, the cursor-and-state transaction, rollback, reopen, concurrent writers and the active-key conflict.
- The probe also showed that a batched pragma sequence can fail after partially applying `journal_mode=wal`.

## Decision

1. **The projection is Turso Database, embedded and local.** Turso 0.7.2 is vendored and pinned exactly, with default features disabled, in ordinary WAL mode. Remote databases, embedded replicas, sync and remote replication, experimental MVCC, FTS and allocator features are not used.
2. **Initialization is one pragma at a time.** Each pragma is issued as its own operation, its result consumed and its value read back. No batched pragma sequence is used or retried.
3. **Turso is derived.** It is not the command log, the retention authority or the control plane. It applies committed commands and is rebuilt from the log through projection control (ADR-013).
4. **One file per partition, one writer per file.** Each partition has one Turso file (ADR-008). Every write goes through one connection behind a mutex; reads use separate serving connections. The page size is 4 KiB, and a Passive WAL checkpoint cap is mandatory (TD-017 §12).
5. **Qualification.** The projection passes the contract suite (API-006 CS1) and the live-versus-rebuild comparison. Replay parity between two Turso instances is not an independent oracle, so scenarios assert expected state, not only agreement between runs.
6. **Upgrades are re-qualified.** Any Turso version or feature change reruns the Turso qualification tests (TP-003 AC-TURSO-1 to AC-TURSO-4) and TD-017 §11 rule 8's plan tests before it lands.

## Alternatives

| Option | Pros | Cons | Evaluation |
|--------|------|------|------------|
| redb | Stable Rust-native KV engine | Reimplements the SQL schema, indexes, joins and every apply arm | Rejected |
| Fjall | Rust-native LSM with a strong write profile | Same port cost; explicit durability and compaction tuning | Not selected |
| libSQL | High SQLite compatibility | C engine behind an async wrapper; not Rust-native | Rejected |
| Bundled SQLite (rusqlite) | Proven in the original evaluation | Not Rust-native; blocking API | Rejected |
| **Turso 0.7 local WAL** | Rust-native async SQL; keeps the schema and query approach | Pre-1.0 compatibility; cold-build cost | **Selected** |

## Consequences

| Type | Impact |
|------|--------|
| Positive | The projection is native async and needs no blocking bridge. |
| Positive | Losing a Turso file costs a rebuild, never data. |
| Negative | Turso is pre-1.0, so every upgrade reruns the Turso qualification and plan tests. |
| Negative | Cold builds are materially larger. |
| Neutral | Replay comparisons between Turso instances are not an independent differential oracle; expected-state assertions carry that role. |

## Risks

| Risk | Prob | Impact | Mitigation |
|------|------|--------|------------|
| SQL that no qualification test covers diverges in an apply arm | M | H | Expected-state assertions in the contract suite, plus live-versus-rebuild comparisons. |
| The applied position advances ahead of the applied state | L | H | One immediate transaction for both; injected rollback and reopen checks. |
| An upgrade changes file or SQL behavior | M | H | Exact version pin; every upgrade reruns the Turso qualification and plan tests. |
| Operators infer remote or sync support from the Turso name | M | H | Configuration and deployment docs name the embedded, local, ordinary-WAL boundary; unsupported modes are rejected before I/O. |

## Validation

| Success Metric | Review Trigger |
|----------------|----------------|
| Live and rebuilt projections match for every scenario of the contract suite | Any image, index, lease or cursor divergence. |
| No reactor blocking under Turso load | A single-thread heartbeat stalls. |
| The Turso version stays exactly the qualified one | A dependency update or feature expansion. |

## Concern Impact

- `technology-radar`: Turso 0.7 is version-sensitive and is re-qualified on every upgrade.
- `resilience`: Turso state is disposable and rebuildable from the log; it never authorizes log retention.

## References

- `docs/helix/00-discover/rust-native-embedded-projection-alternatives.md`
- `docs/helix/00-discover/turso-0.7-compatibility-probe-results.md`
- `docs/helix/02-design/technical-designs/TD-010-object-log-turso-projection.md`
- `docs/helix/02-design/technical-designs/TD-017-kernel-projection-storage-layout.md`
