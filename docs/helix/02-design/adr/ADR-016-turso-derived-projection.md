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
    self_hash: 76ec5fe8523c4fe831441229aa5f09f0bf966ac3849174764a7ba2c2d805f22a
    deps:
      adr-full-async-storage-boundaries: 26d2c37c96eb0801dbb99e4a02213ecfa747aa533572acde3917801a13cebfcd
    reviewed_at: "2026-07-20T00:01:23Z"
---

# ADR-016: Turso is the Rust-native derived SQL projection

| Date | Status | Deciders | Related | Confidence |
|------|--------|----------|---------|------------|
| 2026-07-18 | Accepted | Project owner | ADR-006, ADR-013, ADR-015, TD-004 | Medium |

## Context

The Rust-native projection evaluation compared bundled SQLite, libSQL, redb, Turso Database, Fjall,
and sled. Turso 0.7 accepted the production relational schema, partial indexes, priority/FIFO query,
blocked-gate anti-join, typed-index range, cursor/item atomic transaction, rollback, reopen, concurrent
writers, and active-key conflict. Its no-go result was limited to the synchronous storage-port boundary:
the driver is native async and the current projection apply seam cannot await.

ADR-015 independently removes that boundary. The remaining decision is which Rust-native engine becomes
the first production derived projection.

## Decision

Turso Database is selected as the first Rust-native, local relational projection adapter. It is a
rebuildable projection paired with the segmented durable object log; it is not an authoritative log,
control plane, remote Turso service, or replacement for the standalone SQLite durable backend.

The adapter pins the probed `turso = 0.7.0` with default features disabled and uses ordinary WAL. It does
not enable experimental MVCC, sync/remote replication, FTS, or allocator features. Initialization uses
individual pragma operations with result consumption and readback; it never retries the rusqlite
`execute_batch` sequence that the probe proved can fail after partially applying `journal_mode=wal`.

Production enablement is feature-gated and requires full command/read differential conformance against
the SQLite relational reference, reopen/rebuild and cancellation evidence, and a focused CI job. The
existing broad GitHub Actions kind matrix will not gain a Turso dimension.

## Alternatives

| Option | Pros | Cons | Evaluation |
|--------|------|------|------------|
| redb | Stable Rust-native KV engine; synchronous fit | Reimplements SQL schema, indexes, joins, and every command arm | Rejected for first adapter |
| Fjall | Rust-native LSM; strong write profile | Similar port cost; explicit durability and compaction tuning | Retained as fallback, not selected |
| libSQL | High SQLite compatibility | C engine and async wrapper; does not meet Rust-native objective | Rejected |
| Keep bundled SQLite only | Proven and lowest risk | Does not meet Rust-native goal | Retained as baseline, not sole implementation |
| **Turso 0.7 local WAL projection** | Rust-native SQL; probe preserves current schema/query approach | Pre-1.0 compatibility and cold-build cost | **Selected with feature and conformance gates** |

## Consequences

| Type | Impact |
|------|--------|
| Positive | pqueue gains a Rust-native SQL projection without rewriting its relational model as KV structures. |
| Positive | The adapter exercises ADR-015 with a genuinely native-async store. |
| Negative | Turso is pre-1.0 and its compatibility surface must be re-probed on every upgrade. |
| Negative | Cold builds are materially larger; Turso stays out of the default multi-kind matrix. |
| Neutral | Bundled SQLite remains the reference and rollback projection until Turso clears every gate. |

## Risks

| Risk | Prob | Impact | Mitigation |
|------|------|--------|------------|
| Unprobed SQL diverges in one of the full command arms | M | H | Differential `ProjectionImage` and read-surface suite across the complete command corpus. |
| Cursor advances ahead of materialized state | L | H | One immediate transaction; injected rollback and reopen checks. |
| Upgrade changes file or SQL behavior | M | H | Exact version pin and mandatory compatibility-probe rerun before upgrades. |
| Compile cost grows CI disproportionately | H | M | One path-focused job; no new matrix axis; production feature remains opt-in. |

## Validation

| Success Metric | Review Trigger |
|----------------|----------------|
| SQLite and Turso projections are equal after every supported command and reopen | Any image, query, cursor, lease, or index divergence. |
| Object-log + Turso passes backend conformance and cancellation cuts | Any accepted state not recoverable from the object log. |
| No reactor blocking under Turso load | Single-thread heartbeat stalls. |
| Turso version remains exactly the probed version | Dependency update or feature expansion. |

## Supersession

- **Supersedes**: ADR-006's statement that Rust-native replacement evaluation is out of scope, only for
  the object-log-derived projection. ADR-006's standalone SQLite authority remains unchanged.
- **Superseded by**: None.

## Concern Impact

- `technology-radar`: Turso is Adopt only behind the projection feature and conformance gates; upgrades
  return it to Assess until the probe passes.
- `resilience`: Turso state is disposable and rebuildable from the log; it never authorizes log retention
  by itself.

## References

- `docs/helix/00-discover/rust-native-embedded-projection-alternatives.md`
- `docs/helix/00-discover/turso-0.7-compatibility-probe-results.md`
- `docs/helix/02-design/technical-designs/TD-010-object-log-turso-projection.md`
