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
  status: superseded
  review:
    self_hash: 76ec5fe8523c4fe831441229aa5f09f0bf966ac3849174764a7ba2c2d805f22a
    deps:
      adr-full-async-storage-boundaries: 26d2c37c96eb0801dbb99e4a02213ecfa747aa533572acde3917801a13cebfcd
    reviewed_at: "2026-07-20T00:01:23Z"
---

# ADR-016: Turso is an internal Rust-native projection compatibility adapter

| Date | Status | Deciders | Related | Confidence |
|------|--------|----------|---------|------------|
| 2026-08-03 | Superseded as public product; retained experimentally | Project owner | ADR-006, ADR-012, ADR-015, TD-010 | High |

> The public-product decision in this ADR is superseded. `fireweed-turso` remains an internal,
> experimental compatibility adapter and validation lane; it is not a supported public projection
> selector or server profile. The canonical public projection selectors are exactly `memory`, `sqlite`,
> and `postgres`.

## Context

The Rust-native projection evaluation compared bundled SQLite, libSQL, redb, Turso Database, Fjall,
and sled. Turso 0.7 accepted the production relational schema, partial indexes, priority/FIFO query,
blocked-gate anti-join, typed-index range, cursor/item atomic transaction, rollback, reopen, concurrent
writers, and active-key conflict. Its no-go result was limited to the synchronous storage-port boundary:
the driver is native async and the current projection apply seam cannot await.

ADR-015 independently removes that boundary. The remaining useful result is the compatibility evidence
for an internal native-async adapter, not a fourth public projection family.

## Decision

Turso Database is retained in `fireweed-turso` as an internal, experimental local relational projection
adapter. It remains rebuildable when paired by tests with a durable object log; it is not an
authoritative log, control plane, remote Turso service, public projection kind, or replacement for the
standalone SQLite durable backend.

The adapter pins the probed `turso = 0.7.0` with default features disabled and uses ordinary WAL. It does
not enable experimental MVCC, sync/remote replication, FTS, or allocator features. Initialization uses
individual pragma operations with result consumption and readback; it never retries the rusqlite
`execute_batch` sequence that the probe proved can fail after partially applying `journal_mode=wal`.

Internal enablement is feature-gated and requires command/read differential conformance against the
SQLite relational reference, reopen/rebuild and cancellation evidence, and a focused validation job.
The public selector parser must reject `turso` whether or not that feature is compiled. The supported
server matrix and broad GitHub Actions kind matrix do not gain a Turso dimension.

## Alternatives

| Option | Pros | Cons | Evaluation |
|--------|------|------|------------|
| redb | Stable Rust-native KV engine; synchronous fit | Reimplements SQL schema, indexes, joins, and every command arm | Rejected for first adapter |
| Fjall | Rust-native LSM; strong write profile | Similar port cost; explicit durability and compaction tuning | Retained as fallback, not selected |
| libSQL | High SQLite compatibility | C engine and async wrapper; does not meet Rust-native objective | Rejected |
| Keep bundled SQLite only | Proven and lowest risk | Does not meet Rust-native goal | Retained as baseline, not sole implementation |
| **Turso 0.7 local WAL projection** | Rust-native SQL; probe preserves current schema/query approach | Pre-1.0 compatibility and cold-build cost | **Retained only as an internal experimental adapter** |

## Consequences

| Type | Impact |
|------|--------|
| Positive | Fireweed keeps a Rust-native SQL compatibility target without expanding the public product matrix. |
| Positive | The adapter exercises ADR-015 with a genuinely native-async store. |
| Negative | Turso is pre-1.0 and its compatibility surface must be re-probed on every upgrade. |
| Negative | Cold builds are materially larger; Turso stays out of the default multi-kind matrix. |
| Neutral | Bundled SQLite remains the relational reference; passing internal gates does not promote Turso into the public selector set. |

## Risks

| Risk | Prob | Impact | Mitigation |
|------|------|--------|------------|
| Unprobed SQL diverges in one of the full command arms | M | H | Differential `ProjectionImage` and read-surface suite across the complete command corpus. |
| Cursor advances ahead of materialized state | L | H | One immediate transaction; injected rollback and reopen checks. |
| Upgrade changes file or SQL behavior | M | H | Exact version pin and mandatory compatibility-probe rerun before upgrades. |
| Compile cost grows CI disproportionately | H | M | One path-focused job; no new matrix axis; the experimental feature remains internal. |

## Validation

| Success Metric | Review Trigger |
|----------------|----------------|
| SQLite and Turso projections are equal after every supported command and reopen | Any image, query, cursor, lease, or index divergence. |
| Internal object-log + Turso tests pass backend conformance and cancellation cuts | Any accepted state not recoverable from the object log. |
| Public configuration rejects `turso` in feature-disabled and feature-enabled builds | Any enabled public Turso profile or silent alias acceptance. |
| No reactor blocking under Turso load | Single-thread heartbeat stalls. |
| Turso version remains exactly the probed version | Dependency update or feature expansion. |

## Supersession

- **Supersedes**: ADR-006's statement that Rust-native replacement evaluation is out of scope, only for
  the object-log-derived projection. ADR-006's standalone SQLite authority remains unchanged.
- **Superseded by**: ADR-012 and the orthogonal public storage-product contract for all public selector
  and support-matrix decisions. This ADR remains as evidence for the internal adapter only.

## Concern Impact

- `technology-radar`: Turso remains Assess as an internal experiment; passing conformance gates does not
  make it a supported public projection.
- `resilience`: Turso state is disposable and rebuildable from the log; it never authorizes log retention
  by itself.

## References

- `docs/helix/00-discover/rust-native-embedded-projection-alternatives.md`
- `docs/helix/00-discover/turso-0.7-compatibility-probe-results.md`
- `docs/helix/02-design/technical-designs/TD-010-object-log-turso-projection.md`
