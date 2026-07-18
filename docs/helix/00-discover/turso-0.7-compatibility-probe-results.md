---
ddx:
  id: discover-turso-0-7-compatibility-probe-results
  depends_on:
    - discover-rust-native-embedded-projection-alternatives
    - adr-embedded-engine-integration-and-public-surface
  links:
    - {kind: informed_by, to: discover-rust-native-embedded-projection-alternatives}
    - {kind: informed_by, to: adr-embedded-engine-integration-and-public-surface}
  review:
    self_hash: d1060000d2e85a38d997e55afc7daa69a63567cc99a515140084552a30eca08a
    deps:
      adr-embedded-engine-integration-and-public-surface: e06dc6a96cdcd7293b5ba67e9c17d387cd2bd51c14daef13287bdf62a9e3951e
      discover-rust-native-embedded-projection-alternatives: 09abb88848c53782d2e9b2664714d0a4c7081b698c73da75349492f8fac714ca
    reviewed_at: "2026-07-18T02:36:05Z"
---

# Turso 0.7 Compatibility Probe Results

- **Observed**: 2026-07-17 (America/New_York)
- **Work item**: `pqueue-8b488429`
- **Governing decision**: ADR-006
- **Candidate**: `turso = 0.7.0`, default features disabled
- **Baseline**: `rusqlite = 0.32.1`, bundled SQLite
- **Toolchain**: Rust 1.92.0
- **Decision**: **No-go under the synchronous storage ports; reopened by ADR-015/ADR-016**

## Scope and Command

The probe covers the pod-local, rebuildable relational projection. It does not evaluate Turso as the
standalone durable log authority, enable Turso sync or experimental MVCC, change production dependencies,
or select a different backend. It did not run Niflheim or quiet-host tests.

Run it from the repository root:

```bash
rustup run 1.92.0 cargo run \
  --locked \
  --manifest-path tools/turso-compat-probe/Cargo.toml
```

The tool is a nested Cargo workspace with its own lockfile. It is not a member of the root workspace and no
default GitHub Actions workflow invokes it. Its `include_str!` reads
`crates/pqueue-sqlite/src/relational/helpers.rs` and extracts the `RELATIONAL_SCHEMA` raw string at compile
time, so the schema check cannot silently substitute a reduced copy.

## Observed Results

| Behavior | Turso 0.7.0 | rusqlite 0.32.1 | Implication |
|---|---|---|---|
| Exact current `RELATIONAL_SCHEMA`, including partial indexes | Pass | Pass | The current DDL is accepted unchanged. |
| Exact `open_inner` PRAGMA `execute_batch` | `Misuse("unexpected row during execution")`; immediate readback `journal_mode=wal` | Pass | Turso's `execute_batch` rejects the row produced by `journal_mode` after applying that partial side effect. |
| `pragma_update("journal_mode", "WAL")` | Pass, one result row | N/A | This is a mechanical Turso API adaptation, not a SQL rewrite. |
| Supported configuration readback | `journal_mode=wal`, `synchronous=1`, `busy_timeout=5000` | Pass | Every configured value is asserted after the Turso-specific API calls. |
| Queue/cursor creation plus batched lifecycle transition | Pass | Pass | Three inserts, item A's lease fields, its typed index, and `next_seq=4` committed together. |
| Priority/FIFO eligible query | `item-b`, `item-c` | `item-b`, `item-c` | Leased item A was excluded and ordered eligibility matched byte-for-byte. |
| Blocked-gate anti-join, CTE, group-summary UPSERT, typed-index range | Pass | Existing production baseline; not independently re-probed here | Turso accepted the representative difficult SQL classes; cross-engine state parity is asserted separately below. |
| Injected item-plus-cursor rollback | Pass | Pass | The item was absent and `next_seq` remained `4`. |
| Close and reopen | Full state equal to pre-close state | Full state equal to pre-close state | Item count, lease fields, typed index, cursor, and eligible order survived in both engines. |
| Cross-engine reopened state | Equal | Equal | Both returned 3 items, cursor 4, eligible `[item-b,item-c]`, index `[item-a]`, and identical item-A lease state. |
| `wal_checkpoint(PASSIVE)` result shape | Pass: three integers, observed `(0,82,82)` | Existing baseline | The current checkpoint reader shape is available. Frame counts are run-specific. |
| Sixteen disjoint writers | Pass: 16 tasks, 16 distinct committed IDs | Not used as a performance baseline | Every spawned task and every distinct row is asserted. All completed on their first application retry in this run. |
| Active client-key conflict | `same-key-a` persisted; `same-key-b` received typed constraint | Same winner and constraint class | The partial active-key index behaved deterministically. |

The exact Turso PRAGMA failure is load-bearing evidence. Immediately after `execute_batch` returns the
`Misuse`, the probe queries `PRAGMA journal_mode` and requires `wal`. Calling `execute_batch` therefore
reports failure after a proven partial side effect; retrying the whole initialization batch is unsafe. A
Turso-specific implementation would have to call `pragma_update`, consume its row, apply the remaining
settings, and assert the resulting `wal`, synchronous `1`, and busy timeout `5000` values.

The representative projection transaction creates the queue and cursor, inserts three pending items, adds
item A's typed-index row, updates item A to `Leased` with token hash `A1B2`, expiry `1000`, worker
`worker-a`, retry count `1`, item version `2`, and last command sequence `4`, then advances the cursor to
`4`. All operations share one immediate transaction. After reopen, both engines reconstruct a
`ProjectionState` containing item count, cursor, ordered eligible IDs, typed-index IDs, and every listed
lease field. Each engine must equal its own pre-close state, then Turso and rusqlite must equal each other.

## Concurrency Assertions

The disjoint-writer case creates 16 Tokio tasks and holds them at a 17-party barrier (16 tasks plus the
coordinator). Each task opens its own Turso connection, starts an immediate transaction, inserts its unique
`writer_id`, and commits. The run fails unless:

1. all 16 task joins return success;
2. the result vector contains 16 entries;
3. the table contains 16 rows; and
4. `COUNT(DISTINCT writer_id)` is 16.

The observed attempt vector was:

```text
[1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]
```

This proves the correctness outcome only. It is not evidence of parallel execution or a performance claim.
Ordinary WAL mode may serialize the work internally.

The same-key case uses the production schema's partial unique index, not a probe-only uniqueness table.
Writer A opens an immediate transaction and inserts `same-key-a` with active client key `same-key`. Writer B
is spawned and signals readiness while A still owns the transaction; A commits after a short scheduling
window. Writer B must then fail its `same-key-b` insert with Turso's typed `Constraint` error. The probe
finally queries the production table and requires `same-key-a` to be the sole active row. rusqlite is checked
against the same item inserts and produces the same deterministic winner.

## Build and CI Cost

The standalone lockfile resolved 264 packages on Rust 1.92. The first local debug build, including the pinned
rusqlite comparison, took 3 minutes 47 seconds on this host. These are build observations, not a benchmark.
They reinforce keeping the probe opt-in and outside the default root workspace and Actions matrix.

## Decision

The SQL result is promising but the governing probe stop rule makes the adapter decision **no-go under the
current pqueue ports**. This is not a claim that a Turso adapter is intrinsically impossible. Turso's Rust
API builds the database asynchronously and makes SQL preparation, queries, transactions, commits, and
rollbacks async. pqueue's `LogStore` and `ProjectionStore` operations and the closure passed to
`Backend::write` are synchronous inside the atomic unit-of-work lock. Under that current boundary, an
adapter would require either:

- changing the storage axes and atomic closure to async across the workspace; or
- introducing a blocking database actor and serial request/response handoff.

The first exceeds this probe and ADR-006's stop rule. The second discards the native-async/concurrent-write
reason for selecting Turso and adds another scheduling and failure boundary. Neither is authorized here.
Bundled SQLite through rusqlite remains the production baseline and adapter selection is unchanged.

A future decision may reopen the candidate only if pqueue independently adopts async storage ports or Turso
ships a supported synchronous Rust API. Any Turso version change must rerun this exact compatibility probe.

ADR-015 subsequently selected full-async storage boundaries and ADR-016 selected Turso as the first
Rust-native derived projection. Those decisions satisfy the first reopening condition without changing
this probe's dated evidence. Production status still requires TD-010's full command/read differential,
cancellation, recovery, and server conformance; the bounded probe alone is not that evidence.
