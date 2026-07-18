---
ddx:
  id: discover-rust-native-embedded-projection-alternatives
  depends_on:
    - adr-embedded-engine-integration-and-public-surface
  review:
    self_hash: fb1c9ed0c257673e96716a031b40056110c6ce6f043a858ac41f01718281e280
    deps:
      adr-embedded-engine-integration-and-public-surface: e18689f92ad1070a9d3e96253f41b6d0a3fe67eb9b6eb80f5df07ac24e56c7cc
    reviewed_at: "2026-07-18T01:27:05Z"
---

# Rust-Native Embedded Projection Alternatives

- **Decision date**: 2026-07-17
- **Decision owner**: pqueue storage
- **Governing decision**: ADR-006

## Scope

This report evaluates the pod-local, rebuildable projection paired with an authoritative object log. It
does not select a replacement for the standalone SQLite log-authority profile.

## Recommendation

Keep bundled SQLite through `rusqlite` as the production baseline. Test **Turso Database 0.7.x first**
with a bounded SQL-compatibility probe because it is the only evaluated alternative that is both a Rust
database implementation and capable of preserving the existing relational projection. If that probe
fails, test **redb 4.1.x** as the engine-shaped fallback. A redb port would replace the SQL projection
rather than substitute a driver.

This ordering is based on cost of information, not the raw weighted score. redb scores higher as a storage
engine, but the Turso probe can quickly determine whether pqueue can remove SQLite's C implementation
without rewriting the projection. No production dependency or backend switch is authorized by this
report.

libSQL is not the Rust rewrite of SQLite. It is a Turso-maintained fork of SQLite whose local engine remains
the SQLite C implementation. Its Rust crate is an async wrapper around that engine and its replication
features do not help a pod-local projection whose authority already lives in the object log. The separate
Turso Database project is a from-scratch Rust implementation. The projects' own repositories make this
distinction explicit ([libSQL repository](https://github.com/tursodatabase/libsql),
[Turso Database repository](https://github.com/tursodatabase/turso)).

## Local Constraints

ADR-006 sanctions bundled `rusqlite` for v1 while leaving a pure-Rust engine as a separate evaluation. It
also requires production embedders to retain a durable backend. For the object-log profile evaluated here,
the embedded database is a derived projection, not the acknowledgement authority. A lost projection suffix
may be replayed; a durable high-water ahead of the corresponding projection state is still forbidden.

The current port is not a generic SQL abstraction:

- `SqliteProjectionStore` holds one `Mutex<Connection>`, applies a sealed command batch in one transaction,
  and advances `relational_cursor` in that transaction. The current contract is documented in
  `crates/pqueue-sqlite/src/relational/projection.rs`.
- `ProjectionStore::apply` is synchronous and the composition serializes
  `select -> append -> apply -> render` under its unit-of-work lock. Native async I/O is therefore not an
  automatic benefit; it needs an explicit boundary design. See
  `crates/pqueue-engine/src/compose.rs`.
- The relational projection contains 15 application tables, partial and composite indexes, typed secondary
  indexes, and state-dependent queries. The five central SQL implementation files contain approximately
  5,945 lines and 163 SQL statement sites. See
  `crates/pqueue-sqlite/src/relational/{projection,apply,query,recovery,helpers}.rs`.
- Required behavior includes atomic batch apply and cursor advancement, replayed-prefix skipping, sequence
  gap rejection, deterministic priority/FIFO selection, client-key uniqueness, lease-expiry scans,
  secondary indexes, reopen, reset, and authoritative rebuild.

These constraints favor a single-writer transaction model. They also make SQL compatibility much more
valuable than an isolated key-value benchmark.

## Weighted Evaluation

Scores use a five-point scale. A five means the candidate directly satisfies the current requirement; a
one means a redesign or material unresolved risk. The weights reflect this projection, not general database
quality.

| Criterion | Weight | What it measures |
|---|---:|---|
| SQL compatibility and port cost | 25 | Reuse of the schema, apply logic, indexes, and query semantics |
| Transaction and ordered-query correctness | 20 | Atomic multi-structure writes, range scans, and deterministic ordering |
| API and concurrency fit | 15 | Fit with synchronous `ProjectionStore` and the serialized writer |
| Crash durability and recovery | 15 | Atomic recovery, configurable persistence, and repair behavior |
| Maintenance, stability, and license | 15 | Current releases, format/API promises, warnings, and license suitability |
| Rust implementation share | 10 | Whether the local engine, rather than only its binding, is implemented in Rust |

| Candidate | SQL / port 25 | Txn / query 20 | API fit 15 | Recovery 15 | Maintenance 15 | Rust 10 | Weighted / 5 |
|---|---:|---:|---:|---:|---:|---:|---:|
| Bundled SQLite via `rusqlite` | 5 | 5 | 5 | 5 | 5 | 1 | **4.60** |
| libSQL | 5 | 5 | 2 | 5 | 4 | 2 | **4.10** |
| redb 4.1.x | 1 | 5 | 5 | 4 | 4 | 5 | **3.70** |
| Turso Database 0.7.x | 4 | 4 | 2 | 3 | 3 | 5 | **3.50** |
| fjall 3.1.x | 1 | 5 | 4 | 3 | 4 | 5 | **3.40** |
| sled 0.34 / 1.0 alpha | 1 | 4 | 4 | 2 | 1 | 5 | **2.60** |

The baseline score explains why this is a probe rather than a migration decision. libSQL's score reflects
SQLite compatibility, but it does not satisfy the purpose of removing the C database engine. redb leads the
Rust-native engines, while Turso is tested first because its compatibility claim creates the cheapest
decision boundary.

## Candidate Findings

### Bundled SQLite through `rusqlite`

The workspace pins `rusqlite` 0.32 with `bundled`. The current upstream `rusqlite` documentation describes
the same architecture: `rusqlite` is a Rust wrapper, `libsqlite3-sys` supplies declarations for SQLite's C
API, and `bundled` compiles and links vendored SQLite. `rusqlite` is MIT; bundled SQLite is public domain
([official repository](https://github.com/rusqlite/rusqlite)).

It is the only candidate already proven by pqueue's relational, reconnect, durability, fault-injection, and
hybrid recovery tests. Its synchronous connection and explicit transaction APIs match the current port.
The drawback is implementation provenance and build surface, not a demonstrated correctness gap.

### libSQL

libSQL is a fork of SQLite, not its Rust replacement. The official repository reports that it inherits
SQLite's single-writer model and directs new feature development toward Turso Database. GitHub's language
breakdown was approximately 85.7% C and 6.8% Rust when inspected. The `libsql` Rust crate describes itself
as a batteries-included async wrapper around the SQLite C API; its default local `core` feature includes the
C engine ([repository](https://github.com/tursodatabase/libsql),
[Rust API 0.9.30](https://docs.rs/libsql/latest/libsql/)).

Its async API is a worse direct fit than `rusqlite` for the synchronous projection port. Embedded replicas,
remote access, and replication add capabilities that duplicate rather than simplify pqueue's object-log
authority. libSQL is MIT, but it does not meet the Rust-native objective and offers no compensating local
projection advantage.

### Turso Database 0.7.x

Turso Database is an in-process SQL engine implemented in Rust and licensed MIT. Its Rust API opens and
queries a local database asynchronously, and the project advertises SQLite SQL and file-format
compatibility plus Linux `io_uring` support. The project has not reached 1.0 and recommends independent
backups while compatibility work continues
([repository and status](https://github.com/tursodatabase/turso)).

The current manual makes WAL the default journal mode and documents deferred, immediate, and concurrent
transactions. It also records material limits: compatibility is incomplete; some concurrency features are
experimental; the experimental MVCC mode cannot use indexes and warns that queries may be incorrect. The
probe must use ordinary WAL mode, not experimental MVCC
([manual](https://github.com/tursodatabase/turso/blob/main/docs/manual.md)).

Turso preserves the possibility of keeping SQL, partial indexes, joins, and state-dependent predicates.
The major integration risk is the async API: `ProjectionStore::apply` cannot `.await`, and blocking a Tokio
worker or owning a nested runtime would be an architectural regression. The probe must settle both SQL
behavior and the execution boundary before any adapter work begins.

### redb 4.1.x

redb is a pure-Rust, copy-on-write B+tree store with ACID transactions, MVCC readers, one writer, ordered
range iteration, and MIT-or-Apache-2.0 licensing. The project calls the API and file format stable and
maintained. Its synchronous API and single writer match the current port
([repository](https://github.com/cberner/redb),
[range API](https://docs.rs/redb/latest/redb/trait.ReadableTable.html)).

A write transaction can update multiple tables and either commit or abort. redb is crash-safe, but default
unclean-open recovery may walk the full database; quick repair trades slower commits for near-immediate
recovery ([write transaction API](https://docs.rs/redb/latest/redb/struct.WriteTransaction.html)). For a
rebuildable projection, default repair may be acceptable only if measured against the pod restart budget.

Port cost is high. pqueue would need versioned composite key encodings and explicit tables for items,
client keys, eligibility order, lease expiry, gates, cohorts, typed indexes, summaries, and cursors. Every
command arm must remove stale index keys and insert new ones in the same transaction before advancing the
cursor. The 4.1 changelog also follows recent data-loss and corruption-class fixes, so a prototype must pin
4.1 or newer and include kill/reopen validation
([changelog](https://docs.rs/crate/redb/latest/source/CHANGELOG.md)).

### fjall 3.1.x

Fjall is a safe-Rust LSM engine with lexicographically ordered range and prefix iteration, multiple
keyspaces, cross-keyspace atomicity, optional serializable transactions, automatic compaction, a stable
disk-format policy, and MIT-or-Apache-2.0 licensing. Its single-writer transactional mode fits pqueue's
serialized writer; its LSM layout may favor sustained batch updates
([repository](https://github.com/fjall-rs/fjall),
[transaction API](https://docs.rs/fjall/latest/fjall/struct.SingleWriterWriteTx.html)).

Durability is deliberately explicit. Operations reach operating-system buffers by default; callers select
a persistence mode when disk synchronization is required. That can be useful for a replayable projection,
but only after kill and power-loss behavior proves that a recovered cursor cannot lead recovered item and
index state. Fjall has no native async API; its official Tokio example uses `spawn_blocking`.

Port cost is comparable to redb, with additional LSM compaction and cache tuning. Fjall is the fallback if
the redb probe shows unacceptable copy-on-write amplification or restart cost, not a simultaneous first
probe.

### sled

sled supplies byte-key ranges and optimistic multi-tree transactions, but the stable 0.34.7 release dates
to 2021. The project repository calls sled beta, tells reliability-first users to use SQLite, warns that the
on-disk format requires manual migration before 1.0, and describes a storage-subsystem rewrite. A 1.0 alpha
does not remove those production-readiness risks. The license is MIT-or-Apache-2.0
([official repository](https://github.com/spacejam/sled)).

Optimistic transaction closures may be retried, adding a semantic constraint to the already complex apply
path. sled is excluded from a production probe until the project declares a stable format and removes its
own reliability warning.

## Exact Bounded Probe

The first probe is a standalone Turso 0.7.x compatibility test. It does not modify production dependencies,
feature flags, or backend selection.

1. Open a temporary local database in ordinary WAL mode through Turso's Rust API.
2. Execute the existing `RELATIONAL_SCHEMA` unchanged.
3. In one immediate transaction, exercise representative SQL from each difficult class:
   - create a queue and cursor;
   - insert a pending item and enforce the partial active-client-key uniqueness rule;
   - update lifecycle and lease fields while advancing the cursor;
   - run the priority/FIFO eligible query;
   - run the blocked-gate anti-join;
   - insert and range-query a typed secondary-index key;
   - execute an upsert and a representative CTE used by apply or recovery.
4. Close and reopen the file, then assert that the item, indexes, and cursor agree.
5. Roll back an injected mid-batch failure and assert that neither projection rows nor cursor advanced.
6. Demonstrate one acceptable runtime boundary: either a synchronous Turso API usable under the current
   unit-of-work lock, or a dedicated blocking/storage thread with request/response handoff. A nested Tokio
   runtime or `block_on` on a Tokio worker is a probe failure.

**Pass**: every SQL statement runs unchanged or with mechanical API-only adaptation; ordering, constraints,
rollback, and reopen match SQLite; and the runtime boundary does not change `ProjectionStore` or block a
Tokio worker.

**Fail**: unsupported schema/query semantics, different ordering or constraint behavior, an unstable WAL
reopen, or a required async domain-port refactor. On failure, preserve the evidence and run a separate redb
prototype limited to `meta/cursor`, `items`, `active_by_client_key`, `eligible_by_priority`, and
`leases_by_expiry`, supporting only Push, Lease, Finalize, and Reclaim.

## Risks and Stop Rules

- **Compatibility drift**: Turso's SQLite compatibility is not complete. Pin the tested 0.7.x version and
  treat upgrades as conformance events.
- **Async boundary distortion**: native async I/O does not justify changing the public projection contract
  in this evaluation. Stop if the adapter requires nested-runtime blocking or a broad trait refactor.
- **Silent index divergence**: a KV adapter can atomically commit the wrong index maintenance. It must be
  checked against the in-memory reference after every supported transition before its scope expands.
- **Recovery latency**: redb repair and Fjall journal/compaction recovery must be measured with realistic
  resident counts. A replayable projection still has a pod readiness budget.
- **False durability equivalence**: this report applies to the object-log-derived projection. Replacing the
  standalone SQLite log authority requires a separate durable-ack evaluation and power-loss evidence.
- **Scope growth**: do not port all command arms, cohorts, gates, summaries, or production wiring until a
  bounded probe demonstrates a material benefit over bundled SQLite.

## Source Ledger

All external evidence is primary project documentation inspected on 2026-07-17.

| Source | Version/date visible | Claim supported |
|---|---|---|
| [rusqlite repository](https://github.com/rusqlite/rusqlite) | upstream 0.39 documentation | Rust wrapper, bundled C SQLite, license |
| [libSQL repository](https://github.com/tursodatabase/libsql) | inspected 2026-07-17 | SQLite fork, language share, maintenance direction, license |
| [libSQL Rust API](https://docs.rs/libsql/latest/libsql/) | 0.9.30 | Async Rust wrapper around SQLite C API and local feature shape |
| [Turso Database repository](https://github.com/tursodatabase/turso) | pre-1.0 status, inspected 2026-07-17 | Rust implementation, async API, maintenance, license, compatibility status |
| [Turso Database manual](https://github.com/tursodatabase/turso/blob/main/docs/manual.md) | inspected 2026-07-17 | WAL and transaction modes, limitations, durability design |
| [redb repository](https://github.com/cberner/redb) | 4.1.0, 2026-04-19 | Rust implementation, stability, transactions, MVCC, license |
| [redb API and changelog](https://docs.rs/redb/latest/redb/) | 4.1.0 | ranges, write durability, repair behavior, recent correctness fixes |
| [Fjall repository and API](https://github.com/fjall-rs/fjall) | 3.1.7, 2026-07-17 | Rust LSM design, ranges, transactions, durability, format policy, license |
| [sled repository](https://github.com/spacejam/sled) | stable 0.34.7, 2021-09-12 | beta warning, durability behavior, format risk, license |
