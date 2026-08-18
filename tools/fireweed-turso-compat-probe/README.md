# Turso compatibility probe

This is a disposable, opt-in compatibility probe for fireweed's rebuildable relational projection. It is a
standalone nested Cargo workspace so Turso and its dependency graph are not part of fireweed's root workspace,
default tests, production features, or adapter selection.

Run the pinned probe from the repository root:

```bash
rustup run 1.97.1 cargo run \
  --locked \
  --manifest-path tools/fireweed-turso-compat-probe/Cargo.toml
```

The executable reads `RELATIONAL_SCHEMA` directly from `crates/fireweed-relational/src/schema.rs` at
compile time. It uses ordinary WAL mode; Turso sync and experimental MVCC are disabled. The concurrency
cases assert correctness only and make no throughput or latency claim.

## Reader while writer (Class L gate)

Before Class L plan-reads can leave the Turso writer mutex, this probe records whether a second
`database.connect()` connection can `SELECT` while connection A holds `BEGIN IMMEDIATE`:

- `turso.reader_while_writer.file=pass|fail ...`
- `turso.reader_while_writer.memory=pass|fail ...`
- `turso.wal_truncate_with_reader_open.file=pass|fail ...`
- `turso.drop_open_txn.file=pass|fail ...`

`pass` on read-while-write means B returned the pre-txn row without waiting for A. `fail select_blocked`
means Class L plan-reads must wait for the current writer txn (bounded), and still must not start their
own write txn.

```bash
cargo test --manifest-path tools/fireweed-turso-compat-probe/Cargo.toml --offline \
  -- reader_while_writer
```
