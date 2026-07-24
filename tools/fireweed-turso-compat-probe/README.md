# Turso compatibility probe

This is a disposable, opt-in compatibility probe for pqueue's rebuildable relational projection. It is a
standalone nested Cargo workspace so Turso and its dependency graph are not part of pqueue's root workspace,
default tests, production features, or adapter selection.

Run the pinned probe from the repository root:

```bash
rustup run 1.92.0 cargo run \
  --locked \
  --manifest-path tools/fireweed-turso-compat-probe/Cargo.toml
```

The executable reads `RELATIONAL_SCHEMA` directly from the current `fireweed-sqlite` source at compile time.
It uses ordinary WAL mode; Turso sync and experimental MVCC are disabled. The concurrency cases assert
correctness only and make no throughput or latency claim.
