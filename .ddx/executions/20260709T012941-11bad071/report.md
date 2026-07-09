# pqueue-86c8b0ee — Runtime-wire postgres/sqlite and postgres/postgres storage combos

## Change summary

- `crates/pqueue-server/src/lib.rs`:
  - Added `ProjectionSpec::Postgres { url }` (cfg-gated on `feature = "postgres"`) and its `label()` arm.
  - Added `resolve_postgres_projection(env)`, mirroring `resolve_postgres_log` (DSN from
    `PQUEUE_POSTGRES_PROJECTION_DATABASE_URL` / `PQUEUE_PG_PROJECTION_URL` fallback, fails closed on
    `sslmode=require` without the `tls` feature).
  - Added two `start()` match arms:
    - `(LogSpec::Postgres, ProjectionSpec::Sqlite)` → `ComposedBackend<PostgresLog, SqliteProjectionStore,
      InProcessControlPlane>`, connect + recover inside `spawn_blocking`, driven through `BlockingBackend`.
    - `(LogSpec::Postgres, ProjectionSpec::Postgres)` → `ComposedBackend<PostgresLog, PostgresRelational,
      InProcessControlPlane>` (two independent postgres connections, non-colliding table sets), same
      off-reactor wiring.
- `crates/pqueue-server/src/env_config.rs`:
  - Wired `PQUEUE_PROJECTION_BACKEND=postgres` (cfg-gated) to `resolve_postgres_projection`.
  - Added `(postgres, sqlite)` and `(postgres, postgres)` to the wired-pairing table.
- `crates/pqueue-server/tests/postgres_composed_projections.rs` (new): `postgres_sqlite_combo_runs_under_tokio`
  and `postgres_postgres_combo_runs_under_tokio` — boot `start()` over each combo under `#[tokio::test]`,
  drive push → claim → finalize over RESP with a stock redis client. Env-gated on `PQUEUE_PG_TEST_URL`
  (LOUD-skip otherwise).

## Acceptance verification

1. `postgres_sqlite_combo_runs_under_tokio` — ran live against `PQUEUE_PG_TEST_URL=postgres://pqueue:pqueue@127.0.0.1:55432/pqueue`: **ok**.
2. `postgres_postgres_combo_runs_under_tokio` — ran live against the same DB: **ok**.
3. `rg -n 'postgres.*sqlite|postgres.*postgres' crates/pqueue-server/src/env_config.rs` — matches the new
   wired-pairing comment (`postgres/sqlite, postgres/postgres`), confirming both combos are wired in the
   runtime selector.
4. `rustup run 1.92.0 cargo test -p pqueue-server --features postgres` — full crate suite green, including the
   two new live tests (run with `PQUEUE_PG_TEST_URL` set). `cargo clippy -p pqueue-server --features postgres
   --tests -- -D warnings` clean.

## Non-scope (per bead)

Live `kind` smoke and the TP-003 AC-TXN fault-injection matrix for these two combos are separate,
already-tracked work (bead `pqueue-52e1a2ff` / R3) and are not claimed here.
