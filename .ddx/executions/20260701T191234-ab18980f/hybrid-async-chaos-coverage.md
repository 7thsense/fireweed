# Hybrid-async crash/chaos coverage — bead pqueue-fed791af

Added crash/chaos test coverage for the `objectlog/hybrid-async` converged plan (parent pqueue-b207e65d;
TD-004). The hybrid-async success barrier is: object-log manifest commit (durable) → synchronous in-memory
apply/render (client-visible barrier) → asynchronous SQLite checkpoint that advances the LOGICAL high-water
off the hot path. Each crash window along that path is exercised and the recovery contract asserted.

## New / changed test files

| File | Tests | Level |
|------|-------|-------|
| `crates/pqueue-sqlite/tests/hybrid_async_chaos.rs` (new) | 10 | Unit — async checkpoint store + hybrid recovery + debt controller in isolation |
| `crates/pqueue-objectlog/tests/hybrid_async_chaos.rs` (new) | 5 | End-to-end over `ComposedBackend<ObjectLog, HybridProjectionStore, InProcessControlPlane>` |
| `crates/pqueue-server/tests/server.rs` (edited) | +2 | Full RESP server (`objectlog_hybrid_async_chaos_*`) |

## Crash windows covered

- Crash after object-log commit, before the async SQLite apply → recovery replays the un-checkpointed tail.
- Crash after in-memory apply, before the async SQLite apply → SQLite lags; resume replay at the prefix; the
  checkpointed request-id converges.
- Crash during the SQLite transaction, before the high-water advances → atomic abort; nothing applied; no
  orphaned in-flight lease.
- Crash after the high-water commit → re-delivered committed batch skipped idempotently (no duplicate item /
  lease).
- Crash before response delivery → durable request-id outcome converges on replay (no duplicate work minted).
- Disk-loss of the SQLite image → logical high-water resets to genesis; the durable object log rebuilds
  identical state.
- Disk-full / repeated apply failure → worker poisons, fails closed, never advertises a high-water past the
  poison.
- Async apply backlog / backpressure → debt controller gates new mutations and withholds the recovery
  skip-point until drained.
- request_id replay after each relevant window (push convergence path).

## Load-bearing invariants asserted

- No lost or duplicate leases (across crash/reopen, replay, and disk-loss).
- No orphaned in-flight (leased) records after a rolled-back claim/finalize; leases stay recoverable.
- No high-water advance past a poison (both the debt controller and the poisoned hybrid projection withhold
  the advertised skip-point).

Out of scope (per bead): the 10M-item performance run.

## Verification (all green)

```
cargo test -p pqueue-sqlite hybrid_async_chaos -- --nocapture            # 10 passed
cargo test -p pqueue-objectlog hybrid_async_chaos -- --nocapture         #  5 passed
cargo test -p pqueue-server --test server objectlog_hybrid_async -- --nocapture   # 3 passed
cargo test --workspace --all-features                                    # exit 0
cargo fmt --check                                                        # exit 0
cargo clippy -p pqueue-sqlite -p pqueue-objectlog -p pqueue-server --tests --all-features  # clean
```
