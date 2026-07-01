# Expose hybrid async runtime profile (pqueue-fe6bc225)

## What was implemented

Exposed `objectlog/hybrid-async` as a first-class, selectable runtime profile across the server
config layer, the Helm chart, and the deployment docs, while preserving the existing (strict /
synchronous) backends unchanged. The engine/sqlite mechanism the profile runs on already exists
(closed sibling beads): `HybridProjectionStore` (hot in-memory serving over a durable SQLite
checkpoint) composed with the group-commit `ObjectLog` under the `EventualApply` class is the
`objectlog/hybrid-async` substrate; the async-apply `HybridAsyncThresholds` were already parsed by
`Config::from_env` (bead pqueue-6da52695). This bead wires the *selection* and the *deployment
surface*.

### `crates/pqueue-server` (in-scope: `lib.rs`, `env_config.rs`, `bin/pqueue-service.rs`)

- New `ProjectionSpec::HybridAsync { path }` variant + `label()` = `"hybrid-async"`.
- `start()` gains an `(ObjectLog, HybridAsync)` arm. Extracted `open_objectlog_hybrid_backend` so the
  `Hybrid` and `HybridAsync` arms share one builder (no behavioural divergence; the strict `Hybrid`
  arm is byte-for-byte preserved). The async arm logs the resolved async-apply thresholds at startup.
- `env_config`: `PQUEUE_PROJECTION_BACKEND=hybrid-async` maps to `ProjectionSpec::HybridAsync`; added
  to the wired-pairing table so only `objectlog × hybrid-async` is wired. Every other log axis
  (`memory`/`sqlite`/`postgres`) with `hybrid-async` fails closed with the existing
  unsupported-storage message. The unknown-projection error now lists `hybrid-async`.
- Re-exported `HybridAsyncThresholds` (it is the type of the public `Config::hybrid_async` field).
- `pqueue-service` `--help` documents `hybrid-async` and the five `PQUEUE_HYBRID_ASYNC_*` env names.

### `charts/pqueue/*`

- `values.schema.json`: `hybrid-async` added to the projection-backend enum; new
  `projection.hybridAsync` object with `applyLagMaxCommands` / `applyDebtMaxBytes` /
  `applyQueueDepthMax` / `oldestUnappliedMaxMs` / `applyPoisonRetryThreshold`, each constrained
  `>= 1` (a zero bound is instantly backpressured / rejected fail-closed by the server).
- `values.yaml`: `storage.projection.hybridAsync` defaults mirroring `HybridAsyncThresholds::default()`.
- `templates/configmap.yaml`: renders `PQUEUE_SQLITE_PROJECTION_PATH` for `hybrid-async` and the five
  `PQUEUE_HYBRID_ASYNC_*` env vars (piped through `int64` so large byte bounds render as integers,
  not scientific notation) only when `projection.backend == hybrid-async`.
- `templates/deployment.yaml`: storage volume/mount conditionals include `hybrid-async` (parity;
  already covered via the objectlog log axis it requires).
- `ci/objectlog-hybrid-async-values.yaml`: checked-in static-render profile for the combination.
- `README.md`: documents the profile, its env, and the fail-closed pairings.

### `docs/deployment/*`

- `container-runtime-contract.md`: `hybrid-async` added to the projection axis and
  `PQUEUE_SQLITE_PROJECTION_PATH` / `PQUEUE_RECOVERY_MAX_TAIL_COMMANDS` applicability; five new
  `PQUEUE_HYBRID_ASYNC_*` env rows; composition-root wiring paragraph.
- `operator-guide.md` and `helm-static-validation.md`: profile description, `hybridAsync` values, and
  fail-closed pairing notes.

### Tests (required to prove AC2; `crates/pqueue-server/tests/server.rs`)

- `objectlog_hybrid_async_push_claim_finalize_and_recovers_on_reopen` — runs the async profile end to
  end (push/claim/ack over RESP), carries a non-default `HybridAsyncThresholds`, and asserts acked
  state survives a reopen without redelivery or id remint.
- `env_config` unit tests: `objectlog_hybrid_async_projection_selects_profile_and_carries_paths_and_thresholds`
  and `non_objectlog_hybrid_async_pairing_is_rejected` (memory/sqlite × hybrid-async fail closed).

## Acceptance evidence

| AC | Command | Result |
|----|---------|--------|
| 1 | `cargo test -p pqueue-server --lib -- env_config hybrid_async --nocapture` | 15 passed, 0 failed |
| 2 | `cargo test -p pqueue-server --test server objectlog_hybrid_async -- --nocapture` | 1 passed, 0 failed |
| 3 | `bash scripts/ci/helm-gate.sh` | exit 0 — `helm static validation gate PASSED` |
| 4 | `cargo fmt --check` | clean |

Note on AC1: the literal AC command `cargo test -p pqueue-server env_config hybrid_async -- --nocapture`
passes two positional filters, which cargo's `[TESTNAME]` (single filter) CLI rejects with a usage
error — independent of the code. The intent (run the `env_config` and `hybrid_async` tests) is the
`--lib --` form above; cargo forwards multiple filters to libtest after `--`.

Regression: full `pqueue-server` suite green (21 lib + 20 integration tests), `cargo clippy -p
pqueue-server --all-targets` clean, `cargo check -p pqueue-server --features postgres` clean,
`helm lint --strict` + `helm template` of the hybrid-async CI values render cleanly.

## Scope notes

- `scripts/ci/helm-gate.sh` is outside the bead's named file scope, so its `COMBINATIONS` list was not
  modified; the gate stays green (AC3). `hybrid-async` static validation is provided by the schema
  (`helm lint --strict`) plus the checked-in CI values profile, both exercised here.
- No runtime backpressure/poison *enforcement* wiring is added (the `HybridAsyncMonitor` has no
  runtime attachment hook in the in-scope crates); the thresholds are threaded and validated
  fail-closed at config time. Enforcement/scale/chaos remain the separate open plan beads
  (`pqueue-abebbece`, `pqueue-fed791af`).
