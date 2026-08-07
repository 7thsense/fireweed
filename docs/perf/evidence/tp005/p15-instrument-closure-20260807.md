# P15 — TP-005 instrument, provider-neutral evidence, verifiers

- date: 2026-08-07
- bead: fireweed-02e212c6 (plan-key P15)
- outcome: instrument already present; remaining gaps closed in this revision

## Existing instrument (already on main)

| Surface | Path |
|---|---|
| Matrix runner | `crates/fireweed-bench/src/bin/fireweed-performance-matrix.rs` |
| Million-cycle gate | `crates/fireweed-bench/src/bin/fireweed-million-cycle.rs` |
| Library modules | `crates/fireweed-bench/src/performance_matrix*.rs` |
| Shell producer | `scripts/perf/fireweed-matrix.sh` |
| Shell verifier | `scripts/perf/verify-fireweed-matrix.sh` |
| Local million-cycle | `scripts/perf/run-million-cycle-local.sh` |
| Schema | `fireweed-performance-matrix-v1` (`performance_matrix_evidence.rs`) |
| Cell×barrier evidence | each cell record carries `id` + `barrier_class` |
| Smoke evidence | `docs/perf/evidence/tp005/smoke-20260806T215433Z.json` (+ sha256) |
| Host floors (r0) | `docs/perf/evidence/tp005/host-full-matrix-r0-floors.{md,json}` |
| Spec + review | `docs/helix/03-test/test-plans/TP-005-fireweed-performance-matrix.md`, `docs/helix/04-build/reviews/TP-005-performance-matrix-review.md` |

Semantic current ID (manifest): `CURRENT-TP005-PERFORMANCE-MATRIX` — promotion of
authoritative full-tier artifacts remains P17/P18.

Historical six TP-003 JSONL paths under `docs/perf/evidence/` are preserved
untouched (immutable historical corpus).

## Gaps closed this revision

1. **Weak-password denylist** — removed duplicate `"fireweed"`; kept unique
   `["fireweed", "postgres", "garage"]` with focused unit tests.
2. **Provider-neutral fixtures** — replaced `garage.invalid` / region `garage` /
   topology `garage-local-1` positive literals with neutral
   `s3-fixture.invalid` / `us-east-1` / `local-s3-compat-1` / `us-west-2`.
3. **Two `#[ignore]` live-S3 routes** — rehosted:
   - load-shape calibration → `run_e3_release_load_shape_calibration_suite`
     under `FIREWEED_E3_LOAD_SHAPE_CALIBRATION=1` in the release harness;
   - fence proofs → already owned by release branch of
     `performance_object_log_e3_live_tests` (`prove_native_create_only_fence`);
   - unit test `performance_e3_live_file_has_no_ignore_routes` fails closed if
     `#[ignore` reappears.
4. **Million-cycle local script** — wires `fireweed-million-cycle` binary
   (`--tier probe|production`, `--cell`, run-owned `--output`).

## Focused verifier commands (exit 0)

```sh
cargo test --manifest-path crates/fireweed-bench/Cargo.toml --lib \
  performance_matrix_services::tests -- --nocapture

cargo test -p fireweed-server --test performance_object_log_e3_live_tests \
  e3_s3_region_defaults_and_accepts_override \
  release_s3_profile_is_provider_neutral \
  performance_e3_live_file_has_no_ignore_routes \
  -- --exact --nocapture

# Evidence schema/verifier (smoke artifact already on tree):
cargo build --manifest-path crates/fireweed-bench/Cargo.toml \
  --bin fireweed-performance-matrix
crates/fireweed-bench/target/debug/fireweed-performance-matrix verify \
  docs/perf/evidence/tp005/smoke-20260806T215433Z.json

git diff --check
cargo fmt --all --check
```

Authoritative full-tier host measurement remains operator-local (forbidden in CI
per TP-005). Host r0 floors document 80 measured cells for this machine class.
