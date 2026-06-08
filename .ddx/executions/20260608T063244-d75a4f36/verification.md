# B-001 verification

## Scope

Workspace and CI foundation for TP-003 §5 per-PR gates.

## Changes

- Added workspace license metadata so `cargo deny check` can run cleanly.
- Added `deny.toml` for cargo-deny policy.
- Added scaffold code in each crate so path dependencies are intentionally used and `cargo machete` stays clean.
- Added placeholder property, fuzz, and flaky-harness tests that run under `cargo test --workspace`.
- Expanded `.github/workflows/ci.yml` to run fmt, clippy, test, cargo-deny, cargo-machete, and a coverage scaffold step with `cargo llvm-cov`.

## Validation

- `cargo fmt --all --check`
- `cargo test --workspace`
- `cargo deny check`
- `cargo machete`
- `mkdir -p target/llvm-cov && cargo llvm-cov --workspace --lcov --summary-only --output-path target/llvm-cov/lcov.info --fail-under-lines 0`

## Notes

- `cargo llvm-cov` requires the output directory to exist before writing the LCOV file, so the CI step now creates `target/llvm-cov/` first.
- The property, fuzz, and flaky harness items are scaffolding placeholders only; they exist so CI can execute the lanes now, before real product logic lands.
