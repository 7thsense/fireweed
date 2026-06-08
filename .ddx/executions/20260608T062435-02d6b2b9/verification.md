# B-001 Verification Report

## Acceptance Mapping

- `cargo fmt --all --check` passes.
- `cargo clippy --workspace --all-targets -- -D warnings` passes.
- `cargo test --workspace` passes.
- `pqueue-core` has no `pqueue` crate dependencies.
- `#![forbid(unsafe_code)]` is enforced in all initial crates.
- The workspace and CI foundation are explicit and pinned to Rust 1.92.0.

## Evidence

- Workspace members and dependency flow:
  - `Cargo.toml`
  - `crates/pqueue-core/Cargo.toml`
  - `crates/pqueue-storage/Cargo.toml`
  - `crates/pqueue-postgres/Cargo.toml`
  - `crates/pqueue-service/Cargo.toml`
  - `crates/pqueue-client/Cargo.toml`
- Unsafe denial:
  - `crates/pqueue-core/src/lib.rs`
  - `crates/pqueue-storage/src/lib.rs`
  - `crates/pqueue-postgres/src/lib.rs`
  - `crates/pqueue-service/src/lib.rs`
  - `crates/pqueue-client/src/lib.rs`
- Toolchain pin:
  - `rust-toolchain.toml`
- CI scaffold:
  - `.github/workflows/ci.yml`

## Commands

- `cargo fmt --all --check` -> exit 0
- `cargo clippy --workspace --all-targets -- -D warnings` -> exit 0
- `cargo test --workspace` -> exit 0
- `cargo tree -p pqueue-core -e normal` -> output contains only `pqueue-core`

