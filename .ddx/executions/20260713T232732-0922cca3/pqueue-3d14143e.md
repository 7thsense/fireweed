# pqueue-3d14143e Execution Report

## Scope

Add regression coverage for the reclaimed-index fence surviving reopen and being reloaded before seal.

## Changes

- Added `TestPermanentFenceSurvivesReopen`.
- Added `TestReopenFenceReloadsBeforeSeal`.
- Both tests verify the durable reclaimed-index watermark survives cache removal and that a stale writer still fences before ack.

## Verification

- `rustup run 1.92.0 cargo test -p pqueue-objectlog --test segmented_s3_substrate_tests TestPermanentFenceSurvivesReopen -- --nocapture`
- `rustup run 1.92.0 cargo test -p pqueue-objectlog --test segmented_s3_substrate_tests TestReopenFenceReloadsBeforeSeal -- --nocapture`
- `rustup run 1.92.0 cargo fmt --all --check`
- `rustup run 1.92.0 cargo clippy --workspace --all-targets -- -D warnings`
- `rustup run 1.92.0 cargo test --workspace`
- `go test ./...` -> not applicable: repo has no `go.mod` / Go module
- `lefthook run pre-commit` -> operator-required gate failure: no lefthook config found in repo

