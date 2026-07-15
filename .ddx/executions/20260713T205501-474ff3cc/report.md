# Execution Report

## Scope
- `crates/pqueue-objectlog/src/segmented.rs`
- `crates/pqueue-objectlog/tests/segmented_s3_substrate_tests.rs`
- `crates/pqueue-conformance/tests/objectlog_segment_reclamation_tests.rs`

## Verification
- `rustup run 1.92.0 cargo fmt --all --check`
- `rustup run 1.92.0 cargo clippy --workspace --all-targets -- -D warnings`
- `rustup run 1.92.0 cargo test --workspace`
- `rustup run 1.92.0 cargo test -p pqueue-objectlog --test segmented_s3_substrate_tests TestPartialExpireDoesNotAdvanceDeletionWatermarkPastDeletedPrefix -- --nocapture`
- `rustup run 1.92.0 cargo test -p pqueue-objectlog --test segmented_s3_substrate_tests TestPartialExpireWatermarkDoesNotHideBelowFloorSegments -- --nocapture`
- `rustup run 1.92.0 cargo test -p pqueue-objectlog --test segmented_s3_substrate_tests`
- `rustup run 1.92.0 cargo test -p pqueue-conformance --test objectlog_segment_reclamation_tests`
- `go test ./...` -> no main module selected in this repo
- `lefthook run pre-commit` -> no lefthook config found in repo

## Notes
- Partial-expiry watermark advancement now respects the reclaimed/deleted prefix without using the watermark to hide unreclaimed below-floor entries.
- Legacy manifest copies retain reclaimed markers so cache-less bootstrap still reconstructs the durable horizon.
