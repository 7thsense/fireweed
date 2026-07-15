# Manifest watermark focused runs

Bead: `pqueue-e2871b01`
Bundle: `.ddx/executions/20260713T213939-84ca8499`

## Command results

### Rust toolchain invocation checks

- `rustup 1.92.0 cargo test -p pqueue-objectlog --test segmented_s3_substrate_tests read_horizon_bounds_enumeration_to_live_and_is_monotonic -- --nocapture`
  - Failed immediately with `error: unrecognized subcommand 'cargo'`.
- `cargo +1.92.0 test -p pqueue-objectlog --test segmented_s3_substrate_tests read_horizon_bounds_enumeration_to_live_and_is_monotonic -- --nocapture`
  - Failed immediately with `error: no such command: '+1.92.0'`.
- `rustup run 1.92.0 cargo test -p pqueue-objectlog --test segmented_s3_substrate_tests read_horizon_bounds_enumeration_to_live_and_is_monotonic -- --nocapture`
  - Failed.
  - Failure site: `crates/pqueue-objectlog/tests/segmented_s3_substrate_tests.rs:3017`.
  - Panic: `horizon after trim 1`.

- `rustup run 1.92.0 cargo test -p pqueue-objectlog --test segmented_s3_substrate_tests partial_expire_does_not_hide_undeleted_below_floor_segments -- --nocapture`
  - Passed.

- `rustup run 1.92.0 cargo test -p pqueue-objectlog --test segmented_s3_substrate_tests TestManifestDeletionWatermarkFailClosedBelowFloor -- --nocapture`
  - Failed.
  - Failure site: `crates/pqueue-objectlog/tests/segmented_s3_substrate_tests.rs:3133`.
  - Panic: `assertion left == right failed`, left `None`, right `Some(0)`.

### Go gate

- `go test ./...`
  - Not applicable for this checkout.
  - Evidence: no `*.go` files in the repo root checkout, and the command failed with `pattern ./...: directory prefix . does not contain main module or its selected dependencies`.

### Lefthook gate

- `lefthook run pre-commit`
  - Executed.
  - Result: operator-required gate failure because no config files were present in the checkout.
  - Output: `No config files with names ["lefthook" ".lefthook" ".config/lefthook"] have been found in ".../pqueue-e2871b01-20260713T213939-84ca8499"`.

## Notes

- The exact `TestManifestWatermarkFailClosedBelowFloor` and `TestManifestWatermarkPartialExpireVisibility` symbols do not exist as Rust test functions in this checkout. The closest existing coverage exercised for this bead was the focused `segmented_s3_substrate_tests` surface above.
