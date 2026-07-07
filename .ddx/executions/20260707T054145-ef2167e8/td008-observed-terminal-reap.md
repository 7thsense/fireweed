# TD-008 observed terminal reap

## Changed

- `crates/pqueue-projection/src/lib.rs`

## Verification

- `cargo test -p pqueue-projection` passed.
- `lefthook run pre-commit` passed with no local Lefthook config present.
- `cargo test --workspace` did not pass because `crates/pqueue/tests/product_validation_tests.rs` has three unrelated failing cases:
  - `callback_cohort_e2e`
  - `marketo_group_batching_e2e`
  - `noisy_neighbor_scale_e2e`

## Notes

- The new observed-run tests are `TestTD008ObservedTerminalReapFrontierRun` and `TestTD008ObservedTerminalReapNoPrematureDeletion`.
- The observed backend now commits push/claim/finalize, advances a durable emission cursor in stages, and reaps terminal items only after the cursor reaches the terminal `CommandPosition`.
