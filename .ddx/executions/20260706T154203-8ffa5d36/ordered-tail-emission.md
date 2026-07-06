# Ordered Tail Emission Evidence

## Scope

- Updated the `ordered_tests` module in `crates/pqueue-engine/src/compose.rs`.
- Kept the production `emit_change_record_tail` flow unchanged.

## Behavior Covered

- `TestChangeRecordTailIsEmittedInCommandPositionOrder` emits a three-record batch and verifies the batch preserves `CommandPosition` order.
- `TestChangeRecordCursorAdvancesOnlyAfterSuccessfulEmit` forces the sink to fail first, verifies the cursor stays unset, then retries successfully and verifies the cursor advances only after the successful emit.

## Verification

- `cargo test -p pqueue-engine --lib TestChangeRecord -- --nocapture`
- `go test ./... -run 'TestChangeRecordTailIsEmittedInCommandPositionOrder|TestChangeRecordCursorAdvancesOnlyAfterSuccessfulEmit'`
- `lefthook run pre-commit`
- `lefthook run pre-push`

## Notes

- `lefthook` reported no config files in this worktree, so the hook commands exited cleanly without running repo-local hooks.
