# Execution Report

## Change
- Centralized change-record queue filtering so opted-out queues are skipped at emitter startup and on each tick.
- Added focused tests proving opted-out queues are not emitted and do not advance the cursor.
- Added Go test wrappers so the acceptance command exercises the new Rust tests.

## Validation
- `cargo fmt --all`
- `go test ./... -run 'TestEmitChangeRecordTickSkipsOptedOutQueues|TestEmitChangeRecordTickDoesNotAdvanceCursorForOptOut'`

## Result
- The requested acceptance tests passed.
