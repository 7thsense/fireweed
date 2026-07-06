## Verification

Applied fixes:
- Removed clippy-triggering return bindings and clone/slice patterns in `crates/pqueue-engine/src/compose.rs` and `crates/pqueue-server/src/change_record_sink.rs`.
- Cleaned clippy warnings in `crates/pqueue-server/tests/fjord_surface.rs`.

Verified surfaces:
- `crates/pqueue-server/tests/server.rs` still compiles against `ChangeRecord::idempotency_key()` and the targeted sink test passes.
- `crates/pqueue-projection/src/lib.rs` keeps `emit_change_records: true` explicit in the test helper constructor.

Verified commands:
- `cargo fmt --check`
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- `cargo test -p pqueue-server change_record_sink_delivers -- --exact`
- `cargo check -p pqueue-projection --tests`
- `cargo check -p pqueue-server --tests`
- `go test ./...`

Notes:
- The `go test ./...` run was re-executed without concurrent cargo activity so it could complete against a stable target tree.
