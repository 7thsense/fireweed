# pqueue-703861a7 execution report

Verified:
- `cargo test -p pqueue-sqlite --tests`

Changes made:
- Fixed stale `recovery_high_water` expectations in `crates/pqueue-sqlite/tests/sqlite_projection_tests.rs`.
- Updated the hybrid recovery test to match the durable checkpoint epoch reported after reopen in `crates/pqueue-sqlite/tests/hybrid_async_recovery.rs`.

Relevant reconnect evidence already present in the crate:
- `crates/pqueue-sqlite/tests/composed_log_reconnect.rs`
- `crates/pqueue-sqlite/tests/composed_relational_reconnect.rs`
