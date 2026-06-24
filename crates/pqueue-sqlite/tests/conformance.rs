//! The shared backend-conformance suite (the 16 port-level no-stub scenarios) run against the sqlite
//! backend. Each scenario gets a fresh `:memory:` database.

use pqueue_sqlite::SqliteBackend;

pqueue_conformance::conformance_suite!(|| SqliteBackend::in_memory().expect("open :memory:"));
