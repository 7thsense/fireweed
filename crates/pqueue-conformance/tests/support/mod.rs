// Shared hybrid-async test-fixture support for integration tests.
//
// Path-includes the shared fixture source so every integration test can consume
// the same `base_dir` / `open_hybrid` / `clear_thresholds` / `push` / `drain`
// / `floor_seq` / `ProjectionMode` / `qdef_short_retention` / etc. without
// duplicating them or pulling pqueue-objectlog / pqueue-sqlite into the normal
// library dependency graph.
//
// The included module references `super::{qdef, shard, ts}` — the re-exports
// live in the calling test file (which adds `pub use pqueue_conformance::{qdef, ts}`
// and has its own `fn shard`), so `super::` resolves correctly.

include!("../../src/hybrid_async/mod.rs");
