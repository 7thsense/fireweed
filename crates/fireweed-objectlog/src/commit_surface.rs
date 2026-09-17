//! Compatibility reexports for shared commit-transition planning and recovery.
//!
//! The implementation lives in the engine so every log/projection composition
//! uses the same validation, request receipts, and recovery behavior.

pub use fireweed_engine::commit_surface::*;
