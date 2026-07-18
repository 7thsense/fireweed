//! Turso Database relational adapter foundations.
//!
//! The adapter is feature-gated so the Rust-native database engine is absent from default workspace
//! builds. The `local` feature exposes an embedded async store; remote/cloud sync is deliberately outside
//! this crate's current contract.

#[cfg(feature = "local")]
mod local;
#[cfg(feature = "local")]
mod projection;

#[cfg(feature = "local")]
pub use local::*;

/// Whether this build contains the embedded Turso engine.
pub const LOCAL_FEATURE_ENABLED: bool = cfg!(feature = "local");
