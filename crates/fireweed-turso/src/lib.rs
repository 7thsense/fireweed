//! Turso Database relational adapter foundations.
//!
//! The adapter is feature-gated so the Rust-native database engine is absent from default workspace
//! builds. The `local` feature exposes an embedded async store; remote/cloud sync is deliberately outside
//! this crate's current contract.

#[cfg(feature = "local")]
mod local;
#[cfg(feature = "local")]
mod metrics;
#[cfg(feature = "local")]
mod projection;
#[cfg(feature = "local")]
mod query;
#[cfg(feature = "local")]
mod rebuildable_io;
#[cfg(feature = "local")]
mod tx;

#[cfg(feature = "local")]
pub use local::*;
#[cfg(feature = "local")]
pub use projection::materialize_grouped_cohort_claimed_on;

/// Whether this build contains the embedded Turso engine.
pub const LOCAL_FEATURE_ENABLED: bool = cfg!(feature = "local");

#[cfg(all(test, feature = "local"))]
mod runtime_isolation_tests;
