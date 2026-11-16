#![forbid(unsafe_code)]
//! # Fireweed
//!
//! Mutable-priority leased work queues. [`queue::Queue`] is the only public entry point: open it
//! with `Queue::s3_turso` (production), `Queue::ephemeral_disk` (development and tests) or
//! `Queue::postgres`, then push, claim, settle and inspect through the one interface.

/// Small backend-neutral claim/commit queue interface (API-007).
pub mod queue;
#[doc(hidden)]
#[cfg(all(feature = "objectlog", feature = "turso"))]
pub mod queue_recovery_pipeline;

mod clock;
mod config;

pub use clock::SystemClock;
pub use config::ConfigSecret;

pub use bytes::Bytes;
pub use fireweed_core::{OwnerId, UtcTimestamp};
pub use fireweed_engine::Clock;
