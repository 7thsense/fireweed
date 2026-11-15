//! Shared request logic for Fireweed data-plane implementations.
//!
//! Implementations load the records a request needs into a [`Space`], call
//! a handler with the request time fixed by [`handlers::begin`], and persist
//! what changed. The in-memory implementation keeps the whole `Space`; the
//! PostgreSQL implementation loads the objects a request references.
//! This crate is a shared internal library (API-006 crate rule 4); it is
//! not itself an implementation.

pub mod exec;
pub mod handlers;
pub mod reads;
pub mod state;
pub mod validate;

pub use handlers::Profile;
pub use state::*;
