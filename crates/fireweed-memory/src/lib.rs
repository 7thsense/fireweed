#![forbid(unsafe_code)]
//! # fireweed-memory
//!
//! In-memory reference backend (atomic durability class).
//!
//! **Program B:** product composition is the generic
//! [`fireweed_engine::AsyncLogReplayBackend`] over [`MemoryLog`] ×
//! [`InMemoryProjection`]. Use [`composed_memory_backend`] to open; type call
//! sites against the generic product or port traits — not a family product alias.

mod async_backend;

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

use fireweed_core::{ItemId, UtcTimestamp};
use fireweed_engine::{AsyncLogReplayBackend, Clock, CommandId, IdGen};

pub use async_backend::async_composed_memory_backend;
pub use fireweed_projection::{InMemoryProjection, MemoryLog};

/// Assemble a fresh memory backend (async log-replay product).
pub fn composed_memory_backend() -> AsyncLogReplayBackend<MemoryLog, InMemoryProjection> {
    async_composed_memory_backend()
}

// ---------------------------------------------------------------------------
// Injected utilities: a controllable clock and a sequential id generator
// ---------------------------------------------------------------------------

/// A clock you set explicitly — keeps reclaim/lease tests deterministic.
pub struct ManualClock {
    seconds: AtomicI64,
}

impl ManualClock {
    pub fn at(seconds: i64) -> Self {
        Self {
            seconds: AtomicI64::new(seconds),
        }
    }

    pub fn set(&self, seconds: i64) {
        self.seconds.store(seconds, Ordering::SeqCst);
    }
}

impl Clock for ManualClock {
    fn now(&self) -> UtcTimestamp {
        UtcTimestamp::new(self.seconds.load(Ordering::SeqCst), 0).expect("valid timestamp")
    }
}

/// Sequential id generation.
pub struct SeqIdGen {
    counter: AtomicU64,
}

impl Default for SeqIdGen {
    fn default() -> Self {
        Self {
            counter: AtomicU64::new(0),
        }
    }
}

impl IdGen for SeqIdGen {
    fn next_item_id(&self) -> ItemId {
        let n = self.counter.fetch_add(1, Ordering::SeqCst);
        ItemId::from_u64(n)
    }

    fn next_command_id(&self) -> CommandId {
        let n = self.counter.fetch_add(1, Ordering::SeqCst);
        CommandId::new(format!("cmd-{n}"))
    }
}

#[cfg(test)]
mod tests;
