#![forbid(unsafe_code)]
//! # pqueue-memory
//!
//! In-memory reference backend (atomic durability class), assembled as the orthogonal product
//! `MemoryLog × InMemoryProjection × InProcessControlPlane` by the one generic
//! [`pqueue_engine::ComposedBackend`] (ADR-012). All apply/eligibility/lease/metrics logic lives in
//! [`pqueue_projection`] and the orchestration lives once in [`pqueue_engine::ComposedBackend`]; this crate
//! only assembles the axes and provides the test-only [`ManualClock`]/[`SeqIdGen`] helpers (memory-specific
//! and not expressible through the ports).

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

use pqueue_core::{ItemId, UtcTimestamp};
use pqueue_engine::{Clock, CommandId, ComposedBackend, IdGen, InProcessControlPlane};
use pqueue_projection::{InMemoryProjection, MemoryLog};

// ---------------------------------------------------------------------------
// Composed memory backend (ADR-012)
//
// The memory backend expressed as the orthogonal product `MemoryLog × InMemoryProjection ×
// InProcessControlPlane`, assembled by the one generic `ComposedBackend`. The shared TD-001 conformance
// suite runs against it (see `tests`).
// ---------------------------------------------------------------------------

/// The composed memory backend: `ComposedBackend<MemoryLog, InMemoryProjection, InProcessControlPlane>`.
pub type ComposedMemoryBackend =
    ComposedBackend<MemoryLog, InMemoryProjection, InProcessControlPlane>;

/// Assemble a fresh composed memory backend from one of each axis.
pub fn composed_memory_backend() -> ComposedMemoryBackend {
    ComposedBackend::new(
        MemoryLog::new(),
        InMemoryProjection::new(),
        InProcessControlPlane::new(),
    )
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
