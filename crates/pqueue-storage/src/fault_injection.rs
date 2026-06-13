//! Deterministic fault-injection wrappers for storage traits.
//!
//! Provides `FaultInjectedLogStore<T>` for injecting partial-append failures
//! and a `replay` helper for replaying a shard log into a `ProjectionStore`.
//! Used by `fault_injection_harness_tests` to verify INV-2 and INV-10.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use crate::commands::CommandEnvelope;
use crate::traits::{
    AppendBatchResult, CommandPage, DurabilityProfile, LogStore, LogStoreError, ProjectionError,
    ProjectionStore,
};
use crate::types::{CommandPosition, ShardKey};

// ---------------------------------------------------------------------------
// FailureMode
// ---------------------------------------------------------------------------

/// How a fault-injected store should fail.
#[derive(Debug, Clone)]
pub enum FailureMode {
    /// No failures injected — passthrough.
    None,
    /// Fail the Nth `append_batch` call (1-based).
    FailAtCallN(u32),
    /// Commit only the first `n` commands of the batch, then fail.
    PartialAppend(usize),
}

// ---------------------------------------------------------------------------
// FaultInjectedLogStore
// ---------------------------------------------------------------------------

/// Wraps any `LogStore` and injects deterministic failures for testing.
pub struct FaultInjectedLogStore<T: LogStore> {
    inner: T,
    mode: FailureMode,
    call_count: Arc<AtomicU32>,
}

impl<T: LogStore> FaultInjectedLogStore<T> {
    pub fn new(inner: T, mode: FailureMode) -> Self {
        Self { inner, mode, call_count: Arc::new(AtomicU32::new(0)) }
    }

    pub fn call_count(&self) -> u32 {
        self.call_count.load(Ordering::SeqCst)
    }
}

impl<T: LogStore + Send + Sync> LogStore for FaultInjectedLogStore<T> {
    async fn append_batch(
        &self,
        shard: &ShardKey,
        expected_epoch: Option<u64>,
        commands: Vec<CommandEnvelope>,
    ) -> Result<AppendBatchResult, LogStoreError> {
        let n = self.call_count.fetch_add(1, Ordering::SeqCst) + 1;

        match &self.mode {
            FailureMode::None => self.inner.append_batch(shard, expected_epoch, commands).await,

            FailureMode::FailAtCallN(target) => {
                if n == *target {
                    return Err(LogStoreError::StorageFailure(
                        format!("injected failure at call {}", n),
                    ));
                }
                self.inner.append_batch(shard, expected_epoch, commands).await
            }

            FailureMode::PartialAppend(keep) => {
                let total = commands.len();
                if total <= *keep {
                    // All commands fit; no truncation needed, behave normally.
                    return self.inner.append_batch(shard, expected_epoch, commands).await;
                }
                // Commit only the first `keep` commands, then fail.
                let truncated: Vec<CommandEnvelope> = commands.into_iter().take(*keep).collect();
                if truncated.is_empty() {
                    return Err(LogStoreError::StorageFailure(
                        "partial append: 0 commands committed".to_string(),
                    ));
                }
                self.inner.append_batch(shard, expected_epoch, truncated).await?;
                Err(LogStoreError::StorageFailure(
                    "partial append: truncated batch".to_string(),
                ))
            }
        }
    }

    async fn read_from(
        &self,
        shard: &ShardKey,
        position: Option<CommandPosition>,
        limit: usize,
    ) -> Result<CommandPage, LogStoreError> {
        self.inner.read_from(shard, position, limit).await
    }

    fn durability_profile(&self) -> DurabilityProfile {
        self.inner.durability_profile()
    }
}

// ---------------------------------------------------------------------------
// Replay engine
// ---------------------------------------------------------------------------

/// Replay all commands from `log` for `shard` into `projection`.
///
/// Reads until the log tail, applying each page via `apply_committed`.
/// Returns the final position replayed, or `None` if the log was empty.
pub async fn replay<L: LogStore, P: ProjectionStore>(
    log: &L,
    projection: &P,
    shard: &ShardKey,
) -> Result<Option<CommandPosition>, ReplayError> {
    let mut cursor: Option<CommandPosition> = None;
    let mut last_position: Option<CommandPosition> = None;

    loop {
        let page = log
            .read_from(shard, cursor.clone(), 256)
            .await
            .map_err(ReplayError::Log)?;

        if page.commands.is_empty() {
            break;
        }

        for (pos, env) in &page.commands {
            projection
                .apply_committed(pos.clone(), std::slice::from_ref(env))
                .await
                .map_err(ReplayError::Projection)?;
            last_position = Some(pos.clone());
        }

        match page.next_position {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }

    Ok(last_position)
}

// ---------------------------------------------------------------------------
// ReplayError
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub enum ReplayError {
    Log(LogStoreError),
    Projection(ProjectionError),
}

impl std::fmt::Display for ReplayError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Log(e) => write!(f, "log error during replay: {}", e),
            Self::Projection(e) => write!(f, "projection error during replay: {}", e),
        }
    }
}

// ---------------------------------------------------------------------------
// KillPoint
// ---------------------------------------------------------------------------

/// Deterministic "process kill" simulation for unit tests.
///
/// A test registers kill points by name. The worker checks `should_kill()`
/// between operations; if `true`, it stops processing (simulating a crash).
/// After a kill, replay from the log verifies no work is lost (INV-10).
pub struct KillSchedule {
    killed: Arc<AtomicU32>,
    kill_at: u32,
}

impl KillSchedule {
    /// Create a kill schedule that fires after `kill_after` checkpoints.
    pub fn kill_after(kill_after: u32) -> Self {
        Self { killed: Arc::new(AtomicU32::new(0)), kill_at: kill_after }
    }

    /// Always-passive schedule (no kills).
    pub fn never() -> Self {
        Self::kill_after(u32::MAX)
    }

    /// Check a checkpoint. Returns `true` if the process should stop now.
    pub fn checkpoint(&self) -> bool {
        let n = self.killed.fetch_add(1, Ordering::SeqCst) + 1;
        n >= self.kill_at
    }

    pub fn checkpoint_count(&self) -> u32 {
        self.killed.load(Ordering::SeqCst)
    }
}
