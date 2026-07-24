//! Typed raw-commit seam used by conformance and fault injection.
//!
//! Ordinary queue mutations use the operation-specific engine ports. This request exists for tests and
//! recovery probes that must drive the append/apply boundary directly without supplying arbitrary code to
//! a backend-owned transaction. Every field is owned so later native-async backends can transfer the whole
//! request into an owned commit task without borrowing caller state across a suspension point.

use crate::{CommandEnvelope, CommandPosition, QueueKey};

/// A typed fault control at one of the legal raw-commit suspension boundaries.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum RawCommitFault {
    /// Run append and projection apply normally.
    #[default]
    None,
    /// Resolve before the append starts. No durable or visible effect is allowed.
    BeforeAppend,
    /// Resolve after append but before projection apply.
    ///
    /// On a log-bearing composed backend this leaves a durable tail for recovery to replay. A unified
    /// atomic store may only stage the append, in which case dropping its unit of work leaves no effect.
    AfterAppendBeforeApply,
}

/// One owned raw commit.
#[derive(Debug, Clone)]
pub struct RawCommitRequest {
    shard: QueueKey,
    commands: Vec<CommandEnvelope>,
    expected_epoch: u64,
    fault: RawCommitFault,
}

impl RawCommitRequest {
    /// Build a normal append-plus-apply request.
    pub fn new(shard: QueueKey, commands: Vec<CommandEnvelope>, expected_epoch: u64) -> Self {
        Self {
            shard,
            commands,
            expected_epoch,
            fault: RawCommitFault::None,
        }
    }

    /// Select a typed fault boundary for this request.
    pub fn with_fault(mut self, fault: RawCommitFault) -> Self {
        self.fault = fault;
        self
    }

    pub fn shard(&self) -> &QueueKey {
        &self.shard
    }

    pub fn commands(&self) -> &[CommandEnvelope] {
        &self.commands
    }

    pub fn expected_epoch(&self) -> u64 {
        self.expected_epoch
    }

    pub fn fault(&self) -> RawCommitFault {
        self.fault
    }

    /// Consume the request at an adapter ownership boundary without cloning its command batch.
    pub fn into_parts(self) -> (QueueKey, Vec<CommandEnvelope>, u64, RawCommitFault) {
        (self.shard, self.commands, self.expected_epoch, self.fault)
    }
}

/// The resolved footprint of a typed raw commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawCommitOutcome {
    positions: Vec<CommandPosition>,
    projection_applied: bool,
}

impl RawCommitOutcome {
    /// Resolve a raw commit after append but before projection apply.
    pub fn appended(positions: Vec<CommandPosition>) -> Self {
        Self {
            positions,
            projection_applied: false,
        }
    }

    /// Resolve a raw commit after append and projection apply.
    pub fn applied(positions: Vec<CommandPosition>) -> Self {
        Self {
            positions,
            projection_applied: true,
        }
    }

    /// Positions returned by the append, in command order.
    pub fn positions(&self) -> &[CommandPosition] {
        &self.positions
    }

    /// Consume the outcome and return its positions.
    pub fn into_positions(self) -> Vec<CommandPosition> {
        self.positions
    }

    /// Whether this invocation reached and completed projection apply.
    pub fn projection_applied(&self) -> bool {
        self.projection_applied
    }
}
