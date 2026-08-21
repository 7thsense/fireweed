//! Typed raw-commit seam used by conformance and fault injection.
//!
//! Ordinary queue mutations use the operation-specific engine ports. This request exists for tests and
//! recovery probes that must drive the append/apply boundary directly without supplying arbitrary code to
//! a backend-owned transaction. Every field is owned so later native-async backends can transfer the whole
//! request into an owned commit task without borrowing caller state across a suspension point.

use crate::{CommandEnvelope, CommandPosition, QueueKey};

/// Provenance for the admission that is still live when a raw request reaches append.
///
/// This is an inert carrier. It does not acquire a permit or fence; later contention slices use it to
/// prove that derived append sites activate exactly one routing domain without guessing from commands.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum AppendAdmissionClass {
    /// Generic object-log/raw callers that do not participate in the derived Turso protocol.
    #[default]
    NonDerived,
    /// A derived append executing while its [`crate::KeyedQueueGate`] permit remains live.
    KeyedPermitLive,
    /// A direct derived append that will require selection admission before fence activation.
    SelectionRequired,
    /// A direct derived append whose leased-only/non-work command class bypasses selection fencing.
    Bypass,
    /// An atomic Turso append governed by the native atomic writer path.
    AtomicNative,
    /// A single-owner append performed while reopen blocks serving traffic.
    RecoveryOnly,
    /// The dedicated default item-Claim append owned by the Claim coordinator.
    ClaimCoordinatorLive,
}

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
    append_admission: AppendAdmissionClass,
}

impl RawCommitRequest {
    /// Build a normal append-plus-apply request.
    pub fn new(shard: QueueKey, commands: Vec<CommandEnvelope>, expected_epoch: u64) -> Self {
        Self {
            shard,
            commands,
            expected_epoch,
            fault: RawCommitFault::None,
            append_admission: AppendAdmissionClass::NonDerived,
        }
    }

    /// Select a typed fault boundary for this request.
    pub fn with_fault(mut self, fault: RawCommitFault) -> Self {
        self.fault = fault;
        self
    }

    /// Attach append-time admission provenance without activating admission or fencing.
    pub fn with_append_admission(mut self, append_admission: AppendAdmissionClass) -> Self {
        self.append_admission = append_admission;
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

    pub fn append_admission(&self) -> AppendAdmissionClass {
        self.append_admission
    }

    /// Consume the request at an adapter ownership boundary without cloning its command batch.
    pub fn into_parts(self) -> (QueueKey, Vec<CommandEnvelope>, u64, RawCommitFault) {
        (self.shard, self.commands, self.expected_epoch, self.fault)
    }

    /// Consume the request at a derived adapter boundary while retaining append-admission provenance.
    pub fn into_parts_with_append_admission(
        self,
    ) -> (
        QueueKey,
        Vec<CommandEnvelope>,
        u64,
        RawCommitFault,
        AppendAdmissionClass,
    ) {
        (
            self.shard,
            self.commands,
            self.expected_epoch,
            self.fault,
            self.append_admission,
        )
    }
}

#[cfg(test)]
mod append_admission_tests {
    use fireweed_core::{QueueId, TenantId};

    use super::*;

    fn queue() -> QueueKey {
        QueueKey::new(
            TenantId::new("tenant").unwrap(),
            QueueId::new("queue").unwrap(),
        )
    }

    #[test]
    fn raw_commit_defaults_generic_callers_to_non_derived() {
        let request = RawCommitRequest::new(queue(), Vec::new(), 1);
        assert_eq!(request.append_admission(), AppendAdmissionClass::NonDerived);
    }

    #[test]
    fn raw_commit_carries_each_append_admission_class_without_behavior() {
        let classes = [
            AppendAdmissionClass::NonDerived,
            AppendAdmissionClass::KeyedPermitLive,
            AppendAdmissionClass::SelectionRequired,
            AppendAdmissionClass::Bypass,
            AppendAdmissionClass::AtomicNative,
            AppendAdmissionClass::RecoveryOnly,
            AppendAdmissionClass::ClaimCoordinatorLive,
        ];
        for class in classes {
            let request =
                RawCommitRequest::new(queue(), Vec::new(), 1).with_append_admission(class);
            let (_, _, _, _, carried) = request.into_parts_with_append_admission();
            assert_eq!(carried, class);
        }
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
