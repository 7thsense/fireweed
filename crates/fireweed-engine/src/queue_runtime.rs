//! Pure checked runtime defaults shared by concrete API-007 constructors.
//! This is resource configuration, not a durable queue definition or DTO.
use fireweed_core::queue_contract::{QueueError, QueueLimits, QueueResult};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ValidatedQueueRuntime {
    operations: usize,
    retained_bytes: u64,
    scratch_bytes: u64,
}
impl ValidatedQueueRuntime {
    /// Derive existing defaults without creating threads, directories or clients.
    pub fn for_limits(limits: &QueueLimits) -> QueueResult<Self> {
        limits.validate()?;
        Ok(Self {
            operations: 64,
            retained_bytes: limits
                .max_request_bytes
                .checked_mul(8)
                .ok_or(QueueError::TooLarge)?,
            scratch_bytes: limits
                .max_group_bytes
                .checked_mul(16)
                .ok_or(QueueError::TooLarge)?,
        })
    }
    pub fn operations(self) -> usize {
        self.operations
    }
    pub fn retained_bytes(self) -> u64 {
        self.retained_bytes
    }
    pub fn scratch_bytes(self) -> u64 {
        self.scratch_bytes
    }
}
