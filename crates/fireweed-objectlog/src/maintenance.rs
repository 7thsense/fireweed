//! Object-log adapter types for the engine-owned maintenance policy.
//!
//! These types carry bounded discovery/effect evidence across the object-store maintenance seam.
//! Retention selection itself lives in `fireweed-engine::compose::plan_retention`; this module
//! deliberately does not define a second policy or executor.

#![allow(dead_code)] // report builders used by retention adapters not yet rewired to LogEngine

use std::collections::BTreeMap;
use std::num::{NonZeroU64, NonZeroUsize};
use std::time::Duration;

use fireweed_engine::{EngineError, EngineResult};

use crate::object_store_observability::BlobResultClass;

pub const MAINTENANCE_CURSOR_VERSION: u8 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum MaintenanceExecutionReason {
    CommittedBranch,
    Filtered,
    InFlightWriterGrace,
    EpochChanged,
    BudgetExhausted,
    RetryableFailure,
    PermanentFailure,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MaintenanceFailureCause {
    MissingInheritanceMetadata,
    InvalidInheritanceMetadata,
    Provider(BlobResultClass),
    Internal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaintenanceLimits {
    pub objects: NonZeroUsize,
    pub bytes: NonZeroU64,
    pub requests: NonZeroUsize,
    pub elapsed: Duration,
    pub page_size: NonZeroUsize,
}

impl MaintenanceLimits {
    pub fn new(
        objects: usize,
        bytes: u64,
        requests: usize,
        elapsed: Duration,
        page_size: usize,
    ) -> EngineResult<Self> {
        let invalid = || EngineError::Invalid("maintenance limits must all be nonzero");
        if elapsed.is_zero() {
            return Err(invalid());
        }
        Ok(Self {
            objects: NonZeroUsize::new(objects).ok_or_else(invalid)?,
            bytes: NonZeroU64::new(bytes).ok_or_else(invalid)?,
            requests: NonZeroUsize::new(requests).ok_or_else(invalid)?,
            elapsed,
            page_size: NonZeroUsize::new(page_size).ok_or_else(invalid)?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaintenanceCursor {
    pub version: u8,
    pub resume_after: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaintenanceEffect {
    pub objects: usize,
    pub bytes: u64,
    pub requests: usize,
}

#[derive(Debug)]
pub(crate) struct MaintenanceExecutionFailure {
    pub effect: MaintenanceEffect,
    pub error: EngineError,
    pub fault: Option<crate::object_store_observability::BlobStoreFault>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaintenanceReport {
    pub dry_run: bool,
    pub scanned: usize,
    pub retained: usize,
    pub retained_by_reason: BTreeMap<MaintenanceExecutionReason, usize>,
    pub deleted: usize,
    pub completed_candidates: usize,
    pub would_delete: usize,
    pub would_delete_bytes: u64,
    pub bytes_deleted: u64,
    pub requests: usize,
    pub retryable_failures: usize,
    pub permanent_failures: usize,
    pub failure_cause: Option<MaintenanceFailureCause>,
    pub fenced: bool,
    pub stopped_by: Option<MaintenanceExecutionReason>,
    pub cursor: Option<MaintenanceCursor>,
}

impl MaintenanceReport {
    pub(crate) fn new(dry_run: bool) -> Self {
        Self {
            dry_run,
            scanned: 0,
            retained: 0,
            retained_by_reason: BTreeMap::new(),
            deleted: 0,
            completed_candidates: 0,
            would_delete: 0,
            would_delete_bytes: 0,
            bytes_deleted: 0,
            requests: 0,
            retryable_failures: 0,
            permanent_failures: 0,
            failure_cause: None,
            fenced: false,
            stopped_by: None,
            cursor: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn limits_are_strictly_nonzero() {
        assert!(MaintenanceLimits::new(0, 1, 1, Duration::from_secs(1), 1).is_err());
        assert!(MaintenanceLimits::new(1, 0, 1, Duration::from_secs(1), 1).is_err());
        assert!(MaintenanceLimits::new(1, 1, 0, Duration::from_secs(1), 1).is_err());
        assert!(MaintenanceLimits::new(1, 1, 1, Duration::ZERO, 1).is_err());
        assert!(MaintenanceLimits::new(1, 1, 1, Duration::from_secs(1), 0).is_err());
    }
}
