//! Internal composition of owned native log stages and the stock-Turso projector.
//!
//! Each queue has one owner at a time, by a control-plane lease, and one loop
//! (`owner`) through which every operation, read or write, passes. Writes go to
//! the immutable log and are applied to a local projection; reads come from the
//! projection. Healthy operation reads no log records.
use fireweed_core::queue_contract::{QueueAddress, QueueError, QueueLimits, QueueResult};
mod claim;
mod diagnostics;
pub use diagnostics::{RecoveryCounters, RecoverySnapshot};
mod commit_batch;
pub use commit_batch::AdmittedQueueCommitBatch;
mod insert;
mod lease_clock;
pub use lease_clock::{LeaseClock, SystemLeaseClock};
mod owner;
pub use owner::QueueOwnership;
mod preview;
mod projection_files;
mod provision;
mod query;
mod receipts;
mod state;
mod stats;
mod store;
use fireweed_engine::{
    queue_admission::{OwnedQueueBufferCharge, QueueReservation},
    queue_log_record::{QueueLogFormat, encode_checked_genesis},
    queue_schema_codec::AdmittedQueueDefinition,
};
use fireweed_objectlog::queue_native_log::QueueNativeLog;
use fireweed_queue_spool::ScratchWorkers;
use fireweed_turso::queue_projection::QueueProjection;
use sha2::Sha256;
use std::{path::PathBuf, sync::Arc};
pub use store::NativeQueueStore;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
fn unknown(message: &str) -> QueueError {
    QueueError::OutcomeUnknown(format!("API-007 recovery: {message}"))
}
/// Process-local pipeline capacities. Validate before a factory constructs its
/// store/projection resources; these are not durable queue policy.
#[derive(Debug, Clone, Copy)]
pub struct QueuePipelineOptions {
    pub operations: usize,
    pub hard_record_bytes: u64,
}
impl QueuePipelineOptions {
    pub fn validate(&self) -> QueueResult<()> {
        if self.operations == 0
            || self.operations > Semaphore::MAX_PERMITS
            || fireweed_engine::queue_log_record::standalone_record_bound(self.hard_record_bytes)
                .is_none()
        {
            return Err(QueueError::Invalid(
                "invalid queue pipeline capacities".into(),
            ));
        }
        Ok(())
    }
}
/// A pipeline owns its queues' loops; it has no blocking worker while awaiting stages.
pub struct QueueRecoveryPipeline {
    log: Arc<QueueNativeLog>,
    /// The directory that holds one projection database per owned queue. It is
    /// used by this pipeline alone.
    projection_directory: PathBuf,
    workers: ScratchWorkers,
    scratch_root: Arc<fireweed_queue_spool::ScratchRoot>,
    scratch_quota: Arc<fireweed_queue_spool::ScratchDiskQuota>,
    operation_slots: Arc<Semaphore>,
    hard_record_bytes: u64,
    diagnostics: Arc<RecoveryCounters>,
    ownership: QueueOwnership,
    /// The projection directory's exclusive lock. Every tenure projection holds a
    /// clone, so the OS lock outlives the pipeline until the last tenure is gone.
    directory_lock: projection_files::DirectoryLock,
    queues: std::sync::Mutex<owner::QueueTable>,
    #[cfg(test)]
    lose_append_reply: std::sync::atomic::AtomicBool,
    /// Conditional publishes submitted, whatever their outcome.
    #[cfg(test)]
    published: std::sync::atomic::AtomicU64,
    /// Entries staged into a batch's projection transaction.
    #[cfg(test)]
    staged: std::sync::atomic::AtomicU64,
    /// Planned records `Batch::stage` declined as too large for an empty batch.
    #[cfg(test)]
    lone_too_large: std::sync::atomic::AtomicU64,
    #[cfg(test)]
    before_drop: std::sync::Mutex<Option<Box<dyn FnOnce() + Send>>>,
    #[cfg(test)]
    on_stage: std::sync::Mutex<Option<StageHook>>,
}
#[cfg(test)]
impl Drop for QueueRecoveryPipeline {
    fn drop(&mut self) {
        if let Some(hook) = self.before_drop.get_mut().unwrap().take() {
            hook();
        }
    }
}
#[cfg(test)]
type StageHook = Arc<dyn Fn(owner::Stage) + Send + Sync>;
/// On return, cancellation or unwind, release the operation permit before the
/// final task reservation owner can notify shutdown waiters.
struct OperationCompletion {
    _slot: OwnedSemaphorePermit,
    _job: Arc<QueueReservation>,
}
impl QueueRecoveryPipeline {
    /// The bound a planner gets for the standalone record it produces. The owner
    /// publishes that record as an entry of an entry-list record, which is larger
    /// by a fixed overhead, so this is `hard_record_bytes` less that overhead.
    /// `validate` guarantees it is positive.
    fn planner_bound(&self) -> u64 {
        fireweed_engine::queue_log_record::standalone_record_bound(self.hard_record_bytes)
            .unwrap_or(0)
    }
    /// `projection_directory` is an absolute path that does not exist or is a
    /// directory. It is created if missing, and the projection files an earlier
    /// process left in it are deleted, never read. The pipeline alone uses it.
    pub fn new(
        log: Arc<QueueNativeLog>,
        projection_directory: PathBuf,
        workers: ScratchWorkers,
        options: QueuePipelineOptions,
        scratch_root: Arc<fireweed_queue_spool::ScratchRoot>,
        scratch_quota: Arc<fireweed_queue_spool::ScratchDiskQuota>,
        ownership: QueueOwnership,
    ) -> QueueResult<Arc<Self>> {
        options.validate()?;
        let directory_lock = projection_files::prepare_directory(&projection_directory)?;
        Ok(Arc::new(Self {
            log,
            projection_directory,
            workers,
            scratch_root,
            scratch_quota,
            operation_slots: Arc::new(Semaphore::new(options.operations)),
            hard_record_bytes: options.hard_record_bytes,
            diagnostics: Arc::new(RecoveryCounters::default()),
            ownership,
            directory_lock,
            queues: std::sync::Mutex::new(owner::QueueTable::default()),
            #[cfg(test)]
            lose_append_reply: std::sync::atomic::AtomicBool::new(false),
            #[cfg(test)]
            published: std::sync::atomic::AtomicU64::new(0),
            #[cfg(test)]
            staged: std::sync::atomic::AtomicU64::new(0),
            #[cfg(test)]
            lone_too_large: std::sync::atomic::AtomicU64::new(0),
            #[cfg(test)]
            before_drop: std::sync::Mutex::new(None),
            #[cfg(test)]
            on_stage: std::sync::Mutex::new(None),
        }))
    }
}

#[cfg(test)]
mod tests;
