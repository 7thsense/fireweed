//! Preparation consumes only a source-planned record bound to this acquisition.
use super::*;
impl QueueNativeAcquired {
    pub async fn prepare_group_publication(
        &self,
        log: &QueueNativeLog,
        publication: &fireweed_engine::queue_source_index::QueueSourceGroupPublication<'_>,
        definition: &AdmittedQueueDefinition,
        budget: Arc<QueueNativeSourceBudget>,
        limits: QueueNativeSourcePreparationLimits,
        job: &Arc<QueueReservation>,
    ) -> QueueResult<QueueNativePublication> {
        let cut = self.as_source().source_cut(log, job)?;
        // Equal epochs/offsets never substitute for the original bundle owner.
        // Reject before delegating, so no compiler read or nonrefund debit occurs.
        let record = publication.record_for(&cut, definition, job)?;
        self.compile_and_prepare(log, record, definition, budget, limits, job)
            .await
    }
}

/// All inputs belong to one declared batch and one nonrefunding operation.
/// Original freshness and caller-order prefix eligibility are established by
/// the coordinator before the engine seals the publication.
pub struct QueueNativeBatchPreparation<'a> {
    pub batch: &'a fireweed_core::queue_contract::CommitBatch,
    pub definition: &'a AdmittedQueueDefinition,
    pub budget: Arc<QueueNativeSourceBudget>,
    pub limits: QueueNativeSourcePreparationLimits,
}
impl QueueNativeAcquired {
    /// Reject foreign declaration/cut/job bindings before compiler reads or
    /// preparation work. Preparation retains the engine's physical prefix and
    /// does not submit or acquire a replacement source cut.
    pub async fn prepare_batch_publication(
        &self,
        log: &QueueNativeLog,
        publication: &fireweed_engine::queue_source_index::QueueSourceBatchPublication<'_>,
        input: QueueNativeBatchPreparation<'_>,
        job: &Arc<QueueReservation>,
    ) -> QueueResult<QueueNativePublication> {
        let cut = self.as_source().source_cut(log, job)?;
        let record = publication.record_for(&cut, input.definition, input.batch, job)?;
        self.compile_and_prepare(
            log,
            record,
            input.definition,
            input.budget,
            input.limits,
            job,
        )
        .await
    }
}

/// One source-proven FP4 group is a physical publication barrier in a batch.
pub struct QueueNativeAdministrativePreparation<'a> {
    pub queue: &'a fireweed_core::queue_contract::QueueAddress,
    pub group: &'a fireweed_core::queue_contract::CommitGroup,
    pub definition: &'a AdmittedQueueDefinition,
    pub budget: Arc<QueueNativeSourceBudget>,
    pub limits: QueueNativeSourcePreparationLimits,
}
impl QueueNativeAcquired {
    pub async fn prepare_administrative_publication(
        &self,
        log: &QueueNativeLog,
        publication: &fireweed_queue_spool::SourceAdministrativePublication,
        input: QueueNativeAdministrativePreparation<'_>,
        job: &Arc<QueueReservation>,
    ) -> QueueResult<QueueNativePublication> {
        let cut = self.as_source().source_cut(log, job)?;
        let record =
            publication.record_for(&cut, input.queue, input.group, input.definition, job)?;
        self.compile_and_prepare(
            log,
            record,
            input.definition,
            input.budget,
            input.limits,
            job,
        )
        .await
    }
}
