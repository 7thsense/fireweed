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
        let cut = self.source_cut(log, job)?;
        // Equal epochs/offsets never substitute for the original bundle owner.
        // Reject before delegating, so no compiler read or nonrefund debit occurs.
        let record = publication.record_for(&cut, definition, job)?;
        self.compile_and_prepare(log, record, definition, budget, limits, job)
            .await
    }
}
