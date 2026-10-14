//! One conditional append boundary shared by owned mutation orchestrators.
use super::*;
use fireweed_engine::queue_log_record::OwnedQueueLogRecord;
use fireweed_objectlog::VerifiedApi007Namespace;
impl QueueRecoveryPipeline {
    pub(super) async fn publish_prepared(
        &self,
        namespace: VerifiedApi007Namespace,
        record: OwnedQueueLogRecord,
        expected_head: u64,
        epoch: u64,
        job: &Arc<QueueReservation>,
    ) -> QueueResult<()> {
        // Submission errors, including Busy/TooLarge after possible publication,
        // cannot establish rejection. Every caller proceeds to fresh original-ID
        // resolution. Never return a provisional producer response here.
        let appended = self
            .store
            .append_api007_record(namespace, record, expected_head, job, &self.workers)
            .await;
        #[cfg(test)]
        let appended = if self
            .lose_append_reply
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            assert!(
                appended
                    .as_ref()
                    .is_ok_and(|result| result.outcome().is_ok())
            );
            // The real append completed; suppress only its returned classification.
            Err(unknown("injected lost durable append reply"))
        } else {
            appended
        };
        if let Ok(result) = &appended
            && let Ok(position) = result.outcome()
            && (position.backend_epoch != epoch || position.sequence != expected_head)
        {
            return Err(unknown(
                "append position disagrees with prepared publication",
            ));
        }
        drop(appended);
        Ok(())
    }
}
