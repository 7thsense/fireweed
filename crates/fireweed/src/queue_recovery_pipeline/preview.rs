//! Administrative preview uses the common owned fresh-coverage read protocol.
use super::read::CoveredRead;
use super::*;
#[cfg(not(target_os = "linux"))]
use fireweed_core::queue_administration::AdministrativeOperation;
use fireweed_core::queue_contract::{PreviewRequest, QueueLimits, RequestPreflight, encoded_len};
use fireweed_engine::{
    Clock,
    queue_contract::QueueFuture,
    queue_memory::{IncomingHeaders, administrative_groups_preflight},
};
use fireweed_queue_spool::ScratchDiskQuota;
use fireweed_queue_spool::ScratchRoot;
use fireweed_turso::queue_projection::{
    AdministrativePreparationInput, AdmittedQueuePreview, CommitPreparationInput,
};
struct PreviewRead {
    request: Arc<PreviewRequest>,
    clock: Arc<dyn Clock>,
    root: Arc<ScratchRoot>,
    quota: Arc<ScratchDiskQuota>,
}
impl IncomingHeaders for PreviewRead {
    fn headers(&self) -> QueueResult<u64> {
        self.request
            .headers()?
            .checked_add(
                (self.root.path().as_os_str().len() as u64)
                    .checked_mul(4)
                    .and_then(|n| n.checked_add(4096))
                    .ok_or(QueueError::TooLarge)?,
            )
            .ok_or(QueueError::TooLarge)
    }
}
impl QueueRecoveryPipeline {
    pub async fn preview(
        self: &Arc<Self>,
        request: PreviewRequest,
        limits: QueueLimits,
        clock: Arc<dyn Clock>,
        job: Arc<QueueReservation>,
    ) -> QueueResult<AdmittedQueuePreview> {
        self.covered_read(
            PreviewRead {
                request: Arc::new(request),
                clock,
                root: Arc::clone(&self.scratch_root),
                quota: Arc::clone(&self.scratch_quota),
            },
            limits,
            job,
        )
        .await
    }
}
impl CoveredRead for PreviewRead {
    type Output = AdmittedQueuePreview;
    fn address(&self) -> &QueueAddress {
        &self.request.queue
    }
    fn encoded_size(&self) -> QueueResult<u64> {
        encoded_len(self.request.as_ref())
    }
    fn preflight(&self, limits: &QueueLimits) -> QueueResult<()> {
        self.request.preflight(limits)?;
        administrative_groups_preflight(
            &self.request.queue,
            std::slice::from_ref(&self.request.group),
            limits,
        )?;
        #[cfg(not(target_os = "linux"))]
        if matches!(
            self.request
                .group
                .administration
                .as_ref()
                .map(|a| &a.intent.operation),
            Some(AdministrativeOperation::SelectFirst(_))
        ) {
            return Err(QueueError::Invalid(
                "broad administrative preview not assembled".into(),
            ));
        }
        Ok(())
    }
    fn recover<'a>(
        &'a self,
        pipeline: &'a Arc<QueueRecoveryPipeline>,
        job: Arc<QueueReservation>,
    ) -> QueueFuture<'a, RecoveredQueue> {
        Box::pin(async move {
            let states =
                fireweed_engine::queue_group_state_authority::ColdGroupStateTracker::with_work(
                    &self.request.queue,
                    fireweed_engine::queue_group_state_authority::GroupDeclarations::Preview(
                        self.request.clone(),
                    ),
                    &job,
                )?;
            pipeline
                .recover_checked(
                    self.request.queue.clone(),
                    None,
                    None,
                    Some(RecoveryStateTracker::Groups(states)),
                    job,
                )
                .await
        })
    }
    fn read<'a>(
        &'a self,
        pipeline: &'a Arc<QueueRecoveryPipeline>,
        recovered: &'a RecoveredQueue,
        limits: QueueLimits,
        job: &'a Arc<QueueReservation>,
    ) -> QueueFuture<'a, Self::Output> {
        Box::pin(async move {
            let input = CommitPreparationInput {
                original: None,
                states: Some(
                    recovered
                        .group_state_authority
                        .as_ref()
                        .ok_or_else(|| unknown("preview state proof missing"))?
                        .group(0)?,
                ),
                genesis: pipeline.original_genesis(&self.request.queue, recovered, job)?,
                queue: self.request.queue.clone(),
                group: self.request.group.clone(),
                limits,
                required_next_offset: recovered.target_next_offset,
                required_insertion_sequence: recovered.history.insertion_sequence(),
                required_last_record_epoch: recovered.history.historical_epoch(),
            };
            if input.group.administration.is_none() {
                pipeline
                    .projection
                    .preview_ordinary_commit(input, self.clock.clone(), job, &pipeline.workers)
                    .await
            } else {
                pipeline
                    .projection
                    .preview_administration(
                        AdministrativePreparationInput {
                            commit: input,
                            scratch_root: self.root.clone(),
                            scratch_quota: self.quota.clone(),
                        },
                        self.clock.clone(),
                        job,
                        &pipeline.workers,
                    )
                    .await
            }
        })
    }
    #[cfg(test)]
    fn pause(
        &self,
        pipeline: &QueueRecoveryPipeline,
    ) -> Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)> {
        pipeline.after_preview_read.lock().unwrap().take()
    }
}
