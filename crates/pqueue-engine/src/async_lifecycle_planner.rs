//! Reusable typed lifecycle preparation over native-async storage axes.

use std::sync::Arc;

use crate::{
    AsyncControlPlane, AsyncLifecyclePlan, AsyncLifecyclePlanner, AsyncLogStore,
    AsyncProjectionStore, AsyncRenewRequest, CommandChecksum, CommandEnvelope, EngineError,
    EngineResult, IdGen, OwnedTask, QueueCommand, RawCommitRequest, RenewLeaseCommand,
};

fn validate_renew_duration(
    now: pqueue_core::UtcTimestamp,
    expiry: pqueue_core::UtcTimestamp,
    max_duration_ms: u64,
) -> EngineResult<()> {
    let now_ns = i128::from(now.seconds) * 1_000_000_000 + i128::from(now.nanoseconds);
    let expiry_ns = i128::from(expiry.seconds) * 1_000_000_000 + i128::from(expiry.nanoseconds);
    let max_ns = i128::from(max_duration_ms) * 1_000_000;
    if expiry_ns <= now_ns || expiry_ns - now_ns > max_ns {
        return Err(EngineError::Invalid("invalid lease renewal duration"));
    }
    Ok(())
}

/// Ordinary-item lifecycle planner. It validates and constructs typed commands but owns no commit
/// capability; the composed backend retains the only durable mutation authority.
pub struct ProjectionLifecyclePlanner<C, L, P, I> {
    control: Arc<C>,
    log: Arc<L>,
    projection: Arc<P>,
    ids: Arc<I>,
}

impl<C, L, P, I> ProjectionLifecyclePlanner<C, L, P, I> {
    pub fn from_shared(control: Arc<C>, log: Arc<L>, projection: Arc<P>, ids: Arc<I>) -> Self {
        Self {
            control,
            log,
            projection,
            ids,
        }
    }
}

impl<C, L, P, I> AsyncLifecyclePlanner for ProjectionLifecyclePlanner<C, L, P, I>
where
    C: AsyncControlPlane + 'static,
    L: AsyncLogStore + 'static,
    P: AsyncProjectionStore + 'static,
    I: IdGen + 'static,
{
    fn plan_renew(
        &self,
        request: AsyncRenewRequest,
    ) -> OwnedTask<EngineResult<AsyncLifecyclePlan>> {
        let control = Arc::clone(&self.control);
        let log = Arc::clone(&self.log);
        let projection = Arc::clone(&self.projection);
        let ids = Arc::clone(&self.ids);
        Box::pin(async move {
            if request.targets.is_empty() {
                return Err(EngineError::Invalid("renew item batch must not be empty"));
            }
            let definition = control.queue_definition(request.shard.clone()).await?;
            if definition.tenant_id != request.shard.tenant_id
                || definition.queue_id != request.shard.queue_id
            {
                return Err(EngineError::Storage(
                    "async lifecycle planner returned the wrong queue definition".to_string(),
                ));
            }
            validate_renew_duration(
                request.now,
                request.new_lease_expires_at,
                definition.max_lease_duration_ms,
            )?;
            projection.admit_mutation(request.shard.clone()).await?;
            projection
                .renew_validate(request.shard.clone(), request.targets.clone(), request.now)
                .await?;
            let epoch = log.current_epoch(request.shard.clone()).await?;
            if request
                .expected_epoch
                .is_some_and(|expected| expected != epoch)
            {
                return Err(EngineError::EpochFenced);
            }
            let item_ids = request
                .targets
                .iter()
                .map(|target| target.item_id)
                .collect::<Vec<_>>();
            let envelope = CommandEnvelope {
                command_id: ids.next_command_id(),
                request_id: None,
                request_fingerprint: None,
                request_outcome: None,
                item_ids: item_ids.clone(),
                command: QueueCommand::RenewLease(RenewLeaseCommand {
                    item_ids,
                    lease_expires_at: request.new_lease_expires_at,
                }),
                checksum: CommandChecksum(0),
                created_at: request.now,
            };
            Ok(AsyncLifecyclePlan::renew(RawCommitRequest::new(
                request.shard,
                vec![envelope],
                epoch,
            )))
        })
    }
}

#[cfg(test)]
mod tests {
    use pqueue_core::UtcTimestamp;

    use super::*;

    #[test]
    fn renewal_duration_is_strictly_future_and_capped() {
        let now = UtcTimestamp::new(10, 0).unwrap();
        assert!(validate_renew_duration(now, UtcTimestamp::new(10, 1).unwrap(), 1).is_ok());
        assert!(validate_renew_duration(now, now, 1).is_err());
        assert!(
            validate_renew_duration(now, UtcTimestamp::new(10, 1_000_001).unwrap(), 1).is_err()
        );
    }
}
