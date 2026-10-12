//! Exact cached-address evidence for native missing-authority classification.
use super::*;
struct PresenceWorker {
    address: QueueAddress,
    projection: Arc<QueueProjection>,
    _input: OwnedQueueBufferCharge,
    _job: Arc<QueueReservation>,
}
impl NativeTransactionWork for PresenceWorker {
    type Output = bool;
    async fn run(&mut self, connection: &Connection) -> QueueResult<bool> {
        // Deliberately omit the discovery hash: corruption of that accelerator
        // must not make already known exact-address evidence disappear.
        Ok(one(
            connection,
            "SELECT id FROM q7_queues WHERE tenant=?1 AND name=?2",
            vec![
                Value::Blob(self.address.tenant.as_bytes().to_vec()),
                Value::Blob(self.address.queue.as_bytes().to_vec()),
            ],
            1,
        )
        .await?
        .is_some())
    }
}
impl QueueProjection {
    pub async fn has_exact_queue(
        self: &Arc<Self>,
        address: QueueAddress,
        job: &Arc<QueueReservation>,
        workers: &ScratchWorkers,
    ) -> QueueResult<bool> {
        let input = job.owned_buffer(
            encoded_len(&address)?
                .checked_mul(4)
                .and_then(|n| n.checked_add(4096))
                .ok_or(QueueError::TooLarge)?,
        )?;
        let mut worker = PresenceWorker {
            address,
            projection: self.clone(),
            _input: input,
            _job: job.clone(),
        };
        workers
            .run(move || {
                let projection = worker.projection.clone();
                crate::tx::block_on_owned_apply(owned_native_transaction(
                    &projection,
                    TransactionBehavior::Deferred,
                    &mut worker,
                ))
            })
            .await
    }
}
