//! Remove every row of one queue in a single transaction. The projection only
//! mirrors the log, so a queue that cannot be trusted is dropped and rebuilt.
use super::*;

struct DropWorker {
    address: QueueAddress,
    projection: Arc<QueueProjection>,
    _input: OwnedQueueBufferCharge,
    _job: Arc<QueueReservation>,
}
impl NativeTransactionWork for DropWorker {
    type Output = ();
    async fn run(&mut self, connection: &Connection) -> QueueResult<()> {
        // Deliberately omit the discovery hash: a damaged accelerator must not
        // keep rows alive.
        let Some(row) = one(
            connection,
            "SELECT id FROM q7_queues WHERE tenant=?1 AND name=?2",
            vec![
                Value::Blob(self.address.tenant.as_bytes().to_vec()),
                Value::Blob(self.address.queue.as_bytes().to_vec()),
            ],
            1,
        )
        .await?
        else {
            return Ok(());
        };
        let queue_id = integer(&row[0])?;
        for sql in [
            "DELETE FROM q7_bodies WHERE record_id IN (SELECT id FROM q7_records WHERE queue_id=?1)",
            "DELETE FROM q7_fields WHERE queue_id=?1",
            "DELETE FROM q7_additional WHERE queue_id=?1",
            "DELETE FROM q7_indexes WHERE queue_id=?1",
            "DELETE FROM q7_records WHERE queue_id=?1",
            "DELETE FROM q7_requests WHERE queue_id=?1",
            "DELETE FROM q7_queues WHERE id=?1",
        ] {
            connection
                .execute(sql, vec![Value::Integer(queue_id)])
                .await
                .map_err(unknown)?;
        }
        Ok(())
    }
}
impl QueueProjection {
    /// Delete the queue's rows from all seven tables atomically; absent is a no-op.
    pub async fn drop_queue(
        self: &Arc<Self>,
        address: QueueAddress,
        job: &Arc<QueueReservation>,
        workers: &ScratchWorkers,
    ) -> QueueResult<()> {
        let input = job.owned_buffer(
            encoded_len(&address)?
                .checked_mul(4)
                .and_then(|n| n.checked_add(4096))
                .ok_or(QueueError::TooLarge)?,
        )?;
        let mut worker = DropWorker {
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
                    TransactionBehavior::Immediate,
                    &mut worker,
                ))
            })
            .await
    }
}

#[cfg(test)]
mod tests;
