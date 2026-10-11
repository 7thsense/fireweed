//! Exact original-genesis binding for every native queue snapshot operation.
use super::*;
pub(super) struct NativeQueueSnapshot {
    pub definition: AdmittedQueueDefinition,
    pub queue_id: i64,
    pub next_offset: u64,
    pub insertion_sequence: u64,
    pub last_record_epoch: u64,
}
impl NativeQueueSnapshot {
    pub(super) fn validate_claim_input(&self, input: &ClaimReplayInput) -> QueueResult<()> {
        input
            .request
            .validate(self.definition.schema(), &input.limits)?;
        self.validate_point(
            &input.genesis,
            &input.request.queue,
            (
                input.required_next_offset,
                input.required_insertion_sequence,
                input.required_last_record_epoch,
            ),
        )
    }
    pub(super) fn validate_point(
        &self,
        genesis: &OwnedQueueLogRecord,
        queue: &QueueAddress,
        required: (u64, u64, u64),
    ) -> QueueResult<()> {
        let view = genesis.view()?;
        let address = view.address();
        if queue.tenant != address.tenant
            || queue.queue != address.queue
            || (
                self.next_offset,
                self.insertion_sequence,
                self.last_record_epoch,
            ) != required
        {
            return Err(unknown("queue snapshot authority mismatch"));
        }
        Ok(())
    }
}
pub(super) async fn read_snapshot(
    connection: &Connection,
    genesis: &OwnedQueueLogRecord,
    job: &Arc<QueueReservation>,
) -> QueueResult<Option<NativeQueueSnapshot>> {
    if !genesis.belongs_to(job) {
        return Err(unknown("snapshot genesis belongs to another operation"));
    }
    let view = genesis.view()?;
    if view.format() != QueueLogFormat::ChecksummedV2 {
        return Err(unknown("snapshot requires checked genesis"));
    }
    let QueueLogPayload::Genesis(genesis) = view.payload() else {
        return Err(unknown("snapshot authority is not genesis"));
    };
    let definition = genesis.validate_definition(job)?;
    let address = genesis.address();
    let digest = discovery_digest(&encode(&(address.tenant, address.queue))?);
    let row = one(connection,"SELECT CASE WHEN typeof(id)='integer' THEN id ELSE NULL END,CASE WHEN typeof(applied_next_offset)='integer' THEN applied_next_offset ELSE NULL END,CASE WHEN typeof(next_sequence)='blob' AND length(next_sequence)=8 THEN next_sequence ELSE NULL END,CASE WHEN typeof(definition)='blob' AND length(definition)=length(?4) THEN definition=?4 ELSE 0 END,CASE WHEN typeof(cursor_secret)='blob' AND length(cursor_secret)=32 THEN cursor_secret=?5 ELSE 0 END,CASE WHEN typeof(last_record_epoch)='blob' AND length(last_record_epoch)=8 THEN last_record_epoch ELSE NULL END FROM q7_queues WHERE address_hash=?1 AND tenant=?2 AND name=?3",vec![blob(&digest),blob(address.tenant.as_bytes()),blob(address.queue.as_bytes()),blob(genesis.definition_bytes()),blob(genesis.cursor_secret())],6).await?;
    let Some(row) = row else {
        return Ok(None);
    };
    if integer(&row[3])? != 1 || integer(&row[4])? != 1 {
        return Err(unknown("projection genesis authority mismatch"));
    }
    let next_offset = u64::try_from(integer(&row[1])?).map_err(unknown)?;
    let last_record_epoch = unsigned(&row[5])?;
    if next_offset == 0 || last_record_epoch == 0 || last_record_epoch < view.epoch() {
        return Err(unknown("invalid projection snapshot progress"));
    }
    Ok(Some(NativeQueueSnapshot {
        definition,
        queue_id: integer(&row[0])?,
        next_offset,
        insertion_sequence: unsigned(&row[2])?,
        last_record_epoch,
    }))
}
