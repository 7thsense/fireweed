//! Full-record hydration from the ORIGINAL immutable capture, under one ledger.
use super::*;

/// Limits on accepted input sizes, not estimates of SDK heap usage. Identity
/// assembly, expected-key construction and provider scratch use the separately
/// admitted SDK pool; whole-process/RSS qualification must include that pool.
#[derive(Clone, Copy)]
pub struct QueueNativeSourceRecordLimits {
    pub payload_bytes: u64,
    pub logical_identity_bytes: u64,
    pub physical_identity_bytes: u64,
    pub frame_bytes: u64,
}
impl QueueNativeSourceRecordLimits {
    pub(super) fn validate(self) -> QueueResult<()> {
        if self.payload_bytes == 0
            || self.logical_identity_bytes == 0
            || self.physical_identity_bytes == 0
            || self.frame_bytes == 0
        {
            return Err(unknown("source record limits must be positive"));
        }
        Ok(())
    }
}
impl QueueNativeCaptured {
    /// This read never refreshes, acquires a writer or treats a missing record
    /// inside the captured range as absence. Source-cell/span validation belongs
    /// to the caller before interpreting the returned checked FWQL2 record.
    pub async fn read_source_record(
        &self,
        log: &QueueNativeLog,
        offset: u64,
        limits: QueueNativeSourceRecordLimits,
        budget: &Arc<QueueNativeSourceBudget>,
        job: &Arc<QueueReservation>,
    ) -> QueueResult<OwnedQueueLogRecord> {
        self.semantic_scope(log, job)?;
        budget.scope(job)?;
        limits.validate()?;
        if offset < self.head().bounds().0 || offset >= self.head().bounds().1 {
            return Err(unknown("source record offset outside captured range"));
        }
        let metadata = ownership::shared_charge(job, 65536)?;
        let planning = log.log.with_operation_owner(ownership::physical_owner([
            self.namespace._charge.clone(),
            self.semantic_identity.clone(),
            metadata.clone(),
        ])?);
        // Scratch/owner admission may fail without spending an attempt. Once
        // dispatched, even a missing/invalid entry consumes this request's work.
        budget.grant_record_attempt()?;
        let plan = planning
            .plan_read(&self.cut.cut, offset)
            .await
            .map_err(unknown)?
            .ok_or_else(|| unknown("captured source record is missing"))?;
        drop(planning);
        if plan.base_offset() != offset || plan.record_count() != 1 {
            return Err(unknown("source record planned logical range differs"));
        }
        let payload_bytes =
            u64::try_from(plan.payload_bytes()).map_err(|_| QueueError::TooLarge)?;
        let requirements = plan.requirements().map_err(unknown)?;
        if payload_bytes > limits.payload_bytes
            || requirements.logical_identity_bytes() > limits.logical_identity_bytes
            || requirements.physical_identity_bytes() > limits.physical_identity_bytes
            || requirements.frame_bytes() > limits.frame_bytes
        {
            return Err(QueueError::TooLarge);
        }
        budget.grant_hydration(requirements)?;
        // NativeRecord::into_bytes retains the ENTIRE native frame behind its
        // payload view. Never fund that returned backing storage by payload size.
        let capacity = requirements
            .frame_bytes()
            .checked_mul(4)
            .and_then(|bytes| bytes.checked_add(4096))
            .ok_or(QueueError::TooLarge)?;
        let output = ownership::shared_charge(job, capacity)?;
        let hydration = log.log.with_operation_owner(ownership::physical_owner([
            self.namespace._charge.clone(),
            self.semantic_identity.clone(),
            metadata.clone(),
            output.clone(),
        ])?);
        let limit = plan.payload_bytes();
        let result = hydration.read_planned(plan, limit).await;
        // Physical retirement precedes removing the independent scoped owner.
        drop(hydration);
        let record = result.map_err(read_error)?;
        let view = view_queue_log_record(record.bytes())
            .map_err(|error| unknown(format!("invalid stored source record: {error:?}")))?;
        if record.base_offset() != offset
            || record.record_count() != 1
            || view.format() != QueueLogFormat::ChecksummedV2
            || view.epoch() > self.head().epoch()
            || view.address() != view_address(&self.namespace.address)?
        {
            return Err(unknown(
                "source record scope, format, range or epoch differs",
            ));
        }
        let output = match Arc::try_unwrap(output) {
            Ok(charge) => charge,
            Err(remaining) => {
                drop(record);
                drop(remaining);
                return Err(unknown("source record retained physical output ownership"));
            }
        };
        OwnedQueueLogRecord::from_admitted_bytes(record.into_bytes(), output, job)
    }
}
