//! Recovery retains actual source authority and persisted queue definition.
use super::*;

#[derive(Clone, Copy)]
pub struct QueueNativeRecoveryLimits {
    pub read: QueueNativeSourceReadLimits,
    pub record: QueueNativeSourceRecordLimits,
}
enum RecoverySource {
    Captured(QueueNativeCaptured),
    Acquired(QueueNativeAcquired),
}
impl RecoverySource {
    fn as_source(&self) -> &QueueNativeCaptured {
        match self {
            Self::Captured(source) => source,
            Self::Acquired(source) => source.as_source(),
        }
    }
}
/// Owns the exact source used for both header points and persisted record zero.
/// Constructors consume that source; a failed constructor is terminal for it.
pub struct QueueNativeRecovered {
    // Retire persisted bytes and admitted schema before their source and ledger.
    genesis: OwnedQueueLogRecord,
    definition: AdmittedQueueDefinition,
    sequence: u64,
    source: RecoverySource,
    budget: Arc<QueueNativeSourceBudget>,
}
impl QueueNativeRecovered {
    pub fn as_source(&self) -> &QueueNativeCaptured {
        self.source.as_source()
    }
    pub fn as_acquired(&self) -> Option<&QueueNativeAcquired> {
        match &self.source {
            RecoverySource::Acquired(source) => Some(source),
            RecoverySource::Captured(_) => None,
        }
    }
    pub fn genesis_record(&self) -> &OwnedQueueLogRecord {
        &self.genesis
    }
    pub fn definition(&self) -> &AdmittedQueueDefinition {
        &self.definition
    }
    pub fn sequence(&self) -> u64 {
        self.sequence
    }
    pub fn budget(&self) -> &Arc<QueueNativeSourceBudget> {
        &self.budget
    }
}
impl QueueNativeLog {
    pub async fn recover_captured(
        &self,
        source: QueueNativeCaptured,
        budget: Arc<QueueNativeSourceBudget>,
        limits: QueueNativeRecoveryLimits,
        job: &Arc<QueueReservation>,
    ) -> QueueResult<QueueNativeRecovered> {
        self.recover_source(RecoverySource::Captured(source), budget, limits, job)
            .await
    }
    pub async fn recover_acquired(
        &self,
        source: QueueNativeAcquired,
        budget: Arc<QueueNativeSourceBudget>,
        limits: QueueNativeRecoveryLimits,
        job: &Arc<QueueReservation>,
    ) -> QueueResult<QueueNativeRecovered> {
        self.recover_source(RecoverySource::Acquired(source), budget, limits, job)
            .await
    }
    async fn recover_source(
        &self,
        source: RecoverySource,
        budget: Arc<QueueNativeSourceBudget>,
        limits: QueueNativeRecoveryLimits,
        job: &Arc<QueueReservation>,
    ) -> QueueResult<QueueNativeRecovered> {
        let captured = source.as_source();
        captured.semantic_scope(self, job)?;
        budget.scope(job)?;
        limits.read.validate()?;
        limits.record.validate()?;
        let (start, through) = captured.head().bounds();
        if start != 0 || through == 0 {
            return Err(unknown(
                "recovery source does not contain genesis offset zero",
            ));
        }
        budget.ensure_adoption_capacity(limits.read.page.work)?;
        let header = {
            let reader =
                QueueNativeSourceReader::new(captured, self, job, budget.clone(), limits.read)?;
            fireweed_engine::queue_source_index::load_source_header(&reader, job).await?
        };
        let genesis = captured
            .read_source_record(self, 0, limits.record, &budget, job)
            .await?;
        let definition = {
            let checked = genesis
                .checked()
                .map_err(|error| unknown(format!("invalid recovery genesis: {error:?}")))?;
            header.genesis().verify_record(&checked, 0)?;
            let QueueLogPayload::Genesis(value) = checked.view().payload() else {
                return Err(unknown("recovery record zero is not genesis"));
            };
            value
                .validate_definition(job)
                .map_err(|error| unknown(format!("invalid recovery definition: {error:?}")))?
        };
        Ok(QueueNativeRecovered {
            genesis,
            definition,
            sequence: header.sequence(),
            source,
            budget,
        })
    }
}
