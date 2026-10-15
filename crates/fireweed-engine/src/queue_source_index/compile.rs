//! Sealed compiler output: resolved record bytes and semantic edits share one
//! original cut and Root operation. No caller can construct a usable partial set.
use super::{QueueSourceCut, QueueSourceGenesis, QueueSourceKey, QueueSourceSpan};
use crate::{
    queue_admission::QueueReservation,
    queue_log_record::{CheckedQueueLogRecord, OwnedQueueLogRecord, QueueLogPayload},
    queue_schema_codec::AdmittedQueueDefinition,
};
use fireweed_core::queue_contract::{Lifecycle, QueueError, QueueResult};
use std::sync::Arc;
fn unknown(message: &str) -> QueueError {
    QueueError::OutcomeUnknown(format!("queue source compiler: {message}"))
}

#[derive(Clone, Copy)]
pub struct QueueSourceCompileLimits {
    pub edits: u64,
    pub input_bytes: u64,
}
pub struct QueueSourceCompiled<'a> {
    output: super::edits::SourceEdits<'a>,
}
impl<'a> QueueSourceCompiled<'a> {
    pub fn record(&self) -> &'a OwnedQueueLogRecord {
        self.output.record()
    }
    pub fn edits(&self) -> impl ExactSizeIterator<Item = (&[u8], Option<&[u8]>)> + Clone {
        self.output.edits()
    }
    pub fn verify(&self, cut: &QueueSourceCut<'_>, job: &QueueReservation) -> QueueResult<()> {
        self.output.verify(self.record(), cut, job)?;
        self.output.verify_complete()
    }
    pub(super) fn from_publication(
        complete: super::predecessor::CompleteSourcePublication<'a>,
    ) -> Self {
        Self {
            output: complete.into_output(),
        }
    }
}
/// Compile genuine empty-log genesis. All initial facts publish in the SAME
/// original-token CAS as offset zero; no legacy-genesis-then-promotion window.
/// Other resolved record variants are compiled by the subsequent command path.
pub fn compile_genesis<'a>(
    checked: &CheckedQueueLogRecord<'a>,
    cut: QueueSourceCut<'a>,
    definition: &AdmittedQueueDefinition,
    limits: QueueSourceCompileLimits,
    job: &Arc<QueueReservation>,
) -> QueueResult<QueueSourceCompiled<'a>> {
    cut.validate_publication(checked, job)?;
    let QueueLogPayload::Genesis(genesis) = checked.view().payload() else {
        return Err(unknown("genesis compiler received another record variant"));
    };
    if !definition.belongs_to(job) {
        return Err(unknown("genesis definition belongs to another operation"));
    }
    definition.pin().verify(genesis.definition_bytes())?;
    let payload = checked.payload_interval()?;
    let fact = QueueSourceGenesis {
        source: QueueSourceSpan::from_checked(checked, 0, &checked.owner().bytes()[payload])?,
        definition_pin: definition.pin(),
    };
    let counters = [
        Lifecycle::Pending,
        Lifecycle::Leased,
        Lifecycle::Completed,
        Lifecycle::Failed,
    ];
    let mut output = super::edits::SourceEdits::new(checked.owner(), cut, job, limits, "genesis")?;
    output.insert(QueueSourceKey::Genesis, Some(&fact))?;
    output.insert(QueueSourceKey::Sequence, Some(&0u64))?;
    for lifecycle in counters {
        output.insert(QueueSourceKey::LifecycleCount(lifecycle), Some(&0u64))?;
    }
    output.verify_complete()?;
    Ok(QueueSourceCompiled { output })
}
