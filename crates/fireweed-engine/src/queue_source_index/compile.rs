//! Sealed compiler output: resolved record bytes and semantic edits share one
//! original cut and Root operation. No caller can construct a usable partial set.
use super::{QueueSourceCut, QueueSourceGenesis, QueueSourceKey, QueueSourceSpan};
use crate::{
    queue_admission::{OwnedQueueBufferCharge, QueueReservation},
    queue_log_record::{CheckedQueueLogRecord, OwnedQueueLogRecord, QueueLogPayload},
    queue_schema_codec::AdmittedQueueDefinition,
    queue_storage_layout,
};
use fireweed_core::queue_contract::{Lifecycle, QueueError, QueueResult, encoded_len};
use std::sync::Arc;
fn unknown(message: &str) -> QueueError {
    QueueError::OutcomeUnknown(format!("queue source compiler: {message}"))
}

#[derive(Clone, Copy)]
pub struct QueueSourceCompileLimits {
    pub edits: u64,
    pub input_bytes: u64,
}
struct OwnedEdit {
    key: Vec<u8>,
    value: Option<Vec<u8>>,
}
pub struct QueueSourceCompiled<'a> {
    record: &'a OwnedQueueLogRecord,
    cut: QueueSourceCut<'a>,
    edits: Vec<OwnedEdit>,
    _charge: OwnedQueueBufferCharge,
}
impl<'a> QueueSourceCompiled<'a> {
    pub fn record(&self) -> &'a OwnedQueueLogRecord {
        self.record
    }
    pub fn edits(&self) -> impl ExactSizeIterator<Item = (&[u8], Option<&[u8]>)> + Clone {
        self.edits
            .iter()
            .map(|edit| (edit.key.as_slice(), edit.value.as_deref()))
    }
    pub fn verify(&self, cut: &QueueSourceCut<'_>, job: &QueueReservation) -> QueueResult<()> {
        if !self.cut.same_cut(cut)
            || !self.record.belongs_to(job)
            || !std::ptr::eq(self._charge.reservation(), job)
        {
            return Err(unknown("compiled record, cut or operation differs"));
        }
        Ok(())
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
    let total_edits = 6u64;
    let zero_len = encoded_len(&0u64)?;
    let genesis_len = QueueSourceKey::Genesis.encoded_len()? as u64 + encoded_len(&fact)?;
    let mut input_bytes = genesis_len
        .checked_add(QueueSourceKey::Sequence.encoded_len()? as u64)
        .and_then(|n| n.checked_add(zero_len))
        .ok_or(QueueError::TooLarge)?;
    for lifecycle in counters {
        input_bytes = input_bytes
            .checked_add(QueueSourceKey::LifecycleCount(lifecycle).encoded_len()? as u64)
            .and_then(|n| n.checked_add(zero_len))
            .ok_or(QueueError::TooLarge)?;
    }
    if total_edits > limits.edits || input_bytes > limits.input_bytes {
        return Err(unknown("generated genesis edits exceed admitted limits"));
    }
    let capacity = input_bytes
        .checked_mul(4)
        .and_then(|n| n.checked_add(total_edits * std::mem::size_of::<OwnedEdit>() as u64))
        .and_then(|n| n.checked_add(std::mem::size_of::<QueueSourceCompiled<'_>>() as u64))
        .ok_or(QueueError::TooLarge)?;
    let charge = job.owned_buffer(capacity)?;
    let mut edits = Vec::with_capacity(total_edits as usize);
    edits.push(OwnedEdit {
        key: QueueSourceKey::Genesis.encode()?,
        value: Some(queue_storage_layout::encode(&fact)?),
    });
    edits.push(OwnedEdit {
        key: QueueSourceKey::Sequence.encode()?,
        value: Some(queue_storage_layout::encode(&0u64)?),
    });
    for lifecycle in counters {
        edits.push(OwnedEdit {
            key: QueueSourceKey::LifecycleCount(lifecycle).encode()?,
            value: Some(queue_storage_layout::encode(&0u64)?),
        });
    }
    edits.sort_unstable_by(|a, b| a.key.cmp(&b.key));
    debug_assert_eq!(
        edits
            .iter()
            .map(|edit| edit.key.len() as u64 + edit.value.as_ref().map_or(0, |v| v.len() as u64))
            .sum::<u64>(),
        input_bytes
    );
    Ok(QueueSourceCompiled {
        record: checked.owner(),
        cut,
        edits,
        _charge: charge,
    })
}
