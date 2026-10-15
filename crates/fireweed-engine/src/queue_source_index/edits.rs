//! Shared provisional edit ownership. This type never carries a compiled seal.
use super::{QueueSourceCompileLimits, QueueSourceCut, QueueSourceKey};
use crate::{
    queue_admission::{OwnedQueueBufferCharge, QueueReservation},
    queue_log_record::OwnedQueueLogRecord,
    queue_storage_layout,
};
use fireweed_core::queue_contract::{QueueError, QueueResult, encoded_len};
use serde::Serialize;
use std::{collections::BTreeMap, sync::Arc};

pub(super) struct SourceEdits<'a> {
    record: &'a OwnedQueueLogRecord,
    cut: QueueSourceCut<'a>,
    entries: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    limits: QueueSourceCompileLimits,
    input_bytes: u64,
    label: &'static str,
    // All keys/values and map nodes drop before their retained admission.
    charge: OwnedQueueBufferCharge,
}
impl<'a> SourceEdits<'a> {
    pub fn new(
        record: &'a OwnedQueueLogRecord,
        cut: QueueSourceCut<'a>,
        job: &Arc<QueueReservation>,
        limits: QueueSourceCompileLimits,
        label: &'static str,
    ) -> QueueResult<Self> {
        if !record.belongs_to(job) || !cut.belongs_to(job) {
            return Err(QueueError::OutcomeUnknown(
                "source edit input scope differs".into(),
            ));
        }
        Ok(Self {
            record,
            cut,
            entries: BTreeMap::new(),
            limits,
            input_bytes: 0,
            label,
            charge: job.owned_buffer(1024 + std::mem::size_of::<Self>() as u64)?,
        })
    }
    pub fn verify(
        &self,
        record: &OwnedQueueLogRecord,
        cut: &QueueSourceCut<'_>,
        job: &QueueReservation,
    ) -> QueueResult<()> {
        if !std::ptr::eq(self.record, record)
            || !self.cut.same_cut(cut)
            || !std::ptr::eq(self.charge.reservation(), job)
        {
            return Err(QueueError::OutcomeUnknown(
                "source edit output scope differs".into(),
            ));
        }
        Ok(())
    }
    pub fn edits(&self) -> impl ExactSizeIterator<Item = (&[u8], Option<&[u8]>)> {
        self.entries
            .iter()
            .map(|(key, value)| (key.as_slice(), value.as_deref()))
    }
    fn admit<T: Serialize>(&mut self, key_bytes: usize, value: Option<&T>) -> QueueResult<()> {
        let bytes = (key_bytes as u64)
            .checked_add(value.map_or(Ok(0), encoded_len)?)
            .ok_or(QueueError::TooLarge)?;
        let total = self
            .input_bytes
            .checked_add(bytes)
            .ok_or(QueueError::TooLarge)?;
        if self.entries.len() as u64 >= self.limits.edits || total > self.limits.input_bytes {
            return Err(QueueError::OutcomeUnknown(
                "source edit output exceeds admitted limits".into(),
            ));
        }
        let retained = bytes
            .checked_mul(4)
            .and_then(|n| n.checked_add(1024))
            .ok_or(QueueError::TooLarge)?;
        self.charge.grow(retained).map_err(|error| match error {
            QueueError::OutcomeUnknown(message) => {
                QueueError::OutcomeUnknown(format!("{} output admission: {message}", self.label))
            }
            other => other,
        })?;
        self.input_bytes = total;
        Ok(())
    }
    fn store<T: Serialize>(&mut self, key: Vec<u8>, value: Option<&T>) -> QueueResult<()> {
        if self.entries.contains_key(&key) {
            return Err(QueueError::OutcomeUnknown(
                "duplicate provisional source edit".into(),
            ));
        }
        self.entries
            .insert(key, value.map(queue_storage_layout::encode).transpose()?);
        Ok(())
    }
    pub fn insert<T: Serialize>(
        &mut self,
        key: QueueSourceKey<'_>,
        value: Option<&T>,
    ) -> QueueResult<()> {
        self.admit(key.encoded_len()?, value)?;
        self.store(key.encode()?, value)
    }
    pub fn insert_encoded<T: Serialize>(
        &mut self,
        key: &[u8],
        value: Option<&T>,
    ) -> QueueResult<()> {
        self.admit(key.len(), value)?;
        self.store(key.to_vec(), value)
    }
}
