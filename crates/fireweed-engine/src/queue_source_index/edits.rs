//! Shared provisional edit ownership. This type never carries a compiled seal.
use super::{QueueSourceCompileLimits, QueueSourceCut, QueueSourceKey};
use crate::{
    queue_admission::{OwnedQueueBufferCharge, QueueReservation},
    queue_log_record::OwnedQueueLogRecord,
    queue_storage_layout,
};
use fireweed_core::queue_contract::{QueueError, QueueResult, encoded_len};
use serde::Serialize;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

/// One attempted compilation consumes this quota monotonically. A failed
/// component cannot refund capacity or leave siblings eligible for a seal.
pub(super) struct OutputQuota<'a> {
    record: &'a OwnedQueueLogRecord,
    cut: QueueSourceCut<'a>,
    state: Mutex<QuotaState>,
    _charge: OwnedQueueBufferCharge,
}
struct QuotaState {
    limits: QueueSourceCompileLimits,
    edits: u64,
    bytes: u64,
    poisoned: bool,
}
impl<'a> OutputQuota<'a> {
    pub(super) fn new(
        record: &'a OwnedQueueLogRecord,
        cut: QueueSourceCut<'a>,
        job: &Arc<QueueReservation>,
        limits: QueueSourceCompileLimits,
    ) -> QueueResult<Arc<Self>> {
        if !record.belongs_to(job) || !cut.belongs_to(job) {
            return Err(QueueError::OutcomeUnknown(
                "source quota input scope differs".into(),
            ));
        }
        let charge = job.owned_buffer(
            (std::mem::size_of::<Self>() + 2 * std::mem::size_of::<usize>()) as u64,
        )?;
        Ok(Arc::new(Self {
            record,
            cut,
            state: Mutex::new(QuotaState {
                limits,
                edits: 0,
                bytes: 0,
                poisoned: false,
            }),
            _charge: charge,
        }))
    }
    fn poison(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.poisoned = true;
        }
    }
    fn check(&self) -> QueueResult<()> {
        let state = self
            .state
            .lock()
            .map_err(|_| QueueError::OutcomeUnknown("source output quota lock poisoned".into()))?;
        if state.poisoned {
            return Err(QueueError::OutcomeUnknown(
                "source output quota poisoned".into(),
            ));
        }
        Ok(())
    }
    fn debit(&self, bytes: u64) -> QueueResult<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| QueueError::OutcomeUnknown("source output quota lock poisoned".into()))?;
        if state.poisoned {
            return Err(QueueError::OutcomeUnknown(
                "source output quota poisoned".into(),
            ));
        }
        let next = state
            .edits
            .checked_add(1)
            .zip(state.bytes.checked_add(bytes));
        let Some((edits, bytes)) = next.filter(|(edits, bytes)| {
            *edits <= state.limits.edits && *bytes <= state.limits.input_bytes
        }) else {
            state.poisoned = true;
            return Err(QueueError::OutcomeUnknown(
                "source edit output exceeds admitted limits".into(),
            ));
        };
        state.edits = edits;
        state.bytes = bytes;
        Ok(())
    }
}

/// Serialization is outside the quota mutex. A caught unwind still invalidates
/// every sibling, just like an ordinary error, without locking across callbacks.
struct OutputAttempt<'q, 'a> {
    quota: &'q OutputQuota<'a>,
    armed: bool,
}
impl Drop for OutputAttempt<'_, '_> {
    fn drop(&mut self) {
        if self.armed {
            self.quota.poison();
        }
    }
}

pub(super) struct SourceEdits<'a> {
    record: &'a OwnedQueueLogRecord,
    cut: QueueSourceCut<'a>,
    entries: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    retained_charges: Vec<OwnedQueueBufferCharge>,
    quota: Arc<OutputQuota<'a>>,
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
        let quota = OutputQuota::new(record, cut, job, limits)?;
        Self::with_quota(record, cut, job, quota, label)
    }
    pub(super) fn with_quota(
        record: &'a OwnedQueueLogRecord,
        cut: QueueSourceCut<'a>,
        job: &Arc<QueueReservation>,
        quota: Arc<OutputQuota<'a>>,
        label: &'static str,
    ) -> QueueResult<Self> {
        if !std::ptr::eq(record, quota.record)
            || !cut.same_cut(&quota.cut)
            || !std::ptr::eq(job.as_ref(), quota._charge.reservation())
        {
            quota.poison();
            return Err(QueueError::OutcomeUnknown(
                "source quota output scope differs".into(),
            ));
        }
        quota.check()?;
        let charge = match job.owned_buffer(1024 + std::mem::size_of::<Self>() as u64) {
            Ok(charge) => charge,
            Err(error) => {
                quota.poison();
                return Err(error);
            }
        };
        Ok(Self {
            record,
            cut,
            entries: BTreeMap::new(),
            retained_charges: Vec::new(),
            quota,
            label,
            charge,
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
        self.quota.check()
    }
    pub fn edits(&self) -> impl ExactSizeIterator<Item = (&[u8], Option<&[u8]>)> + Clone {
        self.entries
            .iter()
            .map(|(key, value)| (key.as_slice(), value.as_deref()))
    }
    pub(super) fn record(&self) -> &'a OwnedQueueLogRecord {
        self.record
    }
    /// Consume both maps. No raw or partial output can manufacture a seal.
    pub(super) fn merge(mut self, mut other: Self) -> QueueResult<Self> {
        let quota = Arc::clone(&self.quota);
        let mut attempt = OutputAttempt {
            quota: &quota,
            armed: true,
        };
        self.verify(self.record, &self.cut, self.charge.reservation())?;
        other.verify(self.record, &self.cut, self.charge.reservation())?;
        if !Arc::ptr_eq(&self.quota, &other.quota) {
            other.quota.poison();
            return Err(QueueError::OutcomeUnknown(
                "source merge belongs to another compilation attempt".into(),
            ));
        }
        if other
            .entries
            .keys()
            .any(|key| self.entries.contains_key(key))
        {
            return Err(QueueError::OutcomeUnknown(
                "duplicate complete source edit".into(),
            ));
        }
        let guards = other
            .retained_charges
            .len()
            .checked_add(1)
            .ok_or(QueueError::TooLarge)?;
        let guard_bytes = (guards as u64)
            .checked_mul(std::mem::size_of::<OwnedQueueBufferCharge>() as u64)
            .and_then(|n| n.checked_mul(4))
            .ok_or(QueueError::TooLarge)?;
        let extra = (other.entries.len() as u64)
            .checked_mul(1024)
            .and_then(|n| n.checked_add(guard_bytes))
            .and_then(|n| n.checked_add(1024))
            .ok_or(QueueError::TooLarge)?;
        self.charge.grow(extra).map_err(|error| match error {
            QueueError::OutcomeUnknown(message) => {
                QueueError::OutcomeUnknown(format!("source merge output admission: {message}"))
            }
            other => other,
        })?;
        self.retained_charges
            .try_reserve_exact(guards)
            .map_err(|_| QueueError::TooLarge)?;
        for (key, value) in std::mem::take(&mut other.entries) {
            self.entries.insert(key, value);
        }
        self.retained_charges.append(&mut other.retained_charges);
        self.retained_charges.push(other.charge);
        attempt.armed = false;
        Ok(self)
    }
    /// A missing populated component cannot disappear behind its nonrefunded
    /// quota debit. Typed completion proofs additionally cover empty components.
    pub(super) fn verify_complete(&self) -> QueueResult<()> {
        self.quota.check()?;
        let bytes = self.entries.iter().try_fold(0u64, |sum, (key, value)| {
            sum.checked_add(key.len() as u64)
                .and_then(|n| n.checked_add(value.as_ref().map_or(0, |value| value.len() as u64)))
                .ok_or(QueueError::TooLarge)
        })?;
        let state =
            self.quota.state.lock().map_err(|_| {
                QueueError::OutcomeUnknown("source output quota lock poisoned".into())
            })?;
        if state.edits != self.entries.len() as u64 || state.bytes != bytes {
            drop(state);
            self.quota.poison();
            return Err(QueueError::OutcomeUnknown(
                "complete source output differs from compilation debits".into(),
            ));
        }
        Ok(())
    }
    fn admit<T: Serialize>(&mut self, key_bytes: usize, value: Option<&T>) -> QueueResult<()> {
        let bytes = (key_bytes as u64)
            .checked_add(value.map_or(Ok(0), encoded_len)?)
            .ok_or(QueueError::TooLarge)?;
        self.quota.debit(bytes)?;
        let retained = bytes
            .checked_mul(4)
            .and_then(|n| n.checked_add(1024))
            .ok_or(QueueError::TooLarge)?;
        self.charge.grow(retained).map_err(|error| {
            self.quota.poison();
            match error {
                QueueError::OutcomeUnknown(message) => QueueError::OutcomeUnknown(format!(
                    "{} output admission: {message}",
                    self.label
                )),
                other => other,
            }
        })?;
        Ok(())
    }
    fn store<T: Serialize>(&mut self, key: Vec<u8>, value: Option<&T>) -> QueueResult<()> {
        if self.entries.contains_key(&key) {
            self.quota.poison();
            return Err(QueueError::OutcomeUnknown(
                "duplicate provisional source edit".into(),
            ));
        }
        let encoded = value
            .map(queue_storage_layout::encode)
            .transpose()
            .inspect_err(|_| self.quota.poison())?;
        self.entries.insert(key, encoded);
        Ok(())
    }
    pub fn insert<T: Serialize>(
        &mut self,
        key: QueueSourceKey<'_>,
        value: Option<&T>,
    ) -> QueueResult<()> {
        let quota = Arc::clone(&self.quota);
        let mut attempt = OutputAttempt {
            quota: &quota,
            armed: true,
        };
        let result = (|| {
            self.admit(key.encoded_len()?, value)?;
            self.store(key.encode()?, value)
        })();
        if result.is_ok() {
            attempt.armed = false;
        }
        result
    }
    pub fn insert_encoded<T: Serialize>(
        &mut self,
        key: &[u8],
        value: Option<&T>,
    ) -> QueueResult<()> {
        let quota = Arc::clone(&self.quota);
        let mut attempt = OutputAttempt {
            quota: &quota,
            armed: true,
        };
        let result = (|| {
            self.admit(key.len(), value)?;
            self.store(key.to_vec(), value)
        })();
        if result.is_ok() {
            attempt.armed = false;
        }
        result
    }
}

#[cfg(test)]
mod tests;
