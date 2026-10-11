//! Shared owned-operation admission. The reservation moves into the storage job,
//! never into a caller-only future, and releases only when storage is terminal.
use fireweed_core::queue_contract::{QueueError, QueueResult};
use std::sync::{Arc, Mutex};

#[derive(Debug)]
struct State {
    slots: usize,
    bytes: u64,
    closed: bool,
}
#[derive(Debug)]
pub struct QueueAdmission {
    max_slots: usize,
    max_bytes: u64,
    state: Mutex<State>,
}
#[derive(Debug)]
pub struct QueueReservation {
    admission: Arc<QueueAdmission>,
    bytes: u64,
}
impl QueueAdmission {
    pub fn new(max_slots: usize, max_bytes: u64) -> QueueResult<Arc<Self>> {
        if max_slots == 0 || max_bytes == 0 {
            return Err(QueueError::Invalid(
                "admission limits must be positive".into(),
            ));
        }
        Ok(Arc::new(Self {
            max_slots,
            max_bytes,
            state: Mutex::new(State {
                slots: 0,
                bytes: 0,
                closed: false,
            }),
        }))
    }
    /// Reservation and close are linearized under the same small metadata lock.
    pub fn reserve(self: &Arc<Self>, bytes: u64) -> QueueResult<QueueReservation> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| QueueError::OutcomeUnknown("admission state poisoned".into()))?;
        if state.closed
            || state.slots >= self.max_slots
            || bytes > self.max_bytes.saturating_sub(state.bytes)
        {
            return Err(QueueError::Busy);
        }
        state.slots += 1;
        state.bytes += bytes;
        Ok(QueueReservation {
            admission: Arc::clone(self),
            bytes,
        })
    }
    pub fn close(&self) -> QueueResult<()> {
        self.state
            .lock()
            .map_err(|_| QueueError::OutcomeUnknown("admission state poisoned".into()))?
            .closed = true;
        Ok(())
    }
    pub fn usage(&self) -> QueueResult<(usize, u64)> {
        let state = self
            .state
            .lock()
            .map_err(|_| QueueError::OutcomeUnknown("admission state poisoned".into()))?;
        Ok((state.slots, state.bytes))
    }
}
impl QueueReservation {
    /// Charge loaded records/planning/seal buffers before retaining them. Failure
    /// does not lose the existing reservation or pretend admitted work rolled back.
    pub fn grow(&mut self, additional: u64) -> QueueResult<()> {
        let mut state = self
            .admission
            .state
            .lock()
            .map_err(|_| QueueError::OutcomeUnknown("admission state poisoned".into()))?;
        if additional > self.admission.max_bytes.saturating_sub(state.bytes) {
            return Err(QueueError::OutcomeUnknown(
                "admitted capacity exhausted; retain and resolve owned operation".into(),
            ));
        }
        state.bytes += additional;
        self.bytes += additional;
        Ok(())
    }
}
/// Scoped loaded/planning buffer charge borrows its job reservation, so storage
/// cannot release the job slot while this buffer is still retained.
#[derive(Debug)]
pub struct QueueBufferCharge<'a> {
    reservation: &'a QueueReservation,
    bytes: u64,
}
impl<'a> QueueBufferCharge<'a> {
    /// Transfer two charges from the identical job into one guard without a
    /// release/reacquire interval. This does not increase admitted capacity.
    pub fn merge(mut self, mut other: Self) -> QueueResult<Self> {
        if !std::ptr::eq(self.reservation, other.reservation) {
            return Err(QueueError::OutcomeUnknown(
                "cannot merge charges from different jobs".into(),
            ));
        }
        let combined = self
            .bytes
            .checked_add(other.bytes)
            .ok_or_else(|| QueueError::OutcomeUnknown("combined buffer charge overflow".into()))?;
        self.bytes = combined;
        other.bytes = 0;
        Ok(self)
    }
}
impl QueueReservation {
    pub fn buffer(&self, bytes: u64) -> QueueResult<QueueBufferCharge<'_>> {
        let mut state = self
            .admission
            .state
            .lock()
            .map_err(|_| QueueError::OutcomeUnknown("admission state poisoned".into()))?;
        if bytes > self.admission.max_bytes.saturating_sub(state.bytes) {
            return Err(QueueError::OutcomeUnknown(
                "admitted buffer capacity exhausted; resolve owned operation".into(),
            ));
        }
        state.bytes += bytes;
        Ok(QueueBufferCharge {
            reservation: self,
            bytes,
        })
    }
}
impl Drop for QueueBufferCharge<'_> {
    fn drop(&mut self) {
        if let Ok(mut state) = self.reservation.admission.state.lock() {
            state.bytes -= self.bytes;
        }
    }
}
impl Drop for QueueReservation {
    fn drop(&mut self) {
        if let Ok(mut state) = self.admission.state.lock() {
            state.slots -= 1;
            state.bytes -= self.bytes;
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn merging_same_job_charges_never_releases_capacity_early() {
        let admission = QueueAdmission::new(2, 100).unwrap();
        let first = admission.reserve(10).unwrap();
        let second = admission.reserve(10).unwrap();
        let left = first.buffer(20).unwrap();
        let right = first.buffer(30).unwrap();
        assert_eq!(admission.usage().unwrap(), (2, 70));
        let merged = left.merge(right).unwrap();
        assert_eq!(admission.usage().unwrap(), (2, 70));
        assert!(matches!(
            first.buffer(31),
            Err(QueueError::OutcomeUnknown(_))
        ));
        drop(merged);
        assert_eq!(admission.usage().unwrap(), (2, 20));
        let left = first.buffer(20).unwrap();
        let right = second.buffer(30).unwrap();
        assert!(matches!(
            left.merge(right),
            Err(QueueError::OutcomeUnknown(_))
        ));
        assert_eq!(admission.usage().unwrap(), (2, 20));
    }
    #[test]
    fn transferred_reservation_outlives_caller_and_close() {
        let admission = QueueAdmission::new(1, 100).unwrap();
        let reservation = admission.reserve(40).unwrap();
        let storage_owned = reservation;
        admission.close().unwrap();
        assert!(matches!(admission.reserve(1), Err(QueueError::Busy)));
        assert_eq!(admission.usage().unwrap(), (1, 40));
        drop(storage_owned);
        assert_eq!(admission.usage().unwrap(), (0, 0));
    }
    #[test]
    fn byte_saturation_and_failed_growth_keep_existing_charge() {
        let admission = QueueAdmission::new(2, 100).unwrap();
        let mut reservation = admission.reserve(80).unwrap();
        assert!(matches!(admission.reserve(21), Err(QueueError::Busy)));
        assert!(matches!(
            reservation.grow(21),
            Err(QueueError::OutcomeUnknown(_))
        ));
        assert_eq!(admission.usage().unwrap(), (1, 80));
        reservation.grow(20).unwrap();
        assert_eq!(admission.usage().unwrap(), (1, 100));
        drop(reservation);
        assert_eq!(admission.usage().unwrap(), (0, 0));
    }
    #[test]
    fn temporary_group_buffers_release_without_releasing_owned_job() {
        let admission = QueueAdmission::new(1, 100).unwrap();
        let job = admission.reserve(10).unwrap();
        {
            let _buffer = job.buffer(90).unwrap();
            assert_eq!(admission.usage().unwrap(), (1, 100));
        }
        assert_eq!(admission.usage().unwrap(), (1, 10));
        {
            let _next_group = job.buffer(90).unwrap();
            assert_eq!(admission.usage().unwrap(), (1, 100));
        }
        drop(job);
        assert_eq!(admission.usage().unwrap(), (0, 0));
    }
    #[test]
    fn failed_buffer_charge_keeps_owned_reservation_and_existing_buffers() {
        let admission = QueueAdmission::new(1, 100).unwrap();
        let job = admission.reserve(10).unwrap();
        let retained = job.buffer(80).unwrap();
        assert!(matches!(job.buffer(11), Err(QueueError::OutcomeUnknown(_))));
        assert_eq!(admission.usage().unwrap(), (1, 90));
        drop(retained);
        assert_eq!(admission.usage().unwrap(), (1, 10));
        assert!(matches!(admission.reserve(1), Err(QueueError::Busy)));
        drop(job);
        assert_eq!(admission.usage().unwrap(), (0, 0));
    }
}
