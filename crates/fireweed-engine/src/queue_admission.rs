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
}
