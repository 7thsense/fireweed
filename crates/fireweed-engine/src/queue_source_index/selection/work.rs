//! Local selection work is distinct from source SDK I/O and resident/disk
//! admission. One ledger is retained across captures and physical worker calls.
use crate::queue_admission::{OwnedQueueBufferCharge, QueueReservation};
use fireweed_core::queue_contract::{QueueError, QueueResult};
use std::sync::{Arc, Mutex};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct QueueSourceSelectionWork {
    /// Every visited/probed row, including duplicates and failed qualifications.
    pub rows: u64,
    /// Logical bytes copied into batches/tuples, not a resident-memory bound.
    pub copied_bytes: u64,
    /// Physical scratch steps. Bulk algebra must also charge a checked row-work
    /// ceiling; counting one SQL statement does not bound its internal scan.
    pub scratch_steps: u64,
}
/// Grant before dispatch/copy. Successful grants are never returned, including
/// when the subsequent operation fails, its response is dropped, or a fresh
/// source capture is required. Keep this SAME Arc through all request attempts.
pub struct QueueSourceSelectionBudget {
    remaining: Mutex<QueueSourceSelectionWork>,
    _charge: OwnedQueueBufferCharge,
}
impl QueueSourceSelectionBudget {
    pub fn new(
        limits: QueueSourceSelectionWork,
        job: &Arc<QueueReservation>,
    ) -> QueueResult<Arc<Self>> {
        let charge = job.owned_buffer(
            (std::mem::size_of::<Self>() + 2 * std::mem::size_of::<usize>()) as u64,
        )?;
        Ok(Arc::new(Self {
            remaining: Mutex::new(limits),
            _charge: charge,
        }))
    }
    fn scope(&self, job: &QueueReservation) -> QueueResult<()> {
        if !std::ptr::eq(self._charge.reservation(), job) {
            return Err(unknown("work ledger belongs to another operation"));
        }
        Ok(())
    }
    /// All dimensions are debited atomically. A denied grant dispatches nothing
    /// and cannot partially spend sibling dimensions. No refund/reset API exists.
    pub fn grant(&self, job: &QueueReservation, work: QueueSourceSelectionWork) -> QueueResult<()> {
        self.scope(job)?;
        let mut remaining = self
            .remaining
            .lock()
            .map_err(|_| unknown("work ledger poisoned"))?;
        let rows = remaining.rows.checked_sub(work.rows);
        let copied_bytes = remaining.copied_bytes.checked_sub(work.copied_bytes);
        let scratch_steps = remaining.scratch_steps.checked_sub(work.scratch_steps);
        match (rows, copied_bytes, scratch_steps) {
            (Some(rows), Some(copied_bytes), Some(scratch_steps)) => {
                *remaining = QueueSourceSelectionWork {
                    rows,
                    copied_bytes,
                    scratch_steps,
                };
                Ok(())
            }
            _ => Err(unknown("request selection work exhausted")),
        }
    }
    /// Observation only; this neither reserves work nor authorizes a dispatch.
    pub fn remaining(&self, job: &QueueReservation) -> QueueResult<QueueSourceSelectionWork> {
        self.scope(job)?;
        Ok(*self
            .remaining
            .lock()
            .map_err(|_| unknown("work ledger poisoned"))?)
    }
}
fn unknown(reason: &str) -> QueueError {
    QueueError::OutcomeUnknown(format!("source selection: {reason}"))
}
#[cfg(test)]
mod tests;
