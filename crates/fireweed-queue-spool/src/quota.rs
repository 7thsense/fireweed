use crate::{add, unknown};
use fireweed_core::queue_contract::QueueResult;
use std::sync::{Arc, Mutex};

/// Independent scratch-space quota; it is never a semantic publication limit.
pub struct ScratchDiskQuota {
    limit: u64,
    used: Mutex<u64>,
}
impl ScratchDiskQuota {
    pub fn new(limit: u64) -> QueueResult<Arc<Self>> {
        if limit == 0 {
            return Err(unknown("zero disk quota"));
        }
        Ok(Arc::new(Self {
            limit,
            used: Mutex::new(0),
        }))
    }
    /// Reserve retained temporary-file capacity before creating or growing bytes.
    pub fn reserve(self: &Arc<Self>, bytes: u64) -> QueueResult<OwnedScratchDiskCharge> {
        let mut charge = OwnedScratchDiskCharge {
            quota: Arc::clone(self),
            bytes: 0,
        };
        charge.grow(bytes)?;
        Ok(charge)
    }
    pub fn usage(&self) -> QueueResult<u64> {
        Ok(*self.used.lock().map_err(unknown)?)
    }
}
/// Monotonic capacity owner. Keep it until every protected file handle and
/// backing byte is gone, including partial writes and failed operations.
pub struct OwnedScratchDiskCharge {
    quota: Arc<ScratchDiskQuota>,
    bytes: u64,
}
impl OwnedScratchDiskCharge {
    pub fn grow(&mut self, bytes: u64) -> QueueResult<()> {
        let mut used = self.quota.used.lock().map_err(unknown)?;
        let next = add(*used, bytes)?;
        let owned = add(self.bytes, bytes)?;
        if next > self.quota.limit {
            return Err(unknown("disk quota exhausted"));
        }
        *used = next;
        self.bytes = owned;
        Ok(())
    }
}
impl Drop for OwnedScratchDiskCharge {
    fn drop(&mut self) {
        let mut used = self
            .quota
            .used
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *used -= self.bytes;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn file_owners_share_exact_capacity_and_failed_growth_preserves_it() {
        let quota = ScratchDiskQuota::new(10).unwrap();
        let mut first = quota.reserve(4).unwrap();
        let second = quota.reserve(6).unwrap();
        assert_eq!(quota.usage().unwrap(), 10);
        assert!(first.grow(1).is_err());
        assert_eq!(quota.usage().unwrap(), 10);
        drop(second);
        assert_eq!(quota.usage().unwrap(), 4);
        first.grow(6).unwrap();
        assert_eq!(quota.usage().unwrap(), 10);
        drop(first);
        assert_eq!(quota.usage().unwrap(), 0);
    }
    #[test]
    fn arithmetic_failure_does_not_release_partial_file_capacity() {
        let quota = ScratchDiskQuota::new(u64::MAX).unwrap();
        let mut owner = quota.reserve(7).unwrap();
        assert!(owner.grow(u64::MAX).is_err());
        assert_eq!(quota.usage().unwrap(), 7);
        drop(owner);
        assert_eq!(quota.usage().unwrap(), 0);
    }
}
