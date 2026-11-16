//! Root byte guards follow SDK physical I/O, independently of caller futures.
use super::*;

struct PhysicalOwners<const N: usize> {
    _guards: [Arc<OwnedQueueBufferCharge>; N],
    // Release the allocation's own credit after its retained input guards.
    _allocation: OwnedQueueBufferCharge,
}
fn allocation_bytes<const N: usize>() -> QueueResult<u64> {
    std::mem::size_of::<PhysicalOwners<N>>()
        .checked_add(2 * std::mem::size_of::<usize>())
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or(QueueError::TooLarge)
}
pub(super) fn physical_owner<const N: usize>(
    guards: [Arc<OwnedQueueBufferCharge>; N],
) -> QueueResult<object_log::AppendOwner> {
    let first = guards
        .first()
        .ok_or_else(|| unknown("empty physical owner set"))?;
    if guards
        .iter()
        .any(|guard| !std::ptr::eq(first.reservation(), guard.reservation()))
    {
        return Err(unknown("physical owners belong to different operations"));
    }
    let allocation = first.child_buffer(allocation_bytes::<N>()?)?;
    Ok(Arc::new(PhysicalOwners {
        _guards: guards,
        _allocation: allocation,
    }))
}
pub(super) fn shared_charge(
    job: &Arc<QueueReservation>,
    bytes: u64,
) -> QueueResult<Arc<OwnedQueueBufferCharge>> {
    let allocation =
        (std::mem::size_of::<OwnedQueueBufferCharge>() + 2 * std::mem::size_of::<usize>()) as u64;
    Ok(Arc::new(job.owned_buffer(
        bytes.checked_add(allocation).ok_or(QueueError::TooLarge)?,
    )?))
}

#[cfg(test)]
pub(super) fn physical_owner_bytes<const N: usize>() -> u64 {
    allocation_bytes::<N>().unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    use fireweed_engine::queue_admission::QueueAdmission;
    #[test]
    fn owner_set_requires_exact_preallocation_and_retains_credit_to_last_alias() {
        let guard_allocation = (std::mem::size_of::<OwnedQueueBufferCharge>()
            + 2 * std::mem::size_of::<usize>()) as u64;
        let exact = 16 + guard_allocation + allocation_bytes::<1>().unwrap();
        for deficit in [1, 0] {
            let admission = QueueAdmission::new(1, exact - deficit).unwrap();
            let job = Arc::new(admission.reserve(0).unwrap());
            let guard = shared_charge(&job, 16).unwrap();
            let result = physical_owner([guard.clone()]);
            assert_eq!(result.is_ok(), deficit == 0);
            if let Ok(owner) = result {
                let alias = owner.clone();
                drop(owner);
                drop(guard);
                drop(job);
                assert_eq!(admission.usage().unwrap(), (1, exact));
                drop(alias);
            } else {
                drop(guard);
                drop(job);
            }
            assert_eq!(admission.usage().unwrap(), (0, 0));
        }
    }
    #[test]
    fn foreign_owner_sets_fail_before_allocating_capacity() {
        let admission = QueueAdmission::new(2, 4096).unwrap();
        let first = Arc::new(admission.reserve(0).unwrap());
        let other = Arc::new(admission.reserve(0).unwrap());
        let a = shared_charge(&first, 16).unwrap();
        let b = shared_charge(&other, 16).unwrap();
        let before = admission.usage().unwrap();
        assert!(physical_owner([a.clone(), b.clone()]).is_err());
        assert_eq!(admission.usage().unwrap(), before);
    }
}
