//! Exact declarations for same-operation continuation; no cross-operation cache.
use super::*;
use fireweed_core::queue_contract::CommitBatch;
use fireweed_engine::queue_log_record::OwnedQueueLogRecord;

pub(super) enum ContinuationDeclaration {
    Batch(Arc<CommitBatch>),
    Request,
}
impl ContinuationDeclaration {
    pub(super) fn matches_address(&self, address: &QueueAddress) -> bool {
        match self {
            Self::Batch(batch) => batch.queue == *address,
            Self::Request => true,
        }
    }
    pub(super) fn validate(
        &self,
        old: &RecoveredQueue,
        address: &QueueAddress,
        identity: Option<&(u8, String)>,
        genesis: &OwnedQueueLogRecord,
        progress: (u64, u64, u64),
        job: &Arc<QueueReservation>,
    ) -> QueueResult<()> {
        let originals = old
            .original_authority
            .as_ref()
            .ok_or_else(|| unknown("missing continuation originals"))?;
        match self {
            Self::Batch(batch) => {
                if identity.is_some() {
                    return Err(unknown("batch continuation identity mismatch"));
                }
                originals.validate(batch, genesis, progress, job)?;
                old.group_state_authority
                    .as_ref()
                    .ok_or_else(|| unknown("missing continuation states"))?
                    .validate_batch(batch, genesis, progress, job)
            }
            Self::Request => {
                let (domain, id) =
                    identity.ok_or_else(|| unknown("missing single continuation identity"))?;
                if *domain > 1
                    || old.state_authority.is_some()
                    || old.group_state_authority.is_some()
                {
                    return Err(unknown("unsupported single continuation scope"));
                }
                originals.validate_request(address, *domain, id, genesis, progress, job)?;
                if old.original_identity_present != Some(originals.descriptor(0, id)?.is_some()) {
                    return Err(unknown("single continuation presence mismatch"));
                }
                Ok(())
            }
        }
    }
    pub(super) fn resume(
        &self,
        old: &RecoveredQueue,
        address: &QueueAddress,
        identity: Option<&(u8, String)>,
        genesis: &OwnedQueueLogRecord,
        progress: (u64, u64, u64),
        job: &Arc<QueueReservation>,
    ) -> QueueResult<(ColdOriginalTracker, Option<RecoveryStateTracker>)> {
        self.validate(old, address, identity, genesis, progress, job)?;
        let proof = old.original_authority.as_ref().unwrap();
        match self {
            Self::Batch(batch) => Ok((
                ColdOriginalTracker::resume(proof, batch, genesis, progress, job)?,
                Some(RecoveryStateTracker::Groups(
                    fireweed_engine::queue_group_state_authority::ColdGroupStateTracker::resume(
                        old.group_state_authority.as_ref().unwrap(),
                        batch,
                        genesis,
                        progress,
                        job,
                    )?,
                )),
            )),
            Self::Request => {
                let (domain, id) = identity.unwrap();
                Ok((
                    ColdOriginalTracker::resume_request(
                        proof, address, *domain, id, genesis, progress, job,
                    )?,
                    None,
                ))
            }
        }
    }
    pub(super) fn cold(
        &self,
        address: &QueueAddress,
        identity: Option<&(u8, String)>,
        job: &Arc<QueueReservation>,
    ) -> QueueResult<(ColdOriginalTracker, Option<RecoveryStateTracker>)> {
        match self {
            Self::Batch(batch) => Ok((
                ColdOriginalTracker::new(batch, job)?,
                Some(RecoveryStateTracker::Groups(
                    fireweed_engine::queue_group_state_authority::ColdGroupStateTracker::with_work(
                        address,
                        fireweed_engine::queue_group_state_authority::GroupDeclarations::Batch(
                            batch.clone(),
                        ),
                        job,
                    )?,
                )),
            )),
            Self::Request => {
                let (domain, id) =
                    identity.ok_or_else(|| unknown("missing single continuation identity"))?;
                Ok((
                    ColdOriginalTracker::new_request(address, *domain, id, job)?,
                    None,
                ))
            }
        }
    }
}
