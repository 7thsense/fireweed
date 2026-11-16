//! Shared administrative/ordinary collision and business finalization.
use crate::{ScratchDiskQuota, ScratchWorkers, SealedAdministrativeScratch};
use fireweed_core::queue_contract::*;
use fireweed_engine::{
    queue_administration::{
        AdmittedAdministrativeScan, ProvisionalOrdinaryCandidate,
        propose_ordinary_after_administration,
    },
    queue_admission::{OwnedQueueBufferCharge, QueueReservation},
    queue_planner::RecordSnapshot,
};
use std::{path::Path, sync::Arc};

/// Adapter-owned initial authority and admission remain live through finalization.
pub trait AdministrativeOrdinaryAuthority: Send + 'static {
    fn snapshot(&self) -> &RecordSnapshot;
    #[cfg(feature = "test-support")]
    fn worker_probe(&self) -> Option<Arc<crate::FinalizationProbe>> {
        None
    }
}
impl AdministrativeOrdinaryAuthority
    for fireweed_engine::queue_source_index::QueueSourceOrdinaryAuthority
{
    fn snapshot(&self) -> &RecordSnapshot {
        self.snapshot()
    }
}
pub struct FinalizedAdministrativeSemantics<A> {
    pub scratch: SealedAdministrativeScratch,
    pub summary: AdmittedAdministrativeScan,
    pub ordinary: ProvisionalOrdinaryCandidate,
    pub initial: A,
}
fn storage(message: &str) -> QueueError {
    QueueError::OutcomeUnknown(message.into())
}
struct OwnedScratchDirectory {
    path: std::path::PathBuf,
    _charge: OwnedQueueBufferCharge,
}
impl OwnedScratchDirectory {
    fn as_path(&self) -> &std::path::Path {
        &self.path
    }
}
/// Caller first validates request binding, sealed context and prospective
/// ordinary authority in its covered native snapshot. No durable effects here.
#[allow(clippy::too_many_arguments)]
pub async fn finalize_administrative_semantics<A: AdministrativeOrdinaryAuthority>(
    mut scratch: SealedAdministrativeScratch,
    summary: AdmittedAdministrativeScan,
    ordinary: A,
    group: &CommitGroup,
    schema: &QueueSchema,
    ownership_now: impl FnOnce() -> QueueResult<i64>,
    reservation: &Arc<QueueReservation>,
    workers: &ScratchWorkers,
    directory: &Path,
    quota: Arc<ScratchDiskQuota>,
) -> QueueResult<Result<FinalizedAdministrativeSemantics<A>, Rejection>> {
    let names = directory.as_os_str().len() as u64;
    let path_charge = reservation.owned_buffer(
        names
            .checked_mul(4)
            .and_then(|n| n.checked_add(4096))
            .ok_or_else(|| storage("scratch path bound overflow"))?,
    )?;
    let path = OwnedScratchDirectory {
        path: directory.to_owned(),
        _charge: path_charge,
    };
    let run_quota = Arc::clone(&quota);
    let run_job = Arc::clone(reservation);
    #[cfg(feature = "test-support")]
    let probe = ordinary.worker_probe();
    #[cfg(feature = "test-support")]
    let validation_probe = probe.clone();
    let (mut scratch, ordinary, decision, path) = workers
        .run_or_inline(move || {
            #[cfg(feature = "test-support")]
            if let Some(probe) = validation_probe {
                probe.checkpoint(crate::FinalizationStage::Validate)?;
            }
            let decision = scratch.validate_before_business(
                ordinary.snapshot(),
                path.as_path(),
                run_quota,
                &run_job,
            )?;
            Ok((scratch, ordinary, decision, path))
        })
        .await?;
    if summary.overlaps_explicit_work() {
        return Ok(Err(Rejection::Invalid(
            "administrative target overlaps explicit group work".into(),
        )));
    }
    if decision.administrative_collision {
        return Ok(Err(Rejection::UniqueConflict));
    }
    let proposal = match propose_ordinary_after_administration(
        group,
        ordinary.snapshot(),
        ownership_now,
        schema,
        reservation,
    )? {
        Ok(candidate) => candidate,
        Err(reason) => return Ok(Err(reason)),
    };
    let run_job = Arc::clone(reservation);
    let (scratch, mut proposal, decision) = workers
        .run_or_inline(move || {
            #[cfg(feature = "test-support")]
            if let Some(probe) = probe {
                probe.checkpoint(crate::FinalizationStage::Reduce)?;
            }
            let decision = scratch.reduce_with_ordinary_effects(
                proposal.initial_authority(),
                proposal.unique_effects(),
                path.as_path(),
                quota,
                &run_job,
            )?;
            drop(path);

            Ok((scratch, proposal, decision))
        })
        .await?;
    if decision.final_collision {
        return Ok(Err(Rejection::UniqueConflict));
    }
    proposal.sort_commands_for_publication();
    Ok(Ok(FinalizedAdministrativeSemantics {
        scratch,
        summary,
        ordinary: proposal,
        initial: ordinary,
    }))
}
