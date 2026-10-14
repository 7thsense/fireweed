//! Bounded state and ordinary selected-work authority for declared commit groups.
//! The existing state anchor binds every sibling, owner and checked cut; no opaque
//! work or state payload is retained. Complete administrative hydration is separate.
mod work;
use crate::{
    queue_admission::{OwnedQueueBufferCharge, QueueReservation},
    queue_log_record::OwnedQueueLogRecord,
    queue_recovery::ColdQueueHistory,
    queue_state_authority::{AdmittedStateAuthority, ColdStateTracker, StateDescriptor},
};
use fireweed_core::queue_contract::*;
use std::sync::Arc;
pub use work::{BodyDescriptor, ValueDescriptor, WorkDescriptor, WorkMetadata};
const ANCHOR_COLLECTION: &str = "\0group-state-cut";
struct Binding {
    fingerprint: [u8; 32],
    positions: Vec<(usize, usize)>,
}
pub enum GroupDeclarations {
    Batch(Arc<CommitBatch>),
    Group(Arc<CommitGroup>),
    Preview(Arc<PreviewRequest>),
}
impl GroupDeclarations {
    fn groups(&self) -> &[CommitGroup] {
        match self {
            Self::Batch(batch) => &batch.groups,
            Self::Group(group) => std::slice::from_ref(group.as_ref()),
            Self::Preview(request) => std::slice::from_ref(&request.group),
        }
    }
}
pub struct ColdGroupStateTracker {
    anchor: ColdStateTracker,
    declarations: Option<GroupDeclarations>,
    work_genesis_sha256: Option<[u8; 32]>,
    work: Option<work::WorkSet>,
    bindings: Vec<Binding>,
    states: Vec<ColdStateTracker>,
    charge: OwnedQueueBufferCharge,
    poisoned: bool,
}
pub struct AdmittedGroupStateSet {
    anchor: AdmittedStateAuthority,
    work: Option<work::WorkSet>,
    bindings: Vec<Binding>,
    states: Vec<AdmittedStateAuthority>,
    _charge: OwnedQueueBufferCharge,
}
#[derive(Clone)]
pub struct AdmittedGroupStateAuthority {
    set: Arc<AdmittedGroupStateSet>,
    position: usize,
}
fn unknown(reason: &str) -> QueueError {
    QueueError::OutcomeUnknown(format!("group state authority: {reason}"))
}
impl ColdGroupStateTracker {
    /// Retain the already admitted request owner until checked genesis supplies
    /// the exact schema. No request payload is cloned by this proof.
    pub fn with_work(
        queue: &QueueAddress,
        declarations: GroupDeclarations,
        job: &Arc<QueueReservation>,
    ) -> QueueResult<Self> {
        if match &declarations {
            GroupDeclarations::Batch(batch) => &batch.queue != queue,
            GroupDeclarations::Preview(request) => &request.queue != queue,
            GroupDeclarations::Group(_) => false,
        } {
            return Err(unknown("work declarations address differs"));
        }
        let mut tracker = Self::new(queue, declarations.groups(), job)?;
        tracker.declarations = Some(declarations);
        Ok(tracker)
    }
    /// Called after the enclosing history validates the same checked genesis.
    pub fn initialize_work(
        &mut self,
        history: &ColdQueueHistory,
        checked: &crate::queue_log_record::CheckedQueueLogRecord<'_>,
        job: &Arc<QueueReservation>,
    ) -> QueueResult<()> {
        if self.poisoned {
            return Err(unknown("poisoned group history initialization"));
        }
        let result = self.initialize_work_checked(history, checked, job);
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }
    fn initialize_work_checked(
        &mut self,
        history: &ColdQueueHistory,
        checked: &crate::queue_log_record::CheckedQueueLogRecord<'_>,
        job: &Arc<QueueReservation>,
    ) -> QueueResult<()> {
        let Some(declarations) = self.declarations.as_ref() else {
            return Ok(());
        };
        if self.poisoned
            || self.work.is_some()
            || !std::ptr::eq(self.charge.reservation(), job.as_ref())
        {
            return Err(unknown("work scope initialization is not fresh"));
        }
        let genesis_sha256 = self.anchor.initialization_genesis(history, checked, job)?;
        self.work = Some(work::WorkSet::new(
            declarations.groups(),
            history.definition()?.schema(),
            job,
        )?);
        self.work_genesis_sha256 = Some(genesis_sha256);
        Ok(())
    }

    pub fn new(
        queue: &QueueAddress,
        groups: &[CommitGroup],
        job: &Arc<QueueReservation>,
    ) -> QueueResult<Self> {
        let count = groups.iter().try_fold(0usize, |n, g| {
            n.checked_add(g.state_updates.len())
                .ok_or(QueueError::TooLarge)
        })?;
        let capacity = (groups.len() as u64)
            .checked_mul((std::mem::size_of::<Binding>() as u64) * 4)
            .and_then(|n| {
                n.checked_add(
                    (count as u64)
                        .checked_mul((std::mem::size_of::<ColdStateTracker>() as u64) * 4)?,
                )
            })
            .and_then(|n| n.checked_add(1024))
            .ok_or(QueueError::TooLarge)?;
        let charge = job.owned_buffer(capacity)?;
        let anchor = ColdStateTracker::new_keys(queue, ANCHOR_COLLECTION, std::iter::empty(), job)?;
        // Temporary borrowed declarations and owned mappings are covered by the
        // outer charge before their vectors grow. Each literal identity folds once.
        let mut declarations: Vec<(&str, Vec<&str>)> = Vec::new();
        let mut bindings = Vec::with_capacity(groups.len());
        for group in groups {
            let mut positions = Vec::with_capacity(group.state_updates.len());
            for update in &group.state_updates {
                let collection = if let Some(position) = declarations
                    .iter()
                    .position(|(name, _)| *name == update.collection)
                {
                    position
                } else {
                    declarations.push((&update.collection, Vec::new()));
                    declarations.len() - 1
                };
                let keys = &mut declarations[collection].1;
                let key = if let Some(position) = keys.iter().position(|key| *key == update.key) {
                    position
                } else {
                    keys.push(&update.key);
                    keys.len() - 1
                };
                positions.push((collection, key));
            }
            bindings.push(Binding {
                fingerprint: group.fingerprint(queue)?,
                positions,
            });
        }
        let states = declarations
            .iter()
            .map(|(collection, keys)| {
                ColdStateTracker::new_keys(queue, collection, keys.iter().copied(), job)
            })
            .collect::<QueueResult<Vec<_>>>()?;
        Ok(Self {
            anchor,
            declarations: None,
            work_genesis_sha256: None,
            work: None,
            bindings,
            states,
            charge,
            poisoned: false,
        })
    }
    pub fn resume(
        proof: &Arc<AdmittedGroupStateSet>,
        batch: &CommitBatch,
        genesis: &OwnedQueueLogRecord,
        progress: (u64, u64, u64),
        job: &Arc<QueueReservation>,
    ) -> QueueResult<Self> {
        proof.validate_batch(batch, genesis, progress, job)?;
        let mut tracker = Self::new(&batch.queue, &batch.groups, job)?;
        tracker
            .anchor
            .seed(&proof.anchor, &batch.queue, genesis, progress, job)?;
        if tracker.states.len() != proof.states.len() {
            return Err(unknown("continuation state vector mismatch"));
        }
        if let Some(work) = &proof.work {
            tracker.work = Some(work.copy_admitted(job)?);
        }
        for (state, previous) in tracker.states.iter_mut().zip(&proof.states) {
            state.seed(previous, &batch.queue, genesis, progress, job)?;
        }
        Ok(tracker)
    }
    pub fn observe(&mut self, offset: u64, record: &OwnedQueueLogRecord) -> QueueResult<()> {
        if self.poisoned {
            return Err(unknown("poisoned group state history"));
        }
        let checked = match record.checked() {
            Ok(checked) => checked,
            Err(error) => {
                self.poisoned = true;
                return Err(error);
            }
        };
        self.observe_record(offset, &checked)
    }
    pub fn observe_record(
        &mut self,
        offset: u64,
        checked: &crate::queue_log_record::CheckedQueueLogRecord<'_>,
    ) -> QueueResult<()> {
        if self.poisoned {
            return Err(unknown("poisoned group state history"));
        }
        let result = (|| {
            if self.declarations.is_some() && self.work.is_none() {
                return Err(unknown("work scope not initialized at checked genesis"));
            }
            if offset == 0
                && let Some(expected) = self.work_genesis_sha256
            {
                use sha2::Digest;
                if expected != <[u8; 32]>::from(sha2::Sha256::digest(checked.owner().bytes())) {
                    return Err(unknown(
                        "work scope initialized from a different genesis token",
                    ));
                }
            }
            self.anchor.observe_record(offset, checked)?;
            if let Some(work) = &mut self.work {
                use crate::queue_log_record::QueueLogPayload;
                let commands = match checked.view().payload() {
                    QueueLogPayload::Genesis(_) => None,
                    QueueLogPayload::Publication(frame) => Some(frame.commands()),
                    QueueLogPayload::Claim(claim) => Some(claim.commands().commands()),
                };
                if let Some(commands) = commands {
                    for command in commands {
                        work.observe(command.map_err(|_| unknown("invalid source work command"))?)?;
                    }
                }
            }
            for state in &mut self.states {
                state.observe_record(offset, checked)?;
            }
            Ok(())
        })();
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }
    pub fn finish(
        self,
        history: &ColdQueueHistory,
        genesis: &OwnedQueueLogRecord,
        progress: (u64, u64, u64),
    ) -> QueueResult<AdmittedGroupStateSet> {
        if self.poisoned || (self.declarations.is_some() && self.work.is_none()) {
            return Err(unknown("poisoned or uninitialized group history"));
        }
        let anchor = self.anchor.finish(history, genesis, progress)?;
        let states = self
            .states
            .into_iter()
            .map(|state| state.finish(history, genesis, progress))
            .collect::<QueueResult<Vec<_>>>()?;
        Ok(AdmittedGroupStateSet {
            anchor,
            work: self.work,
            bindings: self.bindings,
            states,
            _charge: self.charge,
        })
    }
}
impl AdmittedGroupStateSet {
    pub fn validate_batch(
        self: &Arc<Self>,
        batch: &CommitBatch,
        genesis: &OwnedQueueLogRecord,
        progress: (u64, u64, u64),
        job: &QueueReservation,
    ) -> QueueResult<()> {
        self.anchor.validate_keys(
            &batch.queue,
            ANCHOR_COLLECTION,
            std::iter::empty(),
            genesis,
            progress,
            job,
        )?;
        if self.bindings.len() != batch.groups.len() {
            return Err(unknown("complete batch declaration mismatch"));
        }
        for (position, group) in batch.groups.iter().enumerate() {
            self.group(position)?
                .validate(&batch.queue, group, genesis, progress, job)?;
        }
        Ok(())
    }
    pub fn group(self: &Arc<Self>, position: usize) -> QueueResult<AdmittedGroupStateAuthority> {
        if position >= self.bindings.len() {
            return Err(unknown("group position outside declared batch"));
        }
        Ok(AdmittedGroupStateAuthority {
            set: self.clone(),
            position,
        })
    }
}
impl AdmittedGroupStateAuthority {
    /// Exact group membership is checked even when sibling field closures unite.
    pub fn work_descriptor(&self, key: &str) -> QueueResult<&WorkDescriptor> {
        self.set
            .work
            .as_ref()
            .ok_or_else(|| unknown("ordinary work scope missing"))?
            .descriptor(self.position, key)
    }
    pub fn has_work_authority(&self) -> bool {
        self.set.work.is_some()
    }

    pub fn validate(
        &self,
        queue: &QueueAddress,
        group: &CommitGroup,
        genesis: &OwnedQueueLogRecord,
        progress: (u64, u64, u64),
        job: &QueueReservation,
    ) -> QueueResult<()> {
        self.set.anchor.validate_keys(
            queue,
            ANCHOR_COLLECTION,
            std::iter::empty(),
            genesis,
            progress,
            job,
        )?;
        let binding = &self.set.bindings[self.position];
        if binding.fingerprint != group.fingerprint(queue)?
            || binding.positions.len() != group.state_updates.len()
        {
            return Err(unknown(
                "declared group differs from recovered state binding",
            ));
        }
        for (position, update) in group.state_updates.iter().enumerate() {
            let (collection, key) = binding.positions[position];
            self.set.states[collection].descriptor_identity(
                key,
                &update.collection,
                &update.key,
            )?;
        }
        Ok(())
    }
    /// Call only after the complete outer group has passed `validate`.
    pub fn descriptor(&self, position: usize, key: &str) -> QueueResult<StateDescriptor> {
        let binding = &self.set.bindings[self.position];
        if position >= binding.positions.len() {
            return Err(unknown("state position outside declared group"));
        }
        let (collection, position) = binding.positions[position];
        self.set.states[collection].descriptor(position, key)
    }
}

#[cfg(test)]
mod tests;
