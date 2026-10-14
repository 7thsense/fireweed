//! Bounded state authority for the exact declared commit groups, including every
//! sibling in an operation-local batch handoff. No state payload is retained.
use crate::{
    queue_admission::{OwnedQueueBufferCharge, QueueReservation},
    queue_log_record::OwnedQueueLogRecord,
    queue_recovery::ColdQueueHistory,
    queue_state_authority::{AdmittedStateAuthority, ColdStateTracker, StateDescriptor},
};
use fireweed_core::queue_contract::*;
use std::sync::Arc;
const ANCHOR_COLLECTION: &str = "\0group-state-cut";
struct Binding {
    fingerprint: [u8; 32],
    positions: Vec<(usize, usize)>,
}
pub struct ColdGroupStateTracker {
    anchor: ColdStateTracker,
    bindings: Vec<Binding>,
    states: Vec<ColdStateTracker>,
    charge: OwnedQueueBufferCharge,
    poisoned: bool,
}
pub struct AdmittedGroupStateSet {
    anchor: AdmittedStateAuthority,
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
            bindings,
            states,
            charge,
            poisoned: false,
        })
    }
    pub fn observe(&mut self, offset: u64, record: &OwnedQueueLogRecord) -> QueueResult<()> {
        if self.poisoned {
            return Err(unknown("poisoned group state history"));
        }
        let result = (|| {
            let view = record.view()?;
            self.anchor.observe_view(offset, record, &view)?;
            for state in &mut self.states {
                state.observe_view(offset, record, &view)?;
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
        if self.poisoned {
            return Err(unknown("poisoned group state history"));
        }
        let anchor = self.anchor.finish(history, genesis, progress)?;
        let states = self
            .states
            .into_iter()
            .map(|state| state.finish(history, genesis, progress))
            .collect::<QueueResult<Vec<_>>>()?;
        Ok(AdmittedGroupStateSet {
            anchor,
            bindings: self.bindings,
            states,
            _charge: self.charge,
        })
    }
}
impl AdmittedGroupStateSet {
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
