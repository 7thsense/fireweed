//! Log-ordered planner metadata for `filesystem--turso`.
//!
//! This is the produce-path authority: Push / BatchUpdate plan here, reserve here, and
//! roll back here if the object-log append fails. Turso apply is not consulted and is
//! not allowed to reject a race the log already acked.
//!
//! Payloads are not stored. This is not a second in-memory projection.

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex};

use fireweed_core::{
    ClientItemKey, GroupKey, ItemId, ItemState, LeaseToken, PriorityValue, RequestId, UtcTimestamp,
};
use fireweed_engine::{
    BatchUpdateSnapshotItem, EngineError, EngineResult, FinalizeKind, FinalizeOutcome,
    PayloadUpdate, QueueCommand, QueueKey, ScheduleUpdate, UpdateFieldsCommand,
};

#[derive(Debug, Clone)]
pub struct PlannerItem {
    pub item_id: ItemId,
    pub client_item_key: ClientItemKey,
    pub state: ItemState,
    pub item_version: u64,
    pub group_key: Option<GroupKey>,
    pub priority: Option<PriorityValue>,
    pub not_before: Option<UtcTimestamp>,
    pub fenced: bool,
    pub superseded: bool,
    pub lease_token: Option<LeaseToken>,
}

#[derive(Debug, Clone)]
enum Undo {
    Push {
        shard: QueueKey,
        keys: Vec<ClientItemKey>,
        ids: Vec<ItemId>,
        request_id: Option<RequestId>,
    },
    Replace {
        shard: QueueKey,
        before: Vec<PlannerItem>,
        removed: Vec<PlannerItem>,
    },
}

#[derive(Clone, Default)]
pub struct PlannerMap {
    inner: Arc<Mutex<Inner>>,
}

#[derive(Default)]
struct Inner {
    shards: HashMap<QueueKey, Shard>,
}

#[derive(Default)]
struct Shard {
    by_id: HashMap<ItemId, PlannerItem>,
    by_key: HashMap<ClientItemKey, ItemId>,
    reserved_ids: HashMap<ItemId, u32>,
    reserved_keys: HashMap<ClientItemKey, u32>,
    push_replay: HashMap<RequestId, (u64, Vec<ItemId>)>,
    paused_intake: bool,
    pending: BTreeSet<ItemId>,
}

impl PlannerMap {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn snapshot(
        &self,
        shard: &QueueKey,
        keys: &[ClientItemKey],
        ids: &[ItemId],
    ) -> Vec<BatchUpdateSnapshotItem> {
        let inner = self.inner.lock().expect("planner map");
        let Some(shard_state) = inner.shards.get(shard) else {
            return Vec::new();
        };
        let mut out = Vec::with_capacity(keys.len() + ids.len());
        let mut seen = HashMap::new();
        for key in keys {
            if let Some(item_id) = shard_state.by_key.get(key)
                && let Some(item) = shard_state.by_id.get(item_id)
                && seen.insert(*item_id, ()).is_none()
            {
                out.push(snapshot_item(item));
            }
        }
        for item_id in ids {
            if let Some(item) = shard_state.by_id.get(item_id)
                && seen.insert(*item_id, ()).is_none()
            {
                out.push(snapshot_item(item));
            }
        }
        out
    }

    pub fn push_replay(
        &self,
        shard: &QueueKey,
        request_id: &RequestId,
    ) -> Option<(u64, Vec<ItemId>)> {
        let inner = self.inner.lock().expect("planner map");
        inner
            .shards
            .get(shard)
            .and_then(|shard| shard.push_replay.get(request_id).cloned())
    }

    pub fn intake_paused(&self, shard: &QueueKey) -> bool {
        let inner = self.inner.lock().expect("planner map");
        inner
            .shards
            .get(shard)
            .is_some_and(|shard| shard.paused_intake)
    }

    /// Insert freshly minted push items. Fails closed on duplicate live keys.
    pub fn reserve_push(
        &self,
        shard: &QueueKey,
        items: &[(
            ClientItemKey,
            ItemId,
            Option<GroupKey>,
            Option<PriorityValue>,
            Option<UtcTimestamp>,
        )],
        request_id: Option<(RequestId, u64, Vec<ItemId>)>,
        max_group: Option<u64>,
    ) -> EngineResult<Reservation> {
        let mut inner = self.inner.lock().expect("planner map");
        let shard_state = inner.shards.entry(shard.clone()).or_default();
        if shard_state.paused_intake {
            return Err(EngineError::Paused { drain_intake: true });
        }
        if let Some((request_id, fingerprint, _)) = &request_id
            && let Some((stored, _)) = shard_state.push_replay.get(request_id)
        {
            if *stored != *fingerprint {
                return Err(EngineError::RequestIdConflict);
            }
        }
        let mut keys = Vec::with_capacity(items.len());
        let mut ids = Vec::with_capacity(items.len());
        let mut group_added: HashMap<GroupKey, u64> = HashMap::new();
        for (key, id, group, _, _) in items {
            if shard_state.by_key.contains_key(key)
                || shard_state.reserved_keys.contains_key(key)
                || shard_state.by_id.contains_key(id)
                || shard_state.reserved_ids.contains_key(id)
            {
                return Err(EngineError::Conflict);
            }
            if let Some(group) = group {
                *group_added.entry(group.clone()).or_insert(0) += 1;
            }
            keys.push(key.clone());
            ids.push(*id);
        }
        if let Some(max) = max_group {
            for (group, added) in &group_added {
                let live = shard_state
                    .by_id
                    .values()
                    .filter(|item| {
                        !item.state.is_terminal()
                            && !item.superseded
                            && item.group_key.as_ref() == Some(group)
                    })
                    .count() as u64;
                if live.saturating_add(*added) > max {
                    return Err(EngineError::Conflict);
                }
            }
        }
        for (key, id, group, priority, not_before) in items {
            shard_state.by_id.insert(
                *id,
                PlannerItem {
                    item_id: *id,
                    client_item_key: key.clone(),
                    state: ItemState::Pending,
                    item_version: 1,
                    group_key: group.clone(),
                    priority: priority.clone(),
                    not_before: *not_before,
                    fenced: false,
                    superseded: false,
                    lease_token: None,
                },
            );
            shard_state.by_key.insert(key.clone(), *id);
            reindex(shard_state, *id);
            bump(&mut shard_state.reserved_ids, *id);
            bump(&mut shard_state.reserved_keys, key.clone());
        }
        if let Some((request_id, fingerprint, replay_ids)) = request_id {
            shard_state
                .push_replay
                .insert(request_id.clone(), (fingerprint, replay_ids));
            Ok(Reservation(Undo::Push {
                shard: shard.clone(),
                keys,
                ids,
                request_id: Some(request_id),
            }))
        } else {
            Ok(Reservation(Undo::Push {
                shard: shard.clone(),
                keys,
                ids,
                request_id: None,
            }))
        }
    }

    /// Release a push reservation after append. `ok` keeps the items; failure removes them.
    pub fn finish_push(&self, shard: &QueueKey, item_ids: &[ItemId], ok: bool) {
        let mut inner = self.inner.lock().expect("planner map");
        let Some(shard_state) = inner.shards.get_mut(shard) else {
            return;
        };
        let mut keys = Vec::new();
        for id in item_ids {
            if let Some(item) = shard_state.by_id.get(id) {
                keys.push(item.client_item_key.clone());
            }
            unbump(&mut shard_state.reserved_ids, *id);
            if !ok {
                shard_state.by_id.remove(id);
            }
            reindex(shard_state, *id);
        }
        for key in keys {
            unbump(&mut shard_state.reserved_keys, key.clone());
            if !ok {
                shard_state.by_key.remove(&key);
            }
        }
    }

    pub fn reserve_updates(
        &self,
        shard: &QueueKey,
        updates: &[UpdateFieldsCommand],
    ) -> EngineResult<Reservation> {
        let mut inner = self.inner.lock().expect("planner map");
        let shard_state = inner.shards.entry(shard.clone()).or_default();
        let mut before = Vec::with_capacity(updates.len());
        for update in updates {
            let Some(item) = shard_state.by_id.get(&update.item_id).cloned() else {
                continue;
            };
            if shard_state.reserved_ids.contains_key(&update.item_id) {
                return Err(EngineError::Conflict);
            }
            before.push(item);
        }
        for update in updates {
            let Some(item) = shard_state.by_id.get_mut(&update.item_id) else {
                continue;
            };
            apply_update_to_item(item, update);
            bump(&mut shard_state.reserved_ids, update.item_id);
        }
        Ok(Reservation(Undo::Replace {
            shard: shard.clone(),
            before,
            removed: Vec::new(),
        }))
    }

    pub fn reserve_claim(
        &self,
        shard: &QueueKey,
        item_ids: &[ItemId],
        lease_token: &LeaseToken,
    ) -> EngineResult<Reservation> {
        let mut inner = self.inner.lock().expect("planner map");
        let shard_state = inner.shards.entry(shard.clone()).or_default();
        let mut before = Vec::with_capacity(item_ids.len());
        for id in item_ids {
            let Some(item) = shard_state.by_id.get(id).cloned() else {
                continue;
            };
            if shard_state.reserved_ids.contains_key(id) {
                return Err(EngineError::Conflict);
            }
            before.push(item);
        }
        for id in item_ids {
            if let Some(item) = shard_state.by_id.get_mut(id) {
                item.state = ItemState::Leased;
                item.item_version = item.item_version.saturating_add(1);
                item.lease_token = Some(lease_token.clone());
                bump(&mut shard_state.reserved_ids, *id);
            }
            reindex(shard_state, *id);
        }
        Ok(Reservation(Undo::Replace {
            shard: shard.clone(),
            before,
            removed: Vec::new(),
        }))
    }

    /// Atomically select and lease up to `max` due pending items.
    pub fn take_claim(
        &self,
        shard: &QueueKey,
        now: UtcTimestamp,
        max: usize,
        lease_token: &LeaseToken,
    ) -> EngineResult<(Vec<ItemId>, Option<Reservation>)> {
        if max == 0 {
            return Ok((Vec::new(), None));
        }
        let mut inner = self.inner.lock().expect("planner map");
        let shard_state = inner.shards.entry(shard.clone()).or_default();
        let mut chosen = Vec::with_capacity(max);
        for id in shard_state.pending.iter().copied() {
            if chosen.len() >= max {
                break;
            }
            if shard_state.reserved_ids.contains_key(&id) {
                continue;
            }
            let Some(item) = shard_state.by_id.get(&id) else {
                continue;
            };
            if item.state != ItemState::Pending || item.fenced || item.superseded {
                continue;
            }
            if item.not_before.is_some_and(|ts| ts > now) {
                continue;
            }
            chosen.push(id);
        }
        if chosen.is_empty() {
            return Ok((Vec::new(), None));
        }
        let mut before = Vec::with_capacity(chosen.len());
        for id in &chosen {
            if let Some(item) = shard_state.by_id.get(id).cloned() {
                before.push(item);
            }
        }
        for id in &chosen {
            if let Some(item) = shard_state.by_id.get_mut(id) {
                item.state = ItemState::Leased;
                item.item_version = item.item_version.saturating_add(1);
                item.lease_token = Some(lease_token.clone());
                bump(&mut shard_state.reserved_ids, *id);
            }
            reindex(shard_state, *id);
        }
        Ok((
            chosen,
            Some(Reservation(Undo::Replace {
                shard: shard.clone(),
                before,
                removed: Vec::new(),
            })),
        ))
    }

    pub fn reserve_finalize(
        &self,
        shard: &QueueKey,
        outcomes: &[FinalizeOutcome],
    ) -> EngineResult<Reservation> {
        let mut inner = self.inner.lock().expect("planner map");
        let shard_state = inner.shards.entry(shard.clone()).or_default();
        let mut before = Vec::with_capacity(outcomes.len());
        for outcome in outcomes {
            let Some(item) = shard_state.by_id.get(&outcome.item_id).cloned() else {
                return Err(EngineError::NotFound);
            };
            if shard_state.reserved_ids.contains_key(&outcome.item_id) {
                return Err(EngineError::Conflict);
            }
            if item.state != ItemState::Leased {
                return Err(EngineError::Invalid("item is not leased"));
            }
            before.push(item);
        }
        for outcome in outcomes {
            apply_finalize_to_shard(shard_state, outcome);
            bump(&mut shard_state.reserved_ids, outcome.item_id);
            reindex(shard_state, outcome.item_id);
        }
        Ok(Reservation(Undo::Replace {
            shard: shard.clone(),
            before,
            removed: Vec::new(),
        }))
    }

    pub fn commit(&self, reservation: Reservation) {
        let mut inner = self.inner.lock().expect("planner map");
        match reservation.0 {
            Undo::Push {
                shard, keys, ids, ..
            } => {
                if let Some(shard_state) = inner.shards.get_mut(&shard) {
                    for id in ids {
                        unbump(&mut shard_state.reserved_ids, id);
                    }
                    for key in keys {
                        unbump(&mut shard_state.reserved_keys, key);
                    }
                }
            }
            Undo::Replace { shard, before, .. } => {
                if let Some(shard_state) = inner.shards.get_mut(&shard) {
                    for item in before {
                        unbump(&mut shard_state.reserved_ids, item.item_id);
                    }
                }
            }
        }
    }

    pub fn rollback(&self, reservation: Reservation) {
        let mut inner = self.inner.lock().expect("planner map");
        match reservation.0 {
            Undo::Push {
                shard,
                keys,
                ids,
                request_id,
            } => {
                if let Some(shard_state) = inner.shards.get_mut(&shard) {
                    for id in &ids {
                        shard_state.by_id.remove(id);
                        unbump(&mut shard_state.reserved_ids, *id);
                    }
                    for key in &keys {
                        shard_state.by_key.remove(key);
                        unbump(&mut shard_state.reserved_keys, key.clone());
                    }
                    if let Some(request_id) = request_id {
                        shard_state.push_replay.remove(&request_id);
                    }
                }
            }
            Undo::Replace {
                shard,
                before,
                removed,
            } => {
                if let Some(shard_state) = inner.shards.get_mut(&shard) {
                    for item in before {
                        let id = item.item_id;
                        unbump(&mut shard_state.reserved_ids, id);
                        shard_state.by_key.insert(item.client_item_key.clone(), id);
                        shard_state.by_id.insert(id, item);
                        reindex(shard_state, id);
                    }
                    for item in removed {
                        shard_state.by_id.remove(&item.item_id);
                        shard_state.by_key.remove(&item.client_item_key);
                    }
                }
            }
        }
    }

    /// Rebuild metadata from a recovered log envelope. No reservations.
    pub fn apply_recovered(&self, shard: &QueueKey, command: &QueueCommand) {
        let mut inner = self.inner.lock().expect("planner map");
        let shard_state = inner.shards.entry(shard.clone()).or_default();
        match command {
            QueueCommand::Push(push) => {
                for item in &push.items {
                    shard_state.by_id.insert(
                        item.item_id,
                        PlannerItem {
                            item_id: item.item_id,
                            client_item_key: item.client_item_key.clone(),
                            state: ItemState::Pending,
                            item_version: 1,
                            group_key: item.group_key.clone(),
                            priority: item.priority.clone(),
                            not_before: item.not_before,
                            fenced: false,
                            superseded: false,
                            lease_token: None,
                        },
                    );
                    shard_state
                        .by_key
                        .insert(item.client_item_key.clone(), item.item_id);
                    reindex(shard_state, item.item_id);
                }
            }
            QueueCommand::UpdateFields(update) => {
                if let Some(item) = shard_state.by_id.get_mut(&update.item_id) {
                    apply_update_to_item(item, update);
                }
            }
            QueueCommand::UpdateFieldsBatch(batch) => {
                for update in &batch.updates {
                    if let Some(item) = shard_state.by_id.get_mut(&update.item_id) {
                        apply_update_to_item(item, update);
                    }
                }
            }
            QueueCommand::Claim(claim) => {
                for id in &claim.item_ids {
                    if let Some(item) = shard_state.by_id.get_mut(id) {
                        item.state = ItemState::Leased;
                        item.item_version = item.item_version.saturating_add(1);
                        item.lease_token = Some(claim.lease_token.clone());
                    }
                    reindex(shard_state, *id);
                }
            }
            QueueCommand::CohortClaim(claim) => {
                for id in &claim.item_ids {
                    if let Some(item) = shard_state.by_id.get_mut(id) {
                        item.state = ItemState::Leased;
                        item.item_version = item.item_version.saturating_add(1);
                        item.lease_token = Some(claim.lease_token.clone());
                    }
                    reindex(shard_state, *id);
                }
            }
            QueueCommand::Finalize(finalize) => {
                for outcome in &finalize.outcomes {
                    apply_finalize_to_shard(shard_state, outcome);
                    reindex(shard_state, outcome.item_id);
                }
            }
            QueueCommand::LeaseExpired(expired) => {
                for id in &expired.item_ids {
                    if let Some(item) = shard_state.by_id.get_mut(id) {
                        item.state = ItemState::Pending;
                        item.lease_token = None;
                    }
                    reindex(shard_state, *id);
                }
            }
            QueueCommand::PauseQueue(_) => shard_state.paused_intake = true,
            QueueCommand::ResumeQueue => shard_state.paused_intake = false,
            QueueCommand::PurgeItems(purge) => {
                for id in &purge.item_ids {
                    if let Some(item) = shard_state.by_id.remove(id) {
                        shard_state.by_key.remove(&item.client_item_key);
                    }
                }
            }
            QueueCommand::ReplacePending(replace) => {
                if let Some(mut old) = shard_state.by_id.remove(&replace.superseded_item_id) {
                    old.superseded = true;
                    shard_state.by_key.remove(&old.client_item_key);
                }
                let item = &replace.replacement;
                shard_state.by_id.insert(
                    item.item_id,
                    PlannerItem {
                        item_id: item.item_id,
                        client_item_key: item.client_item_key.clone(),
                        state: ItemState::Pending,
                        item_version: 1,
                        group_key: item.group_key.clone(),
                        priority: item.priority.clone(),
                        not_before: item.not_before,
                        fenced: false,
                        superseded: false,
                        lease_token: None,
                    },
                );
                shard_state
                    .by_key
                    .insert(item.client_item_key.clone(), item.item_id);
                reindex(shard_state, item.item_id);
            }
            QueueCommand::FenceLease(fence) => {
                for id in &fence.item_ids {
                    if let Some(item) = shard_state.by_id.get_mut(id) {
                        item.fenced = true;
                    }
                }
            }
            QueueCommand::UnfenceLease(unfence) => {
                for id in &unfence.item_ids {
                    if let Some(item) = shard_state.by_id.get_mut(id) {
                        item.fenced = false;
                    }
                }
            }
            _ => {}
        }
    }
}

#[derive(Debug)]
pub struct Reservation(Undo);

fn snapshot_item(item: &PlannerItem) -> BatchUpdateSnapshotItem {
    BatchUpdateSnapshotItem {
        item_id: item.item_id,
        client_item_key: item.client_item_key.clone(),
        state: item.state,
        item_version: item.item_version,
        fenced: item.fenced,
        superseded: item.superseded,
    }
}

fn apply_update_to_item(item: &mut PlannerItem, update: &UpdateFieldsCommand) {
    item.item_version = item.item_version.saturating_add(1);
    if let ScheduleUpdate::Set(next) = &update.set_priority {
        item.priority = next.clone();
    }
    if let ScheduleUpdate::Set(next) = update.set_not_before {
        item.not_before = next;
    }
    let _ = &update.payload;
    if matches!(update.payload, PayloadUpdate::Keep) {
        // bodies stay on Turso
    }
}

fn apply_finalize_to_shard(shard: &mut Shard, outcome: &FinalizeOutcome) {
    let Some(item) = shard.by_id.get_mut(&outcome.item_id) else {
        return;
    };
    match outcome.kind {
        FinalizeKind::Complete => {
            item.state = ItemState::Complete;
            item.lease_token = None;
        }
        FinalizeKind::Fail => {
            item.state = ItemState::Failed;
            item.lease_token = None;
        }
        FinalizeKind::Retry | FinalizeKind::Release | FinalizeKind::Rearm => {
            item.state = ItemState::Pending;
            item.lease_token = None;
            if let Some(not_before) = outcome.not_before {
                item.not_before = Some(not_before);
            }
        }
    }
    item.item_version = item.item_version.saturating_add(1);
}

fn reindex(shard: &mut Shard, id: ItemId) {
    let eligible = shard
        .by_id
        .get(&id)
        .is_some_and(|item| item.state == ItemState::Pending && !item.fenced && !item.superseded);
    if eligible {
        shard.pending.insert(id);
    } else {
        shard.pending.remove(&id);
    }
}

fn bump<K: Eq + std::hash::Hash>(map: &mut HashMap<K, u32>, key: K) {
    *map.entry(key).or_insert(0) += 1;
}

fn unbump<K: Eq + std::hash::Hash>(map: &mut HashMap<K, u32>, key: K) {
    if let Some(count) = map.get_mut(&key) {
        *count = count.saturating_sub(1);
        if *count == 0 {
            map.remove(&key);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fireweed_core::{QueueId, TenantId};
    use fireweed_engine::UpdateFieldsCommand;

    fn shard() -> QueueKey {
        QueueKey::new(TenantId::new("t").unwrap(), QueueId::new("q").unwrap())
    }

    fn key(n: u8) -> ClientItemKey {
        ClientItemKey::new(format!("k{n}")).unwrap()
    }

    fn id(n: u8) -> ItemId {
        ItemId::mint(1, 1, n as u32)
    }

    #[test]
    fn push_then_update_without_turso() {
        let map = PlannerMap::new();
        let q = shard();
        let reservation = map
            .reserve_push(&q, &[(key(1), id(1), None, None, None)], None, None)
            .unwrap();
        map.commit(reservation);
        let snap = map.snapshot(&q, &[key(1)], &[]);
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].state, ItemState::Pending);
        let update = UpdateFieldsCommand {
            item_id: id(1),
            field_ops: Default::default(),
            payload: PayloadUpdate::Keep,
            set_priority: ScheduleUpdate::Keep,
            set_not_before: ScheduleUpdate::Keep,
            set_entity_document: None,
            set_fields: None,
            set_metadata: None,
            set_gate_keys: None,
            api001_batch: true,
        };
        let reservation = map.reserve_updates(&q, &[update]).unwrap();
        map.commit(reservation);
        let snap = map.snapshot(&q, &[key(1)], &[]);
        assert_eq!(snap[0].item_version, 2);
    }

    #[test]
    fn duplicate_key_is_conflict_before_append() {
        let map = PlannerMap::new();
        let q = shard();
        let first = map
            .reserve_push(&q, &[(key(1), id(1), None, None, None)], None, None)
            .unwrap();
        let err = map
            .reserve_push(&q, &[(key(1), id(2), None, None, None)], None, None)
            .unwrap_err();
        assert!(matches!(err, EngineError::Conflict));
        map.rollback(first);
        map.reserve_push(&q, &[(key(1), id(2), None, None, None)], None, None)
            .unwrap();
    }

    #[test]
    fn leased_item_is_visible_to_batch_update_snapshot() {
        let map = PlannerMap::new();
        let q = shard();
        let push = map
            .reserve_push(&q, &[(key(1), id(1), None, None, None)], None, None)
            .unwrap();
        map.commit(push);
        let token = fireweed_core::LeaseToken::new("lease-1").unwrap();
        let claim = map.reserve_claim(&q, &[id(1)], &token).unwrap();
        map.commit(claim);
        let snap = map.snapshot(&q, &[key(1)], &[]);
        assert_eq!(snap[0].state, ItemState::Leased);
    }
}
