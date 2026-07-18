//! SP-02 deterministic operation-level simulation over the real synchronous segmented log.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::{Arc, Mutex};

use pqueue_conformance::{envelope, item, qdef, shard};
use pqueue_engine::{EngineError, EngineResult, PushCommand, QueueCommand};
use pqueue_objectlog::segmented::{
    BlobStore, FaultCutPoint, FaultHook, SegmentConfig, SegmentedObjectLog,
};
use pqueue_objectlog::simulation_support::{
    SimulationBlobPhase as BlobPhase, SimulationDurableCut, production_fault_cut,
};
use pqueue_sim_support::{
    Disposition, DurableCut, Generator, HARNESS_VERSION, Model, Operation, StoreResult,
    TRACE_SCHEMA_VERSION, render_trace, shrink_invariant,
};
use serde::Deserialize;

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum SutMutant {
    None,
    CommittedAt,
    StaleWatermarkCache,
    StaleWriter,
    DeleteBeforeAdvance,
    DuplicateRetry,
    DropAcknowledged,
    HideSuccessVisibility,
}

#[derive(Clone, Copy, Debug)]
struct Script {
    phase: BlobPhase,
    result: StoreResult,
    ordinal: usize,
}

#[derive(Clone, Debug)]
struct StoreEvent {
    phase: BlobPhase,
    result: StoreResult,
    effect: bool,
    key: String,
}

struct StoreState {
    objects: BTreeMap<String, Vec<u8>>,
    versions: Vec<BTreeMap<String, Vec<u8>>>,
    scripts: VecDeque<Script>,
    persistent_scripts: BTreeMap<BlobPhase, StoreResult>,
    phase_calls: BTreeMap<BlobPhase, usize>,
    events: Vec<StoreEvent>,
    mutant: SutMutant,
    head_phase: BlobPhase,
}

impl Default for StoreState {
    fn default() -> Self {
        Self {
            objects: BTreeMap::new(),
            versions: Vec::new(),
            scripts: VecDeque::new(),
            persistent_scripts: BTreeMap::new(),
            phase_calls: BTreeMap::new(),
            events: Vec::new(),
            mutant: SutMutant::None,
            head_phase: BlobPhase::EpochHead,
        }
    }
}

#[derive(Default)]
struct VersionedFakeStore {
    state: Mutex<StoreState>,
}

impl VersionedFakeStore {
    fn set_mutant(&self, mutant: SutMutant) {
        self.state.lock().unwrap().mutant = mutant;
    }
    fn script(&self, phase: BlobPhase, result: StoreResult) {
        self.script_nth(phase, result, 1);
    }
    fn script_nth(&self, phase: BlobPhase, result: StoreResult, ordinal: usize) {
        let mut state = self.state.lock().unwrap();
        let base = state.phase_calls.get(&phase).copied().unwrap_or(0);
        state.scripts.push_back(Script {
            phase,
            result,
            ordinal: base + ordinal,
        });
    }
    fn script_persistent(&self, phase: BlobPhase, result: StoreResult) {
        self.state
            .lock()
            .unwrap()
            .persistent_scripts
            .insert(phase, result);
    }
    fn clear_persistent_script(&self, phase: BlobPhase) {
        self.state.lock().unwrap().persistent_scripts.remove(&phase);
    }
    fn clear_scripts(&self, phase: BlobPhase) {
        self.state
            .lock()
            .unwrap()
            .scripts
            .retain(|script| script.phase != phase);
    }
    fn snapshot(&self) -> BTreeMap<String, Vec<u8>> {
        self.state.lock().unwrap().objects.clone()
    }
    fn events(&self) -> Vec<StoreEvent> {
        self.state.lock().unwrap().events.clone()
    }

    fn set_head_phase(&self, phase: BlobPhase) {
        self.state.lock().unwrap().head_phase = phase;
    }

    fn phase(key: &str, body: &[u8], delete: bool, list: bool, head_phase: BlobPhase) -> BlobPhase {
        if list {
            return BlobPhase::ListPage;
        }
        if delete {
            return BlobPhase::Delete;
        }
        if key.ends_with("read_horizon.json") || key.ends_with("~watermark.json") {
            return BlobPhase::Horizon;
        }
        if key.ends_with(".seg") {
            return BlobPhase::Segment;
        }
        if key.contains("/manifest_candidates/") {
            let entry = serde_json::from_slice::<serde_json::Value>(body)
                .ok()
                .map(|value| value.get("entry").cloned().unwrap_or(value));
            if entry
                .as_ref()
                .and_then(|entry| entry.get("fence"))
                .and_then(|value| value.as_bool())
                == Some(true)
            {
                return BlobPhase::EpochCandidate;
            }
            if entry
                .as_ref()
                .and_then(|value| value.get("retention_floor_through"))
                .is_some_and(|value| !value.is_null())
            {
                return BlobPhase::Floor;
            }
            return BlobPhase::ManifestCandidate;
        }
        if key.contains("/authority_head/") {
            let value = serde_json::from_slice::<serde_json::Value>(body).unwrap_or_default();
            if value
                .get("tail_candidate_key")
                .is_some_and(|value| !value.is_null())
            {
                return head_phase;
            }
            if value
                .get("retention_floor_through")
                .is_some_and(|value| !value.is_null())
            {
                return BlobPhase::Floor;
            }
            return BlobPhase::EpochHead;
        }
        BlobPhase::ManifestHead
    }

    fn scripted(state: &mut StoreState, phase: BlobPhase) -> StoreResult {
        let call = state.phase_calls.entry(phase).or_default();
        *call += 1;
        if let Some(result) = state.persistent_scripts.get(&phase) {
            return *result;
        }
        if let Some(index) = state
            .scripts
            .iter()
            .position(|script| script.phase == phase && script.ordinal == *call)
        {
            state.scripts.remove(index).unwrap().result
        } else {
            StoreResult::Success
        }
    }

    fn transform(state: &StoreState, phase: BlobPhase, body: &[u8]) -> Vec<u8> {
        if state.mutant == SutMutant::StaleWatermarkCache && phase == BlobPhase::Horizon {
            let Ok(mut value) = serde_json::from_slice::<serde_json::Value>(body) else {
                return body.to_vec();
            };
            if let Some(index) = value.get("index").and_then(|index| index.as_u64()) {
                value["index"] = serde_json::Value::from(index + 1);
                return serde_json::to_vec(&value).unwrap();
            }
        }
        if state.mutant != SutMutant::CommittedAt || phase != BlobPhase::ManifestCandidate {
            return body.to_vec();
        }
        let Ok(mut value) = serde_json::from_slice::<serde_json::Value>(body) else {
            return body.to_vec();
        };
        let entry = if value.get("entry").is_some() {
            value.get_mut("entry").unwrap()
        } else {
            &mut value
        };
        if entry.get("segment_key").is_some_and(|key| !key.is_null()) {
            entry["committed_at_ms"] = serde_json::Value::from(0);
        }
        serde_json::to_vec(&value).unwrap()
    }

    fn record(
        state: &mut StoreState,
        phase: BlobPhase,
        result: StoreResult,
        effect: bool,
        key: &str,
    ) {
        if effect {
            state.versions.push(state.objects.clone());
        }
        state.events.push(StoreEvent {
            phase,
            result,
            effect,
            key: key.into(),
        });
    }

    fn list_impl(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        limit: usize,
    ) -> EngineResult<Vec<String>> {
        let mut state = self.state.lock().unwrap();
        let result = Self::scripted(&mut state, BlobPhase::ListPage);
        if result == StoreResult::FailureBeforeEffect {
            return Err(EngineError::Storage("scripted list failure".into()));
        }
        let source = if result == StoreResult::StaleList && state.versions.len() > 1 {
            &state.versions[state.versions.len() - 2]
        } else {
            &state.objects
        };
        let mut keys: Vec<_> = source
            .keys()
            .filter(|key| {
                key.starts_with(prefix) && start_after.is_none_or(|cursor| key.as_str() > cursor)
            })
            .cloned()
            .collect();
        if state.mutant == SutMutant::StaleWatermarkCache
            && prefix.contains("manifest_head/")
            && let Some(bytes) = state
                .objects
                .iter()
                .find(|(key, _)| key.ends_with("read_horizon.json"))
                .map(|(_, bytes)| bytes)
            && let Some(index) = serde_json::from_slice::<serde_json::Value>(bytes)
                .ok()
                .and_then(|value| value.get("index").and_then(|index| index.as_u64()))
        {
            keys.retain(|key| parse_index(key).is_none_or(|candidate| candidate > index));
        }
        keys.sort();
        if result == StoreResult::IncompletePage && !keys.is_empty() {
            keys.truncate(keys.len().div_ceil(2));
        }
        keys.truncate(limit);
        Self::record(&mut state, BlobPhase::ListPage, result, false, prefix);
        Ok(keys)
    }

    fn drop_latest_commit(&self) {
        let mut state = self.state.lock().unwrap();
        let head = state
            .objects
            .keys()
            .filter(|key| key.contains("/authority_head/"))
            .max()
            .cloned();
        if let Some(head) = head {
            state.objects.remove(&head);
        }
    }
}

impl BlobStore for VersionedFakeStore {
    fn put(&self, key: &str, body: &[u8]) -> EngineResult<()> {
        let mut state = self.state.lock().unwrap();
        let phase = Self::phase(key, body, false, false, state.head_phase);
        let result = Self::scripted(&mut state, phase);
        if result == StoreResult::FailureBeforeEffect {
            Self::record(&mut state, phase, result, false, key);
            return Err(EngineError::Storage("scripted put failure".into()));
        }
        let body = Self::transform(&state, phase, body);
        state.objects.insert(key.into(), body);
        Self::record(&mut state, phase, result, true, key);
        if result == StoreResult::EffectThenError {
            Err(EngineError::Storage("scripted lost put response".into()))
        } else {
            Ok(())
        }
    }
    fn put_if_absent(&self, key: &str, body: &[u8]) -> EngineResult<bool> {
        let mut state = self.state.lock().unwrap();
        let phase = Self::phase(key, body, false, false, state.head_phase);
        let result = Self::scripted(&mut state, phase);
        if result == StoreResult::FailureBeforeEffect {
            Self::record(&mut state, phase, result, false, key);
            return Err(EngineError::Storage("scripted create failure".into()));
        }
        if result == StoreResult::CasLoss {
            Self::record(&mut state, phase, result, false, key);
            return Ok(false);
        }
        if state.objects.contains_key(key) {
            Self::record(&mut state, phase, StoreResult::CasLoss, false, key);
            return Ok(false);
        }
        let body = Self::transform(&state, phase, body);
        state.objects.insert(key.into(), body);
        Self::record(&mut state, phase, result, true, key);
        if result == StoreResult::EffectThenError {
            Err(EngineError::Storage("scripted ambiguous create".into()))
        } else {
            Ok(true)
        }
    }
    fn get(&self, key: &str) -> EngineResult<Option<Vec<u8>>> {
        Ok(self.state.lock().unwrap().objects.get(key).cloned())
    }
    fn delete(&self, key: &str) -> EngineResult<bool> {
        let mut state = self.state.lock().unwrap();
        let result = Self::scripted(&mut state, BlobPhase::Delete);
        let deleted_segments = state
            .events
            .iter()
            .filter(|event| {
                event.phase == BlobPhase::Delete && event.effect && event.key.ends_with(".seg")
            })
            .count();
        if state.mutant == SutMutant::StaleWatermarkCache
            && key.ends_with(".seg")
            && deleted_segments == 1
        {
            Self::record(
                &mut state,
                BlobPhase::Delete,
                StoreResult::FailureBeforeEffect,
                false,
                key,
            );
            return Err(EngineError::Storage(
                "historical partial-delete interruption".into(),
            ));
        }
        if result == StoreResult::FailureBeforeEffect {
            Self::record(&mut state, BlobPhase::Delete, result, false, key);
            return Err(EngineError::Storage("scripted delete failure".into()));
        }
        let removed = state.objects.remove(key).is_some();
        Self::record(&mut state, BlobPhase::Delete, result, removed, key);
        if result == StoreResult::EffectThenError {
            Err(EngineError::Storage("scripted ambiguous delete".into()))
        } else {
            Ok(removed)
        }
    }
    fn list(&self, prefix: &str) -> EngineResult<Vec<String>> {
        self.list_impl(prefix, None, usize::MAX)
    }
    fn list_page(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        limit: usize,
    ) -> EngineResult<Vec<String>> {
        self.list_impl(prefix, start_after, limit)
    }
    fn list_from(&self, prefix: &str, start_after: &str) -> EngineResult<Vec<String>> {
        self.list_impl(prefix, Some(start_after), usize::MAX)
    }
}

fn parse_index(key: &str) -> Option<u64> {
    key.rsplit('/')
        .next()?
        .split(['.', '~'])
        .next()?
        .parse()
        .ok()
}

struct CrashAt(FaultCutPoint);
impl FaultHook for CrashAt {
    fn fault_point(&self, cut: FaultCutPoint) -> EngineResult<()> {
        if cut == self.0 {
            Err(EngineError::Storage(format!("crash at {cut:?}")))
        } else {
            Ok(())
        }
    }
}

fn production_cut(cut: DurableCut) -> Option<FaultCutPoint> {
    let reusable = match cut {
        DurableCut::BeforeSegmentWrite => SimulationDurableCut::BeforeSegmentWrite,
        DurableCut::AfterSegmentWriteBeforeManifest => {
            SimulationDurableCut::AfterSegmentWriteBeforeManifest
        }
        DurableCut::AfterManifestCandidateBeforeHead => {
            SimulationDurableCut::AfterManifestCandidateBeforeHead
        }
        DurableCut::AfterManifestBeforeAck => SimulationDurableCut::AfterManifestBeforeAck,
        DurableCut::DuringOwnerReassignment => SimulationDurableCut::DuringOwnerReassignment,
        DurableCut::DuringSegmentExpiry => SimulationDurableCut::DuringSegmentExpiry,
        _ => return None,
    };
    Some(production_fault_cut(reusable))
}

fn cfg() -> SegmentConfig {
    SegmentConfig::new(1 << 20, 100).unwrap()
}
fn push(request: u64, created_at_ms: i64) -> pqueue_engine::CommandEnvelope {
    let mut env = envelope(
        QueueCommand::Push(PushCommand {
            items: vec![item(
                &request.to_string(),
                &format!("k{request}"),
                request as i64,
            )],
        }),
        vec![],
    );
    env.command_id = pqueue_engine::CommandId::new(format!("c{request}"));
    env.created_at = pqueue_core::UtcTimestamp::new(
        created_at_ms.div_euclid(1000),
        (created_at_ms.rem_euclid(1000) * 1_000_000) as u32,
    )
    .unwrap();
    env
}

#[derive(Clone, Debug, Default)]
struct ProductionSnapshot {
    disposition: Disposition,
    epoch: u64,
    next_sequence: u64,
    floor: Option<u64>,
    read_horizon: Option<u64>,
    deletion_watermark: Option<u64>,
    visible: Vec<u64>,
    acknowledged: Vec<u64>,
    unknown: Vec<u64>,
    segment_objects: usize,
}

struct ProductionRunner {
    store: Arc<VersionedFakeStore>,
    log: SegmentedObjectLog<Arc<VersionedFakeStore>>,
    epoch: u64,
    acknowledged: BTreeSet<u64>,
    unknown: BTreeSet<u64>,
    buffered: BTreeSet<u64>,
    submitted: BTreeMap<u64, i64>,
    disposition: Disposition,
    mutant: SutMutant,
}

impl ProductionRunner {
    fn new(mutant: SutMutant) -> Self {
        let store = Arc::new(VersionedFakeStore::default());
        store.set_mutant(mutant);
        let log = SegmentedObjectLog::open(store.clone(), cfg());
        log.create_queue(&qdef()).unwrap();
        log.fence_epoch(&shard(), 0, 0).unwrap();
        store.set_head_phase(BlobPhase::ManifestHead);
        Self {
            store,
            log,
            epoch: 0,
            acknowledged: BTreeSet::new(),
            unknown: BTreeSet::new(),
            buffered: BTreeSet::new(),
            submitted: BTreeMap::new(),
            disposition: Disposition::None,
            mutant,
        }
    }
    fn restart(&mut self) -> EngineResult<()> {
        self.buffered.clear();
        self.log = SegmentedObjectLog::open(self.store.clone(), cfg());
        self.log.create_queue(&qdef())
    }
    fn visible(&self) -> EngineResult<Vec<u64>> {
        let from = self
            .log
            .read_retention_floor(&shard())?
            .map_or(0, |position| position.sequence + 1);
        let mut ids: Vec<_> = self
            .log
            .read_from(&shard(), from)?
            .into_iter()
            .map(|(_, env)| env.command_id.0.strip_prefix('c').unwrap().parse().unwrap())
            .collect();
        ids.sort_unstable();
        Ok(ids)
    }
    fn snapshot(&self) -> EngineResult<ProductionSnapshot> {
        let visible = if self.mutant == SutMutant::HideSuccessVisibility {
            Vec::new()
        } else {
            self.visible()?
        };
        let objects = self.store.snapshot();
        let mut epoch = self.epoch;
        let mut next_sequence = 0;
        for (key, bytes) in &objects {
            if key.contains("/authority_head/")
                && let Ok(value) = serde_json::from_slice::<serde_json::Value>(bytes)
            {
                epoch = epoch.max(
                    value
                        .get("current_epoch")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(0),
                );
                next_sequence =
                    next_sequence.max(value.get("next_seq").and_then(|v| v.as_u64()).unwrap_or(0));
            }
        }
        let mut candidate_keys: Vec<String> = objects
            .iter()
            .filter(|(key, _)| key.contains("/authority_head/"))
            .filter_map(|(_, bytes)| serde_json::from_slice::<serde_json::Value>(bytes).ok())
            .filter_map(|value| {
                value
                    .get("tail_candidate_key")
                    .and_then(|key| key.as_str())
                    .map(str::to_owned)
            })
            .collect();
        let mut visited_candidates = BTreeSet::new();
        let mut referenced_segments = BTreeSet::new();
        while let Some(candidate_key) = candidate_keys.pop() {
            if !visited_candidates.insert(candidate_key.clone()) {
                continue;
            }
            let Some(value) = objects
                .get(&candidate_key)
                .and_then(|bytes| serde_json::from_slice::<serde_json::Value>(bytes).ok())
            else {
                continue;
            };
            if let Some(previous) = value
                .get("previous_candidate_key")
                .and_then(|key| key.as_str())
            {
                candidate_keys.push(previous.to_owned());
            }
            let entry = value.get("entry").unwrap_or(&value);
            if let Some(segment_key) = entry.get("segment_key").and_then(|key| key.as_str()) {
                referenced_segments.insert(segment_key.to_owned());
            }
        }
        Ok(ProductionSnapshot {
            disposition: self.disposition,
            epoch,
            next_sequence,
            floor: self
                .log
                .read_retention_floor(&shard())?
                .map(|position| position.sequence),
            read_horizon: self.log.read_read_horizon(&shard())?,
            deletion_watermark: self.log.read_manifest_deletion_watermark(&shard())?,
            visible,
            acknowledged: self.acknowledged.iter().copied().collect(),
            unknown: self.unknown.iter().copied().collect(),
            segment_objects: referenced_segments
                .iter()
                .filter(|key| objects.contains_key(*key))
                .count(),
        })
    }
    fn committed_times(&self) -> Vec<i64> {
        let objects = self.store.snapshot();
        let referenced: BTreeSet<String> = objects
            .iter()
            .filter(|(key, _)| key.contains("/authority_head/"))
            .filter_map(|(_, bytes)| serde_json::from_slice::<serde_json::Value>(bytes).ok())
            .filter_map(|value| {
                value
                    .get("tail_candidate_key")
                    .and_then(|key| key.as_str())
                    .map(str::to_owned)
            })
            .collect();
        let mut out: Vec<_> = objects
            .iter()
            .filter(|(key, _)| referenced.contains(*key))
            .filter_map(|(_, bytes)| serde_json::from_slice::<serde_json::Value>(bytes).ok())
            .map(|value| value.get("entry").cloned().unwrap_or(value))
            .filter(|entry| {
                entry
                    .get("segment_key")
                    .and_then(|key| key.as_str())
                    .is_some_and(|key| objects.contains_key(key))
            })
            .filter_map(|entry| entry.get("committed_at_ms").and_then(|time| time.as_i64()))
            .collect();
        out.sort_unstable();
        out
    }
    fn apply(&mut self, operation: &Operation) {
        match *operation {
            Operation::Accept {
                request,
                created_at_ms,
            } => {
                self.submitted.insert(request, created_at_ms);
                let writer_epoch = if self.mutant == SutMutant::StaleWriter {
                    self.epoch.saturating_sub(1)
                } else {
                    self.epoch
                };
                if self
                    .log
                    .enqueue(
                        &shard(),
                        &[push(request, created_at_ms)],
                        writer_epoch,
                        created_at_ms,
                    )
                    .is_ok()
                {
                    self.buffered.insert(request);
                }
                self.disposition = Disposition::None;
            }
            Operation::Seal {
                expected_epoch,
                now_ms,
                result,
            } => {
                self.store.set_head_phase(BlobPhase::ManifestHead);
                match result {
                    StoreResult::FailureBeforeEffect => {
                        self.store.script(BlobPhase::Segment, result)
                    }
                    StoreResult::EffectThenError => {
                        self.store.script(BlobPhase::ManifestHead, result)
                    }
                    StoreResult::CasLoss => {
                        self.store
                            .script_persistent(BlobPhase::ManifestHead, result);
                    }
                    _ => {}
                }
                let expected = if self.mutant == SutMutant::StaleWriter {
                    self.epoch
                } else {
                    expected_epoch
                };
                let before: BTreeSet<_> = self.visible().unwrap_or_default().into_iter().collect();
                match self.log.seal(&shard(), expected, now_ms) {
                    Ok(positions) if !positions.is_empty() => {
                        let after = self.visible().unwrap_or_default();
                        for request in after
                            .into_iter()
                            .filter(|request| !before.contains(request))
                        {
                            self.acknowledged.insert(request);
                        }
                        self.disposition = Disposition::Success;
                    }
                    Ok(_) => self.disposition = Disposition::None,
                    Err(_) => {
                        let after = self
                            .restart()
                            .and_then(|()| self.visible())
                            .unwrap_or_default();
                        let new: Vec<_> = after
                            .into_iter()
                            .filter(|request| !before.contains(request))
                            .collect();
                        if new.is_empty() {
                            self.disposition = Disposition::Rejected;
                        } else {
                            self.unknown.extend(new);
                            self.disposition = Disposition::Unknown;
                        }
                    }
                }
                self.store.clear_persistent_script(BlobPhase::ManifestHead);
                self.store.clear_scripts(BlobPhase::Segment);
                self.store.clear_scripts(BlobPhase::ManifestHead);
                let _ = self.restart();
                if self.mutant == SutMutant::DropAcknowledged
                    && self.disposition == Disposition::Success
                {
                    self.store.drop_latest_commit();
                    let _ = self.restart();
                }
            }
            Operation::Retry { request } => {
                let exists = self.visible().unwrap_or_default().contains(&request);
                if exists && self.mutant != SutMutant::DuplicateRetry {
                    self.unknown.remove(&request);
                    self.acknowledged.insert(request);
                    self.disposition = Disposition::Success;
                } else if exists {
                    self.unknown.remove(&request);
                    self.acknowledged.insert(request);
                    let _ = self.log.enqueue(
                        &shard(),
                        &[push(request, self.submitted[&request])],
                        self.epoch,
                        0,
                    );
                    self.buffered.insert(request);
                    self.disposition = Disposition::Success;
                } else if self.buffered.contains(&request) {
                    self.disposition = Disposition::None;
                } else if !self.submitted.contains_key(&request) {
                    self.disposition = Disposition::Rejected;
                } else if self
                    .log
                    .enqueue(
                        &shard(),
                        &[push(request, self.submitted[&request])],
                        self.epoch,
                        0,
                    )
                    .is_ok()
                {
                    self.buffered.insert(request);
                    self.disposition = Disposition::None;
                } else {
                    self.disposition = Disposition::Rejected;
                }
            }
            Operation::Fence { epoch, result } => {
                if epoch != self.epoch + 1 {
                    self.disposition = Disposition::Rejected;
                    return;
                }
                self.store.set_head_phase(BlobPhase::EpochHead);
                match result {
                    StoreResult::FailureBeforeEffect => {
                        self.store.script(BlobPhase::EpochHead, result)
                    }
                    StoreResult::EffectThenError => self.store.script(BlobPhase::EpochHead, result),
                    StoreResult::CasLoss => {
                        self.store.script_persistent(BlobPhase::EpochHead, result);
                    }
                    _ => {}
                }
                let before = self.epoch;
                let outcome = self.log.acquire_epoch(&shard(), 0);
                self.store.clear_persistent_script(BlobPhase::EpochHead);
                self.store.clear_scripts(BlobPhase::EpochHead);
                if result == StoreResult::Success || result == StoreResult::EffectThenError {
                    let _ = self.restart();
                }
                let observed = self
                    .snapshot()
                    .map(|snapshot| snapshot.epoch)
                    .unwrap_or(before);
                if observed > before {
                    self.epoch = observed;
                    self.disposition = if outcome.is_ok() {
                        Disposition::Success
                    } else {
                        Disposition::Unknown
                    };
                } else {
                    self.disposition = Disposition::Rejected;
                }
            }
            Operation::AdvanceHorizon { through_sequence } => {
                self.store.set_head_phase(BlobPhase::Floor);
                let position = self
                    .log
                    .read_from(&shard(), through_sequence)
                    .ok()
                    .and_then(|rows| {
                        rows.into_iter()
                            .find(|(position, _)| position.sequence == through_sequence)
                            .map(|(position, _)| position)
                    });
                self.disposition = position
                    .and_then(|position| {
                        self.log
                            .advance_retention_floor(&shard(), position, self.epoch)
                            .ok()
                    })
                    .map_or(Disposition::Rejected, |_| Disposition::Success);
            }
            Operation::DeleteThrough { through_sequence } => {
                if self.mutant == SutMutant::DeleteBeforeAdvance {
                    for key in self
                        .store
                        .snapshot()
                        .keys()
                        .filter(|key| key.ends_with(".seg"))
                        .cloned()
                        .collect::<Vec<_>>()
                    {
                        let _ = self.store.delete(&key);
                    }
                    self.disposition = Disposition::Success;
                } else {
                    let allowed = self
                        .log
                        .read_retention_floor(&shard())
                        .ok()
                        .flatten()
                        .is_some_and(|position| position.sequence >= through_sequence);
                    let expired = allowed
                        && self
                            .log
                            .expire_segments_through(&shard(), through_sequence, 1_000_000)
                            .is_ok();
                    self.disposition =
                        if expired || (allowed && self.mutant == SutMutant::StaleWatermarkCache) {
                            Disposition::Success
                        } else {
                            Disposition::Rejected
                        };
                }
            }
            Operation::Crash(cut) => self.crash(cut),
            Operation::Restart => {
                let _ = self.restart();
                self.disposition = Disposition::None;
            }
        }
    }
    fn crash(&mut self, cut: DurableCut) {
        let Some(production_cut) = production_cut(cut) else {
            self.disposition = Disposition::Rejected;
            return;
        };
        self.log
            .set_fault_hook(Some(Arc::new(CrashAt(production_cut))));
        let had_buffered_commands = !self.buffered.is_empty();
        let before = self.visible().unwrap_or_default();
        match cut {
            DurableCut::DuringOwnerReassignment => {
                let _ = self.log.acquire_epoch(&shard(), 0);
            }
            DurableCut::DuringSegmentExpiry => {
                if let Some(floor) = self.log.read_retention_floor(&shard()).ok().flatten() {
                    let _ = self
                        .log
                        .expire_segments_through(&shard(), floor.sequence, 1_000_000);
                }
            }
            _ => {
                let _ = self.log.seal(&shard(), self.epoch, 0);
            }
        }
        self.log.set_fault_hook(None);
        let _ = self.restart();
        let after = self.visible().unwrap_or_default();
        for request in after
            .into_iter()
            .filter(|request| !before.contains(request))
        {
            self.unknown.insert(request);
        }
        self.epoch = self
            .snapshot()
            .map(|snapshot| snapshot.epoch)
            .unwrap_or(self.epoch);
        self.disposition = if cut == DurableCut::AfterManifestBeforeAck && !had_buffered_commands {
            Disposition::None
        } else {
            Disposition::Unknown
        };
    }
}

#[derive(Clone, Debug)]
struct RunFailure {
    invariant: &'static str,
    detail: String,
}

fn compare(model: &Model, sut: &ProductionRunner, operation: &Operation) -> Result<(), RunFailure> {
    model.check_required().map_err(|violation| RunFailure {
        invariant: violation.invariant,
        detail: violation.detail,
    })?;
    let expected = model.snapshot();
    let actual = sut.snapshot().map_err(|error| RunFailure {
        invariant: if expected.acknowledged.is_empty() {
            "INV-10"
        } else {
            "INV-2"
        },
        detail: format!("recovery failed: {error:?}"),
    })?;
    if let Operation::Seal { expected_epoch, .. } = operation
        && *expected_epoch < expected.epoch
        && expected.visible_requests != actual.visible
    {
        return Err(RunFailure {
            invariant: "INV-1",
            detail: "stale epoch committed".into(),
        });
    }
    let unique: BTreeSet<_> = actual.visible.iter().copied().collect();
    if unique.len() != actual.visible.len() {
        return Err(RunFailure {
            invariant: "INV-14",
            detail: "duplicate retry committed twice".into(),
        });
    }
    if expected.epoch != actual.epoch {
        return Err(RunFailure {
            invariant: "INV-1",
            detail: format!("epoch {} != {}", expected.epoch, actual.epoch),
        });
    }
    if expected.next_sequence != actual.next_sequence {
        return Err(RunFailure {
            invariant: if sut.mutant == SutMutant::DropAcknowledged {
                "INV-2"
            } else {
                "INV-10"
            },
            detail: format!(
                "next sequence {} != {}",
                expected.next_sequence, actual.next_sequence
            ),
        });
    }
    if expected.floor != actual.floor {
        return Err(RunFailure {
            invariant: "GC-PROGRESS",
            detail: format!("floor {:?} != {:?}", expected.floor, actual.floor),
        });
    }
    if expected.deletion_watermark.is_some() != actual.deletion_watermark.is_some()
        || expected.deletion_watermark.is_some() != actual.read_horizon.is_some()
    {
        return Err(RunFailure {
            invariant: "GC-PROGRESS",
            detail: format!(
                "horizon/watermark {:?}/{:?} expected {:?}",
                actual.read_horizon, actual.deletion_watermark, expected.deletion_watermark
            ),
        });
    }
    if expected.visible_requests != actual.visible {
        return Err(RunFailure {
            invariant: "INV-12",
            detail: format!(
                "visible {:?} != {:?}",
                expected.visible_requests, actual.visible
            ),
        });
    }
    if expected.acknowledged != actual.acknowledged {
        return Err(RunFailure {
            invariant: "INV-2",
            detail: format!(
                "acks {:?} != {:?}",
                expected.acknowledged, actual.acknowledged
            ),
        });
    }
    if expected.unknown != actual.unknown {
        return Err(RunFailure {
            invariant: "INV-14",
            detail: format!("unknown {:?} != {:?}", expected.unknown, actual.unknown),
        });
    }
    if expected.last_disposition != actual.disposition
        && !(sut.mutant == SutMutant::StaleWatermarkCache
            && matches!(operation, Operation::DeleteThrough { .. }))
    {
        return Err(RunFailure {
            invariant: if matches!(
                operation,
                Operation::AdvanceHorizon { .. } | Operation::DeleteThrough { .. }
            ) {
                "GC-PROGRESS"
            } else {
                "INV-14"
            },
            detail: format!(
                "disposition {:?} != {:?} after {operation:?}",
                expected.last_disposition, actual.disposition
            ),
        });
    }
    if matches!(
        operation,
        Operation::Seal { .. }
            | Operation::Crash(
                DurableCut::BeforeSegmentWrite
                    | DurableCut::AfterSegmentWriteBeforeManifest
                    | DurableCut::AfterManifestCandidateBeforeHead
                    | DurableCut::AfterManifestBeforeAck
            )
    ) && model.committed_times() != sut.committed_times()
    {
        return Err(RunFailure {
            invariant: "INV-10",
            detail: format!(
                "committed_at {:?} != {:?}",
                model.committed_times(),
                sut.committed_times()
            ),
        });
    }
    if matches!(
        operation,
        Operation::DeleteThrough { .. } | Operation::Restart
    ) && !(sut.mutant == SutMutant::StaleWatermarkCache
        && matches!(operation, Operation::DeleteThrough { .. }))
    {
        let expected_segments = model
            .segments()
            .iter()
            .filter(|segment| !segment.deleted)
            .count();
        if expected_segments != actual.segment_objects {
            return Err(RunFailure {
                invariant: "GC-PROGRESS",
                detail: format!(
                    "physical segments {expected_segments} != {}",
                    actual.segment_objects
                ),
            });
        }
    }
    Ok(())
}

fn run(seed: u64, operations: &[Operation], mutant: SutMutant) -> Result<(), RunFailure> {
    let mut model = Model::default();
    let mut sut = ProductionRunner::new(mutant);
    for (index, operation) in operations.iter().enumerate() {
        model.apply(operation);
        sut.apply(operation);
        if matches!(
            operation,
            Operation::Seal { .. }
                | Operation::Fence { .. }
                | Operation::AdvanceHorizon { .. }
                | Operation::DeleteThrough { .. }
                | Operation::Crash(_)
                | Operation::Restart
                | Operation::Retry { .. }
        ) {
            compare(&model, &sut, operation).map_err(|mut failure| {
                failure.detail = format!(
                    "{}\nindex={index}\n{}",
                    failure.detail,
                    render_trace(seed, operations)
                );
                failure
            })?;
        }
    }
    Ok(())
}

#[derive(Debug)]
struct GeneratedRunFailure {
    seed: u64,
    invariant: &'static str,
    failing_index: usize,
    full_trace: String,
    minimized_trace: String,
    minimized_operations: Vec<Operation>,
    detail: String,
}

fn generated_run(
    seed: u64,
    length: usize,
    mutant: SutMutant,
) -> Result<(), Box<GeneratedRunFailure>> {
    let operations = Generator::new(seed).trace(length);
    let failure = match run(seed, &operations, mutant) {
        Ok(()) => return Ok(()),
        Err(failure) => failure,
    };
    let invariant = failure.invariant;
    let minimized = shrink_invariant(operations.clone(), 32, invariant, |candidate| {
        run(seed, candidate, mutant)
            .err()
            .map(|failure| failure.invariant)
    });
    let minimized_failure = run(seed, &minimized, mutant).expect_err("shrinker preserves failure");
    assert_eq!(minimized_failure.invariant, invariant);
    let failing_index = failure
        .detail
        .lines()
        .find_map(|line| line.strip_prefix("index="))
        .and_then(|index| index.parse().ok())
        .unwrap_or(operations.len().saturating_sub(1));
    Err(Box::new(GeneratedRunFailure {
        seed,
        invariant,
        failing_index,
        full_trace: render_trace(seed, &operations),
        minimized_trace: render_trace(seed, &minimized),
        minimized_operations: minimized,
        detail: failure.detail,
    }))
}

#[derive(Debug, Deserialize)]
struct CorpusEntry {
    schema_version: u16,
    minimum_harness_version: u16,
    seed: u64,
    name: String,
    mutant: SutMutant,
    expected_invariant: String,
    operations: Vec<CorpusOperation>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
enum CorpusOperation {
    Accept {
        request: u64,
        created_at_ms: i64,
    },
    Seal {
        expected_epoch: u64,
        now_ms: i64,
        result: CorpusStoreResult,
    },
    Retry {
        request: u64,
    },
    Fence {
        epoch: u64,
        result: CorpusStoreResult,
    },
    AdvanceHorizon {
        through_sequence: u64,
    },
    DeleteThrough {
        through_sequence: u64,
    },
    Crash {
        cut: CorpusCut,
    },
    Restart,
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum CorpusStoreResult {
    Success,
    FailureBeforeEffect,
    EffectThenError,
    CasLoss,
}
#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum CorpusCut {
    BeforeSegmentWrite,
    AfterSegmentWriteBeforeManifest,
    AfterManifestCandidateBeforeHead,
    AfterManifestBeforeAck,
    DuringOwnerReassignment,
    DuringSegmentExpiry,
}

impl From<CorpusOperation> for Operation {
    fn from(value: CorpusOperation) -> Self {
        match value {
            CorpusOperation::Accept {
                request,
                created_at_ms,
            } => Self::Accept {
                request,
                created_at_ms,
            },
            CorpusOperation::Seal {
                expected_epoch,
                now_ms,
                result,
            } => Self::Seal {
                expected_epoch,
                now_ms,
                result: result.into(),
            },
            CorpusOperation::Retry { request } => Self::Retry { request },
            CorpusOperation::Fence { epoch, result } => Self::Fence {
                epoch,
                result: result.into(),
            },
            CorpusOperation::AdvanceHorizon { through_sequence } => {
                Self::AdvanceHorizon { through_sequence }
            }
            CorpusOperation::DeleteThrough { through_sequence } => {
                Self::DeleteThrough { through_sequence }
            }
            CorpusOperation::Crash { cut } => Self::Crash(cut.into()),
            CorpusOperation::Restart => Self::Restart,
        }
    }
}
impl From<CorpusStoreResult> for StoreResult {
    fn from(value: CorpusStoreResult) -> Self {
        match value {
            CorpusStoreResult::Success => Self::Success,
            CorpusStoreResult::FailureBeforeEffect => Self::FailureBeforeEffect,
            CorpusStoreResult::EffectThenError => Self::EffectThenError,
            CorpusStoreResult::CasLoss => Self::CasLoss,
        }
    }
}
impl From<CorpusCut> for DurableCut {
    fn from(value: CorpusCut) -> Self {
        match value {
            CorpusCut::BeforeSegmentWrite => Self::BeforeSegmentWrite,
            CorpusCut::AfterSegmentWriteBeforeManifest => Self::AfterSegmentWriteBeforeManifest,
            CorpusCut::AfterManifestCandidateBeforeHead => Self::AfterManifestCandidateBeforeHead,
            CorpusCut::AfterManifestBeforeAck => Self::AfterManifestBeforeAck,
            CorpusCut::DuringOwnerReassignment => Self::DuringOwnerReassignment,
            CorpusCut::DuringSegmentExpiry => Self::DuringSegmentExpiry,
        }
    }
}

fn corpus() -> Vec<CorpusEntry> {
    include_str!("corpus/objectlog-simulation-v2.jsonl")
        .lines()
        .map(|line| serde_json::from_str(line).expect("typed corpus entry"))
        .collect()
}

#[test]
fn typed_corpus_replays_correct_path_and_named_mutant_with_expected_identity() {
    for entry in corpus() {
        assert_eq!(
            entry.schema_version, TRACE_SCHEMA_VERSION,
            "{} schema",
            entry.name
        );
        assert!(
            entry.minimum_harness_version <= HARNESS_VERSION,
            "{} harness",
            entry.name
        );
        let operations: Vec<_> = entry.operations.into_iter().map(Into::into).collect();
        run(entry.seed, &operations, SutMutant::None)
            .unwrap_or_else(|failure| panic!("correct {} failed: {failure:?}", entry.name));
        let failure =
            run(entry.seed, &operations, entry.mutant).expect_err("named mutant must fail");
        assert_eq!(
            failure.invariant, entry.expected_invariant,
            "{}: {}",
            entry.name, failure.detail
        );
    }
}

#[test]
fn every_real_fault_cut_interrupts_the_production_operation_and_recovers() {
    for cut in [
        DurableCut::BeforeSegmentWrite,
        DurableCut::AfterSegmentWriteBeforeManifest,
        DurableCut::AfterManifestCandidateBeforeHead,
        DurableCut::AfterManifestBeforeAck,
        DurableCut::DuringOwnerReassignment,
    ] {
        let operations = if cut == DurableCut::DuringOwnerReassignment {
            vec![Operation::Crash(cut)]
        } else {
            vec![
                Operation::Accept {
                    request: 1,
                    created_at_ms: 1,
                },
                Operation::Crash(cut),
            ]
        };
        run(7, &operations, SutMutant::None)
            .unwrap_or_else(|failure| panic!("{cut:?}: {failure:?}"));
    }
    let expiry = vec![
        Operation::Accept {
            request: 1,
            created_at_ms: 1,
        },
        Operation::Seal {
            expected_epoch: 0,
            now_ms: 1,
            result: StoreResult::Success,
        },
        Operation::AdvanceHorizon {
            through_sequence: 0,
        },
        Operation::Crash(DurableCut::DuringSegmentExpiry),
        Operation::Restart,
    ];
    run(8, &expiry, SutMutant::None).unwrap();
}

#[test]
fn stale_and_incomplete_pages_drive_real_recovery_and_are_recorded() {
    for result in [StoreResult::StaleList, StoreResult::IncompletePage] {
        let mut runner = ProductionRunner::new(SutMutant::None);
        runner.apply(&Operation::Accept {
            request: 1,
            created_at_ms: 1,
        });
        runner.apply(&Operation::Seal {
            expected_epoch: 0,
            now_ms: 1,
            result: StoreResult::Success,
        });
        runner.store.script(BlobPhase::ListPage, result);
        let _ = runner.restart();
        let events = runner.store.events();
        assert!(
            events
                .iter()
                .any(|event| event.phase == BlobPhase::ListPage && event.result == result)
        );
        match runner.visible() {
            Ok(visible) => assert_eq!(visible, vec![1], "{result:?} recovery silently truncated"),
            Err(EngineError::Conflict | EngineError::Storage(_)) => {}
            Err(error) => panic!("{result:?} returned non-fail-closed error: {error:?}"),
        }
    }
}

#[test]
fn historical_cache_mutant_uses_compatibility_cache_after_incomplete_delete() {
    let mut runner = ProductionRunner::new(SutMutant::StaleWatermarkCache);
    for operation in [
        Operation::Accept {
            request: 1,
            created_at_ms: 1,
        },
        Operation::Seal {
            expected_epoch: 0,
            now_ms: 1,
            result: StoreResult::Success,
        },
        Operation::Accept {
            request: 2,
            created_at_ms: 2,
        },
        Operation::Seal {
            expected_epoch: 0,
            now_ms: 2,
            result: StoreResult::Success,
        },
        Operation::AdvanceHorizon {
            through_sequence: 1,
        },
        Operation::DeleteThrough {
            through_sequence: 1,
        },
        Operation::Restart,
    ] {
        runner.apply(&operation);
    }
    let events = runner.store.events();
    let failed_delete = events
        .iter()
        .position(|event| {
            event.phase == BlobPhase::Delete
                && event.key.ends_with(".seg")
                && event.result == StoreResult::FailureBeforeEffect
        })
        .expect("partial segment delete");
    let cache_write = events
        .iter()
        .position(|event| event.phase == BlobPhase::Horizon && event.effect)
        .expect("compatibility cache write");
    let cache_authority_list = events
        .iter()
        .enumerate()
        .find(|(index, event)| {
            *index > cache_write
                && event.phase == BlobPhase::ListPage
                && event.key.contains("manifest_head/")
        })
        .map(|(index, _)| index)
        .expect("restart LIST after compatibility cache");
    assert!(failed_delete < cache_write && cache_write < cache_authority_list);
    assert_eq!(
        runner.snapshot().unwrap().segment_objects,
        1,
        "one undeleted segment is hidden from stale progress"
    );
}

#[test]
fn generated_failures_print_seed_and_identity_preserving_minimized_trace() {
    let failure = (0..1_024)
        .find_map(|seed| {
            generated_run(seed, 64, SutMutant::None)
                .is_ok()
                .then(|| generated_run(seed, 64, SutMutant::HideSuccessVisibility).err())
                .flatten()
        })
        .expect("generated corpus must exercise a successful visible commit");
    assert_eq!(
        failure.invariant, "INV-12",
        "seed={} index={}\nfull:\n{}\nminimized:\n{}",
        failure.seed, failure.failing_index, failure.full_trace, failure.minimized_trace
    );
    assert!(
        failure.minimized_operations.len() <= 32,
        "seed={} index={}\n{}",
        failure.seed,
        failure.failing_index,
        failure.minimized_trace
    );
}

#[test]
fn unmutated_multi_seed_generated_traces_match_production() {
    for seed in 0..128 {
        if let Err(failure) = generated_run(seed, 48, SutMutant::None) {
            panic!(
                "seed={} invariant={} index={} detail={}\nfull:\n{}\nminimized:\n{}",
                failure.seed,
                failure.invariant,
                failure.failing_index,
                failure.detail,
                failure.full_trace,
                failure.minimized_trace
            );
        }
    }
}

#[test]
fn phase_scripts_hit_intended_durable_effects() {
    let mut runner = ProductionRunner::new(SutMutant::None);
    runner.apply(&Operation::Accept {
        request: 1,
        created_at_ms: 1,
    });
    runner.apply(&Operation::Seal {
        expected_epoch: 0,
        now_ms: 1,
        result: StoreResult::EffectThenError,
    });
    assert!(
        runner
            .store
            .events()
            .iter()
            .any(|event| event.phase == BlobPhase::ManifestHead
                && event.result == StoreResult::EffectThenError
                && event.effect)
    );
    assert_eq!(
        runner.disposition,
        Disposition::Success,
        "SP-03 resolves effect-then-error by rereading the exact authoritative create-only address"
    );

    let mut cas_loser = ProductionRunner::new(SutMutant::None);
    cas_loser.apply(&Operation::Accept {
        request: 2,
        created_at_ms: 2,
    });
    cas_loser.apply(&Operation::Seal {
        expected_epoch: 0,
        now_ms: 2,
        result: StoreResult::CasLoss,
    });
    assert!(
        cas_loser
            .store
            .events()
            .iter()
            .any(|event| event.phase == BlobPhase::ManifestHead
                && event.result == StoreResult::CasLoss
                && !event.effect)
    );

    let mut segment_failure = ProductionRunner::new(SutMutant::None);
    segment_failure.apply(&Operation::Accept {
        request: 3,
        created_at_ms: 3,
    });
    segment_failure.apply(&Operation::Seal {
        expected_epoch: 0,
        now_ms: 3,
        result: StoreResult::FailureBeforeEffect,
    });
    assert!(
        segment_failure
            .store
            .events()
            .iter()
            .any(|event| event.phase == BlobPhase::Segment
                && event.result == StoreResult::FailureBeforeEffect
                && !event.effect)
    );
}

#[test]
fn fence_phase_matrix_records_authoritative_epoch_outcomes() {
    for (result, epoch, disposition, effect) in [
        (StoreResult::Success, 1, Disposition::Success, true),
        (
            StoreResult::FailureBeforeEffect,
            0,
            Disposition::Rejected,
            false,
        ),
        (StoreResult::EffectThenError, 1, Disposition::Success, true),
        (StoreResult::CasLoss, 0, Disposition::Rejected, false),
    ] {
        let mut runner = ProductionRunner::new(SutMutant::None);
        let baseline = runner.store.events().len();
        runner.apply(&Operation::Fence { epoch: 1, result });
        let snapshot = runner.snapshot().unwrap();
        assert_eq!(
            (snapshot.epoch, snapshot.disposition),
            (epoch, disposition),
            "{result:?}"
        );
        let events = runner.store.events();
        let events = &events[baseline..];
        assert!(
            events
                .iter()
                .any(|event| event.phase == BlobPhase::EpochHead
                    && event.result == result
                    && event.effect == effect),
            "{result:?}"
        );
        assert!(
            !events
                .iter()
                .any(|event| event.key.contains("/authority_head/")
                    && event.phase == BlobPhase::ManifestHead),
            "epoch update must not be mislabeled as data manifest head for {result:?}"
        );
    }
}
