#![forbid(unsafe_code)]
//! # pqueue-objectlog
//!
//! Driven adapter, **eventual-apply durability class**. The command log is a set of immutable
//! per-command **objects** on an object store; a local filesystem stands in for S3 here (each command is
//! one object file — no server). The priority-ordered projection is the shared
//! [`pqueue_projection::ProjectionData`] materialization, **rebuilt from the object log** on open.
//!
//! Class semantics (TD-007 §2 / Invariant 2): upsert (the atomic XDEL+XADD `replace_if_pending`) is NOT
//! offered on this class — it returns [`EngineError::Unavailable`] (`-ERR pqueue unavailable`). The
//! durability boundary is the object write; the in-memory projection is a derived view reconstructed by
//! replaying the objects, so a lost/late projection update is always recoverable from the durable log.
//!
//! Write ordering is **durable-first**: the command object is written, then the persisted high-water
//! object, then the in-memory `apply_command` (which is infallible because the orchestration ports
//! pre-validate — see the INVARIANT). Object names are zero-padded sequence numbers so lexical order is
//! replay order; the next sequence is `max(existing)+1`, compaction-safe.

mod async_commit;
mod async_log;
pub mod compose_log;
pub mod maintenance;
pub mod object_store_observability;
mod segment_integrity;
pub mod segmented;
#[doc(hidden)]
pub mod simulation_support;

pub use async_commit::{
    ByteAdmissionWaitPolicy, GroupCommitObjectLogProjectionCommitter, MAX_RECOVERY_PAGE_SIZE,
    ObjectLogByteAdmissionConfig, ObjectLogByteAdmissionSnapshot, ObjectLogProjectionCommitter,
    PreparedObjectLogCommit, prepare_serialized_commands, prepare_serialized_commands_for_format,
    serialized_peak_charge,
};
pub use async_log::{
    AsyncObjectLog, DEFAULT_ASYNC_OBJECT_LOG_CAPACITY, DEFAULT_ASYNC_OBJECT_LOG_WORKERS,
};
pub use compose_log::{
    ComposedObjectLogBackend, ObjectLog, composed_objectlog_backend,
    composed_objectlog_backend_group_commit,
};
pub use segmented::{
    FaultCutPoint, FaultHook, SegmentConfig, SegmentWriterFormat, SerializedCommandEnvelope,
};

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::fs;
use std::fs::OpenOptions;
use std::hash::{Hash, Hasher};
use std::io::ErrorKind;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;

use bytes::Bytes;
use pqueue_core::{
    BodyHash, ClientItemKey, GroupKey, ItemId, ItemState, LeaseToken, Metadata, PriorityValue,
    QueueDefinition, QueueId, RequestId, TenantId, UtcTimestamp,
};
use pqueue_engine::{
    Backend, ClaimCommand, ClaimCompatibility, ClaimPort, ClaimRequest, Claimed, ClaimedItem,
    CommandChecksum, CommandEnvelope, CommandId, CommandPage, CommandPosition, CompiledSchema,
    ControlPlaneStore, CreateQueueOutcome, DurabilityClass, EngineError, EngineResult,
    FinalizeCommand, FinalizeOutcome, FinalizePort, HistoricalProjectionRead, IdempotencyDecision,
    IndexHit, IndexQueryPort, ItemView, LeaseExpiredCommand, LeaseView, LiveItemView, LogRead,
    PayloadUpdate, PendingPage, PendingSummary, ProjectionRead, ProjectionSnapshot,
    ProjectionStore, PurgeItemsCommand, PurgePort, PushCommand, PushPort, PushSpec, QueueCommand,
    QueueCounters, QueueIdempotencyCache, QueueKey, QueueMetrics, ReassignLeaseCommand,
    ReassignLeasePort, ReclaimDriver, ReclaimPort, RenewLeaseCommand, RenewLeasePort,
    RequestOutcome, SnapshotRef, SnapshotStore, TerminalEmissionMetrics, TickReport,
    UpdateFieldsPort, UpsertOutcome, UpsertPort, build_push_items, compile_entity_schema,
    require_item_level_claim, validate_entity, validate_gate_command, validate_gate_push,
    validate_purge_force,
};
use pqueue_projection::{InMemoryProjection, ProjectionData, ProjectionImage};

fn store<E: std::fmt::Display>(e: E) -> EngineError {
    EngineError::Storage(e.to_string())
}

fn to_json<T: serde::Serialize>(value: &T) -> EngineResult<String> {
    serde_json::to_string(value).map_err(store)
}

fn push_body_hash(items: &[PushSpec]) -> EngineResult<BodyHash> {
    let bytes = serde_json::to_vec(items).map_err(store)?;
    let mut h = std::collections::hash_map::DefaultHasher::new();
    bytes.hash(&mut h);
    Ok(BodyHash(h.finish()))
}

fn request_expires_at(now: UtcTimestamp, retention_ms: u64) -> UtcTimestamp {
    let total = now.seconds as i128 * 1_000_000_000
        + now.nanoseconds as i128
        + retention_ms as i128 * 1_000_000;
    UtcTimestamp::new(
        total.div_euclid(1_000_000_000) as i64,
        total.rem_euclid(1_000_000_000) as u32,
    )
    .expect("valid ts")
}

fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

/// `{root}/{hex(tenant\0queue)}` — a path-safe, collision-free directory per shard.
fn shard_dir(root: &Path, shard: &QueueKey) -> PathBuf {
    let raw = format!(
        "{}\u{0}{}",
        shard.tenant_id.as_str(),
        shard.queue_id.as_str()
    );
    root.join(hex(raw.as_bytes()))
}

/// Read a shard's durable `assignment_epoch` from its `epoch.json` manifest (TD-003 fence authority).
/// Missing file → 0 (a never-acquired queue is at epoch 0, the genesis owner).
fn read_epoch(root: &Path, shard: &QueueKey) -> u64 {
    let path = shard_dir(root, shard).join("epoch.json");
    fs::read(&path)
        .ok()
        .and_then(|b| serde_json::from_slice::<u64>(&b).ok())
        .unwrap_or(0)
}

struct EpochLockGuard {
    path: PathBuf,
}

impl Drop for EpochLockGuard {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

/// Acquire a best-effort local compare-and-swap lock for a shard's epoch manifest.
///
/// This is the local-filesystem analogue of the manifest-CAS fence: one writer creates the lock file,
/// performs the compare+overwrite while holding it, and removes the lock on drop. It serializes local
/// contenders but does not claim S3-level object-store semantics.
fn with_epoch_lock<T>(
    root: &Path,
    shard: &QueueKey,
    f: impl FnOnce() -> EngineResult<T>,
) -> EngineResult<T> {
    let dir = shard_dir(root, shard);
    fs::create_dir_all(&dir).map_err(store)?;
    let lock_path = dir.join("epoch.lock");
    loop {
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&lock_path)
        {
            Ok(_lock) => {
                let guard = EpochLockGuard {
                    path: lock_path.clone(),
                };
                let result = f();
                drop(guard);
                return result;
            }
            Err(err) if err.kind() == ErrorKind::AlreadyExists => {
                thread::yield_now();
            }
            Err(err) => return Err(store(err)),
        }
    }
}

/// Durably advance a shard's `assignment_epoch` to a strictly-greater value (TD-003 acquire). Returns the
/// new epoch.
fn advance_epoch_object(root: &Path, shard: &QueueKey) -> EngineResult<u64> {
    let current = read_epoch(root, shard);
    let next = current + 1;
    write_epoch_object(root, shard, next, current)
}

/// Write a new manifest epoch only if the observed epoch still matches `expected_current`.
///
/// The local-file CAS uses a lock file to serialize concurrent writers, then performs the compare and
/// overwrite while holding that lock.
fn write_epoch_object(
    root: &Path,
    shard: &QueueKey,
    next_epoch: u64,
    expected_current: u64,
) -> EngineResult<u64> {
    with_epoch_lock(root, shard, || {
        let dir = shard_dir(root, shard);
        let current = read_epoch(root, shard);
        if current != expected_current {
            return Err(EngineError::EpochFenced);
        }
        fs::write(dir.join("epoch.json"), to_json(&next_epoch)?).map_err(store)?;
        Ok(next_epoch)
    })
}

/// The next durable sequence for a shard. A committed high-water object wins; otherwise we start from
/// 0 so a crash before the manifest commit can be overwritten instead of replayed as committed work.
fn next_seq(root: &Path, shard: &QueueKey) -> EngineResult<u64> {
    Ok(read_high_water(root, shard)?.map_or(0, |hw| hw.sequence + 1))
}

/// A committed segment: the start sequence plus the batch of envelopes that were durably appended
/// together before the manifest/high-water boundary advanced.
#[derive(serde::Serialize, serde::Deserialize)]
struct SegmentRecord {
    epoch: u64,
    start_seq: u64,
    envelopes: Vec<CommandEnvelope>,
}

/// Durably write a segment + advance the persisted high-water object. Returns the committed positions
/// for every command in the segment. Touches only the filesystem under `root` (not the in-memory
/// projection).
///
/// Enforces the eventual-apply class ban on the atomic XDEL+XADD upsert (Invariant 2) at the SINGLE
/// durable chokepoint both write paths funnel through (`commit_locked` and the typed raw commit): a
/// `ReplacePending` command is refused with `Unavailable` BEFORE any object is
/// written, so the ban holds at the write path, not just the `replace_if_pending` port.
fn append_segment(
    root: &Path,
    shard: &QueueKey,
    commands: &[CommandEnvelope],
    expected_epoch: u64,
) -> EngineResult<Vec<CommandPosition>> {
    if commands
        .iter()
        .any(|env| matches!(env.command, QueueCommand::ReplacePending(_)))
    {
        return Err(EngineError::Unavailable);
    }
    with_epoch_lock(root, shard, || {
        let dir = shard_dir(root, shard);
        let log_dir = dir.join("log");
        fs::create_dir_all(&log_dir).map_err(store)?;
        let epoch = read_epoch(root, shard);
        if epoch != expected_epoch {
            return Err(EngineError::EpochFenced);
        }
        let start_seq = next_seq(root, shard)?;
        let end_seq = start_seq + commands.len().saturating_sub(1) as u64;
        // Object name: zero-padded so lexical order == segment order.
        fs::write(
            log_dir.join(format!("{start_seq:020}.json")),
            to_json(&SegmentRecord {
                epoch,
                start_seq,
                envelopes: commands.to_vec(),
            })?,
        )
        .map_err(store)?;
        fs::write(
            dir.join("high_water.json"),
            to_json(&HighWater {
                epoch,
                seq: end_seq,
            })?,
        )
        .map_err(store)?;
        Ok(commands
            .iter()
            .enumerate()
            .map(|(i, _)| CommandPosition::new(shard.clone(), epoch, start_seq + i as u64))
            .collect())
    })
}

/// The high-water object payload (a stored field, not recomputed from a possibly-compacted log).
#[derive(serde::Serialize, serde::Deserialize)]
struct HighWater {
    epoch: u64,
    seq: u64,
}

fn read_high_water(root: &Path, shard: &QueueKey) -> EngineResult<Option<CommandPosition>> {
    let path = shard_dir(root, shard).join("high_water.json");
    if !path.exists() {
        return Ok(None);
    }
    let hw: HighWater =
        serde_json::from_str(&fs::read_to_string(&path).map_err(store)?).map_err(store)?;
    Ok(Some(CommandPosition::new(shard.clone(), hw.epoch, hw.seq)))
}

/// A stored snapshot object: its position + opaque payload.
#[derive(serde::Serialize, serde::Deserialize)]
struct SnapshotObject {
    epoch: u64,
    seq: u64,
    payload: Vec<u8>,
}

// Transient, short-lived stack value used only during recovery deserialization; the size
// difference between the variants is irrelevant here (never stored in bulk), so the
// large_enum_variant lint is silenced rather than boxing (which would force a deref on
// both match arms for no runtime benefit).
#[derive(serde::Deserialize)]
#[serde(untagged)]
#[allow(clippy::large_enum_variant)]
enum StoredObject {
    Versioned(SegmentRecord),
    Legacy(CommandEnvelope),
}

struct Inner {
    root: PathBuf,
    projections: HashMap<QueueKey, ProjectionData>,
    queues: HashMap<QueueKey, QueueDefinition>,
    schemas: HashMap<QueueKey, Arc<CompiledSchema>>,
    idempotency: HashMap<QueueKey, QueueIdempotencyCache<Vec<ItemId>>>,
    cmd_seq: u64,
    segment_config: ObjectLogSegmentConfig,
}

/// Segment sizing controls for the object-log reference backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObjectLogSegmentConfig {
    pub segment_max_commands: usize,
    pub segment_max_bytes: usize,
    pub segment_max_latency_ms: u64,
}

impl Default for ObjectLogSegmentConfig {
    fn default() -> Self {
        Self {
            segment_max_commands: 1,
            segment_max_bytes: 0,
            segment_max_latency_ms: 0,
        }
    }
}

/// A point-in-time view of object-log segmenting for a shard.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObjectLogStats {
    pub segment_objects: u64,
    pub command_objects: u64,
}

fn segment_batches(
    commands: &[CommandEnvelope],
    config: ObjectLogSegmentConfig,
) -> Vec<&[CommandEnvelope]> {
    let max_commands = config.segment_max_commands.max(1);
    let max_bytes = config.segment_max_bytes;
    let mut batches = Vec::new();
    let mut start = 0usize;
    while start < commands.len() {
        let mut end = start;
        let mut used_bytes = 0usize;
        while end < commands.len() && end - start < max_commands {
            let encoded = serde_json::to_vec(&commands[end]).expect("command envelope serializes");
            let next = encoded.len();
            if end > start && max_bytes != 0 && used_bytes + next > max_bytes {
                break;
            }
            used_bytes += next;
            end += 1;
            if max_bytes != 0 && used_bytes >= max_bytes {
                break;
            }
        }
        if end == start {
            end = start + 1;
        }
        batches.push(&commands[start..end]);
        start = end;
    }
    batches
}

/// Local filesystem object-log authority without an in-process projection.
///
/// This is the log-only half used by the object_log_sqlite_projection runtime: callers append already
/// validated envelopes, then feed the returned positions into a separate projection store.
pub struct LocalObjectLog {
    inner: Mutex<LogOnlyInner>,
}

struct LogOnlyInner {
    root: PathBuf,
    queues: HashMap<QueueKey, QueueDefinition>,
    segment_config: ObjectLogSegmentConfig,
}

impl Inner {
    fn shard_dir(&self, shard: &QueueKey) -> PathBuf {
        shard_dir(&self.root, shard)
    }

    fn make_envelope(
        &mut self,
        command: QueueCommand,
        item_ids: Vec<ItemId>,
        created_at: UtcTimestamp,
    ) -> CommandEnvelope {
        let n = self.cmd_seq;
        self.cmd_seq += 1;
        CommandEnvelope {
            command_id: CommandId::new(format!("obj-{n}")),
            request_id: None,
            request_fingerprint: None,
            request_outcome: None,
            item_ids,
            command,
            checksum: CommandChecksum(0),
            created_at,
        }
    }

    /// Durable append + infallible in-memory apply (the orchestration unit). Caller MUST pre-validate.
    fn commit_locked(&mut self, shard: &QueueKey, env: CommandEnvelope) -> EngineResult<()> {
        let expected_epoch = read_epoch(&self.root, shard);
        append_segment(
            &self.root,
            shard,
            std::slice::from_ref(&env),
            expected_epoch,
        )?;
        self.projections
            .get_mut(shard)
            .expect("projection exists for a shard that just accepted a durable commit")
            .apply_command(&env.command)
            .expect(
                "post-commit apply must be infallible after a durable object write (caller \
                 pre-validates); a failure means the durable log advanced past the projection",
            );
        Ok(())
    }

    /// All log envelopes for a shard in sequence order (replay order). When a committed high-water exists
    /// we replay only the segment objects at or before that boundary; a torn trailing segment beyond the
    /// manifest is ignored. Legacy one-command object files remain readable when the shard has no
    /// high-water manifest yet.
    fn read_envelopes(&self, shard: &QueueKey) -> EngineResult<Vec<(u64, u64, CommandEnvelope)>> {
        read_envelopes_from_root(&self.root, shard)
    }

    fn read_high_water(&self, shard: &QueueKey) -> EngineResult<Option<CommandPosition>> {
        read_high_water(&self.root, shard)
    }

    /// Install a queue discovered through durable create authority into this handle's derived state.
    ///
    /// Object-log handles are sole-owner data-plane handles. The supported handoff is therefore ordered:
    /// another handle may have durably committed commands before this independently opened handle resolves
    /// `create_queue`, but it must not keep mutating the shard concurrently. Replaying here makes the losing
    /// creator authoritative at the instant create returns instead of installing an empty projection.
    fn hydrate_queue(
        &mut self,
        definition: &QueueDefinition,
        counters: &QueueCounters,
    ) -> EngineResult<()> {
        let shard = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
        if self.projections.contains_key(&shard) {
            return Ok(());
        }
        let mut projection = ProjectionData::new(
            definition.priority_model,
            definition.ordering_mode,
            definition.max_rank_error,
            definition.recurrence,
            &definition.secondary_indexes,
        )
        .with_typed_indexes(&definition.typed_indexes);
        for (_sequence, _epoch, envelope) in self.read_envelopes(&shard)? {
            for item_id in &envelope.item_ids {
                counters.observe(&shard, *item_id);
            }
            if let Some(sequence) = envelope
                .command_id
                .0
                .rsplit('-')
                .next()
                .and_then(|value| value.parse::<u64>().ok())
            {
                self.cmd_seq = self.cmd_seq.max(sequence.saturating_add(1));
            }
            projection.apply_command(&envelope.command)?;
        }
        if let Some(schema) = definition
            .entity_schema
            .as_ref()
            .and_then(|document| document.entity_schema.as_ref())
            .map(compile_entity_schema)
            .transpose()?
        {
            self.schemas.insert(shard.clone(), schema);
        }
        self.projections.insert(shard, projection);
        Ok(())
    }

    /// Reconstruct every queue's projection from the durable object log on open (TD-007 §4 replay), and
    /// restore `cmd_seq` past the highest minted `obj-N` so a post-restart id never collides.
    fn rebuild_all(&mut self, counters: &QueueCounters) -> EngineResult<()> {
        if !self.root.exists() {
            fs::create_dir_all(&self.root).map_err(store)?;
            return Ok(());
        }
        let mut max_cmd_seq: Option<u64> = None;
        for entry in fs::read_dir(&self.root).map_err(store)? {
            let dir = entry.map_err(store)?.path();
            let queue_file = dir.join("queue.json");
            if !queue_file.exists() {
                continue;
            }
            let definition: QueueDefinition =
                serde_json::from_str(&fs::read_to_string(&queue_file).map_err(store)?)
                    .map_err(store)?;
            let key = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
            let shard = key.clone();
            let mut proj = ProjectionData::new(
                definition.priority_model,
                definition.ordering_mode,
                definition.max_rank_error,
                definition.recurrence,
                &definition.secondary_indexes,
            )
            .with_typed_indexes(&definition.typed_indexes);
            for (_seq, _epoch, env) in self.read_envelopes(&shard)? {
                // Command-id is `obj-{node}-{n}` (or legacy `obj-{n}`); the trailing component is the seq.
                if let Some(n) = env
                    .command_id
                    .0
                    .rsplit('-')
                    .next()
                    .and_then(|s| s.parse::<u64>().ok())
                {
                    max_cmd_seq = Some(max_cmd_seq.map_or(n, |m| m.max(n)));
                }
                // Restart-safety: resume the per-queue item counter past every id already in the log so a
                // push after reopen never re-mints an existing id (ADR-009 / `QueueCounters::observe`).
                for id in &env.item_ids {
                    counters.observe(&shard, *id);
                }
                proj.apply_command(&env.command)
                    .expect("durable log replays into a consistent projection");
            }
            if let Some(cs) = definition
                .entity_schema
                .as_ref()
                .and_then(|esd| esd.entity_schema.as_ref())
                .map(compile_entity_schema)
                .transpose()?
            {
                self.schemas.insert(shard.clone(), cs);
            }
            self.projections.insert(shard, proj);
            self.queues.insert(key, definition);
        }
        if let Some(m) = max_cmd_seq {
            self.cmd_seq = m + 1;
        }
        Ok(())
    }
}

fn read_queue_definitions(root: &Path) -> EngineResult<HashMap<QueueKey, QueueDefinition>> {
    let mut queues = HashMap::new();
    if !root.exists() {
        fs::create_dir_all(root).map_err(store)?;
        return Ok(queues);
    }
    for entry in fs::read_dir(root).map_err(store)? {
        let dir = entry.map_err(store)?.path();
        let queue_file = dir.join("queue.json");
        if !queue_file.exists() {
            continue;
        }
        let definition: QueueDefinition =
            serde_json::from_str(&fs::read_to_string(&queue_file).map_err(store)?)
                .map_err(store)?;
        let key = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
        queues.insert(key, definition);
    }
    Ok(queues)
}

fn create_queue_metadata(
    root: &Path,
    queues: &mut HashMap<QueueKey, QueueDefinition>,
    definition: QueueDefinition,
) -> EngineResult<CreateQueueOutcome> {
    let key = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
    if let Some(existing) = queues.get(&key) {
        if existing != &definition {
            return Err(EngineError::QueueDefinitionConflict);
        }
        return Ok(CreateQueueOutcome {
            created: false,
            definition: existing.clone(),
        });
    }
    let dir = shard_dir(root, &key);
    fs::create_dir_all(&dir).map_err(store)?;
    let queue_file = dir.join("queue.json");
    let bytes = to_json(&definition)?;
    let temp_file = dir.join(format!(
        "queue.json.tmp.{}.{:?}",
        std::process::id(),
        thread::current().id()
    ));
    {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp_file)
            .map_err(store)?;
        file.write_all(bytes.as_bytes()).map_err(store)?;
        file.sync_all().map_err(store)?;
    }
    let created = match fs::hard_link(&temp_file, &queue_file) {
        Ok(()) => true,
        Err(err) if err.kind() == ErrorKind::AlreadyExists => false,
        Err(err) => {
            let _ = fs::remove_file(&temp_file);
            return Err(store(err));
        }
    };
    let _ = fs::remove_file(&temp_file);
    let stored: QueueDefinition =
        serde_json::from_str(&fs::read_to_string(&queue_file).map_err(store)?).map_err(store)?;
    queues.insert(key, stored.clone());
    if stored != definition {
        // Cache the durable winner before reporting conflict so an independently opened loser can perform
        // the documented follow-up definition read without reopening the backend.
        return Err(EngineError::QueueDefinitionConflict);
    }
    Ok(CreateQueueOutcome {
        created,
        definition: stored,
    })
}

fn read_envelopes_from_root(
    root: &Path,
    shard: &QueueKey,
) -> EngineResult<Vec<(u64, u64, CommandEnvelope)>> {
    let log_dir = shard_dir(root, shard).join("log");
    if !log_dir.exists() {
        return Ok(Vec::new());
    }
    let high_water = read_high_water(root, shard)?;
    let fallback_epoch = read_epoch(root, shard);
    // Collect (seq, path) first, sorted, so "final object" is well-defined before we parse.
    let mut files: Vec<(u64, PathBuf)> = Vec::new();
    for entry in fs::read_dir(&log_dir).map_err(store)? {
        let path = entry.map_err(store)?.path();
        if let Some(seq) = path
            .file_stem()
            .and_then(|s| s.to_str())
            .and_then(|s| s.parse::<u64>().ok())
        {
            files.push((seq, path));
        }
    }
    files.sort_by_key(|(seq, _)| *seq);
    let last = files.len().saturating_sub(1);
    let mut rows: Vec<(u64, u64, CommandEnvelope)> = Vec::with_capacity(files.len());
    for (i, (seq, path)) in files.iter().enumerate() {
        let json = fs::read_to_string(path).map_err(store)?;
        match serde_json::from_str::<StoredObject>(&json) {
            Ok(StoredObject::Versioned(record)) => {
                if let Some(hw) = &high_water {
                    if record.start_seq > hw.sequence {
                        continue;
                    }
                    for (offset, env) in record.envelopes.into_iter().enumerate() {
                        let seq = record.start_seq + offset as u64;
                        if seq <= hw.sequence {
                            rows.push((seq, record.epoch, env));
                        }
                    }
                }
            }
            Ok(StoredObject::Legacy(env)) if high_water.is_none() && fallback_epoch == 0 => {
                rows.push((*seq, 0, env))
            }
            Ok(StoredObject::Legacy(_)) => {
                return Err(EngineError::Invalid(
                    "legacy object format is ambiguous once the manifest epoch has advanced",
                ));
            }
            // Torn trailing object -> uncommitted, skip. Earlier object -> real corruption, fail.
            Err(_) if i == last => continue,
            Err(e) => return Err(store(e)),
        }
    }
    Ok(rows)
}

impl LocalObjectLog {
    /// Open (or create) a local filesystem object log rooted at `root`.
    pub fn open(root: impl Into<PathBuf>) -> EngineResult<Self> {
        Self::open_with_config(root, ObjectLogSegmentConfig::default())
    }

    /// Open (or create) a local filesystem object log rooted at `root` with explicit segment settings.
    pub fn open_with_config(
        root: impl Into<PathBuf>,
        segment_config: ObjectLogSegmentConfig,
    ) -> EngineResult<Self> {
        let root = root.into();
        let queues = read_queue_definitions(&root)?;
        Ok(Self {
            inner: Mutex::new(LogOnlyInner {
                root,
                queues,
                segment_config,
            }),
        })
    }

    pub fn create_queue(&self, definition: QueueDefinition) -> EngineResult<CreateQueueOutcome> {
        let mut inner = self.inner.lock().expect("object log store poisoned");
        let root = inner.root.clone();
        create_queue_metadata(&root, &mut inner.queues, definition)
    }

    pub fn current_epoch(&self, shard: &QueueKey) -> EngineResult<u64> {
        let inner = self.inner.lock().expect("object log store poisoned");
        if inner.queues.contains_key(shard) {
            Ok(read_epoch(&inner.root, shard))
        } else {
            Err(EngineError::NotFound)
        }
    }

    pub fn acquire_epoch(&self, shard: &QueueKey) -> EngineResult<u64> {
        let inner = self.inner.lock().expect("object log store poisoned");
        if inner.queues.contains_key(shard) {
            advance_epoch_object(&inner.root, shard)
        } else {
            Err(EngineError::NotFound)
        }
    }

    pub fn append(
        &self,
        shard: &QueueKey,
        commands: &[CommandEnvelope],
        expected_epoch: u64,
    ) -> EngineResult<Vec<CommandPosition>> {
        let inner = self.inner.lock().expect("object log store poisoned");
        if !inner.queues.contains_key(shard) {
            return Err(EngineError::NotFound);
        }
        if commands
            .iter()
            .any(|env| matches!(env.command, QueueCommand::ReplacePending(_)))
        {
            return Err(EngineError::Unavailable);
        }
        for env in commands {
            validate_gate_command(false, &env.command)?;
        }
        let mut positions = Vec::with_capacity(commands.len());
        for chunk in segment_batches(commands, inner.segment_config) {
            positions.extend(append_segment(&inner.root, shard, chunk, expected_epoch)?);
        }
        Ok(positions)
    }

    pub fn segment_stats(&self, shard: &QueueKey) -> EngineResult<ObjectLogStats> {
        let inner = self.inner.lock().expect("object log store poisoned");
        if !inner.queues.contains_key(shard) {
            return Err(EngineError::NotFound);
        }
        let log_dir = shard_dir(&inner.root, shard).join("log");
        let segment_objects = if log_dir.exists() {
            fs::read_dir(&log_dir).map_err(store)?.count() as u64
        } else {
            0
        };
        let command_objects = read_envelopes_from_root(&inner.root, shard)?.len() as u64;
        Ok(ObjectLogStats {
            segment_objects,
            command_objects,
        })
    }
}

/// Object-log backed, eventual-apply-class backend (filesystem object store).
pub struct ObjectLogBackend {
    inner: Mutex<Inner>,
    /// This instance's node id, packed into every minted [`ItemId`] (ADR-009). `0` single-instance.
    node_id: u8,
    /// Per-(queue, epoch) item-id sequence — see `QueueCounters`.
    counters: QueueCounters,
}

impl ObjectLogBackend {
    /// Open (or create) an object log rooted at `root`, rebuilding every queue's projection from its
    /// durable objects.
    pub fn open(root: impl Into<PathBuf>) -> EngineResult<Self> {
        Self::open_with_config(root, ObjectLogSegmentConfig::default())
    }

    /// Open (or create) an object log rooted at `root` with explicit segment settings.
    pub fn open_with_config(
        root: impl Into<PathBuf>,
        segment_config: ObjectLogSegmentConfig,
    ) -> EngineResult<Self> {
        let mut inner = Inner {
            root: root.into(),
            projections: HashMap::new(),
            queues: HashMap::new(),
            schemas: HashMap::new(),
            idempotency: HashMap::new(),
            cmd_seq: 0,
            segment_config,
        };
        let counters = QueueCounters::default();
        inner.rebuild_all(&counters)?;
        Ok(Self {
            inner: Mutex::new(inner),
            node_id: 0,
            counters,
        })
    }

    /// Tag this backend with `node_id` — packed into the disambiguation byte of every minted [`ItemId`]
    /// so distinct nodes competing for one queue never mint a colliding id (ADR-009).
    pub fn with_node_id(mut self, node_id: u8) -> Self {
        self.node_id = node_id;
        self
    }

    pub fn segment_stats(&self, shard: &QueueKey) -> EngineResult<ObjectLogStats> {
        let inner = self.inner.lock().expect("objectlog poisoned");
        if !inner.queues.contains_key(shard) {
            return Err(EngineError::NotFound);
        }
        let log_dir = shard_dir(&inner.root, shard).join("log");
        let segment_objects = if log_dir.exists() {
            fs::read_dir(&log_dir).map_err(store)?.count() as u64
        } else {
            0
        };
        let command_objects = read_envelopes_from_root(&inner.root, shard)?.len() as u64;
        Ok(ObjectLogStats {
            segment_objects,
            command_objects,
        })
    }
}

// ---------------------------------------------------------------------------
// Typed raw commit helpers
// ---------------------------------------------------------------------------

struct ObjLogTxn {
    root: PathBuf,
    segment_config: ObjectLogSegmentConfig,
}

impl ObjLogTxn {
    fn append(
        &mut self,
        shard: &QueueKey,
        commands: &[CommandEnvelope],
        expected_epoch: u64,
    ) -> EngineResult<Vec<CommandPosition>> {
        for env in commands {
            validate_gate_command(false, &env.command)?;
        }
        let mut positions = Vec::with_capacity(commands.len());
        for chunk in segment_batches(commands, self.segment_config) {
            positions.extend(append_segment(&self.root, shard, chunk, expected_epoch)?);
        }
        Ok(positions)
    }
}

struct ObjProjectionTxn<'a> {
    projections: &'a mut HashMap<QueueKey, ProjectionData>,
}

impl ObjProjectionTxn<'_> {
    fn apply(
        &mut self,
        positions: &[CommandPosition],
        commands: &[CommandEnvelope],
    ) -> EngineResult<()> {
        for (pos, cmd) in positions.iter().zip(commands) {
            self.projections
                .get_mut(&pos.queue)
                .ok_or(EngineError::NotFound)?
                .apply_command(&cmd.command)?;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Ports
// ---------------------------------------------------------------------------

impl Backend for ObjectLogBackend {
    fn durability_class(&self) -> DurabilityClass {
        DurabilityClass::EventualApply
    }

    fn commit_raw(
        &self,
        request: pqueue_engine::RawCommitRequest,
    ) -> impl std::future::Future<Output = EngineResult<pqueue_engine::RawCommitOutcome>> + Send
    {
        // The log-writer needs only `root` (writes objects to the filesystem); the projection-writer
        // needs only the `projections` map. Disjoint, so the borrow checker is satisfied by destructuring
        // — the log side gets a cheap `PathBuf` clone, the projection side a `&mut` to its map.
        let result = (|| {
            let (shard, commands, expected_epoch, fault) = request.into_parts();
            if fault == pqueue_engine::RawCommitFault::BeforeAppend {
                return Err(EngineError::Invalid("fault-injection: kill before append"));
            }
            let mut guard = self.inner.lock().expect("objectlog poisoned");
            let Inner {
                root,
                projections,
                segment_config,
                ..
            } = &mut *guard;
            let positions = ObjLogTxn {
                root: root.clone(),
                segment_config: *segment_config,
            }
            .append(&shard, &commands, expected_epoch)?;
            if fault == pqueue_engine::RawCommitFault::AfterAppendBeforeApply {
                return Ok(pqueue_engine::RawCommitOutcome::appended(positions));
            }
            ObjProjectionTxn { projections }.apply(&positions, &commands)?;
            Ok(pqueue_engine::RawCommitOutcome::applied(positions))
        })();
        std::future::ready(result)
    }
}

impl ClaimPort for ObjectLogBackend {
    fn claim(
        &self,
        req: ClaimRequest,
    ) -> impl std::future::Future<Output = EngineResult<Claimed>> + Send {
        let result = (|| {
            let mut g = self.inner.lock().expect("poisoned");
            // BQ-14a: gate non-item compatibility (selection lands in BQ-14b/c); item-level path unchanged.
            if req.compatibility != ClaimCompatibility::default() {
                let def = g.queues.get(&req.shard).ok_or(EngineError::NotFound)?;
                require_item_level_claim(&req.compatibility, req.max_items as u64, def)?;
            }
            let candidates: Vec<ItemId> = {
                let proj = g.projections.get(&req.shard).ok_or(EngineError::NotFound)?;
                proj.eligible_candidates(req.eligibility_at(), req.max_items)
            };
            if candidates.is_empty() {
                return Ok(Claimed::default());
            }
            let cmd = QueueCommand::Claim(ClaimCommand {
                item_ids: candidates.clone(),
                lease_token: req.lease_token.clone(),
                lease_expires_at: req.lease_expires_at,
                worker_id: Some(req.worker_id.clone()),
            });
            let env = g.make_envelope(cmd, candidates.clone(), req.now);
            g.commit_locked(&req.shard, env)?;
            let proj = g.projections.get(&req.shard).ok_or(EngineError::NotFound)?;
            Ok(Claimed {
                items: proj.render_claimed(&candidates),
                ..Default::default()
            })
        })();
        std::future::ready(result)
    }
}

impl UpsertPort for ObjectLogBackend {
    #[allow(clippy::too_many_arguments)]
    fn replace_if_pending(
        &self,
        _shard: &QueueKey,
        _client_item_key: &ClientItemKey,
        _priority: Option<PriorityValue>,
        _group_key: Option<GroupKey>,
        _not_before: Option<UtcTimestamp>,
        _payload: Option<Bytes>,
        _fields: BTreeMap<String, Bytes>,
        _metadata: Metadata,
        _entity: Option<serde_json::Value>,
        _now: UtcTimestamp,
        _expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<UpsertOutcome>> + Send {
        // Invariant 2 / TD-007 §2.3: the atomic XDEL+XADD upsert is not offered on the eventual-apply
        // class. Refuse with the structured `Unavailable` (`-ERR pqueue unavailable`).
        std::future::ready(Err(EngineError::Unavailable))
    }
}

impl UpdateFieldsPort for ObjectLogBackend {
    #[allow(clippy::too_many_arguments)]
    fn update_fields(
        &self,
        _shard: &QueueKey,
        _item_id: ItemId,
        _field_ops: BTreeMap<String, Option<Bytes>>,
        _payload: PayloadUpdate,
        _entity: Option<serde_json::Value>,
        _expected_item_version: Option<u64>,
        _now: UtcTimestamp,
        _expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<u64>> + Send {
        // FAC-1: in-place field/payload merge is a read-your-write mutation that returns the new
        // `item_version` from a state this class cannot serve (the durable boundary is the object write;
        // the projection is a derived, possibly-late view). Like `replace_if_pending`, refuse with the
        // structured `Unavailable` (`-ERR pqueue unavailable`) BEFORE committing anything.
        std::future::ready(Err(EngineError::Unavailable))
    }
}

impl PushPort for ObjectLogBackend {
    fn push(
        &self,
        shard: &QueueKey,
        items: Vec<PushSpec>,
        now: UtcTimestamp,
        // Fence threading for this backend family is deferred (B1b continuation); accepted for the port
        // contract so the owner fence is uniform once the relational/object write paths thread it.
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemId>>> + Send {
        let result = (|| {
            validate_gate_push(self.supports_gates(), &items)?;
            let mut g = self.inner.lock().expect("poisoned");
            // Pre-validate the shard exists before any durable object write (commit_locked expects it).
            if !g.projections.contains_key(shard) {
                return Err(EngineError::NotFound);
            }
            {
                let schema = g.schemas.get(shard);
                for item in &items {
                    validate_entity(schema, item.entity.as_ref())?;
                }
            }
            let max_attempts = g
                .queues
                .get(&shard.clone())
                .map(|d| d.retry_policy.max_attempts)
                .unwrap_or(1);
            let n = g.cmd_seq;
            g.cmd_seq += 1;
            let epoch = expected_epoch.unwrap_or(0);
            let counter_base = self.counters.reserve(shard, epoch, items.len() as u32);
            let (push_items, ids) =
                build_push_items(items, epoch, self.node_id, counter_base, max_attempts);
            let env = CommandEnvelope {
                command_id: CommandId::new(format!("obj-{}-{n}", self.node_id)),
                request_id: None,
                request_fingerprint: None,
                request_outcome: None,
                item_ids: ids.clone(),
                command: QueueCommand::Push(PushCommand { items: push_items }),
                checksum: CommandChecksum(0),
                created_at: now,
            };
            g.commit_locked(shard, env)?;
            Ok(ids)
        })();
        std::future::ready(result)
    }

    fn push_with_request_id(
        &self,
        shard: &QueueKey,
        request_id: RequestId,
        items: Vec<PushSpec>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemId>>> + Send {
        let result = (|| {
            validate_gate_push(self.supports_gates(), &items)?;
            let mut g = self.inner.lock().expect("poisoned");
            if !g.projections.contains_key(shard) {
                return Err(EngineError::NotFound);
            }
            {
                let schema = g.schemas.get(shard);
                for item in &items {
                    validate_entity(schema, item.entity.as_ref())?;
                }
            }
            let fingerprint = push_body_hash(&items)?;
            let max_attempts = g
                .queues
                .get(&shard.clone())
                .map(|d| d.retry_policy.max_attempts)
                .unwrap_or(1);
            let retention_ms = g
                .queues
                .get(&shard.clone())
                .map(|d| d.request_id_retention_ms)
                .unwrap_or(60_000);
            let expires_at = request_expires_at(now, retention_ms);
            match g.idempotency.entry(shard.clone()).or_default().check(
                &request_id,
                fingerprint,
                now,
            ) {
                IdempotencyDecision::Replay(ids) => return Ok(ids),
                IdempotencyDecision::Conflict => return Err(EngineError::RequestIdConflict),
                IdempotencyDecision::Proceed | IdempotencyDecision::Expired => {}
            }
            let n = g.cmd_seq;
            g.cmd_seq += 1;
            let epoch = expected_epoch.unwrap_or(0);
            let counter_base = self.counters.reserve(shard, epoch, items.len() as u32);
            let (push_items, ids) =
                build_push_items(items, epoch, self.node_id, counter_base, max_attempts);
            let env = CommandEnvelope {
                command_id: CommandId::new(format!("obj-{}-{n}", self.node_id)),
                request_id: Some(request_id.clone()),
                request_fingerprint: Some(fingerprint.0),
                request_outcome: Some(RequestOutcome::Push {
                    item_ids: ids.clone(),
                }),
                item_ids: ids.clone(),
                command: QueueCommand::Push(PushCommand { items: push_items }),
                checksum: CommandChecksum(0),
                created_at: now,
            };
            g.commit_locked(shard, env)?;
            g.idempotency.entry(shard.clone()).or_default().record(
                request_id,
                fingerprint,
                ids.clone(),
                expires_at,
            );
            Ok(ids)
        })();
        std::future::ready(result)
    }
}

/// Snorri authoritative vectorized claimed-work commit (epic pqueue-2201fd37). The object-log backend is
/// eventual-apply (no single atomic transition boundary), so it inherits the default impl, which returns
/// [`pqueue_engine::EngineError::Unavailable`] — a consumer must reject it before activation.
impl pqueue_engine::CommitTransitionPort for ObjectLogBackend {}

// Gates are a relational-mode feature; the eventual-apply object-log family rejects SetGates with the
// default `Unavailable` (consistent with `validate_gate_command`).
impl pqueue_engine::SetGatesPort for ObjectLogBackend {}

// Priority/not_before reschedule is an atomic-class capability; the eventual-apply object-log family
// refuses it with the default `Unavailable`.
impl pqueue_engine::ReschedulePort for ObjectLogBackend {}

// Active-scope discovery is a relational-class feature (per-group summary); the object-log family refuses it.
impl pqueue_engine::DiscoveryPort for ObjectLogBackend {}

/// Recovery/explain reads are unavailable: this eventual-apply backend has no authoritative commit boundary
/// (it inherits the `Unavailable` default).
impl pqueue_engine::RecoveryReadPort for ObjectLogBackend {}

/// Hot projection query substrate (API-004) is not implemented for any backend in epic pqueue-45e13e4d;
/// the object-log family inherits the all-`Unavailable` default.
impl pqueue_engine::HotProjectionQueryPort for ObjectLogBackend {}

impl FinalizePort for ObjectLogBackend {
    fn finalize(
        &self,
        shard: &QueueKey,
        outcomes: Vec<FinalizeOutcome>,
        now: UtcTimestamp,
        _expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        let result = (|| {
            let mut g = self.inner.lock().expect("poisoned");
            {
                let proj = g.projections.get(shard).ok_or(EngineError::NotFound)?;
                proj.finalize_validate(&outcomes)?;
            }
            let item_ids: Vec<ItemId> = outcomes.iter().map(|o| o.item_id).collect();
            let cmd = QueueCommand::Finalize(FinalizeCommand { outcomes });
            let env = g.make_envelope(cmd, item_ids, now);
            g.commit_locked(shard, env)?;
            Ok(())
        })();
        std::future::ready(result)
    }
}

impl RenewLeasePort for ObjectLogBackend {
    fn renew(
        &self,
        shard: &QueueKey,
        item_ids: Vec<ItemId>,
        new_lease_expires_at: UtcTimestamp,
        now: UtcTimestamp,
        _expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        let result = (|| {
            let mut g = self.inner.lock().expect("poisoned");
            {
                let proj = g.projections.get(shard).ok_or(EngineError::NotFound)?;
                proj.renew_validate(&item_ids)?;
            }
            let cmd = QueueCommand::RenewLease(RenewLeaseCommand {
                item_ids: item_ids.clone(),
                lease_expires_at: new_lease_expires_at,
            });
            let env = g.make_envelope(cmd, item_ids, now);
            g.commit_locked(shard, env)?;
            Ok(())
        })();
        std::future::ready(result)
    }
}

impl ReassignLeasePort for ObjectLogBackend {
    fn reassign(
        &self,
        shard: &QueueKey,
        item_ids: Vec<ItemId>,
        new_lease_token: LeaseToken,
        new_lease_expires_at: UtcTimestamp,
        now: UtcTimestamp,
        _expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        let result = (|| {
            let mut g = self.inner.lock().expect("poisoned");
            {
                let proj = g.projections.get(shard).ok_or(EngineError::NotFound)?;
                proj.reassign_validate(&item_ids)?;
            }
            let cmd = QueueCommand::ReassignLease(ReassignLeaseCommand {
                item_ids: item_ids.clone(),
                lease_token: new_lease_token,
                lease_expires_at: new_lease_expires_at,
            });
            let env = g.make_envelope(cmd, item_ids, now);
            g.commit_locked(shard, env)?;
            Ok(())
        })();
        std::future::ready(result)
    }
}

impl PurgePort for ObjectLogBackend {
    fn purge(
        &self,
        shard: &QueueKey,
        item_ids: Vec<ItemId>,
        force: bool,
        now: UtcTimestamp,
        _expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<u64>> + Send {
        let result = (|| {
            let mut g = self.inner.lock().expect("poisoned");
            let present: Vec<ItemId> = {
                let proj = g.projections.get(shard).ok_or(EngineError::NotFound)?;
                let mut present = Vec::new();
                for id in &item_ids {
                    // De-dup: a repeated id removes once and counts once (Redis XDEL semantics; the
                    // apply arm's second `remove` would be a no-op but `present.len()` would over-count).
                    if present.contains(id) {
                        continue;
                    }
                    if let Some(state) = proj.item_state(id) {
                        validate_purge_force(state == ItemState::Leased, force)?;
                        present.push(*id);
                    }
                }
                present
            };
            if present.is_empty() {
                return Ok(0);
            }
            let count = present.len() as u64;
            let cmd = QueueCommand::PurgeItems(PurgeItemsCommand {
                item_ids: present.clone(),
                force,
            });
            let env = g.make_envelope(cmd, present, now);
            g.commit_locked(shard, env)?;
            Ok(count)
        })();
        std::future::ready(result)
    }
}

impl ReclaimDriver for ObjectLogBackend {
    fn tick(
        &self,
        now: UtcTimestamp,
    ) -> impl std::future::Future<Output = EngineResult<TickReport>> + Send {
        let result = (|| {
            let mut g = self.inner.lock().expect("poisoned");
            let expired: Vec<(QueueKey, Vec<ItemId>)> = g
                .projections
                .iter()
                .filter_map(|(shard, proj)| {
                    let ids = proj.expired_leases(now);
                    (!ids.is_empty()).then(|| (shard.clone(), ids))
                })
                .collect();
            let mut report = TickReport::default();
            for (shard, ids) in expired {
                let cmd = QueueCommand::LeaseExpired(LeaseExpiredCommand {
                    item_ids: ids.clone(),
                });
                let env = g.make_envelope(cmd, ids.clone(), now);
                g.commit_locked(&shard, env)?;
                report.leases_reclaimed += ids.len() as u64;
            }
            Ok(report)
        })();
        std::future::ready(result)
    }
}

impl ReclaimPort for ObjectLogBackend {
    fn reclaim_expired(
        &self,
        shard: &QueueKey,
        limit: Option<usize>,
        now: UtcTimestamp,
        expected_epoch: Option<u64>,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemId>>> + Send {
        let result = (|| {
            let mut g = self.inner.lock().expect("poisoned");
            let mut ids = {
                let proj = g.projections.get(shard).ok_or(EngineError::NotFound)?;
                proj.expired_leases(now)
            };
            if let Some(limit) = limit {
                ids.truncate(limit);
            }
            if ids.is_empty() {
                return Ok(Vec::new());
            }
            // Per-queue and FENCED (unlike the global `ReclaimDriver::tick`, which passes the degenerate
            // None path). Objectlog's `commit_locked`/`append_segment` stamp the queue's current durable
            // epoch but do NOT validate an `expected_epoch` (the TD-003 reject lives at the
            // typed raw-commit seam, not in the data-plane fast path). So replicate that seam's fence
            // rule inline BEFORE the durable object write: `Some(e)` that is not the current durable epoch
            // is a superseded owner → reject `EpochFenced`, nothing appended; `None` is the degenerate
            // sole-owner path (stamp current, never fence).
            if let Some(expected) = expected_epoch
                && expected != read_epoch(&g.root, shard)
            {
                return Err(EngineError::EpochFenced);
            }
            let cmd = QueueCommand::LeaseExpired(LeaseExpiredCommand {
                item_ids: ids.clone(),
            });
            let env = g.make_envelope(cmd, ids.clone(), now);
            g.commit_locked(shard, env)?;
            Ok(ids)
        })();
        std::future::ready(result)
    }
}

impl ControlPlaneStore for ObjectLogBackend {
    fn create_queue(
        &self,
        definition: QueueDefinition,
    ) -> impl std::future::Future<Output = EngineResult<CreateQueueOutcome>> + Send {
        let result = (|| {
            let mut g = self.inner.lock().expect("poisoned");
            let root = g.root.clone();
            let key = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
            match create_queue_metadata(&root, &mut g.queues, definition) {
                Ok(outcome) => {
                    g.hydrate_queue(&outcome.definition, &self.counters)?;
                    Ok(outcome)
                }
                Err(EngineError::QueueDefinitionConflict) => {
                    let stored = g
                        .queues
                        .get(&key)
                        .cloned()
                        .expect("durable conflict winner was cached");
                    g.hydrate_queue(&stored, &self.counters)?;
                    Err(EngineError::QueueDefinitionConflict)
                }
                Err(error) => Err(error),
            }
        })();
        std::future::ready(result)
    }

    fn queue_definition(
        &self,
        key: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<QueueDefinition>> + Send {
        let result = self
            .inner
            .lock()
            .expect("poisoned")
            .queues
            .get(key)
            .cloned()
            .ok_or(EngineError::NotFound);
        std::future::ready(result)
    }

    fn list_queues(
        &self,
        tenant: &TenantId,
    ) -> impl std::future::Future<Output = EngineResult<Vec<QueueId>>> + Send {
        let result: Vec<QueueId> = self
            .inner
            .lock()
            .expect("poisoned")
            .queues
            .keys()
            .filter(|k| k.tenant_id.as_str() == tenant.as_str())
            .map(|k| k.queue_id.clone())
            .collect();
        std::future::ready(Ok(result))
    }

    fn current_epoch(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<u64>> + Send {
        let result = {
            let g = self.inner.lock().expect("poisoned");
            if g.queues.contains_key(shard) {
                Ok(read_epoch(&g.root, shard))
            } else {
                Err(EngineError::NotFound)
            }
        };
        std::future::ready(result)
    }

    fn acquire_epoch(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<u64>> + Send {
        let result = {
            let g = self.inner.lock().expect("poisoned");
            if g.queues.contains_key(shard) {
                advance_epoch_object(&g.root, shard)
            } else {
                Err(EngineError::NotFound)
            }
        };
        std::future::ready(result)
    }
}

impl LogRead for ObjectLogBackend {
    fn read_from(
        &self,
        shard: &QueueKey,
        from: Option<CommandPosition>,
        limit: usize,
    ) -> impl std::future::Future<Output = EngineResult<CommandPage>> + Send {
        let result = (|| {
            let start = match &from {
                Some(p) => p.sequence + 1,
                None => 0,
            };
            let g = self.inner.lock().expect("poisoned");
            let all = g.read_envelopes(shard)?;
            let total = all.len() as u64;
            let entries: Vec<(CommandPosition, CommandEnvelope)> = all
                .into_iter()
                .filter(|(seq, _, _)| *seq >= start)
                .take(limit)
                .map(|(seq, epoch, env)| (CommandPosition::new(shard.clone(), epoch, seq), env))
                .collect();
            let consumed = start + entries.len() as u64;
            let cursor_epoch = entries
                .last()
                .map(|(pos, _)| pos.backend_epoch)
                .unwrap_or(0);
            let next = (consumed < total)
                .then(|| CommandPosition::new(shard.clone(), cursor_epoch, consumed));
            Ok(CommandPage { entries, next })
        })();
        std::future::ready(result)
    }
}

impl LogRead for LocalObjectLog {
    fn read_from(
        &self,
        shard: &QueueKey,
        from: Option<CommandPosition>,
        limit: usize,
    ) -> impl std::future::Future<Output = EngineResult<CommandPage>> + Send {
        let result = (|| {
            let inner = self.inner.lock().expect("object log store poisoned");
            if !inner.queues.contains_key(shard) {
                return Err(EngineError::NotFound);
            }
            let start = match &from {
                Some(p) => p.sequence + 1,
                None => 0,
            };
            let all = read_envelopes_from_root(&inner.root, shard)?;
            let total = all.len() as u64;
            let entries: Vec<(CommandPosition, CommandEnvelope)> = all
                .into_iter()
                .filter(|(seq, _, _)| *seq >= start)
                .take(limit)
                .map(|(seq, epoch, env)| (CommandPosition::new(shard.clone(), epoch, seq), env))
                .collect();
            let consumed = start + entries.len() as u64;
            let cursor_epoch = entries
                .last()
                .map(|(pos, _)| pos.backend_epoch)
                .unwrap_or(0);
            let next = (consumed < total)
                .then(|| CommandPosition::new(shard.clone(), cursor_epoch, consumed));
            Ok(CommandPage { entries, next })
        })();
        std::future::ready(result)
    }
}

/// Secondary-index reads over the (eventually-applied) shared projection (ADR-010). Read-after-write is
/// NOT guaranteed on this class — a hit reflects whatever the log has applied so far — but a delegating
/// impl is provided so the backend satisfies the library bound and serves replayed index state.
impl IndexQueryPort for ObjectLogBackend {
    fn index_get_unique(
        &self,
        shard: &QueueKey,
        index: &str,
        key: &[Vec<u8>],
    ) -> impl std::future::Future<Output = EngineResult<Option<IndexHit>>> + Send {
        let result = (|| {
            let g = self.inner.lock().expect("poisoned");
            let proj = g.projections.get(shard).ok_or(EngineError::NotFound)?;
            proj.index_get_unique(index, key)
        })();
        std::future::ready(result)
    }

    fn index_lookup(
        &self,
        shard: &QueueKey,
        index: &str,
        key: &[Vec<u8>],
    ) -> impl std::future::Future<Output = EngineResult<Vec<IndexHit>>> + Send {
        let result = (|| {
            let g = self.inner.lock().expect("poisoned");
            let proj = g.projections.get(shard).ok_or(EngineError::NotFound)?;
            proj.index_lookup(index, key)
        })();
        std::future::ready(result)
    }
}

impl ProjectionRead for ObjectLogBackend {
    fn select_eligible(
        &self,
        shard: &QueueKey,
        now: UtcTimestamp,
        limit: usize,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemId>>> + Send {
        let result = (|| {
            let g = self.inner.lock().expect("poisoned");
            let proj = g.projections.get(shard).ok_or(EngineError::NotFound)?;
            Ok(proj.select_eligible(now, limit))
        })();
        std::future::ready(result)
    }

    fn peek(
        &self,
        shard: &QueueKey,
        limit: usize,
    ) -> impl std::future::Future<Output = EngineResult<Vec<ItemView>>> + Send {
        let result = (|| {
            let g = self.inner.lock().expect("poisoned");
            let proj = g.projections.get(shard).ok_or(EngineError::NotFound)?;
            Ok(proj.peek(limit))
        })();
        std::future::ready(result)
    }

    fn pending(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<Vec<LeaseView>>> + Send {
        let result = (|| {
            let g = self.inner.lock().expect("poisoned");
            let proj = g.projections.get(shard).ok_or(EngineError::NotFound)?;
            Ok(proj.pending_leases())
        })();
        std::future::ready(result)
    }

    fn pending_summary(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<PendingSummary>> + Send {
        let result = (|| {
            let g = self.inner.lock().expect("poisoned");
            Ok(g.projections
                .get(shard)
                .ok_or(EngineError::NotFound)?
                .pending_summary())
        })();
        std::future::ready(result)
    }

    fn pending_page(
        &self,
        shard: &QueueKey,
        start: Option<ItemId>,
        limit: usize,
    ) -> impl std::future::Future<Output = EngineResult<PendingPage>> + Send {
        let result = (|| {
            let g = self.inner.lock().expect("poisoned");
            Ok(g.projections
                .get(shard)
                .ok_or(EngineError::NotFound)?
                .pending_page(start, limit))
        })();
        std::future::ready(result)
    }

    fn pending_range(
        &self,
        shard: &QueueKey,
        start: Option<ItemId>,
        end: Option<ItemId>,
        consumer: Option<&LeaseToken>,
        limit: usize,
    ) -> impl std::future::Future<Output = EngineResult<Vec<LeaseView>>> + Send {
        let result = (|| {
            let g = self.inner.lock().expect("poisoned");
            Ok(g.projections
                .get(shard)
                .ok_or(EngineError::NotFound)?
                .pending_range(start, end, consumer, limit))
        })();
        std::future::ready(result)
    }

    fn pending_by_ids(
        &self,
        shard: &QueueKey,
        ids: &[ItemId],
    ) -> impl std::future::Future<Output = EngineResult<Vec<LeaseView>>> + Send {
        let result = (|| {
            let g = self.inner.lock().expect("poisoned");
            Ok(g.projections
                .get(shard)
                .ok_or(EngineError::NotFound)?
                .pending_by_ids(ids))
        })();
        std::future::ready(result)
    }

    fn claimed_view(
        &self,
        shard: &QueueKey,
        ids: &[ItemId],
    ) -> impl std::future::Future<Output = EngineResult<Vec<ClaimedItem>>> + Send {
        let result = (|| {
            let g = self.inner.lock().expect("poisoned");
            let proj = g.projections.get(shard).ok_or(EngineError::NotFound)?;
            Ok(proj.render_claimed(ids))
        })();
        std::future::ready(result)
    }

    fn live_items(
        &self,
        shard: &QueueKey,
        keys: &[ClientItemKey],
    ) -> impl std::future::Future<Output = EngineResult<Vec<Option<LiveItemView>>>> + Send {
        let result = (|| {
            let g = self.inner.lock().expect("poisoned");
            let proj = g.projections.get(shard).ok_or(EngineError::NotFound)?;
            Ok(proj.live_items_by_key(keys))
        })();
        std::future::ready(result)
    }

    fn metrics(
        &self,
        queue: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<QueueMetrics>> + Send {
        let result = (|| {
            let g = self.inner.lock().expect("poisoned");
            let shard = queue.clone();
            let proj = g.projections.get(&shard).ok_or(EngineError::NotFound)?;
            Ok(proj.metrics())
        })();
        std::future::ready(result)
    }

    fn terminal_emission_metrics(
        &self,
        shard: &QueueKey,
        now: UtcTimestamp,
        emit_change_records: bool,
        emission_cursor: Option<&CommandPosition>,
    ) -> impl std::future::Future<Output = EngineResult<TerminalEmissionMetrics>> + Send {
        let result = (|| {
            let g = self.inner.lock().expect("poisoned");
            let proj = g.projections.get(shard).ok_or(EngineError::NotFound)?;
            Ok(proj.terminal_emission_metrics(now, emit_change_records, emission_cursor))
        })();
        std::future::ready(result)
    }
}

impl SnapshotStore for ObjectLogBackend {
    fn write_snapshot(
        &self,
        shard: &QueueKey,
        position: CommandPosition,
        snapshot: ProjectionSnapshot,
    ) -> impl std::future::Future<Output = EngineResult<SnapshotRef>> + Send {
        let result = (|| {
            let g = self.inner.lock().expect("poisoned");
            let snap_dir = g.shard_dir(shard).join("snapshots");
            fs::create_dir_all(&snap_dir).map_err(store)?;
            // ref index = max(existing snap-N) + 1 (compaction-safe; never overwrites a retained ref).
            let mut max: Option<u64> = None;
            for entry in fs::read_dir(&snap_dir).map_err(store)? {
                if let Some(n) = entry
                    .map_err(store)?
                    .path()
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .and_then(|s| s.strip_prefix("snap-"))
                    .and_then(|s| s.parse::<u64>().ok())
                {
                    max = Some(max.map_or(n, |m| m.max(n)));
                }
            }
            let ref_id = format!("snap-{}", max.map_or(0, |m| m + 1));
            fs::write(
                snap_dir.join(format!("{ref_id}.json")),
                to_json(&SnapshotObject {
                    epoch: position.backend_epoch,
                    seq: position.sequence,
                    payload: snapshot.payload,
                })?,
            )
            .map_err(store)?;
            Ok(SnapshotRef {
                queue: shard.clone(),
                position,
                ref_id,
            })
        })();
        std::future::ready(result)
    }

    fn latest_snapshot(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<Option<SnapshotRef>>> + Send {
        let result = (|| {
            let g = self.inner.lock().expect("poisoned");
            let snap_dir = g.shard_dir(shard).join("snapshots");
            if !snap_dir.exists() {
                return Ok(None);
            }
            let mut best: Option<(usize, SnapshotObject, String)> = None;
            for entry in fs::read_dir(&snap_dir).map_err(store)? {
                let path = entry.map_err(store)?.path();
                let Some(ref_id) = path.file_stem().and_then(|s| s.to_str()) else {
                    continue;
                };
                let n = ref_id
                    .strip_prefix("snap-")
                    .and_then(|s| s.parse::<usize>().ok());
                let obj: SnapshotObject =
                    serde_json::from_str(&fs::read_to_string(&path).map_err(store)?)
                        .map_err(store)?;
                let n = n.unwrap_or(0);
                if best.as_ref().map(|(bn, _, _)| n >= *bn).unwrap_or(true) {
                    best = Some((n, obj, ref_id.to_string()));
                }
            }
            Ok(best.map(|(_, obj, ref_id)| SnapshotRef {
                queue: shard.clone(),
                position: CommandPosition::new(shard.clone(), obj.epoch, obj.seq),
                ref_id,
            }))
        })();
        std::future::ready(result)
    }

    fn snapshot_at_or_before(
        &self,
        shard: &QueueKey,
        position: &CommandPosition,
    ) -> impl std::future::Future<Output = EngineResult<Option<SnapshotRef>>> + Send {
        let result = (|| {
            let g = self.inner.lock().expect("poisoned");
            let snap_dir = g.shard_dir(shard).join("snapshots");
            if !snap_dir.exists() {
                return Ok(None);
            }
            let mut best: Option<(usize, SnapshotObject, String)> = None;
            for entry in fs::read_dir(&snap_dir).map_err(store)? {
                let path = entry.map_err(store)?.path();
                let Some(ref_id) = path.file_stem().and_then(|s| s.to_str()) else {
                    continue;
                };
                let n = ref_id
                    .strip_prefix("snap-")
                    .and_then(|s| s.parse::<usize>().ok());
                let obj: SnapshotObject =
                    serde_json::from_str(&fs::read_to_string(&path).map_err(store)?)
                        .map_err(store)?;
                let pos = CommandPosition::new(shard.clone(), obj.epoch, obj.seq);
                if pos.precedes(position) || pos == *position {
                    let n = n.unwrap_or(0);
                    if best.as_ref().map(|(bn, _, _)| n >= *bn).unwrap_or(true) {
                        best = Some((n, obj, ref_id.to_string()));
                    }
                }
            }
            Ok(best.map(|(_, obj, ref_id)| SnapshotRef {
                queue: shard.clone(),
                position: CommandPosition::new(shard.clone(), obj.epoch, obj.seq),
                ref_id,
            }))
        })();
        std::future::ready(result)
    }

    fn read_snapshot(
        &self,
        snapshot_ref: &SnapshotRef,
    ) -> impl std::future::Future<Output = EngineResult<ProjectionSnapshot>> + Send {
        let result = (|| {
            let g = self.inner.lock().expect("poisoned");
            let path = g
                .shard_dir(&snapshot_ref.queue)
                .join("snapshots")
                .join(format!("{}.json", snapshot_ref.ref_id));
            if !path.exists() {
                return Err(EngineError::NotFound);
            }
            let obj: SnapshotObject =
                serde_json::from_str(&fs::read_to_string(&path).map_err(store)?).map_err(store)?;
            Ok(ProjectionSnapshot {
                payload: obj.payload,
            })
        })();
        std::future::ready(result)
    }

    fn high_water(
        &self,
        shard: &QueueKey,
    ) -> impl std::future::Future<Output = EngineResult<Option<CommandPosition>>> + Send {
        let g = self.inner.lock().expect("poisoned");
        let result = g.read_high_water(shard);
        std::future::ready(result)
    }

    fn set_high_water(
        &self,
        shard: &QueueKey,
        position: CommandPosition,
    ) -> impl std::future::Future<Output = EngineResult<()>> + Send {
        let result = (|| {
            let g = self.inner.lock().expect("poisoned");
            if let Some(cur) = g.read_high_water(shard)?
                && !cur.precedes(&position)
                && cur != position
            {
                return Err(EngineError::Invalid("high-water regression"));
            }
            let dir = g.shard_dir(shard);
            fs::create_dir_all(&dir).map_err(store)?;
            fs::write(
                dir.join("high_water.json"),
                to_json(&HighWater {
                    epoch: position.backend_epoch,
                    seq: position.sequence,
                })?,
            )
            .map_err(store)?;
            Ok(())
        })();
        std::future::ready(result)
    }
}

impl HistoricalProjectionRead for ObjectLogBackend {
    type AsOfProjection = InMemoryProjection;

    async fn current_position(&self, shard: &QueueKey) -> EngineResult<CommandPosition> {
        self.high_water(shard).await?.ok_or(EngineError::NotFound)
    }

    async fn read_as_of<T, F>(
        &self,
        shard: &QueueKey,
        position: CommandPosition,
        query: F,
    ) -> EngineResult<T>
    where
        T: Send,
        F: FnOnce(&Self::AsOfProjection) -> EngineResult<T> + Send,
    {
        let definition = {
            let g = self.inner.lock().expect("poisoned");
            g.queues.get(shard).cloned().ok_or(EngineError::NotFound)?
        };
        let snapshot_ref = self.snapshot_at_or_before(shard, &position).await?;
        let snapshot = match snapshot_ref.as_ref() {
            Some(snapshot_ref) => Some(self.read_snapshot(snapshot_ref).await?),
            None => None,
        };
        let mut as_of = InMemoryProjection::new();
        as_of.ensure_shard(&definition)?;
        if let Some(snapshot) = snapshot {
            let image = ProjectionImage::from_bytes(&snapshot.payload)?;
            as_of.hydrate_shard(&definition, image)?;
        }
        let mut from = snapshot_ref.map(|s| s.position);
        loop {
            let page = self.read_from(shard, from.clone(), 8192).await?;
            if page.entries.is_empty() {
                break;
            }
            let mut positions = Vec::new();
            let mut envelopes = Vec::new();
            let mut reached_target = false;
            for (entry_position, env) in page.entries {
                if entry_position == position || entry_position.precedes(&position) {
                    positions.push(entry_position.clone());
                    envelopes.push(env);
                } else {
                    reached_target = true;
                    break;
                }
            }
            if !positions.is_empty() {
                as_of.apply_borrowed(&positions, &envelopes)?;
            }
            if reached_target || page.next.is_none() {
                break;
            }
            from = page.next;
        }
        query(&as_of)
    }
}
