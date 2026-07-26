#![forbid(unsafe_code)]
//! # fireweed-objectlog
//!
//! Object-log primitives and the supported composed object-log profiles. The segmented log is durable
//! authority; callers pair it with a projection through [`composed_objectlog_backend`] or
//! [`composed_objectlog_backend_group_commit`]. [`LocalObjectLog`] remains the filesystem log-only
//! substrate used by tests and local composition.

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
    PreparedObjectLogCommit, prepare_serialized_commands, serialized_peak_charge,
};
pub use async_log::{
    AsyncObjectLog, DEFAULT_ASYNC_OBJECT_LOG_CAPACITY, DEFAULT_ASYNC_OBJECT_LOG_WORKERS,
};
pub use compose_log::{
    ComposedObjectLogBackend, ObjectLog, composed_objectlog_backend,
    composed_objectlog_backend_group_commit,
};
pub use segmented::{FaultCutPoint, FaultHook, SegmentConfig, SerializedCommandEnvelope};

use std::collections::HashMap;
use std::fs;
use std::fs::OpenOptions;
use std::io::ErrorKind;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;

use fireweed_core::QueueDefinition;
use fireweed_engine::{
    CommandEnvelope, CommandPage, CommandPosition, CreateQueueOutcome, EngineError, EngineResult,
    LogRead, QueueCommand, QueueKey, validate_gate_command,
};

fn store<E: std::fmt::Display>(e: E) -> EngineError {
    EngineError::Storage(e.to_string())
}

fn to_json<T: serde::Serialize>(value: &T) -> EngineResult<String> {
    serde_json::to_string(value).map_err(store)
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

fn read_queue_definitions(root: &Path) -> EngineResult<HashMap<QueueKey, QueueDefinition>> {
    let mut queues = HashMap::new();
    if !root.exists() {
        create_dir_all_durable(root)?;
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
    create_dir_all_durable(&dir)?;
    let queue_file = dir.join("queue.json");
    let bytes = to_json(&definition)?;
    let (mut file, temp_file) = open_unique_queue_temp(&dir)?;
    let temp = TempFileGuard(temp_file);
    let write_result = (|| {
        file.write_all(bytes.as_bytes()).map_err(store)?;
        file.sync_all().map_err(store)?;
        Ok::<(), EngineError>(())
    })();
    drop(file);
    // Close before propagating a write/sync error so TempFileGuard can remove the file on platforms that
    // do not permit deleting an open handle.
    write_result?;
    let created = match fs::hard_link(&temp.0, &queue_file) {
        Ok(()) => {
            sync_directory(&dir)?;
            true
        }
        Err(err) if err.kind() == ErrorKind::AlreadyExists => false,
        Err(err) => return Err(store(err)),
    };
    drop(temp);
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

static QUEUE_TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

struct TempFileGuard(PathBuf);

impl Drop for TempFileGuard {
    fn drop(&mut self) {
        if fs::remove_file(&self.0).is_ok()
            && let Some(parent) = self.0.parent()
        {
            // Best-effort durability for cleanup on early-return paths. The publication itself was already
            // synced and must not be downgraded merely because cleanup syncing is unavailable.
            let _ = fs::File::open(parent).and_then(|directory| directory.sync_all());
        }
    }
}

fn open_unique_queue_temp(dir: &Path) -> EngineResult<(fs::File, PathBuf)> {
    loop {
        let attempt = QUEUE_TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = dir.join(format!("queue.json.tmp.{}.{}", std::process::id(), attempt));
        match OpenOptions::new().write(true).create_new(true).open(&path) {
            Ok(file) => return Ok((file, path)),
            Err(error) if error.kind() == ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(store(error)),
        }
    }
}

fn sync_directory(dir: &Path) -> EngineResult<()> {
    fs::File::open(dir)
        .and_then(|directory| directory.sync_all())
        .map_err(store)
}

fn create_dir_all_durable(dir: &Path) -> EngineResult<()> {
    let mut missing = Vec::new();
    let mut cursor = dir;
    while !cursor.exists() {
        missing.push(cursor.to_path_buf());
        let Some(parent) = cursor.parent() else {
            break;
        };
        cursor = parent;
    }
    fs::create_dir_all(dir).map_err(store)?;
    for directory in missing.iter().rev() {
        if let Some(parent) = directory.parent() {
            sync_directory(parent)?;
        }
        sync_directory(directory)?;
    }
    if missing.is_empty() {
        sync_directory(dir)?;
    }
    Ok(())
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
    for (i, (_seq, path)) in files.iter().enumerate() {
        let json = fs::read_to_string(path).map_err(store)?;
        match serde_json::from_str::<SegmentRecord>(&json) {
            Ok(record) => {
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
