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

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use bytes::Bytes;
use pqueue_core::{
    ClientItemKey, GroupKey, ItemId, ItemState, LeaseToken, PriorityValue, QueueDefinition,
    QueueId, TenantId, UtcTimestamp,
};
use pqueue_engine::{
    Backend, ClaimCommand, ClaimCompatibility, ClaimPort, ClaimRequest, Claimed, ClaimedItem,
    CommandChecksum, CommandEnvelope, CommandId, CommandPage, CommandPosition, ControlPlaneStore,
    CreateQueueOutcome, DurabilityClass, EngineError, EngineResult, FinalizeCommand,
    FinalizeOutcome, FinalizePort, ItemView, LeaseExpiredCommand, LeaseView, LiveItemView, LogRead,
    LogWriter, PayloadUpdate, ProjectionRead, ProjectionSnapshot, ProjectionWriter,
    PurgeItemsCommand, PurgePort, PushCommand, PushPort, PushSpec, QueueCommand, QueueCounters,
    QueueKey, QueueMetrics, ReassignLeaseCommand, ReassignLeasePort, ReclaimDriver, ReclaimPort,
    RenewLeaseCommand, RenewLeasePort, SnapshotRef, SnapshotStore, TickReport, UpdateFieldsPort,
    UpsertOutcome, UpsertPort, build_push_items, require_item_level_claim, validate_purge_force,
};
use pqueue_projection::ProjectionData;

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

/// Durably advance a shard's `assignment_epoch` to a strictly-greater value (TD-003 acquire). Returns the
/// new epoch.
///
/// SCOPE (BQ-20): this is a plain read-then-overwrite of `epoch.json`, made safe ONLY by the process-wide
/// `inner` mutex that serializes acquire vs append within one owner. It is NOT yet the TD-004
/// manifest-CAS epoch-fence entry the real multi-owner S3 model requires (compare-and-swap against the
/// manifest's recorded epoch, committed before any data segment) — that pairs with the S3-CAS control
/// plane and the per-entry-epoch object format (see `read_from`), tracked as a follow-up.
fn advance_epoch_object(root: &Path, shard: &QueueKey) -> EngineResult<u64> {
    let dir = shard_dir(root, shard);
    fs::create_dir_all(&dir).map_err(store)?;
    let next = read_epoch(root, shard) + 1;
    fs::write(dir.join("epoch.json"), to_json(&next)?).map_err(store)?;
    Ok(next)
}

/// The next durable sequence for a shard: `max(existing object index) + 1` (compaction-safe). Empty
/// log → 0.
fn next_seq(log_dir: &Path) -> EngineResult<u64> {
    let mut max: Option<u64> = None;
    if log_dir.exists() {
        for entry in fs::read_dir(log_dir).map_err(store)? {
            let entry = entry.map_err(store)?;
            if let Some(n) = entry
                .path()
                .file_stem()
                .and_then(|s| s.to_str())
                .and_then(|s| s.parse::<u64>().ok())
            {
                max = Some(max.map_or(n, |m| m.max(n)));
            }
        }
    }
    Ok(max.map_or(0, |m| m + 1))
}

/// Durably write `env` as the next object + advance the persisted high-water object. Returns the
/// committed sequence. Touches only the filesystem under `root` (not the in-memory projection).
///
/// Enforces the eventual-apply class ban on the atomic XDEL+XADD upsert (Invariant 2) at the SINGLE
/// durable chokepoint both write paths funnel through (`commit_locked` and `Backend::write` →
/// `ObjLogWriter`): a `ReplacePending` command is refused with `Unavailable` BEFORE any object is
/// written, so the ban holds at the write path, not just the `replace_if_pending` port.
fn append_object(root: &Path, shard: &QueueKey, env: &CommandEnvelope) -> EngineResult<(u64, u64)> {
    if matches!(env.command, QueueCommand::ReplacePending(_)) {
        return Err(EngineError::Unavailable);
    }
    let dir = shard_dir(root, shard);
    let log_dir = dir.join("log");
    fs::create_dir_all(&log_dir).map_err(store)?;
    let epoch = read_epoch(root, shard); // in-process owner: stamp the queue's current durable epoch.
    let seq = next_seq(&log_dir)?;
    // Object name: zero-padded so lexical order == sequence order.
    fs::write(log_dir.join(format!("{seq:020}.json")), to_json(env)?).map_err(store)?;
    fs::write(
        dir.join("high_water.json"),
        to_json(&HighWater { epoch, seq })?,
    )
    .map_err(store)?;
    Ok((epoch, seq))
}

/// The high-water object payload (a stored field, not recomputed from a possibly-compacted log).
#[derive(serde::Serialize, serde::Deserialize)]
struct HighWater {
    epoch: u64,
    seq: u64,
}

/// A stored snapshot object: its position + opaque payload.
#[derive(serde::Serialize, serde::Deserialize)]
struct SnapshotObject {
    epoch: u64,
    seq: u64,
    payload: Vec<u8>,
}

struct Inner {
    root: PathBuf,
    projections: HashMap<QueueKey, ProjectionData>,
    queues: HashMap<QueueKey, QueueDefinition>,
    cmd_seq: u64,
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
            item_ids,
            command,
            checksum: CommandChecksum(0),
            created_at,
        }
    }

    /// Durable append + infallible in-memory apply (the orchestration unit). Caller MUST pre-validate.
    ///
    /// BQ-20 NOTE: the data-plane fast path is the in-process owner — it STAMPS the queue's current epoch
    /// (via `append_object`) but does NOT validate an `expected_epoch`; the TD-003 fence that REJECTS a
    /// stale epoch lives at the [`ObjLogWriter::append`] seam. Owner-epoch caching on this path is BQ-21.
    fn commit_locked(&mut self, shard: &QueueKey, env: CommandEnvelope) -> EngineResult<()> {
        append_object(&self.root, shard, &env)?;
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

    /// All log envelopes for a shard in sequence order (replay order). Tolerates a torn TRAILING object
    /// (an append interrupted by a crash): since `next_seq` is `max+1`, only the highest-seq object can
    /// be a partial write, and it has no successor, so it is treated as uncommitted and skipped. A parse
    /// failure on any NON-final object is genuine corruption and is propagated.
    fn read_envelopes(&self, shard: &QueueKey) -> EngineResult<Vec<(u64, CommandEnvelope)>> {
        let log_dir = self.shard_dir(shard).join("log");
        if !log_dir.exists() {
            return Ok(Vec::new());
        }
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
        let mut rows: Vec<(u64, CommandEnvelope)> = Vec::with_capacity(files.len());
        for (i, (seq, path)) in files.iter().enumerate() {
            let json = fs::read_to_string(path).map_err(store)?;
            match serde_json::from_str(&json) {
                Ok(env) => rows.push((*seq, env)),
                // Torn trailing object → uncommitted, skip. Earlier object → real corruption, fail.
                Err(_) if i == last => continue,
                Err(e) => return Err(store(e)),
            }
        }
        Ok(rows)
    }

    fn read_high_water(&self, shard: &QueueKey) -> EngineResult<Option<CommandPosition>> {
        let path = self.shard_dir(shard).join("high_water.json");
        if !path.exists() {
            return Ok(None);
        }
        let hw: HighWater =
            serde_json::from_str(&fs::read_to_string(&path).map_err(store)?).map_err(store)?;
        Ok(Some(CommandPosition::new(shard.clone(), hw.epoch, hw.seq)))
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
            let mut proj = ProjectionData::new(definition.priority_model);
            for (_seq, env) in self.read_envelopes(&shard)? {
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
            self.projections.insert(shard, proj);
            self.queues.insert(key, definition);
        }
        if let Some(m) = max_cmd_seq {
            self.cmd_seq = m + 1;
        }
        Ok(())
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
        let mut inner = Inner {
            root: root.into(),
            projections: HashMap::new(),
            queues: HashMap::new(),
            cmd_seq: 0,
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
}

// ---------------------------------------------------------------------------
// UoW writer views (Backend::write)
// ---------------------------------------------------------------------------

struct ObjLogWriter {
    root: PathBuf,
}

impl LogWriter for ObjLogWriter {
    fn append(
        &mut self,
        shard: &QueueKey,
        commands: &[CommandEnvelope],
        expected_epoch: u64,
    ) -> EngineResult<Vec<CommandPosition>> {
        // TD-003 fence: reject a non-current epoch (a stale owner) before writing anything.
        if expected_epoch != read_epoch(&self.root, shard) {
            return Err(EngineError::EpochFenced);
        }
        let mut positions = Vec::with_capacity(commands.len());
        for env in commands {
            let (epoch, seq) = append_object(&self.root, shard, env)?;
            positions.push(CommandPosition::new(shard.clone(), epoch, seq));
        }
        Ok(positions)
    }
}

struct ObjProjectionWriter<'a> {
    projections: &'a mut HashMap<QueueKey, ProjectionData>,
}

impl ProjectionWriter for ObjProjectionWriter<'_> {
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

    fn write<R, F>(&self, f: F) -> impl std::future::Future<Output = EngineResult<R>> + Send
    where
        F: FnOnce(&mut dyn LogWriter, &mut dyn ProjectionWriter) -> EngineResult<R> + Send,
        R: Send,
    {
        // The log-writer needs only `root` (writes objects to the filesystem); the projection-writer
        // needs only the `projections` map. Disjoint, so the borrow checker is satisfied by destructuring
        // — the log side gets a cheap `PathBuf` clone, the projection side a `&mut` to its map.
        let result = {
            let mut guard = self.inner.lock().expect("objectlog poisoned");
            let Inner {
                root, projections, ..
            } = &mut *guard;
            let mut lw = ObjLogWriter { root: root.clone() };
            let mut pw = ObjProjectionWriter { projections };
            f(&mut lw, &mut pw)
        };
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
                proj.eligible_candidates(req.now, req.max_items)
            };
            if candidates.is_empty() {
                return Ok(Claimed::default());
            }
            let cmd = QueueCommand::Claim(ClaimCommand {
                item_ids: candidates.clone(),
                lease_token: req.lease_token.clone(),
                lease_expires_at: req.lease_expires_at,
            });
            let env = g.make_envelope(cmd, candidates.clone(), req.now);
            g.commit_locked(&req.shard, env)?;
            let proj = g.projections.get(&req.shard).ok_or(EngineError::NotFound)?;
            Ok(Claimed {
                items: proj.render_claimed(&candidates),
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
            let mut g = self.inner.lock().expect("poisoned");
            // Pre-validate the shard exists before any durable object write (commit_locked expects it).
            if !g.projections.contains_key(shard) {
                return Err(EngineError::NotFound);
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
}

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
            // None path). Objectlog's `commit_locked`/`append_object` stamp the queue's current durable
            // epoch but do NOT validate an `expected_epoch` (the TD-003 reject lives at the
            // `ObjLogWriter::append` seam, not in the data-plane fast path). So replicate that seam's fence
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
            let key = QueueKey::new(definition.tenant_id.clone(), definition.queue_id.clone());
            if let Some(existing) = g.queues.get(&key) {
                if existing.ordering_mode != definition.ordering_mode
                    || existing.priority_model != definition.priority_model
                {
                    return Err(EngineError::QueueDefinitionConflict);
                }
                return Ok(CreateQueueOutcome {
                    created: false,
                    definition: existing.clone(),
                });
            }
            let shard = key.clone();
            let dir = g.shard_dir(&shard);
            fs::create_dir_all(&dir).map_err(store)?;
            fs::write(dir.join("queue.json"), to_json(&definition)?).map_err(store)?;
            g.projections
                .insert(shard, ProjectionData::new(definition.priority_model));
            g.queues.insert(key, definition.clone());
            Ok(CreateQueueOutcome {
                created: true,
                definition,
            })
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
            // BQ-20: replayed positions are SEQ-authoritative; the epoch label is non-authoritative here.
            // The log object (`{seq}.json`) stores only the envelope, not the epoch it was written under, so
            // a per-entry epoch is not recoverable from the object alone (unlike the sqlite/postgres
            // `log_entries.epoch` column). Carrying the true per-entry epoch needs the object format to
            // record it alongside the manifest-CAS epoch fence — tracked with that schema work (see the
            // `advance_epoch_object` note). The high-water guard is seq-monotonic, and no recovery path
            // re-derives high-water from a replayed cross-epoch position today, so this is latent.
            let entries: Vec<(CommandPosition, CommandEnvelope)> = all
                .into_iter()
                .filter(|(seq, _)| *seq >= start)
                .take(limit)
                .map(|(seq, env)| (CommandPosition::new(shard.clone(), 0, seq), env))
                .collect();
            let consumed = start + entries.len() as u64;
            let next = (consumed < total).then(|| CommandPosition::new(shard.clone(), 0, consumed));
            Ok(CommandPage { entries, next })
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
