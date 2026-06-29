//! # Segmented S3 object-log group-commit substrate (TD-004 production form)
//!
//! This is the **production** object-log substrate, distinct from the in-process [`LocalObjectLog`]
//! file smoke reference. Where `LocalObjectLog` writes ONE object per command, this substrate buffers
//! commands per `(tenant, queue)` and seals **segments** (many commands per durable object) onto an
//! S3-compatible object store, realizing the TD-004 group-commit pipeline:
//!
//! 1. **Buffer** commands per queue in arrival order.
//! 2. **Seal** a segment when EITHER the buffered byte size reaches `target_bytes` OR the oldest
//!    buffered command's age reaches `max_latency_ms` (whichever fires first — TD-004 step 2).
//! 3. **Write segment** — one immutable, checksummed object per sealed segment (TD-004 step 3).
//! 4. **Commit manifest** — append a manifest entry naming the segment via a conditional
//!    (create-only) object write that is the CAS boundary AND the epoch fence (TD-004 step 4).
//! 5. **Ack** — a command's positions are returned to the caller ONLY after its segment's manifest
//!    entry is durably committed (TD-004 step 5). A buffered-but-unsealed command is NOT acked, and a
//!    segment whose manifest commit was fenced is an orphan that no reader ever observes.
//!
//! **Manifest-CAS epoch fence (reused from the `pqueue-e5c6d6fc` pattern).** Each manifest entry records
//! the writer's `assignment_epoch`. The manifest is an append-only series of immutable objects
//! `manifest/{index:020}.json`; a commit is a create-only PUT at the next index (the CAS) gated on the
//! writer's `expected_epoch` still equalling the queue's current epoch (the highest epoch any committed
//! manifest entry records). An epoch handoff publishes a **fence entry** (TD-004 implementation (b)) into
//! the manifest BEFORE the new owner writes data; a stale-epoch writer that tries to seal then observes the
//! higher epoch on the manifest tail and is rejected [`EngineError::EpochFenced`] — no torn segment is
//! committed (the fence is checked before the segment object is written).
//!
//! **Object store seam.** The substrate is generic over [`BlobStore`], whose only required primitive beyond
//! plain `get`/`put`/`list` is `put_if_absent` (create-only PUT = the CAS). [`InMemoryBlobStore`] backs the
//! unit tests with no network; [`S3BlobStore`] is a minimal hand-rolled SigV4 S3 client (PUT/GET/LIST +
//! create-only conditional PUT) that runs the SAME substrate against MinIO / any S3-compatible store.

use std::collections::BTreeMap;
use std::io::{Read, Write as _};
use std::net::TcpStream;
use std::sync::Mutex;

use pqueue_core::QueueDefinition;
use pqueue_engine::{
    CommandEnvelope, CommandPosition, EngineError, EngineResult, QueueCommand, QueueKey,
    validate_gate_command,
};
use sha2::{Digest, Sha256};

// ---------------------------------------------------------------------------
// Object-store seam (the minimal S3 surface the substrate needs)
// ---------------------------------------------------------------------------

/// The minimal S3-compatible object surface the segmented substrate drives. Implemented in-memory (unit
/// tests, no network) and over a real S3 endpoint ([`S3BlobStore`], tested against MinIO).
pub trait BlobStore: Send + Sync {
    /// Unconditional PUT (used for immutable, deterministically-keyed segment objects; a retried write
    /// re-puts identical bytes, so it is idempotent at a stable key — TD-004 "idempotent segment PUT").
    fn put(&self, key: &str, body: &[u8]) -> EngineResult<()>;

    /// Conditional create-only PUT — the CAS primitive. `Ok(true)` if the object was created, `Ok(false)`
    /// if an object already exists at `key` (the conditional failed). This is the manifest commit's
    /// compare-and-set: two writers racing to extend the manifest from the same tail cannot both win.
    fn put_if_absent(&self, key: &str, body: &[u8]) -> EngineResult<bool>;

    /// GET an object. `Ok(None)` if it does not exist.
    fn get(&self, key: &str) -> EngineResult<Option<Vec<u8>>>;

    /// List keys under `prefix` (lexical order not required; the caller sorts).
    fn list(&self, prefix: &str) -> EngineResult<Vec<String>>;
}

/// Share one store between several owners (e.g. two competing epoch holders) — delegates through the `Arc`.
impl<T: BlobStore + ?Sized> BlobStore for std::sync::Arc<T> {
    fn put(&self, key: &str, body: &[u8]) -> EngineResult<()> {
        (**self).put(key, body)
    }
    fn put_if_absent(&self, key: &str, body: &[u8]) -> EngineResult<bool> {
        (**self).put_if_absent(key, body)
    }
    fn get(&self, key: &str) -> EngineResult<Option<Vec<u8>>> {
        (**self).get(key)
    }
    fn list(&self, prefix: &str) -> EngineResult<Vec<String>> {
        (**self).list(prefix)
    }
}

/// In-memory [`BlobStore`] for unit tests — no network. `put_if_absent` is a genuine compare-and-set under
/// the map lock, so the manifest-CAS / epoch-fence path is exercised without an S3 server.
#[derive(Default)]
pub struct InMemoryBlobStore {
    objects: Mutex<BTreeMap<String, Vec<u8>>>,
}

impl InMemoryBlobStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of objects currently stored (test introspection).
    pub fn object_count(&self) -> usize {
        self.objects.lock().expect("blobstore poisoned").len()
    }
}

impl BlobStore for InMemoryBlobStore {
    fn put(&self, key: &str, body: &[u8]) -> EngineResult<()> {
        self.objects
            .lock()
            .expect("blobstore poisoned")
            .insert(key.to_string(), body.to_vec());
        Ok(())
    }

    fn put_if_absent(&self, key: &str, body: &[u8]) -> EngineResult<bool> {
        let mut g = self.objects.lock().expect("blobstore poisoned");
        if g.contains_key(key) {
            return Ok(false);
        }
        g.insert(key.to_string(), body.to_vec());
        Ok(true)
    }

    fn get(&self, key: &str) -> EngineResult<Option<Vec<u8>>> {
        Ok(self
            .objects
            .lock()
            .expect("blobstore poisoned")
            .get(key)
            .cloned())
    }

    fn list(&self, prefix: &str) -> EngineResult<Vec<String>> {
        Ok(self
            .objects
            .lock()
            .expect("blobstore poisoned")
            .keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect())
    }
}

// ---------------------------------------------------------------------------
// Configuration (TD-004 §Configuration Validation)
// ---------------------------------------------------------------------------

/// Group-commit segment-sizing configuration. Two independent seal triggers are supported (TD-004 step 2):
/// a byte-size threshold (`target_bytes`) AND a latency cap (`max_latency_ms`); a segment seals on
/// whichever fires first. `>= 2` distinct configurations are therefore expressible (e.g. a small-latency
/// profile and a large-batch profile), satisfying the substrate's "configurable segment sizing" contract.
#[derive(Debug, Clone, Copy)]
pub struct SegmentConfig {
    /// Seal when the buffered serialized byte size reaches this value (TD-004 `segment_target_bytes`).
    pub target_bytes: usize,
    /// Seal when the oldest buffered command's age reaches this many ms (TD-004 `segment_max_latency_ms`).
    pub max_latency_ms: u64,
    /// Dev/test escape hatch that allows sealing one command per segment. MUST be false in production
    /// (TD-004 "Reject 1-object-per-command"); a production config seals many commands per segment.
    pub dev_unsafe_one_command_segments: bool,
}

impl SegmentConfig {
    /// Validate (`max_latency_ms > 0`, `target_bytes > 0`; TD-004 §Window sanity). Returns the config.
    pub fn new(target_bytes: usize, max_latency_ms: u64) -> EngineResult<Self> {
        if max_latency_ms == 0 {
            return Err(EngineError::Invalid("segment_max_latency_ms must be > 0"));
        }
        if target_bytes == 0 {
            return Err(EngineError::Invalid("segment_target_bytes must be > 0"));
        }
        Ok(Self {
            target_bytes,
            max_latency_ms,
            dev_unsafe_one_command_segments: false,
        })
    }
}

// ---------------------------------------------------------------------------
// Measured counters surface (release-ledger harness)
// ---------------------------------------------------------------------------

/// Release-measurable counters: how many segments sealed, how many objects were PUT to the store (segment
/// objects + manifest objects + fence entries), how many commands committed, and the per-segment
/// group-commit batch sizes. Surfaced to the release ledger harness as the object-log cost evidence
/// (segments, not commands, are the durable-commit unit).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SegmentCounters {
    /// Sealed-and-committed data segments (the durable-commit unit; cost scales with this, not commands).
    pub segments_sealed: u64,
    /// Total objects PUT to the store (segment objects + committed manifest objects + epoch-fence entries).
    pub objects_put: u64,
    /// Commands durably committed (acked) through sealed segments.
    pub commands_committed: u64,
    /// Per-segment group-commit batch size (commands per sealed segment), in seal order.
    pub group_commit_batches: Vec<usize>,
}

impl SegmentCounters {
    /// Mean commands-per-segment (the group-commit amortization factor); `0.0` if nothing sealed.
    pub fn mean_batch_size(&self) -> f64 {
        if self.group_commit_batches.is_empty() {
            return 0.0;
        }
        self.commands_committed as f64 / self.group_commit_batches.len() as f64
    }

    /// Largest sealed group-commit batch (`0` if nothing sealed).
    pub fn max_batch_size(&self) -> usize {
        self.group_commit_batches.iter().copied().max().unwrap_or(0)
    }
}

// ---------------------------------------------------------------------------
// On-store object formats
// ---------------------------------------------------------------------------

/// An immutable sealed segment object: a contiguous run of commands committed under one `epoch`.
#[derive(serde::Serialize, serde::Deserialize)]
struct Segment {
    epoch: u64,
    first_seq: u64,
    commands: Vec<CommandEnvelope>,
}

/// One append-only manifest entry. A data entry names a segment; a `fence` entry records an epoch handoff
/// and names no segment (TD-004 implementation (b): epoch fence published to the manifest before handoff).
#[derive(serde::Serialize, serde::Deserialize, Clone)]
struct ManifestEntry {
    index: u64,
    epoch: u64,
    #[serde(default)]
    fence: bool,
    segment_key: Option<String>,
    first_seq: u64,
    last_seq: u64,
    /// Per-segment checksum over the serialized commands (TD-004 step 2 segment checksum), validated on read.
    checksum: u64,
    committed_at_ms: i64,
}

/// FNV-1a 64-bit checksum (small, dependency-free) over a segment's serialized bytes.
fn checksum(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for b in bytes {
        h ^= *b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

fn store_err<E: std::fmt::Display>(e: E) -> EngineError {
    EngineError::Storage(e.to_string())
}

fn to_json<T: serde::Serialize>(v: &T) -> EngineResult<Vec<u8>> {
    serde_json::to_vec(v).map_err(store_err)
}

fn hex_lower(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

/// Path-safe, collision-free per-queue key prefix: `t/{hex(tenant)}/q/{hex(queue)}/` (TD-004 §Object Layout:
/// tenant/queue leading; hex-encoded so arbitrary id bytes are S3-key-safe).
fn shard_prefix(shard: &QueueKey) -> String {
    format!(
        "t/{}/q/{}/",
        hex_lower(shard.tenant_id.as_str().as_bytes()),
        hex_lower(shard.queue_id.as_str().as_bytes())
    )
}

// ---------------------------------------------------------------------------
// The segmented object log
// ---------------------------------------------------------------------------

struct ShardBuf {
    buffered: Vec<CommandEnvelope>,
    buffered_bytes: usize,
    /// `now` (ms) of the oldest buffered command, for the latency seal trigger.
    oldest_buffered_ms: Option<i64>,
    /// Next sequence to assign (recovered from the manifest on open).
    next_seq: u64,
    /// Next manifest object index to write (recovered from the manifest on open).
    next_manifest_index: u64,
    /// Highest epoch any committed manifest entry records (the queue's current epoch).
    committed_epoch: u64,
}

struct Inner {
    shards: BTreeMap<QueueKey, ShardBuf>,
    counters: SegmentCounters,
}

/// The outcome of an [`SegmentedObjectLog::enqueue`]: any positions that were acked because a size-triggered
/// seal fired during this call, plus how many commands remain buffered (un-acked) for the queue.
#[derive(Debug, Clone, Default)]
pub struct EnqueueOutcome {
    /// Positions acked by a seal that fired during this enqueue (empty if the command was only buffered).
    pub committed: Vec<CommandPosition>,
    /// Commands still buffered (not yet sealed → not yet acked) for this queue.
    pub pending: usize,
}

/// Segmented, group-committing object log over an S3-compatible [`BlobStore`].
pub struct SegmentedObjectLog<S: BlobStore> {
    store: S,
    config: SegmentConfig,
    inner: Mutex<Inner>,
}

impl<S: BlobStore> SegmentedObjectLog<S> {
    /// Open a segmented object log over `store` with `config`.
    pub fn open(store: S, config: SegmentConfig) -> Self {
        Self {
            store,
            config,
            inner: Mutex::new(Inner {
                shards: BTreeMap::new(),
                counters: SegmentCounters::default(),
            }),
        }
    }

    /// Register a queue and recover its committed position + epoch from the manifest (idempotent).
    pub fn create_queue(&self, def: &QueueDefinition) -> EngineResult<()> {
        let shard = QueueKey::new(def.tenant_id.clone(), def.queue_id.clone());
        let (next_seq, next_index, epoch) = self.recover_manifest(&shard)?;
        let mut g = self.inner.lock().expect("segmented log poisoned");
        g.shards.entry(shard).or_insert(ShardBuf {
            buffered: Vec::new(),
            buffered_bytes: 0,
            oldest_buffered_ms: None,
            next_seq,
            next_manifest_index: next_index,
            committed_epoch: epoch,
        });
        Ok(())
    }

    /// Read the manifest from the store and derive `(next_seq, next_manifest_index, current_epoch)`.
    fn recover_manifest(&self, shard: &QueueKey) -> EngineResult<(u64, u64, u64)> {
        let entries = self.read_manifest(shard)?;
        let next_index = entries.iter().map(|e| e.index + 1).max().unwrap_or(0);
        let next_seq = entries
            .iter()
            .filter(|e| !e.fence)
            .map(|e| e.last_seq + 1)
            .max()
            .unwrap_or(0);
        let epoch = entries.iter().map(|e| e.epoch).max().unwrap_or(0);
        Ok((next_seq, next_index, epoch))
    }

    /// All manifest entries for `shard`, sorted by index.
    fn read_manifest(&self, shard: &QueueKey) -> EngineResult<Vec<ManifestEntry>> {
        let prefix = format!("{}manifest/", shard_prefix(shard));
        let mut entries = Vec::new();
        for key in self.store.list(&prefix)? {
            if let Some(bytes) = self.store.get(&key)? {
                let entry: ManifestEntry = serde_json::from_slice(&bytes).map_err(store_err)?;
                entries.push(entry);
            }
        }
        entries.sort_by_key(|e| e.index);
        Ok(entries)
    }

    /// The queue's current `assignment_epoch` (highest epoch any committed manifest entry records).
    pub fn current_epoch(&self, shard: &QueueKey) -> EngineResult<u64> {
        {
            let g = self.inner.lock().expect("segmented log poisoned");
            if !g.shards.contains_key(shard) {
                return Err(EngineError::NotFound);
            }
        }
        Ok(self.recover_manifest(shard)?.2)
    }

    /// Acquire the queue at a NEW, strictly-greater epoch by publishing a **fence entry** to the manifest
    /// via the create-only CAS (TD-003 durable-fence-before-use; TD-004 implementation (b)). After it
    /// commits, a prior-epoch writer's next seal observes the higher epoch and self-fences.
    pub fn acquire_epoch(&self, shard: &QueueKey, now_ms: i64) -> EngineResult<u64> {
        {
            let g = self.inner.lock().expect("segmented log poisoned");
            if !g.shards.contains_key(shard) {
                return Err(EngineError::NotFound);
            }
        }
        let prefix = shard_prefix(shard);
        // Bounded retry against concurrent acquirers (no consensus; the store CAS is the only primitive).
        for _ in 0..16 {
            let (next_seq, next_index, cur_epoch) = self.recover_manifest(shard)?;
            let new_epoch = cur_epoch + 1;
            let entry = ManifestEntry {
                index: next_index,
                epoch: new_epoch,
                fence: true,
                segment_key: None,
                first_seq: next_seq,
                last_seq: next_seq.saturating_sub(1),
                checksum: 0,
                committed_at_ms: now_ms,
            };
            let key = format!("{prefix}manifest/{next_index:020}.json");
            if self.store.put_if_absent(&key, &to_json(&entry)?)? {
                let mut g = self.inner.lock().expect("segmented log poisoned");
                g.counters.objects_put += 1;
                if let Some(buf) = g.shards.get_mut(shard) {
                    buf.committed_epoch = new_epoch;
                    buf.next_manifest_index = next_index + 1;
                }
                return Ok(new_epoch);
            }
            // Lost the CAS race; re-read and retry at the new tail.
        }
        Err(EngineError::Conflict)
    }

    /// Buffer `commands` for `shard` (TD-004 step 1). If the buffered byte size reaches `target_bytes`, a
    /// segment seals synchronously and its positions are acked in [`EnqueueOutcome::committed`]; otherwise
    /// the commands stay buffered and are NOT acked (TD-004 step 5: no ack before manifest commit).
    pub fn enqueue(
        &self,
        shard: &QueueKey,
        commands: &[CommandEnvelope],
        expected_epoch: u64,
        now_ms: i64,
    ) -> EngineResult<EnqueueOutcome> {
        // Class ban + gate validation BEFORE buffering (parity with the file reference write path).
        for env in commands {
            validate_gate_command(false, &env.command)?;
            if matches!(env.command, QueueCommand::ReplacePending(_)) {
                return Err(EngineError::Unavailable);
            }
        }
        let should_seal = {
            let mut g = self.inner.lock().expect("segmented log poisoned");
            let buf = g.shards.get_mut(shard).ok_or(EngineError::NotFound)?;
            for env in commands {
                buf.buffered_bytes += serde_json::to_vec(env).map_err(store_err)?.len();
                buf.buffered.push(env.clone());
                buf.oldest_buffered_ms.get_or_insert(now_ms);
            }
            let one_command_seal = self.config.dev_unsafe_one_command_segments;
            buf.buffered_bytes >= self.config.target_bytes
                || (one_command_seal && !buf.buffered.is_empty())
        };
        let committed = if should_seal {
            self.seal(shard, expected_epoch, now_ms)?
        } else {
            Vec::new()
        };
        let pending = self.pending(shard);
        Ok(EnqueueOutcome { committed, pending })
    }

    /// Seal the buffered commands for `shard` if the oldest has aged past `max_latency_ms` (TD-004 step 2
    /// latency trigger). Returns acked positions (empty if nothing was due).
    pub fn flush_due(
        &self,
        shard: &QueueKey,
        expected_epoch: u64,
        now_ms: i64,
    ) -> EngineResult<Vec<CommandPosition>> {
        let due = {
            let g = self.inner.lock().expect("segmented log poisoned");
            let buf = g.shards.get(shard).ok_or(EngineError::NotFound)?;
            match buf.oldest_buffered_ms {
                Some(oldest) if !buf.buffered.is_empty() => {
                    now_ms.saturating_sub(oldest) >= self.config.max_latency_ms as i64
                }
                _ => false,
            }
        };
        if due {
            self.seal(shard, expected_epoch, now_ms)
        } else {
            Ok(Vec::new())
        }
    }

    /// Force-seal whatever is buffered for `shard` into one segment, commit its manifest entry, and ack.
    ///
    /// The manifest commit is the ack boundary AND the epoch fence: the writer's `expected_epoch` MUST equal
    /// the queue's current epoch (read authoritatively from the manifest). A stale epoch is rejected
    /// [`EngineError::EpochFenced`] BEFORE the segment object is written (no torn/orphan segment), and the
    /// buffer is discarded (the fenced writer surrenders; the new owner re-drives on retry).
    pub fn seal(
        &self,
        shard: &QueueKey,
        expected_epoch: u64,
        now_ms: i64,
    ) -> EngineResult<Vec<CommandPosition>> {
        let prefix = shard_prefix(shard);
        // 1. Snapshot+drain the buffer under the lock; nothing buffered → nothing to do.
        let drained = {
            let mut g = self.inner.lock().expect("segmented log poisoned");
            let buf = g.shards.get_mut(shard).ok_or(EngineError::NotFound)?;
            if buf.buffered.is_empty() {
                return Ok(Vec::new());
            }
            std::mem::take(&mut buf.buffered)
        };
        let n = drained.len();

        // 2. Epoch fence: read the AUTHORITATIVE current epoch + tail from the manifest. A stale writer is
        //    fenced here, before any segment object is written.
        let (cur_seq, cur_index, cur_epoch) = self.recover_manifest(shard)?;
        {
            let mut g = self.inner.lock().expect("segmented log poisoned");
            let buf = g.shards.get_mut(shard).ok_or(EngineError::NotFound)?;
            // Re-sync the in-memory view to the manifest (a peer/new-owner may have advanced it).
            buf.next_seq = cur_seq;
            buf.next_manifest_index = cur_index;
            buf.committed_epoch = cur_epoch;
            if expected_epoch != cur_epoch {
                // Fenced: discard the buffer (the commands are unacked; no segment, no manifest entry).
                buf.buffered_bytes = 0;
                buf.oldest_buffered_ms = None;
                return Err(EngineError::EpochFenced);
            }
        }

        // 3. Write the immutable, checksummed segment object (idempotent at its first-seq key).
        let first_seq = cur_seq;
        let last_seq = first_seq + n as u64 - 1;
        let segment = Segment {
            epoch: cur_epoch,
            first_seq,
            commands: drained.clone(),
        };
        let seg_bytes = to_json(&segment)?;
        let seg_checksum = checksum(&seg_bytes);
        let seg_key = format!("{prefix}seg/{first_seq:020}.seg");
        self.store.put(&seg_key, &seg_bytes)?;
        {
            let mut g = self.inner.lock().expect("segmented log poisoned");
            g.counters.objects_put += 1;
        }

        // 4. Commit the manifest entry via the create-only CAS at the next index.
        let entry = ManifestEntry {
            index: cur_index,
            epoch: cur_epoch,
            fence: false,
            segment_key: Some(seg_key),
            first_seq,
            last_seq,
            checksum: seg_checksum,
            committed_at_ms: now_ms,
        };
        let manifest_key = format!("{prefix}manifest/{cur_index:020}.json");
        let won = self.store.put_if_absent(&manifest_key, &to_json(&entry)?)?;
        if !won {
            // CAS lost: a peer extended the manifest from the same tail. Re-read to learn the new epoch.
            let observed_epoch = self.recover_manifest(shard)?.2;
            let mut g = self.inner.lock().expect("segmented log poisoned");
            let buf = g.shards.get_mut(shard).ok_or(EngineError::NotFound)?;
            buf.buffered_bytes = 0;
            buf.oldest_buffered_ms = None;
            if observed_epoch > expected_epoch {
                return Err(EngineError::EpochFenced);
            }
            // Same-epoch transient race: the segment object is an orphan (no manifest entry); the caller
            // retries. Surface as a conflict so it is not mistaken for an ack.
            return Err(EngineError::Conflict);
        }

        // 5. Ack: the manifest entry is durable. Advance state + counters, then return positions.
        let mut positions = Vec::with_capacity(n);
        for i in 0..n {
            positions.push(CommandPosition::new(
                shard.clone(),
                cur_epoch,
                first_seq + i as u64,
            ));
        }
        {
            let mut g = self.inner.lock().expect("segmented log poisoned");
            g.counters.objects_put += 1;
            g.counters.segments_sealed += 1;
            g.counters.commands_committed += n as u64;
            g.counters.group_commit_batches.push(n);
            let buf = g.shards.get_mut(shard).ok_or(EngineError::NotFound)?;
            buf.next_seq = last_seq + 1;
            buf.next_manifest_index = cur_index + 1;
            buf.buffered_bytes = 0;
            buf.oldest_buffered_ms = None;
        }
        Ok(positions)
    }

    /// Commands buffered (un-acked) for `shard`.
    pub fn pending(&self, shard: &QueueKey) -> usize {
        self.inner
            .lock()
            .expect("segmented log poisoned")
            .shards
            .get(shard)
            .map(|b| b.buffered.len())
            .unwrap_or(0)
    }

    /// Replay every **manifest-committed** command for `shard` in sequence order (recovery / read path).
    /// Only segments named by a committed manifest entry are visible — a buffered-but-unsealed command or a
    /// fenced orphan segment is NOT returned, which is what makes "ack only after manifest commit"
    /// observable. Per-segment checksums are validated.
    pub fn read_all(
        &self,
        shard: &QueueKey,
    ) -> EngineResult<Vec<(CommandPosition, CommandEnvelope)>> {
        let mut out = Vec::new();
        for entry in self.read_manifest(shard)? {
            if entry.fence {
                continue;
            }
            let Some(seg_key) = entry.segment_key.as_ref() else {
                continue;
            };
            let bytes = self
                .store
                .get(seg_key)?
                .ok_or(EngineError::Storage(format!("missing segment {seg_key}")))?;
            if checksum(&bytes) != entry.checksum {
                return Err(EngineError::Storage(format!(
                    "segment checksum mismatch at {seg_key}"
                )));
            }
            let segment: Segment = serde_json::from_slice(&bytes).map_err(store_err)?;
            for (i, env) in segment.commands.into_iter().enumerate() {
                let pos = CommandPosition::new(
                    shard.clone(),
                    segment.epoch,
                    segment.first_seq + i as u64,
                );
                out.push((pos, env));
            }
        }
        Ok(out)
    }

    /// A snapshot of the measured segment/object counters (release-ledger harness surface).
    pub fn counters(&self) -> SegmentCounters {
        self.inner
            .lock()
            .expect("segmented log poisoned")
            .counters
            .clone()
    }
}

// ---------------------------------------------------------------------------
// Minimal hand-rolled S3 client (SigV4 PUT/GET/LIST + create-only conditional PUT)
// ---------------------------------------------------------------------------

/// A minimal S3-compatible [`BlobStore`] over plain HTTP/1.1 + SigV4. Deliberately dependency-light: the
/// only crate it pulls beyond the workspace baseline is `sha2` (already in-tree for the relational
/// projection); HMAC-SHA256, the SigV4 canonical request, the HTTP/1.1 framing, and the (small) ListObjects
/// XML scrape are hand-rolled. Targets MinIO / any S3-compatible store over `http://host:port` (path-style
/// addressing). The manifest CAS uses `If-None-Match: *` (create-only conditional PUT), which MinIO and S3
/// both support and which needs no ETag round-trip.
pub struct S3BlobStore {
    host: String,
    port: u16,
    bucket: String,
    access_key: String,
    secret_key: String,
    region: String,
}

impl S3BlobStore {
    /// Build a client. `endpoint` is `http://host:port` (the orbstack container IP form). The bucket must
    /// exist (or call [`S3BlobStore::create_bucket`]).
    pub fn new(
        endpoint: &str,
        bucket: &str,
        access_key: &str,
        secret_key: &str,
        region: &str,
    ) -> EngineResult<Self> {
        let rest = endpoint
            .strip_prefix("http://")
            .ok_or(EngineError::Invalid("endpoint must be http://host:port"))?;
        let (host, port) = match rest.split_once(':') {
            Some((h, p)) => (
                h.to_string(),
                p.trim_end_matches('/')
                    .parse::<u16>()
                    .map_err(|_| EngineError::Invalid("bad endpoint port"))?,
            ),
            None => (rest.trim_end_matches('/').to_string(), 80),
        };
        Ok(Self {
            host,
            port,
            bucket: bucket.to_string(),
            access_key: access_key.to_string(),
            secret_key: secret_key.to_string(),
            region: region.to_string(),
        })
    }

    /// Create the bucket (idempotent: an existing bucket's 409/200 is treated as success).
    pub fn create_bucket(&self) -> EngineResult<()> {
        let path = format!("/{}", self.bucket);
        let (status, _) = self.request("PUT", &path, &[], &[], &[])?;
        if status == 200 || status == 204 || status == 409 {
            Ok(())
        } else {
            Err(EngineError::Storage(format!(
                "create_bucket failed: HTTP {status}"
            )))
        }
    }

    fn object_path(&self, key: &str) -> String {
        format!("/{}/{}", self.bucket, key)
    }

    /// Sign and send one HTTP/1.1 request over a fresh `Connection: close` TCP stream. Returns
    /// `(status, body)`. `query` is the (unencoded) query params; `extra_headers` are additional
    /// signed headers (e.g. the conditional `If-None-Match`).
    fn request(
        &self,
        method: &str,
        path: &str,
        query: &[(String, String)],
        body: &[u8],
        extra_headers: &[(String, String)],
    ) -> EngineResult<(u16, Vec<u8>)> {
        let (amz_date, datestamp) = amz_dates();
        let payload_hash = hex_lower(&sha256(body));
        let host_header = format!("{}:{}", self.host, self.port);

        // Canonical headers (lowercased, sorted): host, x-amz-content-sha256, x-amz-date, + extras.
        let mut headers: Vec<(String, String)> = vec![
            ("host".into(), host_header.clone()),
            ("x-amz-content-sha256".into(), payload_hash.clone()),
            ("x-amz-date".into(), amz_date.clone()),
        ];
        for (k, v) in extra_headers {
            headers.push((k.to_lowercase(), v.clone()));
        }
        headers.sort_by(|a, b| a.0.cmp(&b.0));
        let signed_headers = headers
            .iter()
            .map(|(k, _)| k.as_str())
            .collect::<Vec<_>>()
            .join(";");
        let canonical_headers = headers
            .iter()
            .map(|(k, v)| format!("{k}:{}\n", v.trim()))
            .collect::<String>();

        let canonical_uri = uri_encode(path, false);
        let mut q = query.to_vec();
        q.sort();
        let canonical_query = q
            .iter()
            .map(|(k, v)| format!("{}={}", uri_encode(k, true), uri_encode(v, true)))
            .collect::<Vec<_>>()
            .join("&");

        let canonical_request = format!(
            "{method}\n{canonical_uri}\n{canonical_query}\n{canonical_headers}\n{signed_headers}\n{payload_hash}"
        );
        let scope = format!("{datestamp}/{}/s3/aws4_request", self.region);
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
            hex_lower(&sha256(canonical_request.as_bytes()))
        );
        let signing_key = self.signing_key(&datestamp);
        let signature = hex_lower(&hmac_sha256(&signing_key, string_to_sign.as_bytes()));
        let authorization = format!(
            "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed_headers}, Signature={signature}",
            self.access_key
        );

        // Build the raw HTTP/1.1 request.
        let target = if canonical_query.is_empty() {
            canonical_uri.clone()
        } else {
            format!("{canonical_uri}?{canonical_query}")
        };
        let mut req = format!("{method} {target} HTTP/1.1\r\n");
        req.push_str(&format!("Host: {host_header}\r\n"));
        req.push_str(&format!("x-amz-date: {amz_date}\r\n"));
        req.push_str(&format!("x-amz-content-sha256: {payload_hash}\r\n"));
        for (k, v) in extra_headers {
            req.push_str(&format!("{k}: {v}\r\n"));
        }
        req.push_str(&format!("Authorization: {authorization}\r\n"));
        req.push_str(&format!("Content-Length: {}\r\n", body.len()));
        req.push_str("Connection: close\r\n\r\n");

        let mut stream = TcpStream::connect((self.host.as_str(), self.port)).map_err(store_err)?;
        stream.write_all(req.as_bytes()).map_err(store_err)?;
        stream.write_all(body).map_err(store_err)?;
        let mut raw = Vec::new();
        stream.read_to_end(&mut raw).map_err(store_err)?;
        parse_http_response(&raw)
    }

    fn signing_key(&self, datestamp: &str) -> [u8; 32] {
        let k_date = hmac_sha256(
            format!("AWS4{}", self.secret_key).as_bytes(),
            datestamp.as_bytes(),
        );
        let k_region = hmac_sha256(&k_date, self.region.as_bytes());
        let k_service = hmac_sha256(&k_region, b"s3");
        hmac_sha256(&k_service, b"aws4_request")
    }
}

impl BlobStore for S3BlobStore {
    fn put(&self, key: &str, body: &[u8]) -> EngineResult<()> {
        let (status, resp) = self.request("PUT", &self.object_path(key), &[], body, &[])?;
        if status == 200 || status == 204 {
            Ok(())
        } else {
            Err(EngineError::Storage(format!(
                "S3 PUT {key} failed: HTTP {status}: {}",
                String::from_utf8_lossy(&resp)
            )))
        }
    }

    fn put_if_absent(&self, key: &str, body: &[u8]) -> EngineResult<bool> {
        // Create-only conditional PUT: `If-None-Match: *` succeeds only if the object does not yet exist.
        let extra = vec![("If-None-Match".to_string(), "*".to_string())];
        let (status, resp) = self.request("PUT", &self.object_path(key), &[], body, &extra)?;
        match status {
            200 | 204 => Ok(true),
            // 412 Precondition Failed (object exists) / 409 Conflict → the CAS lost.
            409 | 412 => Ok(false),
            _ => Err(EngineError::Storage(format!(
                "S3 conditional PUT {key} failed: HTTP {status}: {}",
                String::from_utf8_lossy(&resp)
            ))),
        }
    }

    fn get(&self, key: &str) -> EngineResult<Option<Vec<u8>>> {
        let (status, body) = self.request("GET", &self.object_path(key), &[], &[], &[])?;
        match status {
            200 => Ok(Some(body)),
            404 => Ok(None),
            _ => Err(EngineError::Storage(format!(
                "S3 GET {key} failed: HTTP {status}"
            ))),
        }
    }

    fn list(&self, prefix: &str) -> EngineResult<Vec<String>> {
        let query = vec![
            ("list-type".to_string(), "2".to_string()),
            ("prefix".to_string(), prefix.to_string()),
        ];
        let path = format!("/{}", self.bucket);
        let (status, body) = self.request("GET", &path, &query, &[], &[])?;
        if status != 200 {
            return Err(EngineError::Storage(format!(
                "S3 LIST {prefix} failed: HTTP {status}: {}",
                String::from_utf8_lossy(&body)
            )));
        }
        Ok(scrape_keys(&String::from_utf8_lossy(&body)))
    }
}

/// Scrape `<Key>…</Key>` values out of an S3 ListObjectsV2 XML body (small-result single page is sufficient
/// for the per-queue manifest/segment listings the substrate issues).
fn scrape_keys(xml: &str) -> Vec<String> {
    let mut keys = Vec::new();
    let mut rest = xml;
    while let Some(start) = rest.find("<Key>") {
        let after = &rest[start + 5..];
        if let Some(end) = after.find("</Key>") {
            keys.push(after[..end].to_string());
            rest = &after[end + 6..];
        } else {
            break;
        }
    }
    keys
}

/// Parse `(status_code, body)` from a raw HTTP/1.1 response (headers terminated by `\r\n\r\n`).
fn parse_http_response(raw: &[u8]) -> EngineResult<(u16, Vec<u8>)> {
    let split = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .ok_or(EngineError::Storage("malformed HTTP response".into()))?;
    let head = &raw[..split];
    let body = raw[split + 4..].to_vec();
    let head_str = String::from_utf8_lossy(head);
    let status_line = head_str
        .lines()
        .next()
        .ok_or(EngineError::Storage("empty HTTP response".into()))?;
    // "HTTP/1.1 200 OK"
    let status = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse::<u16>().ok())
        .ok_or(EngineError::Storage("no HTTP status code".into()))?;
    Ok((status, body))
}

// ---------------------------------------------------------------------------
// Crypto + SigV4 helpers (hand-rolled over `sha2`; no extra crates)
// ---------------------------------------------------------------------------

fn sha256(data: &[u8]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(data);
    h.finalize().into()
}

/// HMAC-SHA256 (RFC 2104) over `sha2::Sha256`; block size 64.
fn hmac_sha256(key: &[u8], msg: &[u8]) -> [u8; 32] {
    const BLOCK: usize = 64;
    let mut k = [0u8; BLOCK];
    if key.len() > BLOCK {
        k[..32].copy_from_slice(&sha256(key));
    } else {
        k[..key.len()].copy_from_slice(key);
    }
    let mut ipad = [0x36u8; BLOCK];
    let mut opad = [0x5cu8; BLOCK];
    for i in 0..BLOCK {
        ipad[i] ^= k[i];
        opad[i] ^= k[i];
    }
    let mut inner = Sha256::new();
    inner.update(ipad);
    inner.update(msg);
    let inner_hash = inner.finalize();
    let mut outer = Sha256::new();
    outer.update(opad);
    outer.update(inner_hash);
    outer.finalize().into()
}

/// Percent-encode per SigV4 rules: unreserved `A-Za-z0-9-_.~` pass through; `/` passes through unless
/// `encode_slash`; everything else is `%XX` (uppercase hex).
fn uri_encode(s: &str, encode_slash: bool) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        let keep = b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~');
        if keep || (b == b'/' && !encode_slash) {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// Current UTC as `(amz_date "YYYYMMDDTHHMMSSZ", datestamp "YYYYMMDD")` from the system clock — no chrono.
fn amz_dates() -> (String, String) {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let days = secs.div_euclid(86_400);
    let tod = secs.rem_euclid(86_400);
    let (h, mi, s) = (tod / 3600, (tod % 3600) / 60, tod % 60);
    let (y, mo, d) = civil_from_days(days);
    (
        format!("{y:04}{mo:02}{d:02}T{h:02}{mi:02}{s:02}Z"),
        format!("{y:04}{mo:02}{d:02}"),
    )
}

/// Days-since-1970-01-01 → `(year, month, day)` (Howard Hinnant's civil-from-days algorithm).
fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y }, m, d)
}
